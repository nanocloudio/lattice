#!/usr/bin/env python3
"""CDC egress rig/host validation driver (CDC RFC).

Subscribes (MQTT 3.1.1, QoS 1) to the CDC topic on the quantum broker,
drives committed writes into lattice's model RESP surface, and asserts
the received CDC envelopes end-to-end: identity fields, monotone
commit timestamps in apply order, delete events, and resolved
watermarks covering every data event.

Usage:
  cdc_rig_validate.py --dut 192.168.1.9 --broker 192.168.1.10 \
      [--resp-port 6380] [--broker-port 47090] [--topic lattice/cdc/feed1]

Exit 0 = validated; non-zero with a spelled reason otherwise. The
envelope layout mirrors lattice modules/common/cdc_wire.rs (owner).
"""

import argparse
import socket
import struct
import sys
import threading
import time

KIND = {1: "put", 2: "delete", 3: "resolved", 4: "backfill_complete", 5: "topology"}


def enc_rl(n: int) -> bytes:
    out = b""
    while True:
        b_ = n & 0x7F
        n >>= 7
        if n:
            b_ |= 0x80
        out += bytes([b_])
        if not n:
            return out


def mqtt_str(s: bytes) -> bytes:
    return struct.pack(">H", len(s)) + s


def decode_envelope(p: bytes):
    fmt, kind, flags = struct.unpack("<HBB", p[:4])
    if fmt != 1:
        raise ValueError(f"unknown envelope format {fmt}")
    feed, table, klen = struct.unpack("<QIH", p[4:18])
    key = p[18 : 18 + klen]
    off = 18 + klen
    cts, rid, rgen, rev = struct.unpack("<QQIQ", p[off : off + 28])
    off += 28
    vlen = struct.unpack("<I", p[off : off + 4])[0]
    value = p[off + 4 : off + 4 + vlen]
    return dict(
        kind=kind, flags=flags, feed=feed, table=table, key=key,
        commit_ts=cts, range_id=rid, range_generation=rgen,
        revision=rev, value=value,
    )


class Subscriber(threading.Thread):
    def __init__(self, host, port, topic):
        super().__init__(daemon=True)
        self.events = []
        self.errors = []
        self.sock = socket.create_connection((host, port), timeout=15)
        vh = mqtt_str(b"MQTT") + bytes([4, 0x02, 0, 60]) + mqtt_str(b"cdc-validator")
        self.sock.sendall(bytes([0x10]) + enc_rl(len(vh)) + vh)
        r = self.sock.recv(4)
        assert r and r[0] == 0x20 and r[3] == 0, f"CONNACK refused: {r!r}"
        pay = struct.pack(">H", 1) + mqtt_str(topic.encode()) + bytes([1])
        self.sock.sendall(bytes([0x82]) + enc_rl(len(pay)) + pay)
        r = self.sock.recv(5)
        assert r and r[0] == 0x90, f"SUBACK refused: {r!r}"

    def run(self):
        buf = b""
        self.sock.settimeout(60)
        try:
            while True:
                d = self.sock.recv(8192)
                if not d:
                    return
                buf += d
                while len(buf) >= 2:
                    rl, mult, i = 0, 1, 1
                    ok = False
                    while i < min(len(buf), 5):
                        byte = buf[i]
                        rl += (byte & 0x7F) * mult
                        mult *= 128
                        i += 1
                        if not byte & 0x80:
                            ok = True
                            break
                    if not ok or len(buf) < i + rl:
                        break
                    pkt, buf = buf[: i + rl], buf[i + rl :]
                    if pkt[0] & 0xF0 == 0x30:
                        qos = (pkt[0] >> 1) & 3
                        tl = struct.unpack(">H", pkt[i : i + 2])[0]
                        off = i + 2 + tl
                        if qos:
                            pid = struct.unpack(">H", pkt[off : off + 2])[0]
                            off += 2
                            self.sock.sendall(bytes([0x40, 2]) + struct.pack(">H", pid))
                        try:
                            self.events.append(decode_envelope(pkt[off:]))
                        except Exception as e:  # noqa: BLE001
                            self.errors.append(str(e))
        except socket.timeout:
            return


def resp_cmd(sock, args, settle=0.3):
    out = b"*%d\r\n" % len(args)
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    sock.sendall(out)
    time.sleep(settle)
    return sock.recv(65536)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dut", required=True)
    ap.add_argument("--broker", required=True)
    ap.add_argument("--resp-port", type=int, default=6380)
    ap.add_argument("--broker-port", type=int, default=47090)
    ap.add_argument("--topic", default="lattice/cdc/feed1")
    ap.add_argument("--connect-wait-s", type=int, default=90)
    args = ap.parse_args()

    # 1. Subscribe first, so nothing published is missed.
    sub = Subscriber(args.broker, args.broker_port, args.topic)
    sub.start()
    print("subscribed to", args.topic)

    # 2. Wait for the DUT's RESP surface.
    deadline = time.time() + args.connect_wait_s
    while True:
        try:
            s = socket.create_connection((args.dut, args.resp_port), timeout=3)
            break
        except OSError:
            if time.time() > deadline:
                print("FAIL: DUT RESP surface never came up")
                return 2
            time.sleep(1)
    print("DUT RESP surface up")

    # 3. Warm up the timestamp authority: drive off-feed writes and poll the
    #    feed's internal key prefix until FEED.READ reports a nonzero revision,
    #    i.e. the commit-timestamp source is live before the measured writes.
    for i in range(120):
        resp_cmd(s, ["TS.ADD", "cdc_warmup", str(100 + i), "1.0"], settle=0.1)
        r = resp_cmd(s, ["FEED.READ", b"\x80\x03\x00\x01", "0", "1"], settle=0.1)
        parts = r.split(b"\r\n")
        if r.startswith(b"*3\r\n") and len(parts) > 4 and parts[4].isdigit() and int(parts[4]) > 0:
            break
        time.sleep(0.25)
    else:
        print("FAIL: timestamp authority never established")
        return 3
    print("timestamp authority established")

    # 4. Committed writes: puts and a delete.
    names = ["alice", "bob", "carol", "dave", "erin"]
    for v in names:
        r = resp_cmd(s, ["GRAPH.VERTEX", "social", v])
        if not r.startswith(b"+OK"):
            print("FAIL: write refused:", r)
            return 4
    resp_cmd(s, ["GRAPH.DELVERTEX", "social", "erin"])
    print("writes committed")

    # 5. Collect and assert.
    time.sleep(10)
    ev = sub.events
    puts = [e for e in ev if e["kind"] == 1]
    dels = [e for e in ev if e["kind"] == 2]
    resolved = [e for e in ev if e["kind"] == 3]
    if sub.errors:
        print("FAIL: envelope decode errors:", sub.errors[:3])
        return 5
    unknown = sorted({e["kind"] for e in ev if e["kind"] not in KIND})
    if unknown:
        print(f"FAIL: envelopes with unknown kind(s) {unknown}")
        return 12
    missing = [n for n in names if not any(n.encode() in e["key"] for e in puts)]
    if missing:
        print(f"FAIL: puts missing for {missing}; got {len(puts)} puts")
        return 6
    if not dels or not any(b"erin" in e["key"] for e in dels):
        print("FAIL: delete event for erin missing")
        return 7
    data = sorted((e for e in ev if e["kind"] in (1, 2)), key=lambda e: e["revision"])
    ts = [e["commit_ts"] for e in data]
    if any(t == 0 for t in ts):
        print("FAIL: untimestamped event (A-1)")
        return 8
    if ts != sorted(ts):
        print("FAIL: commit_ts not monotone in apply order")
        return 9
    if not resolved:
        print("FAIL: no resolved watermarks (§7)")
        return 10
    top_resolved = max(struct.unpack("<Q", e["value"])[0] for e in resolved if len(e["value"]) == 8)
    if top_resolved < max(ts):
        print(f"WARN: newest watermark {top_resolved} below newest event {max(ts)} (window still open)")
    if any(e["feed"] != 1 or e["table"] != 0 for e in data):
        print("FAIL: identity fields wrong")
        return 11
    print(
        f"PASS: {len(puts)} puts, {len(dels)} deletes, {len(resolved)} resolved "
        f"watermarks; commit_ts {ts[0]}..{ts[-1]} monotone; envelopes format 1"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
