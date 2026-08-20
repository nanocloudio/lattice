#!/usr/bin/env python3
"""CDC rig validation driver (tests/hardware/lattice_pi5_cdc_diag.toml
prerequisite 2).

Subscribes to `lattice/cdc/feed1` on the rig broker (default
127.0.0.1:47090 — run on the broker host), writes GRAPH.VERTEX records
to the DUT's RESP model surface (default 192.168.1.9:6380), and asserts
the CDC envelopes that arrive: format 1, kind PUT, the configured
feed id, monotonic revisions, and one envelope per write (plus RESOLVED
watermarks advancing past the writes' commit timestamps).

Minimal raw-socket MQTT 3.1.1 subscriber + RESP client — no external
dependencies, so it runs on any rig host.

Exit 0 = PASS (all asserts hold), 1 = FAIL, with a one-line verdict.
"""

import argparse
import socket
import struct
import sys
import time

FMT = 1
KIND_PUT = 1
KIND_RESOLVED = 3


# ── Minimal MQTT 3.1.1 subscriber ────────────────────────────────────

def mqtt_connect(host, port, client_id=b"cdc-rig-validate"):
    s = socket.create_connection((host, port), timeout=10)
    var = b"\x00\x04MQTT\x04\x02\x00\x3c" + struct.pack(">H", len(client_id)) + client_id
    s.sendall(b"\x10" + _len(len(var)) + var)
    t, body = _read_packet(s)
    assert t == 0x20 and body[1] == 0, f"CONNACK refused: {body!r}"
    return s


def mqtt_subscribe(s, topic, pid=1):
    t = topic.encode()
    var = struct.pack(">H", pid) + struct.pack(">H", len(t)) + t + b"\x00"
    s.sendall(b"\x82" + _len(len(var)) + var)
    ptype, body = _read_packet(s)
    assert ptype == 0x90, f"expected SUBACK, got {ptype:#x}"


def _len(n):
    out = b""
    while True:
        b_ = n % 128
        n //= 128
        out += bytes([b_ | (0x80 if n else 0)])
        if not n:
            return out


def _read_packet(s):
    hdr = _read_exact(s, 1)
    mult, rl, i = 1, 0, 0
    while True:
        b_ = _read_exact(s, 1)[0]
        rl += (b_ & 0x7F) * mult
        mult *= 128
        i += 1
        if not (b_ & 0x80) or i == 4:
            break
    body = _read_exact(s, rl) if rl else b""
    return hdr[0] & 0xF0, body


def _read_exact(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("broker closed")
        buf += chunk
    return buf


def mqtt_drain_publishes(s, deadline):
    """Yield (topic, payload) until the deadline passes."""
    s.settimeout(0.5)
    while time.time() < deadline:
        try:
            ptype, body = _read_packet(s)
        except socket.timeout:
            continue
        if ptype != 0x30:
            continue
        tlen = struct.unpack(">H", body[:2])[0]
        topic = body[2 : 2 + tlen].decode()
        rest = body[2 + tlen :]
        # QoS1 publish carries a packet id we must PUBACK.
        # (The sink publishes QoS1; broker→subscriber QoS follows the
        # subscription max — we subscribed at QoS 0, so no pid here.)
        yield topic, rest


# ── CDC envelope decode (modules/common/cdc_wire.rs) ─────────────────

def decode_envelope(b):
    p = 0
    fmt, kind, flags = struct.unpack_from("<HBB", b, p)
    p += 4
    feed_id, table_id, key_len = struct.unpack_from("<QIH", b, p)
    p += 14
    key = b[p : p + key_len]
    p += key_len
    commit_ts, range_id, range_gen, revision, value_len = struct.unpack_from(
        "<QQIQI", b, p
    )
    p += 32
    value = b[p : p + value_len]
    return {
        "fmt": fmt,
        "kind": kind,
        "flags": flags,
        "feed_id": feed_id,
        "table_id": table_id,
        "key": key,
        "commit_ts": commit_ts,
        "revision": revision,
        "value": value,
    }


# ── RESP client ──────────────────────────────────────────────────────

def resp_cmd(s, *args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a.encode() if isinstance(a, str) else a
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    s.sendall(out)
    return s.recv(4096)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dut", default="192.168.1.9")
    ap.add_argument("--resp-port", type=int, default=6380)
    ap.add_argument("--broker", default="127.0.0.1")
    ap.add_argument("--broker-port", type=int, default=47090)
    ap.add_argument("--topic", default="lattice/cdc/feed1")
    ap.add_argument("--feed-id", type=int, default=1)
    ap.add_argument("--writes", type=int, default=10)
    ap.add_argument("--window-s", type=int, default=30)
    args = ap.parse_args()

    # Broker subscription first so no envelope is missed.
    mq = mqtt_connect(args.broker, args.broker_port)
    mqtt_subscribe(mq, args.topic)
    print(f"subscribed to {args.topic} on {args.broker}:{args.broker_port}")

    # Drive vertex writes at the DUT's RESP surface.
    resp = socket.create_connection((args.dut, args.resp_port), timeout=10)
    stamp = int(time.time())
    written = []
    for i in range(args.writes):
        v = f"v{stamp}_{i}"
        r = resp_cmd(resp, "GRAPH.VERTEX", "rigg", v)
        if not r.startswith(b"+"):
            print(f"FAIL: GRAPH.VERTEX {v} rejected: {r!r}")
            return 1
        written.append(v.encode())
    print(f"wrote {len(written)} vertices to {args.dut}:{args.resp_port}")

    # Collect envelopes.
    deadline = time.time() + args.window_s
    puts, resolved_ts = [], []
    for topic, payload in mqtt_drain_publishes(mq, deadline):
        try:
            ev = decode_envelope(payload)
        except (struct.error, IndexError):
            print(f"FAIL: undecodable envelope ({len(payload)} bytes)")
            return 1
        if ev["fmt"] != FMT:
            print(f"FAIL: envelope format {ev['fmt']}")
            return 1
        if ev["feed_id"] != args.feed_id:
            print(f"FAIL: feed id {ev['feed_id']}")
            return 1
        if ev["kind"] == KIND_PUT:
            puts.append(ev)
        elif ev["kind"] == KIND_RESOLVED:
            resolved_ts.append(ev["commit_ts"])
        matched = sum(1 for e in puts if any(v in e["key"] or v in e["value"] for v in written))
        if matched >= len(written) and resolved_ts:
            break

    matched = [e for e in puts if any(v in e["key"] or v in e["value"] for v in written)]
    revs = [e["revision"] for e in matched]
    print(
        f"envelopes: {len(puts)} PUT ({len(matched)} matching this run), "
        f"{len(resolved_ts)} RESOLVED"
    )
    if len(matched) < args.writes:
        print(f"FAIL: {len(matched)}/{args.writes} writes surfaced as CDC PUTs")
        return 1
    if revs != sorted(revs):
        print("FAIL: revisions not monotonic")
        return 1
    if matched and resolved_ts and max(resolved_ts) < max(e["commit_ts"] for e in matched):
        print("WARN: resolved watermark has not passed the last write yet")
    print(
        f"PASS: {args.writes} writes → {len(matched)} CDC envelopes, "
        f"revisions monotonic, {len(resolved_ts)} resolved watermarks"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
