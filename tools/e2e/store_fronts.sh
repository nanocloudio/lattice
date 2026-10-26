#!/usr/bin/env bash
# Live proof of lattice's in-graph fronts: examples/store_fronts/linux.yaml
# includes packaging/stack/lattice_store.yaml by path and puts an exchange_cli
# requester on each front. No server, no socket, no docker.
#
#   sql   a round trip (CREATE, INSERT, SELECT), the executor's refusal as
#         400 with its message, an empty statement refused 400
#   kv    SET/GET, an array reply whole, an unknown command as 400 with the
#         server's error text, a connection command refused 400, a 3072-byte
#         value SET as chunks and read back whole
#   cdc   a held subscription receiving the INSERT's change event (the
#         cdc_pump envelope, decoded and checked), then ended by the
#         requester's ABORT; after a restart, a resume `after: 0` receiving
#         the same event from history; a resume from a compacted position
#         answered 410
#   group three voters of packaging/stack/lattice_store_group3.yaml
#         (examples/store_fronts/group3.yaml): SQL and KV through the leader's
#         fronts commit on the group, and every replica's subscription
#         receives the INSERT's event at the same revision
#   catalogue
#         route_catalogue_front on the store (examples/store_fronts/
#         catalogue.yaml): a generation of two routes published as one
#         request of five records, each route sealed with its own manifest;
#         a route declared with an endpoint short refused 422; the withdrawn
#         form activated and read back sealed over nothing; a stale
#         predecessor refused 409; the next generation after the withdrawn
#         one; the changes after revision 0 holding all of it
#   catalogue_pairs
#         route_catalogue_front wired to the router directly
#         (examples/store_fronts/catalogue_pairs.yaml) on each of its four
#         client pairs in turn — etcd, redis, memcached, memcached_udp: a
#         generation published, the same generation again refused 409, a
#         snapshot page opening with the active pointer at it, the changes
#         holding its seal
#
# Each case runs the graph in a sandbox under target/ (its store directory
# with it), so nothing is written under the repository's own tree.
#
#   ./tools/e2e/store_fronts.sh            # all (group binds 19490..19492)
#   ./tools/e2e/store_fronts.sh sql cdc    # a subset
set -u
cd "$(dirname "$0")/../.."
ROOT=$PWD
FLX=${FLUXOR_BIN:-fluxor}
pass=0
fail=0
ok() { echo "  PASS  $1"; pass=$((pass + 1)); }
no() { echo "  FAIL  $1: $2"; fail=$((fail + 1)); }
want() { case "$1" in *"$2"*) return 0 ;; *) return 1 ;; esac }

mkdir -p "$ROOT/target"
SANDBOX=$(mktemp -d "$ROOT/target/e2e-store-fronts.XXXXXX")

# Signal $1 to every fluxor-linux this script started: `fluxor run` spawns it
# as a CHILD that can outlive its parent, and each runs in a case directory
# inside the sandbox — so the working directory picks out this run's, never
# another suite's graphs running from the same target dir.
reap() {
  local pid cwd
  for pid in $(pgrep -f "$ROOT/target/.*fluxor-linux"); do
    cwd=$(readlink "/proc/$pid/cwd" 2>/dev/null) || continue
    case "$cwd" in "$SANDBOX"/*) kill "-$1" "$pid" 2>/dev/null ;; esac
  done
}

# LEAK GUARD: reap any survivor on exit, and drop the sandbox. A failed run
# keeps its sandbox (each case's graph.log) for diagnosis.
trap 'reap KILL
  if [ "$fail" -eq 0 ]; then rm -rf -- "$SANDBOX"; else echo "sandbox kept: $SANDBOX"; fi' EXIT

# Render the example into case directory $1 (created fresh unless $3 says
# keep): its stack include made absolute, then each `sed` expression in
# $2 applied. The store directory is created beside it.
render() {
  local dir="$SANDBOX/$1"
  if [ "${3:-}" != keep ]; then
    rm -rf -- "$dir"
    mkdir -p "$dir/store_fronts.kv"
  fi
  sed -e "s#stack: ../../packaging/stack/#stack: $ROOT/packaging/stack/#" ${2:+-e "$2"} \
    examples/store_fronts/linux.yaml >"$dir/graph.yaml"
  printf '%s' "$dir"
}

# Run the graph in case directory $1, its stdin produced over time by the
# function named $2, bounded by $3 seconds; print what the requesters wrote.
run_case() {
  local dir=$1 t pid i out
  t=$(mktemp)
  (cd "$dir" && "$2" | FLUXOR_PROJECT_ROOT=$ROOT "$FLX" run graph.yaml >"$t" 2>"$dir/graph.log") 2>/dev/null &
  pid=$!
  i=$(( ${3:-20} * 2 ))
  while kill -0 "$pid" 2>/dev/null && [ "$i" -gt 0 ]; do sleep 0.5; i=$((i - 1)); done
  reap TERM
  kill "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  out=$(cat "$t")
  rm -f "$t"
  printf '%s' "$out"
}

# Decode the first `BODY <hex>` line of $1 as a CdcEvent envelope and check
# it is the put of row 1 ('one') in the rows keyspace; print its revision.
event_revision() {
  printf '%s\n' "$1" | python3 -I -c '
import struct, sys
for line in sys.stdin:
    if not line.startswith("BODY "):
        continue
    b = bytes.fromhex(line[5:].strip())
    fmt, kind, flags, feed, table, klen = struct.unpack_from("<HBBQIH", b, 0)
    key = b[18:18 + klen]
    p = 18 + klen
    ts, rid, rgen, rev, vlen = struct.unpack_from("<QQIQI", b, p)
    val = b[p + 32:p + 32 + vlen]
    good = (fmt == 1 and kind == 1 and flags == 0 and feed == 1 and table == 0
            and key[:4] == bytes.fromhex("80000002") and rid == 1 and rgen == 1
            and p + 32 + vlen == len(b) and b"one" in val)
    print(rev if good else "bad")
    break
'
}

sql_input() {
  sleep 3
  printf 'sql:CREATE TABLE t (id INT PRIMARY KEY, v TEXT)'
  sleep 1
  printf "sql:INSERT INTO t VALUES (1, 'one'), (2, 'two')"
  sleep 1
  printf 'sql:SELECT v FROM t ORDER BY id'
  sleep 1
  printf 'sql:SELECT * FROM nope'
  sleep 1
  printf 'sql:'
  sleep 2
}

sql() {
  local dir r
  dir=$(render sql)
  r=$(run_case "$dir" sql_input 20)
  if want "$r" "$(printf 'one\ntwo')"; then
    ok "sql  (CREATE, INSERT, SELECT -> first columns newline-joined)"
  else
    no sql "reply='$r'"
  fi
  if want "$r" "400 relation does not exist"; then
    ok "sql  (refused statement -> 400 with the executor's message)"
  else
    no sql-refusal "reply='$r'"
  fi
  if printf '%s\n' "$r" | grep -qx '400 '; then
    ok "sql  (empty statement -> 400, empty body)"
  else
    no sql-empty "reply='$r'"
  fi
}

kv_input() {
  sleep 3
  printf 'kv:\x03\x03\x00SET\x08\x00greeting\x02\x00hi'
  sleep 1
  printf 'kv:\x02\x03\x00GET\x08\x00greeting'
  sleep 1
  printf 'kv:\x03\x04\x00MGET\x08\x00greeting\x04\x00none'
  sleep 1
  printf 'kv:\x01\x04\x00NOPE'
  sleep 1
  printf 'kv:\x02\x06\x00SELECT\x01\x001'
  sleep 1
  # A value past one replicated entry: stored as chunks, read back whole.
  python3 -I -c 'import sys; v = b"0123456789abcdef" * 192
sys.stdout.buffer.write(b"kv:\x03\x03\x00SET\x03\x00big" + len(v).to_bytes(2, "little") + v)'
  sleep 1
  printf 'kv:\x02\x03\x00GET\x03\x00big'
  sleep 2
}

kv() {
  local dir r
  dir=$(render kv)
  r=$(run_case "$dir" kv_input 20)
  if want "$r" "$(printf 'OK\nhi\n')"; then
    ok "kv   (SET -> OK, GET -> the value)"
  else
    no kv "reply='$r'"
  fi
  if want "$r" "$(printf '*2\r\n$2\r\nhi\r\n$-1\r\n')"; then
    ok "kv   (MGET -> the whole RESP array)"
  else
    no kv-array "reply='$r'"
  fi
  if want "$r" "400 ERR unknown command 'NOPE'"; then
    ok "kv   (unknown command -> 400 with the server's error text)"
  else
    no kv-refusal "reply='$r'"
  fi
  if want "$r" "400 ERR 'SELECT' needs a connection"; then
    ok "kv   (connection command -> 400 naming why)"
  else
    no kv-session "reply='$r'"
  fi
  if printf '%s' "$r" | python3 -I -c 'import sys; sys.exit(0 if b"0123456789abcdef" * 192 in sys.stdin.buffer.read() else 1)'; then
    ok "kv   (a 3072-byte value: SET as chunks, GET back whole)"
  else
    no kv-large "reply length ${#r}"
  fi
}

cdc_input() {
  sleep 3
  printf 'sql:CREATE TABLE t (id INT PRIMARY KEY, v TEXT)'
  sleep 1
  printf "sql:INSERT INTO t VALUES (1, 'one')"
  sleep 3
}

idle_input() { sleep "${IDLE:-6}"; }

# Twelve writes, enough revisions for a store keeping four to compact.
writes_input() {
  sleep 3
  local i
  for i in 0 1 2 3 4 5 6 7 8 9 a b; do
    printf 'kv:\x03\x03\x00SET\x01\x00k\x01\x00%s' "$i"
    sleep 0.3
  done
  sleep 2
}

cdc() {
  local dir r rev r2 rev2
  dir=$(render cdc)
  r=$(run_case "$dir" cdc_input 20)
  rev=$(event_revision "$r")
  if want "$r" "HOLD 200 revision: " && [ -n "$rev" ] && [ "$rev" != bad ]; then
    ok "cdc  (GET /cdc/80000002 -> 200 HOLD|MORE, then the INSERT's cdc_pump envelope)"
  else
    no cdc "reply='$r' event='$rev'"
  fi
  if want "$r" ABORTED && ! want "$r" "ABORT 0"; then
    ok "cdc  (the requester's ABORT ends the held subscription)"
  else
    no cdc-abort "reply='$r'"
  fi
  # The same store, restarted: resume after revision 0 replays the event.
  render cdc "s#abort_after: 1#abort_after: 1\n      headers: \"after: 0\\\\r\\\\n\"#" keep >/dev/null
  r2=$(IDLE=6 run_case "$dir" idle_input 12)
  rev2=$(event_revision "$r2")
  if want "$r2" "HOLD 200 revision: 0" && [ "$rev2" = "$rev" ]; then
    ok "cdc  (restart, after: 0 -> the same event, from history, revision $rev2)"
  else
    no cdc-resume "reply='$r2' event='$rev2' want='$rev'"
  fi
  # A store keeping four revisions, written twelve times: after: 1 is gone.
  dir=$(render gone "s#store_dir: store_fronts.kv#store_dir: store_fronts.kv\n      history_revisions: 4#")
  run_case "$dir" writes_input 15 >/dev/null
  render gone "s#store_dir: store_fronts.kv#store_dir: store_fronts.kv\n      history_revisions: 4#;s#abort_after: 1#abort_after: 1\n      headers: \"after: 1\\\\r\\\\n\"#" keep >/dev/null
  r=$(IDLE=6 run_case "$dir" idle_input 12)
  if printf '%s\n' "$r" | grep -qx '410 '; then
    ok "cdc  (after a compacted position -> 410)"
  else
    no cdc-gone "reply='$r'"
  fi
}

# The replica leading at the highest term any replica has logged
# (`[raft] leader p=<partition> t=<term>`), or nothing before an election.
# Two replicas can each log leadership — a candidate wins term 1 and another
# term 2 a moment later — so the leader is the one with the highest term,
# never merely one that logged the line.
leader_now() {
  local n t best=-1 who=
  for n in 0 1 2; do
    t=$(grep -o '\[raft\] leader p=[0-9]* t=[0-9]*' "$SANDBOX/group$n/graph.log" 2>/dev/null |
      sed 's/.* t=//' | sort -n | tail -1)
    if [ -n "$t" ] && [ "$t" -gt "$best" ]; then best=$t; who=$n; fi
  done
  printf '%s' "$who"
}

# Replica $1's stdin: wait (up to 60 s) until the group's leadership has held
# for 3 s — no replica has logged a newer leadership in that time — then the
# leader's writes through its fronts, the followers' nothing (a write
# reaches the group through its leader's router).
group_input() {
  local i=0 lead= seen= steady=0
  while [ "$i" -lt 120 ]; do
    lead=$(grep -hs '\[raft\] leader' "$SANDBOX"/group*/graph.log | wc -l)
    if [ "$lead" -gt 0 ] && [ "$lead" = "$seen" ]; then
      steady=$((steady + 1))
      [ "$steady" -ge 6 ] && break
    else
      steady=0
    fi
    seen=$lead
    sleep 0.5
    i=$((i + 1))
  done
  if [ "$(leader_now)" != "$1" ]; then
    sleep 100
    return
  fi
  printf 'sql:CREATE TABLE t (id INT PRIMARY KEY, v TEXT)'
  sleep 3
  printf "sql:INSERT INTO t VALUES (1, 'one')"
  sleep 3
  printf 'sql:SELECT v FROM t'
  sleep 2
  printf 'kv:\x03\x03\x00SET\x01\x00k\x01\x00v'
  sleep 2
  printf 'kv:\x02\x03\x00GET\x01\x00k'
  sleep 100
}

group() {
  local n dir pids=() r rev revs=() leader=
  for n in 0 1 2; do
    dir="$SANDBOX/group$n"
    rm -rf -- "$dir"
    mkdir -p "$dir/wal" "$dir/store_fronts.kv"
    sed -e "s#stack: ../../packaging/stack/#stack: $ROOT/packaging/stack/#" \
      examples/store_fronts/group3.yaml >"$dir/graph.yaml"
  done
  for n in 0 1 2; do
    dir="$SANDBOX/group$n"
    (cd "$dir" && group_input "$n" | FLUXOR_PROJECT_ROOT=$ROOT timeout 120 "$FLX" run graph.yaml \
      --param self_id=$n --param listen_port=1949$n \
      --param peer0_port=19490 --param peer1_port=19491 --param peer2_port=19492 \
      >out.txt 2>graph.log) 2>/dev/null &
    pids+=($!)
  done
  # Until the leader's last answer is in and every replica holds an event
  # (bounded), then stop the group.
  local i=0
  until { grep -qx v "$SANDBOX"/group*/out.txt &&
    grep -qs '^BODY ' "$SANDBOX/group0/out.txt" &&
    grep -qs '^BODY ' "$SANDBOX/group1/out.txt" &&
    grep -qs '^BODY ' "$SANDBOX/group2/out.txt"; } 2>/dev/null || [ "$i" -ge 200 ]; do
    sleep 0.5
    i=$((i + 1))
  done
  reap TERM
  for n in "${pids[@]}"; do
    pkill -TERM -P "$n" 2>/dev/null
    kill "$n" 2>/dev/null
  done
  wait "${pids[@]}" 2>/dev/null
  leader=$(leader_now)
  r=$(cat "$SANDBOX/group${leader:-0}/out.txt")
  if [ -n "$leader" ] && want "$r" "$(printf 'one\nOK\nv')"; then
    ok "group (SQL and KV through the leader's fronts, committed on the group)"
  else
    no group "reply='$r'"
  fi
  for n in 0 1 2; do
    rev=$(event_revision "$(cat "$SANDBOX/group$n/out.txt")")
    revs+=("${rev:-none}")
  done
  if [ "${revs[0]}" != none ] && [ "${revs[0]}" != bad ] \
    && [ "${revs[1]}" = "${revs[0]}" ] && [ "${revs[2]}" = "${revs[0]}" ]; then
    ok "group (every replica's subscription receives the INSERT at revision ${revs[0]})"
  else
    no group-cdc "revisions='${revs[*]}'"
  fi
}

# The catalogue case's stdin: each request one chunk, a second apart.
catalogue_input() {
  python3 -I -c '
import hashlib, struct, sys, time
def env(kind, gen, payload):
    h = b"WHR1" + struct.pack("<HHIQ", kind, 0, len(payload), gen) + bytes([7]) * 16 + payload
    return h + hashlib.sha256(h).digest()
def item(kind, route, ident, rec):
    body = bytes([kind, route]) + ident + rec
    return struct.pack("<H", len(body)) + body
def ep(i):
    return bytes([i]) + bytes(14) + bytes([0xEE])
def gen_items(g, routes):
    out = b""
    for r, n in enumerate(routes):
        for i in range(n):
            out += item(2, r, ep(r * 16 + i), env(2, g, bytes([r * 16 + i]) * 16))
        out += item(1, r, bytes(16), env(1, g, bytes([0xBB]) * 24))
    return out
w = sys.stdout.buffer
for chunk in [b"p1:" + gen_items(1, [1, 2]),
              b"bad:" + gen_items(3, [1, 2]),
              b"wd:",
              b"st:" + gen_items(4, [1]),
              b"p5:" + gen_items(5, [1]),
              b"snap:"]:
    time.sleep(2)
    w.write(chunk)
    w.flush()
time.sleep(3)
'
}

catalogue() {
  local dir t r
  dir="$SANDBOX/catalogue"
  rm -rf -- "$dir"
  mkdir -p "$dir/store_fronts.kv"
  sed -e "s#stack: ../../packaging/stack/#stack: $ROOT/packaging/stack/#" \
    examples/store_fronts/catalogue.yaml >"$dir/graph.yaml"
  t="$dir/out.bin"
  (cd "$dir" && catalogue_input | FLUXOR_PROJECT_ROOT=$ROOT timeout 40 "$FLX" run graph.yaml \
    >"$t" 2>"$dir/graph.log") 2>/dev/null
  reap TERM
  r=$(python3 -I - "$t" <<'PY'
import struct, sys
out = open(sys.argv[1], "rb").read()
svc = b"rc/" + b"\xa1" * 16 + b"/" + b"\xb2" * 16 + b"/"
def value(key):
    # The last write of the key: the changes come in revision order.
    at = out.rfind(struct.pack("<H", len(key)) + key)
    if at < 0:
        return None
    at += 2 + len(key)
    n = struct.unpack_from("<I", out, at)[0]
    return out[at + 4:at + 4 + n]
def sealed(g):
    v = value(svc + b"gen/" + struct.pack(">Q", g) + b"/sealed")
    if not v or v[:4] != b"LCS1":
        return None
    return struct.unpack_from("<III", v, 4)
def manifest(g, r):
    v = value(svc + b"gen/" + struct.pack(">Q", g) + b"/r/" + bytes([r]) + b"/manifest")
    return struct.unpack_from("<I", v, 4)[0] if v and v[:4] == b"LCM1" else None
checks = {
    "422": b"\n422 " in b"\n" + out,
    "409": b"\n409 " in b"\n" + out,
    "gen1": sealed(1) == (2, 3, 7) and manifest(1, 0) == 1 and manifest(1, 1) == 2,
    "gen3": sealed(3) is None,
    "withdrawn": sealed(2) == (0, 0, 0),
    "gen5": sealed(5) == (1, 1, 3),
    "active": (value(svc + b"active") or b"\0" * 8)[:8] == struct.pack("<Q", 5),
}
print(" ".join(k for k, v in checks.items() if not v) or "ok")
PY
)
  if [ "$r" = ok ]; then
    ok "catalogue (two routes, a short route refused, the withdrawn form, a stale predecessor, the changes holding it all)"
  else
    no catalogue "failed: $r (output kept in $dir)"
  fi
}

# The catalogue_pairs case's stdin: each request one chunk, two seconds
# apart (each answered well within that).
catalogue_pairs_input() {
  python3 -I -c '
import hashlib, struct, sys, time
def env(kind, gen, payload):
    h = b"WHR1" + struct.pack("<HHIQ", kind, 0, len(payload), gen) + bytes([7]) * 16 + payload
    return h + hashlib.sha256(h).digest()
def item(kind, route, ident, rec):
    body = bytes([kind, route]) + ident + rec
    return struct.pack("<H", len(body)) + body
def ep(i):
    return bytes([i]) + bytes(14) + bytes([0xEE])
def gen_items(g, routes):
    out = b""
    for r, n in enumerate(routes):
        for i in range(n):
            out += item(2, r, ep(r * 16 + i), env(2, g, bytes([r * 16 + i]) * 16))
        out += item(1, r, bytes(16), env(1, g, bytes([0xBB]) * 24))
    return out
w = sys.stdout.buffer
for chunk in [b"p1:" + gen_items(1, [1, 2]), b"dup:" + gen_items(1, [1, 2]), b"ss:", b"ch:"]:
    time.sleep(2)
    w.write(chunk)
    w.flush()
time.sleep(3)
'
}

catalogue_pairs() {
  local pair proto dir t r
  for pp in etcd:1 redis:2 memcached:3 memcached_udp:4; do
    pair=${pp%%:*}
    proto=${pp##*:}
    dir="$SANDBOX/catalogue_pairs_$pair"
    rm -rf -- "$dir"
    mkdir -p "$dir/catalogue.kv"
    sed -e "s#to: router.memcached_in#to: router.${pair}_in#" \
      -e "s#from: router.memcached_out#from: router.${pair}_out#" \
      -e "s#^      proto: 3\$#      proto: $proto#" \
      examples/store_fronts/catalogue_pairs.yaml >"$dir/graph.yaml"
    t="$dir/out.bin"
    (cd "$dir" && catalogue_pairs_input | FLUXOR_PROJECT_ROOT=$ROOT timeout 30 "$FLX" run graph.yaml \
      >"$t" 2>"$dir/graph.log") 2>/dev/null
    reap TERM
    r=$(python3 -I - "$t" <<'PY'
import struct, sys
out = open(sys.argv[1], "rb").read()
svc = b"rc/" + b"\xa1" * 16 + b"/" + b"\xb2" * 16 + b"/"
active = svc + b"active"
def value(key, at_line=False):
    head = struct.pack("<H", len(key)) + key
    at = out.rfind((b"\n" if at_line else b"") + head)
    if at < 0:
        return None
    at += len(head) + (1 if at_line else 0)
    n = struct.unpack_from("<I", out, at)[0]
    return out[at + 4:at + 4 + n]
seal = value(svc + b"gen/" + struct.pack(">Q", 1) + b"/sealed")
checks = {
    "409": b"\n409 " in out,
    # The snapshot page opens with the active pointer (its first key).
    "snapshot": (value(active, at_line=True) or b"\0" * 8)[:8] == struct.pack("<Q", 1),
    "seal": bool(seal) and seal[:4] == b"LCS1" and struct.unpack_from("<III", seal, 4) == (2, 3, 7),
    "active": (value(active) or b"\0" * 8)[:8] == struct.pack("<Q", 1),
}
print(" ".join(k for k, v in checks.items() if not v) or "ok")
PY
)
    if [ "$r" = ok ]; then
      ok "catalogue_pairs $pair (proto $proto: publish, a duplicate refused, a snapshot, the changes)"
    else
      no "catalogue_pairs $pair" "failed: $r (output kept in $dir)"
    fi
  done
}

for t in ${*:-sql kv cdc group catalogue catalogue_pairs}; do "$t"; done
echo "== $pass passed, $fail failed =="
[ "$fail" -eq 0 ]
