#!/usr/bin/env bash
# etcd v3 differential: one etcdctl command matrix against lattice and against
# the reference server (/usr/bin/etcd), outputs compared line for line.
#
#   tools/e2e/etcd_diff.sh            every case; exit 1 on any difference
#   tools/e2e/etcd_diff.sh -k <re>    only cases whose name matches <re>
#
# What legitimately differs is normalised before comparing: lease IDs, the
# cluster/member IDs and raft term in headers, revision numbers (both servers
# number from their own start, so only their ORDER is compared: each distinct
# revision becomes r1, r2, … in order of appearance), a lease's remaining
# TTL (a clock reading), the reference's shifted ports, and what one member
# reports of itself: versions, sizes, raft counters, timings, hash values
# (each store digests its own layout), and peer URLs (lattice has no etcd
# peer port: its replication is the graph's).
#
# Coverage: every etcdctl v3 command that asks the server something —
# put, get, del, txn, compaction, watch, lease (grant, revoke, timetolive,
# list, keep-alive), member (list, add/update/remove/promote refusals),
# endpoint (health, status, hashkv), alarm (list, disarm), defrag,
# move-leader, snapshot save, auth, user, role, lock, elect, make-mirror.
# Not compared, and why: `check perf` / `check datascale` measure speed
# and memory, not behaviour; `snapshot status` / `snapshot restore` read a
# bbolt file offline (lattice's snapshot is its own format, see
# docs/guides/etcd.md); `version` and `completion` never reach a server;
# etcdctl 3.5 has no `downgrade`.
#
# Needs: `fluxor` on PATH, lattice's modules built (fluxor modules build
# --all), `target/aarch64-unknown-linux-gnu/release/fluxor-linux`, etcdctl
# and etcd 3.5 (Debian's etcd-server / etcd-client).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LAT_PORT=2379
REF_PORT=23790
FILTER=""
[ "${1:-}" = "-k" ] && FILTER="${2:-}"

D="$(mktemp -d /tmp/etcd-diff-XXXXXX)"
LAT_PID=""
REF_PID=""
cleanup() {
  # Each server is its own process group (setsid): the group goes, children
  # (fluxor-linux under `fluxor run`) included.
  [ -n "$LAT_PID" ] && kill -- -"$LAT_PID" 2>/dev/null
  [ -n "$REF_PID" ] && kill -- -"$REF_PID" 2>/dev/null
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT

for tool in fluxor etcd etcdctl; do
  command -v "$tool" >/dev/null || { echo "FAIL: $tool not on PATH"; exit 1; }
done
wait_port() { for _ in $(seq 1 100); do (echo >"/dev/tcp/127.0.0.1/$1") 2>/dev/null && return 0; sleep 0.1; done; return 1; }

# Both ports must be free: a server already listening would be tested in
# place of the one this run starts.
for port in "$LAT_PORT" "$REF_PORT"; do
  if (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "FAIL: port $port is already in use"; exit 1
  fi
done

# lattice: single-node etcd on the disk provider (MVCC history, as the
# reference keeps it), from an empty store.
cd "$ROOT"
rm -rf "$ROOT/kv" && mkdir -p "$ROOT/kv"
FLUXOR_LINUX_BIN="$ROOT/target/aarch64-unknown-linux-gnu/release/fluxor-linux" \
  setsid fluxor run configs/single-etcd-watch-disk.yaml >"$D/lattice.log" 2>&1 &
LAT_PID=$!
# The reference.
setsid etcd --name default --data-dir "$D/ref" \
  --listen-client-urls "http://127.0.0.1:$REF_PORT" --advertise-client-urls "http://127.0.0.1:$REF_PORT" \
  --listen-peer-urls "http://127.0.0.1:$((REF_PORT + 10))" --initial-advertise-peer-urls "http://127.0.0.1:$((REF_PORT + 10))" \
  --initial-cluster "default=http://127.0.0.1:$((REF_PORT + 10))" >"$D/ref.log" 2>&1 &
REF_PID=$!
wait_port "$LAT_PORT" || { echo "FAIL: lattice never bound $LAT_PORT"; tail -20 "$D/lattice.log"; exit 1; }
wait_port "$REF_PORT" || { echo "FAIL: etcd never bound $REF_PORT"; tail -20 "$D/ref.log"; exit 1; }

# Normalise what legitimately differs (see the header).
norm() {
  python3 -c '
import re, sys
t = sys.stdin.read()
t = re.sub(r"(?m)^\{\"level\":.*\n", "", t)                 # client retry logging
t = re.sub(r"\b[0-9a-f]{16}\b", "ID", t)                       # lease/member/cluster ids (hex)
t = re.sub(r"\"(cluster_id|member_id|raft_term|raft_index|raft_applied_index|dbSize|dbSizeInUse|leader|ID)\":\s*\d+", r"\"\1\":N", t)
seen = {}
def rv(m):
    v = m.group(2)
    seen.setdefault(v, "r%d" % (len(seen) + 1))
    return m.group(1) + seen[v]
t = re.sub(r"(\"(?:revision|mod_revision|create_revision|compact_revision)\":\s*)(\d+)", rv, t)
t = re.sub(r"\b\d+\.\d+\.\d+\b", "VER", t)                     # server versions
t = re.sub(r"remaining\(\d+s\)", "remaining(Ns)", t)             # a lease clock
t = re.sub(r":23790\b", ":2379", t)                                # the reference ports
t = re.sub(r":23800\b", ":2380", t)
t = re.sub(r"took = [0-9.]+[a-zµ]*s", "took = T", t)
t = re.sub(r"\b[0-9.]+ [kMG]?B\b", "SIZE", t)
t = re.sub(r"(true|false), (true|false), \d+, \d+, \d+,", r"\1, \2, N, N, N,", t)
t = re.sub(r"(:2379), \d+$", r"\1, HASH", t, flags=re.M)
t = re.sub(r"(started, [^,]*), [^,]*, ", r"\1, PEERS, ", t)
t = re.sub(r"\"TTL\":\s*\d+", "\"TTL\":N", t)
t = re.sub(r"\"lease\":\s*\d+", "\"lease\":N", t)
sys.stdout.write(t)'
}

# One case: `case <name> <etcdctl args…>`. Each server starts empty and sees the
# same sequence, so state-dependent outputs line up.
PASS=0
FAILS=()
run_one() { # run_one <port> <args…>
  local port="$1"; shift
  # An RPC a server does not answer fails fast rather than waiting out a
  # long timeout: both servers answer a local request in milliseconds.
  # A case's stdin (`case_in`) is given as printf %b text; others read none.
  printf '%b' "${STDIN:-}" |
    timeout 6 etcdctl --endpoints="http://127.0.0.1:$port" --dial-timeout=2s --command-timeout=2s "$@" 2>&1
  echo "[exit ${PIPESTATUS[1]}]"
}
case_() {
  local name="$1"; shift
  if [ -n "$FILTER" ] && ! [[ "$name" =~ $FILTER ]]; then
    run_one "$LAT_PORT" "$@" >/dev/null; run_one "$REF_PORT" "$@" >/dev/null
    return
  fi
  local a b
  a="$(run_one "$LAT_PORT" "$@" | norm)"
  b="$(run_one "$REF_PORT" "$@" | norm)"
  if [ "$a" = "$b" ]; then
    PASS=$((PASS + 1)); echo "   ok    $name"
  else
    FAILS+=("$name"); echo "   DIFF  $name"
    diff <(echo "$b") <(echo "$a") | sed 's/^/          /' | head -12
  fi
}

# `case_in <name> <stdin as printf %b text> <etcdctl args…>`
case_in() { local name="$1" input="$2"; shift 2; STDIN="$input" case_ "$name" "$@"; }

# `case_fn <name> <fn>`: a sequence that needs a server's own answers (a
# lease ID it granted). `fn <port>` runs the sequence against one server.
case_fn() {
  local name="$1" fn="$2"
  if [ -n "$FILTER" ] && ! [[ "$name" =~ $FILTER ]]; then
    "$fn" "$LAT_PORT" >/dev/null; "$fn" "$REF_PORT" >/dev/null
    return
  fi
  local a b
  a="$("$fn" "$LAT_PORT" | norm)"
  b="$("$fn" "$REF_PORT" | norm)"
  if [ "$a" = "$b" ]; then
    PASS=$((PASS + 1)); echo "   ok    $name"
  else
    FAILS+=("$name"); echo "   DIFF  $name"
    diff <(echo "$b") <(echo "$a") | sed 's/^/          /' | head -12
  fi
}
grant() { # grant <port> <ttl> → the granted ID (hex)
  etcdctl --endpoints="http://127.0.0.1:$1" --dial-timeout=2s lease grant "$2" 2>/dev/null | awk '{print $2}'
}
# A key held by a lease: listed by TimeToLive, gone with the revoke,
# and a revoked lease reads as gone everywhere after.
lease_keys() {
  local p="$1" id
  id="$(grant "$p" 600)"
  run_one "$p" put lk1 v --lease="$id"
  run_one "$p" put lk2 v --lease="$id"
  run_one "$p" lease timetolive "$id" --keys
  run_one "$p" lease revoke "$id"
  run_one "$p" get --prefix lk
  run_one "$p" lease timetolive "$id"
  run_one "$p" lease revoke "$id"
  run_one "$p" put lk3 v --lease="$id"
  run_one "$p" lease keep-alive --once "$id"
}
# A watch's events carry each key's metadata and, asked, the version
# they replaced (a put over a put, a delete of it, a lease's key).
watch_events() {
  local p="$1" out id
  out="$(mktemp -p "$D")"
  timeout 3 etcdctl --endpoints="http://127.0.0.1:$p" watch --prefix w/ --prev-kv -w json >"$out" 2>&1 &
  local wp=$!
  sleep 0.7
  id="$(grant "$p" 600)"
  etcdctl --endpoints="http://127.0.0.1:$p" put w/a 1 >/dev/null
  etcdctl --endpoints="http://127.0.0.1:$p" put w/a 2 --lease="$id" >/dev/null
  etcdctl --endpoints="http://127.0.0.1:$p" del w/a >/dev/null
  wait "$wp"
  python3 -c '
import json, sys
for line in open(sys.argv[1]):
    line = line.strip()
    if not line.startswith("{"): continue
    for e in json.loads(line).get("Events") or []:
        print(json.dumps(e, sort_keys=True))
' "$out"
}

# A snapshot streams whole: the file lands, and its size is what was sent.
snapshot_save() {
  local p="$1" f
  f="$D/snap-$p.db"
  run_one "$p" snapshot save "$f" | grep -v '^{"level"' | sed "s|$f|FILE|"
  [ -s "$f" ] && echo "[file written]"
}

# Auth end to end: users and roles set up, auth on, each user's access
# granted and refused by their roles' permissions, then undone.
auth_flow() {
  local p="$1"
  local r=(--user=root:rootpw) a=(--user=alice:alicepw)
  run_one "$p" user add root --new-user-password=rootpw
  run_one "$p" role add root
  run_one "$p" user grant-role root root
  run_one "$p" role add reader
  run_one "$p" role grant-permission reader read foo
  run_one "$p" role grant-permission reader --prefix=true readwrite pre/
  run_one "$p" user add alice --new-user-password=alicepw
  run_one "$p" user grant-role alice reader
  run_one "$p" auth enable
  run_one "$p" auth status
  run_one "$p" put foo 1 "${r[@]}"
  run_one "$p" get foo "${a[@]}"
  run_one "$p" put foo 2 "${a[@]}"
  run_one "$p" put pre/x 1 "${a[@]}"
  run_one "$p" get --prefix pre/ "${a[@]}"
  run_one "$p" get foo
  run_one "$p" get foo --user=alice:wrong
  run_one "$p" user list "${r[@]}"
  run_one "$p" role list "${r[@]}"
  run_one "$p" role get reader "${r[@]}"
  run_one "$p" user get alice "${r[@]}"
  run_one "$p" role list "${a[@]}"
  run_one "$p" user add root --new-user-password=x "${r[@]}"
  # What a user may do about themself, and what only root may.
  run_one "$p" auth status "${a[@]}"
  run_one "$p" user get alice "${a[@]}"
  run_one "$p" user get root "${a[@]}"
  run_one "$p" user passwd alice --new-user-password=alicepw2 "${a[@]}"
  run_one "$p" get foo --user=alice:alicepw2
  run_one "$p" user passwd alice --new-user-password=alicepw "${r[@]}"
  # Leases under auth: a key the user cannot write holds them back.
  lid="$(etcdctl --endpoints="http://127.0.0.1:$p" "${r[@]}" lease grant 600 2>/dev/null | awk '{print $2}')"
  run_one "$p" put foo 3 --lease="$lid" "${r[@]}"
  run_one "$p" lease timetolive "$lid" --keys "${a[@]}" | sed "s/${lid:-NOLEASE}/LEASE/"
  lid2="$(etcdctl --endpoints="http://127.0.0.1:$p" "${r[@]}" lease grant 600 2>/dev/null | awk '{print $2}')"
  run_one "$p" put zz 1 --lease="$lid2" "${r[@]}"
  run_one "$p" lease timetolive "$lid2" --keys "${a[@]}" | sed "s/${lid2:-NOLEASE}/LEASE/"
  run_one "$p" lease timetolive "$lid2" "${a[@]}" | sed "s/${lid2:-NOLEASE}/LEASE/"
  run_one "$p" lease revoke "$lid2" "${r[@]}" | sed "s/${lid2:-NOLEASE}/LEASE/"
  run_one "$p" lease revoke "$lid" "${a[@]}" | sed "s/${lid:-NOLEASE}/LEASE/"
  run_one "$p" lease keep-alive --once "$lid" "${a[@]}" | sed "s/${lid:-NOLEASE}/LEASE/"
  run_one "$p" lease revoke "$lid" "${r[@]}" | sed "s/${lid:-NOLEASE}/LEASE/"
  run_one "$p" role revoke-permission reader foo "${r[@]}"
  run_one "$p" get foo "${a[@]}"
  run_one "$p" user revoke-role alice reader "${r[@]}"
  run_one "$p" role delete reader "${r[@]}"
  run_one "$p" user delete alice "${r[@]}"
  run_one "$p" auth disable "${r[@]}"
  run_one "$p" auth status
  run_one "$p" user revoke-role root root
  run_one "$p" user delete root
  run_one "$p" role delete root
  run_one "$p" user list
}

# etcdctl's lock and election recipes (client-side, over KV, Lease,
# Watch and Txn): a lock is taken and the command runs under it; a
# campaign is won and its proclamation observed.
lock_run() {
  local p="$1"
  run_one "$p" lock mylock echo held
}
elect_run() {
  local p="$1"
  timeout 3 etcdctl --endpoints="http://127.0.0.1:$p" elect myelection proposal-a 2>&1 |
    grep -v '^{"level"' | sed -E 's/[0-9a-f]{16}/ID/g'
  run_one "$p" elect -l myelection | head -3 &
  sleep 1
  kill %1 2>/dev/null
  run_one "$p" get --prefix myelection/ --keys-only | sed -E 's/[0-9a-f]{16}/ID/g'
}

# make-mirror: a prefix copied, then followed, onto another prefix of
# the same server (sync from a snapshot read, then a watch).
mirror_run() {
  local p="$1" ep="http://127.0.0.1:$1"
  run_one "$p" put mir/1 a
  run_one "$p" put mir/2 b
  timeout 3 etcdctl --endpoints="$ep" make-mirror --prefix=mir/ --dest-prefix=cpy/ "$ep" >/dev/null 2>&1 &
  local mirror=$!
  sleep 1.5
  run_one "$p" put mir/3 c
  wait "$mirror"
  run_one "$p" get --prefix cpy/
}

# A lease left to expire takes its key with it.
lease_expiry() {
  local p="$1" id
  id="$(grant "$p" 2)"
  run_one "$p" put le v --lease="$id"
  run_one "$p" get le
  sleep 4
  run_one "$p" get le
  run_one "$p" lease timetolive "$id"
}

echo "== KV"
case_ put-a            put a 1
case_ put-b            put b 2
case_ put-c            put c 3
case_ get              get a
case_ get-missing      get zz
case_ get-range        get a c
case_ get-prefix       get --prefix ""
case_ get-from-key     get --from-key b
case_ get-keys-only    get --prefix --keys-only ""
case_ get-count-only   get --prefix --count-only ""
case_ get-limit        get --prefix --limit=2 ""
case_ get-sort-desc    get --prefix --sort-by=KEY --order=DESCEND ""
case_ get-json         -w json get a
case_ put-prev-kv      put a 11 --prev-kv
case_ get-rev          -w json get a --rev=2
case_ del-prev-kv      del b --prev-kv
case_ del-range        del a c
case_ del-prefix       del --prefix ""
echo "== Txn"
case_ txn-put          put t 1
case_in txn-succeed    'value("t") = "1"\n\nput t 2\n\nput t 3\n\n' txn
case_ txn-read         get t
case_in txn-fail       'mod("t") = "1"\n\nput t 9\n\nget t\n\n' txn
echo "== Lease"
case_ lease-grant      lease grant 600
case_ lease-list       lease list
case_ lease-put-none   put x v --lease=1234
case_fn lease-keys     lease_keys
case_fn lease-expiry   lease_expiry
case_ lease-list-after lease list
echo "== Watch"
case_fn watch-prev-kv  watch_events
echo "== Concurrency recipes"
case_fn make-mirror   mirror_run
case_fn lock          lock_run
case_fn elect         elect_run
echo "== Compaction"
case_ compact          compaction 3
case_ get-compacted    get t --rev=2
case_ get-at-compact   get t --rev=3
case_ compact-again    compaction 3
case_ compact-future   compaction 999999
case_ compact-physical compaction --physical 4
echo "== Snapshot"
case_fn snapshot-save  snapshot_save
echo "== Cluster, maintenance, auth"
case_ member-list      member list
case_ member-remove-x  member remove 1234
case_ member-update-x  member update 1234 --peer-urls=http://127.0.0.1:1
case_ member-promote-x member promote 1234
case_ move-leader-x    move-leader 1234
case_ alarm-disarm     alarm disarm
case_ hashkv-compacted endpoint hashkv --rev=2
case_ endpoint-health  endpoint health
case_ endpoint-status  endpoint status
case_ endpoint-hashkv  endpoint hashkv
case_ alarm-list       alarm list
case_ defrag           defrag
case_ auth-status      auth status
case_ user-list        user list
case_ role-list        role list
case_fn auth-flow      auth_flow

echo "== $PASS matched, ${#FAILS[@]} differ${FAILS:+: ${FAILS[*]}}"
[ "${#FAILS[@]}" -eq 0 ]
