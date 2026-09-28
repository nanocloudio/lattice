#!/usr/bin/env bash
# The full single-node composition (configs/single.yaml) end to end: etcd
# and Redis over TLS, memcached in the clear, all on one consensus group
# with a WAL. A write committed before a restart is read after it, and a
# write past what one consensus entry carries is refused in each
# protocol's own words.
#
#   tools/e2e/single_full.sh
#
# Needs: `fluxor` on PATH, lattice's modules built, fluxor-linux,
# etcdctl, redis-cli (with TLS), nc, openssl.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
tools/dev-certs.sh >/dev/null
D="$(mktemp -d /tmp/single-full-XXXXXX)"
PID=""
stop() { [ -n "$PID" ] && kill -- -"$PID" 2>/dev/null; wait "$PID" 2>/dev/null; PID=""; }
trap 'stop; rm -rf "$D"' EXIT
start() {
  FLUXOR_LINUX_BIN="$ROOT/target/aarch64-unknown-linux-gnu/release/fluxor-linux" \
    setsid fluxor run configs/single.yaml >>"$D/run.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 150); do (echo >/dev/tcp/127.0.0.1/2379) 2>/dev/null && break; sleep 0.1; done
  sleep 3 # the consensus group elects itself and replays its WAL
}

E=(etcdctl --endpoints=https://127.0.0.1:2379 --cacert=certs/dev/ca.pem --dial-timeout=3s --command-timeout=10s)
R=(redis-cli --tls --cacert certs/dev/ca.pem -p 6379)
FAIL=0
check() { # check <name> <expected substring> <command…>
  local name="$1" want="$2"; shift 2
  local got
  got="$("$@" 2>&1 | grep -v '^{"level"')"
  if [[ "$got" == *"$want"* ]]; then
    echo "   ok    $name"
  else
    echo "   FAIL  $name: wanted '$want', got: ${got:0:200}"
    FAIL=1
  fi
}
check_empty() { # check_empty <name> <command…>: the command prints nothing
  local name="$1"; shift
  local got
  got="$("$@" 2>&1 | grep -v '^{"level"')"
  if [ -z "$got" ]; then
    echo "   ok    $name"
  else
    echo "   FAIL  $name: wanted nothing, got: ${got:0:200}"
    FAIL=1
  fi
}
# One memcached exchange: a replicated write answers after it commits.
mc() {
  python3 - "$1" <<'PY'
import socket, sys
s = socket.create_connection(("127.0.0.1", 11211), timeout=5)
s.sendall(sys.argv[1].encode().decode("unicode_escape").encode("latin-1"))
out = b""
try:
    while not out.endswith((b"\r\n",)) or out.startswith(b"VALUE") and not out.endswith(b"END\r\n"):
        chunk = s.recv(65536)
        if not chunk:
            break
        out += chunk
except socket.timeout:
    pass
print(out.decode(errors="replace"))
PY
}

lease_id() { "${E[@]}" lease grant "$1" 2>/dev/null | awk '{print $2}'; }

# The WAL lives under the working directory; start from an empty one.
# The three protocols share one flat keyspace, so each writes its own key.
rm -rf wal
start
check etcd-put        "OK"   "${E[@]}" put "e$$" v1
check etcd-get        "v1"   "${E[@]}" get "e$$" --print-value-only
check redis-set       "OK"   "${R[@]}" SET "r$$" r1
check redis-get       "r1"   "${R[@]}" GET "r$$"
check memcached-set   "STORED" mc "set m$$ 0 0 2\r\nm1\r\n"
check shared-keyspace "r1"   "${E[@]}" get "r$$" --print-value-only
BIG="$(head -c 3000 /dev/zero | tr '\0' x)"
check etcd-too-large  "request is too large" "${E[@]}" put big "$BIG"
check redis-too-large "too large"            "${R[@]}" SET big "$BIG"
check memcached-too-large "SERVER_ERROR object too large" mc "set big 0 0 3000\r\n$BIG\r\n"
# Leases: one long-lived, one that lapses soon after the restart.
LONG="$(lease_id 600)"
SHORT="$(lease_id 4)"
check lease-put-long  "OK" "${E[@]}" put "l$$" v --lease="$LONG"
check lease-put-short "OK" "${E[@]}" put "s$$" v --lease="$SHORT"
stop

start
check etcd-after-restart  "v1" "${E[@]}" get "e$$" --print-value-only
check redis-after-restart "r1" "${R[@]}" GET "r$$"
check memcached-after-restart "m1" mc "get m$$\r\n"
# A lease survives the restart with its key, and still ends on time.
check lease-after-restart "l$$"  "${E[@]}" lease timetolive "$LONG" --keys
check lease-listed        "$LONG" "${E[@]}" lease list
sleep 6
check_empty lapsed-lease-key-gone "${E[@]}" get "s$$" --print-value-only
check lapsed-lease-gone   "already expired" "${E[@]}" lease timetolive "$SHORT"
check lease-revoke        "revoked" "${E[@]}" lease revoke "$LONG"
check_empty revoked-key-gone "${E[@]}" get "l$$" --print-value-only
stop
# And stays revoked across another restart. Auth is set up and turned
# on here, to be found on after the next restart.
start
check revoke-durable "already expired" "${E[@]}" lease timetolive "$LONG"
check user-add   "User root created" "${E[@]}" user add root --new-user-password=pw
check role-add   "Role root created" "${E[@]}" role add root
check grant-root "granted"           "${E[@]}" user grant-role root root
check auth-on    "Enabled"           "${E[@]}" auth enable
stop

start
check auth-durable-anon "user name is empty" "${E[@]}" get "e$$"
check auth-durable-root "v1"  "${E[@]}" --user=root:pw get "e$$" --print-value-only
check auth-off          "Disabled" "${E[@]}" --user=root:pw auth disable
stop

if [ "$FAIL" -ne 0 ]; then
  grep -E 'ERROR|panic' "$D/run.log" | tail -10
  exit 1
fi
echo "== single.yaml: all checks passed"
