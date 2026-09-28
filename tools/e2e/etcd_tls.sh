#!/usr/bin/env bash
# etcd over TLS: configs/single-minimal-etcd-tls.yaml driven by etcdctl at
# https://127.0.0.1:2379, trusting the dev CA. A client that does not trust
# the CA is refused at the handshake; one that does reads and writes, and
# etcd's auth works over the same channel.
#
#   tools/e2e/etcd_tls.sh
#
# Needs: `fluxor` on PATH, lattice's modules built, fluxor-linux, etcdctl,
# openssl.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
tools/dev-certs.sh >/dev/null
D="$(mktemp -d /tmp/etcd-tls-XXXXXX)"
PID=""
cleanup() {
  [ -n "$PID" ] && kill -- -"$PID" 2>/dev/null
  rm -rf "$D"
}
trap cleanup EXIT

FLUXOR_LINUX_BIN="$ROOT/target/aarch64-unknown-linux-gnu/release/fluxor-linux" \
  setsid fluxor run configs/single-minimal-etcd-tls.yaml >"$D/run.log" 2>&1 &
PID=$!
for _ in $(seq 1 100); do (echo >/dev/tcp/127.0.0.1/2379) 2>/dev/null && break; sleep 0.1; done

E=(etcdctl --endpoints=https://127.0.0.1:2379 --dial-timeout=3s --command-timeout=5s)
FAIL=0
check() { # check <name> <expected substring> <command…>
  local name="$1" want="$2"; shift 2
  local got
  got="$("$@" 2>&1 | grep -v '^{"level"')"
  if [[ "$got" == *"$want"* ]]; then
    echo "   ok    $name"
  else
    echo "   FAIL  $name: wanted '$want', got: $got"
    FAIL=1
  fi
}

# A client that does not trust the CA gets no answer (etcdctl retries the
# failed handshake until its deadline).
check untrusted-refused "Error:"     "${E[@]}" get k
check put              "OK"          "${E[@]}" --cacert=certs/dev/ca.pem put k v
check get              "v"           "${E[@]}" --cacert=certs/dev/ca.pem get k --print-value-only
check user-add         "User root created" "${E[@]}" --cacert=certs/dev/ca.pem user add root --new-user-password=pw
check role-add         "Role root created" "${E[@]}" --cacert=certs/dev/ca.pem role add root
check grant            "granted"     "${E[@]}" --cacert=certs/dev/ca.pem user grant-role root root
check auth-enable      "Enabled"     "${E[@]}" --cacert=certs/dev/ca.pem auth enable
check auth-anon        "user name is empty" "${E[@]}" --cacert=certs/dev/ca.pem get k
check auth-root        "v"           "${E[@]}" --cacert=certs/dev/ca.pem --user=root:pw get k --print-value-only

if [ "$FAIL" -ne 0 ]; then
  tail -20 "$D/run.log"
  exit 1
fi
echo "== etcd over TLS: all checks passed"
