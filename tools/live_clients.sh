#!/usr/bin/env bash
# Live protocol tests for Lattice's database clients — proves each client works
# STANDALONE (no chronicle) against a REAL server. Each case starts the backend,
# runs the client graph, and asserts the observed effect.
#
#   ./tools/live_clients.sh            # all
#   ./tools/live_clients.sh redis pg   # a subset
set -u
cd "$(dirname "$0")/.."
FLX=${FLUXOR_BIN:-fluxor}
pass=0
fail=0
ok() { echo "  PASS  $1"; pass=$((pass + 1)); }
no() { echo "  FAIL  $1: $2"; fail=$((fail + 1)); }

# Run a graph with optional stdin; strip fluxor's compile preamble so only the
# module's own output is returned.
# NOTE: the payload is the printf FORMAT string, not a %b argument — bash's
# `%b` does not interpret \xHH, which the binary RESP frames need.
# LEAK GUARD: `fluxor run` spawns fluxor-linux as a CHILD; a timeout that
# kills only the parent orphans the child, which runs its scheduler loop
# forever holding a ~100 MB arena. So: background the parent, wait bounded,
# then reap the CHILD FIRST (pkill -P only works while the parent lives) —
# and sweep any survivor from THIS repo's target dir on exit.
trap 'pkill -KILL -f "$PWD/target/.*fluxor-linux" 2>/dev/null' EXIT
run_graph() {
  local t pid i out
  t=$(mktemp)
  printf "${2-}" | "$FLX" run "$1" >"$t" 2>/dev/null &
  pid=$!
  i=$(( ${3:-8} * 2 ))
  while kill -0 "$pid" 2>/dev/null && [ "$i" -gt 0 ]; do sleep 0.5; i=$((i - 1)); done
  pkill -TERM -P "$pid" 2>/dev/null # the fluxor-linux child, first
  kill "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  out=$(cat "$t")
  rm -f "$t"
  printf '%s' "${out##*config.bin$'\n'}"
}
want() { case "$1" in *"$2"*) return 0 ;; *) return 1 ;; esac }

redis() {
  docker rm -f lat-redis >/dev/null 2>&1
  docker run -d --rm --name lat-redis -p 16391:6379 redis:7-alpine >/dev/null 2>&1
  timeout 30 bash -c 'until docker exec lat-redis redis-cli PING 2>/dev/null | grep -q PONG; do sleep 1; done'
  local r v
  r=$(run_graph examples/redis_client/linux.yaml '\x03\x03\x00SET\x08\x00greeting\x02\x00hi')
  v=$(docker exec lat-redis redis-cli GET greeting 2>/dev/null)
  if want "$r" OK && [ "$v" = hi ]; then
    ok "redis  (RESP SET -> OK, value stored)"
  else
    no redis "reply='$r' stored='$v'"
  fi
  docker rm -f lat-redis >/dev/null 2>&1
}

pg() {
  docker rm -f lat-pg >/dev/null 2>&1
  docker run -d --rm --name lat-pg -e POSTGRES_PASSWORD=s3cret -e POSTGRES_USER=app \
    -e POSTGRES_DB=orders -p 15432:5432 postgres:16-alpine >/dev/null 2>&1
  timeout 60 bash -c 'until docker exec lat-pg pg_isready -U app -d orders >/dev/null 2>&1; do sleep 1; done'
  local r
  r=$(run_graph examples/pg_client/linux.yaml 'SELECT 42')
  if want "$r" 42; then
    ok "pg     (SCRAM-SHA-256 auth, SELECT 42 -> 42)"
  else
    no pg "reply='$r'"
  fi
  docker rm -f lat-pg >/dev/null 2>&1
}

mysql() {
  # STOCK mysql:8.0 defaults to caching_sha2_password. Its FAST path needs the
  # server's credential cache warm for this origin, so do one real login first;
  # a cold cache correctly reports that full auth (TLS) would be required.
  docker rm -f lat-mysql >/dev/null 2>&1
  docker run -d --rm --name lat-mysql -e MYSQL_ROOT_PASSWORD=s3cret -e MYSQL_DATABASE=app \
    -p 13306:3306 mysql:8.0 >/dev/null 2>&1
  timeout 120 bash -c 'until docker exec lat-mysql mysqladmin ping -uroot -ps3cret 2>/dev/null | grep -q alive; do sleep 2; done'
  docker run --rm --network host mysql:8.0 \
    mysql -h 127.0.0.1 -P 13306 -uroot -ps3cret -e "SELECT 1" >/dev/null 2>&1
  local r
  r=$(run_graph examples/mysql_client/linux.yaml)
  if want "$r" "authenticated (caching_sha2)"; then
    ok "mysql  (stock 8.0 caching_sha2 fast path)"
  else
    no mysql "status='$r'"
  fi
  docker rm -f lat-mysql >/dev/null 2>&1
}

mongo() {
  docker rm -f lat-mongo >/dev/null 2>&1
  docker run -d --rm --name lat-mongo -e MONGO_INITDB_ROOT_USERNAME=root \
    -e MONGO_INITDB_ROOT_PASSWORD=s3cret -p 17017:27017 mongo:7 >/dev/null 2>&1
  timeout 60 bash -c 'until docker exec lat-mongo mongosh --quiet --eval "db.runCommand({ping:1}).ok" 2>/dev/null | grep -q 1; do sleep 2; done'
  docker exec lat-mongo mongosh admin -u root -p s3cret --quiet \
    --eval 'db.orders.deleteMany({})' >/dev/null 2>&1
  local r v
  r=$(run_graph examples/mongo_client/linux.yaml 'doc-1')
  v=$(docker exec lat-mongo mongosh admin -u root -p s3cret --quiet \
    --eval 'db.orders.find({},{_id:0}).toArray()' 2>/dev/null)
  if want "$r" authenticated && want "$v" doc-1; then
    ok "mongo  (SCRAM-SHA-256 auth + BSON insert)"
  else
    no mongo "status='$r' docs='$v'"
  fi
  docker rm -f lat-mongo >/dev/null 2>&1
}

cassandra() {
  docker rm -f lat-cass >/dev/null 2>&1
  docker run -d --rm --name lat-cass -p 19042:9042 cassandra:4.1 >/dev/null 2>&1
  timeout 180 bash -c 'until docker exec lat-cass cqlsh -e "describe keyspaces" >/dev/null 2>&1; do sleep 3; done'
  local r
  r=$(run_graph examples/cassandra_client/linux.yaml)
  if want "$r" "session ready"; then
    ok "cassandra (CQL STARTUP -> session ready)"
  else
    no cassandra "status='$r'"
  fi
  docker rm -f lat-cass >/dev/null 2>&1
}

for t in ${*:-redis pg mysql mongo cassandra}; do "$t"; done
echo "== $pass passed, $fail failed =="
[ "$fail" -eq 0 ]
