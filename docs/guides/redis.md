# Redis surface

The `redis_edge_anchor` module serves a Redis-compatible RESP2
interface: any Redis client library connects and works within the
command set below. Sources: `modules/app/redis_edge_anchor/mod.rs`
(session, connection state) and `modules/common/redis_codec.rs`
(commands and replies).

## Protocol

The parser and every reply are RESP2. `HELLO` answers the handshake
(including `HELLO 3`) but neither direction switches framing: there
is no RESP3 push or typed reply support. Pipelining is supported and
per-connection response order is preserved.

`AUTH` is a single server password: the anchor's `requirepass` param
(empty means no auth required), compared in constant time. There are
no ACL users. `SELECT` is accepted and ignored — there are no
numbered databases.

## Command set

This is the complete set with a handler; anything else answers
`-ERR unknown command`. "Fenced" marks commands whose write goes
through Raft commit on a replicated graph; reads are served from
local applied state unless the graph sets `lin_reads: 1` (see the
[specification](../architecture/specification.md#read-and-write-semantics)).

| Group | Commands |
|---|---|
| Strings | `GET`, `SET` (`NX`/`XX`, `EX`/`PX`, `KEEPTTL`, `GET` option), `SETNX`, `GETSET`, `APPEND`, `STRLEN`, `MGET`, `MSET` |
| Counters | `INCR`, `INCRBY`, `DECR`, `DECRBY` |
| Keyspace | `DEL`/`UNLINK`, `EXISTS`, `KEYS`, `SCAN` (`MATCH`, `COUNT`), `FLUSHDB`/`FLUSHALL` |
| Time-series | `TS.ADD`, `TS.RANGE` |
| Transactions | `MULTI`, `EXEC`, `DISCARD` |
| Pub/sub session | `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE` |
| Connection | `PING`, `ECHO`, `QUIT`, `SELECT`, `AUTH`, `HELLO`, `RESET`, `CLIENT` (`ID`, `GETNAME`, `SETNAME`, `REPLY`), `COMMAND` |

Notes:

- `UNLINK` behaves exactly like `DEL` (synchronous).
- `FLUSHDB` and `FLUSHALL` are identical and flush the node's whole
  keyspace — including keys written through the other protocol
  anchors in the same graph.
- `MULTI`/`EXEC` queues commands and runs them back to back; it is
  not atomic across the queue and there is no `WATCH` CAS layer.
- `SUBSCRIBE` and friends answer the standard confirmation frames,
  but `PUBLISH` does not exist, so no message is ever delivered.
  Pub/sub is session state only.
- Multi-key commands (`MGET`, `MSET`, multi-key `DEL`) and `KEYS`,
  `SCAN`, `FLUSHDB` are accepted only when one range owns the whole
  keyspace; on a split keyspace they are refused (today as a generic
  `-ERR internal error`). There is no hash-tag co-location scheme.

## Not implemented

No handler, answers `-ERR unknown command`:

- **Data structures** — hashes, lists, sets, sorted sets, streams:
  only the string type (plus `TS.*`) exists.
- **TTL commands** — `EXPIRE`/`PEXPIRE`/`TTL`/`PTTL`/`PERSIST`/
  `GETEX`/`SETEX`/…: `SET`'s `EX`/`PX`/`KEEPTTL` options are parsed,
  but there is no expiry-command surface.
- **Optimistic locking and scripting** — `WATCH`, `UNWATCH`,
  `EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION`.
- **Server introspection** — `INFO`, `TIME`, `CONFIG`, `DBSIZE`,
  `TYPE`, `OBJECT`, `MEMORY`, `SLOWLOG`, `DEBUG`.
- **Pub/sub delivery** — `PUBLISH`, `PUBSUB`.
- **Cluster and replication commands** — `CLUSTER`, `READONLY`,
  `REPLICAOF`, `PSYNC`, …: topology is the graph's, not the
  client's.

## Errors

Exact wire strings, from `modules/common/redis_codec.rs`:

| Condition | Reply |
|---|---|
| Routing epoch advanced | `-MOVED routing epoch advanced` |
| No authority for a fenced operation | `-CLUSTERDOWN no authority available for this operation` |
| Wrong value type | `-WRONGTYPE Operation against a key holding the wrong kind of value` |
| Quota refused | `-BUSY tenant quota exceeded` |
| Unknown command | `-ERR unknown command` |

`-MOVED` and `-CLUSTERDOWN` reuse the closest standard client-retry
signals so an unmodified client re-resolves and retries; they are
not Redis Cluster redirection and carry no slot or address. Standard
clients treat `CLUSTERDOWN` as retryable; retry with backoff, since
the usual cause is a leader election in progress or a lost quorum.

## Configuration

The anchor is enabled by listing `redis_edge_anchor` in the graph
config with its `listen_port` (and optionally `requirepass`) params,
wired to a `kv_request_router` ingress pair. The
[run guide](running.md)'s graphs show the full wiring, validated
end to end with `redis-cli`.
