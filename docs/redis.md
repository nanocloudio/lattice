# Redis Compatibility Guide

Lattice provides a Redis-compatible interface that allows existing Redis clients to connect
and perform operations using the familiar Redis protocol. This guide covers supported commands,
behavioral differences, and configuration options.

## Protocol Support

Lattice supports both RESP2 and RESP3 protocols:

- **RESP2**: Default protocol, compatible with all Redis clients
- **RESP3**: Negotiated via `HELLO` command, provides richer type information

Protocol negotiation happens automatically when clients send the `HELLO` command.

## Connecting to Lattice

Connect using any Redis client library:

```python
# Python (redis-py)
import redis
r = redis.Redis(host='localhost', port=6379)
r.set('key', 'value')
print(r.get('key'))
```

```javascript
// Node.js (ioredis)
const Redis = require('ioredis');
const redis = new Redis(6379, 'localhost');
await redis.set('key', 'value');
console.log(await redis.get('key'));
```

```rust
// Rust (redis-rs)
let client = redis::Client::open("redis://127.0.0.1/")?;
let mut con = client.get_connection()?;
con.set("key", "value")?;
let value: String = con.get("key")?;
```

## Supported Commands

This is the complete set of commands with a handler. Anything not listed here
returns `-ERR unknown command`. See "Not implemented" below for families that
are commonly expected but absent.

### String Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `GET` | Supported | No | Snapshot read by default |
| `SET` | Supported | Conditional | Parses `NX`/`XX`, `EX`/`PX`, and the `GET` option |
| `SETNX` | Supported | Yes | Store only if key does not exist |
| `GETSET` | Supported | Yes | Set new value, return old |
| `APPEND` | Supported | Yes | |
| `STRLEN` | Supported | No | |
| `INCR` | Supported | Yes | |
| `INCRBY` | Supported | Yes | |
| `DECR` | Supported | Yes | |
| `DECRBY` | Supported | Yes | |
| `MGET` | Supported | No | All keys must be in same KPG |
| `MSET` | Supported | No | All keys must be in same KPG |

### Key Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `DEL` | Supported | No | All keys must be in same KPG |
| `UNLINK` | Supported | No | Synchronous in Lattice (same as DEL) |
| `EXISTS` | Supported | No | |
| `KEYS` | Supported | No | Full keyspace scan - use with caution |
| `SCAN` | Supported | No | |
| `FLUSHDB` | Supported | No | Tenant-scoped, not global |
| `FLUSHALL` | Supported | No | Tenant-scoped, not global (same as FLUSHDB) |

### Time-Series Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `TS.ADD` | Supported | Yes | Append a sample; bucket 0, disk-ordered |
| `TS.RANGE` | Supported | No | Range scan over samples |

### Connection Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `PING` | Supported | No | |
| `ECHO` | Supported | No | |
| `QUIT` | Supported | No | Closes the connection |
| `SELECT` | Supported | No | Accepted but ignored; Lattice uses tenants |
| `AUTH` | Supported | No | Single-password `requirepass` only (no ACL users) |
| `HELLO` | Supported | No | RESP2/RESP3 protocol negotiation |
| `RESET` | Supported | No | |
| `CLIENT` | Supported | No | Accepted; connection-management no-op |
| `COMMAND` | Supported | No | Accepted; minimal reply |

### Transaction Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `MULTI` | Supported | No | Queues subsequent commands |
| `EXEC` | Supported | No | Runs the queued commands |
| `DISCARD` | Supported | No | |

`WATCH`/`UNWATCH` are not implemented — there is no optimistic-lock CAS layer on
top of MULTI/EXEC. See "Not implemented" below.

### Pub/Sub Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `SUBSCRIBE` | Supported | No | Acknowledged; no message delivery (no PUBLISH side) |
| `PSUBSCRIBE` | Supported | No | Acknowledged only |
| `UNSUBSCRIBE` | Supported | No | |
| `PUNSUBSCRIBE` | Supported | No | |

Subscription commands are accepted and echo the standard subscribe/unsubscribe
confirmation frames, but there is no `PUBLISH` command, so no messages are ever
delivered. Pub/sub is a stub; use the etcd Watch API for real event streaming.

## Not implemented

The following are commonly expected but have no handler and return
`-ERR unknown command`.

### Data structures

Only the string type exists. Hash, List, Set, Sorted Set, and Stream commands are
absent: `HGET`/`HSET`/`HDEL`/`HGETALL`/…, `LPUSH`/`RPUSH`/`LPOP`/`LRANGE`/…,
`SADD`/`SREM`/`SMEMBERS`/…, `ZADD`/`ZRANGE`/…, `XADD`/`XREAD`/`XRANGE`/….

### TTL / expiration

No TTL command family: `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`,
`PERSIST`, `EXPIRETIME`, `PEXPIRETIME`, `GETEX`, `GETDEL`, `SETEX`, `PSETEX`. The
`EX`/`PX` options are parsed by `SET` but there is no expiry-command surface.

### Optimistic locking / scripting

`WATCH`, `UNWATCH`, `EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION`.

### Other key/server commands

`TYPE`, `RENAME`, `RENAMENX`, `COPY`, `TOUCH`, `RANDOMKEY`, `DBSIZE`, `OBJECT`,
`GETRANGE`, `SETRANGE`, `INCRBYFLOAT`, `INFO`, `TIME`, `CONFIG`, `DEBUG`, `MEMORY`,
`SLOWLOG`.

### Pub/sub publish and introspection

`PUBLISH`, `PUBSUB`.

### Cluster / replication (managed by Lattice)

`CLUSTER` (all subcommands), `READONLY`, `READWRITE`, `REPLICAOF`, `SLAVEOF`,
`PSYNC`, `SYNC`. Topology and replication are managed by the Lattice control plane.

## Behavioral Differences

### Multi-tenancy

Lattice is multi-tenant by design. Each connection is associated with a tenant, and keys are
automatically namespaced:

```
# Client sees:        server/config
# Lattice stores as:  tenant-123/server/config
```

Authentication via `AUTH` maps to tenant credentials.

### Key Placement Groups (KPGs)

Keys are distributed across KPGs based on their hash. Multi-key operations require all keys
to be in the same KPG:

```redis
# Works - single key
SET mykey value

# Works - keys hash to same KPG (use hash tags)
MSET {user}:name "Alice" {user}:email "alice@example.com"

# Error - keys may be in different KPGs
MSET user:1:name "Alice" user:2:name "Bob"
```

Use hash tags `{...}` to ensure related keys are in the same KPG.

### Linearizability

Not all operations are linearizable by default. Operations marked as "Linearizable: Yes"
require LIN-BOUND (consensus) and may fail if linearizability is temporarily unavailable:

```redis
# Always succeeds (eventually consistent)
GET mykey

# May return CLUSTERDOWN if linearizability unavailable
INCR counter
```

When linearizability is unavailable, you'll receive the exact string emitted by the code:
```
-CLUSTERDOWN no authority available for this operation
```

### SELECT Command

The `SELECT` command is accepted for compatibility but has no effect. Lattice uses tenants
instead of numbered databases.

### UNLINK vs DEL

In standard Redis, `UNLINK` is asynchronous while `DEL` is synchronous. In Lattice, both
commands behave identically (synchronous deletion).

## Error Mapping

| Lattice Error | Redis Error class | Exact wire string |
|---------------|-------------------|-------------------|
| Routing epoch advanced | `MOVED` | `-MOVED routing epoch advanced` |
| Linearizability unavailable | `CLUSTERDOWN` | `-CLUSTERDOWN no authority available for this operation` |
| Cross-KPG operation | `CROSSSLOT` | Ensure all keys in same KPG |
| Type error | `WRONGTYPE` | `-WRONGTYPE Operation against a key holding the wrong kind of value` |

## Configuration

There is no `config/lattice.toml`. A node is configured by the fluxor graph YAML under
`configs/*.yaml` (for example `configs/bare-metal-pi5-multiproto.yaml`). The Redis edge
is the `redis_edge_anchor` module wired into the graph; its listener bind address, the
`requirepass` password used by `AUTH`, and the router wiring are set as module parameters
in that YAML. Pick the config that matches the target host and enable the Redis anchor
there.

## Client Library Compatibility

| Language | Library | Status | Notes |
|----------|---------|--------|-------|
| Python | redis-py | Compatible | Basic string operations tested |
| Node.js | ioredis | Compatible | Basic operations tested |
| Rust | redis-rs | Compatible | Basic operations tested |
| Go | go-redis | Compatible | Basic operations tested |
| Java | Jedis | Compatible | Basic operations tested |

## Performance Considerations

1. **Pipeline depth**: Lattice supports pipelining up to the configured `max_pipeline_depth`.
   Deeper pipelines may improve throughput but increase latency.

2. **Linearizable operations**: Operations requiring LIN-BOUND (consensus) have higher latency
   than snapshot reads. Batch writes when possible.

3. **Multi-key operations**: Ensure keys are co-located in the same KPG using hash tags to
   enable efficient multi-key operations.

4. **Pub/Sub**: Not functional (SUBSCRIBE is acknowledged but there is no PUBLISH,
   so no messages are delivered). Use the etcd Watch API for event streaming.
