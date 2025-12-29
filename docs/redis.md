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

### String Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `GET` | Supported | No | Snapshot read by default |
| `SET` | Supported | Conditional | NX/XX options require LIN-BOUND |
| `SETNX` | Supported | Yes | Alias for SET NX |
| `SETEX` | Supported | No | |
| `PSETEX` | Supported | No | |
| `MGET` | Supported | No | All keys must be in same KPG |
| `MSET` | Supported | No | All keys must be in same KPG |
| `GETEX` | Supported | Yes | |
| `GETDEL` | Supported | Yes | |
| `APPEND` | Supported | Yes | |
| `STRLEN` | Supported | No | |
| `INCR` | Supported | Yes | |
| `DECR` | Supported | Yes | |
| `INCRBY` | Supported | Yes | |
| `DECRBY` | Supported | Yes | |
| `INCRBYFLOAT` | Supported | Yes | |
| `GETRANGE` | Supported | No | |
| `SETRANGE` | Supported | Yes | |

### Key Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `DEL` | Supported | No | All keys must be in same KPG |
| `UNLINK` | Supported | No | Synchronous in Lattice (same as DEL) |
| `EXISTS` | Supported | No | |
| `TYPE` | Supported | No | Always returns 'string' in v0.1 |
| `KEYS` | Supported | No | Full keyspace scan - use with caution |
| `SCAN` | Supported | No | |
| `EXPIRE` | Supported | No | |
| `PEXPIRE` | Supported | No | |
| `EXPIREAT` | Supported | No | |
| `PEXPIREAT` | Supported | No | |
| `TTL` | Supported | No | |
| `PTTL` | Supported | No | |
| `PERSIST` | Supported | No | |
| `EXPIRETIME` | Supported | No | |
| `PEXPIRETIME` | Supported | No | |
| `RENAME` | Supported | Yes | Both keys must be in same KPG |
| `RENAMENX` | Supported | Yes | Both keys must be in same KPG |
| `COPY` | Supported | No | Both keys must be in same KPG; DB option ignored |
| `TOUCH` | Supported | No | No-op in Lattice (no LRU tracking) |
| `RANDOMKEY` | Supported | No | |
| `DBSIZE` | Supported | No | Returns key count for tenant |
| `OBJECT` | Partial | No | ENCODING and REFCOUNT only |

### Connection Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `PING` | Supported | No | |
| `ECHO` | Supported | No | |
| `QUIT` | Supported | No | |
| `SELECT` | Supported | No | Accepted but ignored; Lattice uses tenants |
| `AUTH` | Supported | No | Maps to tenant authentication |
| `HELLO` | Supported | No | RESP3 protocol negotiation |
| `RESET` | Supported | No | |
| `CLIENT` | Partial | No | SETNAME, GETNAME, ID, LIST, INFO, PAUSE, UNPAUSE, REPLY |

### Server Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `INFO` | Supported | No | Includes Lattice-specific sections |
| `TIME` | Supported | No | Uses committed tick time |
| `DEBUG` | Partial | No | SLEEP only; disabled in production |
| `CONFIG` | Partial | No | Read-only subset for GET |
| `MEMORY` | Partial | No | USAGE and STATS only |
| `SLOWLOG` | Supported | No | |

### Transaction Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `MULTI` | Supported | No | |
| `EXEC` | Supported | Conditional | LIN-BOUND required with WATCH |
| `DISCARD` | Supported | No | |
| `WATCH` | Supported | Yes | Requires LIN-BOUND for CAS semantics |
| `UNWATCH` | Supported | No | |

### Pub/Sub Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `SUBSCRIBE` | Supported | No | Messages not persisted |
| `PSUBSCRIBE` | Supported | No | |
| `UNSUBSCRIBE` | Supported | No | |
| `PUNSUBSCRIBE` | Supported | No | |
| `PUBLISH` | Supported | No | Fire-and-forget; no persistence |
| `PUBSUB` | Supported | No | CHANNELS, NUMSUB, NUMPAT |

### Cluster Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `CLUSTER` | Partial | No | Compatibility mode: SLOTS, NODES, INFO, KEYSLOT |
| `READONLY` | Supported | No | Enables snapshot reads on replicas |
| `READWRITE` | Supported | No | Requires leader |

## Unsupported Commands

### Data Structures (Not in v0.1 scope)

Hash, List, Set, Sorted Set, and Stream commands are not supported in the initial release:

- Hash: `HGET`, `HSET`, `HDEL`, `HGETALL`, etc.
- List: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, etc.
- Set: `SADD`, `SREM`, `SMEMBERS`, etc.
- Sorted Set: `ZADD`, `ZREM`, `ZRANGE`, etc.
- Stream: `XADD`, `XREAD`, `XRANGE`, etc.

### Scripting (Security concerns)

Lua scripting is not supported due to security and determinism requirements:

- `EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION`

### Cluster Management (Managed by Lattice)

Cluster topology is managed by the Lattice control plane:

- `CLUSTER ADDSLOTS`, `CLUSTER DELSLOTS`, `CLUSTER FAILOVER`, `CLUSTER MEET`, `CLUSTER REPLICATE`, `CLUSTER RESET`

### Replication (Managed by Lattice)

Replication is handled internally by Lattice:

- `REPLICAOF`, `SLAVEOF`, `PSYNC`, `SYNC`

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
WATCH mykey
```

When linearizability is unavailable, you'll receive:
```
CLUSTERDOWN The cluster is down - linearizability unavailable
```

Use `READONLY` mode to accept eventual consistency.

### SELECT Command

The `SELECT` command is accepted for compatibility but has no effect. Lattice uses tenants
instead of numbered databases.

### UNLINK vs DEL

In standard Redis, `UNLINK` is asynchronous while `DEL` is synchronous. In Lattice, both
commands behave identically (synchronous deletion).

### TOUCH Command

The `TOUCH` command is a no-op in Lattice since there's no LRU eviction. Keys are only
removed via explicit deletion or TTL expiration.

## Error Mapping

| Lattice Error | Redis Error | Description |
|---------------|-------------|-------------|
| Routing epoch mismatch | `MOVED 0 host:port` | Redirect to correct node |
| Linearizability unavailable | `CLUSTERDOWN` | Retry or use READONLY mode |
| Cross-KPG operation | `CROSSSLOT` | Ensure all keys in same KPG |
| Type error | `WRONGTYPE` | Operation on wrong data type |

## Configuration

Configure the Redis listener in `config/lattice.toml`:

```toml
[listeners.redis]
bind = "0.0.0.0:6379"
enabled = true
insecure = true  # Set false for production

# TLS (production)
# tls_chain_path = "certs/redis-server.crt"
# tls_key_path = "certs/redis-server.key"

# Protocol and connection settings
protocol_version = "auto"
max_pipeline_depth = 256
idle_timeout_ms = 0

# Pub/Sub settings
pubsub_enabled = true
max_subscriptions = 1000
```

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

4. **Pub/Sub**: Messages are not persisted. Use the etcd Watch API if you need durable
   event streaming.
