# Memcached Compatibility Guide

Lattice provides a Memcached-compatible interface that allows existing Memcached clients to
connect and perform operations using the familiar Memcached protocol. This guide covers
supported commands, behavioral differences, and configuration options.

## Protocol Support

Lattice implements the **ASCII protocol only**. There is no binary protocol support:
no `0x80` request magic, no opcodes, no quiet operations, and no 24-byte binary header.
Configure clients for ASCII/text mode.

## Connecting to Lattice

Connect using any Memcached client library in ASCII/text mode:

```python
# Python (python-memcached - ASCII protocol)
import memcache
mc = memcache.Client(['localhost:11211'])
mc.set('key', 'value')
print(mc.get('key'))
```

```javascript
// Node.js (memcached)
const Memcached = require('memcached');
const mc = new Memcached('localhost:11211');
mc.set('key', 'value', 3600, (err) => {
    mc.get('key', (err, data) => console.log(data));
});
```

## Supported Commands

This is the complete set of commands with a handler, all ASCII. Anything not
listed here (including `cas`, `touch`, `gat`, `gats`, `noop`, `verbosity`) returns
`ERROR`.

### Storage Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `set` | Supported | No | Always overwrites existing key |
| `add` | Supported | No | Store only if key doesn't exist |
| `replace` | Supported | No | Store only if key exists |
| `append` | Supported | No | Append data to existing value |
| `prepend` | Supported | No | Prepend data to existing value |

**ASCII Format:**
```
set <key> <flags> <exptime> <bytes> [noreply]\r\n
<data>\r\n
```

### Retrieval Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `get` | Supported | No | Retrieve one or more keys |
| `gets` | Supported | No | Get with CAS token (derived from `mod_revision`) |

**ASCII Format:**
```
get <key>*\r\n
gets <key>*\r\n
```

### Deletion Command

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `delete` | Supported | No | Delete a key |

**ASCII Format:**
```
delete <key> [noreply]\r\n
```

### Arithmetic Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `incr` | Supported | Yes | Increment; wraps at 64-bit boundary |
| `decr` | Supported | Yes | Decrement; floors at 0 |

**ASCII Format:**
```
incr <key> <value> [noreply]\r\n
decr <key> <value> [noreply]\r\n
```

### Other Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `stats` | Supported | No | Returns server statistics; tenant-scoped |
| `flush_all` | Supported | No | Tenant-scoped, not global |
| `version` | Supported | No | Returns Lattice version |
| `quit` | Supported | No | Close connection |

## Not implemented

The following have no handler and return `ERROR`:

- **`cas`** — the check-and-set write is not wired. `gets` returns a usable CAS
  token (see "CAS" below), but no `cas` command consumes it.
- **`touch`** — no expiration-update command (there is no TTL-command surface).
- **`gat` / `gats`** — get-and-touch variants.
- **Binary protocol** — no `noop`, no quiet operations, no binary opcodes or header.
- **SASL authentication** — not supported.
- **Slab management** (`slabs reassign`/`automove`) — Lattice uses a different
  memory model.
- **LRU crawler** (`lru_crawler …`) — expiry is deterministic TTL, not LRU
  eviction.

## ASCII Protocol Responses

| Response | Description |
|----------|-------------|
| `STORED` | Value stored successfully |
| `NOT_STORED` | add/replace condition failed |
| `NOT_FOUND` | Key does not exist |
| `DELETED` | Key deleted successfully |
| `OK` | Generic success |
| `ERROR` | Unknown error |
| `CLIENT_ERROR <msg>` | Client sent invalid request |
| `SERVER_ERROR <msg>` | Internal server error |

## Expiration Time Handling

Memcached expiration times are interpreted as follows:

| Value | Interpretation |
|-------|----------------|
| 0 | Never expires |
| 1 to 2,592,000 (30 days) | Relative seconds from now |
| > 2,592,000 | Absolute Unix timestamp |

```python
# Never expire
mc.set('key1', 'value', 0)

# Expire in 1 hour (relative)
mc.set('key2', 'value', 3600)

# Expire at specific time (absolute timestamp)
import time
mc.set('key3', 'value', int(time.time()) + 86400)  # Tomorrow
```

## Flags Handling

Memcached flags are 32-bit client-defined values stored with each key:

- Stored alongside the value
- Returned on retrieval
- Not interpreted by the server

Clients typically use flags to store serialization format:

```python
# pylibmc example - flags indicate serialization
mc.set('json_data', {'key': 'value'})  # Client sets flags automatically
```

## CAS (Check-and-Set)

The token returned by `gets` is derived from the `mod_revision` of the underlying
KvRecord:

```
gets counter\r\n
VALUE counter 0 2 <cas_token>\r\n
42\r\n
END\r\n
```

The `cas` write that consumes the token is not implemented — issuing a `cas`
command returns `ERROR`, so the token is informational only.

## Behavioral Differences

### Multi-tenancy

Lattice is multi-tenant by design. Each connection is associated with a tenant, and keys are
automatically namespaced:

```
# Client sees:        session:abc123
# Lattice stores as:  tenant-456/session:abc123
```

### flush_all Scope

In Lattice, `flush_all` only affects the current tenant's keys, not the entire cluster.

### No LRU Eviction

Lattice does not perform LRU eviction. Keys are only removed via:
- Explicit `delete` command
- TTL expiration

This means `stats` output won't include eviction-related metrics.

### Linearizability

Operations marked as "Linearizable: Yes" require LIN-BOUND (consensus) and may fail if
linearizability is temporarily unavailable:

```
# Always succeeds (eventually consistent)
get mykey

# May fail if linearizability / capacity is unavailable
incr counter 1
```

## Error Mapping

The ASCII adapter has **no** dedicated routing-epoch or linearizability error
string. Those conditions surface as the generic responses below; the only
`SERVER_ERROR` strings the memcached path emits are `backpressure`,
`command too large`, `flush failed`, `not yet supported`, and `overflow`.

| Lattice condition | ASCII response emitted today |
|---|---|
| Routing epoch mismatch / store failed | `NOT_STORED` |
| Linearizability unavailable / flow-control | `SERVER_ERROR backpressure` |
| Value/command too large | `SERVER_ERROR command too large` |
| Unsupported command (e.g. `cas`) | `SERVER_ERROR not yet supported` / `ERROR` |

Dedicated `SERVER_ERROR routing epoch changed` /
`SERVER_ERROR linearizability unavailable` strings are described in the
specification's [error mapping](specification.md#error-mapping) section as
intended design, not shipped behavior.

## Configuration

There is no `config/lattice.toml`. A node is configured by the fluxor graph YAML under
`configs/*.yaml` (for example `configs/bare-metal-pi5-multiproto.yaml`). The Memcached
edge is the `memcached_stream_anchor` module wired into the graph; its listener bind
address and router wiring are set as module parameters in that YAML. Pick the config that
matches the target host and enable the Memcached anchor there.

## Client Library Compatibility

Clients must be configured for the ASCII/text protocol (binary is not supported).

| Language | Library | Status | Notes |
|----------|---------|--------|-------|
| Python | python-memcached | Compatible | ASCII protocol tested |
| Python | pymemcache | Compatible | Use default text/ASCII mode |
| Node.js | memcached | Compatible | Basic operations tested |
| PHP | memcached | Compatible | Set binary protocol OFF |
| Ruby | dalli | Compatible | Use `protocol: :meta`/ASCII, not binary |
| Java | spymemcached | Compatible | ASCII connection factory |
| C/C++ | libmemcached | Compatible | ASCII mode |

## Performance Considerations

1. **Multi-get**: Use multi-key `get` to retrieve multiple keys in a single round trip.

2. **Value size**: Keep values small. Large values increase network and storage overhead.
   Consider chunking or using a separate blob store for large objects.

## Telemetry

The Memcached anchor emits three module-scope counters on its `metrics` output port
(manifest `[observability] metrics`, coarse step cadence):

- `cmd_get` — count of `get`/`gets` commands served
- `cmd_set` — count of storage commands served
- `total_connections` — connections accepted

The same counters are also reflected in the `stats` command's `STAT` lines. Per-key hit/miss
and byte throughput metrics are not exported.
