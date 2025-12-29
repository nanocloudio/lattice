# Memcached Compatibility Guide

Lattice provides a Memcached-compatible interface that allows existing Memcached clients to
connect and perform operations using the familiar Memcached protocol. This guide covers
supported commands, behavioral differences, and configuration options.

## Protocol Support

Lattice supports both Memcached protocol variants:

- **ASCII Protocol**: Text-based protocol, human-readable, widely supported
- **Binary Protocol**: More efficient, supports additional features like quiet operations

Protocol is auto-detected from the first byte:
- `0x80`: Binary protocol request magic
- Any ASCII letter: ASCII protocol command

## Connecting to Lattice

Connect using any Memcached client library:

```python
# Python (pylibmc - binary protocol)
import pylibmc
mc = pylibmc.Client(['localhost:11211'], binary=True)
mc.set('key', 'value')
print(mc.get('key'))
```

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

```ruby
# Ruby (dalli - binary protocol)
require 'dalli'
mc = Dalli::Client.new('localhost:11211')
mc.set('key', 'value')
puts mc.get('key')
```

## Supported Commands

### Storage Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `set` | Supported | No | Always overwrites existing key |
| `add` | Supported | No | Store only if key doesn't exist |
| `replace` | Supported | No | Store only if key exists |
| `append` | Supported | No | Append data to existing value |
| `prepend` | Supported | No | Prepend data to existing value |
| `cas` | Supported | Yes | Check-and-set; requires LIN-BOUND |

**ASCII Format:**
```
set <key> <flags> <exptime> <bytes> [noreply]\r\n
<data>\r\n
```

**Binary Opcodes:**
- `set`: 0x01
- `add`: 0x02
- `replace`: 0x03
- `append`: 0x0e
- `prepend`: 0x0f

### Retrieval Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `get` | Supported | No | Retrieve one or more keys |
| `gets` | Supported | No | Get with CAS token |
| `gat` | Supported | No | Get and touch (update expiration) |
| `gats` | Supported | No | Get and touch with CAS token |

**ASCII Format:**
```
get <key>*\r\n
gets <key>*\r\n
gat <exptime> <key>*\r\n
gats <exptime> <key>*\r\n
```

**Binary Opcodes:**
- `get`: 0x00
- `getq` (quiet): 0x09
- `getk` (with key): 0x0c
- `getkq` (quiet with key): 0x0d
- `gat`: 0x1d
- `gatq`: 0x1e

### Deletion Command

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `delete` | Supported | No | Delete a key |

**ASCII Format:**
```
delete <key> [noreply]\r\n
```

**Binary Opcode:** 0x04

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

**Binary Opcodes:**
- `incr`: 0x05
- `decr`: 0x06
- `incrq` (quiet): 0x15
- `decrq` (quiet): 0x16

### Touch Command

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `touch` | Supported | No | Update key expiration without retrieving |

**ASCII Format:**
```
touch <key> <exptime> [noreply]\r\n
```

**Binary Opcode:** 0x1c

### Other Commands

| Command | Status | Linearizable | Notes |
|---------|--------|--------------|-------|
| `stats` | Supported | No | Returns server statistics; tenant-scoped |
| `flush_all` | Supported | No | Tenant-scoped, not global |
| `version` | Supported | No | Returns Lattice version |
| `quit` | Supported | No | Close connection |
| `noop` | Supported | No | Binary only; flush quiet operations |
| `verbosity` | Supported | No | Accepted but no-op in Lattice |

## Binary Protocol Details

### Request Header (24 bytes)

```
     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| Magic         | Opcode        | Key length                    |
       +---------------+---------------+---------------+---------------+
      4| Extras length | Data type     | Reserved                      |
       +---------------+---------------+---------------+---------------+
      8| Total body length                                             |
       +---------------+---------------+---------------+---------------+
     12| Opaque                                                        |
       +---------------+---------------+---------------+---------------+
     16| CAS                                                           |
       |                                                               |
       +---------------+---------------+---------------+---------------+
```

### Binary Status Codes

| Code | Name | Description |
|------|------|-------------|
| 0x0000 | NoError | Success |
| 0x0001 | KeyNotFound | Key does not exist |
| 0x0002 | KeyExists | Key exists (add failed) |
| 0x0003 | ValueTooLarge | Value exceeds size limit |
| 0x0004 | InvalidArguments | Bad command arguments |
| 0x0005 | ItemNotStored | replace condition failed |
| 0x0006 | DeltaBadval | incr/decr on non-numeric value |
| 0x0008 | AuthenticationError | SASL auth failed |
| 0x0009 | AuthenticationContinue | SASL auth continue |
| 0x0081 | UnknownCommand | Command not recognized |
| 0x0082 | OutOfMemory | Server out of memory |
| 0x0085 | Throttled | Rate limit exceeded (Lattice-specific) |
| 0x0086 | Unavailable | Service unavailable (Lattice-specific) |

## ASCII Protocol Responses

| Response | Description |
|----------|-------------|
| `STORED` | Value stored successfully |
| `NOT_STORED` | add/replace condition failed |
| `EXISTS` | CAS conflict |
| `NOT_FOUND` | Key does not exist |
| `DELETED` | Key deleted successfully |
| `TOUCHED` | Key expiration updated |
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

CAS tokens enable optimistic locking:

```python
# Get value with CAS token
result = mc.gets('counter')
value, cas_token = result

# Update only if unchanged
new_value = int(value) + 1
success = mc.cas('counter', str(new_value), cas_token)

if not success:
    # Another client modified the value - retry
    pass
```

In Lattice, CAS tokens are derived from the `mod_revision` of the underlying KvRecord.

## Unsupported Commands

### Slab Management

Lattice uses a different memory model than Memcached:

- `slabs reassign`
- `slabs automove`

### LRU Crawler

Lattice uses deterministic TTL expiration, not LRU eviction:

- `lru_crawler enable`
- `lru_crawler disable`
- `lru_crawler metadump`

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

# May return SERVER_ERROR if linearizability unavailable
incr counter 1
cas mykey 0 3600 5 12345
```

Error response:
```
SERVER_ERROR linearizability unavailable
```

## Error Mapping

| Lattice Error | ASCII Response | Binary Status |
|---------------|----------------|---------------|
| Routing epoch mismatch | `SERVER_ERROR routing epoch changed` | 0x0086 |
| Linearizability unavailable | `SERVER_ERROR linearizability unavailable` | 0x0086 |
| Rate limit exceeded | `SERVER_ERROR rate limit exceeded` | 0x0085 |

## Configuration

Configure the Memcached listener in `config/lattice.toml`:

```toml
[listeners.memcached]
bind = "0.0.0.0:11211"
enabled = true
insecure = true  # Set false for production

# TLS (production)
# tls_chain_path = "certs/memcached-server.crt"
# tls_key_path = "certs/memcached-server.key"

# Protocol settings
protocol_mode = "auto"  # "auto", "ascii", or "binary"
max_value_size = 1048576  # 1MB default

# Connection settings
idle_timeout_ms = 0  # No timeout

# SASL authentication
sasl_enabled = false
# sasl_mechanisms = ["PLAIN"]
```

## Client Library Compatibility

| Language | Library | Status | Notes |
|----------|---------|--------|-------|
| Python | pylibmc | Compatible | ASCII and binary protocols tested |
| Python | python-memcached | Compatible | ASCII protocol tested |
| Node.js | memcached | Compatible | Basic operations tested |
| PHP | memcached | Compatible | Binary protocol recommended |
| Ruby | dalli | Compatible | Binary protocol tested |
| Java | spymemcached | Compatible | Binary protocol recommended |
| C/C++ | libmemcached | Compatible | Both protocols tested |

## Performance Considerations

1. **Binary protocol**: Use binary protocol for better performance with supported clients.
   Binary protocol has lower parsing overhead and supports quiet operations.

2. **Quiet operations**: Binary protocol quiet variants (getq, setq, etc.) suppress
   responses, allowing more efficient batching.

3. **Multi-get**: Use multi-key `get` to retrieve multiple keys in a single round trip.

4. **CAS operations**: CAS requires linearizability (LIN-BOUND). For high-throughput
   scenarios where eventual consistency is acceptable, use regular `set`.

5. **Value size**: Keep values small. Large values increase network and storage overhead.
   Consider chunking or using a separate blob store for large objects.

## Telemetry

Lattice exposes Memcached adapter metrics via Prometheus:

```
# Request counts
lattice_adapter_memcached_requests_total{command="get",tenant_id="...",status="success"}
lattice_adapter_memcached_requests_total{command="set",tenant_id="...",status="success"}

# Error counts
lattice_adapter_memcached_errors_total{command="...",error_type="...",tenant_id="..."}

# Latency histograms
lattice_adapter_memcached_request_duration_seconds{command="get",tenant_id="..."}

# Cache hit/miss rates
lattice_adapter_memcached_get_hits_total{tenant_id="..."}
lattice_adapter_memcached_get_misses_total{tenant_id="..."}

# Connection count
lattice_adapter_memcached_connections_total{protocol="binary"}

# Throughput
lattice_adapter_memcached_bytes_read_total{tenant_id="..."}
lattice_adapter_memcached_bytes_written_total{tenant_id="..."}

# CAS conflicts
lattice_adapter_memcached_cas_badval_total{tenant_id="..."}
```
