# Memcached surface

The `memcached_stream_anchor` module serves the Memcached **ASCII
protocol** over TCP; `memcached_datagram_anchor` serves a single-key
subset over UDP. There is no binary protocol: no `0x80` request
magic, no opcodes, no quiet operations. Configure clients for
ASCII/text mode. Sources:
`modules/app/memcached_stream_anchor/mod.rs`,
`modules/app/memcached_datagram_anchor/mod.rs`,
`modules/common/memcached_codec.rs`.

## Command set (TCP)

This is the complete set with a handler; anything else — including
`cas`, `touch`, `gat`, `gats`, `noop`, `verbosity` — answers
`ERROR`. The `noreply` suffix is not supported: every command
answers.

| Group | Commands | Replies |
|---|---|---|
| Storage | `set`, `add`, `replace`, `append`, `prepend` | `STORED` / `NOT_STORED` |
| Retrieval | `get`, `gets` (multi-key) | `VALUE …` lines + `END` |
| Deletion | `delete` | `DELETED` / `NOT_FOUND` |
| Arithmetic | `incr`, `decr` | the new value / `NOT_FOUND` / `CLIENT_ERROR …` |
| Admin | `stats`, `flush_all`, `version`, `quit` | `STAT …` + `END`, `OK`, `VERSION 0.2.0`, close |

## Semantics and differences from stock memcached

- **Expiry** is the `exptime` field on storage commands,
  interpreted as **relative seconds only** (0 = never). The stock
  30-day threshold, beyond which stock memcached reads the value as
  an absolute Unix timestamp, is not implemented: a large value is
  simply a long relative expiry. Expiry is applied on `set`, `add`,
  and `replace`; `append`/`prepend` leave the existing deadline
  untouched.
- **Flags** are parsed but not stored: `get` always renders the
  flags field as `0`. Clients that encode a serialisation format in
  flags (most language bindings do, for non-string values) will
  misdecode; store plain bytes.
- **CAS**: `gets` returns a real token — the key's `mod_revision`,
  a monotone unsigned 64-bit value — but no `cas` write consumes
  it. The token is informational only; `cas` answers `ERROR`.
- **Arithmetic** is signed 64-bit, Redis-style: a missing key is
  treated as 0 and created, `decr` can go negative, and overflow or
  a non-numeric value answers
  `CLIENT_ERROR cannot increment or decrement non-numeric value`.
  Stock memcached instead answers `NOT_FOUND` for a missing key,
  wraps unsigned, and floors `decr` at 0.
- **`flush_all`** flushes the node's whole keyspace, including keys
  written through the other protocol anchors in the same graph.
- **No LRU eviction**: keys are removed only by `delete` or expiry,
  so `stats` carries no eviction fields.

## `stats`

`stats` answers exactly: `version` (`1.6.0-lattice`),
`pointer_size`, `curr_connections`, `total_connections`, `cmd_get`,
`cmd_set`, then `END`. The counters are also emitted on the
anchor's telemetry port (`cmd_get`, `cmd_set`,
`total_connections`).

## UDP

`memcached_datagram_anchor` implements the standard 8-byte UDP
frame header and serves single-key `get`, `set`, and `delete`; any
other command gets the generic unknown-command reply, and multi-key
`get` is TCP-only. UDP exists in a deployment only when the
datagram anchor module is in the graph.

## Errors

The ASCII surface has no dedicated routing-epoch or linearizability
strings; those conditions surface as generic responses:

| Condition | Reply |
|---|---|
| Routing epoch mismatch / store refused | `NOT_STORED` |
| No authority / flow control | `SERVER_ERROR backpressure` |
| Command or value over the buffer budget | `SERVER_ERROR command too large` |
| Flush refused | `SERVER_ERROR flush failed` |
| Malformed line | `CLIENT_ERROR bad command line` |
| Unknown command | `ERROR` |

## Configuration

The anchor is enabled by listing `memcached_stream_anchor` (and
optionally `memcached_datagram_anchor`) in the graph config with a
`listen_port`, wired to a `kv_request_router` ingress pair, exactly
as the [run guide](running.md)'s graphs wire the Redis anchor.
