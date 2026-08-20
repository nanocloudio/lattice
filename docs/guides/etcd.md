# etcd surface

The `etcd_edge_anchor` module serves a subset of the etcd v3 gRPC
API over HTTP/2. Sources: `modules/app/etcd_edge_anchor/mod.rs`
(HTTP/2 connection and stream handling, dispatch) and
`modules/common/etcd_codec.rs` (protobuf and gRPC framing).

The listener is **plaintext HTTP/2**: the anchor has no TLS of its
own, and no shipped graph wires the `tls` module in front of it.
Configure clients for an insecure endpoint; TLS-terminated
deployments compose the fluxor `tls` module in the graph (see
[deployment.md](deployment.md#transport-security)).

## Supported RPCs

The complete set with a handler (the anchor's `GrpcMethod`
dispatch). Anything else answers gRPC status 12 (UNIMPLEMENTED).

| Service | RPC | Notes |
|---|---|---|
| KV | Range | Single keys and ranges; reads are snapshot-consistency (see below) |
| KV | Put | Quorum-durable before the response |
| KV | DeleteRange | Quorum-durable before the response |
| Watch | Watch | Server-streaming; create and cancel |
| Lease | LeaseGrant | TTL is the client's request value |
| Lease | LeaseRevoke | Revokes and detaches |
| Lease | LeaseKeepAlive | Bidirectional streaming |

Not implemented (status 12): KV `Txn` and `Compact`; Lease
`LeaseTimeToLive` and `LeaseLeases`; all of Auth, Maintenance,
Cluster, Election, and Lock.

## Semantic differences from etcd

- **Reads are snapshot-consistency.** The anchor does not read the
  request's `serializable` flag: every Range is served from the
  node's local applied state (committed, possibly lagging the
  leader), whether or not the client asked for linearizable. A graph
  configured with `lin_reads: 1` fences all reads instead. There is
  no per-session monotonicity guarantee across connections.
- **Watch starts at "current".** The `start_revision` field is
  parsed but not honoured: every watch begins at the current
  revision, so a client cannot resume a watch from a historical
  revision. Events are delivered in revision order; there are no
  progress notifications.
- **Watch and lease state is in-memory** on the serving node:
  clients re-establish watches and re-grant leases after a node
  restart.
- **Transactions are absent**: applications built on `Txn`
  compare-and-swap must be reworked onto single-key Put/Range
  semantics.
- **No auth**: there are no Auth RPCs and no RBAC on this surface.

## Errors

The anchor answers with `grpc-status` trailers only — there is
never a `grpc-message` text. Client libraries will render the bare
status code.

| Condition | gRPC status |
|---|---|
| Success (including a key miss — absence is in the response body) | 0 |
| Read below the compaction floor | 11 (OUT_OF_RANGE — the code etcd uses for compacted reads) |
| Unimplemented RPC | 12 (UNIMPLEMENTED) |
| Routing epoch advanced, no authority, or any internal failure | 13 (INTERNAL) |

Status 13 covering the routing and authority conditions is a known
coarseness: clients cannot yet distinguish "retry after
re-resolving" from "server fault" on this surface, so treat 13 as
retryable with backoff. Distinct FAILED_PRECONDITION and UNAVAILABLE
mappings are a design target
(see the [specification](../architecture/specification.md#design-targets-not-wired)).

## Client configuration

Point a standard etcd v3 client at a plaintext endpoint. Against a
running node with the anchor on the etcd default port:

```sh
etcdctl --endpoints=http://127.0.0.1:2379 put demo-key demo-value
# OK
etcdctl --endpoints=http://127.0.0.1:2379 get demo-key
# demo-key
# demo-value
```

Retries for status 13 are the client's responsibility.

## Configuration

The anchor is enabled by listing `etcd_edge_anchor` in the graph
config with its `listen_port`, wired to a `kv_request_router`
ingress pair, exactly as the [run guide](running.md)'s graphs wire
the Redis anchor.
