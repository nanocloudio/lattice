# Lattice etcd Interoperability Guide

This document covers etcd v3 API compatibility, known limitations, and migration guidance for Lattice.

## Compatibility Overview

Lattice implements a subset of the etcd v3 gRPC API with additional semantics for distributed consistency (LIN-BOUND).

### Supported APIs

This is the complete set of gRPC methods with a handler (the `GrpcMethod` enum in the etcd
edge anchor). Anything not listed here is unimplemented.

| Service | RPC | Notes |
|---------|-----|-------|
| KV | Range | Linearizable and serializable modes |
| KV | Put | Unary |
| KV | DeleteRange | Unary |
| Watch | Watch | Server-streaming |
| Lease | LeaseGrant | Unary |
| Lease | LeaseRevoke | Unary |
| Lease | LeaseKeepAlive | Bidirectional streaming |

### Not implemented

| Service | RPC | Notes |
|---------|-----|-------|
| KV | Txn | No handler; transactions are not available over the etcd surface |
| KV | Compact | No Compact RPC (a `COMPACTED` status may still surface on reads) |
| Lease | LeaseTimeToLive | No handler |
| Lease | LeaseLeases | No handler |
| Auth | Authenticate / UserAdd / RoleAdd / … | No auth RPCs |
| Maintenance | * | Not implemented |
| Cluster | * | Managed by CP-Raft |
| Election | * | Not implemented |
| Lock | * | Not implemented |

## Behavioral Differences

### LIN-BOUND Failures

Lattice enforces stricter linearizability semantics than etcd:

**etcd behavior:** Operations may succeed with weaker consistency during network issues.

**Lattice behavior:** Operations fail with `UNAVAILABLE` when linearizability cannot be guaranteed.

```go
// Handle LIN-BOUND failures
resp, err := client.Get(ctx, "key")
if err != nil {
    if status.Code(err) == codes.Unavailable {
        // LIN-BOUND failure - retry with backoff
        // or fall back to serializable read
    }
}
```

### Serializable Reads

```go
// Linearizable read (default) - requires LIN-BOUND
resp, err := client.Get(ctx, "key")

// Serializable read - no LIN-BOUND requirement
resp, err := client.Get(ctx, "key", clientv3.WithSerializable())
```

Lattice does not guarantee per-session monotonicity for serializable reads across requests or connections.

### Transactions

The etcd `Txn` RPC is not implemented — there is no handler for it. Applications
that rely on `client.Txn(...)` compare-and-swap must be redesigned around single-key
Put/Range semantics.

### Watch Semantics

Watches with `start_revision=0` require LIN-BOUND:

```go
// May fail if LIN-BOUND unavailable
watcher := client.Watch(ctx, "prefix/", clientv3.WithPrefix())

// Check for SNAPSHOT_ONLY trailer
for resp := range watcher {
    if resp.Header.Revision > 0 {
        // Normal linearizable watch
    }
    // Check grpc metadata for watch_semantics=SNAPSHOT_ONLY
}
```

### Routing Epoch

Lattice uses routing epochs for sharding. Clients with stale routing information receive:

```
FAILED_PRECONDITION: routing epoch changed (expected: 42, observed: 41)
```

**Solution:** Clients should refresh routing metadata and retry.

## Client Configuration

### Go Client (clientv3)

```go
cli, err := clientv3.New(clientv3.Config{
    Endpoints:   []string{"https://lattice1:2379"},
    DialTimeout: 5 * time.Second,
    TLS: &tls.Config{
        Certificates: []tls.Certificate{clientCert},
        RootCAs:      caPool,
    },
})
```

### Retry Configuration

```go
// Configure retries for LIN-BOUND failures
import "go.etcd.io/etcd/client/v3/clientv3retry"

cli, err := clientv3.New(clientv3.Config{
    Endpoints: []string{"https://lattice1:2379"},
    // ...
})

// Wrap with retry interceptor
retryClient := clientv3retry.NewRetryClient(cli,
    clientv3retry.WithMax(3),
    clientv3retry.WithBackoff(clientv3retry.BackoffLinear(100*time.Millisecond)),
)
```

## Migration Guide

### From etcd to Lattice

1. **Audit API usage**
   - Check for unsupported APIs (Txn, Compact, Auth, Election, Lock, Maintenance)
   - Rework any `Txn` compare-and-swap into single-key Put/Range flows

2. **Update client configuration**
   - Update endpoints to Lattice cluster
   - Configure mTLS certificates

3. **Handle new error codes**
   - Add handlers for `UNAVAILABLE` (LIN-BOUND failures)
   - Add handlers for `FAILED_PRECONDITION` (routing epoch)

4. **Test serializable reads**
   - Verify monotonicity requirements
   - Consider explicit serializable mode where appropriate

### Data Migration

```bash
# Export from etcd
etcdctl get "" --prefix --write-out=json > backup.json

# Import to Lattice (using etcdctl compatible endpoint)
cat backup.json | jq -r '.kvs[] | "\(.key | @base64d) \(.value | @base64d)"' | \
while read key value; do
    etcdctl --endpoints=https://lattice:2379 put "$key" "$value"
done
```

## Known Limitations

1. **No transactions** — the `Txn` RPC has no handler.

2. **No cross-KPG linearizable ranges**
   - Linearizable range queries limited to single KPG
   - Use serializable mode for multi-KPG ranges

3. **No authentication** — the etcd Auth RPCs (Authenticate/UserAdd/RoleAdd/…) are not
   implemented; there is no RBAC over this surface.

4. **No compaction** — the `Compact` RPC is not implemented.

5. **No elections/locks** — etcd Election/Lock RPCs are not implemented.

(See the "Behavioral Differences" section above for LIN-BOUND, serializable-read, and watch
semantics; those are not repeated here.)

## Conformance Testing

etcd conformance suites are **shadow-tracked** (they live in the git shadow tree, not the
primary checkout — see `standards/test-tracking.md`), so there is no `tests/interop` suite
in the working tree to invoke directly. Consult the test-tracking standard for how the
conformance suites are staged and run.

## Error Reference

### UNAVAILABLE (LIN-BOUND Failure)

```
rpc error: code = Unavailable desc = linearizability unavailable
```

Causes:
- No leader available
- ReadIndex failed
- CP cache stale/expired
- Strict fallback active

Resolution:
- Retry with exponential backoff
- Use serializable mode if acceptable

### FAILED_PRECONDITION (Routing Epoch)

```
rpc error: code = FailedPrecondition desc = routing epoch changed
```

Causes:
- Cluster reconfiguration
- KPG rebalancing

Resolution:
- Refresh routing metadata
- Retry request

## See Also

- [Deployment Guide](deployment.md) - Configuration
- [Specification](specification.md) - Full protocol details
- [Performance](performance.md) - Latency expectations
