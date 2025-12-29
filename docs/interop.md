# Lattice etcd Interoperability Guide

This document covers etcd v3 API compatibility, known limitations, and migration guidance for Lattice.

## Compatibility Overview

Lattice implements a subset of the etcd v3 gRPC API with additional semantics for distributed consistency (LIN-BOUND).

### Supported APIs

| Service | RPC | Status | Notes |
|---------|-----|--------|-------|
| KV | Range | Full | Linearizable and serializable modes |
| KV | Put | Full | All options supported |
| KV | DeleteRange | Full | All options supported |
| KV | Txn | Full | Single-KPG only |
| KV | Compact | Partial | Per-KPG compaction |
| Watch | Watch | Full | Bidirectional streaming |
| Lease | LeaseGrant | Full | |
| Lease | LeaseRevoke | Full | |
| Lease | LeaseKeepAlive | Full | Bidirectional streaming |
| Lease | LeaseTimeToLive | Full | |
| Lease | LeaseLeases | Full | |
| Auth | Authenticate | Partial | Basic auth only |
| Auth | UserAdd | Partial | Minimal subset |
| Auth | RoleAdd | Partial | Minimal subset |

### Unsupported APIs

| Service | RPC | Reason |
|---------|-----|--------|
| Maintenance | * | Use Lattice CLI instead |
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

**Important:** Lattice does not guarantee per-session monotonicity for serializable reads across requests or connections.

### Cross-Shard Transactions

Lattice rejects transactions that span multiple Key Partition Groups (KPGs):

```go
// This may fail with FAILED_PRECONDITION if keys hash to different KPGs
txn := client.Txn(ctx)
resp, err := txn.If(
    clientv3.Compare(clientv3.Version("key1"), "=", 0),
).Then(
    clientv3.OpPut("key1", "value1"),
    clientv3.OpPut("key2", "value2"), // May be in different KPG
).Commit()

if err != nil {
    if status.Code(err) == codes.FailedPrecondition {
        // Check for TxnCrossShardUnsupported
    }
}
```

**Workaround:** Design key schemas to co-locate related keys in the same KPG.

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
   - Check for unsupported APIs (Election, Lock, Maintenance)
   - Check for cross-key transactions

2. **Update client configuration**
   - Update endpoints to Lattice cluster
   - Configure mTLS certificates

3. **Handle new error codes**
   - Add handlers for `UNAVAILABLE` (LIN-BOUND failures)
   - Add handlers for `FAILED_PRECONDITION` (routing epoch)

4. **Test serializable reads**
   - Verify monotonicity requirements
   - Consider explicit serializable mode where appropriate

5. **Design for single-KPG transactions**
   - Review key naming schemes
   - Co-locate related keys under common prefixes

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

### v0.1 Limitations

1. **No cross-KPG transactions**
   - Transactions limited to single KPG
   - Plan key schemas accordingly

2. **No cross-KPG linearizable ranges**
   - Linearizable range queries limited to single KPG
   - Use serializable mode for multi-KPG ranges

3. **Limited auth subset**
   - Basic authentication only
   - Prefix-based RBAC

4. **No scripting**
   - Lua/EVALSHA not supported

5. **No elections/locks**
   - etcd election/lock RPCs not implemented

### Behavioral Differences Summary

| Behavior | etcd | Lattice |
|----------|------|---------|
| Linearizable failures | May succeed with weaker consistency | Fail with UNAVAILABLE |
| Session monotonicity | Best-effort | Not guaranteed for serializable |
| Cross-key transactions | Always allowed | Single-KPG only |
| Watch start=0 | Always linearizable | Requires LIN-BOUND |
| Compaction | Global | Per-KPG |

## Conformance Testing

Run conformance tests to verify compatibility:

```bash
# Run Lattice conformance suite
make interop

# Specific test categories
cargo test --test interop put_get
cargo test --test interop txn
cargo test --test interop watch
cargo test --test interop lease
```

### Test Categories

| Category | Description |
|----------|-------------|
| `put_get` | Basic CRUD operations |
| `txn` | Transaction semantics |
| `watch` | Watch event delivery |
| `lease` | Lease lifecycle |
| `lin_bound` | LIN-BOUND failure modes |
| `error_codes` | Error response format |

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

### FAILED_PRECONDITION (Cross-Shard Txn)

```
rpc error: code = FailedPrecondition desc = TxnCrossShardUnsupported
```

Causes:
- Transaction keys span multiple KPGs

Resolution:
- Redesign key schema
- Split into multiple transactions

## See Also

- [Deployment Guide](deployment.md) - Configuration
- [Specification](specification.md) - Full protocol details
- [Performance](performance.md) - Latency expectations
