# Lattice Performance Guide

This document covers performance characteristics, latency targets, tuning recommendations, and metrics interpretation for Lattice.

## Latency Targets

### L1 Server-Side Latency

Target: **≤10ms p99** for single-key writes under healthy conditions.

This measures from ingress receipt to WAL quorum durability acknowledgment.

| Operation | p50 Target | p99 Target | Notes |
|-----------|------------|------------|-------|
| Put (single key) | ≤2ms | ≤10ms | Quorum-durable |
| Get (linearizable) | ≤1ms | ≤5ms | Includes ReadIndex |
| Get (serializable) | ≤0.5ms | ≤2ms | Local read |
| Delete (single key) | ≤2ms | ≤10ms | Quorum-durable |
| Txn (single KPG) | ≤3ms | ≤15ms | CAS-FENCE + mutations |
| Watch event | ≤1ms | ≤5ms | Event delivery latency |
| Lease grant | ≤2ms | ≤10ms | Quorum-durable |
| Lease keepalive | ≤2ms | ≤10ms | Quorum-durable |

### Network Latency Assumptions

- In-AZ: ≤0.5ms RTT
- Cross-AZ: ≤2ms RTT
- Cross-region: 20-100ms RTT (impacts quorum latency)

## Throughput Characteristics

### Single Node

| Metric | Typical | Maximum |
|--------|---------|---------|
| Writes/sec | 10,000 | 50,000 |
| Reads/sec (linearizable) | 20,000 | 100,000 |
| Reads/sec (serializable) | 50,000 | 200,000 |
| Watches (active) | 10,000 | 100,000 |
| Leases (active) | 10,000 | 100,000 |

### Cluster (3-node)

| Metric | Typical | Maximum |
|--------|---------|---------|
| Writes/sec (cluster) | 10,000 | 50,000 |
| Reads/sec (distributed) | 50,000 | 200,000 |

Write throughput is bounded by leader capacity and quorum latency.

## Tuning Recommendations

### CPU

- Use at least 4 cores for production
- Pin Raft threads to dedicated cores for latency consistency
- Monitor `lattice_cpu_usage` for saturation

```toml
[runtime]
raft_thread_count = 2
apply_thread_count = 2
```

### Memory

- Allocate sufficient memory for working set + indexes
- Monitor `lattice_memory_usage_bytes`
- Consider key count and average value size

**Sizing formula:**
```
Memory = (key_count * avg_key_size) + (key_count * 100 bytes overhead) + (active_watches * 1KB)
```

Example: 1M keys @ 100 bytes = ~200MB for indexes

### Storage

- Use NVMe SSDs for WAL
- Separate WAL and data volumes
- Monitor `lattice_wal_write_latency_seconds`

```toml
[paths]
wal_dir = "/fast-nvme/lattice/wal"
data_dir = "/ssd/lattice/data"
```

### Network

- Use dedicated network for Raft replication
- Enable TCP keepalives
- Monitor `lattice_network_latency_seconds`

```toml
[network]
raft_port = 2380
replication_buffer_size = 67108864  # 64MB
tcp_keepalive_seconds = 30
```

## Configuration Tuning

### Raft Parameters

```toml
[raft]
# Election timeout (ms) - increase for high-latency networks
election_timeout_min_ms = 1000
election_timeout_max_ms = 2000

# Heartbeat interval - typically election_timeout / 10
heartbeat_interval_ms = 100

# Maximum uncommitted entries
max_uncommitted_entries = 1000

# Snapshot threshold
snapshot_entries_threshold = 100000
```

### Batching

```toml
[batching]
# Write batching for throughput
write_batch_size = 100
write_batch_timeout_ms = 1

# Watch event batching
watch_batch_size = 100
watch_batch_timeout_ms = 10
```

### Connection Pooling

```toml
[listeners.grpc]
# Maximum connections
max_connections = 10000

# Connection queue depth
backlog = 1024

# Idle connection timeout
idle_timeout_ms = 300000
```

## Metrics Interpretation

### Latency Metrics

```promql
# Write latency p99
histogram_quantile(0.99,
  rate(lattice_adapter_etcd_request_duration_seconds_bucket{operation="Put"}[5m]))

# Read latency p99
histogram_quantile(0.99,
  rate(lattice_adapter_etcd_request_duration_seconds_bucket{operation="Range"}[5m]))
```

### Throughput Metrics

```promql
# Requests per second
rate(lattice_adapter_etcd_requests_total[5m])

# Error rate
rate(lattice_adapter_etcd_errors_total[5m]) / rate(lattice_adapter_etcd_requests_total[5m])
```

### Resource Utilization

```promql
# CPU usage
rate(process_cpu_seconds_total[5m])

# Memory usage
process_resident_memory_bytes

# File descriptors
process_open_fds
```

### Raft Metrics

```promql
# Replication lag
lattice_replication_lag_seconds

# Uncommitted entries
lattice_raft_uncommitted_entries

# Proposal rate
rate(lattice_raft_proposals_total[5m])
```

## Performance Testing

### Benchmarking

Use the built-in benchmark tool:

```bash
# Write benchmark
lattice benchmark put --keys 10000 --value-size 100 --concurrency 10

# Read benchmark
lattice benchmark get --keys 10000 --concurrency 100

# Mixed workload
lattice benchmark mixed --read-ratio 0.8 --keys 10000 --duration 60s
```

### Load Testing

Using etcd's benchmark tool:

```bash
# Write test
benchmark --endpoints=https://localhost:2379 \
    --cert=/path/to/client.crt --key=/path/to/client.key --cacert=/path/to/ca.crt \
    put --key-size=8 --val-size=256 --total=100000 --conns=100

# Read test
benchmark --endpoints=https://localhost:2379 \
    --cert=/path/to/client.crt --key=/path/to/client.key --cacert=/path/to/ca.crt \
    range /foo --total=100000 --conns=100
```

## Performance Debugging

### High Latency

1. **Check Raft latency**
   ```promql
   lattice_raft_proposal_duration_seconds
   ```
   - High: Network latency or follower slowness

2. **Check WAL latency**
   ```promql
   lattice_wal_write_latency_seconds
   ```
   - High: Storage saturation

3. **Check apply latency**
   ```promql
   lattice_apply_loop_duration_seconds
   ```
   - High: State machine bottleneck

### Low Throughput

1. **Check leader CPU**
   - Leader does more work than followers
   - Consider scaling leader resources

2. **Check batch sizes**
   - Small batches = high overhead
   - Increase batch size/timeout

3. **Check network bandwidth**
   ```promql
   rate(lattice_network_bytes_sent_total[5m])
   ```

### Memory Growth

1. **Check key count**
   ```promql
   lattice_kv_key_count
   ```

2. **Check watch count**
   ```promql
   lattice_watch_active_streams
   ```

3. **Check compaction**
   ```promql
   lattice_kv_compaction_floor_revision
   ```
   - Stalled: Historical data accumulating

## Capacity Planning

### Storage Sizing

```
WAL size = write_rate * retention_period * 2 (safety margin)
Data size = key_count * (avg_key_size + avg_value_size + 100 bytes overhead)
Snapshot size ≈ Data size (compressed)
```

### Memory Sizing

```
Base memory: 500MB
Per 1M keys: 200MB
Per 10K watches: 10MB
Per 10K leases: 10MB
Safety margin: 2x
```

### Connection Sizing

```
Connections per client: 1-10 (pooled)
Maximum connections: 10,000-100,000 (depends on memory)
File descriptors: connections * 2 + 1000 (headroom)
```

## See Also

- [Deployment Guide](deployment.md) - Configuration reference
- [High Availability](high_availability.md) - HA tuning
- [Specification](specification.md) - Protocol details
