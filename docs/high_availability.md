# Lattice High Availability Guide

This document covers high availability deployment patterns, readiness gates, drain orchestration, and rolling upgrade procedures for Lattice.

## Architecture Overview

Lattice inherits its HA model from Clustor Raft:

- **Quorum-based consensus**: 2f+1 nodes tolerate f failures
- **Leader election**: Automatic failover on leader failure
- **Linearizable reads**: Require leader ReadIndex confirmation
- **Snapshot-only reads**: Available from followers (when permitted)

## Deployment Topologies

### Three-Node Cluster (Recommended Minimum)

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Lattice 1  │    │  Lattice 2  │    │  Lattice 3  │
│  (Leader)   │◄──►│  (Follower) │◄──►│  (Follower) │
└─────────────┘    └─────────────┘    └─────────────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────┐
│              Shared Storage / CP-Raft                │
└─────────────────────────────────────────────────────┘
```

- Tolerates 1 node failure
- Maintains quorum with 2 nodes
- Single AZ or cross-AZ

### Five-Node Cluster (Production)

- Tolerates 2 node failures
- Recommended for cross-region deployments
- Higher write latency due to larger quorum

## Readiness Gates

Lattice exposes `/readyz` which evaluates multiple readiness conditions:

### Readiness Conditions

| Condition | Description | Impact if Failed |
|-----------|-------------|------------------|
| `cp_cache_fresh` | CP-Raft cache is not stale/expired | LIN-BOUND failures |
| `routing_epoch_valid` | Routing epoch is current | Request rejections |
| `leader_elected` | Raft leader exists | Write failures |
| `can_serve_reads` | ReadIndex available | Linearizable read failures |
| `adapters_ready` | All adapters initialized | Connection failures |

### Readiness Response

```json
{
  "ready": true,
  "conditions": {
    "cp_cache_fresh": true,
    "routing_epoch_valid": true,
    "leader_elected": true,
    "can_serve_reads": true,
    "adapters_ready": true
  },
  "manifest_digest": "sha256:abc123...",
  "routing_epoch": 42,
  "cache_age_seconds": 15
}
```

### Load Balancer Health Checks

Configure your load balancer to use `/readyz`:

```nginx
upstream lattice {
    server lattice1:2379;
    server lattice2:2379;
    server lattice3:2379;
}

server {
    location /health {
        proxy_pass https://lattice/readyz;
    }
}
```

## Drain Orchestration

### Graceful Drain Procedure

1. **Mark node for drain**
   ```bash
   lattice admin drain --node-id node1
   ```

2. **Stop accepting new connections**
   - Load balancer health check fails
   - Existing connections continue

3. **Wait for in-flight requests**
   - Configurable timeout (default: 30s)
   - Watches receive cancellation

4. **Transfer leadership (if leader)**
   - Initiate leadership transfer
   - Wait for new leader confirmation

5. **Stop accepting Raft messages**
   - Node becomes observer
   - Quorum maintained by remaining nodes

6. **Shutdown**
   ```bash
   lattice admin shutdown --graceful
   ```

### Drain Configuration

```toml
[admin]
drain_timeout_seconds = 30
leadership_transfer_timeout_seconds = 10
shutdown_grace_period_seconds = 5
```

### Monitoring Drain Progress

```bash
# Check drain status
lattice admin drain-status

# Metrics during drain
curl http://localhost:9090/metrics | grep lattice_drain
```

## Rolling Upgrades

### Pre-Upgrade Checklist

- [ ] Verify cluster health: all nodes Ready
- [ ] Check replication lag: `lattice_replication_lag_seconds < 1`
- [ ] Backup current state: `lattice snapshot export`
- [ ] Review changelog for breaking changes
- [ ] Test upgrade in staging environment

### Upgrade Procedure

1. **Upgrade one node at a time**
   ```bash
   # Node 1
   lattice admin drain --node-id node1
   # Wait for drain to complete
   systemctl stop lattice
   # Upgrade binary
   cp lattice-new /usr/local/bin/lattice
   systemctl start lattice
   # Wait for node to become Ready
   lattice admin wait-ready --timeout 60s
   ```

2. **Verify cluster health after each node**
   ```bash
   lattice admin cluster-status
   ```

3. **Proceed to next node only if healthy**

### Rollback Procedure

If an upgrade fails:

1. **Stop the upgraded node**
   ```bash
   systemctl stop lattice
   ```

2. **Restore previous binary**
   ```bash
   cp lattice-backup /usr/local/bin/lattice
   ```

3. **Clear any incompatible state**
   ```bash
   rm -rf /var/lib/lattice/data/*
   ```

4. **Restore from snapshot**
   ```bash
   lattice snapshot import /backup/snapshot.db
   ```

5. **Start the node**
   ```bash
   systemctl start lattice
   ```

## Failure Scenarios

### Leader Failure

**Symptoms:**
- Write requests fail
- `lattice_leader_elected` metric goes to 0
- Linearizable reads fail

**Recovery:**
- Automatic leader election (typically < 1s)
- Clients retry to new leader
- No data loss (committed data is replicated)

**Mitigation:**
- Use client retries with exponential backoff
- Deploy across failure domains

### Follower Failure

**Symptoms:**
- Replication lag increases for that node
- Cluster continues operating normally

**Recovery:**
- Node catches up on restart
- Snapshot transfer if too far behind

**Mitigation:**
- Monitor `lattice_replication_lag_seconds`
- Alert on extended failures

### Network Partition

**Symptoms:**
- Minority partition: cannot serve requests
- Majority partition: operates normally

**Recovery:**
- Partition heals
- Minority nodes sync and rejoin

**Mitigation:**
- Deploy across 3+ failure domains
- Configure appropriate election timeouts

### CP-Raft Unavailability

**Symptoms:**
- `cp_cache_fresh` goes to false
- After grace period: LIN-BOUND failures
- Operations fail with `UNAVAILABLE`

**Recovery:**
- Restore CP-Raft connectivity
- Cache refreshes automatically

**Mitigation:**
- Deploy CP-Raft with high availability
- Configure appropriate cache TTL and grace period

## Monitoring and Alerting

### Critical Alerts

| Alert | Condition | Severity |
|-------|-----------|----------|
| ClusterQuorumLost | < 50% nodes healthy | Critical |
| LeaderUnavailable | No leader for > 30s | Critical |
| LINBOUNDFailures | Error rate > 1% | High |
| ReplicationLag | Lag > 60s | High |
| CPCacheStale | Cache stale > 60s | High |

### Recommended Dashboards

1. **Cluster Health**
   - Leader status per node
   - Replication lag per follower
   - CP cache state

2. **Request Performance**
   - Request rate by operation
   - Latency percentiles
   - Error rates

3. **Capacity**
   - Active connections
   - Active watches
   - Active leases
   - WAL segment count

## Disaster Recovery

### Backup Strategy

```bash
# Export snapshot
lattice snapshot export /backup/lattice-$(date +%Y%m%d).db

# Verify snapshot integrity
lattice snapshot verify /backup/lattice-$(date +%Y%m%d).db
```

### Recovery Procedure

1. **Stop all nodes**
2. **Clear data directories**
3. **Import snapshot on one node**
   ```bash
   lattice snapshot import /backup/lattice-20240101.db
   ```
4. **Start the node as single-node cluster**
5. **Add other nodes to cluster**

### Cross-Region DR

For cross-region disaster recovery:

1. **Async replication** to DR region
2. **Unfenced promotion** on failover
   - LIN-BOUND unavailable until fenced
3. **Fenced promotion** restores full functionality

See [Disaster Recovery](deployment.md#disaster-recovery) for details.

## See Also

- [Deployment Guide](deployment.md) - Initial setup
- [Performance](performance.md) - Tuning for HA workloads
- [Specification](specification.md) - LIN-BOUND semantics
