# Lattice documentation

Lattice is a multi-protocol data store built as a graph of
cooperative [fluxor](../../fluxor/) modules on the clustor Raft
substrate. Protocol anchor modules translate client wire protocols
— Redis RESP, Memcached ASCII, etcd v3 gRPC, SQL, documents, CQL,
RESP models, Prometheus metrics — into one KV request envelope; a
router shards by key
and drives writes through Raft commit; a deterministic MVCC state
machine applies them in log order. Durability, quorum, leadership,
and snapshots belong to the substrate; lattice never writes a
durable channel of its own.

## Start here

- [guides/running.md](guides/running.md) — bring up a single node
  or a three-node cluster and smoke check it with a Redis client
- [architecture/specification.md](architecture/specification.md) —
  invariants, data model, routing, read semantics, error mapping
- [guides/deployment.md](guides/deployment.md) — building,
  configuration shape, verification, troubleshooting

## Architecture reference

- [architecture/specification.md](architecture/specification.md) —
  the normative reference: invariants, terminology, partitioning
  and routing epochs, LIN-BOUND read semantics, snapshots and
  compaction, TTL/lease/watch, error mapping, and the explicit list
  of design targets not yet wired
- [architecture/cdc.md](architecture/cdc.md) — change data capture:
  the event format, the ordered-ack sink contract, checkpointing,
  and retention
- [architecture/limit_register.md](architecture/limit_register.md)
  — machine-checked deliberate ceilings

## Guides

- [guides/running.md](guides/running.md) — validated bring-up,
  smoke checks, failover
- [guides/deployment.md](guides/deployment.md) — build,
  configuration, transport security, verification
- [guides/high_availability.md](guides/high_availability.md) —
  topologies, readiness, rolling upgrades, failure behaviour,
  backup and restore
- [guides/tuning.md](guides/tuning.md) — the pacing, durability,
  and consensus knobs and what they trade
- [guides/redis.md](guides/redis.md) — the Redis surface: command
  set, differences, errors
- [guides/memcached.md](guides/memcached.md) — the Memcached
  surface: command set, differences, errors
- [guides/etcd.md](guides/etcd.md) — the etcd v3 surface: RPCs,
  differences, errors
- [guides/metrics.md](guides/metrics.md) — the Prometheus metrics
  surface: endpoints, the PromQL/MetricsQL/KQL query subsets, bounds
