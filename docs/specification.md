1. Title
   Lattice Key-Value Overlay Technical Specification v0.1

2. Overview

* Lattice is a multi-protocol **distributed key-value store** implemented as a **single binary** built on the Clustor Raft consensus core. Lattice maps multiple client-facing KV protocols onto a unified state machine and durability contract while inheriting Clustor’s ordering, durability ledgers, strict fallback behavior, read gates, snapshot model, and security guardrails (Clustor spec §§3–14).
* Lattice exposes three protocol surfaces (“adapters”), each translating protocol semantics into the same internal KV primitives:

  * **etcd v3 client surface** (gRPC KV/Watch/Lease/Auth/Txn subset; linearizable reads and transactions supported when Clustor ReadIndex is available).
  * **Redis client surface** (RESP2/RESP3; subset of commands with transactional and scripting constraints defined herein).
  * **Memcached client surface** (ASCII and optional binary protocol subset; CAS semantics mapped to internal revisions).
* **LIN-BOUND (linearizability guardrail):** Lattice MUST NOT claim or emulate **linearizable** semantics for any adapter operation unless the Clustor leader can serve ReadIndex (Clustor §10.3) and the operation's response is fenced by the same proof and index equality. LIN-BOUND gates linearizable reads, compare/CAS evaluation, and transaction fences; it does **not** gate non-conditional writes, which follow Clustor's durability rules independently (see §9.1 for the complete scope). When LIN-BOUND is not satisfied, the system MUST fail closed for linearizable operations (with protocol-specific errors) and MAY offer **snapshot-only** (bounded-staleness) reads where explicitly allowed.
* **KVSOURCE:** Every observable key-value effect MUST be derived from WAL entries or signed snapshots. No adapter may introduce an alternate durable channel.
* **L1 (Latency Scope):** ≤10 ms p99 in-AZ server-side latency measured from edge ingress receipt to WAL quorum durability for single-key writes under healthy conditions. This measurement excludes client RTT but includes adapter parsing and authorization overhead. This target applies to durable writes; snapshot-only reads do not wait for WAL and are not subject to this bound. This is a planning target; compliance requirements remain the durability and gating rules in this specification.
* Control-plane objects (tenant/auth/routing/quota/certificates and adapter policies) are stored in **CP-Raft** (Clustor ControlPlaneRaft) and cached by nodes. Stale/expired caches enforce strict fallback and LIN-BOUND failure behavior per Clustor §14.2 cache state matrix.
* Lattice is designed to be deployed as a uniform fleet where each node runs the same binary and can host multiple tenant shards (“KPGs”) that correspond to Clustor partitions.

3. Scope

* This document specifies Lattice’s KV state machine, adapter contracts for etcd/Redis/Memcached, routing and sharding, transaction rules, watch/lease behavior, revision/CAS semantics, quotas, and isolation boundaries.
* Clustor remains authoritative for Raft, durability modes, WAL/snapshots, read gates, strict fallback, ledger ordering, security primitives, cache state semantics, and version negotiation (Clustor spec §§3–14).

4. Non-Goals

* Re-defining consensus, WAL layouts, strict fallback, durability proofs, or snapshot cryptography already specified by Clustor.
* Supporting protocol features that require semantics Lattice cannot safely provide under LIN-BOUND (e.g., Redis cluster redirections, Redis Lua with non-deterministic side effects, etcd multi-object compare-and-swap across shards without explicit routing constraints).
* Providing multi-binary architectures (no separate “edge” daemon requirement); optional listeners are built into the single binary and can be disabled by configuration.
* Byzantine fault tolerance; Lattice inherits Clustor’s crash-only fault model.

5. Terminology and Concepts

5.1 Core Terms

| Term               | Definition                                                                                                                                              |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| KPG                | Key-Value Partition Group: a tenant-scoped Clustor partition hosting a KV state machine.                                                                |
| CP-Raft            | Clustor ControlPlaneRaft cluster holding tenant manifests, routing epochs, adapter policies, quotas, and PKI material.                                  |
| Adapter            | Protocol module mapping an external protocol (etcd/Redis/Memcached) to the Lattice KV primitives and Clustor durability/read gates.                     |
| revision           | Monotone per-KPG commit sequence number equal to the Raft commit index for that KPG. For mutations, this equals the Raft log index of the entry that performed the mutation (`mod_revision`). For read fences (e.g., linearizable reads), the returned `header.revision` equals the commit index used to fence the read, which may be greater than any key's `mod_revision` observed in the response. Clustor's ReadIndex (§10.3) returns a commit index that Lattice uses directly as a revision fence. |
| kv_epoch           | Monotone configuration epoch per tenant, equal to the CP-Raft routing epoch; fences sharding and policy.                                                |
| LIN-BOUND          | Linearizability guardrail that requires Clustor ReadIndex eligibility (Clustor §10.3) before claiming linearizable semantics.                           |
| snapshot-only read | Read that may observe a committed state without a ReadIndex fence; MUST be explicitly labeled/limited per adapter mapping. In the etcd context, `serializable=true` maps to snapshot-only semantics. |
| keyspace           | Tenant-scoped namespace for keys. Keys are byte strings; adapters define permissible encodings and normalization.                                       |
| compare            | A conditional predicate used in transactions (etcd-style compare, Redis WATCH-like fencing, Memcached CAS).                                             |
| kv_ttl             | Time-to-live for keys with expiration; implemented deterministically in the apply loop.                                                                 |

5.2 Default Timers

* Defaults are operator-configurable and tenant-overridable within operator-defined bounds stored in CP-Raft:

  * `lease_ttl_default_ms = 60_000`, max `600_000`.
  * `watch_idle_timeout_ms = 300_000`.
  * `expiry_scan_period_ms = 1_000` (deterministic tick, see §9.6).
  * `auth_cache_ttl_ms = 30_000`, grace aligned to Clustor cache state (`controlplane.cache_grace_ms`).

6. System Model

6.1 Components

* **Single binary node:** Runs protocol listeners (etcd gRPC, Redis TCP, Memcached TCP/UDP optional), hosts multiple KPGs (Clustor partitions), and a control agent that maintains CP-Raft caches and publishes readiness.
* **KPG runtime:** For each KPG: apply loop, snapshots, compaction policy integration, flow control integration, adapter-facing request router.
* **CP cache agent:** Watches CP-Raft for tenant manifests, routing epochs, adapter policies, quotas, certificates, and feature flags; enforces cache state semantics and strict fallback coupling inherited from Clustor.

6.2 Environment

* Linux ≥5.15, io_uring supported, NVMe SSDs with barriers enabled; deployments that violate Clustor storage guardrails MUST fail bootstrap.
* Clocks disciplined; excessive skew triggers leadership fencing and disables time-based behaviors (lease renewal, expiry ticks) until recovered.

6.3 Crash Model

* Crash behavior, WAL ordering, durability acks, and snapshot import/export are inherited from Clustor (Clustor §§6.2, 9.5, 10.5).

6.4 Assumptions

* Each production tenant provisions ≥3 KPG voters (5 for DR profiles) unless explicitly overridden via CP-Raft override ledger with documented justification.
* CP-Raft outages shorter than `controlplane.cache_grace_ms` retain cached metadata; stale/expired caches force strict fallback and LIN-BOUND failures per Clustor cache matrix.

7. KV Data Model

7.1 Entities

* **KV record:** `Key → ValueState{value_bytes, create_revision, mod_revision, version, lease_id?, expiry_at_ms?, metadata_flags}`.

  * `version` increments on each successful mutation.
  * `mod_revision` equals the Raft log index of the mutation entry (the KPG’s revision).
* **Lease record:** `LeaseID → LeaseState{ttl_ms, granted_at_ms, keepalive_deadline_ms, attached_keys[], epoch}`.

  * `epoch` increments on lease revoke/recreate fencing.
* **Watch stream state (durable subset):** `WatchID → WatchState{key_range, start_revision, filters, progress_notify, last_sent_revision}`.

  * `last_sent_revision` is the revision fence of the last event delivered to the client (which may be a mutation's `mod_revision` or a progress notification's commit index). For adapters implementing etcd-conformant watch semantics (§10.1.6), implementations MUST persist `last_sent_revision` for replay correctness. Without persistence, watch behavior is non-conformant to the etcd contract: the system cannot guarantee which events have been delivered, risking duplicate or missed notifications. Other volatile transport state (e.g., connection handles) is not persisted. Adapters without watch semantics (Redis, Memcached) do not require this persistence.
* **Auth/principal cache entries:** Derived from CP-Raft manifests; not authored by the KPG apply loop.

7.2 Invariants

* **KVSOURCE:** All KV mutations are WAL-backed and replay-deterministic from snapshot + WAL.
* **REV-MONOTONE:** `revision` is strictly increasing per KPG; every mutation has a unique `mod_revision`.
* **CAS-FENCE:** CAS-style and transactional compare operations MUST be evaluated at a single revision fence (this does not apply to snapshot-only read predicates). All CAS-style operations (including Memcached `cas`) are **revision-equality compares** against `mod_revision`. etcd Txn supports additional compare predicates (value, version, lease) that are evaluated within the same revision fence but are not revision-equality compares; these are distinct from CAS semantics. If LIN-BOUND cannot be satisfied for linearizable compare semantics, the operation MUST fail closed (see §10).
* **TTL-DETERMINISM:** Expiration is applied deterministically using apply-loop ticks and committed time sources defined in §9.6; wall-clock sampling outside the apply loop MUST NOT directly delete keys.

8. Partitioning and Routing

8.1 Sharding

* Keys are tenant-scoped; routing uses a versioned hash:

  * `kpg = hash64(tenant_id, key_bytes, hash_seed_version) % tenant_kpg_count`
* For etcd-compatible range operations, Lattice MUST route by **range-to-KPG mapping**:

  * Single-KPG range reads are supported directly.
  * Multi-KPG range reads require a CP-Raft approved fanout plan and are exposed only where the adapter semantics tolerate it (see §10.1.5 for etcd Range and §10.2.9 for Redis SCAN).
* Any change to hash seed or tenant_kpg_count requires CP-Raft routing epoch migration; stale epochs are rejected as `dirty_epoch` (adapter-specific mapping in §11).

8.2 Placement and Rebalance

* CP-Raft assigns KPGs to nodes and performs membership changes via Clustor §9.9.
* Rebalance phases: (1) CP-Raft plan; (2) learner catch-up; (3) routing epoch flip; (4) clients reconnect/retry. Requests arriving with an older kv_epoch MUST be rejected with `dirty_epoch`.

8.3 Routing Guarantees

* Within a KPG, operations are ordered by WAL order and revisions are comparable.
* Across KPGs, no total order is guaranteed; adapters MUST NOT claim global ordering unless the operation is explicitly restricted to a single KPG.

9. State and Lifecycle

9.1 LIN-BOUND and Read Semantics Matrix

* Lattice maps Clustor’s read gate predicate (Clustor §10.3) into adapter behavior. Define:

  * `can_linearize = clustor_read_gate_predicate() == true` (leader only).
  * `can_snapshot_read = true` when the node can serve snapshot-only reads per Clustor role rules (leader snapshot-only always allowed; follower snapshot-only is **conditional** on Clustor granting follower snapshot capability and MUST NOT be assumed unconditionally).
* **LIN-BOUND scope:** LIN-BOUND gates operations that require linearizable semantics, specifically: (1) linearizable reads, (2) compare/CAS evaluation, and (3) transaction fences. LIN-BOUND does **not** gate writes directly; writes follow Clustor's durability rules independently.
* Service matrix:
  | Context / operation class | Healthy (ReadIndex eligible) | Strict fallback / ReadIndex ineligible |
  | --- | --- | --- |
  | Linearizable reads (etcd serializable=false, Redis command requiring linearization, Memcached CAS read-modify-write) | Allowed | MUST fail closed (adapter-specific errors) |
  | Snapshot-only reads (explicitly permitted) | Allowed | Allowed, but MUST be labeled/limited per adapter |
  | Writes (non-conditional) | Allowed (configured durability) | Allowed under Clustor rules (LIN-BOUND does not gate non-conditional writes; durability mode may be forced Strict per cache state) |
  | Watches (linearizable start) | Allowed | MUST either fail closed or start as snapshot-only with explicit limitations (etcd Watch mapping in §10.1.6) |

9.2 Leader/Follower Behavior

* Only KPG leaders may serve linearizable reads and commit-fenced transactions.
* Followers MAY serve snapshot-only reads per the `can_snapshot_read` rules in §9.1; otherwise adapters must redirect or fail.

9.3 Durability and ACK Contract

* All adapter-visible ACKs for mutations MUST follow Clustor’s durability ACK contract (Clustor §10.4) and MUST NOT acknowledge before quorum durability.
* Adapter retries MUST be idempotent using adapter-provided idempotency keys mapped to an internal `IdemKey → {ack_revision, outcome_digest}` table with bounded TTL and deterministic replay. Eviction of idempotency records (after TTL expiry) MUST itself be WAL-backed to ensure replay correctness; alternatively, if eviction is not WAL-backed, the idempotency outcome MUST be **deterministically recomputable** from replayed WAL entries without ambiguity. "Deterministically recomputable" means: given the same WAL entries and apply-loop state, the operation outcome (success/failure, resulting revision, response payload) is identical on every replay. Operations with non-deterministic outcomes (e.g., dependent on wall-clock time or external state) MUST use WAL-backed eviction.

9.4 Startup and Quarantine

* Integrity checks, scrub, and quarantine behavior are inherited from Clustor (Clustor §9.4). Adapters MUST surface quarantined partitions as unavailable and must not “mask” quarantine with protocol-level success.

9.5 Snapshots and Compaction Integration

* Snapshot emission/import is inherited from Clustor.
* KV compaction is a logical policy layered on Clustor’s WAL compaction floor: Lattice MAY garbage-collect historical MVCC revisions only when it does not violate Clustor floors and only when it preserves adapter correctness (notably etcd watch and historical range queries).
* Lattice MUST persist a `kv_compaction_floor_revision` and ensure it never exceeds Clustor’s effective compaction floor index mapping.

9.6 Deterministic Time and TTL

* Lattice defines a deterministic tick source: `Tick(ms)` entries committed periodically to the WAL (operator-configurable, default 1s) OR derived from Clustor snapshot manifest time fences if enabled by a feature gate. Exactly one source MUST be active per KPG at any time; both sources are WAL-committed; no non-WAL time source is permitted.
* TTL expirations MUST be evaluated only when processing a committed tick or on a mutation that touches the key.
* Lease keepalive updates are mutations and therefore WAL-backed; they are subject to Clustor durability ACK rules.

10. Protocol Adapters

10.1 etcd v3 Adapter

10.1.1 Transport and Identity

* Exposes an etcd v3-compatible gRPC endpoint with mTLS required. AuthN/Z is derived from CP-Raft principal manifests.
* Requests MUST carry `kv_epoch` (routing epoch) implicitly via SNI/tenant routing or explicitly via metadata; mismatches are `dirty_epoch`.

10.1.2 Revision Model

* Lattice maps etcd's per-key `mod_revision` field to the KPG's internal `mod_revision` (the Raft log index of the mutation).
* For responses, the returned `header.revision` MUST equal the revision fence used for the operation. For linearizable reads, this fence MUST be a ReadIndex-fenced commit index. Note: `header.revision` may exceed any individual key's `mod_revision` when the read fence is newer than the last mutation (see §5.1 definition of revision).

10.1.3 Range (Get)

* **Linearizable Range:** `serializable=false` MUST require `can_linearize=true`. The adapter MUST perform a ReadIndex fence and read at the resulting commit index.
* **Serializable Range:** `serializable=true` MAY be served as snapshot-only and MUST clearly obey the constraints: it may lag behind the leader's latest committed revision fence. Per-session monotonicity is a **best-effort optimization** (via sticky session routing) and is NOT a semantic guarantee; clients MUST NOT rely on monotonic reads across requests, even on the same gRPC connection or stream, or after routing changes.
* Multi-KPG ranges:

  * MUST be rejected for linearizable ranges unless CP-Raft provides a fanout plan with a single revision fence mechanism; v0.1 defaults to rejecting linearizable multi-KPG ranges.
  * Serializable multi-KPG ranges MAY be supported with per-KPG revision fences and merged results; the adapter MUST set a response flag `partial_revision_fence=true` and MUST NOT claim a single global revision fence ordering. **Note:** `partial_revision_fence` is etcd adapter-specific response metadata and is non-normative for other adapters.

10.1.4 Put/Delete

* Put and Delete MUST be quorum-durable before responding success.
* `prev_kv` returns the previous state as of the mutation fence; computed in the apply loop.

10.1.5 Txn (Compare/Then/Else)

* Transactions are evaluated in the apply loop at a single revision fence.
* Supported compares: `mod_revision`, `create_revision`, `version`, `value` (byte compare), `lease`.
* A Txn that spans keys routed to different KPGs MUST be rejected in v0.1 with error identifier `TxnCrossShardUnsupported` (stable string constant) unless CP-Raft enables a feature gate that provides a deterministic coordination mechanism (out of scope for v0.1).
* Under LIN-BOUND failure, Txn MUST fail closed with `UNAVAILABLE` and a structured error hint `LinearizabilityUnavailable`.

10.1.6 Watch

* Watch creation with `start_revision=0` (start at “now”):

  * MUST require `can_linearize=true` to guarantee it starts at a linearizable point-in-time.
  * If `can_linearize=false`, the adapter MUST either reject watch creation or start as snapshot-only with a protocol-visible flag `watch_semantics=SNAPSHOT_ONLY` (implemented as a custom gRPC trailer) and MUST NOT claim etcd-linearizable semantics.
* Watch delivery order: per-key order is by `mod_revision`; per-watch order is by WAL order within the KPG.
* Progress notifications are generated by committed ticks or commit index advancement; they MUST be deterministic with respect to WAL state and committed ticks (see §9.6).

10.1.7 Lease

* Lease grant/revoke/keepalive are WAL-backed mutations.
* Keys attached to a lease MUST expire only through deterministic tick processing (§9.6).
* Keepalive responses must reflect the committed lease state, not local wall-clock.

10.1.8 Auth

* v0.1 supports a minimal subset: authenticate principal, role-based key prefix permissions, and token TTLs managed by CP-Raft.
* If CP-Raft cache is Stale/Expired, operations requiring policy evaluation MUST fail closed as `Not authorized` or `UNAVAILABLE` per the policy in §11.

10.2 Redis Adapter

10.2.1 Protocol and Connection

* Exposes RESP2 by default; RESP3 MAY be enabled via a CP-Raft feature gate (see §12.1). TLS is supported; mTLS is required in production profiles.
* Client routing is by tenant and key hash; pipelining is supported, but responses must preserve request order per connection.

10.2.2 Data Types

* v0.1 supports: Strings, Hashes, and TTL metadata. Lists/Sets/ZSets MAY be added later under a feature gate with deterministic apply constraints.

10.2.3 Command Support and Semantics

* Supported commands (normative subset):

  * Strings: `GET`, `SET` (with `NX/XX`, `EX/PX`), `MGET` (single-KPG only by default), `INCR/DECR` (single key).
  * TTL: `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`.
  * Hashes: `HGET`, `HSET`, `HDEL`, `HMGET` (single-KPG).
  * Admin-lite: `PING`, `INFO` (restricted), `AUTH` (optional; mapped to CP-Raft).
* Unsupported commands MUST return `-ERR unknown command` or a more specific error string, but MUST NOT silently degrade semantics.

10.2.4 Linearizable vs Snapshot-Only Reads

* Redis does not expose linearizable toggles; therefore Lattice defines:

  * Commands where linearizability is commonly expected: `GET` on a key previously written on the same connection after an acknowledged `SET`, conditional `SET NX/XX`, and read-modify-write (`INCR/DECR`). These MUST require `can_linearize=true` or fail closed with `-TRYAGAIN linearizability unavailable`. **Note:** Conditional writes (`SET NX/XX`, `INCR/DECR`) fail under LIN-BOUND because their semantics include an implicit read/compare fence, not because LIN-BOUND gates writes directly (see §9.1 LIN-BOUND scope). **Important:** Connection identity alone is insufficient for linearizability guarantees; the guarantee depends on stable routing to the same KPG leader. If leader routing changes (e.g., due to leader failover, load balancer reassignment, or connection migration), the causal ordering guarantee is broken and requires re-establishment via explicit synchronization (e.g., a successful write followed by read).
  * Commands permitted as snapshot-only: `TTL/PTTL` MAY be snapshot-only if configured, but must be labeled in `INFO` and telemetry. Default is linearizable where possible.
  * `MGET` and `HMGET` (single-KPG only): MUST require `can_linearize=true` for linearizable semantics, or MAY be served as snapshot-only if explicitly configured. If served as snapshot-only, responses MUST be labeled in telemetry.

10.2.5 Transactions (MULTI/EXEC)

* `MULTI/EXEC` is supported only when all queued commands target a **single KPG**.
* Under LIN-BOUND failure, `EXEC` MUST fail closed with `-TRYAGAIN linearizability unavailable`.
* `WATCH` MUST NOT be supported in v0.1. Future versions MAY support `WATCH` restricted to single-KPG keys and mapped to compare fences; any such implementation MUST be WAL-backed to remain deterministic.

10.2.6 Lua/Scripting

* `EVAL/EVALSHA` MUST be disabled in v0.1 unless a feature gate ensures determinism, bounded execution, and explicit command allowlists. Any enabled scripting MUST execute inside the apply loop or as a pre-checked deterministic plan.

10.2.7 SCAN

* `SCAN` MAY be supported only as snapshot-only and MUST be bounded:

  * Single-KPG keyspace scan is allowed.
  * Multi-KPG scan requires fanout and returns non-deterministic interleaving; v0.1 defaults to single-KPG only.

10.3 Memcached Adapter

10.3.1 Protocol

* ASCII protocol is supported; binary protocol MAY be enabled via a feature gate. UDP is optional and MUST be disabled by default in production profiles.

10.3.2 Core Commands

* Supported: `get`, `gets`, `set`, `add`, `replace`, `append`, `prepend`, `delete`, `incr`, `decr`, `touch`.
* `cas` is supported and maps to Lattice compare fences using `mod_revision`-derived CAS tokens.

10.3.3 CAS Semantics

* A `gets` response returns a CAS token equal to the key's current `mod_revision`.
* A `cas` write MUST succeed only if the current `mod_revision` equals the provided token, evaluated at a linearizable fence. If `can_linearize=false`, `cas` MUST fail closed with `SERVER_ERROR linearizability unavailable`. **Note:** `cas` fails under LIN-BOUND because it includes a revision-equality compare fence, not because LIN-BOUND gates writes directly (see §9.1 LIN-BOUND scope).
* Non-CAS writes update the token to the new mutation's `mod_revision`.
* **CAS token monotonicity:** CAS tokens are monotone **per key only**, not globally across keys. Different keys may have tokens that do not reflect a total ordering.
* **CAS token width:** The CAS token MUST fit within an unsigned 64-bit integer (the standard Memcached CAS token range) to ensure interoperability with existing Memcached clients. Implementations MUST NOT emit tokens exceeding `2^64 - 1`.

10.3.4 Expiration

* Memcached expirations are mapped to `kv_ttl` and deterministic ticks. Absolute expiry times are converted to lease-like deadlines but MUST still be applied only during deterministic tick processing.
* **Expiry bounds:** Expiry times exceeding operator-defined maximum TTL bounds (see §5.2) MUST be clamped to the maximum or rejected with `SERVER_ERROR expiry exceeds maximum TTL`. The chosen behavior is implementation-defined but MUST be consistent within a deployment.

11. Errors and Protocol Mapping

**Error identifier stability policy:** Error identifiers marked as "stable string constant" (e.g., `TxnCrossShardUnsupported`, `LinearizabilityUnavailable`) are part of the API contract and MUST NOT be renamed or removed without a major version bump. Clients MAY depend on exact string matching for these identifiers. Error identifiers not explicitly marked as stable are implementation details and MAY change across minor versions.

11.1 Common Conditions

* `dirty_epoch`: request references an older kv_epoch/routing epoch or is routed to the wrong KPG.
* `LinearizabilityUnavailable` (stable string constant): LIN-BOUND failure due to Clustor read gate failure, strict fallback, or cache staleness/expiry.
* `ThrottleEnvelope`: backpressure from Clustor flow control or per-tenant quotas.

11.2 Adapter Error Mapping (normative)

* etcd v3:

  * `dirty_epoch` → gRPC `FAILED_PRECONDITION` with structured details `{expected_epoch, observed_epoch}`.
  * `LinearizabilityUnavailable` → gRPC `UNAVAILABLE` with details `{reason: ControlPlaneUnavailable|StrictFallback|ProofMismatch}` aligned to Clustor.
  * `TxnCrossShardUnsupported` → gRPC `FAILED_PRECONDITION` with details `{reason: "TxnCrossShardUnsupported"}` (stable string constant).
* Redis:

  * `dirty_epoch` → `-MOVED` is NOT used (Redis Cluster is out of scope); instead return `-TRYAGAIN routing epoch changed`.
  * `LinearizabilityUnavailable` → `-TRYAGAIN linearizability unavailable`.
* Memcached:

  * `dirty_epoch` → `SERVER_ERROR routing epoch changed`.
  * `LinearizabilityUnavailable` → `SERVER_ERROR linearizability unavailable`.

12. Control Plane (CP-Raft)

12.1 Responsibilities

* Tenant manifests: KPG count, routing epoch, hash seed version, adapter enablement, command allowlists, TTL bounds.
* Quotas: per-tenant QPS/bytes/token budgets; enforced via ThrottleEnvelope mapping.
* PKI: tenant identities, cert bundles, RBAC roles, break-glass override policy (inherited from Clustor model).
* Feature gates: enabling RESP3, memcached binary protocol, scans, scripting, multi-KPG range fanout plans, and any future cross-shard transaction mechanism.

12.2 Caches and Freshness

* CP cache semantics follow Clustor cache states (Fresh/Cached/Stale/Expired).
* Any cache transition to Stale/Expired MUST:

  * Force strict fallback behavior per Clustor.
  * Cause LIN-BOUND to fail for linearizable operations.
  * Revoke any follower-read capabilities and any watch-start linearization guarantees.

13. Multi-Tenant Isolation and Quotas

* **Namespaces:** Tenants are isolated by keyspace and identity. No cross-tenant reads/writes unless CP-Raft defines an explicit bridge policy (out of scope for v0.1).
* **Quota enforcement:**

  * Token buckets per tenant for requests and bytes.
  * Sustained overage beyond `quota.overage_grace_ms` triggers disconnects for Redis/Memcached and request failures for etcd.
* **Noisy neighbor controls:** Lattice MUST integrate Clustor flow control telemetry and throttle reasons and surface them per adapter.

14. Observability and Operations

* Metrics namespaces: `lattice.kv.*`, `lattice.adapter.etcd.*`, `lattice.adapter.redis.*`, `lattice.adapter.memcached.*`, `lattice.quota.*`, plus inherited `clustor.*` metrics.
* Required telemetry fields:

  * `lin_bound.can_linearize`, `lin_bound.failed_clause`, `read_gate.*` (mirrors Clustor), `kv.revision`, `kv.compaction_floor_revision`, `ttl.expiry_queue_depth`, `watch.active_streams`, `lease.active_leases`, `adapter.<name>.requests_total`, `adapter.<name>.errors_total`, `quota.throttle_total`, `dirty_epoch_total`.
* Readiness: `/readyz` MUST include tenant routing epoch cache age, adapter enablement states, and a digest of the active tenant manifest used for routing.

15. Disaster Recovery and Upgrades

* DR is performed via Clustor snapshot export/import. Under unfenced DR, Lattice MUST treat linearizable guarantees as unavailable until fenced promotion completes and Clustor read gates pass.
* Rolling upgrades follow Clustor guidance: drain listeners, transfer KPG leadership, upgrade, rejoin.
* Compatibility: adapters must negotiate protocol versions conservatively; any feature requiring new semantics must be gated in CP-Raft and exposed via readiness and telemetry.

Appendix A: Adapter Conformance (Non-Normative)

* Recommended conformance suites:

  * etcd v3: linearizable get/put/txn/watch/lease behaviors under healthy and strict fallback conditions; explicit failure modes for LIN-BOUND.
  * Redis: command subset correctness, pipelining order, transaction single-KPG constraint, deterministic TTL.
  * Memcached: CAS token mapping, expiry behavior, failure mapping under strict fallback.
* Fault injection: CP-Raft cache stale/expired transitions, leader changes, disk latency spikes, snapshot import under load, routing epoch flips, and quota throttling.

Appendix B: Example Mappings (Non-Normative)

B.1 Memcached CAS token example (ASCII protocol)

* This example uses the Memcached ASCII protocol (§10.3.1). Binary protocol representations differ but MUST preserve the same CAS semantics.
* `gets k` returns `VALUE k 0 3 42\r\nfoo\r\nEND\r\n` where the final token (`42` in this illustrative example) encodes the current `mod_revision` (format is implementation-defined but MUST be a lossless unsigned 64-bit representation, and MUST be stable and monotone per key). The token value shown is illustrative; no specific values are reserved or have sentinel semantics.
* `cas k 0 0 3 42\r\nbar\r\n` succeeds only if the key's current `mod_revision` equals the provided token at a linearizable fence.

B.2 Redis linearizability failure example

* `GET k` under strict fallback → `-TRYAGAIN linearizability unavailable\r\n`
* `TTL k` under snapshot-only allowance → returns a best-effort TTL, and `INFO` indicates `ttl_semantics=snapshot_only`.

B.3 etcd Txn single-KPG enforcement example

* A Txn comparing `keyA` and `keyB` that hash to different KPGs → `FAILED_PRECONDITION` with details `{ "reason": "TxnCrossShardUnsupported" }`.
