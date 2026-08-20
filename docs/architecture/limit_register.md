# Limit register

The list of lattice's *deliberate* hard ceilings, following the same
discipline as fluxor's and clustor's registers: a policy ceiling
found in source but absent here is a bug. Every checkable row's
`Symbol` must exist in its `Source` file as a `const` whose value
matches `Value`; a ceiling changed in source without updating this
register is a defect. A `Value` of `—` marks a row that is
documented but not value-checkable (the constant is not written as a
plain integer).

Pacing and backpressure bounds (batch sizes, rings that overwrite,
windows that defer work to the next tick) are not registered: they
shape throughput but never refuse work.

## Deliberate caps

| Cap | Symbol | Source | Value | Reason |
|---|---|---|---|---|
| State-worker scratch (KV frames, snapshot export/import budget) | `SCRATCH_BUF_SIZE` | modules/app/kv_state_worker/mod.rs | 8192 | Policy: bounds every per-step frame copy AND (minus the app-snapshot header) the snapshot body the worker will export or accumulate. Disk stores never approach it — captures answer with a small disk-resident marker; only memory-store bodies are size-bound, and an over-budget encode is REFUSED (no chunks, the WAL stays authoritative) and counted on the worker's denial counter. Upgrade path: a deployment-envelope-sized elastic buffer, not a bigger const. |
| App-snapshot body on the round-trip | `SNAPSHOT_BODY_MAX` | modules/app/kv_state_worker/mod.rs | — | Derived: `SCRATCH_BUF_SIZE − APP_SNAPSHOT_HDR` (8192 − 28 = 8164 bytes). This is the ceiling that binds FIRST on the app-snapshot round-trip: the substrate's own body cap is wider, so the worker's transfer buffer is the tighter constraint, and the binding ceiling is registered here. |
| Snapshot export chunk | `SNAPSHOT_CHUNK_MAX` | modules/app/kv_state_worker/mod.rs | 4096 | Wire pacing: one capture chunk per channel frame; totals stream, so the chunk size bounds frames, not bodies. |
