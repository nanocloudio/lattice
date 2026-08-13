# Limit register

The list of lattice's *deliberate* hard ceilings, following the same
discipline as fluxor's and clustor's `limit_register.md`: a policy
ceiling found in source but absent here is a bug, and every checkable
row is machine-verified against source by the harness suite
`tests/harness/tests/limit_register.rs` (the `Symbol` must exist in
`Source` as a `const` whose value matches `Value`).

## Deliberate caps

| Cap | Symbol | Source | Value | Reason |
|---|---|---|---|---|
| State-worker scratch (KV frames, snapshot export/import budget) | `SCRATCH_BUF_SIZE` | modules/app/kv_state_worker/mod.rs | 8192 | Policy: bounds every per-step frame copy AND (minus the app-snapshot header) the snapshot body the worker will export or accumulate. This is the ceiling that binds FIRST on the app-snapshot round-trip (clustor's `MAX_SNAPSHOT_BODY` is 16 KiB). Disk stores never approach it — captures answer with a 40 B disk-resident marker; only memory-store bodies are size-bound, and an over-budget encode is REFUSED (no chunks, WAL stays authoritative) and counted on the worker's denial counter. Upgrade path: deployment-envelope-sized elastic buffer (fluxor `rfc_resource_model.md` §3.6), not a bigger const. |
| Snapshot export chunk | `SNAPSHOT_CHUNK_MAX` | modules/app/kv_state_worker/mod.rs | 4096 | Wire pacing: one capture chunk per channel frame; totals stream, so the chunk size bounds frames, not bodies. |
