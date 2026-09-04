//! Wire format helpers for Lattice inter-module channel messages.
//!
//! Lattice reuses Clustor's 3-byte envelope format:
//!   [msg_type: u8] [len: u16 LE] [payload: len bytes]
//!
//! Lattice MSG_* constants live in the 0xC0..=0xEF range to avoid
//! colliding with Clustor's (0x01..=0x6F) and Quantum's (0x90..=0xBF)
//! ranges. See ~/Development/nanocloudio/standards/wire-registry.md once
//! the registry stabilises.
//!
//! ## Stability — DRAFT
//!
//! Every `MSG_*` constant and payload layout in this file is treated as
//! draft until the Lattice facade in `modules/common/replica_facade.rs`
//! is promoted to v1. External consumers MUST go through the facade,
//! not these constants. Numeric ids and payload field orders may change
//! without notice.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules; each consumer uses a subset of the surface so single-module rustc invocations see unused items"
)]

// ── Envelope header ────────────────────────────────────────────────────

/// 3-byte envelope header: `[msg_type:u8][len:u16 LE]`. Total wire
/// frame is 3 bytes header + `len` bytes payload.
pub const ENVELOPE_HDR: usize = 3;

// ── KV path (router ↔ workers ↔ anchors) ───────────────────────────────
//
// Field discipline across the four KV envelopes:
//   - `corr_id` is opaque to every hop except the originating anchor;
//     anchors use it as the inflight key in their per-slot reply path.
//   - `conn_id` rides every hop so the response path is anchor-slot
//     identifiable without lookups. The worker treats it as opaque.
//   - `kpg_id` enters at the router (it's the routing decision) and
//     is preserved by the worker on its reply.
//   - `tenant_id` is needed by the router for quota/auth admission;
//     the worker only needs it for log/diagnostic context, so we keep
//     it in COMMAND but drop it from APPLIED (recoverable from inflight).

/// Anchor → router. Carries the original request as parsed at the
/// edge, including the protocol of origin so the router knows which
/// output port to send the eventual response on.
/// Payload: `[corr_id:u64 LE][protocol:u8][tenant_id:u32 LE]
///           [conn_id:u8][consistency:u8][op:u8][body_len:u16 LE][body…]`
pub const MSG_KV_REQUEST: u8 = 0xC0;

/// Router → anchor. The router preserves `corr_id` from the request
/// verbatim and stamps `conn_id` back in so the anchor can look up the
/// originating slot without decoding tricks. Body shape is
/// protocol-neutral; each anchor maps `result` + body to its protocol
/// reply format (RESP, etcd v3, memcached ASCII).
/// Payload: `[corr_id:u64 LE][conn_id:u8][result:u8][revision:u64 LE]
///           [body_len:u16 LE][body…][fence tail]`
///
/// The fence tail is `[applied_index:u64 LE][applied_term:u64 LE]
/// [source_id:u32 LE][durability:u8][catalog_generation:u64 LE]
/// [commit_frontier:u64 LE]` — [`KV_RESPONSE_FENCE_TAIL_LEN`] bytes,
/// always present.
///
/// It sits AFTER the body rather than in the head because every one of
/// the fifteen consumers reads `body_len` at offset 18 and slices
/// `[20..20+body_len]`, checking `payload_len >= 20 + body_len` — a
/// bound, not an equality. Appending is therefore invisible to all of
/// them, while widening the head would move `body_len` and break every
/// one at once.
///
/// It exists because RFC §14.8 requires a `lattice.data` response to
/// carry "applied index and source-aware consistency/durability
/// fences", and this envelope is what `lattice_data_anchor` has to
/// build one out of. `revision` alone cannot: it is the MVCC position
/// of the answer, not evidence about which authority produced it or
/// what it managed to make durable.
///
/// What the router can honestly fill, and nothing beyond it:
///
/// - `applied_index` / `applied_term` — the worker's applied position,
///   which the router already tracks for the linearizable-read fence
///   (`MSG_APP_APPLIED_POS` carries both).
/// - `source_id` — the partition group that answered, from the
///   `MSG_KV_APPLIED` reply. §21 invariant 11 wants identity travelling
///   with the epoch, and this is the identity half.
/// - `durability` — [`crate::db_context::Durability`]'s byte. A
///   consensus-path write reports `ReplicatedVolatile`: a committed
///   Raft entry IS replicated to a quorum, and the router does not
///   observe the fsync proof that would justify `ReplicatedDurable`.
///   Everything else reports `Volatile`. Reads make nothing durable.
/// - `catalog_generation` — RFC §14.2's schema generation, counted by
///   the worker (see `kv_state_worker`) and relayed from
///   `MSG_KV_APPLIED`. It rides the fence tail because it answers the
///   same shape of question the rest of the tail does: not "what is the
///   answer" but "what was true when the answer was produced".
///   `relational_executor` uses it to decide whether a cached table
///   name -> id mapping was still valid AT THE MOMENT the read
///   executed, which is the only instant that makes the answer sound.
pub const MSG_KV_RESPONSE: u8 = 0xC1;

/// Size of [`MSG_KV_RESPONSE`]'s trailing fence record.
pub const KV_RESPONSE_FENCE_TAIL_LEN: usize = 8 + 8 + 4 + 1 + 8 + 8;

/// Router → worker. Stamps the routing decision as `kpg_id` and carries
/// the §23 canonical identity `(tenant, database, keyspace)` the worker
/// scopes the store to. `route_epoch` is omitted in Phase 1 because
/// there is only one KPG; when multi-KPG lands, append `route_epoch:u32
/// LE` at the tail. The head layout is owned by [`KvCommandHead`] —
/// build and parse it there, never by hand, or the router and worker
/// drift (this head grew from 15 to 27 bytes when identity landed).
/// Payload: `[corr_id:u64 LE][kpg_id:u16 LE][conn_id:u8][consistency:u8]
///           [op:u8][tenant:u32 LE][database:u32 LE][keyspace:u32 LE]
///           [body_len:u16 LE][body…]`
pub const MSG_KV_COMMAND: u8 = 0xC2;

/// Worker → router. Carries the deterministic result and the new
/// revision; preserves `corr_id`, `kpg_id`, and `conn_id` from the
/// inbound COMMAND so the router can re-protocol the reply without
/// needing its own inflight table for those fields.
/// Payload: `[corr_id:u64 LE][kpg_id:u16 LE][conn_id:u8][result:u8]
///           [revision:u64 LE][body_len:u16 LE][body…]
///           [catalog_generation:u64 LE]`
///
/// The generation trails the body for the same reason the fence tail
/// trails MSG_KV_RESPONSE's: the router reads `body_len` at a fixed
/// offset and slices, so appending was invisible to it until it was
/// taught to look.
pub const MSG_KV_APPLIED: u8 = 0xC3;

// ── Relational path (connector ↔ executor) ─────────────────────────────
//
// The connectors send SQL TEXT and receive TYPED rows. Two boundaries
// are being drawn deliberately:
//
//   - The connector does not parse. §14.2 makes parsing and binding a
//     SHARED relational module, so `pg_edge_anchor` and
//     `mysql_edge_anchor` both ship the statement text and let one
//     parser decide what it means. Two parsers would eventually
//     disagree and nothing would surface the disagreement.
//   - The executor does not format. §14.3 gives the connector its own
//     "result encoding", and the two genuinely differ (PostgreSQL
//     renders a boolean `t`/`f`, MySQL `1`/`0`), so values travel typed
//     and each connector renders them.

/// Connector → executor: one statement to execute.
/// Payload: `[corr_id:u64 LE][dialect:u8][tenant_id:u32 LE][conn_id:u8]
///           [sql_len:u16 LE][sql…]`
/// See `modules/common/sql_exec.rs` for the codec.
pub const MSG_SQL_REQUEST: u8 = 0xC4;

/// Executor → connector: the outcome, and the result set when there is
/// one.
/// Payload: `[corr_id:u64 LE][conn_id:u8][outcome:u8][tag:u8]
///           [affected:u64 LE][col_count:u16 LE]`
///          then per column `[type:3][name_len:u8][name…]`
///          then `[row_count:u16 LE]` and per cell
///          `[len:u32 LE | 0xFFFFFFFF = NULL][value…]`
///
/// `corr_id` and `conn_id` come back verbatim so the connector finds the
/// originating slot without a lookup, exactly as on the KV path.
pub const MSG_SQL_RESPONSE: u8 = 0xC5;

/// relational_executor → txn_coordinator: submit a cross-range write set
/// for two-phase commit. The executor reaches this after the router
/// refuses its single-range `KV_OP_TXN` with `KV_RESULT_CROSS_RANGE`. The
/// executor forwards the SAME `KV_OP_TXN` body it built (a set of
/// mod-revision comparisons and a set of PUTs); the coordinator splits it
/// per row — one participant per (comparison, PUT) pair — and drives
/// §13.2 with non-targeted participant ops the router routes by key, so
/// the coordinator needs no partition map. It answers
/// `MSG_TXN_SUBMIT_RESULT` with the outcome.
///
/// Payload: `[client_corr:u64 LE][proto:u8][conn:u8][read_ts:u64 LE]
///           [txn_body_len:u16 LE][txn_body…]`, where `txn_body` is a
/// `KV_OP_TXN` body: `[cmp_count:u16][cmps…][then_count:u16][puts…]
/// [else_count:u16][elses…]`. The initial coordinator handles the
/// INSERT shape — `cmp_count == then_count`, each comparison a
/// `TXN_CMP_MOD_EQUAL` absence check paired by order with its PUT.
pub const MSG_TXN_SUBMIT: u8 = 0xC6;

/// txn_coordinator → relational_executor: the outcome of a submitted
/// cross-range transaction. `client_corr`/`proto`/`conn` echo the submit
/// so the executor finds the originating statement.
/// Payload: `[client_corr:u64 LE][proto:u8][conn:u8][outcome:u8]`
/// where outcome: 0 = committed, 1 = aborted (a comparison failed —
/// duplicate key), 2 = error (fault; no decision reached).
pub const MSG_TXN_SUBMIT_RESULT: u8 = 0xC7;

/// Outcome bytes for `MSG_TXN_SUBMIT_RESULT`.
pub const TXN_SUBMIT_COMMITTED: u8 = 0;
pub const TXN_SUBMIT_ABORTED: u8 = 1;
pub const TXN_SUBMIT_ERROR: u8 = 2;

/// placement_advisor → range_supervisor: execute a split at
/// `split_key` NOW. The advisor emits this only under an explicit
/// `auto_execute` opt-in (§12.4: execution stays operator-gated — the
/// operator both enables the automation and pre-declares the split key,
/// so the recommender never invents a split point). It automates the
/// *timing* (fire when load crosses the policy threshold), not the
/// judgement of where to split. The supervisor honours it only when
/// idle and configured to accept runtime commands (`op_kind = 4`).
/// Payload: `[split_key_len:u16 LE][split_key…]`.
pub const MSG_PLACEMENT_SPLIT_CMD: u8 = 0xCD;

/// Rebalancer → range_supervisor: relocate a range's replica set from
/// one node to another. Like the split command it carries only the
/// *decision*, not consensus: an ARMED op_kind-3 supervisor (no
/// `reloc_*` declaration, `cmd_in` wired) takes the move from this
/// command and runs the `RelocatePhase` machine against clustor's
/// membership. Payload:
/// `[partition_id:u16 LE][source_node:u8][target_node:u8]`.
pub const MSG_PLACEMENT_RELOCATE_CMD: u8 = 0xCE;

// ── Watch service (registry ↔ fanout ↔ anchor) ─────────────────────────

/// Anchor → registry: create/cancel/resume control.
/// Payload: `[ctrl:u8][session_id:u64 LE][session_epoch:u32 LE]
///           [tenant_id:u32 LE][kpg_id:u16 LE][filter_len:u16 LE][filter…]`
/// `ctrl`: 0=create, 1=resume, 2=cancel, 3=progress_notify.
pub const MSG_WATCH_CTRL: u8 = 0xC8;

/// Registry → anchor: control ack with assigned `session_id` /
/// `session_epoch` after worker placement.
pub const MSG_WATCH_CTRL_ACK: u8 = 0xC9;

/// kv_state_worker → registry / fanout: durable mutation event for fanout.
/// Payload: `[kpg_id:u16 LE][revision:u64 LE][op:u8]
///           [key_len:u16 LE][key…][value_len:u16 LE][value…]`
pub const MSG_WATCH_EVENT: u8 = 0xCA;

/// Fanout → anchor: framed watch response ready for client delivery.
/// Body is the protocol-native frame (etcd v3 WatchResponse, etc.).
pub const MSG_WATCH_FRAME: u8 = 0xCB;

/// Registry → fanout: replay-plan delta after worker rebind.
/// Payload: `[session_id:u64 LE][from_revision:u64 LE][to_revision:u64 LE]`
///          `[key_len:u16 LE][key…][range_end_len:u16 LE][range_end…]`
///
/// The KEY SPAN rides along because the fanout has no watch table — it
/// is deliberately transformation-free and holds no per-watch state.
/// Without the span it could only backfill by scanning the whole
/// keyspace and discarding, which turns one client's reconnect into a
/// full-database read. The registry already knows the span; carrying it
/// keeps the cost proportional to what the watcher actually watches.
///
/// `from_revision` is EXCLUSIVE and `to_revision` inclusive, with `0`
/// meaning "up to now" — the same window convention `KV_OP_SCAN_VERSIONS`
/// uses, because the fanout passes these straight through to it. The
/// registry sets `from` to the watch's `last_sent_revision`, so a
/// reconnect resumes immediately above what the client already has
/// rather than re-delivering the last event it saw.
pub const MSG_WATCH_REPLAY_PLAN: u8 = 0xCC;

// ── Lease service (manager ↔ scheduler ↔ anchor) ───────────────────────

/// Anchor → manager: grant / revoke / keepalive control.
/// Payload: `[ctrl:u8][lease_id:u64 LE][ttl_ms:u32 LE][tenant_id:u32 LE]`
/// `ctrl`: 0=grant, 1=revoke, 2=keepalive, 3=time_to_live.
pub const MSG_LEASE_CTRL: u8 = 0xD0;

/// Manager → anchor: committed lease state response.
/// Payload: `[lease_id:u64 LE][session_epoch:u32 LE]
///           [ttl_ms:u32 LE][granted_at_ms:u64 LE]
///           [keepalive_deadline_ms:u64 LE]`
pub const MSG_LEASE_STATE: u8 = 0xD1;

/// The cluster clock: a committed tick carrying the current time.
/// Payload: `[tick_ms:u64 LE]`
///
/// `ttl_scheduler` proposes one on a cadence; it reaches consumers only
/// after it commits, wrapped in [`LATTICE_RECORD_TAG`] in a replicated
/// graph and emitted directly onto the command channel in a graph with
/// no replication. `kv_state_worker` reads it in-band on `commands`, so
/// it is ordered against the mutations around it, and `lease_manager`
/// reads it via the scheduler's `tick_out`. Nothing downstream of this
/// message samples a local timer for a TTL decision.
pub const MSG_LEASE_TICK: u8 = 0xD2;

/// Manager → kv_state_worker: lease-attached key revoke.
/// Payload: `[lease_id:u64 LE][kpg_id:u16 LE]
///           [key_len:u16 LE][key…]`
pub const MSG_LEASE_REVOKE: u8 = 0xD3;

/// Substrate `control_plane` → session-bearing app modules
/// (`watch_registry`, `lease_manager`, `kv_state_worker`): the
/// cluster's placement epoch has advanced. Every session in every
/// downstream module bumps its own `session_epoch` so stale frames
/// in flight get fenced.
///
/// Payload (8 bytes): `[prev_epoch:u32 LE][new_epoch:u32 LE]`.
///
/// Byte-compatible with clustor's `control_plane.epoch_events`
/// output (see `deps/clustor/modules/app/control_plane/mod.rs`,
/// which writes this with `wire::channel_write_msg(..., 0xD4, ...)`).
/// The first event fires on placement-router init as a
/// `0 → 1` transition; subsequent events advance monotonically
/// whenever the substrate detects a rebalance / admin change.
pub const MSG_PLACEMENT_EPOCH_EVENT: u8 = 0xD4;

/// `kv_state_worker` → `ttl_scheduler`: a KV record's expiry deadline.
/// Payload: `[deadline_ms:u64 LE][key_hash:u64 LE]`, deadline `0` =
/// cancel any pending entry for the key.
///
/// The deadline is computed from the committed clock at apply time, so
/// every replica registers the same value from the same log position.
/// The entry schedules RECLAMATION only — whether a record is visible
/// is decided by its own stored deadline when a command touches it, so
/// a lost, late, or duplicated registration changes when a slot is
/// released and nothing a client can observe. For the same reason a
/// write that carries no TTL sends nothing: a stale entry left behind
/// fires a sweep that frees nothing.
pub const MSG_TTL_REGISTER: u8 = 0xD5;

/// `kv_state_worker` → `ttl_scheduler`: resume the clock at this
/// frontier. Payload: `[frontier_ms:u64 LE]`.
///
/// Sent once after a snapshot install, ahead of the re-registrations
/// that rebuild the queue. A snapshot restores the records and the
/// clock they were deadlined against (see
/// `kv_store::snapshot_clock_ms`), but the scheduler's own `now_ms`
/// and its expiry queue live only in its arena, so without this it
/// would resume from ZERO and propose ticks far below the frontier the
/// worker just restored — every one of them discarded as backwards,
/// freezing expiry for as long as the previous incarnation had been up.
///
/// This is a HINT and cannot be anything else: it only moves what the
/// scheduler PROPOSES, and a proposal becomes the time only once it
/// comes back committed. A replica that misses it proposes low values
/// that are discarded, which is the same outcome as not restarting.
pub const MSG_TTL_CLOCK_RESUME: u8 = 0xD6;

// ── Clustor consensus bridge (Phase 7) ────────────────────────────────
//
// Byte-compatible with clustor's `gateway.client_requests` ingress (see
// `clustor/modules/common/wire.rs::MSG_CLIENT_PROPOSAL`). When the
// Phase 7 proposal adapter is wired (`kv_request_router.proposal_out`
// → `gateway.client_requests`), each write KV_COMMAND gets re-emitted as
// a `MSG_CLIENT_REQUEST` envelope. the gateway codec parses the wrapper,
// stamps a correlation_id, and forwards `MSG_CLIENT_PROPOSAL` into
// the Raft ingestion pipeline. consensus → lattice_apply_bridge
// then drives the deterministic worker on the committed copy.
//
// Payload: `[conn_id:u8][kv_command_payload…]` where the body is the
// raw MSG_KV_COMMAND payload (15-byte head + op body, no 3-byte
// envelope prefix — clustor's outer framing supplies that).
pub const MSG_CLIENT_PROPOSAL: u8 = 0x10;

// MVCC timestamp-lease opcodes and grant-frame width, shared by the
// timestamp allocator, the apply bridge, and the state worker. Mirror the
// canonical protocol definitions in `mvcc.rs` (mounted by the allocator);
// modules that do not mount `mvcc.rs` reference these rather than inlining
// the byte. The grant frame is `[corr_id:u64][TimestampLease:40]`.
pub const MSG_TS_LEASE_REQUEST: u8 = 0xE3;
pub const MSG_TS_LEASE_GRANT: u8 = 0xE4;
pub const TS_LEASE_GRANT_WIRE_LEN: usize = 48;

// ── Gateway reject feedback (clustor → router) ────────────────────────
//
// **Byte-identical to `clustor/modules/common/wire.rs` by
// requirement** (`MSG_CLIENT_REJECT`, `CLIENT_REJECT_NOT_LEADER`),
// inlined like `MSG_CLIENT_PROPOSAL` above to avoid build-time coupling
// to a sibling crate's source path.
//
// The gateway checks leadership BEFORE assigning a correlation id
// (`gateway/codec.rs`: the NOT_LEADER return precedes `next_corr_id`),
// so a rejected proposal is refused pre-assignment and the reject can
// only name the CONN, never a corr_id. The router therefore correlates
// rejects by conn, not by op — see `drain_gateway_rejects` for how.
//
// Frame on `gateway.responses` (per `gateway/surface.rs::send_response`
// — NOT the standard 3-byte envelope):
//   `[conn_id:u8][msg_type:u8][len:u16 LE][payload…]`
// For `MSG_CLIENT_REJECT` the payload is `CLIENT_REJECT_BODY_LEN`
// bytes: `[status:u8][reserved:u8][retry_after_ms:u16 LE]
// [entry_credits:i16 LE][byte_credits:i32 LE]`, where `reserved`
// carries the believed LEADER ID when status is NOT_LEADER — the hint
// a failover-capable client needs.
pub const MSG_CLIENT_REJECT: u8 = 0x15;
pub const CLIENT_REJECT_NOT_LEADER: u8 = 0x02;
pub const CLIENT_REJECT_BODY_LEN: usize = 10;

/// Leading byte of every lattice-replicated Raft entry body.
///
/// REQUIRED FOR CORRECTNESS — not a version field. Clustor's consensus
/// infers an entry's *type* by sniffing the first byte of the committed
/// body (`clustor/modules/app/consensus/apply.rs`, `emit_committed_entry`):
///
///   `0xAD` (`ADMIN_MARKER`)         → applied as an admin command
///   `0xCC` (`CONFIG_CHANGE_MARKER`) → applied as a Raft config change
///
/// The body it sniffs is documented as opaque application data, and
/// lattice's `MSG_KV_COMMAND` payload begins with `[corr_id:u64 LE]` —
/// a dense counter. Its low byte therefore cycles through every value
/// each 256 writes, so an unprefixed lattice cluster hands consensus a
/// forged admin command on write 173 and a forged config change on
/// write 204, and on write 460 the C_new path removes the node from its
/// own voter set. Writes then stop permanently while reads keep serving
/// from local state. Verified on the pi5-a rig 2026-07-27: `[raft]
/// removed self` at exactly the 460th SET, reproducible, and
/// independent of request rate, credit wiring and `segment_bytes`.
///
/// Prefixing the body with a constant that is neither marker makes the
/// collision structurally impossible. `lattice_apply_bridge` strips this
/// byte before re-emitting `MSG_KV_COMMAND` to the worker.
///
/// The real fix belongs upstream — a log entry needs an explicit type
/// field rather than a sniffed prefix — but that is a clustor WAL
/// format change. This keeps lattice correct against clustor as it is.
pub const LATTICE_ENTRY_TAG: u8 = 0x4C; // 'L'

/// Leading byte of a lattice-replicated **record** entry body — the
/// second entry kind next to [`LATTICE_ENTRY_TAG`], for replicated
/// control records that are NOT KV commands and must not reach the
/// state machine (timestamp-allocator leases, GC floor records).
///
/// Body layout after the tag: `[record_msg_type:u8][record bytes…]`.
/// `lattice_apply_bridge` demuxes on this tag and re-emits the record
/// on its `records_out` port as an ordinary `[record_msg_type]`
/// envelope, in commit order, exactly once per committed entry — which
/// is what lets `timestamp_allocator.committed_state` (and later the
/// GC floor loop) be fed from the same committed log as everything
/// else, replay included, with no second durability channel.
///
/// Same constraint as [`LATTICE_ENTRY_TAG`]: the value must be neither
/// of clustor's sniffed markers (`0xAD`, `0xCC`).
pub const LATTICE_RECORD_TAG: u8 = 0x52; // 'R'

// ── Linearizable-read fence (ReadIndex, RFC §1.3 / spec Phase 5) ──────
//
// Byte-compatible with clustor's consensus read protocol. The
// router SUBMITS a fence request carrying only the correlation id
// (`MSG_CLIENT_READ_REQUEST [corr_id:u64 LE]` — the stashed read body
// stays router-side), and consensus answers on its `applied`
// stream once leadership is probe-confirmed AND `apply_index` has
// reached the fence commit horizon with a fresh read permit:
//   MSG_CLIENT_READ_RESPONSE  [corr_id:u64]     → release the read
//   MSG_CLIENT_REJECT_INTERNAL [corr_id:u64]…   → LIN-BOUND fail-closed
// The same `applied` stream also carries per-entry MSG_CLIENT_RESPONSE
// write acks — a release drain must filter by msg type.
pub const MSG_CLIENT_RESPONSE: u8 = 0x11;
pub const MSG_CLIENT_READ_REQUEST: u8 = 0x16;
pub const MSG_CLIENT_REJECT_INTERNAL: u8 = 0x17;
pub const MSG_CLIENT_READ_RESPONSE: u8 = 0x18;

// ── Auth / quota / throttle ────────────────────────────────────────────

/// Anchor → auth_manager: principal mapping request.
/// Payload: `[protocol:u8][tenant_id:u32 LE][cred_len:u16 LE][cred…]`
pub const MSG_AUTH_REQUEST: u8 = 0xD8;

/// Auth_manager → anchor: decision (allow/deny + role token).
/// Payload: `[decision:u8][principal_len:u8][principal…]`
pub const MSG_AUTH_DECISION: u8 = 0xD9;

/// Quota_manager → router / anchor: throttle envelope.
/// Payload: `[tenant_id:u32 LE][reason:u8][retry_after_ms:u32 LE]`
pub const MSG_QUOTA_THROTTLE: u8 = 0xDA;

/// Quota_manager → anchor: connection disconnect signal (noisy neighbour).
/// Payload: `[conn_id:u8][reason:u8]`
pub const MSG_QUOTA_DISCONNECT: u8 = 0xDB;

// ── Compaction / retention ─────────────────────────────────────────────

/// Module → compaction_coordinator: declare a retention floor below
/// which compaction must not advance.
/// Payload: `[source:u8][kpg_id:u16 LE][floor_revision:u64 LE]`
pub const MSG_RETENTION_FLOOR: u8 = 0xE0;

/// Compaction_coordinator → durability: aggregated floor input.
/// Payload: `[kpg_id:u16 LE][floor_revision:u64 LE]`
pub const MSG_COMPACTION_FLOOR: u8 = 0xE1;

// ── MVCC GC floor (RFC §18, §21 invariant 13) — Phase-2 slice B ───────
//
// The 0xE0 operational band, continued. Taken before this block:
// 0xE0 retention floor, 0xE1 compaction floor, 0xE2 ttl map update,
// 0xE3/0xE4/0xE6/0xE7 timestamp-allocator lease traffic (see
// `modules/common/mvcc.rs`), 0xE5 applied position, 0xE8/0xE9 adapter
// metrics. 0xEA..0xEF were free; three are allocated here, and
// 0xED/0xEE carry the CDC pump↔sink frames (MSG_PUBLISH / MSG_ACK — see
// the fluxor SDK contract, source `modules/sdk/contracts/exchange.rs` in
// the fluxor repo, `deps/fluxor/` in this checkout).
//
// Why these exist when MSG_RETENTION_FLOOR / MSG_COMPACTION_FLOOR
// already do: those two are a purely LOCAL aggregation — a source
// declares a number, the coordinator takes a min, and whoever listens
// may act. §18 requires the opposite shape for GC. Reclaiming history
// is irreversible, so the floor it happens behind must be a
// REPLICATED DECISION: proposed, committed, and only then acted on.
// A locally-computed floor would let a leader that is about to lose
// leadership destroy versions the next leader still owes to a reader.
// The claim/propose/commit triple below is that decision path, and the
// worker's compaction is gated on the committed record alone.

/// Claim source → compaction_coordinator: a fenced retention claim
/// (§18's claim set). Richer than `MSG_RETENTION_FLOOR`, which carries
/// no identity and no freshness and therefore cannot distinguish "this
/// source has nothing to protect" from "this source is gone".
///
/// Payload (`GC_CLAIM_WIRE_LEN`, integers LE):
///
/// ```text
/// [version:u16][payload_len:u16]   // = GC_CLAIM_WIRE_LEN - 4
/// [kpg_id:u16][source:u8][rsvd:u8]
/// [claim_id:u64]                   // holder identity within `source`
/// [floor_revision:u64]             // oldest revision the holder needs
/// [expiry_unix_ms:u64]             // 0 = no expiry; else freshness bound
/// [seq:u32][rsvd2:u32]             // monotone per (source, claim_id)
/// ```
///
/// `expiry_unix_ms` is a LIVENESS bound, not an ordering input: a claim
/// past its expiry is STALE, and a stale required source BLOCKS
/// advancement rather than being read as "claims nothing" (§18: missing
/// or stale claim sources block unsafe advancement).
pub const MSG_RETENTION_CLAIM: u8 = 0xEC;

/// compaction_coordinator → consensus: PROPOSE a GC floor advance.
/// Payload: one encoded GC floor record (`GC_FLOOR_WIRE_LEN` bytes,
/// layout in `modules/common/compaction_floor.rs::GcFloorRecord`).
///
/// A proposal grants NOTHING. Nothing may be reclaimed on the strength
/// of it, and a coordinator that loses leadership between proposing and
/// committing simply never sees the confirmation — the same shape as
/// the timestamp allocator's `AwaitingCommit` (see `mvcc.rs`).
pub const MSG_GC_FLOOR_PROPOSE: u8 = 0xEA;

/// consensus (via `lattice_apply_bridge`) → kv_state_worker and
/// compaction_coordinator: a COMMITTED GC floor.
///
/// Payload is byte-identical to `MSG_GC_FLOOR_PROPOSE` by design: the
/// durable record IS the statement "versions below `floor` are no
/// longer owed to anyone", and it is the ONLY thing that authorizes
/// physical reclamation. `kv_state_worker` calls
/// `DiskStore::compact(floor)` for a floor it has seen in one of these
/// and for no other floor.
pub const MSG_GC_FLOOR_COMMITTED: u8 = 0xEB;

// ── App state-machine snapshot (backlog §61/62, clustor RFC §2.1) ──────
//
// The state half of a Raft snapshot. Without it a snapshot is a bare
// `(term, index)` manifest, so replay from a compaction floor cannot
// reconstruct KV state and compaction is only safe on a `skip_replay`
// bench rig.
//
// These opcodes and payload shapes are DEFINED BY CLUSTOR
// (`clustor/modules/common/wire.rs`, RFC §2.1) — lattice is one
// consumer of that contract alongside loam. They are mirrored here
// verbatim because lattice modules can't include clustor's wire.rs;
// they must not be renumbered independently.
//
//   durability → worker : MSG_APP_SNAPSHOT_REQUEST (capture now)
//   worker → durability : MSG_APP_SNAPSHOT_CHUNK   (encoded state)
//   durability → worker : MSG_APP_SNAPSHOT_DURABLE (export now persisted)
//   durability → worker : MSG_APP_SNAPSHOT_RESET   (discard state)
//                              then MSG_APP_SNAPSHOT_CHUNK stream
//
// A worker that cannot encode within its export budget stays SILENT
// rather than emitting a short body: no snapshot means compaction does
// not advance, which is safe, whereas a truncated body loses keys.

/// State-machine snapshot chunk. App → engine on the export path,
/// engine → app on the install path. Payload:
/// `[term:u64 LE][last_included_index:u64 LE][offset:u64 LE]
///  [done:u8][reserved:u8;3][body…]`, body = `kv_store::snapshot_encode`.
///
/// On the export path the (term, index) is the app's OWN applied
/// position — NOT an echo of the request. This is load-bearing: the
/// label must name exactly the entry whose effects the body contains.
/// A label behind the true state makes a restoring follower replay
/// entries already folded in (double-applying `INCR`); a label ahead
/// of it silently drops entries. The requested position is only a hint
/// about when to capture — by the time the app answers it may have
/// applied further, and the engine takes the body's label as
/// authoritative.
pub const MSG_APP_SNAPSHOT_CHUNK: u8 = 0x57;

/// durability → app: capture current state now.
/// Payload: `[term:u64 LE][last_included_index:u64 LE]`.
pub const MSG_APP_SNAPSHOT_REQUEST: u8 = 0x58;

/// durability → app: discard current state; a chunk stream for a
/// snapshot ahead of the app's apply position follows.
/// Payload: `[term:u64 LE][last_included_index:u64 LE]`.
pub const MSG_APP_SNAPSHOT_RESET: u8 = 0x59;

/// orchestrator → app (elastic split): capture
/// ONLY the keys in a half-open span and ship them as a `MSG_APP_SNAPSHOT_CHUNK`
/// stream, so a span can be moved to a demand-provisioned partition without
/// copying the whole store. Payload:
/// `[term:u64 LE][last_included_index:u64 LE][start_len:u16 LE][start…][end_len:u16 LE][end…]`
/// (bounds are STORED-key bytes; empty start = MIN, empty end = MAX). The
/// chunk stream and the target's install path are identical to the whole-store
/// snapshot — a span snapshot is a whole-store snapshot of just the span.
pub const MSG_APP_SNAPSHOT_SPAN_REQUEST: u8 = 0x5A;

/// durability → app: the exported snapshot at `last_included_index` is now
/// DURABLE (body written crash-atomically AND its boot pointer persisted).
/// Payload: `[term:u64 LE][last_included_index:u64 LE]`.
///
/// The acknowledgement a GC-snapshot-mode worker requires before it may
/// advance `gc_snapshot_revision` past `last_included_index`: local export
/// completion is NOT proof of durability, and a floor advanced onto a
/// not-yet-durable snapshot is read back as `Compacted` after a crash
/// (see `kv_store` replay). Leader-local, like the durable snapshot itself.
/// Clustor-defined (0x5B); mirrored here verbatim, not renumbered.
pub const MSG_APP_SNAPSHOT_DURABLE: u8 = 0x5B;

/// app → orchestrator: a snapshot install completed and the installed
/// state is resident and serving. Emitted on the state worker's
/// `install_ack_out` (unwired in most graphs); the elastic-split driver
/// gates its routing cutover on this ack rather than a wall-clock guess.
/// Payload: `[partition_id:u16 LE][applied_index:u64 LE]`.
pub const MSG_APP_SNAPSHOT_INSTALLED: u8 = 0x5C;

/// `MSG_APP_SNAPSHOT_CHUNK` fixed header size; body follows.
pub const APP_SNAPSHOT_HDR: usize = 28;

/// lattice_apply_bridge → kv_state_worker: "every command written to
/// this channel before this point is applied at (term, index)".
/// Payload: `[term:u64 LE][index:u64 LE]`.
///
/// Rides `kv_out` — the SAME channel as `MSG_KV_COMMAND` — precisely
/// so channel ordering does the synchronisation: the worker cannot
/// observe a position before the commands it covers. A side channel
/// would race and yield a label that doesn't match the state.
///
/// Emitted once per drained batch rather than per entry: the position
/// only has to be exact at the moments a snapshot could be taken, and
/// batch boundaries are such moments. That keeps the hot apply path
/// at one extra message per tick instead of one per commit.
pub const MSG_APP_APPLIED_POS: u8 = 0xE5;

// ── Adapter metrics fan-in ─────────────────────────────────────────────

/// Module → adapter_metrics: typed observability event.
/// Payload: `[event_kind:u8][tenant_id:u32 LE][value:u64 LE]`
pub const MSG_ADAPTER_EVENT: u8 = 0xE8;

/// Adapter_metrics → telemetry / http_surface: rollup blob.
/// Payload: opaque telemetry encoding (see fluxor `monitor` contract).
pub const MSG_ADAPTER_ROLLUP: u8 = 0xE9;

// ── KV envelope head codecs ────────────────────────────────────────────
//
// The fixed heads that precede an op body on the KV path. Each layout
// lives in exactly ONE place — a struct with `encode`/`decode` and a
// `LEN` — so a field added to a head changes every producer and consumer
// at once. Before this, the offsets were hand-written in ~19 modules,
// and the drift showed: `MSG_KV_COMMAND`'s head grew 15→27 bytes for the
// §23 identity while several comments still said 15.

/// The head of a [`MSG_KV_COMMAND`] payload — everything the router
/// stamps before the op body. Built by the router, parsed by the worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KvCommandHead {
    pub corr_id: u64,
    pub kpg_id: u16,
    pub conn_id: u8,
    pub consistency: u8,
    pub op: u8,
    /// §23 canonical identity the worker scopes the store to.
    pub tenant: u32,
    pub database: u32,
    pub keyspace: u32,
    /// MVCC commit timestamp for the command's writes.
    /// Stamped by the proposer from a `timestamp_allocator` lease
    /// BEFORE the command enters consensus, so it is part of the
    /// committed bytes and every replica materializes the same value.
    /// `0` = no allocator wired/established — versions then carry no
    /// timestamp, which the feed surface reports rather than hides.
    pub commit_ts: u64,
    pub body_len: u16,
}

impl KvCommandHead {
    /// Wire length of the head; the op body follows immediately.
    pub const LEN: usize = 8 + 2 + 1 + 1 + 1 + 4 + 4 + 4 + 8 + 2;

    /// Encode into `out[..LEN]`. `false` if `out` is too short.
    pub fn encode(&self, out: &mut [u8]) -> bool {
        if out.len() < Self::LEN {
            return false;
        }
        out[0..8].copy_from_slice(&self.corr_id.to_le_bytes());
        out[8..10].copy_from_slice(&self.kpg_id.to_le_bytes());
        out[10] = self.conn_id;
        out[11] = self.consistency;
        out[12] = self.op;
        out[13..17].copy_from_slice(&self.tenant.to_le_bytes());
        out[17..21].copy_from_slice(&self.database.to_le_bytes());
        out[21..25].copy_from_slice(&self.keyspace.to_le_bytes());
        out[25..33].copy_from_slice(&self.commit_ts.to_le_bytes());
        out[33..35].copy_from_slice(&self.body_len.to_le_bytes());
        true
    }

    /// Decode from `src[..LEN]`. `None` if `src` is too short.
    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < Self::LEN {
            return None;
        }
        Some(Self {
            corr_id: u64::from_le_bytes(src[0..8].try_into().ok()?),
            kpg_id: u16::from_le_bytes([src[8], src[9]]),
            conn_id: src[10],
            consistency: src[11],
            op: src[12],
            tenant: u32::from_le_bytes(src[13..17].try_into().ok()?),
            database: u32::from_le_bytes(src[17..21].try_into().ok()?),
            keyspace: u32::from_le_bytes(src[21..25].try_into().ok()?),
            commit_ts: u64::from_le_bytes(src[25..33].try_into().ok()?),
            body_len: u16::from_le_bytes([src[33], src[34]]),
        })
    }
}

/// The head of a [`MSG_KV_APPLIED`] payload — the worker's reply head.
/// The op body follows, then `[catalog_generation:u64 LE]` (not part of
/// this head — it trails the body so the router can slice by `body_len`
/// without knowing it is there).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KvAppliedHead {
    pub corr_id: u64,
    pub kpg_id: u16,
    pub conn_id: u8,
    pub result: u8,
    pub revision: u64,
    /// The range's commit-timestamp frontier: the
    /// highest MVCC commit timestamp this worker has bound to any
    /// applied write. Everything at or below it is applied; with
    /// in-band lease assignment the value is monotone in apply order.
    /// `0` = no timestamp authority established yet.
    pub commit_frontier: u64,
    pub body_len: u16,
}

impl KvAppliedHead {
    pub const LEN: usize = 8 + 2 + 1 + 1 + 8 + 8 + 2;

    pub fn encode(&self, out: &mut [u8]) -> bool {
        if out.len() < Self::LEN {
            return false;
        }
        out[0..8].copy_from_slice(&self.corr_id.to_le_bytes());
        out[8..10].copy_from_slice(&self.kpg_id.to_le_bytes());
        out[10] = self.conn_id;
        out[11] = self.result;
        out[12..20].copy_from_slice(&self.revision.to_le_bytes());
        out[20..28].copy_from_slice(&self.commit_frontier.to_le_bytes());
        out[28..30].copy_from_slice(&self.body_len.to_le_bytes());
        true
    }

    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < Self::LEN {
            return None;
        }
        Some(Self {
            corr_id: u64::from_le_bytes(src[0..8].try_into().ok()?),
            kpg_id: u16::from_le_bytes([src[8], src[9]]),
            conn_id: src[10],
            result: src[11],
            revision: u64::from_le_bytes(src[12..20].try_into().ok()?),
            commit_frontier: u64::from_le_bytes(src[20..28].try_into().ok()?),
            body_len: u16::from_le_bytes([src[28], src[29]]),
        })
    }
}

/// The head of a [`MSG_KV_RESPONSE`] payload — the router's reply head.
/// The op body follows, then a [`FenceTail`] (always present).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KvResponseHead {
    pub corr_id: u64,
    pub conn_id: u8,
    pub result: u8,
    pub revision: u64,
    pub body_len: u16,
}

impl KvResponseHead {
    pub const LEN: usize = 8 + 1 + 1 + 8 + 2;

    pub fn encode(&self, out: &mut [u8]) -> bool {
        if out.len() < Self::LEN {
            return false;
        }
        out[0..8].copy_from_slice(&self.corr_id.to_le_bytes());
        out[8] = self.conn_id;
        out[9] = self.result;
        out[10..18].copy_from_slice(&self.revision.to_le_bytes());
        out[18..20].copy_from_slice(&self.body_len.to_le_bytes());
        true
    }

    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < Self::LEN {
            return None;
        }
        Some(Self {
            corr_id: u64::from_le_bytes(src[0..8].try_into().ok()?),
            conn_id: src[8],
            result: src[9],
            revision: u64::from_le_bytes(src[10..18].try_into().ok()?),
            body_len: u16::from_le_bytes([src[18], src[19]]),
        })
    }
}

/// The fence tail that trails every [`MSG_KV_RESPONSE`] body — the proof
/// the answering group carried (§14.8), plus the schema generation the
/// compute-side catalog cache validates against. It trails the body (not
/// the head) so a consumer reading only `body_len` never has to know it
/// is there; [`KV_RESPONSE_FENCE_TAIL_LEN`] is its width. This is the
/// structure that drifted twice — applied index/durability, then
/// `catalog_generation` — which is why it is owned here now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FenceTail {
    pub applied_index: u64,
    pub applied_term: u64,
    pub source_id: u32,
    pub durability: u8,
    pub catalog_generation: u64,
    /// The answering range's commit-timestamp frontier,
    /// copied from [`KvAppliedHead::commit_frontier`]. Feed consumers
    /// read the resolved frontier from here on every response.
    pub commit_frontier: u64,
}

impl FenceTail {
    pub const LEN: usize = KV_RESPONSE_FENCE_TAIL_LEN;

    pub fn encode(&self, out: &mut [u8]) -> bool {
        if out.len() < Self::LEN {
            return false;
        }
        out[0..8].copy_from_slice(&self.applied_index.to_le_bytes());
        out[8..16].copy_from_slice(&self.applied_term.to_le_bytes());
        out[16..20].copy_from_slice(&self.source_id.to_le_bytes());
        out[20] = self.durability;
        out[21..29].copy_from_slice(&self.catalog_generation.to_le_bytes());
        out[29..37].copy_from_slice(&self.commit_frontier.to_le_bytes());
        true
    }

    pub fn decode(src: &[u8]) -> Option<Self> {
        if src.len() < Self::LEN {
            return None;
        }
        Some(Self {
            applied_index: u64::from_le_bytes(src[0..8].try_into().ok()?),
            applied_term: u64::from_le_bytes(src[8..16].try_into().ok()?),
            source_id: u32::from_le_bytes(src[16..20].try_into().ok()?),
            durability: src[20],
            catalog_generation: u64::from_le_bytes(src[21..29].try_into().ok()?),
            commit_frontier: u64::from_le_bytes(src[29..37].try_into().ok()?),
        })
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Write a 3-byte envelope header at `out[0..3]`. Returns true on
/// success, false if `len > u16::MAX` or `out.len() < 3`.
pub fn write_envelope_hdr(out: &mut [u8], msg_type: u8, len: usize) -> bool {
    if out.len() < ENVELOPE_HDR || len > u16::MAX as usize {
        return false;
    }
    out[0] = msg_type;
    let len16 = len as u16;
    out[1] = (len16 & 0xFF) as u8;
    out[2] = (len16 >> 8) as u8;
    true
}

/// Parse a 3-byte envelope header. Returns `Some((msg_type, payload_len))`
/// on success, `None` if `bytes.len() < 3`.
pub fn read_envelope_hdr(bytes: &[u8]) -> Option<(u8, usize)> {
    if bytes.len() < ENVELOPE_HDR {
        return None;
    }
    let msg_type = bytes[0];
    let len = u16::from_le_bytes([bytes[1], bytes[2]]) as usize;
    Some((msg_type, len))
}

/// Read a little-endian u64 starting at `bytes[offset]`. Returns
/// `None` on short input.
pub fn read_u64_le(bytes: &[u8], offset: usize) -> Option<u64> {
    if bytes.len() < offset + 8 {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[offset..offset + 8]);
    Some(u64::from_le_bytes(buf))
}

/// Read a little-endian u32 starting at `bytes[offset]`.
pub fn read_u32_le(bytes: &[u8], offset: usize) -> Option<u32> {
    if bytes.len() < offset + 4 {
        return None;
    }
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&bytes[offset..offset + 4]);
    Some(u32::from_le_bytes(buf))
}

/// Read a little-endian u16 starting at `bytes[offset]`.
pub fn read_u16_le(bytes: &[u8], offset: usize) -> Option<u16> {
    if bytes.len() < offset + 2 {
        return None;
    }
    Some(u16::from_le_bytes([bytes[offset], bytes[offset + 1]]))
}

/// Write a little-endian u64 to `out[offset..offset+8]`. Returns true
/// on success, false if `out` is too short.
pub fn write_u64_le(out: &mut [u8], offset: usize, value: u64) -> bool {
    if out.len() < offset + 8 {
        return false;
    }
    out[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    true
}

/// Write a little-endian u32 to `out[offset..offset+4]`.
pub fn write_u32_le(out: &mut [u8], offset: usize, value: u32) -> bool {
    if out.len() < offset + 4 {
        return false;
    }
    out[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    true
}

/// Write a little-endian u16 to `out[offset..offset+2]`.
pub fn write_u16_le(out: &mut [u8], offset: usize, value: u16) -> bool {
    if out.len() < offset + 2 {
        return false;
    }
    out[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    true
}
