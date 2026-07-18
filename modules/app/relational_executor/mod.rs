//! relational_executor — the shared relational execution engine
//! (RFC database foundation §14.2, §14.5).
//!
//! Compiles typed relational operations into bounded Lattice KV reads,
//! ordered scans and writes. ONE executor serves both connectors: §14.2
//! makes the intermediate representation, catalogs, name resolution,
//! binding, planning and execution shared modules rather than
//! per-connector ones. Two executors would satisfy the letter of that
//! and break its intent — they would eventually disagree about what a
//! statement means for a given schema, and nothing would surface the
//! disagreement until someone compared the ports.
//!
//! ## Durability is inherited, not implemented
//!
//! Every write leaves here as a `MSG_KV_REQUEST` and acquires its
//! durability from the same Raft + WAL path a Redis `SET` takes. This
//! module contains no persistence of its own, which is the point of
//! §14.1's "client of the canonical KV API, not another storage engine":
//! a `CREATE TABLE` that survived a crash differently from a `SET` would
//! mean there were two durability stories to reason about.
//!
//! ## One statement at a time, and why
//!
//! Execution is a state machine driven one KV round trip per
//! cooperative step, with a single statement in flight and a short queue
//! behind it. Serializing is a THROUGHPUT limit, deliberately taken in
//! exchange for two properties that matter more at this stage:
//!
//! - Fixed memory. A statement's working set — the descriptor, the
//!   staged result rows — is bounded by one job's arrays rather than by
//!   how many clients happened to connect.
//! - No interleaving to reason about. A `CREATE TABLE` allocates an id,
//!   writes a descriptor and writes a name-index entry as three separate
//!   durable writes. Concurrent statements would need those three to be
//!   atomic against each other, and the transaction machinery that makes
//!   them atomic is Phase 5. Until it exists, serializing is the honest
//!   way to keep the catalog consistent rather than the fast one.
//!
//! A full queue is reported as `ERR_BUSY` rather than dropped, so a
//! client backs off instead of assuming its statement was wrong.
//!
//! ## Statement atomicity (§13.1), and the boundary it stops at
//!
//! A multi-row `INSERT` executes as ONE `KV_OP_TXN`: every row key is
//! compared absent, then every row is written, inside a single Raft
//! entry. A crash cannot leave a partial statement, and a duplicate
//! key anywhere in the batch fails the WHOLE statement with nothing
//! written. The boundary: on an ordered multi-range map, a batch whose
//! rows straddle a range boundary refuses BY NAME (feature not
//! supported) — cross-range atomicity is the Phase 5 coordinator's to
//! provide, and half a statement is worse than a refusal.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface. unreachable_patterns: defensive `_ =>` arms are intentional so a new variant cannot silently bypass the error path"
)]
#![allow(
    clippy::manual_memcpy,
    clippy::needless_range_loop,
    reason = "hand-written index loops build wire envelopes byte-by-byte throughout these modules; the explicit form is the module idiom"
)]
use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/wire.rs"]
mod wire;

// Only `sql_exec` is mounted: it mounts `sql_core`, which mounts
// `relational`, so all three agree on one set of types. Mounting them
// separately would give this module two incompatible `relational` trees.
#[path = "../../common/sql_exec.rs"]
mod sql_exec;

use sql_exec::sql_core;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use sql_core::relational::{
    self, ColumnDescriptor, LogicalType, ObjectKind, TableDescriptor, Value, MAX_COLUMNS,
    MAX_KEY_COLUMNS, MAX_PRIMARY_KEY_LEN, MAX_ROW_BYTES,
};
use sql_core::{
    bind_access, bind_create_table, bind_insert_row, bind_projection, bound_row_key_values,
    error_code, parse, Access, BoundColumn, Dialect, SqlError, Statement,
};
use sql_exec::{
    kv_key, prefix_successor, ResponseBuilder, ERR_BUSY, ERR_CORRUPT, ERR_DUPLICATE_KEY,
    ERR_NO_SUCH_TABLE, ERR_RESULT_TOO_LARGE, ERR_STORE, ERR_TABLE_EXISTS, ERR_TIMEOUT, OUTCOME_OK,
    TAG_ALTER, TAG_BEGIN, TAG_COMMIT, TAG_CREATE_TABLE, TAG_DROP_TABLE, TAG_EMPTY, TAG_INSERT,
    TAG_ROLLBACK, TAG_SELECT, TAG_SET, TAG_SHOW,
};
use types::{
    KV_OP_DELETE, KV_OP_GET, KV_OP_INCR, KV_OP_PUT, KV_OP_RANGE_SCAN, KV_RESULT_INTEGER,
    KV_RESULT_NOT_FOUND, KV_RESULT_OK, KV_RESULT_RANGE, PROTO_INTERNAL_SQL, PUT_FLAG_NX,
};
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE, MSG_SQL_REQUEST, MSG_SQL_RESPONSE};

// ── Capacities ────────────────────────────────────────────────────────

/// Staged result rows. The cap on a result set: a `SELECT` whose rows
/// exceed it is reported `ERR_RESULT_TOO_LARGE` rather than truncated,
/// because a short result set is indistinguishable from a complete one.
/// Cursors are the real answer and are Phase 9 work.
const ROWS_BUF: usize = 24576;

/// Response payload buffer: the staged rows plus a header, the column
/// descriptors, and slack.
const RESP_BUF: usize = ROWS_BUF + 4096;

/// Inbound envelope scratch.
const SCRATCH_BUF: usize = 4096 + sql_exec::SQL_REQUEST_HEAD + 64;

/// `ORDER BY` on the primary key sorts the staged result in the executor
/// because a store need not return a scan in key order (the in-memory KV
/// engine does not). These bound the sort: at most this many rows, whose
/// sort keys total at most this many bytes. A larger ordered result is
/// reported `ERR_RESULT_TOO_LARGE`, never silently truncated or
/// mis-ordered.
const MAX_SORT_ROWS: usize = 512;
const SORT_KEY_BUF: usize = 8192;

/// Secondary indexes one table may carry.
const MAX_TABLE_INDEXES: usize = 4;

/// Staged primary keys during an index-served SELECT: pk pages queue
/// here as `[len:u16][bytes]` records while rows fetch one at a time.
const IX_PK_BUF: usize = 2048;

/// Statements waiting behind the one in flight.
const QUEUE_DEPTH: usize = 4;

/// Rows scanned per `KV_OP_RANGE_SCAN` page. Bounded so one page fits a
/// KV response and one step's work stays small.
const SCAN_PAGE: u16 = 64;

/// Keys deleted per `KV_OP_DELETE` during a `DROP TABLE`.
const DELETE_PAGE: usize = 32;

/// Aggregation bounds: result items per query, and distinct groups per
/// `GROUP BY`. A query exceeding either is reported, never truncated.
const MAX_AGG_ITEMS: usize = 8;
const MAX_GROUPS: usize = 32;
const AGG_KEY_MAX: usize = 48;
/// Aggregate function discriminants stored in `agg_item_func`.
const AGG_NONE: u8 = 0; // a bare group column
const AGG_COUNT: u8 = 1;
const AGG_SUM: u8 = 2;
const AGG_MIN: u8 = 3;
const AGG_MAX: u8 = 4;
const AGG_AVG: u8 = 5;

/// Longest KV user key this module builds: the keyspace prefix plus the
/// widest of a primary-key composite and a catalog name key.
const MAX_KV_KEY: usize = sql_exec::KEYSPACE_PREFIX_LEN
    + if MAX_PRIMARY_KEY_LEN > relational::CATALOG_NAME_KEY_MAX {
        MAX_PRIMARY_KEY_LEN
    } else {
        relational::CATALOG_NAME_KEY_MAX
    };

/// Sequence id of the table-id allocator. Table ids are handed out by a
/// durable `INCR` on this one key, so two concurrent `CREATE TABLE`s
/// cannot receive the same id even though this module serializes them —
/// the allocator does not depend on that serialization, and a future
/// concurrent executor inherits the guarantee.
const TABLE_ID_SEQUENCE: u32 = 0;

/// The database every object belongs to in this build. `sql_exec`'s key
/// composition pins tenant and database to 0; a second database needs the
/// KV envelope widened, and that is recorded there.
const DATABASE_ID: u32 = 0;

// ── Phases ────────────────────────────────────────────────────────────
//
// One phase per outstanding KV round trip. The name says which REPLY the
// phase is waiting for, not what it is about to send, because that is
// what `on_kv_response` dispatches on.

/// How long the single serializing executor will wait for one KV reply
/// before declaring the statement wedged and reclaiming its slot. A
/// dropped reply (channel overflow on a saturated storage path) would
/// otherwise leave `phase != P_IDLE` forever, wedging every session that
/// queues behind it. Set well above any legitimate round trip — even a
/// Raft commit with an fsync stall completes in well under a second — so
/// the watchdog only ever fires on a genuine loss, never on slowness.
/// Kept under a well-behaved client's read timeout so the client receives
/// the abort as an ordinary `57014` error rather than a socket timeout.
const WEDGE_TIMEOUT_MS: u64 = 10_000;

/// The watchdog timer is armed for a shade longer than the deadline so
/// that when its wake lands, `now - inflight_since_ms` is unambiguously
/// past `WEDGE_TIMEOUT_MS` and the check in `module_step` fires on that
/// very wake rather than waiting for a later, traffic-driven step.
const WEDGE_TIMER_MS: u32 = 10_500;

/// Timer provider (`provider::contract::TIMER`) and its opcodes, mirrored
/// from the kernel ABI: `CREATE` mints a one-shot fd, `SET` arms it for
/// `arg[0..4]` ms. A fired timer fd makes this module runnable, which is
/// how the watchdog is reached without incoming SQL.
const TIMER_CONTRACT: u32 = 0x0006;
const TIMER_CREATE: u32 = 0x0604;
const TIMER_SET: u32 = 0x0605;
const TIMER_CANCEL: u32 = 0x0606;

const P_IDLE: u8 = 0;
/// Waiting for the name-index lookup that resolves a table name to its id.
/// Longest table name the executor will cache. Longer names simply do
/// not get cached; they still work, they just pay the lookup.
const MAX_CACHED_NAME: usize = 64;

const P_NAME: u8 = 1;
/// CREATE: waiting for the allocator INCR.
const P_CT_ALLOC: u8 = 2;
/// CREATE: waiting for the descriptor write.
const P_CT_DESCR: u8 = 3;
/// CREATE: waiting for the name-index write.
const P_CT_NAME: u8 = 4;
/// INSERT/SELECT/DROP: waiting for the descriptor read.
const P_DESCR: u8 = 5;
/// INSERT: waiting for a row write.
const P_INS_ROW: u8 = 6;
/// SELECT: waiting for a scan page.
const P_SEL_SCAN: u8 = 7;
/// SELECT: waiting for a point read.
const P_SEL_POINT: u8 = 8;
/// DROP: waiting for a scan page of row keys to delete.
const P_DROP_SCAN: u8 = 9;
/// DROP: waiting for a row-key delete batch.
const P_DROP_DEL: u8 = 10;
/// DROP: waiting for the descriptor delete.
const P_DROP_DESCR: u8 = 11;
/// DROP: waiting for the name-index delete.
const P_DROP_NAME: u8 = 12;
/// Multi-row INSERT: waiting for the single atomic TXN.
const P_INS_TXN: u8 = 13;
/// CREATE INDEX: waiting for the index-name existence check.
const P_CI_NAME: u8 = 14;
/// CREATE INDEX: waiting for the id allocator INCR.
const P_CI_ALLOC: u8 = 15;
/// CREATE INDEX: waiting for the is-the-table-empty probe.
const P_CI_EMPTY: u8 = 16;
/// CREATE INDEX: waiting for the descriptor write.
const P_CI_DESCR: u8 = 17;
/// CREATE INDEX: waiting for the name-index write.
const P_CI_NAME_W: u8 = 18;
/// DROP INDEX: waiting for the index-name lookup.
const P_DI_NAME: u8 = 19;
/// DROP INDEX: waiting for an entry-scan page.
const P_DI_SCAN: u8 = 20;
/// DROP INDEX: waiting for an entry-delete batch.
const P_DI_DEL: u8 = 21;
/// DROP INDEX: waiting for the descriptor delete.
const P_DI_DESCR: u8 = 22;
/// DROP INDEX: waiting for the name-index delete.
const P_DI_NAME_W: u8 = 23;
/// INSERT/SELECT: waiting for the catalog page listing the table's
/// indexes.
const P_LIST: u8 = 24;
/// SELECT: waiting for an index-entry scan page.
const P_SEL_IX: u8 = 25;
/// SELECT: waiting for one row fetched by primary key from an index
/// entry.
const P_SEL_IXROW: u8 = 26;
/// DELETE (whole table / restart): waiting for a scan page of rows.
const P_DEL_SCAN: u8 = 28;
/// DELETE: waiting for the delete of a page's rows + their index entries.
const P_DEL_DEL: u8 = 29;
/// UPDATE (whole table): waiting for a scan page of rows.
const P_UPD_SCAN: u8 = 31;
/// UPDATE: waiting for the write-back of one modified row (+ index fix).
const P_UPD_WRITE: u8 = 32;
/// SELECT with aggregates: waiting for a scan page to fold into groups.
const P_AGG_SCAN: u8 = 33;
/// ALTER TABLE ADD COLUMN: waiting for the descriptor write-back.
const P_ALTER_WRITE: u8 = 34;

// ── State ─────────────────────────────────────────────────────────────

/// A queued statement, still in wire form. Kept as bytes rather than
/// parsed because parsing borrows the text, and a parsed form cannot
/// outlive the step that produced it.
#[derive(Clone, Copy)]
struct Queued {
    len: u16,
    from_mysql: bool,
    buf: [u8; SCRATCH_BUF],
}

impl Queued {
    const fn empty() -> Self {
        Self {
            len: 0,
            from_mysql: false,
            buf: [0; SCRATCH_BUF],
        }
    }
}

#[repr(C)]
struct ExecState {
    syscalls: *const SyscallTable,

    // Inputs: pg_in[0], mysql_in[1], kv_in[2]
    pg_in: i32,
    mysql_in: i32,
    kv_in: i32,
    // Outputs: pg_out[0], mysql_out[1], kv_out[2], metrics[3]
    pg_out: i32,
    mysql_out: i32,
    kv_out: i32,
    metrics_out: i32,

    // ── Catalog name cache (RFC §14.2 schema generation) ─────────────
    //
    // The name -> id lookup is a whole KV round trip and ~45% of a
    // point read's latency. Caching it is only sound with a validator:
    // the catalog lives in STORAGE and another compute graph can change
    // it under us, and a stale mapping reads the WRONG TABLE — a wrong
    // answer, not a slow one.
    //
    // Two rules make it sound, learned from a failed first version:
    //
    // 1. Only SELECT statements are served from the cache. A statement
    //    that mutates the catalog bumps the generation ITSELF, so its
    //    own reply looks exactly like another writer invalidating it —
    //    the first version failed `backup_and_restore_round_trip` on
    //    precisely that ("the drop must bite": DROP invalidated its own
    //    entry mid-statement and failed closed). A SELECT never bumps,
    //    so any generation movement it observes is genuinely another
    //    writer.
    // 2. The mapping is assumed, never trusted: the reply carries the
    //    generation current when the read EXECUTED, and a mismatch
    //    throws the answer away and redoes the statement with a real
    //    lookup. A SELECT has written nothing, so the redo is always
    //    safe.
    cache_name: [u8; MAX_CACHED_NAME],
    cache_name_len: u8,
    cache_table_id: u32,
    cache_generation: u64,
    cache_valid: bool,
    /// This job resolved its table through the cache; its result must
    /// be generation-validated before it reaches the client.
    used_cached_name: bool,
    /// The table name this job resolved, kept so a stale-cache retry
    /// can redo the lookup without re-parsing.
    pending_name: [u8; MAX_CACHED_NAME],
    pending_name_len: u8,
    /// Generation observed on the most recent reply; what a fresh
    /// cache entry is stamped with.
    last_generation: u64,

    // ── The job in flight ────────────────────────────────────────────
    phase: u8,
    /// Reply on `mysql_out` rather than `pg_out`.
    from_mysql: bool,
    corr_id: u64,
    conn_id: u8,
    dialect: u8,
    tenant_id: u32,
    /// The statement text, owned so it survives across steps.
    sql: [u8; sql_core::MAX_SQL_LEN],
    sql_len: u16,
    /// Correlation id of the outstanding KV request. Monotonic, so a
    /// reply from an abandoned job is recognised and dropped rather than
    /// applied to the current one — which would answer one client's
    /// statement with another's data.
    kv_corr: u64,

    table_id: u32,
    /// In-flight CREATE/DROP INDEX id.
    index_id: u32,
    td: TableDescriptor,
    /// The table's readable synchronous indexes, loaded per statement
    /// (one bounded catalog page). Small on purpose: a table carrying
    /// more secondary indexes than this refuses at CREATE INDEX time.
    idx: [relational::IndexDescriptor; MAX_TABLE_INDEXES],
    idx_count: u8,
    /// SELECT-via-index: primary keys staged from entry pages, drained
    /// one point read at a time.
    ix_pks: [u8; IX_PK_BUF],
    ix_pks_len: u16,
    ix_pks_at: u16,
    /// The ORDERED index value being probed (encode_index_value form).
    filter_ix: [u8; 256],
    filter_ix_len: u16,
    /// Progress: which INSERT row, or the scan cursor.
    row_index: u16,
    cursor: u64,
    affected: u64,

    /// Staged result rows in `ResponseBuilder::push_cell` encoding.
    rows: [u8; ROWS_BUF],
    rows_len: usize,
    row_count: u16,
    /// Column ids of the projection, resolved once when the descriptor
    /// arrives so each scan page does not re-resolve them.
    proj: [u16; MAX_COLUMNS],
    proj_len: usize,
    /// Residual predicates the executor applies to every staged row — the
    /// whole `WHERE` conjunction (the access-driving predicate is applied
    /// redundantly, which keeps the plan simple). `filt_count == 0` = no
    /// filter. Each `filt_op` is a `sql_core::CmpOp` discriminant
    /// (0=Eq 1=Lt 2=Le 3=Gt 4=Ge).
    filt_count: u8,
    filt_col: [u16; sql_core::MAX_PREDICATES],
    filt_op: [u8; sql_core::MAX_PREDICATES],
    filt_len: [u16; sql_core::MAX_PREDICATES],
    filt_buf: [[u8; relational::MAX_PLAIN_VALUE_LEN]; sql_core::MAX_PREDICATES],
    /// One `col [NOT] IN (v…)` residual: the column's value must (not)
    /// match one of the plain-encoded list entries.
    in_active: bool,
    in_negate: bool,
    in_col: u16,
    in_count: u8,
    in_val_len: [u16; sql_core::MAX_IN_VALUES],
    in_vals: [[u8; relational::MAX_PLAIN_VALUE_LEN]; sql_core::MAX_IN_VALUES],
    /// The bounds of a bounded SELECT scan, stored so each continuation
    /// page reproduces them exactly. A full scan, a key prefix, and a key
    /// range differ only here.
    scan_lo: [u8; MAX_KV_KEY],
    scan_lo_len: u16,
    scan_hi: [u8; MAX_KV_KEY],
    scan_hi_len: u16,
    /// `LIMIT`, or `u32::MAX` for none.
    limit: u32,

    /// `ORDER BY` state. When `sort_active`, every staged result row has a
    /// sort key (its primary key, which is order-preserving) captured in
    /// `sort_keys`, and `reply_rows` reorders the staged rows by it before
    /// shipping. The in-scan LIMIT early-stop is disabled while sorting so
    /// the smallest keys — not the first rows scanned — survive the limit.
    sort_active: bool,
    sort_n: u16,
    sort_key_off: [u32; MAX_SORT_ROWS],
    sort_key_len: [u16; MAX_SORT_ROWS],
    sort_keys: [u8; SORT_KEY_BUF],
    sort_keys_len: u32,
    /// ORDER BY target column + direction. The sort key captured per row is
    /// this column's ORDER-PRESERVING encoding, so any column (not just the
    /// primary key) and either direction can be honoured.
    sort_col: u16,
    sort_ty: LogicalType,
    sort_desc: bool,
    /// `SELECT DISTINCT`.
    distinct: bool,
    /// `OFFSET` — result rows to skip after ordering.
    offset: u32,

    /// Aggregation state. When `agg_active`, the scan folds each matching
    /// row into a per-group accumulator instead of staging it, and the
    /// reply is one computed row per group.
    agg_active: bool,
    agg_n: u8,          // projection item count
    agg_group_col: u16, // GROUP BY column id; 0 = a single global group
    agg_group_ty: LogicalType,
    agg_item_func: [u8; MAX_AGG_ITEMS], // AGG_* per item
    agg_item_col: [u16; MAX_AGG_ITEMS], // the aggregated/grouped column id
    agg_item_ty: [LogicalType; MAX_AGG_ITEMS],
    agg_item_name: [[u8; 32]; MAX_AGG_ITEMS], // result header
    agg_item_name_len: [u8; MAX_AGG_ITEMS],
    agg_grp_count: u16,
    agg_grp_key: [[u8; AGG_KEY_MAX]; MAX_GROUPS],
    agg_grp_key_len: [u16; MAX_GROUPS],
    acc_count: [u64; MAX_GROUPS * MAX_AGG_ITEMS],
    acc_sum: [f64; MAX_GROUPS * MAX_AGG_ITEMS],
    acc_min: [f64; MAX_GROUPS * MAX_AGG_ITEMS],
    acc_max: [f64; MAX_GROUPS * MAX_AGG_ITEMS],
    acc_seen: [bool; MAX_GROUPS * MAX_AGG_ITEMS],

    queue: [Queued; QUEUE_DEPTH],
    queue_len: usize,

    // Telemetry (manifest order).
    m_statements: u64,
    m_rows_returned: u64,
    m_rows_written: u64,
    m_errors: u64,
    /// SELECTs redone because the catalog moved under a cached name
    /// mapping. Near zero in steady state; rising means DDL is racing
    /// reads, or the cache is being invalidated too eagerly.
    m_stale_name_retries: u64,
    m_queue_rejects: u64,
    /// Statements the watchdog aborted because their KV round-trip never
    /// returned. Non-zero means the storage path dropped a reply (or
    /// wedged) and the executor reclaimed the slot to stay live.
    m_stmt_timeouts: u64,
    step_ctr: u64,
    /// `dev_millis` when the in-flight statement issued its outstanding KV
    /// request. The watchdog measures `now - inflight_since_ms` against
    /// `WEDGE_TIMEOUT_MS`; meaningful only while `busy()`.
    inflight_since_ms: u64,
    /// One-shot timer fd (or `-1` if the platform refused one). Armed for
    /// `WEDGE_TIMEOUT_MS` at every KV hop so the scheduler wakes this
    /// module to check the deadline even when no SQL traffic is arriving —
    /// without it, a wedged, input-starved executor is never stepped and
    /// the watchdog can never fire.
    timer_fd: i32,

    scratch: [u8; SCRATCH_BUF],
    resp: [u8; RESP_BUF],
    env: [u8; RESP_BUF + wire::ENVELOPE_HDR],
}

impl ExecState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.pg_in = -1;
        self.mysql_in = -1;
        self.kv_in = -1;
        self.pg_out = -1;
        self.mysql_out = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.phase = P_IDLE;
        self.from_mysql = false;
        self.corr_id = 0;
        self.conn_id = 0;
        self.dialect = 0;
        self.tenant_id = 0;
        self.sql_len = 0;
        self.kv_corr = 0;
        self.table_id = 0;
        self.index_id = 0;
        self.td = TableDescriptor::EMPTY;
        self.idx = [relational::IndexDescriptor::EMPTY; MAX_TABLE_INDEXES];
        self.idx_count = 0;
        self.ix_pks = [0; IX_PK_BUF];
        self.ix_pks_len = 0;
        self.ix_pks_at = 0;
        self.filter_ix = [0; 256];
        self.filter_ix_len = 0;
        self.row_index = 0;
        self.cursor = 0;
        self.affected = 0;
        self.rows_len = 0;
        self.row_count = 0;
        self.proj_len = 0;
        self.filt_count = 0;
        self.in_active = false;
        self.scan_lo_len = 0;
        self.scan_hi_len = 0;
        self.limit = u32::MAX;
        self.sort_active = false;
        self.sort_n = 0;
        self.sort_keys_len = 0;
        self.sort_col = 0;
        self.sort_ty = LogicalType::Null;
        self.sort_desc = false;
        self.distinct = false;
        self.offset = 0;
        self.agg_active = false;
        self.agg_n = 0;
        self.agg_group_col = 0;
        self.agg_group_ty = LogicalType::Null;
        self.agg_grp_count = 0;
        self.queue_len = 0;
        self.m_statements = 0;
        self.m_rows_returned = 0;
        self.m_rows_written = 0;
        self.m_errors = 0;
        self.m_stale_name_retries = 0;
        self.cache_name_len = 0;
        self.cache_table_id = 0;
        self.cache_generation = 0;
        self.cache_valid = false;
        self.used_cached_name = false;
        self.pending_name_len = 0;
        self.last_generation = 0;
        self.m_queue_rejects = 0;
        self.m_stmt_timeouts = 0;
        self.step_ctr = 0;
        self.inflight_since_ms = 0;
        self.timer_fd = -1;
    }

    fn busy(&self) -> bool {
        self.phase != P_IDLE
    }
}

// ── Channel helpers ───────────────────────────────────────────────────

unsafe fn read_envelope(sys: &SyscallTable, chan: i32, scratch: &mut [u8]) -> Option<(u8, usize)> {
    if chan < 0 {
        return None;
    }
    let poll = (sys.channel_poll)(chan, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return None;
    }
    let mut hdr = [0u8; wire::ENVELOPE_HDR];
    if (sys.channel_read)(chan, hdr.as_mut_ptr(), wire::ENVELOPE_HDR) < wire::ENVELOPE_HDR as i32 {
        return None;
    }
    let msg_type = hdr[0];
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if payload_len > scratch.len() {
        return None;
    }
    if payload_len == 0 {
        return Some((msg_type, 0));
    }
    if ((sys.channel_read)(chan, scratch.as_mut_ptr(), payload_len) as usize) < payload_len {
        return None;
    }
    Some((msg_type, payload_len))
}

/// Write one envelope. `env` is caller-supplied because the payloads
/// here are far too large for a stack buffer inside a module step.
unsafe fn write_envelope(
    sys: &SyscallTable,
    chan: i32,
    msg_type: u8,
    payload_len: usize,
    env: &mut [u8],
) -> bool {
    if chan < 0 || payload_len > u16::MAX as usize {
        return false;
    }
    // The payload is already at `env[ENVELOPE_HDR..]`; only the header
    // is stamped here, so a large response is never copied twice.
    env[0] = msg_type;
    env[1] = (payload_len & 0xFF) as u8;
    env[2] = ((payload_len >> 8) & 0xFF) as u8;
    let total = wire::ENVELOPE_HDR + payload_len;
    (sys.channel_write)(chan, env.as_mut_ptr(), total) == total as i32
}

// ── Reply ─────────────────────────────────────────────────────────────

/// Send a response with no rows: a completion or an error.
fn reply_simple(exec: &mut ExecState, outcome: u8, tag: u8, affected: u64) {
    let (corr, conn, from_mysql) = (exec.corr_id, exec.conn_id, exec.from_mysql);
    let n = {
        let Some(b) = ResponseBuilder::new(&mut exec.resp, corr, conn, outcome, tag, affected, &[])
        else {
            exec.phase = P_IDLE;
            return;
        };
        b.finish()
    };
    ship(exec, n, from_mysql);
    if outcome != OUTCOME_OK {
        exec.m_errors = exec.m_errors.wrapping_add(1);
    }
    finish_job(exec);
}

/// Answer a constant-expression select with a one-row, one-column
/// result set.
///
/// The column is synthesised here rather than read from a catalog,
/// because there is no table involved. PostgreSQL names an unaliased
/// expression `?column?`, and matching that spelling matters: a client
/// that displays headers shows what a real server would.
fn reply_constant(exec: &mut ExecState, value: sql_core::Literal<'_>, alias: Option<&[u8]>) {
    let (ty, cell) = {
        let mut tmp = [0u8; relational::MAX_PLAIN_VALUE_LEN];
        match literal_cell(value, &mut tmp) {
            Some((ty, Some(n))) => {
                exec.rows[..n].copy_from_slice(&tmp[..n]);
                (ty, Some(n))
            }
            Some((ty, None)) => (ty, None),
            None => {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            }
        }
    };
    let name = alias.unwrap_or(b"?column?");
    let Some(col) = ColumnDescriptor::new(1, ty, true, false, name) else {
        reply_simple(exec, sql_exec::ERR_NAME_TOO_LONG, TAG_EMPTY, 0);
        return;
    };
    let (corr, conn, from_mysql) = (exec.corr_id, exec.conn_id, exec.from_mysql);
    let n = {
        let (resp, rows) = (&mut exec.resp, &exec.rows);
        let cols: [&ColumnDescriptor; 1] = [&col];
        match ResponseBuilder::new(resp, corr, conn, OUTCOME_OK, TAG_SELECT, 1, &cols) {
            Some(b) => {
                // One cell: `[len:u32][value]`, the same encoding
                // `push_cell` produces. A NULL is the marker with no
                // bytes following — distinct from a zero-length value,
                // because `SELECT NULL` and `SELECT ''` are different
                // answers.
                let mut staged = [0u8; 4 + relational::MAX_PLAIN_VALUE_LEN];
                let total = match cell {
                    None => {
                        staged[..4].copy_from_slice(&sql_exec::NULL_CELL.to_le_bytes());
                        4
                    }
                    Some(n) => {
                        staged[..4].copy_from_slice(&(n as u32).to_le_bytes());
                        staged[4..4 + n].copy_from_slice(&rows[..n]);
                        4 + n
                    }
                };
                b.finish_with_rows(1, &staged[..total])
            }
            None => None,
        }
    };
    ship(exec, n, from_mysql);
    finish_job(exec);
}

/// Encode a bare literal as a typed cell.
///
/// The type comes from the literal's own syntax here — unlike a column
/// value, there is no schema to take it from. The inner `Option` is the
/// NULL/present distinction: `None` means SQL NULL and carries no bytes,
/// which is NOT the same as a present value of length zero.
fn literal_cell(
    lit: sql_core::Literal<'_>,
    out: &mut [u8],
) -> Option<(LogicalType, Option<usize>)> {
    let (ty, v) = match lit {
        sql_core::Literal::Int(x) => (LogicalType::BigInt, Value::BigInt(x)),
        sql_core::Literal::Float(x) => (LogicalType::Double, Value::Double(x)),
        sql_core::Literal::Bool(b) => (LogicalType::Boolean, Value::Boolean(b)),
        sql_core::Literal::Str(s) => (
            LogicalType::VarChar {
                length: relational::MAX_TEXT_LEN as u16,
            },
            Value::Text(s),
        ),
        // A bare NULL, or a function call the connector answers. Typed
        // as text because that is the widest thing a client will accept
        // without complaint, and the value is NULL either way.
        sql_core::Literal::Null => (
            LogicalType::VarChar {
                length: relational::MAX_TEXT_LEN as u16,
            },
            Value::Null,
        ),
    };
    if v.is_null() {
        return Some((ty, None));
    }
    let n = relational::encode_value_plain(out, ty, v)?;
    Some((ty, Some(n)))
}

/// Send the staged result set.
fn reply_rows(exec: &mut ExecState) {
    let (corr, conn, from_mysql) = (exec.corr_id, exec.conn_id, exec.from_mysql);
    // DISTINCT / ORDER BY / OFFSET / LIMIT are all applied here, once every
    // matching row is staged, in SQL's order: dedupe, then sort, then skip
    // OFFSET, then take LIMIT.
    if defer_limit(exec) && !finalize_result(exec) {
        reply_simple(exec, ERR_RESULT_TOO_LARGE, TAG_EMPTY, 0);
        return;
    }
    let (count, rows_len) = (exec.row_count, exec.rows_len);
    // Resolve the projection's descriptors into a local array of
    // references, then build. `td` and `resp` are disjoint fields, so
    // both borrows coexist.
    let mut cols: [&ColumnDescriptor; MAX_COLUMNS] = [&ColumnDescriptor::EMPTY; MAX_COLUMNS];
    let mut ncols = 0usize;
    {
        let td = &exec.td;
        for i in 0..exec.proj_len {
            match td.column(exec.proj[i]) {
                Some(c) => {
                    cols[ncols] = c;
                    ncols += 1;
                }
                None => {
                    // The projection was resolved from this same
                    // descriptor, so this is unreachable — but a
                    // mismatched column would ship a result set whose
                    // headers disagree with its cells, so it fails closed.
                    reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
                    return;
                }
            }
        }
    }
    let n = {
        let (resp, rows) = (&mut exec.resp, &exec.rows);
        match ResponseBuilder::new(
            resp,
            corr,
            conn,
            OUTCOME_OK,
            TAG_SELECT,
            u64::from(count),
            &cols[..ncols],
        ) {
            Some(b) => b.finish_with_rows(count, &rows[..rows_len]),
            None => None,
        }
    };
    match n {
        Some(_) => {
            exec.m_rows_returned = exec.m_rows_returned.wrapping_add(u64::from(count));
            ship(exec, n, from_mysql);
            finish_job(exec);
        }
        // The staged rows fit their own buffer but not the response.
        // Report it; a truncated result set must not be sent.
        None => reply_simple(exec, ERR_RESULT_TOO_LARGE, TAG_EMPTY, 0),
    }
}

/// Copy a finished response payload into the envelope buffer and write.
fn ship(exec: &mut ExecState, payload_len: Option<usize>, from_mysql: bool) {
    let Some(len) = payload_len else {
        return;
    };
    let dest = if from_mysql {
        exec.mysql_out
    } else {
        exec.pg_out
    };
    {
        let (env, resp) = (&mut exec.env, &exec.resp);
        if wire::ENVELOPE_HDR + len > env.len() {
            return;
        }
        env[wire::ENVELOPE_HDR..wire::ENVELOPE_HDR + len].copy_from_slice(&resp[..len]);
    }
    unsafe {
        let sys = exec.syscalls;
        if !sys.is_null() {
            let _ = write_envelope(&*sys, dest, MSG_SQL_RESPONSE, len, &mut exec.env);
        }
    }
}

/// Clear the job and start the next queued statement, if any.
fn finish_job(exec: &mut ExecState) {
    exec.phase = P_IDLE;
    // The statement is done; stop the wake timer so its (level-triggered)
    // expiry does not keep re-stepping an idle executor. A dequeued
    // statement that issues a KV op re-arms it in `kv_send`.
    disarm_watchdog(exec);
    exec.rows_len = 0;
    exec.row_count = 0;
    exec.proj_len = 0;
    exec.filt_count = 0;
    exec.limit = u32::MAX;
    exec.affected = 0;
    exec.row_index = 0;
    exec.cursor = 0;
    if exec.queue_len > 0 {
        let next = exec.queue[0];
        let n = exec.queue_len;
        let mut i = 1;
        while i < n {
            exec.queue[i - 1] = exec.queue[i];
            i += 1;
        }
        exec.queue_len = n - 1;
        begin_statement(exec, next.from_mysql, next.len as usize, &next.buf);
    }
}

// ── KV request construction ───────────────────────────────────────────

/// Send one KV request and record the phase its reply belongs to.
///
/// A failed write ends the job with `ERR_STORE` rather than retrying: a
/// full outbound channel that stays full is a wedged graph, and spinning
/// on it would turn that into a livelock while the client waited.
fn kv_send(exec: &mut ExecState, op: u8, body_len: usize, next_phase: u8) {
    exec.kv_corr = exec.kv_corr.wrapping_add(1);
    let corr = exec.kv_corr;
    let (tenant, conn) = (exec.tenant_id, exec.conn_id);
    // Body has already been staged at `env[ENVELOPE_HDR + 18..]`.
    const REQ_HEAD: usize = 18;
    let ok = {
        let env = &mut exec.env;
        let at = wire::ENVELOPE_HDR;
        if at + REQ_HEAD + body_len > env.len() {
            false
        } else {
            env[at..at + 8].copy_from_slice(&corr.to_le_bytes());
            env[at + 8] = PROTO_INTERNAL_SQL;
            env[at + 9..at + 13].copy_from_slice(&tenant.to_le_bytes());
            env[at + 13] = conn;
            env[at + 14] = 0; // default consistency
            env[at + 15] = op;
            env[at + 16..at + 18].copy_from_slice(&(body_len as u16).to_le_bytes());
            true
        }
    };
    if !ok {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    let (sent, now) = unsafe {
        let sys = exec.syscalls;
        if sys.is_null() {
            (false, 0)
        } else {
            let ok = write_envelope(
                &*sys,
                exec.kv_out,
                MSG_KV_REQUEST,
                REQ_HEAD + body_len,
                &mut exec.env,
            );
            (ok, dev_millis(&*sys))
        }
    };
    if sent {
        exec.phase = next_phase;
        // Arm the watchdog: this is the instant we begin waiting for a
        // reply that may never come. Refreshed at every KV hop so a
        // legitimately multi-hop statement is judged per-hop, not by its
        // whole-statement wall time.
        exec.inflight_since_ms = now;
        arm_watchdog(exec);
    } else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
    }
}

/// Arm the one-shot wake timer for `WEDGE_TIMER_MS`. Its firing makes the
/// module runnable, so `module_step` runs the watchdog check even if no
/// SQL is arriving to step it. A no-op when the platform gave no timer.
fn arm_watchdog(exec: &mut ExecState) {
    if exec.timer_fd < 0 {
        return;
    }
    unsafe {
        let sys = exec.syscalls;
        if sys.is_null() {
            return;
        }
        let mut ms = WEDGE_TIMER_MS.to_le_bytes();
        ((*sys).provider_call)(exec.timer_fd, TIMER_SET, ms.as_mut_ptr(), 4);
    }
}

/// Disarm the wake timer once a statement leaves flight. The timer is
/// level-triggered — an expired-but-active timer keeps the module
/// runnable — so cancelling on completion is what stops a fired watchdog
/// from hot-spinning the executor while it is idle.
fn disarm_watchdog(exec: &mut ExecState) {
    if exec.timer_fd < 0 {
        return;
    }
    unsafe {
        let sys = exec.syscalls;
        if sys.is_null() {
            return;
        }
        ((*sys).provider_call)(exec.timer_fd, TIMER_CANCEL, core::ptr::null_mut(), 0);
    }
}

/// Offset in `env` where a KV request body is staged.
const BODY_AT: usize = wire::ENVELOPE_HDR + 18;

/// Stage a `KV_OP_GET` body: `[key_len:u16][key…]`.
fn stage_get(exec: &mut ExecState, key: &[u8]) -> Option<usize> {
    let env = &mut exec.env;
    let need = 2 + key.len();
    if BODY_AT + need > env.len() {
        return None;
    }
    env[BODY_AT..BODY_AT + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    env[BODY_AT + 2..BODY_AT + need].copy_from_slice(key);
    Some(need)
}

/// Stage a `KV_OP_PUT` body:
/// `[key_len:u16][key][value_len:u32][value][flags:u8][expiry:u64]`.
fn stage_put(exec: &mut ExecState, key: &[u8], value: &[u8], flags: u8) -> Option<usize> {
    let env = &mut exec.env;
    let need = 2 + key.len() + 4 + value.len() + 1 + 8;
    if BODY_AT + need > env.len() {
        return None;
    }
    let mut p = BODY_AT;
    env[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    env[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    env[p..p + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
    p += 4;
    env[p..p + value.len()].copy_from_slice(value);
    p += value.len();
    env[p] = flags;
    p += 1;
    env[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
    Some(need)
}

/// Stage a `KV_OP_INCR` body: `[key_len:u16][key][delta:i64]`.
fn stage_incr(exec: &mut ExecState, key: &[u8], delta: i64) -> Option<usize> {
    let env = &mut exec.env;
    let need = 2 + key.len() + 8;
    if BODY_AT + need > env.len() {
        return None;
    }
    env[BODY_AT..BODY_AT + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    let mut p = BODY_AT + 2;
    env[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    env[p..p + 8].copy_from_slice(&delta.to_le_bytes());
    Some(need)
}

/// Stage a `KV_OP_RANGE_SCAN` body:
/// `[start_len:u16][start][end_len:u16][end][cursor:u64][limit:u16]`.
fn stage_range_scan(
    exec: &mut ExecState,
    start: &[u8],
    end: &[u8],
    cursor: u64,
    limit: u16,
) -> Option<usize> {
    let env = &mut exec.env;
    let need = 2 + start.len() + 2 + end.len() + 8 + 2;
    if BODY_AT + need > env.len() {
        return None;
    }
    let mut p = BODY_AT;
    env[p..p + 2].copy_from_slice(&(start.len() as u16).to_le_bytes());
    p += 2;
    env[p..p + start.len()].copy_from_slice(start);
    p += start.len();
    env[p..p + 2].copy_from_slice(&(end.len() as u16).to_le_bytes());
    p += 2;
    env[p..p + end.len()].copy_from_slice(end);
    p += end.len();
    env[p..p + 8].copy_from_slice(&cursor.to_le_bytes());
    p += 8;
    env[p..p + 2].copy_from_slice(&limit.to_le_bytes());
    Some(need)
}

// ── Key builders ──────────────────────────────────────────────────────

/// The catalog name-index key for a table name.
fn name_key(out: &mut [u8], name: &[u8]) -> Option<usize> {
    let mut body = [0u8; relational::CATALOG_NAME_KEY_MAX];
    let n = relational::encode_catalog_name_key(&mut body, DATABASE_ID, ObjectKind::Table, name)?;
    kv_key(out, relational::KS_RELATIONAL_CATALOG_NAME, &body[..n])
}

/// The catalog key for a table descriptor.
fn descr_key(out: &mut [u8], table_id: u32) -> Option<usize> {
    let mut body = [0u8; relational::CATALOG_KEY_LEN];
    let n = relational::encode_catalog_key(&mut body, DATABASE_ID, ObjectKind::Table, table_id)?;
    kv_key(out, relational::KS_RELATIONAL_CATALOG, &body[..n])
}

/// The catalog-name key for an INDEX name.
fn index_name_key(out: &mut [u8], name: &[u8]) -> Option<usize> {
    let mut body = [0u8; relational::CATALOG_NAME_KEY_MAX];
    let n = relational::encode_catalog_name_key(&mut body, DATABASE_ID, ObjectKind::Index, name)?;
    kv_key(out, relational::KS_RELATIONAL_CATALOG_NAME, &body[..n])
}

/// The catalog key for an index descriptor.
fn index_descr_key(out: &mut [u8], index_id: u32) -> Option<usize> {
    let mut body = [0u8; relational::CATALOG_KEY_LEN];
    let n = relational::encode_catalog_key(&mut body, DATABASE_ID, ObjectKind::Index, index_id)?;
    kv_key(out, relational::KS_RELATIONAL_CATALOG, &body[..n])
}

/// The table-id allocator key.
fn allocator_key(out: &mut [u8]) -> Option<usize> {
    let mut body = [0u8; 8];
    let n = relational::encode_sequence_key(&mut body, TABLE_ID_SEQUENCE)?;
    kv_key(out, relational::KS_RELATIONAL_SEQUENCE, &body[..n])
}

/// The scan bounds for every row of one table.
fn table_bounds(table_id: u32, start: &mut [u8], end: &mut [u8]) -> Option<(usize, usize)> {
    let mut body = [0u8; relational::TABLE_ID_LEN];
    let bn = relational::encode_table_prefix(&mut body, table_id)?;
    let sn = kv_key(start, relational::KS_RELATIONAL_TABLE, &body[..bn])?;
    let en = prefix_successor(&start[..sn], end)?;
    Some((sn, en))
}

/// Emit one diagnostic line. Present because the alternative when a
/// path fails is guessing which of a dozen `ERR_STORE` sites fired, and
/// a module that cannot report its own effective state makes silence
/// ambiguous.
fn diag(exec: &ExecState, tag: &[u8], a: u8, b: u64) {
    unsafe {
        let sys = exec.syscalls;
        if sys.is_null() {
            return;
        }
        let mut line = [0u8; 96];
        let n = tag.len().min(48);
        line[..n].copy_from_slice(&tag[..n]);
        let mut p = n;
        line[p] = b' ';
        p += 1;
        // Two hex-ish fields is enough to name the site and its code.
        for shift in [4u32, 0] {
            line[p] = hexit((a >> shift) & 0xF);
            p += 1;
        }
        line[p] = b' ';
        p += 1;
        for shift in (0..16).rev() {
            line[p] = hexit(((b >> (shift * 4)) & 0xF) as u8);
            p += 1;
        }
        dev_log(&*sys, 2, line.as_ptr(), p);
    }
}

fn hexit(n: u8) -> u8 {
    if n < 10 {
        b'0' + n
    } else {
        b'a' + (n - 10)
    }
}

// ── Statement entry ───────────────────────────────────────────────────

/// Accept one `MSG_SQL_REQUEST`: run it, or queue it, or refuse.
fn on_sql_request(exec: &mut ExecState, from_mysql: bool, payload: &[u8]) {
    if exec.busy() {
        if exec.queue_len >= QUEUE_DEPTH {
            // Report it. A dropped statement is a client hung forever;
            // ERR_BUSY is a condition it can back off from.
            exec.m_queue_rejects = exec.m_queue_rejects.wrapping_add(1);
            reject_now(exec, from_mysql, payload, ERR_BUSY);
            return;
        }
        let slot = exec.queue_len;
        let n = payload.len().min(SCRATCH_BUF);
        exec.queue[slot].len = n as u16;
        exec.queue[slot].from_mysql = from_mysql;
        exec.queue[slot].buf[..n].copy_from_slice(&payload[..n]);
        exec.queue_len += 1;
        return;
    }
    begin_statement(exec, from_mysql, payload.len(), payload);
}

/// Answer a request we are not going to run, without disturbing the job
/// in flight. The correlation and connection come from the REFUSED
/// request, not from the running one — replying with the running job's
/// identity would deliver the error to the wrong client.
fn reject_now(exec: &mut ExecState, from_mysql: bool, payload: &[u8], code: u8) {
    let Some(req) = sql_exec::decode_sql_request(payload) else {
        return;
    };
    let (corr, conn) = (req.corr_id, req.conn_id);
    let n = {
        let Some(b) = ResponseBuilder::new(&mut exec.resp, corr, conn, code, TAG_EMPTY, 0, &[])
        else {
            return;
        };
        b.finish()
    };
    exec.m_errors = exec.m_errors.wrapping_add(1);
    // `ship` reads `exec.corr_id`-independent state only, so this does
    // not touch the running job.
    ship(exec, n, from_mysql);
}

/// Parse a statement and take the first step of executing it.
fn begin_statement(exec: &mut ExecState, from_mysql: bool, len: usize, payload: &[u8]) {
    let Some(req) = sql_exec::decode_sql_request(&payload[..len.min(payload.len())]) else {
        return; // malformed envelope: nothing to reply to
    };
    exec.from_mysql = from_mysql;
    exec.corr_id = req.corr_id;
    exec.conn_id = req.conn_id;
    exec.dialect = req.dialect;
    exec.tenant_id = req.tenant_id;
    let sl = req.sql.len().min(sql_core::MAX_SQL_LEN);
    exec.sql[..sl].copy_from_slice(&req.sql[..sl]);
    exec.sql_len = sl as u16;
    exec.m_statements = exec.m_statements.wrapping_add(1);
    // Per-STATEMENT: whether this job's result needs generation
    // validation is a property of this statement, not of the module's
    // lifetime.
    exec.used_cached_name = false;
    // The job is now "in flight" for the purposes of `reply_simple`
    // even before a KV request goes out, so a statement answered
    // entirely from here still clears correctly.
    exec.phase = P_NAME;
    step_statement(exec);
}

/// Dispatch on the parsed statement. Called with the text already in
/// `exec.sql`; the text is copied to a local first because parsing
/// borrows it and the dispatch mutates `exec`.
fn step_statement(exec: &mut ExecState) {
    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = exec.sql_len as usize;
    text[..n].copy_from_slice(&exec.sql[..n]);
    let dialect = if exec.dialect == sql_exec::DIALECT_MYSQL {
        Dialect::MySql
    } else {
        Dialect::Postgres
    };
    let stmt = match parse(&text[..n], dialect) {
        Ok(s) => s,
        Err(e) => {
            reply_simple(exec, error_code(e), TAG_EMPTY, 0);
            return;
        }
    };
    match stmt {
        Statement::Empty => reply_simple(exec, OUTCOME_OK, TAG_EMPTY, 0),
        Statement::Session(s) => {
            // Session statements are answered here, not stored. The
            // connector renders each one; the executor only says which
            // it was. BEGIN/COMMIT are accepted and are NOT transactional
            // yet — recorded in the module docs, because a client that
            // believed them would expect rollback to work.
            let tag = match s {
                sql_core::SessionStatement::Begin => TAG_BEGIN,
                sql_core::SessionStatement::Commit => TAG_COMMIT,
                sql_core::SessionStatement::Rollback => TAG_ROLLBACK,
                sql_core::SessionStatement::Set => TAG_SET,
                sql_core::SessionStatement::Show(_) => TAG_SHOW,
                sql_core::SessionStatement::SelectConstant { value, alias } => {
                    // A constant select must return a ROW, not just a
                    // completion. `SELECT 1` is the liveness probe every
                    // driver and pooler sends, and answering it with an
                    // empty result set makes a healthy server look
                    // broken.
                    reply_constant(exec, value, alias);
                    return;
                }
            };
            reply_simple(exec, OUTCOME_OK, tag, 0);
        }
        // Every DDL/DML path starts by resolving the table name, so they
        // share one first step.
        Statement::CreateTable(ct) => start_name_lookup(exec, ct.name),
        Statement::DropTable { name, .. } => start_name_lookup(exec, name),
        // CREATE INDEX resolves the TABLE first (the index hangs off
        // its descriptor); DROP INDEX resolves the INDEX name.
        Statement::CreateIndex { table, .. } => start_name_lookup(exec, table),
        Statement::DropIndex { name, .. } => start_index_name_lookup(exec, name),
        Statement::Insert(ins) => start_name_lookup(exec, ins.table),
        Statement::Select(s) => start_name_lookup(exec, s.table),
        Statement::Delete(d) => start_name_lookup(exec, d.table),
        Statement::Update(u) => start_name_lookup(exec, u.table),
        Statement::AlterTableAddColumn { table, .. } => start_name_lookup(exec, table),
    }
}

/// Is this statement's first keyword SELECT?
///
/// Deliberately dumber than the parser: leading ASCII whitespace, then
/// a case-insensitive `SELECT` followed by more whitespace. Anything
/// else — including comments, which this never sees in practice — is
/// "no", and "no" only costs the lookup. The parser proper runs after
/// the lookup returns (`after_name`), so at cache-decision time this
/// prefix test is all the statement-kind information that exists, and
/// it errs in the only safe direction.
fn statement_is_select(exec: &ExecState) -> bool {
    let sql = &exec.sql[..exec.sql_len as usize];
    let mut i = 0;
    while i < sql.len() && sql[i].is_ascii_whitespace() {
        i += 1;
    }
    if sql.len() < i + 7 {
        return false;
    }
    sql[i..i + 6].eq_ignore_ascii_case(b"SELECT") && sql[i + 6].is_ascii_whitespace()
}

fn start_name_lookup(exec: &mut ExecState, table: &[u8]) {
    // Remember the name so a stale-cache retry can redo this lookup
    // without re-parsing the statement.
    let keep = table.len().min(MAX_CACHED_NAME);
    exec.pending_name[..keep].copy_from_slice(&table[..keep]);
    exec.pending_name_len = keep as u8;

    // Cache hit, SELECT only (see the cache fields for why mutating
    // statements must not be served from here). The id is assumed, and
    // `used_cached_name` arms the generation validator in
    // `on_kv_response` — nothing reaches the client until the reply
    // proves the mapping was current when the read executed.
    if statement_is_select(exec)
        && exec.cache_valid
        && exec.cache_name_len as usize == table.len()
        && table.len() <= MAX_CACHED_NAME
        && exec.cache_name[..table.len()] == *table
    {
        exec.used_cached_name = true;
        let id_be = exec.cache_table_id.to_be_bytes();
        after_name(exec, KV_RESULT_OK, &id_be);
        return;
    }

    exec.used_cached_name = false;
    let mut key = [0u8; MAX_KV_KEY];
    let Some(kn) = name_key(&mut key, table) else {
        reply_simple(exec, sql_exec::ERR_NAME_TOO_LONG, TAG_EMPTY, 0);
        return;
    };
    let Some(bn) = stage_get(exec, &key[..kn]) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_GET, bn, P_NAME);
}

// ── KV reply dispatch ─────────────────────────────────────────────────

/// A decoded `MSG_KV_RESPONSE`: `[corr:8][conn:1][result:1][rev:8]
/// [blen:2][body]`.
struct KvReply<'a> {
    corr_id: u64,
    result: u8,
    body: &'a [u8],
    /// The catalog generation current when this reply's command
    /// executed, from the MSG_KV_RESPONSE fence tail. Zero if the
    /// responder sent no tail — and zero matches no cache entry, so an
    /// absent tail degrades to always revalidating.
    catalog_generation: u64,
}

fn decode_kv_reply(payload: &[u8]) -> Option<KvReply<'_>> {
    if payload.len() < 20 {
        return None;
    }
    let blen = u16::from_le_bytes([payload[18], payload[19]]) as usize;
    if payload.len() < 20 + blen {
        return None;
    }
    let mut c = [0u8; 8];
    c.copy_from_slice(&payload[0..8]);
    // Fence tail sits after the body; the catalog generation is its
    // last 8 bytes (see wire::MSG_KV_RESPONSE).
    let gen_at = 20 + blen + wire::KV_RESPONSE_FENCE_TAIL_LEN - 8;
    let catalog_generation = match payload.get(gen_at..gen_at + 8) {
        Some(g) => u64::from_le_bytes([g[0], g[1], g[2], g[3], g[4], g[5], g[6], g[7]]),
        None => 0,
    };
    Some(KvReply {
        corr_id: u64::from_le_bytes(c),
        result: payload[9],
        body: &payload[20..20 + blen],
        catalog_generation,
    })
}

fn on_kv_response(exec: &mut ExecState, payload: &[u8]) {
    let Some(r) = decode_kv_reply(payload) else {
        return;
    };
    // A reply from an abandoned job must not be applied to the current
    // one: it would answer this client's statement with another's data.
    if exec.phase == P_IDLE || r.corr_id != exec.kv_corr {
        return;
    }
    // Copy the body out before mutating `exec` — the payload lives in
    // `exec.scratch`.
    let mut body = [0u8; SCRATCH_BUF];
    let bl = r.body.len().min(body.len());
    body[..bl].copy_from_slice(&r.body[..bl]);
    let (result, body_len) = (r.result, bl);
    exec.last_generation = r.catalog_generation;

    // Late validation of an optimistic cache hit. The mapping was
    // assumed, not trusted; this is where it is checked, BEFORE any
    // reply handler can turn the result into an answer. Only SELECTs
    // are ever served from the cache, so nothing has been written and
    // redoing the whole statement against a real lookup is always
    // safe.
    if exec.used_cached_name && r.catalog_generation != exec.cache_generation {
        exec.cache_valid = false;
        exec.used_cached_name = false;
        exec.m_stale_name_retries = exec.m_stale_name_retries.wrapping_add(1);
        let n = exec.pending_name_len as usize;
        let mut name = [0u8; MAX_CACHED_NAME];
        name[..n].copy_from_slice(&exec.pending_name[..n]);
        start_name_lookup(exec, &name[..n]);
        return;
    }

    match exec.phase {
        P_NAME => after_name(exec, result, &body[..body_len]),
        P_CT_ALLOC => after_ct_alloc(exec, result, &body[..body_len]),
        P_CT_DESCR => after_ct_descr(exec, result),
        P_CT_NAME => after_ct_name(exec, result),
        P_DESCR => after_descr(exec, result, &body[..body_len]),
        P_INS_ROW => after_ins_row(exec, result),
        P_INS_TXN => after_ins_txn(exec, result, &body[..body_len]),
        P_CI_NAME => after_ci_name(exec, result),
        P_CI_ALLOC => after_ci_alloc(exec, result, &body[..body_len]),
        P_CI_EMPTY => after_ci_empty(exec, result, &body[..body_len]),
        P_CI_DESCR => after_ci_descr(exec, result),
        P_CI_NAME_W => after_ci_name_w(exec, result),
        P_DI_NAME => after_di_name(exec, result, &body[..body_len]),
        P_DI_SCAN => after_di_scan(exec, result, &body[..body_len]),
        P_DI_DEL => after_di_del(exec, result),
        P_DI_DESCR => after_di_descr(exec, result),
        P_DI_NAME_W => after_di_name_w(exec, result),
        P_LIST => after_list(exec, result, &body[..body_len]),
        P_SEL_IX => after_sel_ix(exec, result, &body[..body_len]),
        P_SEL_IXROW => after_sel_ixrow(exec, result, &body[..body_len]),
        P_SEL_SCAN => after_sel_scan(exec, result, &body[..body_len]),
        P_AGG_SCAN => after_agg_scan(exec, result, &body[..body_len]),
        P_ALTER_WRITE => match result {
            KV_RESULT_OK => reply_simple(exec, OUTCOME_OK, TAG_ALTER, 0),
            _ => reply_simple(exec, ERR_STORE, TAG_EMPTY, 0),
        },
        P_SEL_POINT => after_sel_point(exec, result, &body[..body_len]),
        P_DROP_SCAN => after_drop_scan(exec, result, &body[..body_len]),
        P_DROP_DEL => after_drop_del(exec, result),
        P_DROP_DESCR => after_drop_descr(exec, result),
        P_DROP_NAME => after_drop_name(exec, result),
        P_DEL_SCAN => after_del_scan(exec, result, &body[..body_len]),
        P_DEL_DEL => after_del_del(exec, result),
        P_UPD_SCAN => after_upd_scan(exec, result, &body[..body_len]),
        P_UPD_WRITE => after_upd_write(exec, result, &body[..body_len]),
        _ => reply_simple(exec, ERR_STORE, TAG_EMPTY, 0),
    }
}

/// Re-parse the statement into a local buffer so a handler can consult
/// it. Returns the text length; the caller parses `text[..n]`.
fn statement_text(exec: &ExecState, text: &mut [u8]) -> usize {
    let n = (exec.sql_len as usize).min(text.len());
    text[..n].copy_from_slice(&exec.sql[..n]);
    n
}

fn dialect_of(exec: &ExecState) -> Dialect {
    if exec.dialect == sql_exec::DIALECT_MYSQL {
        Dialect::MySql
    } else {
        Dialect::Postgres
    }
}

/// The name lookup came back. Every statement kind branches here on
/// whether the table exists.
fn after_name(exec: &mut ExecState, result: u8, body: &[u8]) {
    let exists = result == KV_RESULT_OK;
    if !exists && result != KV_RESULT_NOT_FOUND {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    if exists {
        // The name-index value is the table id, big-endian so the value
        // and the key halves of the catalog agree on byte order.
        if body.len() < 4 {
            reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
            return;
        }
        exec.table_id = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        // A REAL lookup refreshes the cache, stamped with the
        // generation observed on the reply that produced it.
        //
        // Generation ZERO means "no generation available" — the
        // separated path, where DataResponse cannot carry it yet — and
        // an unavailable generation must disable the cache, not arm it:
        // a mapping stamped 0 would validate against every later 0 and
        // serve a stale table id across compute graphs. On the combined
        // path a found table implies at least one catalog write, so a
        // real generation is never 0 here.
        if !exec.used_cached_name && exec.last_generation != 0 {
            let n = exec.pending_name_len as usize;
            if n > 0 && n <= MAX_CACHED_NAME {
                exec.cache_name[..n].copy_from_slice(&exec.pending_name[..n]);
                exec.cache_name_len = n as u8;
                exec.cache_table_id = exec.table_id;
                exec.cache_generation = exec.last_generation;
                exec.cache_valid = true;
            }
        }
    }

    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    let stmt = match parse(&text[..n], dialect_of(exec)) {
        Ok(s) => s,
        Err(e) => {
            reply_simple(exec, error_code(e), TAG_EMPTY, 0);
            return;
        }
    };
    match stmt {
        Statement::CreateTable(ct) => {
            if exists {
                // `IF NOT EXISTS` makes an existing table a success —
                // which is what the client asked for, and reporting an
                // error would break idempotent schema setup scripts.
                if ct.if_not_exists {
                    reply_simple(exec, OUTCOME_OK, TAG_CREATE_TABLE, 0);
                } else {
                    reply_simple(exec, ERR_TABLE_EXISTS, TAG_EMPTY, 0);
                }
                return;
            }
            // Allocate an id with a durable INCR. Doing this BEFORE
            // writing anything means a crash here leaks an id and
            // nothing else; the reverse order could leave a descriptor
            // no name resolves to.
            let mut key = [0u8; MAX_KV_KEY];
            let Some(kn) = allocator_key(&mut key) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            let Some(bn) = stage_incr(exec, &key[..kn], 1) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            kv_send(exec, KV_OP_INCR, bn, P_CT_ALLOC);
        }
        Statement::DropTable { if_exists, .. } => {
            if !exists {
                if if_exists {
                    reply_simple(exec, OUTCOME_OK, TAG_DROP_TABLE, 0);
                } else {
                    reply_simple(exec, ERR_NO_SUCH_TABLE, TAG_EMPTY, 0);
                }
                return;
            }
            start_descr_read(exec);
        }
        Statement::Insert(_)
        | Statement::Select(_)
        | Statement::CreateIndex { .. }
        | Statement::Delete(_)
        | Statement::Update(_)
        | Statement::AlterTableAddColumn { .. } => {
            if !exists {
                reply_simple(exec, ERR_NO_SUCH_TABLE, TAG_EMPTY, 0);
                return;
            }
            start_descr_read(exec);
        }
        _ => reply_simple(exec, ERR_STORE, TAG_EMPTY, 0),
    }
}

fn start_descr_read(exec: &mut ExecState) {
    let mut key = [0u8; MAX_KV_KEY];
    let Some(kn) = descr_key(&mut key, exec.table_id) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let Some(bn) = stage_get(exec, &key[..kn]) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_GET, bn, P_DESCR);
}

fn after_ct_alloc(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_INTEGER || body.len() < 8 {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    let mut v = [0u8; 8];
    v.copy_from_slice(&body[0..8]);
    let counter = i64::from_le_bytes(v);
    // Table id 0 is reserved so a zero-valued key is never mistaken for
    // a real table; the allocator's first INCR returns 1.
    if counter <= 0 || counter > u32::MAX as i64 {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    exec.table_id = counter as u32;

    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    let Ok(Statement::CreateTable(ct)) = parse(&text[..n], dialect_of(exec)) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    // `catalog_revision` is the table id for now. It exists so a plan can
    // detect a schema change and rebind (§14.5); with no online schema
    // change implemented there is nothing yet to bump it, and using a
    // fabricated clock value would be worse than a stable one.
    let td = match bind_create_table(&ct, exec.table_id, DATABASE_ID, u64::from(exec.table_id)) {
        Ok(td) => td,
        Err(e) => {
            reply_simple(exec, error_code(e), TAG_EMPTY, 0);
            return;
        }
    };
    exec.td = td;

    let mut key = [0u8; MAX_KV_KEY];
    let Some(kn) = descr_key(&mut key, exec.table_id) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let mut val = [0u8; 8192];
    let Some(vn) = td.encode(&mut val) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let Some(bn) = stage_put(exec, &key[..kn], &val[..vn], 0) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_PUT, bn, P_CT_DESCR);
}

fn after_ct_descr(exec: &mut ExecState, result: u8) {
    if result != KV_RESULT_OK {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    // The name index is written LAST, and that order is the one thing
    // making a crash mid-CREATE safe: the name is what every statement
    // resolves through, so until it exists the table is invisible. A
    // crash after the descriptor leaves an unreachable descriptor —
    // wasted space, not a broken catalog. The reverse order would leave
    // a name resolving to a descriptor that is not there.
    let mut key = [0u8; MAX_KV_KEY];
    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    let Ok(Statement::CreateTable(ct)) = parse(&text[..n], dialect_of(exec)) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let Some(kn) = name_key(&mut key, ct.name) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let id = exec.table_id.to_be_bytes();
    // NX so a concurrent creator cannot be overwritten. This executor
    // serializes, so it cannot race itself — but the guarantee should
    // come from the write, not from the scheduling, or it evaporates the
    // moment a second executor exists.
    let Some(bn) = stage_put(exec, &key[..kn], &id, PUT_FLAG_NX) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_PUT, bn, P_CT_NAME);
}

fn after_ct_name(exec: &mut ExecState, result: u8) {
    match result {
        KV_RESULT_OK => reply_simple(exec, OUTCOME_OK, TAG_CREATE_TABLE, 0),
        // NX refused: someone else created the name between our lookup
        // and our write. `CAS_FAILED` is how the engine spells a failed
        // NX, since the flag is a compare-and-set against "absent".
        types::KV_RESULT_CAS_FAILED | types::KV_RESULT_EXISTS => {
            reply_simple(exec, ERR_TABLE_EXISTS, TAG_EMPTY, 0)
        }
        _ => reply_simple(exec, ERR_STORE, TAG_EMPTY, 0),
    }
}

/// The descriptor arrived. Branch to the statement's real work.
fn after_descr(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_OK {
        // The name index resolved but the descriptor is gone. That is
        // corruption, not absence: absence would have failed the name
        // lookup.
        reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
        return;
    }
    let Some(td) = TableDescriptor::decode(body) else {
        reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
        return;
    };
    exec.td = td;

    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    let stmt = match parse(&text[..n], dialect_of(exec)) {
        Ok(s) => s,
        Err(e) => {
            reply_simple(exec, error_code(e), TAG_EMPTY, 0);
            return;
        }
    };
    match stmt {
        Statement::Insert(_)
        | Statement::Select(_)
        | Statement::Delete(_)
        | Statement::Update(_) => {
            // All need the table's indexes first: INSERT/UPDATE/DELETE
            // maintain them inside a transaction, SELECT may be SERVED by
            // one. One bounded catalog page answers it.
            exec.row_index = 0;
            exec.affected = 0;
            send_index_list(exec);
        }
        Statement::AlterTableAddColumn { column, ty, .. } => {
            // Append a nullable column to the loaded descriptor and write it
            // back. Existing rows are untouched — they simply lack the
            // column and read it as NULL. The catalog PUT bumps the schema
            // generation, so any cached descriptor is invalidated.
            if sql_core::resolve_column(&exec.td, column).is_ok() {
                reply_simple(exec, ERR_DUPLICATE_KEY, TAG_EMPTY, 0);
                return;
            }
            let next_id = (exec.td.columns().len() + 1) as u16;
            let Some(cd) = ColumnDescriptor::new(next_id, ty, true, false, column) else {
                reply_simple(exec, sql_exec::ERR_UNKNOWN_COLUMN, TAG_EMPTY, 0);
                return;
            };
            if exec.td.add_column(cd).is_err() {
                reply_simple(
                    exec,
                    error_code(sql_core::SqlError::TooManyItems),
                    TAG_EMPTY,
                    0,
                );
                return;
            }
            let mut key = [0u8; MAX_KV_KEY];
            let Some(kn) = descr_key(&mut key, exec.table_id) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            let mut val = [0u8; 8192];
            let Some(vn) = exec.td.encode(&mut val) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            let Some(bn) = stage_put(exec, &key[..kn], &val[..vn], 0) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            kv_send(exec, KV_OP_PUT, bn, P_ALTER_WRITE);
        }
        Statement::DropTable { .. } => start_drop_scan(exec),
        Statement::CreateIndex { column, name, .. } => {
            // The indexed column must exist; refusing here beats a
            // descriptor no query could ever use.
            if sql_core::resolve_column(&exec.td, column).is_err() {
                reply_simple(exec, sql_exec::ERR_UNKNOWN_COLUMN, TAG_EMPTY, 0);
                return;
            }
            let mut key = [0u8; MAX_KV_KEY];
            let Some(kn) = index_name_key(&mut key, name) else {
                reply_simple(exec, sql_exec::ERR_NAME_TOO_LONG, TAG_EMPTY, 0);
                return;
            };
            let Some(bn) = stage_get(exec, &key[..kn]) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            kv_send(exec, KV_OP_GET, bn, P_CI_NAME);
        }
        _ => reply_simple(exec, ERR_STORE, TAG_EMPTY, 0),
    }
}

// ── INSERT ────────────────────────────────────────────────────────────

/// Encode and send the row at `exec.row_index`, or finish.
/// Encode row `idx` of an INSERT into `key`/`row`. Returns the two
/// lengths, or the SQL error code to refuse with.
fn encode_insert_row(
    exec: &ExecState,
    ins: &sql_core::Insert<'_>,
    idx: usize,
    key: &mut [u8; MAX_KV_KEY],
    row: &mut [u8; MAX_ROW_BYTES],
) -> Result<(usize, usize), u8> {
    let mut bound = [BoundColumn {
        column_id: 0,
        ty: LogicalType::Null,
        value: Value::Null,
    }; MAX_COLUMNS];
    let bn = bind_insert_row(&exec.td, ins, idx, &mut bound).map_err(error_code)?;
    let mut kv = [Value::Null; MAX_KEY_COLUMNS];
    let kn = bound_row_key_values(&exec.td, &bound[..bn], &mut kv).map_err(error_code)?;
    let mut ktypes = [LogicalType::Null; MAX_KEY_COLUMNS];
    let tn = exec.td.primary_key_types(&mut ktypes).ok_or(ERR_CORRUPT)?;
    let mut keybody = [0u8; MAX_PRIMARY_KEY_LEN];
    let kb = relational::encode_primary_key(&mut keybody, exec.table_id, &ktypes[..tn], &kv[..kn])
        .ok_or(sql_exec::ERR_LITERAL_TOO_LONG)?;
    let kk = kv_key(key, relational::KS_RELATIONAL_TABLE, &keybody[..kb]).ok_or(ERR_STORE)?;
    let mut cols = [relational::ColumnValue {
        column_id: 0,
        ty: LogicalType::Null,
        value: Value::Null,
    }; MAX_COLUMNS];
    for i in 0..bn {
        cols[i] = relational::ColumnValue {
            column_id: bound[i].column_id,
            ty: bound[i].ty,
            value: bound[i].value,
        };
    }
    let rn = relational::encode_row(row, &cols[..bn]).ok_or(sql_exec::ERR_TOO_MANY_ITEMS)?;
    Ok((kk, rn))
}

/// Index-entry user key for row `idx` under index `d`:
/// `kv_key(KS_RELATIONAL_INDEX, index_user_key(id, ordered(value), pk))`.
fn encode_insert_index_key(
    exec: &ExecState,
    ins: &sql_core::Insert<'_>,
    idx: usize,
    d: &relational::IndexDescriptor,
    key: &mut [u8; MAX_KV_KEY],
) -> Result<usize, u8> {
    let mut bound = [BoundColumn {
        column_id: 0,
        ty: LogicalType::Null,
        value: Value::Null,
    }; MAX_COLUMNS];
    let bn = bind_insert_row(&exec.td, ins, idx, &mut bound).map_err(error_code)?;
    // The indexed column's value in this row. An index over a column
    // the row leaves NULL indexes the NULL ordering — encode it like
    // any value, so `WHERE col = x` and the index agree on order.
    let col_id = d.key_columns()[0];
    let (ty, value) = bound[..bn]
        .iter()
        .find(|b| b.column_id == col_id)
        .map(|b| (b.ty, b.value))
        .unwrap_or((LogicalType::Null, Value::Null));
    let mut valbuf = [0u8; relational::MAX_INDEX_VALUE_LEN];
    let ivn = relational::encode_index_value(&mut valbuf, &[ty], &[value])
        .ok_or(sql_exec::ERR_LITERAL_TOO_LONG)?;
    // The row's primary key (the body handed to kv_key for the row).
    let mut kv = [Value::Null; MAX_KEY_COLUMNS];
    let kn = bound_row_key_values(&exec.td, &bound[..bn], &mut kv).map_err(error_code)?;
    let mut ktypes = [LogicalType::Null; MAX_KEY_COLUMNS];
    let tn = exec.td.primary_key_types(&mut ktypes).ok_or(ERR_CORRUPT)?;
    let mut pk = [0u8; MAX_PRIMARY_KEY_LEN];
    let pkn = relational::encode_primary_key(&mut pk, exec.table_id, &ktypes[..tn], &kv[..kn])
        .ok_or(sql_exec::ERR_LITERAL_TOO_LONG)?;
    let mut body =
        [0u8; relational::INDEX_ID_LEN + 2 * relational::MAX_INDEX_VALUE_LEN + 2 * 512 + 4];
    let bn2 = relational::index_user_key(d.index_id, &valbuf[..ivn], &pk[..pkn], &mut body)
        .ok_or(sql_exec::ERR_LITERAL_TOO_LONG)?;
    kv_key(key, relational::KS_RELATIONAL_INDEX, &body[..bn2]).ok_or(ERR_STORE)
}

/// Multi-row INSERT as ONE `KV_OP_TXN`: compare every row key absent
/// (`mod_revision == 0`), THEN write every row. The worker evaluates
/// the whole thing atomically inside one Raft entry, which is what SQL
/// statement atomicity demands — the previous one-PUT-per-row loop
/// could crash midway and leave some rows present (the gap the module
/// header used to record). A failed comparison means a duplicate key
/// and NOTHING is written. On an ordered multi-range map the router
/// executes the TXN only when every key lives in ONE range and refuses
/// `KV_RESULT_CROSS_RANGE` otherwise — refusing loudly beats writing
/// half a statement; the cross-range path is the Phase 5 coordinator's
/// to earn.
fn send_insert_txn(exec: &mut ExecState, ins: &sql_core::Insert<'_>) {
    let rows = ins.row_count();
    // Body budget: the request must fit every hop's 4 KiB scratch
    // (router / worker), with envelope + head slack.
    const TXN_BODY_MAX: usize = 3800;
    let mut p = BODY_AT;
    let end_guard = BODY_AT + TXN_BODY_MAX;
    // [cmp_count] then per row: [cmp_op][klen][key][witness 0]
    exec.env[p..p + 2].copy_from_slice(&(rows as u16).to_le_bytes());
    p += 2;
    for idx in 0..rows {
        let mut key = [0u8; MAX_KV_KEY];
        let mut row = [0u8; MAX_ROW_BYTES];
        let (kk, _) = match encode_insert_row(exec, ins, idx, &mut key, &mut row) {
            Ok(v) => v,
            Err(code) => {
                reply_simple(exec, code, TAG_EMPTY, 0);
                return;
            }
        };
        let need = 1 + 2 + kk + 8;
        if p + need > end_guard {
            reply_simple(exec, sql_exec::ERR_TOO_MANY_ITEMS, TAG_EMPTY, 0);
            return;
        }
        exec.env[p] = types::TXN_CMP_MOD_EQUAL;
        p += 1;
        exec.env[p..p + 2].copy_from_slice(&(kk as u16).to_le_bytes());
        p += 2;
        exec.env[p..p + kk].copy_from_slice(&key[..kk]);
        p += kk;
        exec.env[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
        p += 8;
    }
    // [then_count]: one PUT per row plus one per (row, index) entry.
    let then_count = rows + rows * exec.idx_count as usize;
    if then_count > u16::MAX as usize {
        reply_simple(exec, sql_exec::ERR_TOO_MANY_ITEMS, TAG_EMPTY, 0);
        return;
    }
    exec.env[p..p + 2].copy_from_slice(&(then_count as u16).to_le_bytes());
    p += 2;
    for idx in 0..rows {
        let mut key = [0u8; MAX_KV_KEY];
        let mut row = [0u8; MAX_ROW_BYTES];
        let (kk, rn) = match encode_insert_row(exec, ins, idx, &mut key, &mut row) {
            Ok(v) => v,
            Err(code) => {
                reply_simple(exec, code, TAG_EMPTY, 0);
                return;
            }
        };
        let put_len = 2 + kk + 4 + rn + 1 + 8;
        let need = 1 + 2 + put_len;
        if p + need > end_guard {
            reply_simple(exec, sql_exec::ERR_TOO_MANY_ITEMS, TAG_EMPTY, 0);
            return;
        }
        exec.env[p] = KV_OP_PUT;
        p += 1;
        exec.env[p..p + 2].copy_from_slice(&(put_len as u16).to_le_bytes());
        p += 2;
        exec.env[p..p + 2].copy_from_slice(&(kk as u16).to_le_bytes());
        p += 2;
        exec.env[p..p + kk].copy_from_slice(&key[..kk]);
        p += kk;
        exec.env[p..p + 4].copy_from_slice(&(rn as u32).to_le_bytes());
        p += 4;
        exec.env[p..p + rn].copy_from_slice(&row[..rn]);
        p += rn;
        // The comparisons already fenced absence; a plain PUT keeps
        // the branch free of nested CAS semantics.
        exec.env[p] = 0;
        p += 1;
        exec.env[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
        p += 8;

        // §14.4: the row's index entries ride the SAME transaction.
        // Entry values are empty — the entry IS its key; the row is
        // fetched by the primary key embedded in it.
        for i in 0..exec.idx_count as usize {
            let d = exec.idx[i];
            let mut ixkey = [0u8; MAX_KV_KEY];
            let kk = match encode_insert_index_key(exec, ins, idx, &d, &mut ixkey) {
                Ok(v) => v,
                Err(code) => {
                    reply_simple(exec, code, TAG_EMPTY, 0);
                    return;
                }
            };
            let put_len = 2 + kk + 4 + 1 + 8;
            let need = 1 + 2 + put_len;
            if p + need > end_guard {
                reply_simple(exec, sql_exec::ERR_TOO_MANY_ITEMS, TAG_EMPTY, 0);
                return;
            }
            exec.env[p] = KV_OP_PUT;
            p += 1;
            exec.env[p..p + 2].copy_from_slice(&(put_len as u16).to_le_bytes());
            p += 2;
            exec.env[p..p + 2].copy_from_slice(&(kk as u16).to_le_bytes());
            p += 2;
            for b in 0..kk {
                exec.env[p + b] = ixkey[b];
            }
            p += kk;
            exec.env[p..p + 4].copy_from_slice(&0u32.to_le_bytes());
            p += 4;
            exec.env[p] = 0;
            p += 1;
            exec.env[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
            p += 8;
        }
    }
    // [else_count] = 0: a failed comparison answers `succeeded = 0`
    // and writes nothing.
    exec.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
    p += 2;
    exec.affected = rows as u64;
    kv_send(exec, types::KV_OP_TXN, p - BODY_AT, P_INS_TXN);
}

fn after_ins_txn(exec: &mut ExecState, result: u8, body: &[u8]) {
    match result {
        types::KV_RESULT_TXN if !body.is_empty() => {
            if body[0] == 1 {
                let affected = exec.affected;
                exec.m_rows_written = exec.m_rows_written.wrapping_add(affected);
                reply_simple(exec, OUTCOME_OK, TAG_INSERT, affected);
            } else {
                // A comparison failed: some row's key already exists.
                // Nothing was written — that is the whole point.
                reply_simple(exec, ERR_DUPLICATE_KEY, TAG_EMPTY, 0);
            }
        }
        types::KV_RESULT_CROSS_RANGE => {
            // The rows straddle a range boundary and there is no
            // cross-range coordinator yet: refuse BY NAME rather than
            // write half a statement.
            reply_simple(exec, sql_exec::ERR_UNSUPPORTED, TAG_EMPTY, 0)
        }
        _ => reply_simple(exec, ERR_STORE, TAG_EMPTY, 0),
    }
}

// ── Index list (one catalog page) ────────────────────────────────────

/// Scan the catalog's Index-kind band and stage this table's readable
/// synchronous indexes into `exec.idx`.
fn send_index_list(exec: &mut ExecState) {
    let mut start = [0u8; MAX_KV_KEY];
    // Prefix: [database:u32 BE][kind:Index] — every index descriptor
    // of this database, any table; the reply filters by table id.
    let mut body = [0u8; 5];
    body[0..4].copy_from_slice(&DATABASE_ID.to_be_bytes());
    body[4] = ObjectKind::Index as u8;
    let Some(sn) = kv_key(&mut start, relational::KS_RELATIONAL_CATALOG, &body) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let mut end = [0u8; MAX_KV_KEY];
    let Some(en) = prefix_successor(&start[..sn], &mut end) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let Some(bn) = stage_range_scan(exec, &start[..sn], &end[..en], 0, 32) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_RANGE_SCAN, bn, P_LIST);
}

fn after_list(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    exec.idx_count = 0;
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    let mut at = 10usize;
    for _ in 0..count {
        if body.len() < at + 2 {
            reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
            return;
        }
        let klen = u16::from_le_bytes([body[at], body[at + 1]]) as usize;
        let voff = at + 2 + klen;
        if body.len() < voff + 4 {
            reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
            return;
        }
        let vlen = u32::from_le_bytes(body[voff..voff + 4].try_into().unwrap_or([0; 4])) as usize;
        let vend = voff + 4 + vlen;
        if body.len() < vend {
            reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
            return;
        }
        if let Some(d) = relational::IndexDescriptor::decode(&body[voff + 4..vend]) {
            if d.table_id == exec.table_id
                && relational::write_insert_allowed(d.phase)
                && d.exactness == relational::IndexExactness::Synchronous
                && (exec.idx_count as usize) < MAX_TABLE_INDEXES
            {
                exec.idx[exec.idx_count as usize] = d;
                exec.idx_count += 1;
            }
        }
        at = vend;
    }
    // One page bounds the survey. More indexes than a page holds is a
    // config this executor refuses to create (MAX_TABLE_INDEXES), so a
    // non-zero continuation cursor here means OTHER tables' indexes —
    // fine to ignore only because the filter above is by table AND the
    // page is 32 wide against a 4-per-table cap across a one-database
    // catalog. Revisit when databases multiply.
    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    match parse(&text[..n], dialect_of(exec)) {
        Ok(Statement::Insert(_)) => send_insert_row(exec),
        Ok(Statement::Select(sel)) => start_select(exec, &sel),
        Ok(Statement::Delete(del)) => start_delete(exec, &del),
        Ok(Statement::Update(upd)) => start_update(exec, &upd),
        _ => reply_simple(exec, ERR_STORE, TAG_EMPTY, 0),
    }
}

// ── SELECT served by a secondary index ───────────────────────────────

/// The readable synchronous index whose single key column is
/// `column_id`, if any.
fn index_on(exec: &ExecState, column_id: u16) -> Option<&relational::IndexDescriptor> {
    exec.idx[..exec.idx_count as usize]
        .iter()
        .find(|d| d.key_columns() == [column_id] && d.readable())
}

/// One page of index entries; stage their PRIMARY KEYS and fetch rows.
fn send_ix_scan(exec: &mut ExecState) {
    let mut prefix = [0u8; MAX_KV_KEY];
    let vn = exec.filter_ix_len as usize;
    let mut val = [0u8; relational::MAX_INDEX_VALUE_LEN];
    val[..vn].copy_from_slice(&exec.filter_ix[..vn]);
    let mut pbody = [0u8; relational::INDEX_ID_LEN + 2 * relational::MAX_INDEX_VALUE_LEN + 2];
    let Some(pb) = relational::index_user_prefix(exec.index_id, &val[..vn], &mut pbody) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let Some(pn) = kv_key(&mut prefix, relational::KS_RELATIONAL_INDEX, &pbody[..pb]) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let mut end = [0u8; MAX_KV_KEY];
    let Some(en) = prefix_successor(&prefix[..pn], &mut end) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let cursor = exec.cursor;
    let Some(bn) = stage_range_scan(exec, &prefix[..pn], &end[..en], cursor, 32) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_RANGE_SCAN, bn, P_SEL_IX);
}

fn after_sel_ix(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    exec.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    exec.ix_pks_len = 0;
    exec.ix_pks_at = 0;
    let mut at = 10usize;
    for _ in 0..count {
        if body.len() < at + 2 {
            reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
            return;
        }
        let klen = u16::from_le_bytes([body[at], body[at + 1]]) as usize;
        let kend = at + 2 + klen;
        if body.len() < kend + 4 {
            reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
            return;
        }
        let vlen = u32::from_le_bytes(body[kend..kend + 4].try_into().unwrap_or([0; 4])) as usize;
        // Entry keys are full user keys: [keyspace:4][composite body].
        if klen < sql_exec::KEYSPACE_PREFIX_LEN {
            reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
            return;
        }
        let kbody = &body[at + 2 + sql_exec::KEYSPACE_PREFIX_LEN..kend];
        let mut pk = [0u8; relational::MAX_PRIMARY_KEY_LEN];
        let Some((_, pk_len)) = relational::index_user_key_primary(kbody, &mut pk) else {
            reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
            return;
        };
        let need = 2 + pk_len;
        let w = exec.ix_pks_len as usize;
        if w + need > IX_PK_BUF {
            reply_simple(exec, ERR_RESULT_TOO_LARGE, TAG_EMPTY, 0);
            return;
        }
        exec.ix_pks[w..w + 2].copy_from_slice(&(pk_len as u16).to_le_bytes());
        exec.ix_pks[w + 2..w + 2 + pk_len].copy_from_slice(&pk[..pk_len]);
        exec.ix_pks_len = (w + need) as u16;
        at = kend + 4 + vlen;
    }
    send_next_ix_row(exec);
}

/// Fetch the next staged primary key's row, or advance the scan, or
/// finish.
fn send_next_ix_row(exec: &mut ExecState) {
    if exec.ix_pks_at >= exec.ix_pks_len {
        if exec.cursor != 0 {
            send_ix_scan(exec);
        } else {
            reply_rows(exec);
        }
        return;
    }
    let at = exec.ix_pks_at as usize;
    let pk_len = u16::from_le_bytes([exec.ix_pks[at], exec.ix_pks[at + 1]]) as usize;
    let mut key = [0u8; MAX_KV_KEY];
    let mut pk = [0u8; relational::MAX_PRIMARY_KEY_LEN];
    pk[..pk_len].copy_from_slice(&exec.ix_pks[at + 2..at + 2 + pk_len]);
    exec.ix_pks_at = (at + 2 + pk_len) as u16;
    let Some(kn) = kv_key(&mut key, relational::KS_RELATIONAL_TABLE, &pk[..pk_len]) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let Some(bn) = stage_get(exec, &key[..kn]) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_GET, bn, P_SEL_IXROW);
}

fn after_sel_ixrow(exec: &mut ExecState, result: u8, body: &[u8]) {
    match result {
        KV_RESULT_OK => {
            let mut row = [0u8; MAX_ROW_BYTES];
            let n = body.len().min(row.len());
            row[..n].copy_from_slice(&body[..n]);
            if !stage_row(exec, &row[..n]) {
                reply_simple(exec, ERR_RESULT_TOO_LARGE, TAG_EMPTY, 0);
                return;
            }
            send_next_ix_row(exec);
        }
        // An entry pointing at a missing row: index corruption —
        // reported, never skipped, because a silently shorter result
        // is indistinguishable from a correct one.
        KV_RESULT_NOT_FOUND => reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0),
        _ => reply_simple(exec, ERR_STORE, TAG_EMPTY, 0),
    }
}

// ── CREATE INDEX / DROP INDEX (Phase 6) ──────────────────────────────

/// Start a DROP INDEX: resolve the index NAME (kind Index).
fn start_index_name_lookup(exec: &mut ExecState, name: &[u8]) {
    let mut key = [0u8; MAX_KV_KEY];
    let Some(kn) = index_name_key(&mut key, name) else {
        reply_simple(exec, sql_exec::ERR_NAME_TOO_LONG, TAG_EMPTY, 0);
        return;
    };
    let Some(bn) = stage_get(exec, &key[..kn]) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_GET, bn, P_DI_NAME);
}

/// CREATE INDEX: the index-name existence probe answered.
fn after_ci_name(exec: &mut ExecState, result: u8) {
    match result {
        KV_RESULT_OK => reply_simple(exec, ERR_TABLE_EXISTS, TAG_EMPTY, 0),
        KV_RESULT_NOT_FOUND => {
            // Allocate an id. Indexes share the table allocator: one
            // catalog, one id space, no second counter to reconcile.
            let mut key = [0u8; MAX_KV_KEY];
            let Some(kn) = allocator_key(&mut key) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            let Some(bn) = stage_incr(exec, &key[..kn], 1) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            kv_send(exec, KV_OP_INCR, bn, P_CI_ALLOC);
        }
        _ => reply_simple(exec, ERR_STORE, TAG_EMPTY, 0),
    }
}

fn after_ci_alloc(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_INTEGER || body.len() < 8 {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    let counter = i64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
    if counter <= 0 || counter > u32::MAX as i64 {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    exec.index_id = counter as u32;
    // Is the table empty? One-row probe of its prefix. A populated
    // table needs the backfill worker (an index that reads incomplete
    // would return wrong absences), which lands as its own module —
    // until then this refuses BY NAME.
    let mut start = [0u8; MAX_KV_KEY];
    let mut end = [0u8; MAX_KV_KEY];
    let Some((sn, en)) = table_bounds(exec.table_id, &mut start, &mut end) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let Some(bn) = stage_range_scan(exec, &start[..sn], &end[..en], 0, 1) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_RANGE_SCAN, bn, P_CI_EMPTY);
}

fn after_ci_empty(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    let populated = u16::from_le_bytes([body[8], body[9]]) > 0;
    // Empty table: exact by construction, born Public. Populated
    // table: born Backfilling — writers maintain it from this moment
    // (write_insert_allowed), readers refuse it (read_allowed is
    // Public only), and the index_backfill worker walks the existing
    // rows and flips it Public. CONCURRENTLY-style: the statement
    // returns when the descriptor is durable, not when the build
    // finishes; the index simply doesn't serve until it is exact.
    // (Descriptor first, name last — the same crash rule CREATE TABLE
    // follows.)
    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    let Ok(Statement::CreateIndex { name, column, .. }) = parse(&text[..n], dialect_of(exec))
    else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let Ok(col) = sql_core::resolve_column(&exec.td, column) else {
        reply_simple(exec, sql_exec::ERR_UNKNOWN_COLUMN, TAG_EMPTY, 0);
        return;
    };
    let Ok(mut d) = relational::IndexDescriptor::new(
        exec.index_id,
        &exec.td,
        name,
        &[col.column_id],
        false,
        relational::IndexExactness::Synchronous,
    ) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    d.phase = if populated {
        relational::SchemaPhase::Backfilling
    } else {
        relational::SchemaPhase::Public
    };
    d.direction = relational::SchemaDirection::Add;
    let mut rec = [0u8; 512];
    let Some(rn) = d.encode(&mut rec) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let mut key = [0u8; MAX_KV_KEY];
    let Some(kn) = index_descr_key(&mut key, exec.index_id) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let Some(bn) = stage_put(exec, &key[..kn], &rec[..rn], PUT_FLAG_NX) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_PUT, bn, P_CI_DESCR);
}

fn after_ci_descr(exec: &mut ExecState, result: u8) {
    if result != KV_RESULT_OK {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    let Ok(Statement::CreateIndex { name, .. }) = parse(&text[..n], dialect_of(exec)) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let mut key = [0u8; MAX_KV_KEY];
    let Some(kn) = index_name_key(&mut key, name) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let id_be = exec.index_id.to_be_bytes();
    let Some(bn) = stage_put(exec, &key[..kn], &id_be, PUT_FLAG_NX) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_PUT, bn, P_CI_NAME_W);
}

fn after_ci_name_w(exec: &mut ExecState, result: u8) {
    match result {
        KV_RESULT_OK => reply_simple(exec, OUTCOME_OK, sql_exec::TAG_CREATE_INDEX, 0),
        types::KV_RESULT_CAS_FAILED | types::KV_RESULT_EXISTS => {
            reply_simple(exec, ERR_TABLE_EXISTS, TAG_EMPTY, 0)
        }
        _ => reply_simple(exec, ERR_STORE, TAG_EMPTY, 0),
    }
}

/// DROP INDEX: the name lookup answered.
fn after_di_name(exec: &mut ExecState, result: u8, body: &[u8]) {
    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    let Ok(Statement::DropIndex { if_exists, .. }) = parse(&text[..n], dialect_of(exec)) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    match result {
        KV_RESULT_NOT_FOUND => {
            if if_exists {
                reply_simple(exec, OUTCOME_OK, sql_exec::TAG_DROP_INDEX, 0);
            } else {
                reply_simple(exec, ERR_NO_SUCH_TABLE, TAG_EMPTY, 0);
            }
        }
        KV_RESULT_OK if body.len() >= 4 => {
            exec.index_id = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
            exec.cursor = 0;
            send_di_scan(exec);
        }
        _ => reply_simple(exec, ERR_STORE, TAG_EMPTY, 0),
    }
}

/// One page of the index's entries (whole-index prefix).
fn send_di_scan(exec: &mut ExecState) {
    let mut prefix = [0u8; MAX_KV_KEY];
    let Some(pn) = index_id_prefix_user(&mut prefix, exec.index_id) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let mut succ = [0u8; MAX_KV_KEY];
    let Some(sn) = prefix_successor(&prefix[..pn], &mut succ) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let cursor = exec.cursor;
    let Some(bn) = stage_range_scan(exec, &prefix[..pn], &succ[..sn], cursor, 32) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_RANGE_SCAN, bn, P_DI_SCAN);
}

fn after_di_scan(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    exec.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
    let count = u16::from_le_bytes([body[8], body[9]]);
    if count == 0 {
        if exec.cursor != 0 {
            send_di_scan(exec);
        } else {
            send_di_descr_delete(exec);
        }
        return;
    }
    // Delete this page's keys in one multi-key DELETE.
    let mut del = [0u8; SCRATCH_BUF];
    let mut dn = 2usize;
    del[0..2].copy_from_slice(&count.to_le_bytes());
    let mut at = 10usize;
    for _ in 0..count {
        if body.len() < at + 2 {
            reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
            return;
        }
        let klen = u16::from_le_bytes([body[at], body[at + 1]]) as usize;
        let key_end = at + 2 + klen;
        if body.len() < key_end + 4 {
            reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
            return;
        }
        let vlen =
            u32::from_le_bytes(body[key_end..key_end + 4].try_into().unwrap_or([0; 4])) as usize;
        if dn + 2 + klen > del.len() {
            reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
            return;
        }
        del[dn..dn + 2].copy_from_slice(&(klen as u16).to_le_bytes());
        del[dn + 2..dn + 2 + klen].copy_from_slice(&body[at + 2..key_end]);
        dn += 2 + klen;
        at = key_end + 4 + vlen;
    }
    let env = &mut exec.env;
    if BODY_AT + dn > env.len() {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    env[BODY_AT..BODY_AT + dn].copy_from_slice(&del[..dn]);
    kv_send(exec, KV_OP_DELETE, dn, P_DI_DEL);
}

fn after_di_del(exec: &mut ExecState, result: u8) {
    if result != KV_RESULT_OK && result != KV_RESULT_INTEGER {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    if exec.cursor != 0 {
        send_di_scan(exec);
    } else {
        send_di_descr_delete(exec);
    }
}

fn send_di_descr_delete(exec: &mut ExecState) {
    let mut key = [0u8; MAX_KV_KEY];
    let Some(kn) = index_descr_key(&mut key, exec.index_id) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let kslice: &[u8] = &key[..kn];
    let Some(bn) = stage_delete(exec, &[kslice]) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_DELETE, bn, P_DI_DESCR);
}

fn after_di_descr(exec: &mut ExecState, result: u8) {
    if result != KV_RESULT_OK && result != KV_RESULT_INTEGER && result != KV_RESULT_NOT_FOUND {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    let Ok(Statement::DropIndex { name, .. }) = parse(&text[..n], dialect_of(exec)) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let mut key = [0u8; MAX_KV_KEY];
    let Some(kn) = index_name_key(&mut key, name) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let kslice: &[u8] = &key[..kn];
    let Some(bn) = stage_delete(exec, &[kslice]) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_DELETE, bn, P_DI_NAME_W);
}

fn after_di_name_w(exec: &mut ExecState, result: u8) {
    if result == KV_RESULT_OK || result == KV_RESULT_INTEGER || result == KV_RESULT_NOT_FOUND {
        reply_simple(exec, OUTCOME_OK, sql_exec::TAG_DROP_INDEX, 0);
    } else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
    }
}

/// User-key of the whole-index scan prefix under KS_RELATIONAL_INDEX.
fn index_id_prefix_user(out: &mut [u8], index_id: u32) -> Option<usize> {
    // The user-key body is just the fixed-width id: every entry's body
    // starts with it (see relational::index_user_key).
    kv_key(
        out,
        relational::KS_RELATIONAL_INDEX,
        &index_id.to_be_bytes(),
    )
}

fn send_insert_row(exec: &mut ExecState) {
    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    let Ok(Statement::Insert(ins)) = parse(&text[..n], dialect_of(exec)) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    // Statement atomicity (§13.1): more than one row — or ANY row of a
    // table with secondary indexes (§14.4: index entries update in the
    // same transaction as primary records) — goes as ONE atomic TXN
    // command instead of a resumable PUT loop.
    if (ins.row_count() > 1 || exec.idx_count > 0) && exec.row_index == 0 {
        send_insert_txn(exec, &ins);
        return;
    }
    if exec.row_index as usize >= ins.row_count() {
        let affected = exec.affected;
        exec.m_rows_written = exec.m_rows_written.wrapping_add(affected);
        reply_simple(exec, OUTCOME_OK, TAG_INSERT, affected);
        return;
    }

    let mut bound = [BoundColumn {
        column_id: 0,
        ty: LogicalType::Null,
        value: Value::Null,
    }; MAX_COLUMNS];
    let bn = match bind_insert_row(&exec.td, &ins, exec.row_index as usize, &mut bound) {
        Ok(k) => k,
        Err(e) => {
            reply_simple(exec, error_code(e), TAG_EMPTY, 0);
            return;
        }
    };

    // Key: [keyspace][table_id][ordered key columns], in KEY order.
    let mut kv = [Value::Null; MAX_KEY_COLUMNS];
    let kn = match bound_row_key_values(&exec.td, &bound[..bn], &mut kv) {
        Ok(k) => k,
        Err(e) => {
            reply_simple(exec, error_code(e), TAG_EMPTY, 0);
            return;
        }
    };
    let mut ktypes = [LogicalType::Null; MAX_KEY_COLUMNS];
    let Some(tn) = exec.td.primary_key_types(&mut ktypes) else {
        reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
        return;
    };
    let mut keybody = [0u8; MAX_PRIMARY_KEY_LEN];
    let Some(kb) =
        relational::encode_primary_key(&mut keybody, exec.table_id, &ktypes[..tn], &kv[..kn])
    else {
        reply_simple(exec, sql_exec::ERR_LITERAL_TOO_LONG, TAG_EMPTY, 0);
        return;
    };
    let mut key = [0u8; MAX_KV_KEY];
    let Some(kk) = kv_key(&mut key, relational::KS_RELATIONAL_TABLE, &keybody[..kb]) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };

    // Value: the row.
    let mut cols = [relational::ColumnValue {
        column_id: 0,
        ty: LogicalType::Null,
        value: Value::Null,
    }; MAX_COLUMNS];
    for i in 0..bn {
        cols[i] = relational::ColumnValue {
            column_id: bound[i].column_id,
            ty: bound[i].ty,
            value: bound[i].value,
        };
    }
    let mut row = [0u8; MAX_ROW_BYTES];
    let Some(rn) = relational::encode_row(&mut row, &cols[..bn]) else {
        reply_simple(exec, sql_exec::ERR_TOO_MANY_ITEMS, TAG_EMPTY, 0);
        return;
    };

    // NX: an INSERT must not silently replace an existing row. SQL
    // spells that as a duplicate-key error, and without the flag an
    // INSERT would behave as an UPSERT — losing data the client never
    // asked to overwrite.
    let Some(bl) = stage_put(exec, &key[..kk], &row[..rn], PUT_FLAG_NX) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_PUT, bl, P_INS_ROW);
}

fn after_ins_row(exec: &mut ExecState, result: u8) {
    match result {
        KV_RESULT_OK => {
            exec.affected += 1;
            exec.row_index += 1;
            send_insert_row(exec);
        }
        // NX refused because the key is already there. The engine spells
        // that `CAS_FAILED` — the NX flag IS a compare-and-set against
        // "absent" — and for an INSERT it means exactly one thing: a row
        // already exists at this primary key.
        types::KV_RESULT_CAS_FAILED | types::KV_RESULT_EXISTS => {
            reply_simple(exec, ERR_DUPLICATE_KEY, TAG_EMPTY, 0)
        }
        _ => reply_simple(exec, ERR_STORE, TAG_EMPTY, 0),
    }
}

// ── SELECT ────────────────────────────────────────────────────────────

fn start_select(exec: &mut ExecState, s: &sql_core::Select<'_>) {
    // An aggregate/grouped query computes its result columns rather than
    // projecting stored ones, so it takes a separate path.
    if let sql_core::Projection::Aggregate { .. } = s.projection {
        start_aggregate(exec, s);
        return;
    }
    // Resolve the projection once. Each scan page then only needs the
    // column ids, not the statement text.
    let mut cols: [&ColumnDescriptor; MAX_COLUMNS] = [&ColumnDescriptor::EMPTY; MAX_COLUMNS];
    let ncols = match bind_projection(&exec.td, &s.projection, &mut cols) {
        Ok(k) => k,
        Err(e) => {
            reply_simple(exec, error_code(e), TAG_EMPTY, 0);
            return;
        }
    };
    for i in 0..ncols {
        exec.proj[i] = cols[i].column_id;
    }
    exec.proj_len = ncols;
    exec.limit = s.limit.unwrap_or(u32::MAX);
    exec.filt_count = 0;
    exec.scan_lo_len = 0;
    exec.scan_hi_len = 0;
    exec.sort_active = false;
    exec.sort_n = 0;
    exec.sort_keys_len = 0;
    exec.sort_desc = false;
    exec.distinct = s.distinct;
    exec.offset = s.offset.unwrap_or(0);
    exec.rows_len = 0;
    exec.row_count = 0;
    exec.cursor = 0;

    // Every predicate in the conjunction becomes a residual filter applied
    // to each staged row, so the answer is the full `WHERE` regardless of
    // which predicate `plan_access` picks to drive the scan.
    if let Err(e) = install_filters(exec, &s.where_) {
        reply_simple(exec, error_code(e), TAG_EMPTY, 0);
        return;
    }

    // ORDER BY on ANY column, either direction. The store need not return a
    // scan in any particular order (the memory engine does not), so the
    // result is sorted in the executor by the sort column's ORDER-
    // PRESERVING encoding, captured per row during the scan.
    if let Some(ob) = s.order {
        match sql_core::resolve_column(&exec.td, ob.column) {
            Ok(c) => {
                exec.sort_active = true;
                exec.sort_col = c.column_id;
                exec.sort_ty = c.ty;
                exec.sort_desc = ob.desc;
            }
            Err(e) => {
                reply_simple(exec, error_code(e), TAG_EMPTY, 0);
                return;
            }
        }
    }

    let access = match sql_core::plan_access(&exec.td, &s.where_) {
        Ok(a) => a,
        Err(e) => {
            reply_simple(exec, error_code(e), TAG_EMPTY, 0);
            return;
        }
    };
    match access {
        Access::PointRead(v) => {
            let mut ktypes = [LogicalType::Null; MAX_KEY_COLUMNS];
            let Some(tn) = exec.td.primary_key_types(&mut ktypes) else {
                reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
                return;
            };
            let mut keybody = [0u8; MAX_PRIMARY_KEY_LEN];
            let Some(kb) =
                relational::encode_primary_key(&mut keybody, exec.table_id, &ktypes[..tn], &[v])
            else {
                reply_simple(exec, sql_exec::ERR_LITERAL_TOO_LONG, TAG_EMPTY, 0);
                return;
            };
            let mut key = [0u8; MAX_KV_KEY];
            let Some(kk) = kv_key(&mut key, relational::KS_RELATIONAL_TABLE, &keybody[..kb]) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            let Some(bn) = stage_get(exec, &key[..kk]) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            kv_send(exec, KV_OP_GET, bn, P_SEL_POINT);
        }
        Access::KeyPrefix(v) => {
            // A leading key column of a composite key: scan the prefix
            // it pins rather than the whole table.
            let mut ktypes = [LogicalType::Null; MAX_KEY_COLUMNS];
            let Some(tn) = exec.td.primary_key_types(&mut ktypes) else {
                reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
                return;
            };
            let mut pbody = [0u8; MAX_PRIMARY_KEY_LEN];
            let Some(pb) = relational::encode_primary_key_prefix(
                &mut pbody,
                exec.table_id,
                &ktypes[..tn],
                &[v],
            ) else {
                reply_simple(exec, sql_exec::ERR_LITERAL_TOO_LONG, TAG_EMPTY, 0);
                return;
            };
            let mut start = [0u8; MAX_KV_KEY];
            let Some(sn) = kv_key(&mut start, relational::KS_RELATIONAL_TABLE, &pbody[..pb]) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            let mut end = [0u8; MAX_KV_KEY];
            let Some(en) = prefix_successor(&start[..sn], &mut end) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            send_scan(exec, &start[..sn], &end[..en], P_SEL_SCAN);
        }
        Access::KeyRange { op, value } => {
            // A range on the (single-column) primary key: turn the
            // comparison into byte bounds and scan just that slice. The
            // primary-key encoding is order-preserving, so value order is
            // key-byte order.
            let mut start = [0u8; MAX_KV_KEY];
            let mut end = [0u8; MAX_KV_KEY];
            let Some((sn, en)) = key_range_bounds(exec, op, value, &mut start, &mut end) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            send_scan(exec, &start[..sn], &end[..en], P_SEL_SCAN);
        }
        Access::FilteredScan {
            column_id,
            ty,
            op,
            value,
        } => {
            // The predicate is already installed as a residual filter, so
            // both paths below return exactly the client's WHERE. The only
            // choice here is HOW to find candidate rows.
            //
            // An exact, readable index on this column serves an EQUALITY
            // predicate in `(value, primary key)` order instead of a full
            // scan + post-filter (§14.4's reason to exist). A range
            // predicate has no such single-probe form here, so it falls
            // through to the filtered full scan. An active ORDER BY also
            // takes the full-scan path, which captures a sort key per row;
            // the index path stages through point reads and would not. A
            // multi-predicate conjunction also takes the full scan — the
            // index would narrow by one predicate but the others still need
            // the row, and the point-read fan-out is not worth it here.
            if op == sql_core::CmpOp::Eq && !exec.sort_active && exec.filt_count == 1 {
                if let Some(d) = index_on(exec, column_id) {
                    let index_id = d.index_id;
                    let mut valbuf = [0u8; relational::MAX_INDEX_VALUE_LEN];
                    let Some(ivn) = relational::encode_index_value(&mut valbuf, &[ty], &[value])
                    else {
                        reply_simple(exec, sql_exec::ERR_TYPE_MISMATCH, TAG_EMPTY, 0);
                        return;
                    };
                    exec.index_id = index_id;
                    exec.filter_ix[..ivn].copy_from_slice(&valbuf[..ivn]);
                    exec.filter_ix_len = ivn as u16;
                    exec.cursor = 0;
                    send_ix_scan(exec);
                    return;
                }
            }
            start_full_scan(exec);
        }
        Access::FullScan => start_full_scan(exec),
    }
}

/// Stable byte code for a comparison operator, stored in `filter_op`.
fn cmp_discriminant(op: sql_core::CmpOp) -> u8 {
    match op {
        sql_core::CmpOp::Eq => 0,
        sql_core::CmpOp::Lt => 1,
        sql_core::CmpOp::Le => 2,
        sql_core::CmpOp::Gt => 3,
        sql_core::CmpOp::Ge => 4,
        sql_core::CmpOp::IsNull => 5,
        sql_core::CmpOp::IsNotNull => 6,
    }
}

/// Byte bounds `[start, end)` for a range on the single-column primary
/// key. The key encoding is order-preserving, so value order is key order
/// and the comparison becomes a byte slice of the table's key space.
///
/// `> x` starts one byte past `K(x)` (the immediate byte successor,
/// `K(x)` with a trailing `0x00`) so `K(x)` itself is excluded while every
/// proper extension of it — a longer key that sorts after `x` — is kept.
/// `<= x` ends at that same successor so `K(x)` is included and its
/// extensions are not.
fn key_range_bounds(
    exec: &ExecState,
    op: sql_core::CmpOp,
    value: relational::Value<'_>,
    start: &mut [u8],
    end: &mut [u8],
) -> Option<(usize, usize)> {
    let mut lo = [0u8; MAX_KV_KEY];
    let mut hi = [0u8; MAX_KV_KEY];
    let (lon, hin) = table_bounds(exec.table_id, &mut lo, &mut hi)?;

    // K(value): the wrapped row key for this exact primary-key value.
    let mut ktypes = [LogicalType::Null; MAX_KEY_COLUMNS];
    let tn = exec.td.primary_key_types(&mut ktypes)?;
    let mut kbody = [0u8; MAX_PRIMARY_KEY_LEN];
    let kb = relational::encode_primary_key(&mut kbody, exec.table_id, &ktypes[..tn], &[value])?;
    let mut k = [0u8; MAX_KV_KEY];
    let kn = kv_key(&mut k, relational::KS_RELATIONAL_TABLE, &kbody[..kb])?;
    if kn + 1 > k.len() {
        return None;
    }

    // A copy of K(value) with a trailing 0x00: the smallest key strictly
    // greater than K(value).
    let mut ksucc = [0u8; MAX_KV_KEY];
    ksucc[..kn].copy_from_slice(&k[..kn]);
    ksucc[kn] = 0;
    let ksn = kn + 1;

    let (s, e): (&[u8], &[u8]) = match op {
        sql_core::CmpOp::Gt => (&ksucc[..ksn], &hi[..hin]),
        sql_core::CmpOp::Ge => (&k[..kn], &hi[..hin]),
        sql_core::CmpOp::Lt => (&lo[..lon], &k[..kn]),
        sql_core::CmpOp::Le => (&lo[..lon], &ksucc[..ksn]),
        // Equality never reaches here: bind_access maps `= x` on a
        // single-column key to a PointRead, not a KeyRange. Null tests are
        // filters, never key bounds — also unreachable.
        sql_core::CmpOp::Eq | sql_core::CmpOp::IsNull | sql_core::CmpOp::IsNotNull => {
            (&k[..kn], &ksucc[..ksn])
        }
    };
    if s.len() > start.len() || e.len() > end.len() {
        return None;
    }
    start[..s.len()].copy_from_slice(s);
    end[..e.len()].copy_from_slice(e);
    Some((s.len(), e.len()))
}

fn start_full_scan(exec: &mut ExecState) {
    let mut start = [0u8; MAX_KV_KEY];
    let mut end = [0u8; MAX_KV_KEY];
    let Some((sn, en)) = table_bounds(exec.table_id, &mut start, &mut end) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    send_scan(exec, &start[..sn], &end[..en], P_SEL_SCAN);
}

fn send_scan(exec: &mut ExecState, start: &[u8], end: &[u8], phase: u8) {
    send_scan_paged(exec, start, end, phase, SCAN_PAGE);
}

/// `send_scan` with an explicit page size. A DELETE/UPDATE must scan in
/// pages no larger than the per-transaction write cap (`DELETE_PAGE`):
/// UPDATE advances its cursor by the whole page, so a page bigger than the
/// cap would leave the rows past the cap unwritten AND skipped.
fn send_scan_paged(exec: &mut ExecState, start: &[u8], end: &[u8], phase: u8, page: u16) {
    // Remember the bounds so every continuation page reproduces them.
    // Without this, paging a prefix or range scan would silently widen to
    // the whole table on the second page.
    if start.len() <= exec.scan_lo.len() && end.len() <= exec.scan_hi.len() {
        exec.scan_lo[..start.len()].copy_from_slice(start);
        exec.scan_lo_len = start.len() as u16;
        exec.scan_hi[..end.len()].copy_from_slice(end);
        exec.scan_hi_len = end.len() as u16;
    }
    let cursor = exec.cursor;
    let Some(bn) = stage_range_scan(exec, start, end, cursor, page) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_RANGE_SCAN, bn, phase);
}

/// Rebuild the scan bounds for the statement in flight. Cheaper than
/// storing them, and guaranteed consistent with the first page's bounds
/// because it is the same computation.
fn current_scan_bounds(
    exec: &ExecState,
    start: &mut [u8],
    end: &mut [u8],
) -> Option<(usize, usize)> {
    // The bounds were pinned by `send_scan` when the first page went out;
    // reuse them verbatim so a prefix or range scan does not widen mid-run.
    if exec.scan_lo_len != 0 || exec.scan_hi_len != 0 {
        let sn = exec.scan_lo_len as usize;
        let en = exec.scan_hi_len as usize;
        if sn <= start.len() && en <= end.len() {
            start[..sn].copy_from_slice(&exec.scan_lo[..sn]);
            end[..en].copy_from_slice(&exec.scan_hi[..en]);
            return Some((sn, en));
        }
    }
    table_bounds(exec.table_id, start, end)
}

/// Append one projected row to the staging buffer.
///
/// Returns `false` when the row does not fit, which ends the statement
/// with `ERR_RESULT_TOO_LARGE`. Stopping is right: a truncated result
/// set is indistinguishable from a complete one to the client.
fn stage_row(exec: &mut ExecState, row: &[u8]) -> bool {
    let mut cell = [0u8; relational::MAX_PLAIN_VALUE_LEN];
    for i in 0..exec.proj_len {
        let col_id = exec.proj[i];
        let Some(col) = exec.td.column(col_id) else {
            return false;
        };
        let (marker, cn) = match relational::row_lookup(row, col_id, col.ty) {
            // Present and non-NULL: re-encode into the response cell.
            Some(relational::ColumnLookup::Present(v)) if !v.is_null() => {
                match relational::encode_value_plain(&mut cell, col.ty, v) {
                    Some(n) => (n as u32, n),
                    None => return false,
                }
            }
            // Present-and-NULL and ABSENT both render as SQL NULL, but
            // they are different facts: absent means the row predates the
            // column. With no column defaults implemented the rendering
            // is the same, and conflating them here would be wrong the
            // moment defaults exist — so the arms stay separate.
            Some(relational::ColumnLookup::Present(_)) => (sql_exec::NULL_CELL, 0),
            Some(relational::ColumnLookup::Absent) => (sql_exec::NULL_CELL, 0),
            None => return false,
        };
        if exec.rows_len + 4 + cn > exec.rows.len() {
            return false;
        }
        let at = exec.rows_len;
        exec.rows[at..at + 4].copy_from_slice(&marker.to_le_bytes());
        exec.rows_len += 4;
        if marker != sql_exec::NULL_CELL {
            let at = exec.rows_len;
            exec.rows[at..at + cn].copy_from_slice(&cell[..cn]);
            exec.rows_len += cn;
        }
    }
    exec.row_count += 1;
    true
}

/// True when the result must be fully staged before it is shaped, so the
/// in-scan LIMIT early-stop must NOT fire: DISTINCT dedupes across all
/// rows, ORDER BY sorts them, and OFFSET counts from the sorted front.
fn defer_limit(exec: &ExecState) -> bool {
    exec.sort_active || exec.distinct || exec.offset > 0
}

/// Record one staged row's sort key — the ORDER BY column's order-
/// preserving encoding, so a byte comparison orders rows exactly as the
/// column's values do. Returns `false` when the sort bounds are exceeded,
/// which fails the statement closed rather than dropping or mis-ordering.
fn capture_sort_key(exec: &mut ExecState, key: &[u8]) -> bool {
    let i = exec.sort_n as usize;
    let at = exec.sort_keys_len as usize;
    if i >= MAX_SORT_ROWS || at + key.len() > exec.sort_keys.len() {
        return false;
    }
    exec.sort_keys[at..at + key.len()].copy_from_slice(key);
    exec.sort_key_off[i] = at as u32;
    exec.sort_key_len[i] = key.len() as u16;
    exec.sort_keys_len = (at + key.len()) as u32;
    exec.sort_n += 1;
    true
}

/// Shape the staged result: DISTINCT, then ORDER BY, then OFFSET, then
/// LIMIT — SQL's evaluation order. Rewrites `exec.rows`/`row_count` in
/// place (through `exec.env` as scratch). Returns `false` when the staged
/// result exceeds the sort/dedupe bound, failing closed.
///
/// The staged `rows` buffer is a flat run of `row_count` rows, each
/// `proj_len` cells in `push_cell` encoding; boundaries are re-derived by
/// walking it. Everything downstream works on an index permutation of the
/// original rows, so a captured sort key (indexed by original position)
/// stays aligned through the dedupe and the sort.
fn finalize_result(exec: &mut ExecState) -> bool {
    let count = exec.row_count as usize;
    if count > MAX_SORT_ROWS {
        return false;
    }
    // Row boundaries.
    let mut off = [0u32; MAX_SORT_ROWS];
    let mut len = [0u32; MAX_SORT_ROWS];
    let mut at = 0usize;
    for r in 0..count {
        let start = at;
        for _ in 0..exec.proj_len {
            let Some(m) = exec.rows.get(at..at + 4) else {
                return false;
            };
            let marker = u32::from_le_bytes([m[0], m[1], m[2], m[3]]);
            at += 4;
            if marker != sql_exec::NULL_CELL {
                at += marker as usize;
            }
        }
        off[r] = start as u32;
        len[r] = (at - start) as u32;
    }

    // The surviving row indices, in their original (scan) order.
    let mut idx = [0u16; MAX_SORT_ROWS];
    let mut nidx = 0usize;
    for r in 0..count {
        if exec.distinct {
            // Keep only the first occurrence of each identical row.
            let (ro, rl) = (off[r] as usize, len[r] as usize);
            let dup = (0..nidx).any(|k| {
                let kr = idx[k] as usize;
                len[kr] == len[r]
                    && exec.rows[off[kr] as usize..off[kr] as usize + rl] == exec.rows[ro..ro + rl]
            });
            if dup {
                continue;
            }
        }
        idx[nidx] = r as u16;
        nidx += 1;
    }

    // ORDER BY: stable insertion sort of the index list by the captured
    // sort key (order-preserving bytes), reversed for DESC.
    if exec.sort_active && exec.sort_n as usize == count {
        let key_of = |r: usize| -> &[u8] {
            let o = exec.sort_key_off[r] as usize;
            let l = exec.sort_key_len[r] as usize;
            &exec.sort_keys[o..o + l]
        };
        for i in 1..nidx {
            let cur = idx[i];
            let mut j = i;
            while j > 0 {
                let a = key_of(idx[j - 1] as usize);
                let b = key_of(cur as usize);
                let before = if exec.sort_desc { a < b } else { a > b };
                if !before {
                    break;
                }
                idx[j] = idx[j - 1];
                j -= 1;
            }
            idx[j] = cur;
        }
    }

    // OFFSET then LIMIT over the shaped index list.
    let start = (exec.offset as usize).min(nidx);
    let end = if exec.limit == u32::MAX {
        nidx
    } else {
        (start + exec.limit as usize).min(nidx)
    };

    // Emit the surviving rows in order into `env`, then copy back.
    let mut dn = 0usize;
    let mut rows_out = 0u16;
    for &ri in &idx[start..end] {
        let (o, l) = (off[ri as usize] as usize, len[ri as usize] as usize);
        if dn + l > exec.env.len() {
            return false;
        }
        exec.env[dn..dn + l].copy_from_slice(&exec.rows[o..o + l]);
        dn += l;
        rows_out += 1;
    }
    exec.rows[..dn].copy_from_slice(&exec.env[..dn]);
    exec.rows_len = dn;
    exec.row_count = rows_out;
    true
}

/// Order two same-type values. `None` when they are incomparable — a
/// float NaN, or a type pairing that should never occur for one column.
/// The plain encoding is little-endian and therefore NOT byte-orderable,
/// so range predicates compare decoded values, not their bytes.
fn cmp_values(a: relational::Value<'_>, b: relational::Value<'_>) -> Option<core::cmp::Ordering> {
    use relational::Value as V;
    match (a, b) {
        (V::Boolean(x), V::Boolean(y)) => Some(x.cmp(&y)),
        (V::SmallInt(x), V::SmallInt(y)) => Some(x.cmp(&y)),
        (V::Int(x), V::Int(y)) => Some(x.cmp(&y)),
        (V::BigInt(x), V::BigInt(y)) => Some(x.cmp(&y)),
        (V::Float(x), V::Float(y)) => x.partial_cmp(&y),
        (V::Double(x), V::Double(y)) => x.partial_cmp(&y),
        (
            V::Decimal {
                unscaled: xu,
                scale: xs,
            },
            V::Decimal {
                unscaled: yu,
                scale: ys,
            },
        ) => {
            // Same column ⇒ same declared scale in the common case; be
            // robust to a mismatch by scaling the shallower operand up.
            if xs == ys {
                Some(xu.cmp(&yu))
            } else if xs < ys {
                let f = 10i128.checked_pow((ys - xs) as u32)?;
                Some(xu.checked_mul(f)?.cmp(&yu))
            } else {
                let f = 10i128.checked_pow((xs - ys) as u32)?;
                Some(xu.cmp(&yu.checked_mul(f)?))
            }
        }
        (V::Text(x), V::Text(y)) | (V::Bytes(x), V::Bytes(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Resolve and encode a `WHERE` — its conjunct predicates and any `IN`
/// list — into the residual-filter slots, checked against each scan row.
fn install_filters(
    exec: &mut ExecState,
    w: &sql_core::Where<'_>,
) -> Result<(), sql_core::SqlError> {
    exec.in_active = false;
    if w.in_active {
        let col = sql_core::resolve_column(&exec.td, w.in_col)?;
        exec.in_col = col.column_id;
        exec.in_negate = w.in_negate;
        exec.in_count = w.in_count as u8;
        for i in 0..w.in_count {
            let value = sql_core::bind_value(w.in_vals[i], col.ty, col.nullable)?;
            let mut vbuf = [0u8; relational::MAX_PLAIN_VALUE_LEN];
            let Some(vn) = relational::encode_value_plain(&mut vbuf, col.ty, value) else {
                return Err(sql_core::SqlError::TypeMismatch);
            };
            exec.in_val_len[i] = vn as u16;
            exec.in_vals[i][..vn].copy_from_slice(&vbuf[..vn]);
        }
        exec.in_active = true;
    }
    let preds = w.as_slice();
    exec.filt_count = 0;
    for pred in preds {
        let col = sql_core::resolve_column(&exec.td, pred.column)?;
        let i = exec.filt_count as usize;
        exec.filt_col[i] = col.column_id;
        exec.filt_op[i] = cmp_discriminant(pred.op);
        // A null test carries no value to encode.
        if matches!(
            pred.op,
            sql_core::CmpOp::IsNull | sql_core::CmpOp::IsNotNull
        ) {
            exec.filt_len[i] = 0;
        } else {
            let value = sql_core::bind_value(pred.value, col.ty, col.nullable)?;
            let mut vbuf = [0u8; relational::MAX_PLAIN_VALUE_LEN];
            let Some(vn) = relational::encode_value_plain(&mut vbuf, col.ty, value) else {
                return Err(sql_core::SqlError::TypeMismatch);
            };
            exec.filt_len[i] = vn as u16;
            exec.filt_buf[i][..vn].copy_from_slice(&vbuf[..vn]);
        }
        exec.filt_count += 1;
    }
    Ok(())
}

/// Does `row` satisfy every residual predicate (their conjunction) and the
/// optional IN list?
fn passes_filter(exec: &ExecState, row: &[u8]) -> bool {
    for i in 0..exec.filt_count as usize {
        if !passes_one(exec, i, row) {
            return false;
        }
    }
    if exec.in_active && !passes_in(exec, row) {
        return false;
    }
    true
}

/// Membership test for `col [NOT] IN (…)`. A NULL/absent column matches
/// nothing (SQL three-valued logic), so it fails IN and — because the
/// value is UNKNOWN, not FALSE — fails NOT IN too.
fn passes_in(exec: &ExecState, row: &[u8]) -> bool {
    let Some(col) = exec.td.column(exec.in_col) else {
        return false;
    };
    let mut cell = [0u8; relational::MAX_PLAIN_VALUE_LEN];
    let cn = match relational::row_lookup(row, exec.in_col, col.ty) {
        Some(relational::ColumnLookup::Present(v)) if !v.is_null() => {
            match relational::encode_value_plain(&mut cell, col.ty, v) {
                Some(n) => n,
                None => return false,
            }
        }
        _ => return false,
    };
    let mut found = false;
    for i in 0..exec.in_count as usize {
        let l = exec.in_val_len[i] as usize;
        if l == cn && exec.in_vals[i][..l] == cell[..cn] {
            found = true;
            break;
        }
    }
    if exec.in_negate {
        !found
    } else {
        found
    }
}

fn passes_one(exec: &ExecState, i: usize, row: &[u8]) -> bool {
    let filt_col = exec.filt_col[i];
    let Some(col) = exec.td.column(filt_col) else {
        return false;
    };
    // Null tests are the only predicates true OF a NULL/absent value, so
    // they are decided before the present-value path.
    let present = matches!(
        relational::row_lookup(row, filt_col, col.ty),
        Some(relational::ColumnLookup::Present(v)) if !v.is_null()
    );
    match exec.filt_op[i] {
        5 => return !present, // IS NULL: absent or NULL
        6 => return present,  // IS NOT NULL
        _ => {}
    }
    match relational::row_lookup(row, filt_col, col.ty) {
        Some(relational::ColumnLookup::Present(v)) if !v.is_null() => {
            // Decode the stored filter literal back to a value and compare
            // the two by type. Equality could byte-compare the canonical
            // encoding, but `<`/`>` cannot, so all five operators share the
            // one typed path.
            let Some((fv, _)) = relational::decode_value_plain(
                &exec.filt_buf[i][..exec.filt_len[i] as usize],
                col.ty,
            ) else {
                return false;
            };
            let Some(ord) = cmp_values(v, fv) else {
                return false;
            };
            use core::cmp::Ordering::{Equal, Greater, Less};
            match exec.filt_op[i] {
                0 => ord == Equal,   // Eq
                1 => ord == Less,    // Lt
                2 => ord != Greater, // Le
                3 => ord == Greater, // Gt
                4 => ord != Less,    // Ge
                _ => false,
            }
        }
        // `col OP value` is never TRUE of NULL. SQL three-valued logic
        // makes the comparison UNKNOWN, and a WHERE keeps only rows
        // where it is TRUE.
        _ => false,
    }
}

fn after_sel_scan(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        diag(exec, b"[rex] scan bad", result, body.len() as u64);
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    let mut c = [0u8; 8];
    c.copy_from_slice(&body[0..8]);
    let next_cursor = u64::from_le_bytes(c);
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;

    let mut p = 10usize;
    for _ in 0..count {
        if body.len() < p + 2 {
            reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
            return;
        }
        let klen = u16::from_le_bytes([body[p], body[p + 1]]) as usize;
        let key_off = p + 2;
        p += 2 + klen;
        if body.len() < p + 4 {
            reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
            return;
        }
        let vlen = u32::from_le_bytes([body[p], body[p + 1], body[p + 2], body[p + 3]]) as usize;
        p += 4;
        if body.len() < p + vlen {
            reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
            return;
        }
        // Copy the row out: `stage_row` borrows `exec` mutably.
        let mut row = [0u8; MAX_ROW_BYTES];
        if vlen > row.len() {
            reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
            return;
        }
        row[..vlen].copy_from_slice(&body[p..p + vlen]);
        p += vlen;

        if !passes_filter(exec, &row[..vlen]) {
            continue;
        }
        let _ = key_off;
        // ORDER BY / DISTINCT / OFFSET all need every matching row staged
        // before the result is shaped, so the in-scan LIMIT early-stop is
        // disabled for them. Without any of those the store's order is the
        // answer and the limit can stop the scan early.
        if !defer_limit(exec) && u64::from(exec.row_count) >= u64::from(exec.limit) {
            reply_rows(exec);
            return;
        }
        // Capture the sort key — the ORDER BY column's order-preserving
        // encoding — BEFORE staging, so its index lines up with the row.
        if exec.sort_active {
            let mut skey = [0u8; relational::MAX_ORDERED_VALUE_LEN];
            let sn = match relational::row_lookup(&row[..vlen], exec.sort_col, exec.sort_ty) {
                Some(relational::ColumnLookup::Present(v)) => {
                    relational::encode_value_ordered(&mut skey, exec.sort_ty, v)
                }
                // Absent/NULL: the NULL key, which sorts first (last under
                // DESC) — a defined, stable place, never interleaved.
                _ => relational::encode_value_ordered(
                    &mut skey,
                    exec.sort_ty,
                    relational::Value::Null,
                ),
            };
            let Some(sn) = sn else {
                reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
                return;
            };
            if !capture_sort_key(exec, &skey[..sn]) {
                reply_simple(exec, ERR_RESULT_TOO_LARGE, TAG_EMPTY, 0);
                return;
            }
        }
        if !stage_row(exec, &row[..vlen]) {
            reply_simple(exec, ERR_RESULT_TOO_LARGE, TAG_EMPTY, 0);
            return;
        }
    }

    if next_cursor == 0
        || (!defer_limit(exec) && u64::from(exec.row_count) >= u64::from(exec.limit))
    {
        reply_rows(exec);
        return;
    }
    exec.cursor = next_cursor;
    let mut start = [0u8; MAX_KV_KEY];
    let mut end = [0u8; MAX_KV_KEY];
    // A prefix scan and a full scan differ only in bounds, and the
    // bounds are recomputed identically each page, so one continuation
    // path serves both.
    let Some((sn, en)) = current_scan_bounds(exec, &mut start, &mut end) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    send_scan(exec, &start[..sn], &end[..en], P_SEL_SCAN);
}

fn after_sel_point(exec: &mut ExecState, result: u8, body: &[u8]) {
    match result {
        KV_RESULT_OK => {
            let mut row = [0u8; MAX_ROW_BYTES];
            let n = body.len().min(row.len());
            row[..n].copy_from_slice(&body[..n]);
            // A point read pins the row by its primary key, but the
            // conjunction may carry further predicates on other columns —
            // apply them, or the extra clauses would be silently dropped.
            if passes_filter(exec, &row[..n]) && !stage_row(exec, &row[..n]) {
                reply_simple(exec, ERR_RESULT_TOO_LARGE, TAG_EMPTY, 0);
                return;
            }
            reply_rows(exec);
        }
        // No row at that key: an EMPTY result set, not an error. A
        // `SELECT` that matches nothing succeeded.
        KV_RESULT_NOT_FOUND => reply_rows(exec),
        _ => reply_simple(exec, ERR_STORE, TAG_EMPTY, 0),
    }
}

// ── SELECT with aggregates / GROUP BY ─────────────────────────────────

fn is_numeric(ty: LogicalType) -> bool {
    matches!(
        ty,
        LogicalType::SmallInt
            | LogicalType::Int
            | LogicalType::BigInt
            | LogicalType::Float
            | LogicalType::Double
    )
}

/// Read a column's value as an `f64` for numeric folding. `None` for
/// absent, NULL, or non-numeric — those do not contribute to SUM/AVG/etc.
fn value_as_f64(row: &[u8], col_id: u16, ty: LogicalType) -> Option<f64> {
    match relational::row_lookup(row, col_id, ty) {
        Some(relational::ColumnLookup::Present(v)) if !v.is_null() => match v {
            relational::Value::SmallInt(x) => Some(x as f64),
            relational::Value::Int(x) => Some(x as f64),
            relational::Value::BigInt(x) => Some(x as f64),
            relational::Value::Float(x) => Some(x as f64),
            relational::Value::Double(x) => Some(x),
            _ => None,
        },
        _ => None,
    }
}

fn agg_disc(f: sql_core::AggFunc) -> u8 {
    match f {
        sql_core::AggFunc::Count => AGG_COUNT,
        sql_core::AggFunc::Sum => AGG_SUM,
        sql_core::AggFunc::Min => AGG_MIN,
        sql_core::AggFunc::Max => AGG_MAX,
        sql_core::AggFunc::Avg => AGG_AVG,
    }
}

fn default_agg_name(func: u8) -> &'static [u8] {
    match func {
        AGG_COUNT => b"count",
        AGG_SUM => b"sum",
        AGG_MIN => b"min",
        AGG_MAX => b"max",
        AGG_AVG => b"avg",
        _ => b"?column?",
    }
}

fn set_item_name(exec: &mut ExecState, i: usize, name: &[u8]) {
    let n = name.len().min(32);
    exec.agg_item_name[i][..n].copy_from_slice(&name[..n]);
    exec.agg_item_name_len[i] = n as u8;
}

fn start_aggregate(exec: &mut ExecState, s: &sql_core::Select<'_>) {
    let sql_core::Projection::Aggregate { items, count } = &s.projection else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    if *count > MAX_AGG_ITEMS {
        reply_simple(
            exec,
            error_code(sql_core::SqlError::TooManyItems),
            TAG_EMPTY,
            0,
        );
        return;
    }
    // Result-shaping over a grouped result is a follow-on; refuse it by
    // name rather than silently return groups unshaped (an ignored LIMIT
    // returns more rows than asked, an ignored ORDER BY the wrong order).
    if s.order.is_some() || s.distinct || s.offset.is_some() || s.limit.is_some() {
        reply_simple(
            exec,
            error_code(sql_core::SqlError::Unsupported),
            TAG_EMPTY,
            0,
        );
        return;
    }
    // GROUP BY column.
    exec.agg_group_col = 0;
    exec.agg_group_ty = LogicalType::Null;
    if let Some(g) = s.group_by {
        match sql_core::resolve_column(&exec.td, g) {
            Ok(c) => {
                exec.agg_group_col = c.column_id;
                exec.agg_group_ty = c.ty;
            }
            Err(e) => {
                reply_simple(exec, error_code(e), TAG_EMPTY, 0);
                return;
            }
        }
    }
    // Bind each projection item.
    exec.agg_n = *count as u8;
    for i in 0..*count {
        let it = items[i];
        match it.func {
            None => {
                // A bare group column.
                let c = match sql_core::resolve_column(&exec.td, it.column) {
                    Ok(c) => c,
                    Err(e) => {
                        reply_simple(exec, error_code(e), TAG_EMPTY, 0);
                        return;
                    }
                };
                exec.agg_item_func[i] = AGG_NONE;
                exec.agg_item_col[i] = c.column_id;
                exec.agg_item_ty[i] = c.ty;
                let name = it.alias.unwrap_or(it.column);
                set_item_name(exec, i, name);
            }
            Some(func) => {
                let d = agg_disc(func);
                exec.agg_item_func[i] = d;
                if it.star {
                    exec.agg_item_col[i] = 0; // COUNT(*): no column
                    exec.agg_item_ty[i] = LogicalType::BigInt;
                } else {
                    let c = match sql_core::resolve_column(&exec.td, it.column) {
                        Ok(c) => c,
                        Err(e) => {
                            reply_simple(exec, error_code(e), TAG_EMPTY, 0);
                            return;
                        }
                    };
                    // SUM/MIN/MAX/AVG need a numeric column; COUNT(col)
                    // counts non-nulls of any type.
                    if d != AGG_COUNT && !is_numeric(c.ty) {
                        reply_simple(
                            exec,
                            error_code(sql_core::SqlError::Unsupported),
                            TAG_EMPTY,
                            0,
                        );
                        return;
                    }
                    exec.agg_item_col[i] = c.column_id;
                    exec.agg_item_ty[i] = c.ty;
                }
                let name = it.alias.unwrap_or_else(|| default_agg_name(d));
                set_item_name(exec, i, name);
            }
        }
    }
    if let Err(e) = install_filters(exec, &s.where_) {
        reply_simple(exec, error_code(e), TAG_EMPTY, 0);
        return;
    }
    exec.agg_active = true;
    exec.agg_grp_count = 0;
    exec.rows_len = 0;
    exec.row_count = 0;
    exec.cursor = 0;
    // A global aggregate (no GROUP BY) always returns exactly one row, even
    // over an empty table — pre-create group 0 so COUNT is 0, not no-row.
    if exec.agg_group_col == 0 {
        exec.agg_grp_count = 1;
        exec.agg_grp_key_len[0] = 0;
        clear_group_acc(exec, 0);
    }
    let mut start = [0u8; MAX_KV_KEY];
    let mut end = [0u8; MAX_KV_KEY];
    let Some((sn, en)) = table_bounds(exec.table_id, &mut start, &mut end) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    send_scan(exec, &start[..sn], &end[..en], P_AGG_SCAN);
}

fn clear_group_acc(exec: &mut ExecState, g: usize) {
    for i in 0..MAX_AGG_ITEMS {
        let b = g * MAX_AGG_ITEMS + i;
        exec.acc_count[b] = 0;
        exec.acc_sum[b] = 0.0;
        exec.acc_min[b] = 0.0;
        exec.acc_max[b] = 0.0;
        exec.acc_seen[b] = false;
    }
}

/// Find the group for `row`, creating it if new. Returns its index, or
/// `None` if the group cap is exceeded (the statement then fails closed).
fn group_of(exec: &mut ExecState, row: &[u8]) -> Option<usize> {
    if exec.agg_group_col == 0 {
        return Some(0); // pre-created global group
    }
    // Encode the group value canonically; equal values encode equally.
    let mut key = [0u8; AGG_KEY_MAX];
    let klen = match relational::row_lookup(row, exec.agg_group_col, exec.agg_group_ty) {
        Some(relational::ColumnLookup::Present(v)) => {
            relational::encode_value_plain(&mut key, exec.agg_group_ty, v)?
        }
        // Absent/NULL groups together under the NULL key.
        _ => relational::encode_value_plain(&mut key, exec.agg_group_ty, relational::Value::Null)?,
    };
    for g in 0..exec.agg_grp_count as usize {
        if exec.agg_grp_key_len[g] as usize == klen && exec.agg_grp_key[g][..klen] == key[..klen] {
            return Some(g);
        }
    }
    let g = exec.agg_grp_count as usize;
    if g >= MAX_GROUPS || klen > AGG_KEY_MAX {
        return None;
    }
    exec.agg_grp_key[g][..klen].copy_from_slice(&key[..klen]);
    exec.agg_grp_key_len[g] = klen as u16;
    exec.agg_grp_count += 1;
    clear_group_acc(exec, g);
    Some(g)
}

fn fold_row(exec: &mut ExecState, row: &[u8]) -> bool {
    let Some(g) = group_of(exec, row) else {
        return false; // group cap exceeded
    };
    for i in 0..exec.agg_n as usize {
        let b = g * MAX_AGG_ITEMS + i;
        match exec.agg_item_func[i] {
            AGG_NONE => {}
            AGG_COUNT => {
                if exec.agg_item_col[i] == 0 {
                    exec.acc_count[b] += 1; // COUNT(*)
                } else if matches!(
                    relational::row_lookup(row, exec.agg_item_col[i], exec.agg_item_ty[i]),
                    Some(relational::ColumnLookup::Present(v)) if !v.is_null()
                ) {
                    exec.acc_count[b] += 1; // COUNT(col): non-nulls
                }
            }
            _ => {
                if let Some(v) = value_as_f64(row, exec.agg_item_col[i], exec.agg_item_ty[i]) {
                    if !exec.acc_seen[b] {
                        exec.acc_min[b] = v;
                        exec.acc_max[b] = v;
                        exec.acc_seen[b] = true;
                    } else {
                        if v < exec.acc_min[b] {
                            exec.acc_min[b] = v;
                        }
                        if v > exec.acc_max[b] {
                            exec.acc_max[b] = v;
                        }
                    }
                    exec.acc_sum[b] += v;
                    exec.acc_count[b] += 1;
                }
            }
        }
    }
    true
}

fn after_agg_scan(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    let next_cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    let mut p = 10usize;
    for _ in 0..count {
        let Some(klen) = body
            .get(p..p + 2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
        else {
            reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
            return;
        };
        p += 2 + klen;
        let Some(vlen) = body
            .get(p..p + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
        else {
            reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
            return;
        };
        p += 4;
        if p + vlen > body.len() {
            reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
            return;
        }
        let mut rowbuf = [0u8; MAX_ROW_BYTES];
        if vlen > rowbuf.len() {
            reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
            return;
        }
        rowbuf[..vlen].copy_from_slice(&body[p..p + vlen]);
        p += vlen;
        if passes_filter(exec, &rowbuf[..vlen]) && !fold_row(exec, &rowbuf[..vlen]) {
            reply_simple(exec, ERR_RESULT_TOO_LARGE, TAG_EMPTY, 0);
            return;
        }
    }
    if next_cursor == 0 {
        reply_agg(exec);
        return;
    }
    exec.cursor = next_cursor;
    let mut start = [0u8; MAX_KV_KEY];
    let mut end = [0u8; MAX_KV_KEY];
    let Some((sn, en)) = current_scan_bounds(exec, &mut start, &mut end) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    send_scan(exec, &start[..sn], &end[..en], P_AGG_SCAN);
}

/// The result type of an aggregate item, for the row-description header.
fn agg_result_ty(exec: &ExecState, i: usize) -> LogicalType {
    match exec.agg_item_func[i] {
        AGG_NONE => exec.agg_item_ty[i],
        AGG_COUNT => LogicalType::BigInt,
        AGG_AVG => LogicalType::Double,
        AGG_SUM => {
            if matches!(
                exec.agg_item_ty[i],
                LogicalType::Float | LogicalType::Double
            ) {
                LogicalType::Double
            } else {
                LogicalType::BigInt
            }
        }
        // MIN/MAX keep the element type.
        _ => exec.agg_item_ty[i],
    }
}

/// Append one aggregate result cell to the staging buffer (`push_cell`
/// wire form: a 4-byte marker then the payload; `None` is SQL NULL).
fn agg_stage_cell(exec: &mut ExecState, cell: Option<&[u8]>) -> bool {
    let payload = cell.unwrap_or(&[]);
    if exec.rows_len + 4 + payload.len() > exec.rows.len() {
        return false;
    }
    let marker: u32 = match cell {
        None => sql_exec::NULL_CELL,
        Some(_) => payload.len() as u32,
    };
    let at = exec.rows_len;
    exec.rows[at..at + 4].copy_from_slice(&marker.to_le_bytes());
    exec.rows_len += 4;
    if cell.is_some() {
        let at = exec.rows_len;
        exec.rows[at..at + payload.len()].copy_from_slice(payload);
        exec.rows_len += payload.len();
    }
    true
}

fn reply_agg(exec: &mut ExecState) {
    let (corr, conn, from_mysql) = (exec.corr_id, exec.conn_id, exec.from_mysql);
    let n = exec.agg_n as usize;
    let groups = exec.agg_grp_count as usize;

    // Stage every group's cells into the row buffer first, so the response
    // builder (which borrows `resp`) never overlaps a read of the
    // accumulators. Each cell is computed into a local buffer, then staged.
    exec.rows_len = 0;
    for g in 0..groups {
        for i in 0..n {
            let base = g * MAX_AGG_ITEMS + i;
            let mut buf = [0u8; AGG_KEY_MAX];
            let staged = match exec.agg_item_func[i] {
                AGG_NONE => {
                    let kl = exec.agg_grp_key_len[g] as usize;
                    buf[..kl].copy_from_slice(&exec.agg_grp_key[g][..kl]);
                    agg_stage_cell(exec, Some(&buf[..kl]))
                }
                AGG_COUNT => match encode_i64_cell(&mut buf, exec.acc_count[base] as i64) {
                    Some(k) => agg_stage_cell(exec, Some(&buf[..k])),
                    None => false,
                },
                AGG_AVG => {
                    if exec.acc_count[base] == 0 {
                        agg_stage_cell(exec, None)
                    } else {
                        let avg = exec.acc_sum[base] / exec.acc_count[base] as f64;
                        match encode_f64_cell(&mut buf, avg) {
                            Some(k) => agg_stage_cell(exec, Some(&buf[..k])),
                            None => false,
                        }
                    }
                }
                AGG_SUM => {
                    if exec.acc_count[base] == 0 {
                        agg_stage_cell(exec, None) // SUM over no rows is NULL
                    } else if matches!(agg_result_ty(exec, i), LogicalType::Double) {
                        match encode_f64_cell(&mut buf, exec.acc_sum[base]) {
                            Some(k) => agg_stage_cell(exec, Some(&buf[..k])),
                            None => false,
                        }
                    } else {
                        match encode_i64_cell(&mut buf, exec.acc_sum[base] as i64) {
                            Some(k) => agg_stage_cell(exec, Some(&buf[..k])),
                            None => false,
                        }
                    }
                }
                AGG_MIN | AGG_MAX => {
                    if !exec.acc_seen[base] {
                        agg_stage_cell(exec, None)
                    } else {
                        let v = if exec.agg_item_func[i] == AGG_MIN {
                            exec.acc_min[base]
                        } else {
                            exec.acc_max[base]
                        };
                        match encode_numeric_cell(&mut buf, exec.agg_item_ty[i], v) {
                            Some(k) => agg_stage_cell(exec, Some(&buf[..k])),
                            None => false,
                        }
                    }
                }
                _ => false,
            };
            if !staged {
                reply_simple(exec, ERR_RESULT_TOO_LARGE, TAG_EMPTY, 0);
                return;
            }
        }
    }

    // Synthetic result columns: computed values, not stored ones.
    let mut descs = [ColumnDescriptor::EMPTY; MAX_AGG_ITEMS];
    for i in 0..n {
        let nl = exec.agg_item_name_len[i] as usize;
        let mut nm = [0u8; 32];
        nm[..nl].copy_from_slice(&exec.agg_item_name[i][..nl]);
        descs[i] = match ColumnDescriptor::new(0, agg_result_ty(exec, i), true, false, &nm[..nl]) {
            Some(d) => d,
            None => {
                reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
                return;
            }
        };
    }
    let mut colrefs: [&ColumnDescriptor; MAX_AGG_ITEMS] = [&ColumnDescriptor::EMPTY; MAX_AGG_ITEMS];
    for i in 0..n {
        colrefs[i] = &descs[i];
    }

    let built = {
        let (resp, rows) = (&mut exec.resp, &exec.rows);
        match ResponseBuilder::new(
            resp,
            corr,
            conn,
            OUTCOME_OK,
            TAG_SELECT,
            groups as u64,
            &colrefs[..n],
        ) {
            Some(b) => b.finish_with_rows(groups as u16, &rows[..exec.rows_len]),
            None => None,
        }
    };
    match built {
        Some(_) => {
            exec.m_rows_returned = exec.m_rows_returned.wrapping_add(groups as u64);
            ship(exec, built, from_mysql);
            finish_job(exec);
        }
        None => reply_simple(exec, ERR_RESULT_TOO_LARGE, TAG_EMPTY, 0),
    }
}

/// Encode a scalar into a plain result cell (the `encode_value_plain`
/// form the row path uses), for the aggregate reply.
fn encode_i64_cell(out: &mut [u8], v: i64) -> Option<usize> {
    relational::encode_value_plain(out, LogicalType::BigInt, relational::Value::BigInt(v))
}
fn encode_f64_cell(out: &mut [u8], v: f64) -> Option<usize> {
    relational::encode_value_plain(out, LogicalType::Double, relational::Value::Double(v))
}
fn encode_numeric_cell(out: &mut [u8], ty: LogicalType, v: f64) -> Option<usize> {
    let value = match ty {
        LogicalType::SmallInt => relational::Value::SmallInt(v as i16),
        LogicalType::Int => relational::Value::Int(v as i32),
        LogicalType::BigInt => relational::Value::BigInt(v as i64),
        LogicalType::Float => relational::Value::Float(v as f32),
        LogicalType::Double => relational::Value::Double(v),
        _ => return None,
    };
    relational::encode_value_plain(out, ty, value)
}

// ── DROP TABLE ────────────────────────────────────────────────────────

fn start_drop_scan(exec: &mut ExecState) {
    exec.cursor = 0;
    let mut start = [0u8; MAX_KV_KEY];
    let mut end = [0u8; MAX_KV_KEY];
    let Some((sn, en)) = table_bounds(exec.table_id, &mut start, &mut end) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    send_scan(exec, &start[..sn], &end[..en], P_DROP_SCAN);
}

/// A page of row keys came back; delete them.
///
/// The scan always restarts from cursor 0. After a page is deleted those
/// rows are gone, so the next scan from the start returns the next page —
/// and carrying a cursor across a mutation would be wrong anyway, since
/// the provider's cursor is an ordinal over live entries and deleting
/// shifts it.
fn after_drop_scan(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    if count == 0 {
        // No rows left: remove the descriptor, then the name.
        let mut key = [0u8; MAX_KV_KEY];
        let Some(kn) = descr_key(&mut key, exec.table_id) else {
            reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
            return;
        };
        let Some(bn) = stage_delete(exec, &[&key[..kn]]) else {
            reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
            return;
        };
        kv_send(exec, KV_OP_DELETE, bn, P_DROP_DESCR);
        return;
    }

    // Collect up to DELETE_PAGE keys and delete them in one op.
    let mut keys = [[0u8; MAX_KV_KEY]; DELETE_PAGE];
    let mut klens = [0usize; DELETE_PAGE];
    let mut nk = 0usize;
    let mut p = 10usize;
    for _ in 0..count {
        if nk >= DELETE_PAGE {
            break;
        }
        if body.len() < p + 2 {
            reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
            return;
        }
        let klen = u16::from_le_bytes([body[p], body[p + 1]]) as usize;
        p += 2;
        if body.len() < p + klen + 4 || klen > MAX_KV_KEY {
            reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
            return;
        }
        keys[nk][..klen].copy_from_slice(&body[p..p + klen]);
        klens[nk] = klen;
        nk += 1;
        p += klen;
        let vlen = u32::from_le_bytes([body[p], body[p + 1], body[p + 2], body[p + 3]]) as usize;
        p += 4 + vlen;
    }

    let mut refs: [&[u8]; DELETE_PAGE] = [&[]; DELETE_PAGE];
    for i in 0..nk {
        refs[i] = &keys[i][..klens[i]];
    }
    // Split the borrow: `stage_delete` needs `&mut exec` while `refs`
    // borrows the local key array, which is fine — the array is local.
    let Some(bn) = stage_delete(exec, &refs[..nk]) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    exec.affected += nk as u64;
    kv_send(exec, KV_OP_DELETE, bn, P_DROP_DEL);
}

/// Stage a `KV_OP_DELETE` body: `[key_count:u16]` then per key
/// `[key_len:u16][key…]`.
fn stage_delete(exec: &mut ExecState, keys: &[&[u8]]) -> Option<usize> {
    let env = &mut exec.env;
    let mut need = 2usize;
    for k in keys {
        need += 2 + k.len();
    }
    if BODY_AT + need > env.len() || keys.len() > u16::MAX as usize {
        return None;
    }
    env[BODY_AT..BODY_AT + 2].copy_from_slice(&(keys.len() as u16).to_le_bytes());
    let mut p = BODY_AT + 2;
    for k in keys {
        env[p..p + 2].copy_from_slice(&(k.len() as u16).to_le_bytes());
        p += 2;
        env[p..p + k.len()].copy_from_slice(k);
        p += k.len();
    }
    Some(need)
}

fn after_drop_del(exec: &mut ExecState, result: u8) {
    if result != KV_RESULT_OK && result != KV_RESULT_INTEGER {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    start_drop_scan(exec);
}

// ── DELETE ────────────────────────────────────────────────────────────
//
// `DELETE FROM t [WHERE pk = literal]`. A whole-table delete and a
// single-primary-key delete are one code path: both resolve to a scan
// range (the whole table, or one row's `[key, successor)`) and delete
// every row the scan returns, plus each row's secondary-index entries
// (rebuilt from the stored row). Like DROP, each page re-scans from
// cursor 0 — deleting shifts the provider's ordinal. A predicate on a
// non-key column, or a leading column of a composite key, is refused BY
// NAME for now; those want the scan-and-filter path the range-predicate
// work will add.

/// Write `[key_len:u16][key…]` into `env` at `*p`, bumping `*p`.
fn put_key(env: &mut [u8], p: &mut usize, key: &[u8]) -> Option<()> {
    if *p + 2 + key.len() > env.len() {
        return None;
    }
    env[*p..*p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    *p += 2;
    env[*p..*p + key.len()].copy_from_slice(key);
    *p += key.len();
    Some(())
}

fn start_delete(exec: &mut ExecState, _d: &sql_core::Delete) {
    exec.affected = 0;
    start_delete_scan(exec);
}

/// (Re)scan the DELETE's target range from cursor 0 and delete the next
/// page. `affected` is NOT reset here (only in `start_delete`) so it
/// accumulates across pages.
fn start_delete_scan(exec: &mut ExecState) {
    exec.cursor = 0;
    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    let Ok(Statement::Delete(d)) = parse(&text[..n], dialect_of(exec)) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    if let Err(e) = install_filters(exec, &d.where_) {
        reply_simple(exec, error_code(e), TAG_EMPTY, 0);
        return;
    }
    let access = match sql_core::plan_access(&exec.td, &d.where_) {
        Ok(a) => a,
        Err(e) => {
            reply_simple(exec, error_code(e), TAG_EMPTY, 0);
            return;
        }
    };
    let mut start = [0u8; MAX_KV_KEY];
    let mut end = [0u8; MAX_KV_KEY];
    match access {
        // A non-key predicate (or a lone composite-key prefix) drives no
        // tighter bound than the whole table; the residual filters select
        // the rows. A key prefix does narrow the scan, so keep it.
        Access::FullScan | Access::FilteredScan { .. } => {
            let Some((sn, en)) = table_bounds(exec.table_id, &mut start, &mut end) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            send_scan_paged(
                exec,
                &start[..sn],
                &end[..en],
                P_DEL_SCAN,
                DELETE_PAGE as u16,
            );
        }
        Access::KeyPrefix(v) => {
            let mut ktypes = [LogicalType::Null; MAX_KEY_COLUMNS];
            let Some(tn) = exec.td.primary_key_types(&mut ktypes) else {
                reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
                return;
            };
            let mut pbody = [0u8; MAX_PRIMARY_KEY_LEN];
            let Some(pb) = relational::encode_primary_key_prefix(
                &mut pbody,
                exec.table_id,
                &ktypes[..tn],
                &[v],
            ) else {
                reply_simple(exec, sql_exec::ERR_LITERAL_TOO_LONG, TAG_EMPTY, 0);
                return;
            };
            let Some(sn) = kv_key(&mut start, relational::KS_RELATIONAL_TABLE, &pbody[..pb]) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            let Some(en) = prefix_successor(&start[..sn], &mut end) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            send_scan_paged(
                exec,
                &start[..sn],
                &end[..en],
                P_DEL_SCAN,
                DELETE_PAGE as u16,
            );
        }
        Access::PointRead(v) => {
            let mut ktypes = [LogicalType::Null; MAX_KEY_COLUMNS];
            let Some(tn) = exec.td.primary_key_types(&mut ktypes) else {
                reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
                return;
            };
            let mut keybody = [0u8; MAX_PRIMARY_KEY_LEN];
            let Some(kb) =
                relational::encode_primary_key(&mut keybody, exec.table_id, &ktypes[..tn], &[v])
            else {
                reply_simple(exec, sql_exec::ERR_LITERAL_TOO_LONG, TAG_EMPTY, 0);
                return;
            };
            let Some(sn) = kv_key(&mut start, relational::KS_RELATIONAL_TABLE, &keybody[..kb])
            else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            let Some(en) = prefix_successor(&start[..sn], &mut end) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            send_scan_paged(
                exec,
                &start[..sn],
                &end[..en],
                P_DEL_SCAN,
                DELETE_PAGE as u16,
            );
        }
        Access::KeyRange { op, value } => {
            // A range on the primary key deletes every row in the byte
            // slice it names. The re-scan-from-zero loop shrinks the same
            // bounds each pass until the slice is empty.
            let Some((sn, en)) = key_range_bounds(exec, op, value, &mut start, &mut end) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            send_scan_paged(
                exec,
                &start[..sn],
                &end[..en],
                P_DEL_SCAN,
                DELETE_PAGE as u16,
            );
        }
    }
}

/// A page of matched rows came back. Delete each row key plus its index
/// entries in one `KV_OP_DELETE`, then loop.
fn after_del_scan(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        reply_simple(exec, sql_exec::ERR_TYPE_MISMATCH, TAG_EMPTY, 0);
        return;
    }
    let count = u16::from_le_bytes([body[8], body[9]]);
    if count == 0 {
        reply_simple(exec, OUTCOME_OK, sql_exec::TAG_DELETE, exec.affected);
        return;
    }
    let next_cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
    let Some((bn, rows)) = stage_page_deletes(exec, body) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    if rows > 0 {
        // Something to delete: remove it, then restart the scan from zero —
        // the delete shifts store ordinals, so the cursor is invalid.
        exec.affected += rows;
        kv_send(exec, KV_OP_DELETE, bn, P_DEL_DEL);
        return;
    }
    // The page held no rows the WHERE keeps. Advance the cursor to continue
    // this pass; a full pass with nothing to delete ends the statement.
    // (Without a residual filter every scanned row matches, so this branch
    // is only reached once the range is exhausted.)
    if next_cursor == 0 {
        reply_simple(exec, OUTCOME_OK, sql_exec::TAG_DELETE, exec.affected);
        return;
    }
    exec.cursor = next_cursor;
    let mut start = [0u8; MAX_KV_KEY];
    let mut end = [0u8; MAX_KV_KEY];
    let Some((sn, en)) = current_scan_bounds(exec, &mut start, &mut end) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    send_scan_paged(
        exec,
        &start[..sn],
        &end[..en],
        P_DEL_SCAN,
        DELETE_PAGE as u16,
    );
}

fn after_del_del(exec: &mut ExecState, result: u8) {
    if result != KV_RESULT_OK && result != KV_RESULT_INTEGER {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    start_delete_scan(exec);
}

/// Build a `KV_OP_DELETE` body in `exec.env` covering every row in the
/// scan `body` plus each row's secondary-index entries, rebuilt from the
/// stored row via `row_lookup`. Returns `(body_len, rows_deleted)`.
fn stage_page_deletes(exec: &mut ExecState, body: &[u8]) -> Option<(usize, u64)> {
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    let mut p = BODY_AT + 2; // reserve the [key_count:u16] header
    let mut keys: u64 = 0;
    let mut rows: u64 = 0;
    let mut at = 10usize;
    for _ in 0..count {
        if rows as usize >= DELETE_PAGE {
            break;
        }
        let klen = u16::from_le_bytes([*body.get(at)?, *body.get(at + 1)?]) as usize;
        let koff = at + 2;
        let kend = koff + klen;
        let vlen = u32::from_le_bytes(body.get(kend..kend + 4)?.try_into().ok()?) as usize;
        let voff = kend + 4;
        let vend = voff + vlen;
        if vend > body.len() {
            return None;
        }
        // Residual predicates: delete only rows the whole WHERE keeps. A
        // page with no match advances the scan cursor instead of looping.
        if !passes_filter(exec, &body[voff..vend]) {
            at = vend;
            continue;
        }
        // Row key.
        put_key(&mut exec.env, &mut p, &body[koff..kend])?;
        keys += 1;
        // Index entries: the primary key is the row key minus its
        // keyspace prefix (encode_primary_key's output).
        let pk_body_start = koff + sql_exec::KEYSPACE_PREFIX_LEN;
        let idxn = exec.idx_count as usize;
        for i in 0..idxn {
            let d = exec.idx[i];
            let col = *d.key_columns().first()?;
            let ty = exec.td.column(col)?.ty;
            let v = match relational::row_lookup(&body[voff..vend], col, ty) {
                Some(relational::ColumnLookup::Present(v)) => v,
                _ => continue, // an absent/null indexed column has no entry
            };
            let mut valbuf = [0u8; 256];
            let ivn = relational::encode_index_value(&mut valbuf, &[ty], &[v])?;
            let mut ibody = [0u8; MAX_KV_KEY];
            let ibn = relational::index_user_key(
                d.index_id,
                &valbuf[..ivn],
                &body[pk_body_start..kend],
                &mut ibody,
            )?;
            let mut ikey = [0u8; MAX_KV_KEY];
            let ikn = kv_key(&mut ikey, relational::KS_RELATIONAL_INDEX, &ibody[..ibn])?;
            put_key(&mut exec.env, &mut p, &ikey[..ikn])?;
            keys += 1;
        }
        rows += 1;
        at = vend;
    }
    exec.env[BODY_AT..BODY_AT + 2].copy_from_slice(&(keys as u16).to_le_bytes());
    Some((p - BODY_AT, rows))
}

// ── UPDATE ────────────────────────────────────────────────────────────
//
// `UPDATE t SET c = v [, …] [WHERE pk = literal]`. Read-modify-write:
// scan the target range (one row or the whole table), and for each page
// build ONE transaction that PUTs each row's new value and, for every
// SET column that is indexed, DELETEs the old index entry and PUTs the
// new one — so the index tracks the value. Unlike DELETE the row set is
// stable (a PUT keeps the key), so pages advance by cursor, not restart.
// A SET on a primary-key column (which would move the row) and a
// non-key / composite-prefix predicate are refused BY NAME for now.

/// Append a PUT sub-op to a TXN `then` branch.
fn put_op(env: &mut [u8], p: &mut usize, key: &[u8], value: &[u8]) -> Option<()> {
    let put_len = 2 + key.len() + 4 + value.len() + 1 + 8;
    if *p + 1 + 2 + put_len > env.len() {
        return None;
    }
    env[*p] = KV_OP_PUT;
    *p += 1;
    env[*p..*p + 2].copy_from_slice(&(put_len as u16).to_le_bytes());
    *p += 2;
    env[*p..*p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    *p += 2;
    env[*p..*p + key.len()].copy_from_slice(key);
    *p += key.len();
    env[*p..*p + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
    *p += 4;
    env[*p..*p + value.len()].copy_from_slice(value);
    *p += value.len();
    env[*p] = 0; // flags
    *p += 1;
    env[*p..*p + 8].copy_from_slice(&0u64.to_le_bytes()); // expiry
    *p += 8;
    Some(())
}

/// Append a single-key DELETE sub-op to a TXN `then` branch.
fn del_op(env: &mut [u8], p: &mut usize, key: &[u8]) -> Option<()> {
    let del_len = 2 + 2 + key.len(); // [key_count:u16][key_len:u16][key]
    if *p + 1 + 2 + del_len > env.len() {
        return None;
    }
    env[*p] = KV_OP_DELETE;
    *p += 1;
    env[*p..*p + 2].copy_from_slice(&(del_len as u16).to_le_bytes());
    *p += 2;
    env[*p..*p + 2].copy_from_slice(&1u16.to_le_bytes());
    *p += 2;
    env[*p..*p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    *p += 2;
    env[*p..*p + key.len()].copy_from_slice(key);
    *p += key.len();
    Some(())
}

fn start_update(exec: &mut ExecState, _u: &sql_core::Update) {
    exec.affected = 0;
    exec.cursor = 0;
    start_update_scan(exec);
}

/// Scan the UPDATE's target range from the current cursor. Bounds are
/// re-derived each page; the cursor advances normally (updates keep keys).
fn start_update_scan(exec: &mut ExecState) {
    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    let Ok(Statement::Update(u)) = parse(&text[..n], dialect_of(exec)) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    // Validate the SET list: every column exists, none is part of the
    // primary key (moving a row is a future feature).
    for i in 0..u.set_count {
        let col = match sql_core::resolve_column(&exec.td, u.sets[i].column) {
            Ok(c) => c,
            Err(e) => {
                reply_simple(exec, error_code(e), TAG_EMPTY, 0);
                return;
            }
        };
        if exec.td.primary_key().contains(&col.column_id) {
            reply_simple(
                exec,
                error_code(sql_core::SqlError::Unsupported),
                TAG_EMPTY,
                0,
            );
            return;
        }
        // Type-check the literal against the column now (fail before any
        // write), the same binding the write path will use.
        if sql_core::bind_value(u.sets[i].value, col.ty, col.nullable).is_err() {
            reply_simple(exec, sql_exec::ERR_TYPE_MISMATCH, TAG_EMPTY, 0);
            return;
        }
    }
    if let Err(e) = install_filters(exec, &u.where_) {
        reply_simple(exec, error_code(e), TAG_EMPTY, 0);
        return;
    }
    let access = match sql_core::plan_access(&exec.td, &u.where_) {
        Ok(a) => a,
        Err(e) => {
            reply_simple(exec, error_code(e), TAG_EMPTY, 0);
            return;
        }
    };
    let mut start = [0u8; MAX_KV_KEY];
    let mut end = [0u8; MAX_KV_KEY];
    match access {
        // A non-key predicate names no tighter bound than the whole table;
        // the residual filters, applied per row in `stage_page_update`,
        // select which rows to write.
        Access::FullScan | Access::FilteredScan { .. } => {
            let Some((sn, en)) = table_bounds(exec.table_id, &mut start, &mut end) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            send_scan_paged(
                exec,
                &start[..sn],
                &end[..en],
                P_UPD_SCAN,
                DELETE_PAGE as u16,
            );
        }
        Access::KeyPrefix(v) => {
            let mut ktypes = [LogicalType::Null; MAX_KEY_COLUMNS];
            let Some(tn) = exec.td.primary_key_types(&mut ktypes) else {
                reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
                return;
            };
            let mut pbody = [0u8; MAX_PRIMARY_KEY_LEN];
            let Some(pb) = relational::encode_primary_key_prefix(
                &mut pbody,
                exec.table_id,
                &ktypes[..tn],
                &[v],
            ) else {
                reply_simple(exec, sql_exec::ERR_LITERAL_TOO_LONG, TAG_EMPTY, 0);
                return;
            };
            let Some(sn) = kv_key(&mut start, relational::KS_RELATIONAL_TABLE, &pbody[..pb]) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            let Some(en) = prefix_successor(&start[..sn], &mut end) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            send_scan_paged(
                exec,
                &start[..sn],
                &end[..en],
                P_UPD_SCAN,
                DELETE_PAGE as u16,
            );
        }
        Access::PointRead(v) => {
            let mut ktypes = [LogicalType::Null; MAX_KEY_COLUMNS];
            let Some(tn) = exec.td.primary_key_types(&mut ktypes) else {
                reply_simple(exec, ERR_CORRUPT, TAG_EMPTY, 0);
                return;
            };
            let mut keybody = [0u8; MAX_PRIMARY_KEY_LEN];
            let Some(kb) =
                relational::encode_primary_key(&mut keybody, exec.table_id, &ktypes[..tn], &[v])
            else {
                reply_simple(exec, sql_exec::ERR_LITERAL_TOO_LONG, TAG_EMPTY, 0);
                return;
            };
            let Some(sn) = kv_key(&mut start, relational::KS_RELATIONAL_TABLE, &keybody[..kb])
            else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            let Some(en) = prefix_successor(&start[..sn], &mut end) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            send_scan_paged(
                exec,
                &start[..sn],
                &end[..en],
                P_UPD_SCAN,
                DELETE_PAGE as u16,
            );
        }
        Access::KeyRange { op, value } => {
            // A range on the primary key updates every row in the byte
            // slice it names. Bounds are re-derived identically each page;
            // the cursor advances because an UPDATE keeps the row key.
            let Some((sn, en)) = key_range_bounds(exec, op, value, &mut start, &mut end) else {
                reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
                return;
            };
            send_scan_paged(
                exec,
                &start[..sn],
                &end[..en],
                P_UPD_SCAN,
                DELETE_PAGE as u16,
            );
        }
    }
}

fn after_upd_scan(exec: &mut ExecState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    let count = u16::from_le_bytes([body[8], body[9]]);
    if count == 0 {
        reply_simple(exec, OUTCOME_OK, sql_exec::TAG_UPDATE, exec.affected);
        return;
    }
    // Remember where to resume: the page's next cursor.
    exec.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
    let (bn, rows, code) = stage_page_update(exec, body);
    if code != OUTCOME_OK {
        reply_simple(exec, code, TAG_EMPTY, 0);
        return;
    }
    exec.affected += rows;
    kv_send(exec, types::KV_OP_TXN, bn, P_UPD_WRITE);
}

fn after_upd_write(exec: &mut ExecState, result: u8, body: &[u8]) {
    // The TXN had no comparisons, so success is KV_RESULT_TXN with the
    // committed flag set. Anything else is a store fault.
    let committed = result == types::KV_RESULT_TXN && !body.is_empty() && body[0] == 1;
    if !committed {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    if exec.cursor != 0 {
        start_update_scan(exec);
    } else {
        reply_simple(exec, OUTCOME_OK, sql_exec::TAG_UPDATE, exec.affected);
    }
}

/// Build the per-page UPDATE transaction into `exec.env`. Returns
/// `(body_len, rows, outcome)`; a non-`OUTCOME_OK` outcome means the
/// caller should reply that error instead of sending.
fn stage_page_update(exec: &mut ExecState, page: &[u8]) -> (usize, u64, u8) {
    // Re-parse to recover the SET list (its Values borrow `text`, which
    // must live for the whole build).
    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    let Ok(Statement::Update(u)) = parse(&text[..n], dialect_of(exec)) else {
        return (0, 0, ERR_STORE);
    };

    // Resolve the SET assignments to (column_id, ty, bound value).
    let mut set_id = [0u16; MAX_COLUMNS];
    let mut set_ty = [LogicalType::Null; MAX_COLUMNS];
    let mut set_val = [Value::Null; MAX_COLUMNS];
    let sc = u.set_count;
    for i in 0..sc {
        let Ok(col) = sql_core::resolve_column(&exec.td, u.sets[i].column) else {
            return (0, 0, ERR_CORRUPT);
        };
        let Ok(v) = sql_core::bind_value(u.sets[i].value, col.ty, col.nullable) else {
            return (0, 0, sql_exec::ERR_TYPE_MISMATCH);
        };
        set_id[i] = col.column_id;
        set_ty[i] = col.ty;
        set_val[i] = v;
    }
    let set_of = |cid: u16| -> Option<usize> { (0..sc).find(|&i| set_id[i] == cid) };

    // Copy the column and index metadata out so the row loop borrows
    // only `exec.env` (mut) and the local `page`.
    let mut col_id = [0u16; MAX_COLUMNS];
    let mut col_ty = [LogicalType::Null; MAX_COLUMNS];
    let ncol = exec.td.columns().len().min(MAX_COLUMNS);
    for (i, c) in exec.td.columns().iter().take(ncol).enumerate() {
        col_id[i] = c.column_id;
        col_ty[i] = c.ty;
    }
    let nidx = exec.idx_count as usize;
    let mut ix_id = [0u32; MAX_TABLE_INDEXES];
    let mut ix_col = [0u16; MAX_TABLE_INDEXES];
    let mut ix_ty = [LogicalType::Null; MAX_TABLE_INDEXES];
    for i in 0..nidx {
        let d = exec.idx[i];
        let Some(&c) = d.key_columns().first() else {
            return (0, 0, ERR_CORRUPT);
        };
        let Some(cd) = exec.td.column(c) else {
            return (0, 0, ERR_CORRUPT);
        };
        ix_id[i] = d.index_id;
        ix_col[i] = c;
        ix_ty[i] = cd.ty;
    }

    // TXN header: [cmp_count=0][then_count placeholder][ops][else_count=0].
    let mut p = BODY_AT;
    exec.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
    p += 2;
    let then_at = p;
    p += 2; // then_count backfilled at the end
    let mut then_count: u64 = 0;
    let mut rows: u64 = 0;

    let count = u16::from_le_bytes([page[8], page[9]]) as usize;
    let mut at = 10usize;
    for _ in 0..count {
        if rows as usize >= DELETE_PAGE {
            break;
        }
        let klen = match page.get(at..at + 2) {
            Some(b) => u16::from_le_bytes([b[0], b[1]]) as usize,
            None => return (0, 0, ERR_STORE),
        };
        let koff = at + 2;
        let kend = koff + klen;
        let vlen = match page.get(kend..kend + 4) {
            Some(b) => u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize,
            None => return (0, 0, ERR_STORE),
        };
        let voff = kend + 4;
        let vend = voff + vlen;
        if vend > page.len() {
            return (0, 0, ERR_STORE);
        }
        let row_key = &page[koff..kend];
        let old_row = &page[voff..vend];
        let pk_body = &page[koff + sql_exec::KEYSPACE_PREFIX_LEN..kend];

        // Residual predicates: write only rows the whole WHERE keeps. The
        // cursor advances past skipped rows, so no restart is needed.
        if !passes_filter(exec, old_row) {
            at = vend;
            continue;
        }

        // New row = each present column, with SET columns overridden.
        let mut cols = [relational::ColumnValue {
            column_id: 0,
            ty: LogicalType::Null,
            value: Value::Null,
        }; MAX_COLUMNS];
        let mut nc = 0usize;
        for c in 0..ncol {
            let value = if let Some(si) = set_of(col_id[c]) {
                set_val[si]
            } else {
                match relational::row_lookup(old_row, col_id[c], col_ty[c]) {
                    Some(relational::ColumnLookup::Present(v)) => v,
                    _ => continue, // column not in this row — leave it out
                }
            };
            cols[nc] = relational::ColumnValue {
                column_id: col_id[c],
                ty: col_ty[c],
                value,
            };
            nc += 1;
        }
        let mut newrow = [0u8; MAX_ROW_BYTES];
        let Some(rn) = relational::encode_row(&mut newrow, &cols[..nc]) else {
            return (0, 0, sql_exec::ERR_TOO_MANY_ITEMS);
        };
        if put_op(&mut exec.env, &mut p, row_key, &newrow[..rn]).is_none() {
            return (0, 0, sql_exec::ERR_TOO_MANY_ITEMS);
        }
        then_count += 1;

        // Index maintenance for every SET column that is indexed and
        // whose value actually changed.
        for i in 0..nidx {
            let Some(si) = set_of(ix_col[i]) else {
                continue; // this index's column is not being SET
            };
            let old_v = match relational::row_lookup(old_row, ix_col[i], ix_ty[i]) {
                Some(relational::ColumnLookup::Present(v)) => v,
                _ => Value::Null,
            };
            let new_v = set_val[si];
            if old_v == new_v {
                continue; // key unchanged — nothing to fix
            }
            // Delete the old entry, add the new one.
            let mut ov = [0u8; 256];
            let mut nvb = [0u8; 256];
            let (Some(on), Some(nn)) = (
                relational::encode_index_value(&mut ov, &[ix_ty[i]], &[old_v]),
                relational::encode_index_value(&mut nvb, &[ix_ty[i]], &[new_v]),
            ) else {
                return (0, 0, ERR_STORE);
            };
            let mut obody = [0u8; MAX_KV_KEY];
            let mut nbody = [0u8; MAX_KV_KEY];
            let (Some(obn), Some(nbn)) = (
                relational::index_user_key(ix_id[i], &ov[..on], pk_body, &mut obody),
                relational::index_user_key(ix_id[i], &nvb[..nn], pk_body, &mut nbody),
            ) else {
                return (0, 0, ERR_STORE);
            };
            let mut okey = [0u8; MAX_KV_KEY];
            let mut nkey = [0u8; MAX_KV_KEY];
            let (Some(okn), Some(nkn)) = (
                kv_key(&mut okey, relational::KS_RELATIONAL_INDEX, &obody[..obn]),
                kv_key(&mut nkey, relational::KS_RELATIONAL_INDEX, &nbody[..nbn]),
            ) else {
                return (0, 0, ERR_STORE);
            };
            if del_op(&mut exec.env, &mut p, &okey[..okn]).is_none()
                || put_op(&mut exec.env, &mut p, &nkey[..nkn], &[]).is_none()
            {
                return (0, 0, sql_exec::ERR_TOO_MANY_ITEMS);
            }
            then_count += 2;
        }

        rows += 1;
        at = vend;
    }

    if then_count > u16::MAX as u64 {
        return (0, 0, sql_exec::ERR_TOO_MANY_ITEMS);
    }
    exec.env[then_at..then_at + 2].copy_from_slice(&(then_count as u16).to_le_bytes());
    // else_count = 0.
    if p + 2 > exec.env.len() {
        return (0, 0, sql_exec::ERR_TOO_MANY_ITEMS);
    }
    exec.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
    p += 2;
    (p - BODY_AT, rows, OUTCOME_OK)
}

fn after_drop_descr(exec: &mut ExecState, result: u8) {
    if result != KV_RESULT_OK && result != KV_RESULT_INTEGER {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    let mut text = [0u8; sql_core::MAX_SQL_LEN];
    let n = statement_text(exec, &mut text);
    let Ok(Statement::DropTable { name, .. }) = parse(&text[..n], dialect_of(exec)) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let mut key = [0u8; MAX_KV_KEY];
    let Some(kn) = name_key(&mut key, name) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    let Some(bn) = stage_delete(exec, &[&key[..kn]]) else {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    };
    kv_send(exec, KV_OP_DELETE, bn, P_DROP_NAME);
}

fn after_drop_name(exec: &mut ExecState, result: u8) {
    if result != KV_RESULT_OK && result != KV_RESULT_INTEGER {
        reply_simple(exec, ERR_STORE, TAG_EMPTY, 0);
        return;
    }
    reply_simple(exec, OUTCOME_OK, TAG_DROP_TABLE, 0);
}

// ── Module ABI ────────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ExecState>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    _params: *const u8,
    _params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    if state.is_null() || syscalls.is_null() {
        return -1;
    }
    if state_size < core::mem::size_of::<ExecState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let exec = unsafe { &mut *state.cast::<ExecState>() };
    exec.init(sys_ptr);

    // inputs:  pg_in[0], mysql_in[1], kv_in[2]
    // outputs: pg_out[0], mysql_out[1], kv_out[2], metrics[3]
    exec.pg_in = in_chan;
    exec.pg_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        exec.mysql_in = dev_channel_port(sys, 0, 1);
        exec.kv_in = dev_channel_port(sys, 0, 2);
        exec.mysql_out = dev_channel_port(sys, 1, 1);
        exec.kv_out = dev_channel_port(sys, 1, 2);
        exec.metrics_out = dev_channel_port(sys, 1, 3);
    }
    // Without a KV path the module can parse but never execute, and a
    // client would hang on the first real statement. Refuse to start
    // rather than accept connections it cannot serve.
    if exec.kv_out < 0 || exec.kv_in < 0 {
        return -1;
    }
    // Mint the watchdog's wake timer. A platform that refuses one leaves
    // `timer_fd = -1`; the watchdog then degrades to firing only on
    // scheduler-/traffic-driven steps rather than being unreachable-safe,
    // so a missing timer is a soft loss, not a startup failure.
    unsafe {
        let sys = &*sys_ptr;
        exec.timer_fd = (sys.provider_open)(TIMER_CONTRACT, TIMER_CREATE, core::ptr::null_mut(), 0);
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let exec = unsafe { &mut *state.cast::<ExecState>() };
    unsafe {
        let sys_ptr = exec.syscalls;
        if sys_ptr.is_null() {
            return 0;
        }
        let sys = &*sys_ptr;

        // KV replies first: they advance the statement in flight, and
        // draining them before accepting new work keeps the queue from
        // filling behind a job that was ready to finish.
        if let Some((mt, len)) = read_envelope(sys, exec.kv_in, &mut exec.scratch) {
            if mt == MSG_KV_RESPONSE {
                let mut tmp = [0u8; SCRATCH_BUF];
                tmp[..len].copy_from_slice(&exec.scratch[..len]);
                on_kv_response(exec, &tmp[..len]);
            }
        }
        if let Some((mt, len)) = read_envelope(sys, exec.pg_in, &mut exec.scratch) {
            if mt == MSG_SQL_REQUEST {
                let mut tmp = [0u8; SCRATCH_BUF];
                tmp[..len].copy_from_slice(&exec.scratch[..len]);
                on_sql_request(exec, false, &tmp[..len]);
            }
        }
        if let Some((mt, len)) = read_envelope(sys, exec.mysql_in, &mut exec.scratch) {
            if mt == MSG_SQL_REQUEST {
                let mut tmp = [0u8; SCRATCH_BUF];
                tmp[..len].copy_from_slice(&exec.scratch[..len]);
                on_sql_request(exec, true, &tmp[..len]);
            }
        }

        // Watchdog: a statement in flight whose KV reply never arrived is
        // aborted so the executor does not wedge behind it. `now == 0`
        // means the platform has no monotonic clock — then the watchdog
        // simply never fires, which is safer than aborting on a fabricated
        // duration. `busy()` guarantees `inflight_since_ms` was armed by
        // the `kv_send` that entered the current phase.
        if exec.busy() {
            let now = dev_millis(sys);
            if now != 0 && now.saturating_sub(exec.inflight_since_ms) > WEDGE_TIMEOUT_MS {
                exec.m_stmt_timeouts = exec.m_stmt_timeouts.wrapping_add(1);
                reply_simple(exec, ERR_TIMEOUT, TAG_EMPTY, 0);
            }
        }

        exec.step_ctr = exec.step_ctr.wrapping_add(1);
        if exec.step_ctr.is_multiple_of(5000) {
            telemetry::emit_counters(
                sys,
                exec.metrics_out,
                &[
                    exec.m_statements,
                    exec.m_rows_returned,
                    exec.m_rows_written,
                    exec.m_errors,
                    exec.m_queue_rejects,
                ],
            );
        }
    }
    0
}
