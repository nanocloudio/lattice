//! index_backfill — the §26 Phase 6 online index backfill worker.
//!
//! Completes indexes created over POPULATED tables. `CREATE INDEX` on
//! such a table writes its descriptor at `SchemaPhase::Backfilling`:
//! from that phase on, every writer maintains the index inside its own
//! transaction (`write_insert_allowed`), while readers refuse it
//! (`read_allowed` is `Public` only — §21 invariant 19: an index that
//! has not reached exactness must not serve). This worker's whole job
//! is to make the pre-existing rows catch up and then flip the phase.
//!
//! ## Why the ordinal cursor is safe here
//!
//! The row scan resumes by the provider's ordinal cursor, persisted in
//! `KS_RELATIONAL_JOB` after every page — so a crash resumes, not
//! restarts (§26: "every operation is resumable"). An ordinal is not
//! stable under concurrent INSERTs, but every insert since the
//! descriptor reached `Backfilling` ALSO wrote its own index entry
//! (executor-side maintenance), so a row the shifted cursor skips is a
//! row maintenance already covered, and a row visited twice is an
//! idempotent overwrite. `UPDATE`/`DELETE` statements do not exist yet;
//! when they do, backfill correctness must be revisited alongside them
//! (named here so that revisit cannot be forgotten).
//!
//! ## One index at a time
//!
//! The catalog poll picks the FIRST Backfilling index and drives it to
//! Public before looking again. Serial by design, like the executor
//! and the supervisor: bounded memory, no interleaving to reason
//! about, and index builds are rare operator-initiated events.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface"
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

// One mount chain: sql_exec → sql_core → relational (→ db_ops), the
// same rule every relational consumer follows.
#[path = "../../common/sql_exec.rs"]
mod sql_exec;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use sql_exec::relational::{
    self, IndexDescriptor, LogicalType, ObjectKind, TableDescriptor, Value,
};
use sql_exec::{kv_key, prefix_successor};
use types::{
    KV_OP_DELETE, KV_OP_GET, KV_OP_PUT, KV_OP_RANGE_SCAN, KV_RESULT_NOT_FOUND, KV_RESULT_OK,
    KV_RESULT_RANGE, PROTO_INTERNAL_JOB,
};
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE};

// ── Capacities ───────────────────────────────────────────────────────

/// Database id (single-database composition, like the executor).
const DATABASE_ID: u32 = 0;

/// Rows per backfill page.
const PAGE_LIMIT: u16 = 16;

/// Catalog poll cadence while idle, ms.
const POLL_MS: u64 = 1000;

const ENV_BUF: usize = 4096;
const PAGE_BUF: usize = 3584;
const MAX_KV_KEY: usize = 512;

// ── States ───────────────────────────────────────────────────────────

const S_IDLE: u8 = 0; // waiting out the poll interval
const S_CATALOG: u8 = 1; // catalog page in flight
const S_TDESCR: u8 = 2; // table descriptor read in flight
const S_JOB: u8 = 3; // job-cursor read in flight
const S_SCAN: u8 = 4; // row page in flight
const S_ENTRY: u8 = 5; // one entry PUT in flight
const S_SAVE: u8 = 6; // job-cursor persist in flight
const S_PUBLISH: u8 = 7; // descriptor flip to Public in flight
const S_CLEANUP: u8 = 8; // job-cursor delete in flight

#[repr(C)]
struct BfState {
    syscalls: *const SyscallTable,
    kv_in: i32,
    kv_out: i32,
    metrics_out: i32,

    state: u8,
    last_poll_ms: u64,
    corr: u64,

    /// The index being built and its table.
    d: IndexDescriptor,
    td: TableDescriptor,
    cursor: u64,
    /// Staged row page: `[klen u16][key][vlen u32][value]` pairs.
    page: [u8; PAGE_BUF],
    page_len: u16,
    page_at: u16,
    page_pairs: u16,

    env: [u8; ENV_BUF],

    m_indexed: u64,
    m_published: u64,
    m_errors: u64,
    step_ctr: u32,
    logged_fault: u8,
}

impl BfState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.kv_in = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.state = S_IDLE;
        self.last_poll_ms = 0;
        self.corr = 0;
        self.d = IndexDescriptor::EMPTY;
        self.td = TableDescriptor::EMPTY;
        self.cursor = 0;
        self.page = [0; PAGE_BUF];
        self.page_len = 0;
        self.page_at = 0;
        self.page_pairs = 0;
        self.env = [0; ENV_BUF];
        self.m_indexed = 0;
        self.m_published = 0;
        self.m_errors = 0;
        self.step_ctr = 0;
        self.logged_fault = 0;
    }
}

// ── KV plumbing (executor pattern) ───────────────────────────────────

const BODY_AT: usize = wire::ENVELOPE_HDR + 18;

fn kv_send(bf: &mut BfState, op: u8, body_len: usize, next: u8) -> bool {
    bf.corr = bf.corr.wrapping_add(1).max(1);
    const REQ_HEAD: usize = 18;
    let at = wire::ENVELOPE_HDR;
    if at + REQ_HEAD + body_len > bf.env.len() {
        return false;
    }
    bf.env[at..at + 8].copy_from_slice(&bf.corr.to_le_bytes());
    bf.env[at + 8] = PROTO_INTERNAL_JOB;
    bf.env[at + 9..at + 13].copy_from_slice(&0u32.to_le_bytes());
    bf.env[at + 13] = 0;
    bf.env[at + 14] = 0;
    bf.env[at + 15] = op;
    bf.env[at + 16..at + 18].copy_from_slice(&(body_len as u16).to_le_bytes());
    let sent = unsafe {
        let sys = bf.syscalls;
        !sys.is_null()
            && write_envelope(
                &*sys,
                bf.kv_out,
                MSG_KV_REQUEST,
                REQ_HEAD + body_len,
                &mut bf.env,
            )
    };
    if sent {
        bf.state = next;
    }
    sent
}

fn stage_get(bf: &mut BfState, key: &[u8]) -> Option<usize> {
    let need = 2 + key.len();
    if BODY_AT + need > bf.env.len() {
        return None;
    }
    bf.env[BODY_AT..BODY_AT + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    bf.env[BODY_AT + 2..BODY_AT + need].copy_from_slice(key);
    Some(need)
}

fn stage_put(bf: &mut BfState, key: &[u8], value: &[u8]) -> Option<usize> {
    let need = 2 + key.len() + 4 + value.len() + 1 + 8;
    if BODY_AT + need > bf.env.len() {
        return None;
    }
    let mut p = BODY_AT;
    bf.env[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    bf.env[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    bf.env[p..p + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
    p += 4;
    bf.env[p..p + value.len()].copy_from_slice(value);
    p += value.len();
    bf.env[p] = 0;
    p += 1;
    bf.env[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
    Some(need)
}

fn stage_scan(
    bf: &mut BfState,
    start: &[u8],
    end: &[u8],
    cursor: u64,
    limit: u16,
) -> Option<usize> {
    let need = 2 + start.len() + 2 + end.len() + 10;
    if BODY_AT + need > bf.env.len() {
        return None;
    }
    let mut p = BODY_AT;
    bf.env[p..p + 2].copy_from_slice(&(start.len() as u16).to_le_bytes());
    p += 2;
    bf.env[p..p + start.len()].copy_from_slice(start);
    p += start.len();
    bf.env[p..p + 2].copy_from_slice(&(end.len() as u16).to_le_bytes());
    p += 2;
    bf.env[p..p + end.len()].copy_from_slice(end);
    p += end.len();
    bf.env[p..p + 8].copy_from_slice(&cursor.to_le_bytes());
    p += 8;
    bf.env[p..p + 2].copy_from_slice(&limit.to_le_bytes());
    Some(need)
}

fn stage_delete_one(bf: &mut BfState, key: &[u8]) -> Option<usize> {
    let need = 2 + 2 + key.len();
    if BODY_AT + need > bf.env.len() {
        return None;
    }
    bf.env[BODY_AT..BODY_AT + 2].copy_from_slice(&1u16.to_le_bytes());
    bf.env[BODY_AT + 2..BODY_AT + 4].copy_from_slice(&(key.len() as u16).to_le_bytes());
    bf.env[BODY_AT + 4..BODY_AT + need].copy_from_slice(key);
    Some(need)
}

// ── Keys ─────────────────────────────────────────────────────────────

fn catalog_index_bounds(start: &mut [u8], end: &mut [u8]) -> Option<(usize, usize)> {
    let mut body = [0u8; 5];
    body[0..4].copy_from_slice(&DATABASE_ID.to_be_bytes());
    body[4] = ObjectKind::Index as u8;
    let sn = kv_key(start, relational::KS_RELATIONAL_CATALOG, &body)?;
    let en = prefix_successor(&start[..sn], end)?;
    Some((sn, en))
}

fn index_descr_key(out: &mut [u8], index_id: u32) -> Option<usize> {
    let mut body = [0u8; relational::CATALOG_KEY_LEN];
    let n = relational::encode_catalog_key(&mut body, DATABASE_ID, ObjectKind::Index, index_id)?;
    kv_key(out, relational::KS_RELATIONAL_CATALOG, &body[..n])
}

fn table_descr_key(out: &mut [u8], table_id: u32) -> Option<usize> {
    let mut body = [0u8; relational::CATALOG_KEY_LEN];
    let n = relational::encode_catalog_key(&mut body, DATABASE_ID, ObjectKind::Table, table_id)?;
    kv_key(out, relational::KS_RELATIONAL_CATALOG, &body[..n])
}

fn job_key(out: &mut [u8], index_id: u32) -> Option<usize> {
    let mut body = [0u8; 8];
    let n = relational::encode_job_key(&mut body, u64::from(index_id))?;
    kv_key(out, relational::KS_RELATIONAL_JOB, &body[..n])
}

fn table_bounds(table_id: u32, start: &mut [u8], end: &mut [u8]) -> Option<(usize, usize)> {
    let mut body = [0u8; relational::TABLE_ID_LEN];
    let bn = relational::encode_table_prefix(&mut body, table_id)?;
    let sn = kv_key(start, relational::KS_RELATIONAL_TABLE, &body[..bn])?;
    let en = prefix_successor(&start[..sn], end)?;
    Some((sn, en))
}

// ── The pump ─────────────────────────────────────────────────────────

fn fault(bf: &mut BfState, what: &[u8]) {
    // Faults return to polling: the next poll retries from the durable
    // cursor. Counted so a stuck build is visible, not silent.
    bf.state = S_IDLE;
    bf.m_errors = bf.m_errors.wrapping_add(1);
    if bf.logged_fault == 0 {
        bf.logged_fault = 1;
        unsafe {
            if !bf.syscalls.is_null() {
                dev_log(&*bf.syscalls, 3, what.as_ptr(), what.len());
            }
        }
    }
}

fn send_catalog_scan(bf: &mut BfState) {
    let mut start = [0u8; MAX_KV_KEY];
    let mut end = [0u8; MAX_KV_KEY];
    let Some((sn, en)) = catalog_index_bounds(&mut start, &mut end) else {
        fault(bf, b"[ixbf] catalog bounds");
        return;
    };
    let Some(bn) = stage_scan(bf, &start[..sn], &end[..en], 0, 32) else {
        fault(bf, b"[ixbf] catalog stage");
        return;
    };
    let _ = kv_send(bf, KV_OP_RANGE_SCAN, bn, S_CATALOG);
}

fn on_catalog(bf: &mut BfState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        fault(bf, b"[ixbf] catalog scan failed");
        return;
    }
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    let mut at = 10usize;
    for _ in 0..count {
        let Some(klen) = body
            .get(at..at + 2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
        else {
            fault(bf, b"[ixbf] catalog page corrupt");
            return;
        };
        let voff = at + 2 + klen;
        let Some(vlen) = body
            .get(voff..voff + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
        else {
            fault(bf, b"[ixbf] catalog page corrupt");
            return;
        };
        let vend = voff + 4 + vlen;
        let Some(rec) = body.get(voff + 4..vend) else {
            fault(bf, b"[ixbf] catalog page corrupt");
            return;
        };
        if let Some(d) = IndexDescriptor::decode(rec) {
            if d.phase == relational::SchemaPhase::Backfilling {
                bf.d = d;
                // Need the table's descriptor for column types.
                let mut key = [0u8; MAX_KV_KEY];
                let Some(kn) = table_descr_key(&mut key, d.table_id) else {
                    fault(bf, b"[ixbf] table key");
                    return;
                };
                let mut k = [0u8; MAX_KV_KEY];
                k[..kn].copy_from_slice(&key[..kn]);
                let Some(bn) = stage_get(bf, &k[..kn]) else {
                    fault(bf, b"[ixbf] table stage");
                    return;
                };
                let _ = kv_send(bf, KV_OP_GET, bn, S_TDESCR);
                return;
            }
        }
        at = vend;
    }
    // Nothing to build.
    bf.state = S_IDLE;
}

fn on_tdescr(bf: &mut BfState, result: u8, body: &[u8]) {
    if result != KV_RESULT_OK {
        fault(bf, b"[ixbf] table descriptor missing");
        return;
    }
    let Some(td) = TableDescriptor::decode(body) else {
        fault(bf, b"[ixbf] table descriptor corrupt");
        return;
    };
    bf.td = td;
    // Resume point, if any.
    let mut key = [0u8; MAX_KV_KEY];
    let Some(kn) = job_key(&mut key, bf.d.index_id) else {
        fault(bf, b"[ixbf] job key");
        return;
    };
    let mut k = [0u8; MAX_KV_KEY];
    k[..kn].copy_from_slice(&key[..kn]);
    let Some(bn) = stage_get(bf, &k[..kn]) else {
        fault(bf, b"[ixbf] job stage");
        return;
    };
    let _ = kv_send(bf, KV_OP_GET, bn, S_JOB);
}

fn on_job(bf: &mut BfState, result: u8, body: &[u8]) {
    bf.cursor = match result {
        KV_RESULT_OK if body.len() >= 8 => {
            u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]))
        }
        KV_RESULT_NOT_FOUND => 0,
        _ => {
            fault(bf, b"[ixbf] job read failed");
            return;
        }
    };
    send_row_scan(bf);
}

fn send_row_scan(bf: &mut BfState) {
    let mut start = [0u8; MAX_KV_KEY];
    let mut end = [0u8; MAX_KV_KEY];
    let Some((sn, en)) = table_bounds(bf.d.table_id, &mut start, &mut end) else {
        fault(bf, b"[ixbf] table bounds");
        return;
    };
    let cursor = bf.cursor;
    let Some(bn) = stage_scan(bf, &start[..sn], &end[..en], cursor, PAGE_LIMIT) else {
        fault(bf, b"[ixbf] scan stage");
        return;
    };
    let _ = kv_send(bf, KV_OP_RANGE_SCAN, bn, S_SCAN);
}

fn on_scan(bf: &mut BfState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        fault(bf, b"[ixbf] row scan failed");
        return;
    }
    bf.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
    let pairs = u16::from_le_bytes([body[8], body[9]]);
    let take = (body.len() - 10).min(PAGE_BUF);
    bf.page[..take].copy_from_slice(&body[10..10 + take]);
    bf.page_len = take as u16;
    bf.page_at = 0;
    bf.page_pairs = pairs;
    if pairs == 0 {
        if bf.cursor != 0 {
            send_row_scan(bf);
        } else {
            publish(bf);
        }
    } else {
        send_next_entry(bf);
    }
}

/// PUT the index entry for the pair at `page_at`, or advance.
fn send_next_entry(bf: &mut BfState) {
    if bf.page_pairs == 0 {
        // Page done: persist the cursor, then next page or publish.
        let mut key = [0u8; MAX_KV_KEY];
        let Some(kn) = job_key(&mut key, bf.d.index_id) else {
            fault(bf, b"[ixbf] job key");
            return;
        };
        let cur = bf.cursor.to_le_bytes();
        let mut k = [0u8; MAX_KV_KEY];
        k[..kn].copy_from_slice(&key[..kn]);
        let Some(bn) = stage_put(bf, &k[..kn], &cur) else {
            fault(bf, b"[ixbf] job persist stage");
            return;
        };
        let _ = kv_send(bf, KV_OP_PUT, bn, S_SAVE);
        return;
    }
    let at = bf.page_at as usize;
    let page_len = bf.page_len as usize;
    let Some(klen) = bf
        .page
        .get(at..at + 2)
        .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
    else {
        fault(bf, b"[ixbf] page corrupt");
        return;
    };
    let koff = at + 2;
    let voff = koff + klen;
    if voff + 4 > page_len {
        fault(bf, b"[ixbf] page corrupt");
        return;
    }
    let vlen = u32::from_le_bytes([
        bf.page[voff],
        bf.page[voff + 1],
        bf.page[voff + 2],
        bf.page[voff + 3],
    ]) as usize;
    let rend = voff + 4 + vlen;
    if rend > page_len || klen < sql_exec::KEYSPACE_PREFIX_LEN {
        fault(bf, b"[ixbf] page corrupt");
        return;
    }
    // The row's PRIMARY KEY is its user-key body (past the keyspace).
    let mut pk = [0u8; relational::MAX_PRIMARY_KEY_LEN];
    let pk_len = klen - sql_exec::KEYSPACE_PREFIX_LEN;
    if pk_len > pk.len() {
        fault(bf, b"[ixbf] pk oversize");
        return;
    }
    for i in 0..pk_len {
        pk[i] = bf.page[koff + sql_exec::KEYSPACE_PREFIX_LEN + i];
    }
    // The indexed column's value from the row payload.
    let col_id = bf.d.key_columns()[0];
    let Some(col) = bf.td.column(col_id) else {
        fault(bf, b"[ixbf] column gone");
        return;
    };
    let ty = col.ty;
    let mut rowbuf = [0u8; PAGE_BUF];
    for i in 0..vlen {
        rowbuf[i] = bf.page[voff + 4 + i];
    }
    let value = match relational::row_lookup(&rowbuf[..vlen], col_id, ty) {
        Some(relational::ColumnLookup::Present(v)) => v,
        Some(relational::ColumnLookup::Absent) => Value::Null,
        None => {
            fault(bf, b"[ixbf] row corrupt");
            return;
        }
    };
    let mut valbuf = [0u8; relational::MAX_INDEX_VALUE_LEN];
    let Some(ivn) = relational::encode_index_value(&mut valbuf, &[ty], &[value]) else {
        fault(bf, b"[ixbf] value encode");
        return;
    };
    let mut ebody =
        [0u8; relational::INDEX_ID_LEN + 2 * relational::MAX_INDEX_VALUE_LEN + 2 * 512 + 4];
    let Some(en) =
        relational::index_user_key(bf.d.index_id, &valbuf[..ivn], &pk[..pk_len], &mut ebody)
    else {
        fault(bf, b"[ixbf] entry key");
        return;
    };
    let mut ekey = [0u8; MAX_KV_KEY + 64];
    let Some(ekn) = kv_key(&mut ekey, relational::KS_RELATIONAL_INDEX, &ebody[..en]) else {
        fault(bf, b"[ixbf] entry wrap");
        return;
    };
    // Consume the pair BEFORE sending: a full channel retries the SAME
    // page from the durable cursor, which re-puts idempotently.
    bf.page_at = rend as u16;
    bf.page_pairs -= 1;
    let mut k = [0u8; MAX_KV_KEY + 64];
    k[..ekn].copy_from_slice(&ekey[..ekn]);
    let Some(bn) = stage_put(bf, &k[..ekn], &[]) else {
        fault(bf, b"[ixbf] entry stage");
        return;
    };
    let _ = kv_send(bf, KV_OP_PUT, bn, S_ENTRY);
}

/// The whole span is indexed: flip the descriptor to Public.
fn publish(bf: &mut BfState) {
    bf.d.phase = relational::SchemaPhase::Public;
    let mut rec = [0u8; 512];
    let Some(rn) = bf.d.encode(&mut rec) else {
        fault(bf, b"[ixbf] descr encode");
        return;
    };
    let mut key = [0u8; MAX_KV_KEY];
    let Some(kn) = index_descr_key(&mut key, bf.d.index_id) else {
        fault(bf, b"[ixbf] descr key");
        return;
    };
    let mut k = [0u8; MAX_KV_KEY];
    k[..kn].copy_from_slice(&key[..kn]);
    let Some(bn) = stage_put(bf, &k[..kn], &rec[..rn]) else {
        fault(bf, b"[ixbf] descr stage");
        return;
    };
    let _ = kv_send(bf, KV_OP_PUT, bn, S_PUBLISH);
}

fn on_kv_response(bf: &mut BfState, result: u8, body: &[u8]) {
    match bf.state {
        S_CATALOG => on_catalog(bf, result, body),
        S_TDESCR => on_tdescr(bf, result, body),
        S_JOB => on_job(bf, result, body),
        S_SCAN => on_scan(bf, result, body),
        S_ENTRY => {
            if result != KV_RESULT_OK {
                fault(bf, b"[ixbf] entry put failed");
                return;
            }
            bf.m_indexed = bf.m_indexed.wrapping_add(1);
            send_next_entry(bf);
        }
        S_SAVE => {
            if result != KV_RESULT_OK {
                fault(bf, b"[ixbf] job persist failed");
                return;
            }
            if bf.cursor != 0 {
                send_row_scan(bf);
            } else {
                publish(bf);
            }
        }
        S_PUBLISH => {
            if result != KV_RESULT_OK {
                fault(bf, b"[ixbf] publish failed");
                return;
            }
            bf.m_published = bf.m_published.wrapping_add(1);
            unsafe {
                if !bf.syscalls.is_null() {
                    let msg = b"[ixbf] index published";
                    dev_log(&*bf.syscalls, 2, msg.as_ptr(), msg.len());
                }
            }
            // Remove the job cursor; a leftover would resume a
            // finished build (harmless but noisy).
            let mut key = [0u8; MAX_KV_KEY];
            let Some(kn) = job_key(&mut key, bf.d.index_id) else {
                fault(bf, b"[ixbf] job key");
                return;
            };
            let mut k = [0u8; MAX_KV_KEY];
            k[..kn].copy_from_slice(&key[..kn]);
            let Some(bn) = stage_delete_one(bf, &k[..kn]) else {
                fault(bf, b"[ixbf] job delete stage");
                return;
            };
            let _ = kv_send(bf, KV_OP_DELETE, bn, S_CLEANUP);
        }
        S_CLEANUP => {
            bf.state = S_IDLE;
        }
        _ => {}
    }
}

// ── Module ABI ───────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<BfState>() as u32
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
    if state_size < core::mem::size_of::<BfState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let bf = unsafe { &mut *state.cast::<BfState>() };
    bf.init(sys_ptr);
    bf.kv_in = in_chan;
    bf.kv_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        bf.metrics_out = dev_channel_port(sys, 1, 1);
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let bf = unsafe { &mut *state.cast::<BfState>() };
    let sys_ptr = bf.syscalls;
    if sys_ptr.is_null() {
        return -1;
    }
    let now = unsafe { dev_millis(&*sys_ptr) };

    unsafe {
        let sys = &*sys_ptr;
        let mut env = [0u8; ENV_BUF];
        if let Some((msg, payload)) = read_one_envelope(sys, bf.kv_in, &mut env) {
            if msg == MSG_KV_RESPONSE && payload.len() >= 20 {
                let corr = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
                let result = payload[9];
                let blen = u16::from_le_bytes([payload[18], payload[19]]) as usize;
                if corr == bf.corr && payload.len() >= 20 + blen {
                    let mut body = [0u8; ENV_BUF];
                    body[..blen].copy_from_slice(&payload[20..20 + blen]);
                    on_kv_response(bf, result, &body[..blen]);
                }
            }
        }
    }

    if bf.state == S_IDLE && now.wrapping_sub(bf.last_poll_ms) >= POLL_MS {
        bf.last_poll_ms = now;
        send_catalog_scan(bf);
    }

    bf.step_ctr = bf.step_ctr.wrapping_add(1);
    if bf.step_ctr.is_multiple_of(5000) {
        unsafe {
            telemetry::emit_counters(
                &*sys_ptr,
                bf.metrics_out,
                &[bf.m_indexed, bf.m_published, bf.m_errors],
            );
        }
    }
    0
}

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
    env[0] = msg_type;
    env[1] = (payload_len & 0xFF) as u8;
    env[2] = ((payload_len >> 8) & 0xFF) as u8;
    let total = wire::ENVELOPE_HDR + payload_len;
    (sys.channel_write)(chan, env.as_mut_ptr(), total) == total as i32
}

unsafe fn read_one_envelope<'a>(
    sys: &SyscallTable,
    chan: i32,
    scratch: &'a mut [u8],
) -> Option<(u8, &'a [u8])> {
    if chan < 0 {
        return None;
    }
    let poll = (sys.channel_poll)(chan, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return None;
    }
    let mut hdr = [0u8; 3];
    if (sys.channel_read)(chan, hdr.as_mut_ptr(), 3) < 3 {
        return None;
    }
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if payload_len > scratch.len() {
        return None;
    }
    if payload_len > 0
        && ((sys.channel_read)(chan, scratch.as_mut_ptr(), payload_len) as usize) < payload_len
    {
        return None;
    }
    Some((hdr[0], &scratch[..payload_len]))
}
