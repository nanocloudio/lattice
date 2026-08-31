//! Cross-range transaction coordinator (RFC database foundation §13).
//!
//! Drives a cross-range transaction through the §13.2 two-phase decision,
//! using the decision machine in `modules/common/txn_coordinator.rs` and
//! the participant ops:
//!
//! ```text
//!   KV_OP_TXN_RECORD    write/advance the home record (the authority)
//!   KV_OP_TXN_PREPARE   stage this participant's writes, collect a vote
//!   KV_OP_TXN_RESOLVE   make the staged writes real, or drop them
//! ```
//!
//! Two modes select how the transaction arrives and how its participant
//! ops reach their ranges:
//!
//! - **Declared (`mode 1`)** — one transaction named in the graph config
//!   (keys, partitions, values, read timestamp), run `start_delay_ms`
//!   after boot. Participant ops are addressed to a NAMED partition
//!   through `KV_OP_TARGETED`, the one door the routing map allows to
//!   reach a chosen range, so the whole of §13.2 is observable on a real
//!   graph with ordinary clients reading the result.
//! - **Dynamic (`mode 2`)** — a transaction submitted at runtime by
//!   `relational_executor` over `MSG_TXN_SUBMIT`: a `KV_OP_TXN` body the
//!   router refused as single-range. The coordinator splits it into one
//!   participant per row and drives §13.2 with NON-targeted participant
//!   ops the router routes by key — so it needs no partition map — then
//!   answers the outcome on `MSG_TXN_SUBMIT_RESULT`. One transaction runs
//!   at a time; a second submit waits behind the first.
//!
//! **Resume.** A coordinator that replicated a decision and then crashed
//! before resolving every intent rebuilds straight into resolution: its
//! reopening `Pending` write is refused with the durable status, and it
//! re-resolves under that decision (`Coordinator::resume_decided`) instead
//! of reporting the outcome unknown.
//!
//! **Helping** lives in `kv_request_router` (`maybe_start_help`): a
//! `KV_RESULT_TXN_PENDING` reply carries the txn id, and the router reads
//! the home record and resolves the stranded intent under the decision it
//! finds there.
//!
//! The read timestamp comes from the declaration or the submit, never from
//! `timestamp_allocator`; `may_commit` refuses zero, so an unleased
//! transaction cannot commit.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    clippy::missing_safety_doc,
    clippy::not_unsafe_ptr_arg_deref,
    reason = "fluxor module ABI: raw pointers are the contract; the PIC build path-mounts the fluxor SDK wholesale and each module consumes only a subset of the ABI surface"
)]

use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/wire.rs"]
mod wire;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/txn_coordinator.rs"]
mod coord;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use coord::{Coordinator, Participant, Phase, Step, TransactionRecord, TxnOutcome, TxnStatus};
use types::{
    KV_OP_PUT, KV_OP_TARGETED, KV_OP_TXN_PREPARE, KV_OP_TXN_RECORD, KV_OP_TXN_RESOLVE,
    KV_RESULT_CAS_FAILED, KV_RESULT_INTEGER, KV_RESULT_OK, PROTO_INTERNAL_LIFECYCLE,
    TXN_CMP_MOD_EQUAL,
};
use wire::{
    MSG_KV_REQUEST, MSG_KV_RESPONSE, MSG_TXN_SUBMIT, MSG_TXN_SUBMIT_RESULT, TXN_SUBMIT_ABORTED,
    TXN_SUBMIT_COMMITTED, TXN_SUBMIT_ERROR,
};

/// Bound on a declared key or value. Small on purpose: this declares a
/// transaction, not a payload.
const OPERAND_MAX: usize = 64;
/// Two participants. The decision machine handles up to
/// `MAX_PARTICIPANTS`; the router's `MAX_PARTITION_PORTS` is 2, so two
/// is what a graph can actually address today. Declaring a third would
/// name a partition with no port.
const PARTICIPANTS: usize = 2;

const ENV_BUF: usize = 1024;
/// Offset in `env` where a KV request body is staged.
const BODY_AT: usize = wire::ENVELOPE_HDR + 18;
/// Inner-body staging area (after the 3-byte TARGETED wrapper).
const INNER_AT: usize = BODY_AT + 3;

// ── Module state ──────────────────────────────────────────────────────

const S_IDLE: u8 = 0;
const S_WAIT_DELAY: u8 = 1;
const S_RUNNING: u8 = 2;
const S_DONE: u8 = 3;

#[repr(C)]
struct CoordState {
    syscalls: *const SyscallTable,
    kv_in: i32,
    kv_out: i32,
    metrics_out: i32,

    // ── Declaration ──────────────────────────────────────────────────
    mode: u8,
    start_delay_ms: u32,
    txn_id_lo: u64,
    read_timestamp: u64,
    partition: [u16; PARTICIPANTS],
    key_hex: [[u8; OPERAND_MAX * 2]; PARTICIPANTS],
    key_hex_len: [u16; PARTICIPANTS],
    val_hex: [[u8; OPERAND_MAX * 2]; PARTICIPANTS],
    val_hex_len: [u16; PARTICIPANTS],

    // ── Decoded ──────────────────────────────────────────────────────
    key: [[u8; OPERAND_MAX]; PARTICIPANTS],
    key_len: [u16; PARTICIPANTS],
    val: [[u8; OPERAND_MAX]; PARTICIPANTS],
    val_len: [u16; PARTICIPANTS],

    // ── Dynamic mode (mode 2): cross-range txns submitted at runtime by
    //    relational_executor over `submit_in`, answered on `result_out` ──
    submit_in: i32,
    result_out: i32,
    /// True while a dynamic transaction is in flight, so its terminal
    /// outcome is reported back to the submitter exactly once.
    dyn_active: bool,
    dyn_reported: bool,
    dyn_client_corr: u64,
    dyn_proto: u8,
    dyn_conn: u8,
    /// Monotonic id source for dynamic transactions (the config `txn_id`
    /// is a single declared transaction; dynamic ones need distinct ids).
    dyn_seq: u64,

    // ── Runtime ──────────────────────────────────────────────────────
    machine: Coordinator,
    state: u8,
    /// True for a dynamic (mode 2) transaction: participant ops are sent
    /// NON-targeted so the router routes each by key, and every prepare
    /// carries the row's absence comparison (INSERT semantics).
    dynamic: bool,
    boot_ms: u64,
    corr: u64,
    /// What the outstanding request was, so the reply can be
    /// interpreted. `Step::Wait` = nothing outstanding.
    inflight: Step,
    step_ctr: u32,
    env: [u8; ENV_BUF],

    // Telemetry: phase, commits, aborts, refusals.
    m_phase: u64,
    m_committed: u64,
    m_aborted: u64,
    m_errors: u64,
}

impl CoordState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.kv_in = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.submit_in = -1;
        self.result_out = -1;
        self.dyn_active = false;
        self.dyn_reported = false;
        self.dyn_client_corr = 0;
        self.dyn_proto = 0;
        self.dyn_conn = 0;
        self.dyn_seq = 0;
        self.dynamic = false;
        self.mode = 0;
        self.start_delay_ms = 3_000;
        self.txn_id_lo = 0;
        self.read_timestamp = 0;
        self.partition = [0, 1];
        self.key_hex_len = [0; PARTICIPANTS];
        self.val_hex_len = [0; PARTICIPANTS];
        self.key_len = [0; PARTICIPANTS];
        self.val_len = [0; PARTICIPANTS];
        self.state = S_IDLE;
        self.boot_ms = 0;
        self.corr = 0;
        self.inflight = Step::Wait;
        self.step_ctr = 0;
        self.m_phase = 0;
        self.m_committed = 0;
        self.m_aborted = 0;
        self.m_errors = 0;
    }
}

define_params! {
    CoordState;

    // 0 = inert. An inert coordinator is legal and costs nothing: wire
    // it into a graph before you have a transaction for it.
    1, mode, u8, 0 => |s, d, len| { s.mode = p_u8(d, len, 0, 0); };

    // The manual execution gate, same shape as range_supervisor's:
    // give the graph time to come up and replay before a transaction
    // starts staging intents across it.
    2, start_delay_ms, u32, 3000
        => |s, d, len| { s.start_delay_ms = p_u32(d, len, 0, 3000); };

    // Low half of the transaction id; the high half is a fixed marker
    // so a declared transaction is recognisable in a record dump. Zero
    // is refused at init, because a zero id is not a transaction.
    3, txn_id, u32, 0 => |s, d, len| { s.txn_id_lo = p_u32(d, len, 0, 0) as u64; };

    // Read timestamp. `may_commit` refuses zero, so a config that
    // omits this cannot commit — deliberately, since an unleased
    // timestamp breaks serializability in a way no later check catches.
    4, read_timestamp, u32, 0
        => |s, d, len| { s.read_timestamp = p_u32(d, len, 0, 0) as u64; };

    5, partition_a, u16, 0 => |s, d, len| { s.partition[0] = p_u16(d, len, 0, 0); };
    6, partition_b, u16, 1 => |s, d, len| { s.partition[1] = p_u16(d, len, 0, 1); };

    // Operands, hex-encoded. TLV str chunks append, so a long value
    // arriving in pieces accumulates rather than truncating.
    7, key_a, str, 0 => |s, d, len| { append_hex(&mut s.key_hex[0], &mut s.key_hex_len[0], d, len); };
    8, val_a, str, 0 => |s, d, len| { append_hex(&mut s.val_hex[0], &mut s.val_hex_len[0], d, len); };
    9, key_b, str, 0 => |s, d, len| { append_hex(&mut s.key_hex[1], &mut s.key_hex_len[1], d, len); };
    10, val_b, str, 0 => |s, d, len| { append_hex(&mut s.val_hex[1], &mut s.val_hex_len[1], d, len); };
}

/// Append a TLV str chunk to a hex operand buffer. Overflow latches
/// `u16::MAX`, which init refuses — a truncated key would address the
/// wrong record, so this fails closed rather than short.
///
/// # Safety
/// `d` must point at `len` readable bytes.
unsafe fn append_hex(out: &mut [u8; OPERAND_MAX * 2], out_len: &mut u16, d: *const u8, len: usize) {
    if *out_len == u16::MAX {
        return;
    }
    let at = *out_len as usize;
    if at + len <= OPERAND_MAX * 2 {
        for i in 0..len {
            out[at + i] = *d.add(i);
        }
        *out_len = (at + len) as u16;
    } else {
        *out_len = u16::MAX;
    }
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn hex_decode(src: &[u8], out: &mut [u8]) -> Option<usize> {
    if !src.len().is_multiple_of(2) || src.len() / 2 > out.len() {
        return None;
    }
    for i in 0..src.len() / 2 {
        out[i] = (hex_nibble(src[i * 2])? << 4) | hex_nibble(src[i * 2 + 1])?;
    }
    Some(src.len() / 2)
}

/// The declared transaction id. The high half is a fixed marker so a
/// declared transaction is recognisable in a record dump.
fn txn_id(s: &CoordState) -> u128 {
    (0x4C41_5454_4943_4500u128 << 64) | s.txn_id_lo as u128
}

/// A participant's range id. Derived from the partition rather than
/// declared: the record's identity must match what a participant would
/// compute for itself, and two sources of truth for a range id is how
/// `WrongTransaction` starts firing on correct transactions.
fn range_id(partition: u16) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0..2].copy_from_slice(&partition.to_le_bytes());
    id
}

/// The home range: participant 0's. The record lives there and it is
/// the only authority on the outcome.
fn home_range(s: &CoordState) -> [u8; 16] {
    range_id(s.partition[0])
}

// ── KV request plumbing ───────────────────────────────────────────────

/// Send one TARGETED KV request with `inner_len` bytes staged at
/// `INNER_AT`. False on a full channel — retried next step.
fn kv_send_targeted(s: &mut CoordState, partition: u16, inner_op: u8, inner_len: usize) -> bool {
    s.corr = s.corr.wrapping_add(1).max(1);
    let corr = s.corr;
    const REQ_HEAD: usize = 18;
    let body_len = 3 + inner_len;
    let at = wire::ENVELOPE_HDR;
    if at + REQ_HEAD + body_len > s.env.len() {
        return false;
    }
    s.env[at..at + 8].copy_from_slice(&corr.to_le_bytes());
    s.env[at + 8] = PROTO_INTERNAL_LIFECYCLE;
    s.env[at + 9..at + 13].copy_from_slice(&0u32.to_le_bytes());
    s.env[at + 13] = 0;
    s.env[at + 14] = 0;
    s.env[at + 15] = KV_OP_TARGETED;
    s.env[at + 16..at + 18].copy_from_slice(&(body_len as u16).to_le_bytes());
    s.env[BODY_AT..BODY_AT + 2].copy_from_slice(&partition.to_le_bytes());
    s.env[BODY_AT + 2] = inner_op;
    // SAFETY: `syscalls` is non-null (checked at init and each step).
    unsafe {
        let sys = s.syscalls;
        !sys.is_null()
            && write_envelope(
                &*sys,
                s.kv_out,
                MSG_KV_REQUEST,
                REQ_HEAD + body_len,
                &mut s.env,
            )
    }
}

/// Send one PLAIN (non-targeted) KV request with `inner_len` bytes staged
/// at `INNER_AT`, op `inner_op`. The router routes it by the op's OWN key
/// — which is what a dynamic participant op needs, since the coordinator
/// holds no map. The staged content is shifted down over the 3-byte
/// TARGETED slot (`INNER_AT` = `BODY_AT + 3`) the helpers leave room for.
fn kv_send_plain(s: &mut CoordState, inner_op: u8, inner_len: usize) -> bool {
    s.corr = s.corr.wrapping_add(1).max(1);
    let corr = s.corr;
    const REQ_HEAD: usize = 18;
    let at = wire::ENVELOPE_HDR;
    if at + REQ_HEAD + inner_len > s.env.len() {
        return false;
    }
    s.env.copy_within(INNER_AT..INNER_AT + inner_len, BODY_AT);
    s.env[at..at + 8].copy_from_slice(&corr.to_le_bytes());
    s.env[at + 8] = PROTO_INTERNAL_LIFECYCLE;
    s.env[at + 9..at + 13].copy_from_slice(&0u32.to_le_bytes());
    s.env[at + 13] = 0;
    s.env[at + 14] = 0;
    s.env[at + 15] = inner_op;
    s.env[at + 16..at + 18].copy_from_slice(&(inner_len as u16).to_le_bytes());
    // SAFETY: `syscalls` non-null (checked at init and each step).
    unsafe {
        let sys = s.syscalls;
        !sys.is_null()
            && write_envelope(
                &*sys,
                s.kv_out,
                MSG_KV_REQUEST,
                REQ_HEAD + inner_len,
                &mut s.env,
            )
    }
}

/// Stage + send `KV_OP_TXN_RECORD` at `status` to the home partition.
fn send_record(s: &mut CoordState, status: TxnStatus) -> bool {
    let mut rec = s.machine.record;
    rec.status = status;
    let mut buf = [0u8; 256];
    let Some(n) = rec.encode(&mut buf) else {
        return false;
    };
    if INNER_AT + n > s.env.len() {
        return false;
    }
    s.env[INNER_AT..INNER_AT + n].copy_from_slice(&buf[..n]);
    if s.dynamic {
        // The router routes a record by `[KS_TXN_RECORD || txn_id]`, so a
        // plain send lands it on the right range with no map.
        kv_send_plain(s, KV_OP_TXN_RECORD, n)
    } else {
        kv_send_targeted(s, s.partition[0], KV_OP_TXN_RECORD, n)
    }
}

/// Stage + send `KV_OP_TXN_PREPARE` to participant `i`.
///
/// Body: `[txn_id:u128][home:16][provisional_ts:u64][epoch:u32]` then a
/// `KV_OP_TXN` body carrying no comparisons and one PUT — this
/// participant's half of the transaction, and only its half. Sending a
/// participant an op for a key it does not own is how a two-phase
/// commit silently writes to the wrong range.
fn send_prepare(s: &mut CoordState, i: usize) -> bool {
    let kl = s.key_len[i] as usize;
    let vl = s.val_len[i] as usize;
    // PUT body: [klen:u16][key][vlen:u32][val][flags:u8][expiry:u64]
    let put_len = 2 + kl + 4 + vl + 1 + 8;
    // A dynamic (SQL INSERT) participant also carries a one-comparison
    // absence check: [cmp_op:u8][klen:u16][key][witness:u64].
    let cmp_len = if s.dynamic { 1 + 2 + kl + 8 } else { 0 };
    let need = 16 + 16 + 8 + 4 + 2 + cmp_len + 2 + 1 + 2 + put_len + 2;
    if INNER_AT + need > s.env.len() {
        return false;
    }
    let id = txn_id(s);
    let home = home_range(s);
    let ts = s.machine.record.provisional_commit_timestamp;
    let epoch = s.machine.record.epoch;

    let mut p = INNER_AT;
    s.env[p..p + 16].copy_from_slice(&id.to_le_bytes());
    p += 16;
    s.env[p..p + 16].copy_from_slice(&home);
    p += 16;
    s.env[p..p + 8].copy_from_slice(&ts.to_le_bytes());
    p += 8;
    s.env[p..p + 4].copy_from_slice(&epoch.to_le_bytes());
    p += 4;
    if s.dynamic {
        // cmp_count = 1: the row's key MUST be absent (INSERT). A failed
        // comparison is this participant's vote to abort, which is how a
        // cross-range duplicate key is refused without writing half a row.
        s.env[p..p + 2].copy_from_slice(&1u16.to_le_bytes());
        p += 2;
        s.env[p] = types::TXN_CMP_MOD_EQUAL;
        p += 1;
        s.env[p..p + 2].copy_from_slice(&(kl as u16).to_le_bytes());
        p += 2;
        s.env[p..p + kl].copy_from_slice(&s.key[i][..kl]);
        p += kl;
        s.env[p..p + 8].copy_from_slice(&0u64.to_le_bytes()); // witness 0 = absent
        p += 8;
    } else {
        // cmp_count = 0. The declared form carries no comparisons; a
        // participant with none votes on staging alone.
        s.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
        p += 2;
    }
    // then_count = 1
    s.env[p..p + 2].copy_from_slice(&1u16.to_le_bytes());
    p += 2;
    s.env[p] = KV_OP_PUT;
    p += 1;
    s.env[p..p + 2].copy_from_slice(&(put_len as u16).to_le_bytes());
    p += 2;
    s.env[p..p + 2].copy_from_slice(&(kl as u16).to_le_bytes());
    p += 2;
    s.env[p..p + kl].copy_from_slice(&s.key[i][..kl]);
    p += kl;
    s.env[p..p + 4].copy_from_slice(&(vl as u32).to_le_bytes());
    p += 4;
    s.env[p..p + vl].copy_from_slice(&s.val[i][..vl]);
    p += vl;
    s.env[p] = 0; // flags
    p += 1;
    s.env[p..p + 8].copy_from_slice(&0u64.to_le_bytes()); // no expiry
    p += 8;
    // else_count = 0
    s.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes());
    p += 2;

    let inner_len = p - INNER_AT;
    if s.dynamic {
        // Non-targeted: the router routes the prepare by its own key, so
        // the coordinator needs no map to place it.
        kv_send_plain(s, KV_OP_TXN_PREPARE, inner_len)
    } else {
        kv_send_targeted(s, s.partition[i], KV_OP_TXN_PREPARE, inner_len)
    }
}

/// Stage + send `KV_OP_TXN_RESOLVE` to participant `i`.
fn send_resolve(s: &mut CoordState, i: usize, committed: bool) -> bool {
    let kl = s.key_len[i] as usize;
    let need = 16 + 16 + 4 + 1 + 8 + 2 + 2 + kl;
    if INNER_AT + need > s.env.len() {
        return false;
    }
    let id = txn_id(s);
    let home = home_range(s);
    let epoch = s.machine.record.epoch;
    let commit_ts = s.machine.record.provisional_commit_timestamp;

    let mut p = INNER_AT;
    s.env[p..p + 16].copy_from_slice(&id.to_le_bytes());
    p += 16;
    s.env[p..p + 16].copy_from_slice(&home);
    p += 16;
    s.env[p..p + 4].copy_from_slice(&epoch.to_le_bytes());
    p += 4;
    s.env[p] = u8::from(committed);
    p += 1;
    s.env[p..p + 8].copy_from_slice(&commit_ts.to_le_bytes());
    p += 8;
    s.env[p..p + 2].copy_from_slice(&1u16.to_le_bytes());
    p += 2;
    s.env[p..p + 2].copy_from_slice(&(kl as u16).to_le_bytes());
    p += 2;
    s.env[p..p + kl].copy_from_slice(&s.key[i][..kl]);
    p += kl;

    let inner_len = p - INNER_AT;
    if s.dynamic {
        kv_send_plain(s, KV_OP_TXN_RESOLVE, inner_len)
    } else {
        kv_send_targeted(s, s.partition[i], KV_OP_TXN_RESOLVE, inner_len)
    }
}

/// A read u16/u32/etc. helper over a cursor.
fn take_u16(b: &[u8], p: &mut usize) -> Option<u16> {
    let v = u16::from_le_bytes(b.get(*p..*p + 2)?.try_into().ok()?);
    *p += 2;
    Some(v)
}

/// Begin a dynamic (mode 2) cross-range transaction from a
/// `MSG_TXN_SUBMIT` payload. Splits the forwarded `KV_OP_TXN` body into
/// one participant per PUT (INSERT shape: `cmp_count == then_count`),
/// builds the machine, and enters `S_RUNNING`. On any malformed or
/// out-of-scope input it reports `TXN_SUBMIT_ERROR` and stays idle rather
/// than staging half a transaction.
fn start_dynamic_txn(s: &mut CoordState, payload: &[u8]) {
    // Header: [client_corr:8][proto:1][conn:1][read_ts:8][body_len:2].
    if payload.len() < 20 {
        return;
    }
    s.dyn_client_corr = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
    s.dyn_proto = payload[8];
    s.dyn_conn = payload[9];
    let read_ts = u64::from_le_bytes(payload[10..18].try_into().unwrap_or([0; 8]));
    let body_len = u16::from_le_bytes(payload[18..20].try_into().unwrap_or([0; 2])) as usize;
    s.dyn_reported = false;

    let ok = (|| -> Option<()> {
        let body = payload.get(20..20 + body_len)?;
        let mut p = 0usize;
        let cmp_count = take_u16(body, &mut p)? as usize;
        // Skip the comparisons: [op:1][klen:2][key][witness:8]. The
        // coordinator regenerates an absence comparison per participant,
        // so it only needs the PUT key/val, but it must step past these.
        for _ in 0..cmp_count {
            let _op = *body.get(p)?;
            p += 1;
            let klen = take_u16(body, &mut p)? as usize;
            p += klen + 8;
        }
        let then_count = take_u16(body, &mut p)? as usize;
        // This driver handles the INSERT shape: one comparison and one PUT
        // per row, at most `PARTICIPANTS` rows. A body with index entries
        // (then_count > cmp_count) or more rows than participant ports is
        // declined, not mis-split.
        if then_count == 0 || then_count > PARTICIPANTS || then_count != cmp_count {
            return None;
        }
        for i in 0..then_count {
            let op = *body.get(p)?;
            p += 1;
            if op != KV_OP_PUT {
                return None;
            }
            let put_len = take_u16(body, &mut p)? as usize;
            let put_end = p + put_len;
            let klen = take_u16(body, &mut p)? as usize;
            let key = body.get(p..p + klen)?;
            p += klen;
            let vlen = u32::from_le_bytes(body.get(p..p + 4)?.try_into().ok()?) as usize;
            p += 4;
            let val = body.get(p..p + vlen)?;
            if klen > OPERAND_MAX || vlen > OPERAND_MAX {
                return None;
            }
            s.key[i][..klen].copy_from_slice(key);
            s.key_len[i] = klen as u16;
            s.val[i][..vlen].copy_from_slice(val);
            s.val_len[i] = vlen as u16;
            // A participant identity distinct per row, consistent with the
            // record. Routing is by key (plain sends), so this need only
            // be unique, not a real range id.
            s.partition[i] = i as u16;
            p = put_end; // past flags+expiry, whatever their exact width
        }
        // A fresh id per dynamic transaction.
        s.dyn_seq = s.dyn_seq.wrapping_add(1);
        s.txn_id_lo = 0x1000_0000_0000_0000u64.wrapping_add(s.dyn_seq);
        let n = then_count;
        let record = TransactionRecord::begin(txn_id(s), home_range(s), read_ts, 0, 1, 0, 0).ok()?;
        let mut participants = [Participant {
            range_id: [0; 16],
            generation: 1,
            prepared: false,
        }; PARTICIPANTS];
        for (i, part) in participants.iter_mut().enumerate().take(n) {
            part.range_id = range_id(s.partition[i]);
        }
        let machine = Coordinator::begin(record, &participants[..n]).ok()?;
        s.machine = machine;
        Some(())
    })();

    if ok.is_some() {
        s.dynamic = true;
        s.dyn_active = true;
        s.inflight = Step::Wait;
        s.state = S_RUNNING;
    } else {
        // Malformed / out of scope: answer once, stay idle.
        s.dyn_active = true; // so report runs, then clears it
        report_dynamic_error(s);
    }
}

/// Report a dynamic transaction's terminal outcome to the submitter and
/// return to idle for the next one.
fn report_dynamic_result(s: &mut CoordState) {
    let outcome = match s.machine.outcome() {
        Some(TxnOutcome::Committed) => TXN_SUBMIT_COMMITTED,
        Some(TxnOutcome::Aborted) => TXN_SUBMIT_ABORTED,
        _ => TXN_SUBMIT_ERROR,
    };
    emit_dynamic_result(s, outcome);
}

/// Report an immediate error (bad submit) without running a machine.
fn report_dynamic_error(s: &mut CoordState) {
    emit_dynamic_result(s, TXN_SUBMIT_ERROR);
}

fn emit_dynamic_result(s: &mut CoordState, outcome: u8) {
    let at = wire::ENVELOPE_HDR;
    if at + 11 <= s.env.len() {
        s.env[at..at + 8].copy_from_slice(&s.dyn_client_corr.to_le_bytes());
        s.env[at + 8] = s.dyn_proto;
        s.env[at + 9] = s.dyn_conn;
        s.env[at + 10] = outcome;
        // SAFETY: `syscalls` non-null (checked each step).
        unsafe {
            let sys = s.syscalls;
            if !sys.is_null() {
                write_envelope(&*sys, s.result_out, MSG_TXN_SUBMIT_RESULT, 11, &mut s.env);
            }
        }
    }
    s.dyn_reported = true;
    s.dyn_active = false;
    s.dynamic = false;
    s.state = S_IDLE;
}

/// Issue whatever the machine asks for next, one request at a time.
///
/// One in-flight request rather than a pipeline, deliberately: the
/// participants are two ports on one router and the transaction is not
/// latency-critical, while a pipeline would need per-correlation
/// demultiplexing that buys nothing here and is somewhere else to get
/// the mapping from reply to participant wrong.
fn pump(s: &mut CoordState) {
    if s.inflight != Step::Wait {
        return;
    }
    let step = s.machine.step();
    // Instrumentation. The merge pump taught this the hard way: five
    // plausible hypotheses each produced a fix that changed nothing,
    // and one line showing what was ACTUALLY happening ended it in a
    // minute. `st` is the Step discriminant, `ph` the machine phase.
    {
        let mut msg = *b"[txnc] step st=0 ph=0 err=00";
        msg[15] = b'0'
            + match step {
                Step::WriteRecord(_) => 1,
                Step::Prepare(_) => 2,
                Step::Resolve { .. } => 3,
                Step::Wait => 4,
                Step::Done(_) => 5,
            };
        msg[20] = b'0'
            + match s.machine.phase() {
                Phase::Idle => 0,
                Phase::OpeningRecord => 1,
                Phase::Preparing => 2,
                Phase::Staging => 3,
                Phase::Deciding => 4,
                Phase::Resolving => 5,
                Phase::Done => 6,
            };
        msg[26] = b'0' + ((s.m_errors / 10) % 10) as u8;
        msg[27] = b'0' + (s.m_errors % 10) as u8;
        // SAFETY: syscalls non-null while stepping.
        unsafe { dev_log(&*s.syscalls, 2, msg.as_ptr(), msg.len()) };
    }
    let sent = match step {
        Step::WriteRecord(status) => send_record(s, status),
        Step::Prepare(i) => send_prepare(s, i),
        Step::Resolve { index, committed } => send_resolve(s, index, committed),
        Step::Wait => return,
        Step::Done(outcome) => {
            s.state = S_DONE;
            match outcome {
                TxnOutcome::Committed => s.m_committed += 1,
                TxnOutcome::Aborted => s.m_aborted += 1,
                _ => s.m_errors += 1,
            }
            s.m_phase = 9;
            return;
        }
    };
    if sent {
        s.inflight = step;
    } else {
        // Channel full, or the request would not fit. Release the
        // machine's claim so the same step is re-issued next tick
        // rather than skipped — a skipped prepare is a participant
        // that never votes.
        match step {
            Step::Prepare(i) => s.machine.on_send_lost(i),
            Step::Resolve { index, .. } => s.machine.on_send_lost(index),
            _ => {}
        }
        s.m_errors += 1;
    }
}

/// Interpret a reply to the outstanding request. `record_status` is the
/// first response-body byte — meaningful only on a refused record write,
/// where the store returns the EXISTING record's status.
fn on_kv_response(s: &mut CoordState, result: u8, record_status: u8) {
    let step = s.inflight;
    s.inflight = Step::Wait;
    match step {
        Step::WriteRecord(status) => {
            if result == KV_RESULT_OK {
                s.machine.on_record_acked(status);
                s.m_phase = match status {
                    TxnStatus::Pending => 1,
                    TxnStatus::Staging => 2,
                    TxnStatus::Committed => 3,
                    TxnStatus::Aborted => 4,
                };
            } else if result == KV_RESULT_CAS_FAILED {
                // The lattice refused the status move: another authority
                // already advanced this record. If THIS write was the
                // reopening `Pending` (a restart) and the record is
                // already DECIDED, the authority is a previous incarnation
                // of THIS coordinator — resume resolution under the
                // decision it durably made rather than concluding the
                // outcome is unknown (the intents would otherwise wait for
                // a router helper). Any other case is a genuine
                // not-the-authority: stand down.
                let resumed = if status == TxnStatus::Pending {
                    match TxnStatus::from_u8(record_status) {
                        Some(TxnStatus::Committed) => {
                            s.machine.resume_decided(true);
                            s.m_phase = 3;
                            true
                        }
                        Some(TxnStatus::Aborted) => {
                            s.machine.resume_decided(false);
                            s.m_phase = 4;
                            true
                        }
                        _ => false,
                    }
                } else {
                    false
                };
                if !resumed {
                    s.machine.on_record_refused();
                    s.m_errors += 1;
                }
            } else {
                s.m_errors += 1;
            }
        }
        Step::Prepare(i) => {
            // OK = prepared. CAS_FAILED = a comparison failed or
            // another transaction holds an intent: a vote to abort,
            // not a transport error. Anything else is a fault, and a
            // fault is not a yes.
            match result {
                KV_RESULT_OK => s.machine.on_prepare_reply(i, true),
                KV_RESULT_CAS_FAILED => s.machine.on_prepare_reply(i, false),
                _ => {
                    s.m_errors += 1;
                    s.machine.on_send_lost(i);
                }
            }
        }
        Step::Resolve { index, .. } => {
            if result == KV_RESULT_INTEGER {
                s.machine.on_resolve_acked(index);
            } else {
                // Resolution is retried until acked, forever: an
                // unresolved intent outlives its decision and blocks
                // every reader of that key.
                s.m_errors += 1;
                s.machine.on_send_lost(index);
            }
        }
        Step::Wait | Step::Done(_) => {}
    }
}

// ── ABI ───────────────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<CoordState>() as u32
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
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    if state.is_null() || syscalls.is_null() {
        return -1;
    }
    if state_size < core::mem::size_of::<CoordState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    // SAFETY: the runtime guarantees `state` points at `state_size`
    // bytes of zeroed, suitably aligned storage for this module.
    let s = unsafe { &mut *state.cast::<CoordState>() };
    s.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(s, params, params_len) };
    }
    s.kv_in = in_chan;
    s.kv_out = out_chan;
    // SAFETY: sys_ptr non-null, checked above.
    unsafe {
        s.metrics_out = dev_channel_port(&*sys_ptr, 1, 1);
    }

    if s.mode == 0 {
        s.state = S_IDLE;
        return 0;
    }
    if s.mode == 2 {
        // Dynamic cross-range transactions from relational_executor. No
        // declaration: sit idle until a `MSG_TXN_SUBMIT` arrives on
        // `submit_in`, drive it, and answer on `result_out`.
        // SAFETY: sys_ptr non-null, checked above.
        unsafe {
            s.submit_in = dev_channel_port(&*sys_ptr, 0, 1);
            s.result_out = dev_channel_port(&*sys_ptr, 1, 2);
        }
        if s.submit_in < 0 || s.result_out < 0 {
            return -1;
        }
        s.state = S_IDLE;
        return 0;
    }
    if s.mode != 1 {
        return -1;
    }
    // A zero id is not a transaction, and a zero read timestamp is an
    // unleased one. Refuse at boot rather than stage intents for a
    // transaction that `may_commit` will refuse at the last moment,
    // when they are already in the store.
    if s.txn_id_lo == 0 || s.read_timestamp == 0 {
        return -1;
    }
    if s.partition[0] == s.partition[1] {
        // Both halves in one partition is not a cross-range
        // transaction; it is a KV_OP_TXN, which already works and is
        // atomic without any of this.
        return -1;
    }
    for i in 0..PARTICIPANTS {
        let (khl, vhl) = (s.key_hex_len[i], s.val_hex_len[i]);
        if khl == 0 || vhl == 0 || khl == u16::MAX || vhl == u16::MAX {
            return -1;
        }
        let (kh, vh) = (khl as usize, vhl as usize);
        let mut kbuf = [0u8; OPERAND_MAX];
        let mut vbuf = [0u8; OPERAND_MAX];
        let mut khex = [0u8; OPERAND_MAX * 2];
        let mut vhex = [0u8; OPERAND_MAX * 2];
        khex[..kh].copy_from_slice(&s.key_hex[i][..kh]);
        vhex[..vh].copy_from_slice(&s.val_hex[i][..vh]);
        let (Some(kn), Some(vn)) = (
            hex_decode(&khex[..kh], &mut kbuf),
            hex_decode(&vhex[..vh], &mut vbuf),
        ) else {
            return -1;
        };
        s.key[i][..kn].copy_from_slice(&kbuf[..kn]);
        s.key_len[i] = kn as u16;
        s.val[i][..vn].copy_from_slice(&vbuf[..vn]);
        s.val_len[i] = vn as u16;
    }

    let Ok(record) =
        TransactionRecord::begin(txn_id(s), home_range(s), s.read_timestamp, 0, 1, 0, 0)
    else {
        return -1;
    };
    let participants = [
        Participant {
            range_id: range_id(s.partition[0]),
            generation: 1,
            prepared: false,
        },
        Participant {
            range_id: range_id(s.partition[1]),
            generation: 1,
            prepared: false,
        },
    ];
    let Ok(machine) = Coordinator::begin(record, &participants) else {
        return -1;
    };
    s.machine = machine;
    s.state = S_WAIT_DELAY;
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    // SAFETY: `state` is the pointer handed to `module_new`.
    let s = unsafe { &mut *state.cast::<CoordState>() };
    let sys_ptr = s.syscalls;
    if sys_ptr.is_null() {
        return -1;
    }
    // SAFETY: sys_ptr non-null.
    let now = unsafe { dev_millis(&*sys_ptr) };
    if s.boot_ms == 0 {
        s.boot_ms = now.max(1);
    }

    // Drain one KV response per step (single in-flight correlation).
    // SAFETY: sys_ptr non-null; `env` is module-owned storage.
    unsafe {
        let sys = &*sys_ptr;
        let mut env = [0u8; ENV_BUF];
        if let Some((msg, payload)) = read_one_envelope(sys, s.kv_in, &mut env) {
            // MSG_KV_RESPONSE head: [corr:8][conn:1][result:1][rev:8][blen:2]
            // Log EVERY envelope, matched or not. "No reply arrived" and
            // "a reply arrived that we discarded" are different bugs and
            // look identical from the machine's side.
            let mut m = *b"[txnc] rx msg=000 len=0000 res=000 match=0";
            m[14] = b'0' + (msg / 100) % 10;
            m[15] = b'0' + (msg / 10) % 10;
            m[16] = b'0' + msg % 10;
            let pl = payload.len();
            m[22] = b'0' + ((pl / 1000) % 10) as u8;
            m[23] = b'0' + ((pl / 100) % 10) as u8;
            m[24] = b'0' + ((pl / 10) % 10) as u8;
            m[25] = b'0' + (pl % 10) as u8;
            if payload.len() >= 20 {
                let r = payload[9];
                m[31] = b'0' + (r / 100) % 10;
                m[32] = b'0' + (r / 10) % 10;
                m[33] = b'0' + r % 10;
                let corr = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
                m[40] = if corr == s.corr { b'1' } else { b'0' };
            }
            dev_log(sys, 2, m.as_ptr(), m.len());
            if msg == MSG_KV_RESPONSE && payload.len() >= 20 {
                let corr = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
                if corr == s.corr {
                    // First response-body byte, if any: on a refused
                    // record write it carries the existing record's
                    // status, which the RESUME path reads.
                    let body_first = payload.get(20).copied().unwrap_or(0);
                    on_kv_response(s, payload[9], body_first);
                }
            }
        }
    }

    // Dynamic mode: accept one submitted cross-range transaction when
    // idle. Serial by design — one transaction at a time, like the
    // declared path; a second submit waits behind the first.
    if s.mode == 2 && !s.dyn_active {
        // SAFETY: sys_ptr non-null; `env` module-owned.
        unsafe {
            let sys = &*sys_ptr;
            let mut env = [0u8; ENV_BUF];
            if let Some((msg, payload)) = read_one_envelope(sys, s.submit_in, &mut env) {
                if msg == MSG_TXN_SUBMIT {
                    start_dynamic_txn(s, payload);
                }
            }
        }
    }

    match s.state {
        S_WAIT_DELAY => {
            if now.wrapping_sub(s.boot_ms) >= s.start_delay_ms as u64 {
                s.state = S_RUNNING;
            }
        }
        S_RUNNING => {
            pump(s);
            if s.machine.phase() == Phase::Done {
                pump(s);
            }
            // Dynamic transactions report their terminal outcome to the
            // submitter once, then return to idle for the next one.
            if s.dynamic && s.machine.phase() == Phase::Done && !s.dyn_reported {
                report_dynamic_result(s);
            }
        }
        _ => {}
    }

    s.step_ctr = s.step_ctr.wrapping_add(1);
    if s.step_ctr.is_multiple_of(5000) {
        // SAFETY: sys_ptr non-null.
        unsafe {
            telemetry::emit_counters(
                &*sys_ptr,
                s.metrics_out,
                &[s.m_phase, s.m_committed, s.m_aborted, s.m_errors],
            );
        }
    }
    0
}

/// Stamp the 3-byte envelope header over a payload already staged at
/// `env[ENVELOPE_HDR..]` and write it. Mirrors the supervisor's helper.
///
/// # Safety
/// `sys` must be a live syscall table and `chan` a valid output port.
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

/// Read one envelope from `chan`. Mirrors the supervisor's helper.
///
/// # Safety
/// `sys` must be a live syscall table.
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
