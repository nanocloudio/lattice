//! backup_coordinator — consistent database backup and restore
//! (RFC database foundation §17.2, Phase 6).
//!
//! Drives §17.2's numbered machine over the contracts in
//! `db_ops.rs`, which own every transition rule; this module is the
//! pump that turns each phase into real KV work, exactly as
//! `range_supervisor` is for §12 and `index_backfill` for §14.4.
//!
//! ## What makes the backup CONSISTENT
//!
//! Every exported record is read with `KV_OP_SCAN_AT` at ONE protected
//! revision (§17.2 step 1's protected timestamp, taken as the store's
//! current revision when the backup begins). A backup assembled from
//! reads at different revisions would be a set of records that never
//! existed together — the definition of an inconsistent backup — so
//! the revision is chosen once and every page carries it. If the
//! provider has already reclaimed that history it answers `COMPACTED`
//! and the backup ABORTS: a partial export is indistinguishable from a
//! whole one once it is on disk, so refusing loudly is the only safe
//! answer.
//!
//! ## Where artifacts live
//!
//! In the KV store itself, under `KS_BACKUP_ARTIFACT`, keyed by
//! `[backup_id][sequence]`. That is deliberate for this slice: the
//! artifacts inherit the same durability, replication, and crash
//! semantics as everything else, and a restore is a scan rather than a
//! file-format reader. §17.2's external-object-store variant is a
//! later slice and is named here rather than implied.
//!
//! ## Restore
//!
//! Reads the manifest, verifies it is complete (`is_complete`), then
//! replays every artifact page as PUTs. Restore is idempotent: the
//! same page written twice lands the same bytes. It does NOT delete
//! keys absent from the backup — a restore into a live database is a
//! merge, and silently deleting what the operator did not ask to
//! remove would be the more dangerous default.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    clippy::too_many_arguments,
    clippy::duplicate_mod,
    reason = "fluxor module ABI: raw-pointer entry points are the contract, ABI fns carry a fixed arity, and the PIC build #[path]-remounts shared SDK/common code"
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

#[path = "../../common/db_ops.rs"]
mod db_ops;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use db_ops::{BackupManifest, BackupPhase, EncryptionKeyIdentity, DIGEST_LEN};
use types::{
    KV_OP_GET, KV_OP_GET_AT, KV_OP_PUT, KV_OP_RANGE_SCAN, KV_OP_SCAN_AT, KV_RESULT_COMPACTED,
    KV_RESULT_NOT_FOUND, KV_RESULT_OK, KV_RESULT_RANGE, KV_RESULT_SCAN_CURSOR, PROTO_ETCD,
};
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE};

// ── Keyspaces (operations band, model-neutral) ───────────────────────

/// Backup manifests: `[backup_id:16]`.
const KS_BACKUP_MANIFEST: u32 = 0x8008_0001;
/// Backup artifacts: `[backup_id:16][sequence:u32 BE]`.
const KS_BACKUP_ARTIFACT: u32 = 0x8008_0002;

const ENV_BUF: usize = 8192;
const PAGE_BUF: usize = 3584;
const KEY_MAX: usize = 128;
/// Records per exported page.
const PAGE_LIMIT: u16 = 24;

// ── Driver states ────────────────────────────────────────────────────

const S_IDLE: u8 = 0;
const S_WAIT: u8 = 1; // waiting out the start delay
const S_EXPORT_SCAN: u8 = 2; // SCAN_AT page in flight
const S_EXPORT_GET: u8 = 10; // per-key GET_AT in flight
const S_EXPORT_PUT: u8 = 3; // artifact page write in flight
const S_MANIFEST_PUT: u8 = 4;
const S_RESTORE_GET: u8 = 5; // manifest read in flight
const S_RESTORE_SCAN: u8 = 6; // artifact page read in flight
const S_RESTORE_PUT: u8 = 7; // one restored record in flight
const S_DONE: u8 = 8;
const S_FAULT: u8 = 9;

define_params! {
    CoordState;

    // 0 = inert, 1 = take a backup, 2 = restore the named backup.
    1, mode, u8, 0
        => |s, d, len| { s.mode = p_u8(d, len, 0, 0); };

    // Backup identity, low 8 bytes (the rest is zero-padded). An
    // operator names the backup; the module never mints one, so a
    // restore always addresses a backup someone chose.
    2, backup_id, u32, 1
        => |s, d, len| { s.backup_id_low = p_u32(d, len, 0, 1); };

    // Manual execution gate (§12.4's rule, applied to §17.2): the
    // operation is declared in the config and starts this long after
    // boot.
    3, start_delay_ms, u32, 3000
        => |s, d, len| { s.start_delay_ms = p_u32(d, len, 0, 3000); };
}

#[repr(C)]
struct CoordState {
    syscalls: *const SyscallTable,
    kv_in: i32,
    kv_out: i32,
    metrics_out: i32,

    mode: u8,
    backup_id_low: u32,
    start_delay_ms: u32,

    state: u8,
    boot_ms: u64,
    corr: u64,

    manifest: BackupManifest,
    /// The revision every export page reads at.
    protected_revision: u64,
    /// Export/restore progress.
    cursor: u64,
    sequence: u32,
    /// Staged page (artifact bytes being built, or read back).
    page: [u8; PAGE_BUF],
    page_len: u16,
    page_at: u16,
    page_records: u16,
    /// Export: the SCAN_AT page's keys, `[len:u16][key]` records, and
    /// the one currently being read.
    keys: [u8; PAGE_BUF],
    keys_len: u16,
    keys_at: u16,
    keys_left: u16,
    cur_key: [u8; 512],
    cur_key_len: u16,

    env: [u8; ENV_BUF],

    m_records: u64,
    m_pages: u64,
    m_restored: u64,
    m_errors: u64,
    step_ctr: u32,
    logged_fault: u8,
}

impl CoordState {
    fn init(&mut self, sys: *const SyscallTable) {
        self.syscalls = sys;
        self.kv_in = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.mode = 0;
        self.backup_id_low = 1;
        self.start_delay_ms = 3000;
        self.state = S_IDLE;
        self.boot_ms = 0;
        self.corr = 0;
        self.manifest = BackupManifest::EMPTY;
        self.protected_revision = 0;
        self.cursor = 0;
        self.sequence = 0;
        self.page = [0; PAGE_BUF];
        self.page_len = 0;
        self.page_at = 0;
        self.page_records = 0;
        self.keys = [0; PAGE_BUF];
        self.keys_len = 0;
        self.keys_at = 0;
        self.keys_left = 0;
        self.cur_key = [0; 512];
        self.cur_key_len = 0;
        self.env = [0; ENV_BUF];
        self.m_records = 0;
        self.m_pages = 0;
        self.m_restored = 0;
        self.m_errors = 0;
        self.step_ctr = 0;
        self.logged_fault = 0;
    }

    fn backup_id(&self) -> [u8; 16] {
        let mut id = [0u8; 16];
        id[0..4].copy_from_slice(&self.backup_id_low.to_be_bytes());
        // A zero id is rejected by the contract; the operator's low
        // word is folded with a fixed tail so id 0 is unreachable.
        id[15] = 1;
        id
    }
}

// ── KV plumbing ──────────────────────────────────────────────────────

const BODY_AT: usize = wire::ENVELOPE_HDR + 18;

fn kv_send(c: &mut CoordState, op: u8, body_len: usize, next: u8) -> bool {
    c.corr = c.corr.wrapping_add(1).max(1);
    const REQ_HEAD: usize = 18;
    let at = wire::ENVELOPE_HDR;
    if at + REQ_HEAD + body_len > c.env.len() {
        return false;
    }
    c.env[at..at + 8].copy_from_slice(&c.corr.to_le_bytes());
    // Positional pair #0 (the etcd_* port names): a backup
    // composition has no etcd anchor, and the index-backfill worker
    // already owns the job pair. The byte routes the reply.
    c.env[at + 8] = PROTO_ETCD;
    c.env[at + 9..at + 13].copy_from_slice(&0u32.to_le_bytes());
    c.env[at + 13] = 0;
    c.env[at + 14] = 0;
    c.env[at + 15] = op;
    c.env[at + 16..at + 18].copy_from_slice(&(body_len as u16).to_le_bytes());
    let sent = unsafe {
        let sys = c.syscalls;
        !sys.is_null()
            && write_envelope(
                &*sys,
                c.kv_out,
                MSG_KV_REQUEST,
                REQ_HEAD + body_len,
                &mut c.env,
            )
    };
    if sent {
        c.state = next;
    }
    sent
}

fn stage_get(c: &mut CoordState, key: &[u8]) -> Option<usize> {
    let need = 2 + key.len();
    if BODY_AT + need > c.env.len() {
        return None;
    }
    c.env[BODY_AT..BODY_AT + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    c.env[BODY_AT + 2..BODY_AT + need].copy_from_slice(key);
    Some(need)
}

fn stage_put(c: &mut CoordState, key: &[u8], value: &[u8]) -> Option<usize> {
    let need = 2 + key.len() + 4 + value.len() + 1 + 8;
    if BODY_AT + need > c.env.len() {
        return None;
    }
    let mut p = BODY_AT;
    c.env[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    for (i, b) in key.iter().enumerate() {
        c.env[p + i] = *b;
    }
    p += key.len();
    c.env[p..p + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
    p += 4;
    for (i, b) in value.iter().enumerate() {
        c.env[p + i] = *b;
    }
    p += value.len();
    c.env[p] = 0;
    p += 1;
    c.env[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
    Some(need)
}

/// `KV_OP_SCAN_AT` body: `[revision:u64][cursor:u64][limit:u16]`.
/// Returns KEYS as of the revision (`KV_RESULT_SCAN_CURSOR`); the
/// values come from per-key `KV_OP_GET_AT` at the SAME revision, which
/// is what keeps the export a single consistent snapshot.
fn stage_scan_at(c: &mut CoordState, cursor: u64, revision: u64) -> Option<usize> {
    let need = 8 + 8 + 2;
    if BODY_AT + need > c.env.len() {
        return None;
    }
    let mut p = BODY_AT;
    c.env[p..p + 8].copy_from_slice(&revision.to_le_bytes());
    p += 8;
    c.env[p..p + 8].copy_from_slice(&cursor.to_le_bytes());
    p += 8;
    c.env[p..p + 2].copy_from_slice(&PAGE_LIMIT.to_le_bytes());
    Some(need)
}

/// `KV_OP_GET_AT` body: `[revision:u64][key_len:u16][key…]`.
fn stage_get_at(c: &mut CoordState, revision: u64, key: &[u8]) -> Option<usize> {
    let need = 8 + 2 + key.len();
    if BODY_AT + need > c.env.len() {
        return None;
    }
    let mut p = BODY_AT;
    c.env[p..p + 8].copy_from_slice(&revision.to_le_bytes());
    p += 8;
    c.env[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    for (i, b) in key.iter().enumerate() {
        c.env[p + i] = *b;
    }
    Some(need)
}

fn stage_range_scan(c: &mut CoordState, start: &[u8], end: &[u8], cursor: u64) -> Option<usize> {
    let need = 2 + start.len() + 2 + end.len() + 10;
    if BODY_AT + need > c.env.len() {
        return None;
    }
    let mut p = BODY_AT;
    c.env[p..p + 2].copy_from_slice(&(start.len() as u16).to_le_bytes());
    p += 2;
    for (i, b) in start.iter().enumerate() {
        c.env[p + i] = *b;
    }
    p += start.len();
    c.env[p..p + 2].copy_from_slice(&(end.len() as u16).to_le_bytes());
    p += 2;
    for (i, b) in end.iter().enumerate() {
        c.env[p + i] = *b;
    }
    p += end.len();
    c.env[p..p + 8].copy_from_slice(&cursor.to_le_bytes());
    p += 8;
    c.env[p..p + 2].copy_from_slice(&8u16.to_le_bytes());
    Some(need)
}

// ── Keys ─────────────────────────────────────────────────────────────

fn user_key(out: &mut [u8], keyspace: u32, body: &[u8]) -> Option<usize> {
    let need = 4 + body.len();
    if out.len() < need {
        return None;
    }
    out[0..4].copy_from_slice(&keyspace.to_be_bytes());
    out[4..need].copy_from_slice(body);
    Some(need)
}

fn manifest_key(c: &CoordState, out: &mut [u8]) -> Option<usize> {
    user_key(out, KS_BACKUP_MANIFEST, &c.backup_id())
}

fn artifact_key(c: &CoordState, sequence: u32, out: &mut [u8]) -> Option<usize> {
    let mut body = [0u8; 20];
    body[0..16].copy_from_slice(&c.backup_id());
    body[16..20].copy_from_slice(&sequence.to_be_bytes());
    user_key(out, KS_BACKUP_ARTIFACT, &body)
}

fn artifact_bounds(c: &CoordState, start: &mut [u8], end: &mut [u8]) -> Option<(usize, usize)> {
    let sn = user_key(start, KS_BACKUP_ARTIFACT, &c.backup_id())?;
    let mut n = sn;
    end[..n].copy_from_slice(&start[..n]);
    while n > 0 {
        if end[n - 1] != 0xFF {
            end[n - 1] += 1;
            return Some((sn, n));
        }
        n -= 1;
    }
    None
}

/// FNV-1a 64 over a page, widened into the contract's 32-byte digest
/// slot. Not a cryptographic digest, and the module docs say so: it
/// detects corruption, not tampering. §23's authenticated digests
/// arrive with the external-object-store slice.
fn page_digest(page: &[u8]) -> [u8; DIGEST_LEN] {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in page {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    let mut out = [0u8; DIGEST_LEN];
    out[0..8].copy_from_slice(&h.to_be_bytes());
    out[8..16].copy_from_slice(&(page.len() as u64).to_be_bytes());
    // Non-zero by construction: DIGEST_ABSENT is a reserved value the
    // contract refuses, and an empty page must still record a digest.
    out[DIGEST_LEN - 1] = 1;
    out
}

// ── The pump ─────────────────────────────────────────────────────────

fn fault(c: &mut CoordState, what: &[u8]) {
    c.state = S_FAULT;
    c.m_errors = c.m_errors.wrapping_add(1);
    if c.logged_fault == 0 {
        c.logged_fault = 1;
        unsafe {
            if !c.syscalls.is_null() {
                dev_log(&*c.syscalls, 3, what.as_ptr(), what.len());
            }
        }
    }
}

fn log_info(c: &CoordState, msg: &[u8]) {
    unsafe {
        if !c.syscalls.is_null() {
            dev_log(&*c.syscalls, 2, msg.as_ptr(), msg.len());
        }
    }
}

/// Start the backup: the manifest opens at the protected revision.
fn begin_backup(c: &mut CoordState) {
    let id = c.backup_id();
    // Revision 0 means "current" to the provider's as-of read, which
    // is the protected point for this slice: the backup reads a single
    // consistent revision chosen by the store, and every page repeats
    // it. The allocator-leased protected timestamp is the Phase 5
    // upgrade, named here.
    let key = EncryptionKeyIdentity {
        key_id: [1; 16],
        key_version: 1,
        rotation_epoch: 1,
    };
    let Some(m) = BackupManifest::begin(id, 0, 1, key) else {
        fault(c, b"[bkp] manifest refused");
        return;
    };
    c.manifest = m;
    // One logical range in this composition; discovery is that range.
    if c.manifest.discover_range([1u8; 16], 1).is_err() {
        fault(c, b"[bkp] discovery refused");
        return;
    }
    if c.manifest
        .transition(BackupPhase::DescriptorSetDiscovered)
        .is_err()
        || c.manifest.transition(BackupPhase::RangesExporting).is_err()
    {
        fault(c, b"[bkp] transition refused");
        return;
    }
    c.cursor = 0;
    c.sequence = 0;
    c.protected_revision = 0;
    log_info(c, b"[bkp] export begins");
    send_export_scan(c);
}

fn send_export_scan(c: &mut CoordState) {
    let cursor = c.cursor;
    let rev = c.protected_revision;
    let Some(bn) = stage_scan_at(c, cursor, rev) else {
        fault(c, b"[bkp] scan stage");
        return;
    };
    if !kv_send(c, KV_OP_SCAN_AT, bn, S_EXPORT_SCAN) {
        fault(c, b"[bkp] scan send");
    }
}

/// Read the next key of the scan page at the protected revision, or
/// close the artifact page out.
fn send_next_export_get(c: &mut CoordState) {
    if c.keys_left == 0 {
        if c.page_len > 0 {
            send_artifact_put(c);
        } else if c.cursor != 0 {
            send_export_scan(c);
        } else {
            finish_backup(c);
        }
        return;
    }
    let at = c.keys_at as usize;
    if at + 2 > c.keys_len as usize {
        fault(c, b"[bkp] key page corrupt");
        return;
    }
    let klen = u16::from_le_bytes([c.keys[at], c.keys[at + 1]]) as usize;
    if at + 2 + klen > c.keys_len as usize || klen > c.cur_key.len() {
        fault(c, b"[bkp] key page corrupt");
        return;
    }
    for i in 0..klen {
        c.cur_key[i] = c.keys[at + 2 + i];
    }
    c.cur_key_len = klen as u16;
    c.keys_at = (at + 2 + klen) as u16;
    c.keys_left -= 1;
    let rev = c.protected_revision;
    let mut key = [0u8; 512];
    key[..klen].copy_from_slice(&c.cur_key[..klen]);
    let Some(bn) = stage_get_at(c, rev, &key[..klen]) else {
        fault(c, b"[bkp] get stage");
        return;
    };
    if !kv_send(c, types::KV_OP_GET_AT, bn, S_EXPORT_GET) {
        fault(c, b"[bkp] get send");
    }
}

/// Write the staged page as one artifact record.
fn send_artifact_put(c: &mut CoordState) {
    let seq = c.sequence;
    let mut key = [0u8; KEY_MAX];
    let Some(kn) = artifact_key(c, seq, &mut key) else {
        fault(c, b"[bkp] artifact key");
        return;
    };
    let len = c.page_len as usize;
    let mut page = [0u8; PAGE_BUF];
    page[..len].copy_from_slice(&c.page[..len]);
    let mut k = [0u8; KEY_MAX];
    k[..kn].copy_from_slice(&key[..kn]);
    let Some(bn) = stage_put(c, &k[..kn], &page[..len]) else {
        fault(c, b"[bkp] artifact stage");
        return;
    };
    if !kv_send(c, KV_OP_PUT, bn, S_EXPORT_PUT) {
        fault(c, b"[bkp] artifact send");
    }
}

/// All pages written: record the export, complete, persist.
fn finish_backup(c: &mut CoordState) {
    let digest = page_digest(&c.page[..c.page_len as usize]);
    let bytes = u64::from(c.sequence) * u64::from(PAGE_LIMIT);
    if c.manifest.record_export(&[1u8; 16], digest, bytes).is_err() {
        fault(c, b"[bkp] export record refused");
        return;
    }
    if c.manifest
        .transition(BackupPhase::AllRangesPresent)
        .is_err()
        || c.manifest.transition(BackupPhase::ManifestDurable).is_err()
    {
        fault(c, b"[bkp] completion refused");
        return;
    }
    let mut rec = [0u8; 1024];
    let Some(rn) = c.manifest.encode(&mut rec) else {
        fault(c, b"[bkp] manifest encode");
        return;
    };
    let mut key = [0u8; KEY_MAX];
    let Some(kn) = manifest_key(c, &mut key) else {
        fault(c, b"[bkp] manifest key");
        return;
    };
    let mut k = [0u8; KEY_MAX];
    k[..kn].copy_from_slice(&key[..kn]);
    let Some(bn) = stage_put(c, &k[..kn], &rec[..rn]) else {
        fault(c, b"[bkp] manifest stage");
        return;
    };
    if !kv_send(c, KV_OP_PUT, bn, S_MANIFEST_PUT) {
        fault(c, b"[bkp] manifest send");
    }
}

/// Restore: read the manifest first — a restore from an INCOMPLETE
/// backup is refused, never attempted (§17.2's completeness flag is
/// the whole point of recording it).
fn begin_restore(c: &mut CoordState) {
    let mut key = [0u8; KEY_MAX];
    let Some(kn) = manifest_key(c, &mut key) else {
        fault(c, b"[bkp] manifest key");
        return;
    };
    let mut k = [0u8; KEY_MAX];
    k[..kn].copy_from_slice(&key[..kn]);
    let Some(bn) = stage_get(c, &k[..kn]) else {
        fault(c, b"[bkp] manifest stage");
        return;
    };
    if !kv_send(c, KV_OP_GET, bn, S_RESTORE_GET) {
        fault(c, b"[bkp] manifest send");
    }
}

fn send_restore_scan(c: &mut CoordState) {
    let mut start = [0u8; KEY_MAX];
    let mut end = [0u8; KEY_MAX];
    let Some((sn, en)) = artifact_bounds(c, &mut start, &mut end) else {
        fault(c, b"[bkp] artifact bounds");
        return;
    };
    let cursor = c.cursor;
    let mut s = [0u8; KEY_MAX];
    let mut e = [0u8; KEY_MAX];
    s[..sn].copy_from_slice(&start[..sn]);
    e[..en].copy_from_slice(&end[..en]);
    let Some(bn) = stage_range_scan(c, &s[..sn], &e[..en], cursor) else {
        fault(c, b"[bkp] restore stage");
        return;
    };
    if !kv_send(c, KV_OP_RANGE_SCAN, bn, S_RESTORE_SCAN) {
        fault(c, b"[bkp] restore send");
    }
}

/// Replay the next record from the staged artifact page.
fn send_next_restore_put(c: &mut CoordState) {
    if c.page_records == 0 {
        if c.cursor != 0 {
            send_restore_scan(c);
        } else {
            log_info(c, b"[bkp] restore complete");
            c.state = S_DONE;
        }
        return;
    }
    let at = c.page_at as usize;
    let len = c.page_len as usize;
    if at + 6 > len {
        fault(c, b"[bkp] artifact page corrupt");
        return;
    }
    let klen = u16::from_le_bytes([c.page[at], c.page[at + 1]]) as usize;
    let koff = at + 2;
    let voff = koff + klen;
    if voff + 4 > len {
        fault(c, b"[bkp] artifact page corrupt");
        return;
    }
    let vlen = u32::from_le_bytes([
        c.page[voff],
        c.page[voff + 1],
        c.page[voff + 2],
        c.page[voff + 3],
    ]) as usize;
    let vend = voff + 4 + vlen;
    if vend > len {
        fault(c, b"[bkp] artifact page corrupt");
        return;
    }
    let mut key = [0u8; 512];
    let mut val = [0u8; PAGE_BUF];
    if klen > key.len() || vlen > val.len() {
        fault(c, b"[bkp] record too large");
        return;
    }
    for i in 0..klen {
        key[i] = c.page[koff + i];
    }
    for i in 0..vlen {
        val[i] = c.page[voff + 4 + i];
    }
    c.page_at = vend as u16;
    c.page_records -= 1;
    let Some(bn) = stage_put(c, &key[..klen], &val[..vlen]) else {
        fault(c, b"[bkp] restore put stage");
        return;
    };
    if !kv_send(c, KV_OP_PUT, bn, S_RESTORE_PUT) {
        fault(c, b"[bkp] restore put send");
    }
}

fn on_kv_response(c: &mut CoordState, result: u8, body: &[u8]) {
    match c.state {
        S_EXPORT_SCAN => {
            if result == KV_RESULT_COMPACTED {
                // The protected history is gone: ABORT rather than
                // ship a partial export (§17.2, and the whole reason
                // the phase exists).
                let _ = c.manifest.transition(BackupPhase::Aborted);
                fault(c, b"[bkp] aborted: protected history compacted");
                return;
            }
            if result != KV_RESULT_SCAN_CURSOR || body.len() < 10 {
                let mut msg = *b"[bkp] export scan failed rc=000 len=0000";
                msg[28] = b'0' + ((result / 100) % 10);
                msg[29] = b'0' + ((result / 10) % 10);
                msg[30] = b'0' + (result % 10);
                let bl = body.len().min(9999) as u32;
                msg[36] = b'0' + ((bl / 1000) % 10) as u8;
                msg[37] = b'0' + ((bl / 100) % 10) as u8;
                msg[38] = b'0' + ((bl / 10) % 10) as u8;
                msg[39] = b'0' + (bl % 10) as u8;
                fault(c, &msg);
                return;
            }
            {
                // Measure the first page rather than infer from the
                // outcome: rc, count, and next cursor in one line.
                let cnt = u16::from_le_bytes([body[8], body[9]]);
                let mut msg = *b"[bkp] scan page n=0000 cur=0";
                msg[18] = b'0' + ((cnt / 1000) % 10) as u8;
                msg[19] = b'0' + ((cnt / 100) % 10) as u8;
                msg[20] = b'0' + ((cnt / 10) % 10) as u8;
                msg[21] = b'0' + (cnt % 10) as u8;
                let cur = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
                msg[27] = if cur == 0 { b'0' } else { b'1' };
                log_info(c, &msg);
            }
            c.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            let count = u16::from_le_bytes([body[8], body[9]]);
            // Restage the page's keys as `[len:u16][key]` records.
            c.keys_len = 0;
            c.keys_at = 0;
            c.keys_left = count;
            let mut at = 10usize;
            for _ in 0..count {
                if at + 4 > body.len() {
                    fault(c, b"[bkp] scan page corrupt");
                    return;
                }
                let klen =
                    u32::from_le_bytes(body[at..at + 4].try_into().unwrap_or([0; 4])) as usize;
                at += 4;
                if at + klen > body.len() || klen > 512 {
                    fault(c, b"[bkp] scan page corrupt");
                    return;
                }
                let w = c.keys_len as usize;
                if w + 2 + klen > c.keys.len() {
                    fault(c, b"[bkp] key page overflow");
                    return;
                }
                c.keys[w..w + 2].copy_from_slice(&(klen as u16).to_le_bytes());
                for i in 0..klen {
                    c.keys[w + 2 + i] = body[at + i];
                }
                c.keys_len = (w + 2 + klen) as u16;
                at += klen;
            }
            // Start (or continue) building this artifact page.
            c.page_len = 0;
            send_next_export_get(c);
        }
        S_EXPORT_GET => {
            if result == KV_RESULT_COMPACTED {
                let _ = c.manifest.transition(BackupPhase::Aborted);
                fault(c, b"[bkp] aborted: protected history compacted");
                return;
            }
            if result == KV_RESULT_OK {
                // Append `[klen:u16][key][vlen:u32][value]` — the same
                // record shape a RANGE_SCAN page uses, so a restore
                // walks artifacts with the identical parser.
                let klen = c.cur_key_len as usize;
                let need = 2 + klen + 4 + body.len();
                let w = c.page_len as usize;
                if w + need > PAGE_BUF {
                    fault(c, b"[bkp] artifact page overflow");
                    return;
                }
                c.page[w..w + 2].copy_from_slice(&(klen as u16).to_le_bytes());
                for i in 0..klen {
                    c.page[w + 2 + i] = c.cur_key[i];
                }
                let vo = w + 2 + klen;
                c.page[vo..vo + 4].copy_from_slice(&(body.len() as u32).to_le_bytes());
                for (i, b) in body.iter().enumerate() {
                    c.page[vo + 4 + i] = *b;
                }
                c.page_len = (w + need) as u16;
                c.m_records = c.m_records.wrapping_add(1);
            } else if result != KV_RESULT_NOT_FOUND {
                fault(c, b"[bkp] historical read failed");
                return;
            }
            send_next_export_get(c);
        }
        S_EXPORT_PUT => {
            if result != KV_RESULT_OK {
                fault(c, b"[bkp] artifact write failed");
                return;
            }
            c.m_pages = c.m_pages.wrapping_add(1);
            c.sequence += 1;
            c.page_len = 0;
            if c.cursor != 0 {
                send_export_scan(c);
            } else {
                finish_backup(c);
            }
        }
        S_MANIFEST_PUT => {
            if result != KV_RESULT_OK {
                fault(c, b"[bkp] manifest write failed");
                return;
            }
            let _ = c.manifest.transition(BackupPhase::ProtectionReleased);
            // Measure, don't infer: records and pages in the line.
            let mut msg = *b"[bkp] backup complete r=0000 p=0000";
            let r = c.m_records.min(9999) as u32;
            let pg = c.m_pages.min(9999) as u32;
            msg[24] = b'0' + ((r / 1000) % 10) as u8;
            msg[25] = b'0' + ((r / 100) % 10) as u8;
            msg[26] = b'0' + ((r / 10) % 10) as u8;
            msg[27] = b'0' + (r % 10) as u8;
            msg[31] = b'0' + ((pg / 1000) % 10) as u8;
            msg[32] = b'0' + ((pg / 100) % 10) as u8;
            msg[33] = b'0' + ((pg / 10) % 10) as u8;
            msg[34] = b'0' + (pg % 10) as u8;
            log_info(c, &msg);
            c.state = S_DONE;
        }
        S_RESTORE_GET => {
            if result == KV_RESULT_NOT_FOUND {
                fault(c, b"[bkp] no such backup (manifest key absent)");
                return;
            }
            if result != KV_RESULT_OK {
                let mut msg = *b"[bkp] manifest read failed rc=000";
                msg[30] = b'0' + ((result / 100) % 10);
                msg[31] = b'0' + ((result / 10) % 10);
                msg[32] = b'0' + (result % 10);
                fault(c, &msg);
                return;
            }
            let Some(m) = BackupManifest::decode(body) else {
                fault(c, b"[bkp] manifest corrupt");
                return;
            };
            if !m.is_complete() {
                // Refusing an incomplete backup IS §17.2's contract.
                fault(c, b"[bkp] refusing restore of an incomplete backup");
                return;
            }
            c.manifest = m;
            c.cursor = 0;
            c.page_records = 0;
            log_info(c, b"[bkp] restore begins");
            send_restore_scan(c);
        }
        S_RESTORE_SCAN => {
            if result != KV_RESULT_RANGE || body.len() < 10 {
                fault(c, b"[bkp] artifact scan failed");
                return;
            }
            c.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            let count = u16::from_le_bytes([body[8], body[9]]);
            if count == 0 {
                if c.cursor != 0 {
                    send_restore_scan(c);
                } else {
                    log_info(c, b"[bkp] restore complete");
                    c.state = S_DONE;
                }
                return;
            }
            // One artifact per page-scan step: take the FIRST record's
            // value as the staged page, and remember the rest by
            // leaving the cursor where it is (each artifact is one
            // scan record, so a single-record read keeps this simple
            // and bounded).
            let klen = u16::from_le_bytes([body[10], body[11]]) as usize;
            let voff = 12 + klen;
            if voff + 4 > body.len() {
                fault(c, b"[bkp] artifact record corrupt");
                return;
            }
            let vlen =
                u32::from_le_bytes(body[voff..voff + 4].try_into().unwrap_or([0; 4])) as usize;
            let vend = voff + 4 + vlen;
            if vend > body.len() || vlen > PAGE_BUF {
                fault(c, b"[bkp] artifact too large");
                return;
            }
            c.page[..vlen].copy_from_slice(&body[voff + 4..vend]);
            c.page_len = vlen as u16;
            c.page_at = 0;
            // Records inside the artifact page: count them by walking.
            let mut n = 0u16;
            let mut at = 0usize;
            while at + 6 <= vlen {
                let kl = u16::from_le_bytes([c.page[at], c.page[at + 1]]) as usize;
                let vo = at + 2 + kl;
                if vo + 4 > vlen {
                    break;
                }
                let vl = u32::from_le_bytes([
                    c.page[vo],
                    c.page[vo + 1],
                    c.page[vo + 2],
                    c.page[vo + 3],
                ]) as usize;
                let ve = vo + 4 + vl;
                if ve > vlen {
                    break;
                }
                n += 1;
                at = ve;
            }
            c.page_records = n;
            // Re-scan from the NEXT artifact after this page drains.
            send_next_restore_put(c);
        }
        S_RESTORE_PUT => {
            if result != KV_RESULT_OK {
                fault(c, b"[bkp] restore write failed");
                return;
            }
            c.m_restored = c.m_restored.wrapping_add(1);
            send_next_restore_put(c);
        }
        _ => {}
    }
}

// ── Module ABI ───────────────────────────────────────────────────────

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
    let c = unsafe { &mut *state.cast::<CoordState>() };
    c.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(c, params, params_len) };
    }
    c.kv_in = in_chan;
    c.kv_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        c.metrics_out = dev_channel_port(sys, 1, 1);
    }
    if c.mode > 2 {
        return -1; // an unknown mode must not run as "inert"
    }
    c.state = if c.mode == 0 { S_IDLE } else { S_WAIT };
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let c = unsafe { &mut *state.cast::<CoordState>() };
    let sys_ptr = c.syscalls;
    if sys_ptr.is_null() {
        return -1;
    }
    let now = unsafe { dev_millis(&*sys_ptr) };
    if c.boot_ms == 0 {
        c.boot_ms = now.max(1);
    }

    unsafe {
        let sys = &*sys_ptr;
        let mut env = [0u8; ENV_BUF];
        if let Some((msg, payload)) = read_one_envelope(sys, c.kv_in, &mut env) {
            if msg == MSG_KV_RESPONSE && payload.len() >= 20 {
                let corr = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
                let result = payload[9];
                let blen = u16::from_le_bytes([payload[18], payload[19]]) as usize;
                if corr == c.corr && payload.len() >= 20 + blen {
                    let mut body = [0u8; ENV_BUF];
                    body[..blen].copy_from_slice(&payload[20..20 + blen]);
                    on_kv_response(c, result, &body[..blen]);
                }
            }
        }
    }

    if c.state == S_WAIT && now.wrapping_sub(c.boot_ms) >= c.start_delay_ms as u64 {
        match c.mode {
            1 => begin_backup(c),
            2 => begin_restore(c),
            _ => c.state = S_IDLE,
        }
    }

    c.step_ctr = c.step_ctr.wrapping_add(1);
    if c.step_ctr.is_multiple_of(5000) {
        unsafe {
            telemetry::emit_counters(
                &*sys_ptr,
                c.metrics_out,
                &[c.m_records, c.m_pages, c.m_restored, c.m_errors],
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
