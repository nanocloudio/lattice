//! backup_coordinator — consistent database backup and EXACT restore.
//!
//! Drives the backup procedure's numbered machine over the contracts
//! in `db_ops.rs`, which own every transition rule; this module is
//! the pump that turns each phase into real KV work, exactly as
//! `range_supervisor` is for range lifecycle and `index_backfill` is
//! for secondary indexes.
//!
//! ## What makes the backup CONSISTENT
//!
//! The backup reads the store at ONE revision R: `KV_OP_HASH_KV` names
//! the store's revision and digests its live keys there, and every page
//! is a `KV_OP_RANGE_META` at R — every live key with its value, create
//! and mod revision, version and lease. A backup assembled from reads at
//! different revisions would be a set of records that never existed
//! together. R is protected for the whole export: before it reads R the
//! coordinator takes a retention hold (`KV_OP_RETAIN`, named `bkp/<backup
//! id>`) at the store's current revision, so no Compact — a client's, or
//! the store's own history compaction — passes it while the hold lives;
//! the hold lives `HOLD_TTL_MS` (60 s) on the store's clock and is renewed
//! between pages, then released once the manifest is durable (a
//! coordinator that dies stops holding within the TTL). If the store had
//! already reclaimed the history (the hold is refused, or a page is) it
//! answers `COMPACTED` and the backup ABORTS: a partial export is
//! indistinguishable from a whole one once it is on disk.
//!
//! The manifest carries the snapshot's metadata: R
//! (`protected_timestamp`), the consensus index the state was applied
//! through when a log orders the store (`raft_index`, from the answer's
//! fence tail; 0 when none), the epoch it is fenced under (`fence`, the
//! `epoch` param) and the digest and key count R had (`state_hash`,
//! `state_keys`).
//!
//! ## Where artifacts live
//!
//! In the KV store itself, under `KS_BACKUP_ARTIFACT`, keyed by
//! `[backup_id][sequence]`: the artifacts inherit the same durability,
//! replication, and crash semantics as everything else. A page is a run
//! of `KV_OP_RESTORE` records (`types.rs`) — key, value, and the
//! metadata the key had at R. Backups held in an external object store
//! are not provided.
//!
//! ## Restore: exact, into a fresh store
//!
//! Reads the manifest (from the store the artifacts are in, the `kv`
//! pair), verifies it is complete, then sends every artifact page as one
//! `KV_OP_RESTORE` to the TARGET store (the `target` pair; unwired, the
//! `kv` pair's own store, which having been written refuses it). Every
//! key lands with the revision, version and lease it had, and the
//! target's revision passes R, so a revision a client held before the
//! backup — a resource version, the mod revision it compares — means the
//! same after the restore or a migration. The target must be fresh
//! (never written) or fenced by this same restore (a resumed one): the
//! first page writes the manifest's fence into it, and a store fenced
//! under another epoch, or written, refuses the restore whole. Last, the
//! target's digest (`KV_OP_HASH_KV`) must be the manifest's, its revision
//! at least R and its compaction floor at least R (history below R is not
//! there): a restore that reads back anything else faults. A completed
//! restore logs what it measured: `[bkp] restore complete r=<records>
//! h=<held> rev=<R> at=<target revision> floor=<target floor> verified`,
//! `held` counting the records the target already had exactly (a resumed
//! or repeated restore's pages).
//!
//! Not carried: a large value (a manifest record faults the backup), a
//! key's TTL deadline, and history before R (the target answers windows
//! from R on).

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

#[path = "../../common/envelope.rs"]
mod envelope;
use envelope::{read_envelope, Inbox, ENVELOPE_HDR};

use db_ops::{
    BackupManifest, BackupPhase, EncryptionKeyIdentity, BACKUP_ARTIFACT_FORMAT_VERSION, DIGEST_LEN,
};
use types::{
    KV_OP_GET, KV_OP_HASH_KV, KV_OP_PUT, KV_OP_RANGE_META, KV_OP_RESTORE, KV_OP_RETAIN,
    KV_RESULT_COMPACTED, KV_RESULT_EXISTS, KV_RESULT_KVS, KV_RESULT_MANIFEST, KV_RESULT_NOT_FOUND,
    KV_RESULT_OK, PROTO_ETCD, RESTORE_HEAD_LEN, RESTORE_META_LEN, RETAIN_ANSWER_LEN,
    RETAIN_AT_CURRENT,
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
/// Records per exported read.
const PAGE_LIMIT: u16 = 24;
/// A backed-up key, and its successor (the next read's start).
const USER_KEY: usize = 512;

// ── Driver states ────────────────────────────────────────────────────

const S_IDLE: u8 = 0;
const S_WAIT: u8 = 1; // waiting out the start delay
const S_EXPORT_HASH: u8 = 2; // the snapshot's revision and digest
const S_EXPORT_RANGE: u8 = 3; // a RANGE_META page at R in flight
const S_EXPORT_PUT: u8 = 4; // an artifact page write in flight
const S_MANIFEST_PUT: u8 = 5;
const S_RESTORE_GET: u8 = 6; // manifest read in flight
const S_RESTORE_ARTIFACT: u8 = 7; // an artifact page read in flight
const S_RESTORE_PAGE: u8 = 8; // a KV_OP_RESTORE page in flight
const S_RESTORE_VERIFY: u8 = 9; // the target's digest
const S_DONE: u8 = 10;
const S_FAULT: u8 = 11;
const S_HOLD_TAKE: u8 = 12; // the retention hold, taken at the current revision
const S_HOLD_RENEW: u8 = 13; // the hold renewed between pages
const S_HOLD_RELEASE: u8 = 14; // the hold released, the backup durable

/// How long the backup's retention hold lives past the store's clock
/// unless renewed: a coordinator that dies mid-export stops holding the
/// store's history within it.
const HOLD_TTL_MS: u64 = 60_000;
/// The hold is renewed between pages once this old (a third of its
/// life: two renewals may be lost before it lapses).
const HOLD_RENEW_MS: u64 = HOLD_TTL_MS / 3;

define_params! {
    CoordState;

    // 0 = inert, 1 = take a backup, 2 = restore the named backup.
    1, mode, u8, 0
        => |s, d, len| { s.mode = p_u8(d, len, 0, 0); };

    // Backup identity: the 16-byte id is these four bytes (big-endian),
    // eleven zero bytes, then 1 — so no id is all zero, which the
    // contract refuses. An operator names the backup; the module never
    // mints one, so a restore always addresses a backup someone chose.
    2, backup_id, u32, 1
        => |s, d, len| { s.backup_id_low = p_u32(d, len, 0, 1); };

    // Manual execution gate (the range-lifecycle rule, applied to
    // backups): the
    // operation is declared in the config and starts this long after
    // boot.
    3, start_delay_ms, u32, 3000
        => |s, d, len| { s.start_delay_ms = p_u32(d, len, 0, 3000); };

    // The epoch a backup is fenced under (its manifest's `fence`): the
    // restore writes it into the target store's fence, and a store fenced
    // under another epoch refuses the restore. A management plane names
    // the epoch its own writes compare, and moves past it after a
    // restore. Non-zero.
    4, epoch, u32, 1
        => |s, d, len| { s.epoch = p_u32(d, len, 0, 1); };
}

#[repr(C)]
struct CoordState {
    syscalls: *const SyscallTable,
    kv_in: i32,
    /// The kv input's envelope reading (`common/envelope.rs`).
    kv_inbox: Inbox,
    kv_out: i32,
    /// The restore's target store (unwired: the kv pair's).
    target_in: i32,
    target_inbox: Inbox,
    target_out: i32,
    metrics_out: i32,

    mode: u8,
    backup_id_low: u32,
    start_delay_ms: u32,
    epoch: u32,

    state: u8,
    boot_ms: u64,
    corr: u64,

    manifest: BackupManifest,
    /// The revision every export page reads at (R).
    protected_revision: u64,
    /// The retention hold's floor (at most R) and when it was last taken
    /// or renewed.
    hold_floor: u64,
    hold_ms: u64,
    /// Artifact pages written (backup) or read (restore).
    sequence: u32,
    /// Every page's bytes, chained (the manifest entry's digest), and
    /// their count.
    page_hash: u64,
    page_bytes: u64,
    /// Staged artifact page (built, or read back).
    page: [u8; PAGE_BUF],
    page_len: u16,
    page_records: u16,
    /// Export: the RANGE_META answer being consumed — its body, the next
    /// entry's offset, the entries left, and whether more lie past it.
    ans: [u8; ENV_BUF],
    ans_len: u16,
    ans_at: u16,
    ans_left: u16,
    ans_more: u8,
    /// The last key exported; the next read starts at its successor.
    last_key: [u8; USER_KEY + 1],
    last_key_len: u16,
    /// Restore: pages the target took.
    restored_pages: u32,
    /// Restore: records the target already held exactly (a resumed or
    /// repeated restore's pages).
    m_held: u64,

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
        self.kv_inbox = Inbox::new();
        self.kv_out = -1;
        self.target_in = -1;
        self.target_inbox = Inbox::new();
        self.target_out = -1;
        self.metrics_out = -1;
        self.mode = 0;
        self.backup_id_low = 1;
        self.start_delay_ms = 3000;
        self.epoch = 1;
        self.state = S_IDLE;
        self.boot_ms = 0;
        self.corr = 0;
        self.manifest = BackupManifest::EMPTY;
        self.protected_revision = 0;
        self.hold_floor = 0;
        self.hold_ms = 0;
        self.sequence = 0;
        self.page_hash = FNV_OFFSET;
        self.page_bytes = 0;
        self.page = [0; PAGE_BUF];
        self.page_len = 0;
        self.page_records = 0;
        self.ans = [0; ENV_BUF];
        self.ans_len = 0;
        self.ans_at = 0;
        self.ans_left = 0;
        self.ans_more = 0;
        self.last_key = [0; USER_KEY + 1];
        self.last_key_len = 0;
        self.restored_pages = 0;
        self.m_held = 0;
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

const REQ_HEAD: usize = 18;
const BODY_AT: usize = wire::ENVELOPE_HDR + REQ_HEAD;

/// The pair a request goes on: the store the artifacts are in, or the
/// restore's target.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Pair {
    Source,
    Target,
}

/// Send the staged body (`body_len` bytes at `BODY_AT`) as `op` on
/// `pair`, and move to `next`.
fn kv_send(c: &mut CoordState, pair: Pair, op: u8, body_len: usize, next: u8) -> bool {
    c.corr = c.corr.wrapping_add(1).max(1);
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
    let chan = match pair {
        Pair::Target if c.target_out >= 0 => c.target_out,
        _ => c.kv_out,
    };
    let sent = unsafe {
        let sys = c.syscalls;
        !sys.is_null()
            && write_envelope(&*sys, chan, MSG_KV_REQUEST, REQ_HEAD + body_len, &mut c.env)
    };
    if sent {
        c.state = next;
    }
    sent
}

/// Stage `parts` back to back as the request body: its length.
fn stage(c: &mut CoordState, parts: &[&[u8]]) -> Option<usize> {
    let mut n = 0usize;
    for p in parts {
        copy(c.env.get_mut(BODY_AT + n..BODY_AT + n + p.len())?, p);
        n += p.len();
    }
    Some(n)
}

fn send(c: &mut CoordState, pair: Pair, op: u8, parts: &[&[u8]], next: u8, what: &[u8]) {
    match stage(c, parts) {
        Some(n) if kv_send(c, pair, op, n, next) => {}
        _ => fault(c, what),
    }
}

/// `src` into the head of `dst`, byte by byte (a length the build cannot
/// prove equal would pull in a panic path a module cannot link).
fn copy(dst: &mut [u8], src: &[u8]) {
    for (d, s) in dst.iter_mut().zip(src) {
        *d = *s;
    }
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

/// The retention hold's name: `bkp/` and the backup id.
fn hold_name(c: &CoordState) -> [u8; 20] {
    let mut n = [0u8; 20];
    n[0..4].copy_from_slice(b"bkp/");
    n[4..20].copy_from_slice(&c.backup_id());
    n
}

/// `KV_OP_RETAIN` of this backup's hold at `floor` (0 releases it).
fn send_hold(c: &mut CoordState, floor: u64, next: u8) {
    let name = hold_name(c);
    let ttl = if floor == 0 { 0 } else { HOLD_TTL_MS };
    send(
        c,
        Pair::Source,
        KV_OP_RETAIN,
        &[
            &floor.to_le_bytes(),
            &ttl.to_le_bytes(),
            &(name.len() as u16).to_le_bytes(),
            &name,
        ],
        next,
        b"[bkp] hold send",
    );
}

fn now_ms(c: &CoordState) -> u64 {
    if c.syscalls.is_null() {
        return 0;
    }
    // SAFETY: the module's syscall table.
    unsafe { dev_millis(&*c.syscalls) }
}

fn artifact_key(c: &CoordState, sequence: u32, out: &mut [u8]) -> Option<usize> {
    let mut body = [0u8; 20];
    body[0..16].copy_from_slice(&c.backup_id());
    body[16..20].copy_from_slice(&sequence.to_be_bytes());
    user_key(out, KS_BACKUP_ARTIFACT, &body)
}

// ── Digests ──────────────────────────────────────────────────────────

const FNV_OFFSET: u64 = 0xcbf29ce484222325;

/// FNV-1a 64 continued over `bytes`.
fn fnv(mut h: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Every page's chained FNV-1a 64 and their byte count, widened into the
/// contract's 32-byte digest slot. Not a cryptographic digest: it detects
/// corruption, not tampering (authenticated digests arrive with the
/// external-object-store slice).
fn pages_digest(h: u64, bytes: u64) -> [u8; DIGEST_LEN] {
    let mut out = [0u8; DIGEST_LEN];
    out[0..8].copy_from_slice(&h.to_be_bytes());
    out[8..16].copy_from_slice(&bytes.to_be_bytes());
    // Non-zero by construction: DIGEST_ABSENT is a reserved value the
    // contract refuses, and an empty backup must still record a digest.
    out[DIGEST_LEN - 1] = 1;
    out
}

/// The one logical range this composition backs up.
const RANGE_ID: [u8; 16] = [1u8; 16];

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

/// `v` as `width` decimal digits into `out` at `at`.
fn digits(out: &mut [u8], at: usize, width: usize, mut v: u64) {
    for i in (0..width).rev() {
        if let Some(b) = out.get_mut(at + i) {
            *b = b'0' + (v % 10) as u8;
        }
        v /= 10;
    }
}

/// Backup step 1: the snapshot's revision and digest, in one read.
fn begin_backup(c: &mut CoordState) {
    c.sequence = 0;
    c.page_hash = FNV_OFFSET;
    c.page_bytes = 0;
    c.page_len = 0;
    c.last_key_len = 0;
    log_info(c, b"[bkp] export begins");
    // Hold the store's history first, at its current revision, THEN read
    // the revision the export reads at (at least the hold's): nothing the
    // export reads can be compacted under it — by a client's Compact or
    // the store's own history compaction — while the hold lives, and it
    // is renewed for as long as the export runs.
    send_hold(c, RETAIN_AT_CURRENT, S_HOLD_TAKE);
}

/// The hold is taken: the snapshot's revision and digest.
fn send_snapshot_hash(c: &mut CoordState) {
    send(
        c,
        Pair::Source,
        KV_OP_HASH_KV,
        &[&0u64.to_le_bytes()],
        S_EXPORT_HASH,
        b"[bkp] hash send",
    );
}

/// The manifest opens at R, with the snapshot's metadata.
fn open_manifest(c: &mut CoordState, rev: u64, hash: u32, keys: u64, raft_index: u64) {
    let key = EncryptionKeyIdentity {
        key_id: [1; 16],
        key_version: 1,
        rotation_epoch: 1,
    };
    let Some(m) = BackupManifest::begin(c.backup_id(), 0, rev, key) else {
        fault(c, b"[bkp] manifest refused");
        return;
    };
    c.manifest = m;
    c.manifest.metadata_revision = rev;
    c.manifest.raft_index = raft_index;
    c.manifest.fence = u64::from(c.epoch);
    c.manifest.state_hash = hash;
    c.manifest.state_keys = keys;
    // One logical range in this composition; discovery is that range.
    if c.manifest.discover_range(RANGE_ID, 1).is_err()
        || c.manifest
            .transition(BackupPhase::DescriptorSetDiscovered)
            .is_err()
        || c.manifest.transition(BackupPhase::RangesExporting).is_err()
    {
        fault(c, b"[bkp] transition refused");
        return;
    }
    c.protected_revision = rev;
    send_export_range(c);
}

/// The next page of live keys at R: from the start, or from the key past
/// the last exported (`\0` end: every key on).
fn send_export_range(c: &mut CoordState) {
    if now_ms(c).saturating_sub(c.hold_ms) >= HOLD_RENEW_MS {
        // Renewed first; the page is read once it is.
        let floor = c.hold_floor;
        send_hold(c, floor, S_HOLD_RENEW);
        return;
    }
    let mut start = [0u8; USER_KEY + 1];
    let mut sl = c.last_key_len as usize;
    copy(&mut start[..sl], &c.last_key[..sl]);
    if sl > 0 {
        // The successor: the key itself, then the least byte.
        start[sl] = 0;
        sl += 1;
    }
    let rev = c.protected_revision;
    send(
        c,
        Pair::Source,
        KV_OP_RANGE_META,
        &[
            &(sl as u16).to_le_bytes(),
            &start[..sl],
            &1u16.to_le_bytes(),
            &[0],
            &rev.to_le_bytes(),
            &u32::from(PAGE_LIMIT).to_le_bytes(),
            // flags (none: a manifest is refused, not exported), sort
            // target and order (key ascending), and no revision bounds.
            &[0, 0, 0],
            &[0u8; 32],
        ],
        S_EXPORT_RANGE,
        b"[bkp] range send",
    );
}

/// Take the held answer's entries into the page; write the page when the
/// next does not fit; read on when the answer is spent; finish when the
/// store is.
fn drain_export(c: &mut CoordState) {
    while c.ans_left > 0 {
        let at = c.ans_at as usize;
        let len = c.ans_len as usize;
        let a = &c.ans[..len];
        let Some(kl) = a
            .get(at..at + 2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)
        else {
            fault(c, b"[bkp] range page corrupt");
            return;
        };
        let voff = at + 2 + kl;
        let Some(vl) = a
            .get(voff..voff + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
        else {
            fault(c, b"[bkp] range page corrupt");
            return;
        };
        let moff = voff + 4 + vl;
        let end = moff + 32;
        if end > len || kl > USER_KEY {
            fault(c, b"[bkp] range page corrupt");
            return;
        }
        // The record: key, value, metadata, flags — `KV_OP_RESTORE`'s.
        let need = 2 + kl + 4 + vl + RESTORE_META_LEN;
        let w = c.page_len as usize;
        if w + need > PAGE_BUF {
            if w == 0 {
                fault(c, b"[bkp] record larger than a page");
            } else {
                send_artifact_put(c);
            }
            return;
        }
        copy(&mut c.page[w..w + need - 1], &c.ans[at..end]);
        c.page[w + need - 1] = 0;
        c.page_len = (w + need) as u16;
        c.page_records += 1;
        copy(&mut c.last_key[..kl], &c.ans[at + 2..at + 2 + kl]);
        c.last_key_len = kl as u16;
        c.ans_at = end as u16;
        c.ans_left -= 1;
        c.m_records = c.m_records.wrapping_add(1);
    }
    if c.ans_more != 0 {
        send_export_range(c);
    } else if c.page_len > 0 {
        send_artifact_put(c);
    } else {
        finish_backup(c);
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
    c.page_hash = fnv(c.page_hash, &page[..len]);
    c.page_bytes += len as u64;
    send(
        c,
        Pair::Source,
        KV_OP_PUT,
        &[
            &(kn as u16).to_le_bytes(),
            &key[..kn],
            &(len as u32).to_le_bytes(),
            &page[..len],
            &[0],
            &0u64.to_le_bytes(),
        ],
        S_EXPORT_PUT,
        b"[bkp] artifact send",
    );
}

/// All pages written: record the export, complete, persist.
fn finish_backup(c: &mut CoordState) {
    let digest = pages_digest(c.page_hash, c.page_bytes);
    if c.manifest
        .record_export(&RANGE_ID, digest, c.page_bytes)
        .is_err()
    {
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
    send(
        c,
        Pair::Source,
        KV_OP_PUT,
        &[
            &(kn as u16).to_le_bytes(),
            &key[..kn],
            &(rn as u32).to_le_bytes(),
            &rec[..rn],
            &[0],
            &0u64.to_le_bytes(),
        ],
        S_MANIFEST_PUT,
        b"[bkp] manifest send",
    );
}

/// Restore: read the manifest first — a restore from an INCOMPLETE
/// backup is refused, never attempted (the completeness flag is
/// the whole point of recording it).
fn begin_restore(c: &mut CoordState) {
    let mut key = [0u8; KEY_MAX];
    let Some(kn) = manifest_key(c, &mut key) else {
        fault(c, b"[bkp] manifest key");
        return;
    };
    send(
        c,
        Pair::Source,
        KV_OP_GET,
        &[&(kn as u16).to_le_bytes(), &key[..kn]],
        S_RESTORE_GET,
        b"[bkp] manifest send",
    );
}

/// The next artifact page, by sequence — until the manifest's bytes are
/// read; then (an empty backup: once, with no records) the target's
/// fence and revision, then its digest.
fn next_restore_step(c: &mut CoordState) {
    let total = c.manifest.entries().first().map_or(0, |e| e.byte_size);
    if c.page_bytes < total {
        let mut key = [0u8; KEY_MAX];
        let Some(kn) = artifact_key(c, c.sequence, &mut key) else {
            fault(c, b"[bkp] artifact key");
            return;
        };
        send(
            c,
            Pair::Source,
            KV_OP_GET,
            &[&(kn as u16).to_le_bytes(), &key[..kn]],
            S_RESTORE_ARTIFACT,
            b"[bkp] artifact read send",
        );
        return;
    }
    if c.page_bytes != total {
        fault(c, b"[bkp] artifact bytes disagree with the manifest");
        return;
    }
    let digest = c.manifest.entries().first().map(|e| e.digest);
    if digest != Some(pages_digest(c.page_hash, c.page_bytes)) {
        fault(c, b"[bkp] artifact digest disagrees with the manifest");
        return;
    }
    if c.restored_pages == 0 {
        // Nothing was backed up: the target is still fenced and carried
        // past R.
        c.page_len = 0;
        c.page_records = 0;
        send_restore_page(c);
        return;
    }
    send(
        c,
        Pair::Target,
        KV_OP_HASH_KV,
        &[&0u64.to_le_bytes()],
        S_RESTORE_VERIFY,
        b"[bkp] verify send",
    );
}

/// The staged page as one `KV_OP_RESTORE` to the target.
fn send_restore_page(c: &mut CoordState) {
    let len = c.page_len as usize;
    let mut page = [0u8; PAGE_BUF];
    page[..len].copy_from_slice(&c.page[..len]);
    let fence = c.manifest.fence;
    let rev = c.manifest.protected_timestamp;
    let n = c.page_records;
    send(
        c,
        Pair::Target,
        KV_OP_RESTORE,
        &[
            &fence.to_le_bytes(),
            &rev.to_le_bytes(),
            &n.to_le_bytes(),
            &page[..len],
        ],
        S_RESTORE_PAGE,
        b"[bkp] restore page send",
    );
}

/// The records in an artifact page (each a `KV_OP_RESTORE` record), or
/// `None` when the page does not walk whole.
fn count_records(page: &[u8]) -> Option<u16> {
    let mut at = 0usize;
    let mut n = 0u16;
    while at < page.len() {
        let kl = u16::from_le_bytes([*page.get(at)?, *page.get(at + 1)?]) as usize;
        let vo = at + 2 + kl;
        let vl = u32::from_le_bytes(page.get(vo..vo + 4)?.try_into().ok()?) as usize;
        at = vo + 4 + vl + RESTORE_META_LEN;
        if at > page.len() {
            return None;
        }
        n = n.checked_add(1)?;
    }
    Some(n)
}

/// A KV answer's result code as three digits at `at`.
fn with_rc(msg: &mut [u8], at: usize, rc: u8) {
    digits(msg, at, 3, u64::from(rc));
}

fn on_kv_response(c: &mut CoordState, result: u8, body: &[u8], applied_index: u64) {
    match c.state {
        S_HOLD_TAKE | S_HOLD_RENEW => {
            if result == KV_RESULT_COMPACTED {
                let _ = c.manifest.transition(BackupPhase::Aborted);
                fault(c, b"[bkp] aborted: protected history compacted");
                return;
            }
            if result != KV_RESULT_OK || body.len() < RETAIN_ANSWER_LEN {
                let mut msg = *b"[bkp] retention hold refused rc=000";
                with_rc(&mut msg, 32, result);
                fault(c, &msg);
                return;
            }
            c.hold_floor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
            c.hold_ms = now_ms(c);
            if c.state == S_HOLD_TAKE {
                send_snapshot_hash(c);
            } else {
                send_export_range(c);
            }
        }
        S_HOLD_RELEASE => {
            if result != KV_RESULT_OK {
                // The backup is durable; the hold lapses on its own.
                log_info(c, b"[bkp] hold release refused: it lapses");
            }
            c.hold_floor = 0;
            // Measure, don't infer: records, pages and the revision.
            let mut msg = *b"[bkp] backup complete r=0000 p=0000 rev=0000000000";
            digits(&mut msg, 24, 4, c.m_records.min(9999));
            digits(&mut msg, 31, 4, c.m_pages.min(9999));
            digits(&mut msg, 40, 10, c.protected_revision);
            log_info(c, &msg);
            c.state = S_DONE;
        }
        S_EXPORT_HASH => {
            if result != KV_RESULT_OK || body.len() < 36 {
                let mut msg = *b"[bkp] snapshot digest failed rc=000";
                with_rc(&mut msg, 32, result);
                fault(c, &msg);
                return;
            }
            let hash = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
            let rev = u64::from_le_bytes(body[12..20].try_into().unwrap_or([0; 8]));
            let keys = u64::from_le_bytes(body[28..36].try_into().unwrap_or([0; 8]));
            open_manifest(c, rev, hash, keys, applied_index);
        }
        S_EXPORT_RANGE => {
            if result == KV_RESULT_COMPACTED {
                // The protected history is gone: ABORT rather than
                // ship a partial export (the whole reason the
                // protection phase exists).
                let _ = c.manifest.transition(BackupPhase::Aborted);
                fault(c, b"[bkp] aborted: protected history compacted");
                return;
            }
            if result == KV_RESULT_MANIFEST {
                let _ = c.manifest.transition(BackupPhase::Aborted);
                fault(c, b"[bkp] aborted: a large value is not carried");
                return;
            }
            if result != KV_RESULT_KVS || body.len() < 11 || body.len() > ENV_BUF {
                let mut msg = *b"[bkp] export read failed rc=000";
                with_rc(&mut msg, 28, result);
                fault(c, &msg);
                return;
            }
            let count = u16::from_le_bytes([body[9], body[10]]);
            if count == 0 && body[8] != 0 {
                // More to read and nothing that fit: one record is
                // larger than an answer.
                fault(c, b"[bkp] record larger than an answer");
                return;
            }
            c.ans[..body.len()].copy_from_slice(body);
            c.ans_len = body.len() as u16;
            c.ans_at = 11;
            c.ans_left = count;
            c.ans_more = body[8];
            drain_export(c);
        }
        S_EXPORT_PUT => {
            if result != KV_RESULT_OK {
                fault(c, b"[bkp] artifact write failed");
                return;
            }
            c.m_pages = c.m_pages.wrapping_add(1);
            c.sequence += 1;
            c.page_len = 0;
            c.page_records = 0;
            drain_export(c);
        }
        S_MANIFEST_PUT => {
            if result != KV_RESULT_OK {
                fault(c, b"[bkp] manifest write failed");
                return;
            }
            let _ = c.manifest.transition(BackupPhase::ProtectionReleased);
            // The protection is released in the store too.
            send_hold(c, 0, S_HOLD_RELEASE);
        }
        S_RESTORE_GET => {
            if result == KV_RESULT_NOT_FOUND {
                fault(c, b"[bkp] no such backup (manifest key absent)");
                return;
            }
            if result != KV_RESULT_OK {
                let mut msg = *b"[bkp] manifest read failed rc=000";
                with_rc(&mut msg, 30, result);
                fault(c, &msg);
                return;
            }
            let Some(m) = BackupManifest::decode(body) else {
                fault(c, b"[bkp] manifest corrupt");
                return;
            };
            if !m.is_complete() {
                // Refusing an incomplete backup IS the contract.
                fault(c, b"[bkp] refusing restore of an incomplete backup");
                return;
            }
            if m.artifact_format_version != BACKUP_ARTIFACT_FORMAT_VERSION || m.fence == 0 {
                fault(c, b"[bkp] refusing restore: artifact format or fence");
                return;
            }
            c.manifest = m;
            c.sequence = 0;
            c.page_hash = FNV_OFFSET;
            c.page_bytes = 0;
            c.restored_pages = 0;
            c.m_held = 0;
            log_info(c, b"[bkp] restore begins");
            next_restore_step(c);
        }
        S_RESTORE_ARTIFACT => {
            if result == KV_RESULT_NOT_FOUND {
                fault(c, b"[bkp] artifact missing");
                return;
            }
            if result != KV_RESULT_OK || body.len() > PAGE_BUF {
                fault(c, b"[bkp] artifact read failed");
                return;
            }
            let Some(n) = count_records(body) else {
                fault(c, b"[bkp] artifact page corrupt");
                return;
            };
            c.page[..body.len()].copy_from_slice(body);
            c.page_len = body.len() as u16;
            c.page_records = n;
            c.page_hash = fnv(c.page_hash, body);
            c.page_bytes += body.len() as u64;
            send_restore_page(c);
        }
        S_RESTORE_PAGE => {
            if result == KV_RESULT_EXISTS {
                fault(
                    c,
                    b"[bkp] restore refused: the target is not fresh, or fenced by another restore",
                );
                return;
            }
            if result != KV_RESULT_OK {
                let mut msg = *b"[bkp] restore page failed rc=000";
                with_rc(&mut msg, 29, result);
                fault(c, &msg);
                return;
            }
            // `[written:u16][held:u16]`: a record the target already held
            // with exactly this value and metadata is a page applied before
            // (a resumed restore, or one repeated over its own result).
            let held = match body {
                [_, _, h0, h1, ..] => u16::from_le_bytes([*h0, *h1]),
                _ => 0,
            };
            c.m_restored = c.m_restored.wrapping_add(u64::from(c.page_records));
            c.m_held = c.m_held.wrapping_add(u64::from(held));
            c.restored_pages += 1;
            c.sequence += 1;
            next_restore_step(c);
        }
        S_RESTORE_VERIFY => {
            if result != KV_RESULT_OK || body.len() < 36 {
                fault(c, b"[bkp] restore verify failed");
                return;
            }
            let hash = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
            let floor = u64::from_le_bytes(body[4..12].try_into().unwrap_or([0; 8]));
            let rev = u64::from_le_bytes(body[12..20].try_into().unwrap_or([0; 8]));
            let keys = u64::from_le_bytes(body[28..36].try_into().unwrap_or([0; 8]));
            if hash != c.manifest.state_hash
                || keys != c.manifest.state_keys
                || rev < c.manifest.protected_timestamp
            {
                fault(c, b"[bkp] restore digest disagrees with the snapshot's");
                return;
            }
            // History below R is not in the target (each key holds only its
            // version as of R): a read from under R must answer compacted,
            // never a partial history.
            if floor < c.manifest.protected_timestamp {
                fault(c, b"[bkp] restore left history below the snapshot readable");
                return;
            }
            // Measured on the target: records (and of them, held already),
            // R, the target's revision and its compaction floor.
            let mut msg = *b"[bkp] restore complete r=0000 h=0000 rev=0000000000 at=0000000000 floor=0000000000 verified";
            digits(&mut msg, 25, 4, c.m_restored.min(9999));
            digits(&mut msg, 32, 4, c.m_held.min(9999));
            digits(&mut msg, 41, 10, c.manifest.protected_timestamp);
            digits(&mut msg, 55, 10, rev);
            digits(&mut msg, 72, 10, floor);
            log_info(c, &msg);
            c.state = S_DONE;
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
        c.target_in = dev_channel_port(sys, 0, 1);
        c.target_out = dev_channel_port(sys, 1, 2);
    }
    if c.mode > 2 || c.epoch == 0 {
        return -1; // an unknown mode must not run as "inert"
    }
    // A target named by half its pair would send pages nowhere it reads.
    if (c.target_in < 0) != (c.target_out < 0) {
        return -1;
    }
    c.state = if c.mode == 0 { S_IDLE } else { S_WAIT };
    0
}

/// Read one KV answer from `chan` and apply it when it is the one in
/// flight.
unsafe fn take_answer(c: &mut CoordState, target: bool) {
    let sys = &*c.syscalls;
    let mut env = [0u8; ENVELOPE_HDR + ENV_BUF];
    let got = if target {
        read_envelope(&mut c.target_inbox, sys, c.target_in, &mut env)
    } else {
        read_envelope(&mut c.kv_inbox, sys, c.kv_in, &mut env)
    };
    let Some((msg, len)) = got else {
        return;
    };
    let payload = &env[..len];
    if msg != MSG_KV_RESPONSE || payload.len() < 20 {
        return;
    }
    let corr = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
    let result = payload[9];
    let blen = u16::from_le_bytes([payload[18], payload[19]]) as usize;
    if corr != c.corr || payload.len() < 20 + blen {
        return;
    }
    // The fence tail's applied index: the consensus position the answer
    // was produced at (0 where no log orders the store).
    let applied_index = payload
        .get(20 + blen..20 + blen + 8)
        .map_or(0, |b| u64::from_le_bytes(b.try_into().unwrap_or([0; 8])));
    let mut body = [0u8; ENV_BUF];
    body[..blen].copy_from_slice(&payload[20..20 + blen]);
    on_kv_response(c, result, &body[..blen], applied_index);
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
        take_answer(c, false);
        if c.target_in >= 0 {
            take_answer(c, true);
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
