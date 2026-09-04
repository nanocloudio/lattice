//! cdc_feed — the CDC egress pump's feed state machine, extracted from
//! the module shell so the whole protocol is host-testable.
//!
//! Dual-target facade: `#[path]`-mounted by `modules/app/cdc_pump`
//! (which owns channels, params and the syscall table) and by the host
//! conformance suite (`tests/harness/tests/contract_cdc_feed.rs`,
//! which drives it against a simulated KV provider and sink). Every
//! side effect goes through [`FeedIo`]; the core never touches a
//! channel or a clock, so a test can replay any interleaving —
//! multi-page windows, backpressure, link loss, compaction lapse,
//! crash-resume — deterministically.
//!
//! # Checkpoint protocol
//!
//! Pointer record `\x00cdc/ckpt/<feed_id>` holds
//! `[generation:u64][owner_epoch:u32]` and is the only conditionally
//! written record (`KV_OP_CAS` on its `mod_revision` witness).
//! Generation records `\x00cdc/ckpt/<feed_id>/g/<generation>`
//! hold `[state:u8][scan_cursor:u64][FeedCursor:50]` and are plain
//! puts — a torn write of generation n+1 is invisible until the
//! pointer names it. A CAS failure re-reads the pointer: a higher
//! epoch means this pump is deposed (HALTED); its own epoch means a
//! self-race, resolved by adopting the stored position when it is
//! ahead (monotonic max — a cursor never regresses) and retrying.
//!
//! # Streaming windows
//!
//! A window is `(window_from, window_hi]`: BOTH bounds are frozen for
//! the window's whole life. The provider's page cursor is an ordinal
//! within that fixed window (a key-ordered walk — NOT revision order),
//! so moving either bound between pages shifts every ordinal and the
//! resumed page silently skips events. `window_from` latches
//! `cur_revision` when the window opens; `window_hi` pins to the first
//! page's head revision; `cur_revision` advances ONLY at window close.
//! (The historical bug: `from` tracked the last published event's
//! revision, which in a key-ordered walk is an arbitrary mid-window
//! revision — every multi-page window dropped events and the
//! checkpoint advanced past them.)
//!
//! # Replay (LINK_DOWN contract)
//!
//! Every published-but-unacked frame is retained verbatim in a bounded
//! ring. LINK_DOWN marks the whole ring unknowable; LINK_UP replays it
//! in order. A full ring is backpressure: the pump stops reading feed
//! pages until acks drain it, and reports STALLED past a grace period.
//!
//! # Resolved watermarks
//!
//! `resolved(T)` promises nothing at or below T remains unpublished,
//! so T can never be the LIVE fence frontier: any response's fence
//! tail (a checkpoint ack, a mid-window page) can carry a frontier
//! covering feed-range writes the pump has not read yet. The only
//! honest T is `safe_frontier` — the frontier of the response that
//! CLOSED a window at the head. At that instant every feed-range write
//! at or below it is published or in the ring, and the emit gate
//! additionally requires the ring empty (all acked).
//!
//! # Compaction lapse (`on_lapse`)
//!
//! `KV_RESULT_COMPACTED` on a feed page means GC reclaimed the open
//! window. With `on_lapse: backfill` the feed RE-LATCHES: it re-reads
//! the pointer (adopting the live CAS witness), then instead of
//! resuming the compacted cursor it starts a fresh backfill at that
//! response's head revision — checkpointed immediately as a state
//! flip, so a crash mid-relatch converges to the same place. With
//! `on_lapse: halt` it parks HALTED. A backfill whose own snapshot
//! gets compacted is an operator problem and HALTs either way.
//!
//! # System-prefix guard
//!
//! The system prefix is structural here: [`FeedCore::validate_config`]
//! refuses a keyrange whose prefix begins with `0x00`, so a feed can
//! never observe its own checkpoint writes.

#![allow(
    dead_code,
    reason = "shared via #[path] into the cdc_pump module and host tests; each consumer uses a subset of the surface"
)]
#![allow(
    clippy::duplicate_mod,
    reason = "dual-target facade: consumers #[path]-remount the shared common code"
)]

#[path = "types.rs"]
pub mod types;

#[path = "wire.rs"]
pub mod wire;

#[path = "cdc_wire.rs"]
pub mod cdc_wire;

// The single mount of the SDK contract for this tree. Consumers that mount
// `cdc_feed` reach it as `cdc_feed::exchange` rather than mounting the file a
// second time, which rustc would treat as an unrelated module.
#[path = "../../target/fluxor/fluxor-abi/sdk/contracts/exchange.rs"]
pub mod exchange;

#[path = "db_ops.rs"]
pub mod db_ops;

#[path = "compaction_floor.rs"]
pub mod compaction_floor;

use cdc_wire::{
    resolved_event, sink_msg_key, CdcEvent, CDC_FLAG_BACKFILL, CDC_KIND_DELETE, CDC_KIND_PUT,
};
use compaction_floor::{RetentionClaim, CLAIM_SOURCE_WATCH, GC_CLAIM_WIRE_LEN};
use exchange::{
    Ack, Publish, FLAG_BROADCAST, MSG_PUBLISH, STATUS_LINK_DOWN, STATUS_LINK_UP, STATUS_OK,
};
use types::{
    KV_OP_CAS, KV_OP_GET, KV_OP_PUT, KV_OP_SCAN_VERSIONS, KV_OP_SNAPSHOT_VERSIONS,
    KV_RESULT_CAS_FAILED, KV_RESULT_COMPACTED, KV_RESULT_NOT_FOUND, KV_RESULT_OK,
    KV_RESULT_VERSIONS, PROTO_ETCD, VERSION_KIND_DELETE,
};
use wire::{MSG_KV_REQUEST, MSG_RETENTION_CLAIM};

/// Every side effect the feed machine can have. The module shell
/// implements this over syscalls; the host harness implements it over
/// Vecs and a scripted world. All three sends are complete channel
/// frames (3-byte envelope included) — a `false` return is channel
/// backpressure and the core treats it as retryable.
pub trait FeedIo {
    /// Ship a `MSG_KV_REQUEST` frame toward the router pair. The
    /// slice is `&mut` because the kernel channel-write ABI takes a
    /// mutable pointer; implementations must not actually mutate it.
    fn send_kv(&mut self, frame: &mut [u8]) -> bool;
    /// Ship a `MSG_PUBLISH` frame toward the sink.
    fn ship(&mut self, frame: &mut [u8]) -> bool;
    /// Ship a `MSG_RETENTION_CLAIM` frame toward the GC coordinator.
    fn send_claim(&mut self, frame: &mut [u8]) -> bool;
    /// Diagnostic log line (state moves, CAS outcomes, wedge ticks) —
    /// a stuck pump must be diagnosable from the boot log alone.
    fn log(&mut self, tag: &[u8; 4], value: u64);
}

// ── Feed lifecycle states ─────────────────────────────────────────────

/// `resume_retry_at_ms` sentinel: a fence refusal was seen this step; the
/// real retry deadline is stamped next step. Real deadlines are always
/// ≫ 1, and `0` means unarmed / ready to read.
pub const RESUME_ARM_PENDING: u64 = 1;

pub const ST_CREATED: u8 = 0;
pub const ST_BACKFILL: u8 = 1;
pub const ST_STREAMING: u8 = 2;
pub const ST_STALLED: u8 = 3;
pub const ST_NEEDS_BACKFILL: u8 = 4;
pub const ST_WEDGED: u8 = 5;
pub const ST_HALTED: u8 = 6;

// ── Internal KV request phases ────────────────────────────────────────

pub const P_IDLE: u8 = 0;
/// Pointer read (startup resume, and rule-5 re-read after CAS failure).
pub const P_PTR_READ: u8 = 1;
pub const P_GEN_READ: u8 = 2;
pub const P_GEN_WRITE: u8 = 3;
pub const P_PTR_CAS: u8 = 4;
pub const P_FEED_PAGE: u8 = 5;
pub const P_BACKFILL_PAGE: u8 = 6;

/// Published-but-unacked ring capacity. Full ring = backpressure.
pub const UNACKED_CAP: usize = 16;

pub const KEY_MAX: usize = 256;
pub const SCRATCH: usize = 12288;

/// Default feed-page request size (`page_limit` param). The worker
/// byte-bounds its reply anyway; the limit only caps work per response.
pub const PAGE_LIMIT_DEFAULT: u16 = 16;

/// Consecutive internal-error responses before the feed is WEDGED.
pub const WEDGE_BUDGET: u32 = 16;

/// A response overdue past this re-opens its phase (the request is
/// reissued — duplicate reads are free, duplicate checkpoint writes
/// are idempotent, duplicate events are covered by at-least-once).
pub const PHASE_TIMEOUT_MS: u64 = 3000;

/// One retained unacked publish: the full frame for replay, plus the
/// SAFE RESUME POSITION this entry promotes when it and everything
/// before it is acked. An event's own revision is NOT that position:
/// the provider walks windows in key order, so acking one
/// high-revision event mid-window says nothing about lower-revision
/// events still unread — a fold that maxed raw event revisions let a
/// checkpoint overshoot an open window and lose everything unacked
/// behind it on resume. Instead each entry carries the position that
/// BECOMES durable-safe at its contiguous ack:
///
/// - streaming event: `ck_rev = window_from` (a resume re-reads the
///   whole window — duplicates, never gaps); the window's LAST entry
///   is patched to `window_hi` at close.
/// - backfill row: `ck_rev = backfill_at`, `ck_scan` = the row's
///   post-publish scan ordinal, `ck_state = BACKFILL`.
/// - backfill-complete marker: `ck_state = STREAMING` — the durable
///   state flip rides the marker's ACK, so a checkpoint can never
///   claim the scan finished while rows are still unacked.
/// - resolved watermark: neutral (`ck_rev = 0`, fold is monotonic max).
#[repr(C)]
pub struct Unacked {
    pub corr: u64,
    pub ck_rev: u64,
    pub ck_scan: u64,
    pub ck_state: u8,
    pub commit_ts: u64,
    pub len: u16,
    pub acked: bool,
    pub refused: bool,
    pub frame: [u8; exchange::PUBLISH_FRAME_MAX],
}

/// The whole feed machine. The module shell owns one inside its state
/// struct; a host test heap-boxes one (it is large — the ring retains
/// worst-case frames).
#[repr(C)]
pub struct FeedCore {
    // ── params (written by the shell's TLV parser / test setup) ──
    pub feed_id: u32,
    pub table_id: u32,
    pub proto: u32,
    pub owner_epoch: u32,
    /// `on_lapse`: 1 = backfill, 0 = halt.
    pub on_lapse_backfill: u32,
    /// Start with a backfill when no checkpoint exists.
    pub backfill_on_create: u32,
    pub checkpoint_interval_events: u32,
    pub checkpoint_interval_ms: u32,
    pub resolved_interval_ms: u32,
    pub claim_interval_ms: u32,
    pub retention_budget_ms: u32,
    pub stall_grace_ms: u32,
    /// Feed-page request size, clamped to `1..=PAGE_LIMIT_DEFAULT`.
    pub page_limit: u16,
    /// Keyrange prefix, set via hex param.
    pub prefix: [u8; KEY_MAX],
    pub prefix_len: u16,

    // ── feed state ──
    pub state: u8,
    pub phase: u8,
    pub link_up: bool,
    /// Config refused at init (bad prefix) — hard no-serve.
    pub config_bad: bool,
    /// A compaction lapse is being re-latched: the next pointer read
    /// starts a fresh backfill instead of adopting the (compacted)
    /// generation cursor.
    pub force_backfill: bool,

    pub kv_corr: u64,
    pub corr_next: u64,
    /// When the outstanding KV request was issued (phase-timeout base).
    pub phase_since_ms: u64,

    // Cursor (in-flight; the durable one is whatever the checkpoint
    // holds). Single-range v1: revision + timestamp carry the
    // position, range identity is the implicit range.
    pub cur_revision: u64,
    pub cur_timestamp: u64,
    /// Frozen lower bound of the open window (exclusive). Latched from
    /// `cur_revision` when the window opens and NEVER moved until it
    /// closes — the provider's page ordinals are relative to it.
    pub window_from: u64,
    /// Pinned upper bound of the open window; 0 = none open.
    pub window_hi: u64,
    /// Provider ordinal within the open window.
    pub window_cursor: u64,

    // Backfill.
    pub backfill_at: u64,
    pub backfill_cursor: u64,

    // Checkpoint plumbing.
    pub ptr_witness: u64,
    pub ptr_generation: u64,
    /// Contiguous-acked checkpoint candidate.
    pub ck_revision: u64,
    pub ck_timestamp: u64,
    pub ck_state: u8,
    pub ck_scan_cursor: u64,
    /// Last durably checkpointed position.
    pub ck_durable_revision: u64,
    /// Lifecycle state byte of the last durable checkpoint; a state
    /// flip (BACKFILL -> STREAMING) is durable progress even when the
    /// cursor has not moved, so it makes a checkpoint due by itself.
    pub ck_durable_state: u8,
    pub last_ck_ms: u64,
    pub acked_since_ck: u32,

    // Resolved watermark cadence.
    /// Raw fence frontier — the newest commit frontier any response
    /// tail carried. Diagnostic only; never emitted as resolved.
    pub frontier: u64,
    /// The emit-safe frontier: latched from `frontier` only when a
    /// window CLOSES caught-up at the head, because only then is every
    /// feed-range write at or below it published or ringed.
    pub safe_frontier: u64,
    pub last_resolved_sent: u64,
    pub last_resolved_ms: u64,
    pub caught_up: bool,

    // Retention claim cadence.
    pub last_claim_ms: u64,
    pub claim_seq: u32,

    // Stall tracking.
    pub blocked_since_ms: u64,
    /// Backfill scan finished; the `backfill_complete` marker is still
    /// owed (ring was full when the scan completed).
    pub backfill_marker_owed: bool,
    /// Caught-up poll cadence.
    pub last_poll_ms: u64,
    /// Resume-read retry pacing: the linearizable pointer read is
    /// REJECTED until the leader is elected and apply has caught the
    /// commit horizon — that rejection is the readiness signal, not a
    /// fault, so it is retried on a cadence and never wedges.
    /// `RESUME_ARM_PENDING` = refusal seen this step; `0` = unarmed;
    /// otherwise the next-attempt timestamp.
    pub resume_retry_at_ms: u64,

    pub wedge_ctr: u32,

    // Unacked ring: `ring[tail..head)` modulo capacity.
    pub ring_head: u32,
    pub ring_tail: u32,
    pub ring: [Unacked; UNACKED_CAP],

    // ── counters (manifest order) ──
    pub m_published: u64,
    pub m_acked: u64,
    pub m_refusals: u64,
    pub m_replays: u64,
    pub m_checkpoints: u64,
    pub m_resolved: u64,
    pub m_backfill_rows: u64,
    pub m_compacted: u64,

    pub scratch: [u8; SCRATCH],
}

impl FeedCore {
    pub fn init(&mut self) {
        self.feed_id = 1;
        self.table_id = 0;
        self.proto = PROTO_ETCD as u32;
        self.owner_epoch = 1;
        self.on_lapse_backfill = 1;
        self.backfill_on_create = 1;
        self.checkpoint_interval_events = 32;
        self.checkpoint_interval_ms = 1000;
        self.resolved_interval_ms = 1000;
        self.claim_interval_ms = 1000;
        self.retention_budget_ms = 60_000;
        self.stall_grace_ms = 2000;
        self.page_limit = PAGE_LIMIT_DEFAULT;
        self.prefix = [0; KEY_MAX];
        self.prefix_len = 0;
        self.state = ST_CREATED;
        self.phase = P_IDLE;
        self.link_up = false;
        self.config_bad = false;
        self.force_backfill = false;
        self.kv_corr = 0;
        self.corr_next = 1;
        self.phase_since_ms = 0;
        self.cur_revision = 0;
        self.cur_timestamp = 0;
        self.window_from = 0;
        self.window_hi = 0;
        self.window_cursor = 0;
        self.backfill_at = 0;
        self.backfill_cursor = 0;
        self.ptr_witness = 0;
        self.ptr_generation = 0;
        self.ck_revision = 0;
        self.ck_timestamp = 0;
        self.ck_state = ST_CREATED;
        self.ck_scan_cursor = 0;
        self.ck_durable_revision = 0;
        self.ck_durable_state = ST_CREATED;
        self.last_ck_ms = 0;
        self.acked_since_ck = 0;
        self.frontier = 0;
        self.safe_frontier = 0;
        self.last_resolved_sent = 0;
        self.last_resolved_ms = 0;
        self.caught_up = false;
        self.last_claim_ms = 0;
        self.claim_seq = 0;
        self.blocked_since_ms = 0;
        self.backfill_marker_owed = false;
        self.last_poll_ms = 0;
        self.resume_retry_at_ms = 0;
        self.wedge_ctr = 0;
        self.ring_head = 0;
        self.ring_tail = 0;
        let mut i = 0;
        while i < UNACKED_CAP {
            self.ring[i] = Unacked {
                corr: 0,
                ck_rev: 0,
                ck_scan: 0,
                ck_state: ST_STREAMING,
                commit_ts: 0,
                len: 0,
                acked: false,
                refused: false,
                frame: [0; exchange::PUBLISH_FRAME_MAX],
            };
            i += 1;
        }
        self.m_published = 0;
        self.m_acked = 0;
        self.m_refusals = 0;
        self.m_replays = 0;
        self.m_checkpoints = 0;
        self.m_resolved = 0;
        self.m_backfill_rows = 0;
        self.m_compacted = 0;
        // `scratch` is deliberately left as-is: module state arrives
        // zeroed, and staging a 12 KiB temporary on the module stack
        // to re-zero it risks the stack budget for nothing.
    }

    /// Post-params validation. A keyrange under the system prefix
    /// would feed on its own checkpoints — a self-amplifying loop.
    /// Refused, loudly and permanently. Also clamps `page_limit`.
    pub fn validate_config(&mut self) {
        if self.prefix_len == 0 || self.prefix[0] == 0x00 {
            self.config_bad = true;
            self.state = ST_HALTED;
        }
        self.page_limit = self.page_limit.clamp(1, PAGE_LIMIT_DEFAULT);
    }

    pub fn ring_len(&self) -> u32 {
        self.ring_head.wrapping_sub(self.ring_tail)
    }

    pub fn ring_full(&self) -> bool {
        self.ring_len() as usize >= UNACKED_CAP
    }

    // ── KV request issuance ──────────────────────────────────────────

    /// Send one MSG_KV_REQUEST with the standard 18-byte head. One
    /// outstanding request at a time (`kv_corr`), the pair discipline.
    fn kv_send(
        &mut self,
        io: &mut impl FeedIo,
        now_ms: u64,
        op: u8,
        body_len: usize,
        phase: u8,
    ) -> bool {
        self.kv_send_c(io, now_ms, op, body_len, phase, 0)
    }

    /// `kv_send` with an explicit consistency byte. The checkpoint
    /// RESUME reads (pointer + generation) go out LINEARIZABLE: after
    /// a restart the store legally serves serializable reads from
    /// mid-replay state, and a resume decision taken on that state
    /// adopts a superseded cursor or invents a fresh feed. The fence
    /// delays the answer until apply has caught the commit horizon —
    /// the semantics a read-my-own-durable-writes resume actually
    /// requires. Everything else stays serializable: feed windows
    /// re-read harmlessly and the CAS witness discipline covers
    /// checkpoint write races.
    fn kv_send_c(
        &mut self,
        io: &mut impl FeedIo,
        now_ms: u64,
        op: u8,
        body_len: usize,
        phase: u8,
        consistency: u8,
    ) -> bool {
        const REQ_HEAD: usize = 18;
        let at = wire::ENVELOPE_HDR;
        let total = at + REQ_HEAD + body_len;
        if total > self.scratch.len() {
            return false;
        }
        // Namespaced correlation ids. The router's inflight table is
        // keyed by corr_id alone and is SHARED by every requester on the
        // graph, so a producer that counts 1, 2, 3… collides with any
        // other that does the same (the relational executor does exactly
        // that) — a collision drops one request at dispatch or routes
        // its reply to the other requester's channel. Bit 61 keeps this
        // feed's ids disjoint both from small-integer requesters and
        // from the router's own help space (bit 63); the low bits still
        // count monotonically. Translating corr ids into a router-unique
        // space at dispatch would remove the shared-namespace hazard for
        // every producer at once; until a producer-blind translation
        // exists, each internal requester owns a namespace bit.
        const CDC_CORR_BASE: u64 = 0x2000_0000_0000_0000;
        let next = (self.kv_corr.wrapping_add(1)) & 0x0FFF_FFFF_FFFF_FFFF;
        self.kv_corr = CDC_CORR_BASE | next.max(1);
        let payload = REQ_HEAD + body_len;
        self.scratch[0] = MSG_KV_REQUEST;
        self.scratch[1] = (payload & 0xFF) as u8;
        self.scratch[2] = ((payload >> 8) & 0xFF) as u8;
        self.scratch[at..at + 8].copy_from_slice(&self.kv_corr.to_le_bytes());
        self.scratch[at + 8] = self.proto as u8;
        self.scratch[at + 9..at + 13].copy_from_slice(&0u32.to_le_bytes());
        self.scratch[at + 13] = 0; // conn slot: pump-internal
        self.scratch[at + 14] = consistency;
        self.scratch[at + 15] = op;
        self.scratch[at + 16..at + 18].copy_from_slice(&(body_len as u16).to_le_bytes());
        let sent = io.send_kv(&mut self.scratch[..total]);
        if sent {
            self.phase = phase;
            self.phase_since_ms = now_ms;
        }
        sent
    }

    // ── Checkpoint keys ──────────────────────────────────────────────

    /// `\x00cdc/ckpt/<feed_id LE 4>` — the pointer record.
    pub fn ptr_key(&self, out: &mut [u8; KEY_MAX]) -> usize {
        let tag = b"\x00cdc/ckpt/";
        out[..tag.len()].copy_from_slice(tag);
        out[tag.len()..tag.len() + 4].copy_from_slice(&self.feed_id.to_le_bytes());
        tag.len() + 4
    }

    /// `\x00cdc/ckpt/<feed_id>/g/<generation LE 8>` — a generation record.
    pub fn gen_key(&self, generation: u64, out: &mut [u8; KEY_MAX]) -> usize {
        let n = self.ptr_key(out);
        out[n..n + 3].copy_from_slice(b"/g/");
        out[n + 3..n + 11].copy_from_slice(&generation.to_le_bytes());
        n + 11
    }

    fn encode_gen_record(&self, out: &mut [u8]) -> Option<usize> {
        if out.len() < GEN_RECORD_LEN {
            return None;
        }
        out[0] = self.ck_state;
        out[1..9].copy_from_slice(&self.ck_scan_cursor.to_le_bytes());
        let mut range_id = [0u8; 16];
        range_id[15] = 1;
        let cur = db_ops::FeedCursor {
            database_id: 0,
            partition_map_id: 0,
            range_id,
            range_generation: 1,
            revision: self.ck_revision,
            timestamp: self.ck_timestamp,
            format_version: db_ops::FEED_EVENT_FORMAT_VERSION,
        };
        let n = cur.encode(&mut out[9..])?;
        Some(9 + n)
    }

    // ── Publishing ───────────────────────────────────────────────────

    /// Encode one event into a publish frame and ship it, retaining the
    /// frame in the unacked ring together with the safe resume
    /// position its contiguous ack promotes (see [`Unacked`]).
    /// `false` = ring full / channel refused (caller treats as
    /// backpressure and retries the page later).
    fn publish_event(
        &mut self,
        io: &mut impl FeedIo,
        ev: &CdcEvent<'_>,
        broadcast: bool,
        ck_rev: u64,
        ck_scan: u64,
        ck_state: u8,
    ) -> bool {
        if self.ring_full() {
            return false;
        }
        let corr = self.corr_next;
        let mut mkey = [0u8; 4 + KEY_MAX];
        let mklen = match sink_msg_key(self.table_id, ev.key, &mut mkey) {
            Some(n) => n,
            None => return false,
        };
        let mut payload = [0u8; cdc_wire::CDC_ENVELOPE_MAX];
        let plen = match ev.encode(&mut payload) {
            Some(n) => n,
            None => return false,
        };
        let publ = Publish {
            corr,
            flags: if broadcast { FLAG_BROADCAST } else { 0 },
            msg_key: &mkey[..mklen],
            payload: &payload[..plen],
        };
        let slot = (self.ring_head as usize) % UNACKED_CAP;
        let frame_len = {
            let entry = &mut self.ring[slot];
            let Some(n) = publ.encode(&mut entry.frame) else {
                return false;
            };
            entry.corr = corr;
            entry.ck_rev = ck_rev;
            entry.ck_scan = ck_scan;
            entry.ck_state = ck_state;
            entry.commit_ts = ev.commit_ts;
            entry.len = n as u16;
            entry.acked = false;
            entry.refused = false;
            n
        };
        if !self.ship_frame(io, slot, frame_len) {
            // Channel backpressure: the entry stays staged in the ring
            // slot but head does not advance; the caller retries.
            return false;
        }
        self.corr_next = self.corr_next.wrapping_add(1).max(1);
        self.ring_head = self.ring_head.wrapping_add(1);
        self.m_published = self.m_published.wrapping_add(1);
        true
    }

    fn ship_frame(&mut self, io: &mut impl FeedIo, slot: usize, len: usize) -> bool {
        let total = 3 + len;
        let mut buf = [0u8; 3 + exchange::PUBLISH_FRAME_MAX];
        buf[0] = MSG_PUBLISH;
        buf[1] = (len & 0xFF) as u8;
        buf[2] = ((len >> 8) & 0xFF) as u8;
        buf[3..total].copy_from_slice(&self.ring[slot].frame[..len]);
        io.ship(&mut buf[..total])
    }

    /// Replay every unacked frame in ring order after LINK_UP (the
    /// unacked set became unknowable at LINK_DOWN).
    fn replay_unacked(&mut self, io: &mut impl FeedIo) {
        let mut i = self.ring_tail;
        while i != self.ring_head {
            let slot = (i as usize) % UNACKED_CAP;
            if !self.ring[slot].acked && !self.ring[slot].refused {
                let len = self.ring[slot].len as usize;
                if !self.ship_frame(io, slot, len) {
                    // Backpressure mid-replay: stop; the remaining
                    // frames replay on later steps (ordering per key
                    // preserved — nothing new is published while any
                    // replay is owed, because the ring is drained
                    // front-first).
                    break;
                }
                self.m_replays = self.m_replays.wrapping_add(1);
            }
            i = i.wrapping_add(1);
        }
    }

    /// Fold one sink frame in: link-state signals drive the replay
    /// contract, replies advance the contiguous acked prefix and the
    /// checkpoint candidate.
    pub fn on_sink_ack(&mut self, io: &mut impl FeedIo, ack: &Ack) {
        if ack.is_link_state() {
            match ack.status {
                STATUS_LINK_DOWN => {
                    self.link_up = false;
                }
                STATUS_LINK_UP => {
                    let was_up = self.link_up;
                    self.link_up = true;
                    if !was_up {
                        io.log(b"lkup", u64::from(self.state));
                        self.replay_unacked(io);
                    }
                }
                _ => {}
            }
            return;
        }
        let mut i = self.ring_tail;
        while i != self.ring_head {
            let slot = (i as usize) % UNACKED_CAP;
            if self.ring[slot].corr == ack.corr {
                if ack.status == STATUS_OK {
                    self.ring[slot].acked = true;
                    self.m_acked = self.m_acked.wrapping_add(1);
                    self.acked_since_ck = self.acked_since_ck.wrapping_add(1);
                } else {
                    // Typed refusal: counted, surfaced, and deliberately
                    // NOT skipped — the contiguous prefix stops here, so
                    // the checkpoint can never claim a hole was delivered.
                    self.ring[slot].refused = true;
                    self.m_refusals = self.m_refusals.wrapping_add(1);
                }
                break;
            }
            i = i.wrapping_add(1);
        }
        // Advance the contiguous prefix — monotonic max, never a raw
        // assignment: control events ride the ring with ck_rev 0, and
        // a cursor that regressed on their ack would block every later
        // checkpoint behind a position already durable (monotonic-max
        // discipline, applied locally). The BACKFILL -> STREAMING flip
        // and the acked scan ordinal promote here too — durable claims
        // follow ACKS, never publishes.
        while self.ring_tail != self.ring_head {
            let slot = (self.ring_tail as usize) % UNACKED_CAP;
            if !self.ring[slot].acked {
                break;
            }
            self.ck_revision = self.ck_revision.max(self.ring[slot].ck_rev);
            self.ck_timestamp = self.ck_timestamp.max(self.ring[slot].commit_ts);
            if self.ck_state == ST_BACKFILL {
                if self.ring[slot].ck_state == ST_STREAMING && self.ring[slot].ck_rev > 0 {
                    // The completion marker folded: every row before it
                    // is acked, the scan is durably done.
                    self.ck_state = ST_STREAMING;
                    self.ck_scan_cursor = 0;
                } else if self.ring[slot].ck_state == ST_BACKFILL {
                    self.ck_scan_cursor = self.ck_scan_cursor.max(self.ring[slot].ck_scan);
                }
            }
            self.ring_tail = self.ring_tail.wrapping_add(1);
        }
    }

    /// Build the span bounds `[prefix, prefix-successor)` into `out`,
    /// returning `(start_len, end_len)` with start at `out[0..]` and
    /// end at `out[KEY_MAX..]`.
    fn span_bounds(&self, out: &mut [u8; KEY_MAX * 2]) -> Option<(usize, usize)> {
        let n = self.prefix_len as usize;
        out[..n].copy_from_slice(&self.prefix[..n]);
        // Successor: increment the last non-0xFF byte, truncating after
        // it. All-0xFF has no successor (unbounded above is not a legal
        // feed range).
        let mut e = [0u8; KEY_MAX];
        e[..n].copy_from_slice(&self.prefix[..n]);
        let mut i = n;
        loop {
            if i == 0 {
                return None;
            }
            i -= 1;
            if e[i] != 0xFF {
                e[i] += 1;
                break;
            }
        }
        let elen = i + 1;
        out[KEY_MAX..KEY_MAX + elen].copy_from_slice(&e[..elen]);
        Some((n, elen))
    }

    fn issue_ptr_read(&mut self, io: &mut impl FeedIo, now_ms: u64) -> bool {
        // The pointer is read with SNAPSHOT_VERSIONS over exactly its
        // own key (span `[k, k||0x00)`), NOT a plain GET: the CAS
        // witness is the record's `mod_revision`, which only a versions
        // scan reports per row. A head-revision witness wedges the
        // first checkpoint after any restart (the pointer is never the
        // newest write then).
        let mut k = [0u8; KEY_MAX];
        let klen = self.ptr_key(&mut k);
        let elen = klen + 1; // k || 0x00 — the immediate successor
        let body_len = 8 + 2 + klen + 2 + elen + 8 + 2;
        let mut p = BODY_AT;
        self.scratch[p..p + 8].copy_from_slice(&0u64.to_le_bytes()); // latest
        p += 8;
        self.scratch[p..p + 2].copy_from_slice(&(klen as u16).to_le_bytes());
        p += 2;
        self.scratch[p..p + klen].copy_from_slice(&k[..klen]);
        p += klen;
        self.scratch[p..p + 2].copy_from_slice(&(elen as u16).to_le_bytes());
        p += 2;
        self.scratch[p..p + klen].copy_from_slice(&k[..klen]);
        self.scratch[p + klen] = 0x00;
        p += elen;
        self.scratch[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
        p += 8;
        self.scratch[p..p + 2].copy_from_slice(&1u16.to_le_bytes());
        // REQ_LINEARIZABLE = 0x01 (types.rs).
        self.kv_send_c(
            io,
            now_ms,
            KV_OP_SNAPSHOT_VERSIONS,
            body_len,
            P_PTR_READ,
            0x01,
        )
    }

    fn issue_gen_read(&mut self, io: &mut impl FeedIo, now_ms: u64) -> bool {
        let mut k = [0u8; KEY_MAX];
        let klen = self.gen_key(self.ptr_generation, &mut k);
        let body_len = 2 + klen;
        self.scratch[BODY_AT..BODY_AT + 2].copy_from_slice(&(klen as u16).to_le_bytes());
        self.scratch[BODY_AT + 2..BODY_AT + 2 + klen].copy_from_slice(&k[..klen]);
        self.kv_send_c(io, now_ms, KV_OP_GET, body_len, P_GEN_READ, 0x01)
    }

    fn issue_gen_write(&mut self, io: &mut impl FeedIo, now_ms: u64) -> bool {
        let mut k = [0u8; KEY_MAX];
        let klen = self.gen_key(self.ptr_generation.wrapping_add(1), &mut k);
        let mut rec = [0u8; GEN_RECORD_LEN];
        let Some(rlen) = self.encode_gen_record(&mut rec) else {
            return false;
        };
        // PUT body: [klen][key][vlen:u32][value][flags:u8][ttl:u64].
        let body_len = 2 + klen + 4 + rlen + 1 + 8;
        let mut p = BODY_AT;
        self.scratch[p..p + 2].copy_from_slice(&(klen as u16).to_le_bytes());
        p += 2;
        self.scratch[p..p + klen].copy_from_slice(&k[..klen]);
        p += klen;
        self.scratch[p..p + 4].copy_from_slice(&(rlen as u32).to_le_bytes());
        p += 4;
        self.scratch[p..p + rlen].copy_from_slice(&rec[..rlen]);
        p += rlen;
        self.scratch[p] = 0;
        p += 1;
        self.scratch[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
        self.kv_send(io, now_ms, KV_OP_PUT, body_len, P_GEN_WRITE)
    }

    fn issue_ptr_cas(&mut self, io: &mut impl FeedIo, now_ms: u64) -> bool {
        let mut k = [0u8; KEY_MAX];
        let klen = self.ptr_key(&mut k);
        // Pointer value: [generation:u64][owner_epoch:u32].
        let mut v = [0u8; 12];
        v[0..8].copy_from_slice(&self.ptr_generation.wrapping_add(1).to_le_bytes());
        v[8..12].copy_from_slice(&self.owner_epoch.to_le_bytes());
        // CAS body: [klen][key][witness:u64][vlen:u32][value].
        let body_len = 2 + klen + 8 + 4 + v.len();
        let mut p = BODY_AT;
        self.scratch[p..p + 2].copy_from_slice(&(klen as u16).to_le_bytes());
        p += 2;
        self.scratch[p..p + klen].copy_from_slice(&k[..klen]);
        p += klen;
        self.scratch[p..p + 8].copy_from_slice(&self.ptr_witness.to_le_bytes());
        p += 8;
        self.scratch[p..p + 4].copy_from_slice(&(v.len() as u32).to_le_bytes());
        p += 4;
        self.scratch[p..p + 12].copy_from_slice(&v);
        self.kv_send(io, now_ms, KV_OP_CAS, body_len, P_PTR_CAS)
    }

    fn issue_feed_page(&mut self, io: &mut impl FeedIo, now_ms: u64) -> bool {
        let mut bounds = [0u8; KEY_MAX * 2];
        let Some((slen, elen)) = self.span_bounds(&mut bounds) else {
            self.state = ST_HALTED;
            return false;
        };
        // SCAN_VERSIONS body:
        // [slen][start][elen][end][from][to][cursor][limit].
        //
        // `from` is the window's FROZEN lower bound, never the
        // in-flight cursor: the provider walks the window in key order
        // and its page cursor is an ordinal within `(from, to]`, so a
        // moved bound would shift every ordinal and skip events.
        let body_len = 2 + slen + 2 + elen + 8 + 8 + 8 + 2;
        let mut p = BODY_AT;
        self.scratch[p..p + 2].copy_from_slice(&(slen as u16).to_le_bytes());
        p += 2;
        self.scratch[p..p + slen].copy_from_slice(&bounds[..slen]);
        p += slen;
        self.scratch[p..p + 2].copy_from_slice(&(elen as u16).to_le_bytes());
        p += 2;
        self.scratch[p..p + elen].copy_from_slice(&bounds[KEY_MAX..KEY_MAX + elen]);
        p += elen;
        self.scratch[p..p + 8].copy_from_slice(&self.window_from.to_le_bytes());
        p += 8;
        self.scratch[p..p + 8].copy_from_slice(&self.window_hi.to_le_bytes());
        p += 8;
        self.scratch[p..p + 8].copy_from_slice(&self.window_cursor.to_le_bytes());
        p += 8;
        self.scratch[p..p + 2].copy_from_slice(&self.page_limit.to_le_bytes());
        self.kv_send(io, now_ms, KV_OP_SCAN_VERSIONS, body_len, P_FEED_PAGE)
    }

    /// Open a fresh window: freeze the lower bound at `cur_revision`,
    /// `to = 0` ("up to now"); the response's head revision pins
    /// `window_hi` so later pages of the same window use stable
    /// ordinals against the same fixed `(from, to]`.
    fn issue_open_window(&mut self, io: &mut impl FeedIo, now_ms: u64) -> bool {
        self.window_from = self.cur_revision;
        self.window_hi = 0;
        self.window_cursor = 0;
        self.issue_feed_page(io, now_ms)
    }

    fn issue_backfill_page(&mut self, io: &mut impl FeedIo, now_ms: u64) -> bool {
        let mut bounds = [0u8; KEY_MAX * 2];
        let Some((slen, elen)) = self.span_bounds(&mut bounds) else {
            self.state = ST_HALTED;
            return false;
        };
        // SNAPSHOT_VERSIONS body:
        // [at_rev][slen][start][elen][end][cursor][limit].
        let body_len = 8 + 2 + slen + 2 + elen + 8 + 2;
        let mut p = BODY_AT;
        self.scratch[p..p + 8].copy_from_slice(&self.backfill_at.to_le_bytes());
        p += 8;
        self.scratch[p..p + 2].copy_from_slice(&(slen as u16).to_le_bytes());
        p += 2;
        self.scratch[p..p + slen].copy_from_slice(&bounds[..slen]);
        p += slen;
        self.scratch[p..p + 2].copy_from_slice(&(elen as u16).to_le_bytes());
        p += 2;
        self.scratch[p..p + elen].copy_from_slice(&bounds[KEY_MAX..KEY_MAX + elen]);
        p += elen;
        self.scratch[p..p + 8].copy_from_slice(&self.backfill_cursor.to_le_bytes());
        p += 8;
        self.scratch[p..p + 2].copy_from_slice(&self.page_limit.to_le_bytes());
        self.kv_send(
            io,
            now_ms,
            KV_OP_SNAPSHOT_VERSIONS,
            body_len,
            P_BACKFILL_PAGE,
        )
    }

    // ── KV response handling ─────────────────────────────────────────

    /// Walk a KV_RESULT_VERSIONS body, publishing events. Returns
    /// `(next_cursor, emitted, all_published)`; `all_published = false`
    /// means backpressure stopped the walk mid-page (the page must be
    /// re-read from the same window ordinal).
    fn publish_versions_page(
        &mut self,
        io: &mut impl FeedIo,
        body: &[u8],
        backfill: bool,
    ) -> Option<(u64, u16, bool)> {
        if body.len() < 10 {
            return None;
        }
        let next_cursor = u64::from_le_bytes(body[0..8].try_into().ok()?);
        let count = u16::from_le_bytes([body[8], body[9]]) as usize;
        let mut p = 10usize;
        let mut published: u16 = 0;
        for _ in 0..count {
            if body.len() < p + 23 {
                return None;
            }
            let revision = u64::from_le_bytes(body[p..p + 8].try_into().ok()?);
            let commit_ts = u64::from_le_bytes(body[p + 8..p + 16].try_into().ok()?);
            let kind = body[p + 16];
            let klen = u16::from_le_bytes([body[p + 17], body[p + 18]]) as usize;
            p += 19;
            let key = body.get(p..p + klen)?;
            p += klen;
            let vlen = u32::from_le_bytes(body.get(p..p + 4)?.try_into().ok()?) as usize;
            p += 4;
            let value = body.get(p..p + vlen)?;
            p += vlen;
            let ev = CdcEvent {
                kind: if kind == VERSION_KIND_DELETE {
                    CDC_KIND_DELETE
                } else {
                    CDC_KIND_PUT
                },
                flags: if backfill { CDC_FLAG_BACKFILL } else { 0 },
                feed_id: self.feed_id as u64,
                table_id: self.table_id,
                key,
                commit_ts,
                range_id: 1,
                range_generation: 1,
                revision,
                value,
            };
            let (ck_rev, ck_scan, ck_state) = if backfill {
                (
                    self.backfill_at,
                    self.backfill_cursor.wrapping_add(1),
                    ST_BACKFILL,
                )
            } else {
                // Mid-window, the only safe resume point is the frozen
                // lower bound; the close patches the window's last
                // entry up to `window_hi`.
                (self.window_from, 0, ST_STREAMING)
            };
            if !self.publish_event(io, &ev, false, ck_rev, ck_scan, ck_state) {
                // Ring/channel backpressure. Events already published
                // from this page advanced only the window ORDINAL; the
                // window bounds are frozen, so the page re-reads from
                // this ordinal and re-published events are the
                // at-least-once duplicates.
                return Some((next_cursor, published, false));
            }
            published += 1;
            if backfill {
                self.m_backfill_rows = self.m_backfill_rows.wrapping_add(1);
                self.backfill_cursor = self.backfill_cursor.wrapping_add(1);
                // ck_scan_cursor deliberately NOT advanced here: the
                // durable scan ordinal follows the fold (acks), not
                // the publish walk.
            } else {
                // The window's bounds stay FROZEN while it is open —
                // only the ordinal moves. `cur_revision` advances at
                // window close (it is the next window's lower bound).
                self.cur_timestamp = commit_ts;
                self.window_cursor = self.window_cursor.wrapping_add(1);
            }
        }
        Some((next_cursor, published, true))
    }

    /// Latch a fresh backfill at `revision` (a response head). Used by
    /// the fresh-feed path and by the compaction-lapse relatch; both
    /// checkpoint the state flip on the normal cadence, so a crash
    /// before it lands converges by re-running the same relatch.
    fn latch_backfill(&mut self, revision: u64) {
        self.state = ST_BACKFILL;
        self.ck_state = ST_BACKFILL;
        self.backfill_at = revision;
        self.backfill_cursor = 0;
        self.ck_scan_cursor = 0;
        self.cur_revision = revision;
        // Monotonic max: the head revision is ≥ anything acked so far,
        // but never regress a candidate the fold already advanced.
        self.ck_revision = self.ck_revision.max(revision);
        self.window_from = 0;
        self.window_hi = 0;
        self.window_cursor = 0;
        self.caught_up = false;
        // Entries still in flight belong to the SUPERSEDED stream: on
        // fold they may advance the revision (≤ the latch head, so the
        // max is a no-op) but must not flip the durable state out of
        // BACKFILL or resurrect a scan ordinal.
        let mut i = self.ring_tail;
        while i != self.ring_head {
            let slot = (i as usize) % UNACKED_CAP;
            self.ring[slot].ck_state = ST_BACKFILL;
            self.ring[slot].ck_scan = 0;
            i = i.wrapping_add(1);
        }
    }

    pub fn on_kv_response(
        &mut self,
        io: &mut impl FeedIo,
        now_ms: u64,
        result: u8,
        revision: u64,
        commit_frontier: u64,
        body: &[u8],
    ) {
        // Fence tail: the commit frontier rides every response. Raw
        // frontier only — the emit-safe latch happens at window close.
        if commit_frontier > self.frontier {
            self.frontier = commit_frontier;
        }
        let phase = self.phase;
        self.phase = P_IDLE;
        match phase {
            P_PTR_READ => match result {
                KV_RESULT_VERSIONS => {
                    // 0 entries = no checkpoint; 1 entry = the pointer
                    // with its own mod_revision (the CAS witness)
                    // leading the versions entry layout.
                    if body.len() < 10 {
                        self.bump_wedge(io);
                        return;
                    }
                    let count = u16::from_le_bytes([body[8], body[9]]);
                    if count == 0 {
                        // Fresh feed. Latch the enable frontier off this
                        // response's engine revision (the per-range
                        // "as of").
                        self.ptr_witness = 0;
                        self.ptr_generation = 0;
                        let want_backfill = self.backfill_on_create != 0 || self.force_backfill;
                        self.force_backfill = false;
                        if want_backfill && revision > 0 {
                            self.latch_backfill(revision);
                        } else {
                            self.state = ST_STREAMING;
                            self.ck_state = ST_STREAMING;
                            self.cur_revision = 0;
                            self.ck_revision = self.ck_revision.max(self.cur_revision);
                        }
                        return;
                    }
                    // Entry: [mod_revision:8][commit_ts:8][kind:1]
                    // [klen:2][key…][vlen:4][value…]; the pointer value
                    // is [generation:u64][owner_epoch:u32].
                    if body.len() < 10 + 23 {
                        self.bump_wedge(io);
                        return;
                    }
                    let witness = u64::from_le_bytes(body[10..18].try_into().unwrap_or([0; 8]));
                    let klen = u16::from_le_bytes([body[27], body[28]]) as usize;
                    let voff = 10 + 19 + klen + 4;
                    if body.len() < voff + 12 {
                        self.state = ST_HALTED;
                        return;
                    }
                    let generation =
                        u64::from_le_bytes(body[voff..voff + 8].try_into().unwrap_or([0; 8]));
                    let epoch =
                        u32::from_le_bytes(body[voff + 8..voff + 12].try_into().unwrap_or([0; 4]));
                    if epoch > self.owner_epoch {
                        // Deposed. Stop, release, do not retry.
                        self.state = ST_HALTED;
                        return;
                    }
                    self.ptr_generation = generation;
                    self.ptr_witness = witness;
                    io.log(b"ptrr", generation);
                    if self.force_backfill {
                        // Compaction-lapse relatch: the generation
                        // record names a compacted cursor, so adopting
                        // it would just re-earn the COMPACTED refusal —
                        // the livelock this branch exists to break.
                        // Start a fresh backfill at this response's
                        // head instead; the CAS witness adopted above
                        // keeps the checkpoint chain intact.
                        self.force_backfill = false;
                        self.latch_backfill(revision);
                        io.log(b"lbkf", revision);
                        return;
                    }
                    let _ = self.issue_gen_read(io, now_ms);
                }
                _ => {
                    // The fence refused (no leader yet / apply behind
                    // the horizon). Expected at boot; retry on a
                    // cadence.
                    self.resume_retry_at_ms = RESUME_ARM_PENDING;
                }
            },
            P_GEN_READ => match result {
                KV_RESULT_OK => {
                    if body.len() < GEN_RECORD_LEN {
                        self.state = ST_HALTED;
                        return;
                    }
                    let state = body[0];
                    let scan_cursor = u64::from_le_bytes(body[1..9].try_into().unwrap_or([0; 8]));
                    let Some(cur) =
                        db_ops::FeedCursor::decode(&body[9..9 + db_ops::FeedCursor::WIRE_LEN])
                    else {
                        self.state = ST_HALTED;
                        return;
                    };
                    self.cur_revision = cur.revision;
                    self.cur_timestamp = cur.timestamp;
                    self.ck_revision = cur.revision;
                    self.ck_timestamp = cur.timestamp;
                    self.ck_durable_revision = cur.revision;
                    self.ck_durable_state = state;
                    if state == ST_BACKFILL {
                        self.state = ST_BACKFILL;
                        self.ck_state = ST_BACKFILL;
                        self.backfill_at = cur.revision;
                        self.backfill_cursor = scan_cursor;
                        self.ck_scan_cursor = scan_cursor;
                    } else {
                        self.state = ST_STREAMING;
                        self.ck_state = ST_STREAMING;
                    }
                    io.log(b"resu", cur.revision);
                }
                KV_RESULT_NOT_FOUND => {
                    // Pointer names a generation that never landed — a
                    // torn write of a PREVIOUS incarnation's pointer,
                    // which the protocol makes impossible for committed
                    // pointers. Fail loudly rather than guess a
                    // position.
                    self.state = ST_HALTED;
                }
                _ => {
                    // Fence refusal on the generation read: re-run the
                    // resume from the pointer on the retry cadence.
                    self.resume_retry_at_ms = RESUME_ARM_PENDING;
                }
            },
            P_GEN_WRITE => match result {
                KV_RESULT_OK => {
                    let _ = self.issue_ptr_cas(io, now_ms);
                }
                _ => self.bump_wedge(io),
            },
            P_PTR_CAS => match result {
                KV_RESULT_OK => {
                    self.ptr_generation = self.ptr_generation.wrapping_add(1);
                    self.ptr_witness = revision;
                    self.ck_durable_revision = self.ck_revision;
                    self.ck_durable_state = self.ck_state;
                    self.acked_since_ck = 0;
                    self.m_checkpoints = self.m_checkpoints.wrapping_add(1);
                    io.log(b"ckpt", self.ck_durable_revision);
                }
                KV_RESULT_CAS_FAILED => {
                    io.log(b"casf", self.ptr_witness);
                    // Re-read, epoch-check, monotonic-max
                    // merge, retry on a later cadence tick.
                    let _ = self.issue_ptr_read(io, now_ms);
                }
                _ => self.bump_wedge(io),
            },
            P_FEED_PAGE => match result {
                KV_RESULT_VERSIONS => {
                    self.wedge_ctr = 0;
                    // Pin the window's upper bound on its FIRST page:
                    // the provider normalized `to = 0` to the engine
                    // revision this head reports, and later pages of
                    // the window must use the same bound or their
                    // resume ordinals shift as new writes land.
                    if self.window_hi == 0 {
                        self.window_hi = revision;
                    }
                    let Some((next_cursor, _published, complete)) =
                        self.publish_versions_page(io, body, false)
                    else {
                        self.bump_wedge(io);
                        return;
                    };
                    if !complete {
                        // Backpressure: keep the window open; the page
                        // re-reads from the same ordinal later.
                        return;
                    }
                    if next_cursor == 0 {
                        // Window (window_from, window_hi] fully
                        // published. The LAST published entry is the
                        // one whose contiguous ack makes the whole
                        // window durable — promote its safe resume
                        // position to the window's upper bound. With
                        // the ring already drained, every published
                        // event is acked and the bound is safe RIGHT
                        // NOW — without this, a window whose acks beat
                        // its close would leave the durable cursor
                        // parked one window behind forever.
                        if self.ring_head != self.ring_tail {
                            let last = (self.ring_head.wrapping_sub(1) as usize) % UNACKED_CAP;
                            self.ring[last].ck_rev = self.ring[last].ck_rev.max(self.window_hi);
                        } else {
                            self.ck_revision = self.ck_revision.max(self.window_hi);
                        }
                        self.cur_revision = self.cur_revision.max(self.window_hi);
                        self.window_from = 0;
                        self.window_hi = 0;
                        self.window_cursor = 0;
                        self.caught_up = revision <= self.cur_revision;
                        if self.caught_up {
                            // The one instant a resolved frontier is
                            // honest: everything at or below this
                            // response's frontier is published or in
                            // the ring (the emit gate requires the
                            // ring drained on top).
                            self.safe_frontier = self.frontier;
                        }
                    } else {
                        self.window_cursor = next_cursor;
                    }
                }
                KV_RESULT_COMPACTED => {
                    // The retention budget lapsed and GC reclaimed the
                    // window. COMPACTED — not a GC-side signal — drives
                    // the transition, so a pump that got past the
                    // danger zone first was never needlessly killed.
                    self.m_compacted = self.m_compacted.wrapping_add(1);
                    self.window_from = 0;
                    self.window_hi = 0;
                    self.window_cursor = 0;
                    if self.on_lapse_backfill != 0 {
                        self.state = ST_NEEDS_BACKFILL;
                    } else {
                        self.state = ST_HALTED;
                    }
                }
                _ => self.bump_wedge(io),
            },
            P_BACKFILL_PAGE => match result {
                KV_RESULT_VERSIONS => {
                    self.wedge_ctr = 0;
                    let Some((next_cursor, _published, complete)) =
                        self.publish_versions_page(io, body, true)
                    else {
                        self.bump_wedge(io);
                        return;
                    };
                    if !complete {
                        return;
                    }
                    if next_cursor == 0 {
                        // Scan complete at the frontier. Emit the
                        // marker (retried from the step loop if the
                        // ring is full right now) and stream from F.
                        // The DURABLE state (`ck_state`) stays
                        // BACKFILL until the marker's ack folds — a
                        // checkpoint written before that must resume
                        // the scan, because unacked rows are not
                        // delivered rows.
                        self.backfill_marker_owed = true;
                        self.state = ST_STREAMING;
                        self.cur_revision = self.backfill_at;
                    } else {
                        self.backfill_cursor = next_cursor;
                    }
                }
                KV_RESULT_COMPACTED => {
                    // Budget interaction: a backfill that cannot read
                    // its own snapshot is an operator problem — HALTED,
                    // never a re-latch livelock.
                    self.m_compacted = self.m_compacted.wrapping_add(1);
                    self.state = ST_HALTED;
                }
                _ => self.bump_wedge(io),
            },
            _ => {}
        }
    }

    fn bump_wedge(&mut self, io: &mut impl FeedIo) {
        self.wedge_ctr = self.wedge_ctr.wrapping_add(1);
        if self.wedge_ctr >= WEDGE_BUDGET {
            // The source cannot serve within surface bounds. Spelled
            // terminal condition, distinct from STALLED (the sink is
            // fine).
            self.state = ST_WEDGED;
        }
        // Every wedge tick is logged with its phase, packed as
        // phase*1000 + count: a wedging pump must be diagnosable from
        // the boot log alone.
        io.log(
            b"wdge",
            u64::from(self.phase) * 1000 + u64::from(self.wedge_ctr % 1000),
        );
    }

    // ── Retention claim ──────────────────────────────────────────────

    fn publish_claim(&mut self, io: &mut impl FeedIo, now_ms: u64) {
        self.claim_seq = self.claim_seq.wrapping_add(1);
        let claim = RetentionClaim {
            kpg_id: 0,
            source: CLAIM_SOURCE_WATCH,
            claim_id: self.feed_id as u64,
            floor_revision: self.ck_durable_revision,
            // The budget IS the expiry: when the pump dies, the claim
            // goes stale on its own and the GC side stops honouring
            // it — release without cooperation, enforced by
            // ClaimTable's existing expiry fencing.
            expiry_unix_ms: now_ms.wrapping_add(self.retention_budget_ms as u64),
            seq: self.claim_seq,
        };
        let mut buf = [0u8; 3 + GC_CLAIM_WIRE_LEN];
        buf[0] = MSG_RETENTION_CLAIM;
        buf[1] = GC_CLAIM_WIRE_LEN as u8;
        buf[2] = 0;
        if claim.encode(&mut buf[3..]).is_some() {
            let _ = io.send_claim(&mut buf);
        }
    }

    // ── step ─────────────────────────────────────────────────────────

    /// One scheduler step's worth of progress. The shell calls this
    /// after draining acks and the KV response into `on_sink_ack` /
    /// `on_kv_response`.
    pub fn step(&mut self, io: &mut impl FeedIo, now_ms: u64) {
        if self.config_bad || self.state == ST_HALTED || self.state == ST_WEDGED {
            return;
        }

        // A response overdue past the timeout re-opens the phase. The
        // request (or its reply) was lost to backpressure somewhere on
        // the pair — rare, but a pump that waits forever on it is a
        // wedged feed with a healthy store, which must not exist.
        if self.phase != P_IDLE && now_ms.wrapping_sub(self.phase_since_ms) >= PHASE_TIMEOUT_MS {
            io.log(b"tmot", u64::from(self.phase));
            self.phase = P_IDLE;
            // Window/backfill provider cursors survive: the bounds are
            // frozen, so re-reading the same page is an at-least-once
            // duplicate, never a gap. An interrupted checkpoint pair
            // restarts from the gen write.
        }

        // Progress, one action per step, only when no KV request is
        // outstanding.
        if self.phase == P_IDLE {
            match self.state {
                ST_CREATED => {
                    if self.resume_retry_at_ms == RESUME_ARM_PENDING {
                        // Refusal seen this step: arm the cadence.
                        self.resume_retry_at_ms = now_ms.wrapping_add(250);
                    } else if self.resume_retry_at_ms == 0 || now_ms >= self.resume_retry_at_ms {
                        self.resume_retry_at_ms = 0;
                        let _ = self.issue_ptr_read(io, now_ms);
                    }
                }
                ST_NEEDS_BACKFILL => {
                    // on_lapse: backfill. Re-latch through a fresh
                    // pointer read — it re-adopts the live CAS witness
                    // (kept intact for the next checkpoint) and the
                    // `force_backfill` flag makes the response START a
                    // backfill at that head instead of resuming the
                    // compacted generation cursor.
                    self.force_backfill = true;
                    self.state = ST_CREATED;
                    let _ = self.issue_ptr_read(io, now_ms);
                }
                ST_BACKFILL | ST_STREAMING | ST_STALLED => {
                    // Checkpoint cadence first: it is the only durable
                    // progress and must not be starved by a busy feed.
                    let ck_due = (self.acked_since_ck >= self.checkpoint_interval_events
                        || now_ms.wrapping_sub(self.last_ck_ms)
                            >= self.checkpoint_interval_ms as u64)
                        && (self.ck_revision > self.ck_durable_revision
                            || self.ck_state != self.ck_durable_state);
                    if ck_due {
                        self.last_ck_ms = now_ms;
                        let _ = self.issue_gen_write(io, now_ms);
                    } else if self.link_up && !self.ring_full() {
                        self.blocked_since_ms = 0;
                        if self.state == ST_STALLED {
                            self.state = if self.ck_state == ST_BACKFILL {
                                ST_BACKFILL
                            } else {
                                ST_STREAMING
                            };
                        }
                        if self.backfill_marker_owed {
                            let mut sc = [0u8; 8];
                            let mut ev = resolved_event(self.feed_id as u64, 0, &mut sc);
                            ev.kind = cdc_wire::CDC_KIND_BACKFILL_COMPLETE;
                            ev.commit_ts = self.cur_timestamp;
                            ev.value = &[];
                            // The marker's fold IS the durable
                            // BACKFILL -> STREAMING flip.
                            let at = self.cur_revision;
                            if self.publish_event(io, &ev, true, at, 0, ST_STREAMING) {
                                self.backfill_marker_owed = false;
                            }
                        } else if self.state == ST_BACKFILL {
                            let _ = self.issue_backfill_page(io, now_ms);
                        } else {
                            // Open a window when none is open; else
                            // read the next page of the open one. When
                            // caught up, poll at a coarse cadence
                            // instead of every step.
                            if self.window_hi == 0 {
                                if !self.caught_up || now_ms.wrapping_sub(self.last_poll_ms) > 100 {
                                    self.last_poll_ms = now_ms;
                                    let _ = self.issue_open_window(io, now_ms);
                                }
                            } else {
                                let _ = self.issue_feed_page(io, now_ms);
                            }
                        }
                    } else {
                        // Sink blocked (link down or ring full):
                        // STALLED after the grace period, loudly.
                        if self.blocked_since_ms == 0 {
                            self.blocked_since_ms = now_ms;
                        } else if now_ms.wrapping_sub(self.blocked_since_ms)
                            >= self.stall_grace_ms as u64
                            && (self.state == ST_STREAMING || self.state == ST_BACKFILL)
                        {
                            self.state = ST_STALLED;
                        }
                    }
                }
                _ => {}
            }
        }

        // Resolved watermark cadence. The frontier emitted is
        // `safe_frontier` — latched only at a caught-up window close —
        // never the live fence frontier, which can cover feed-range
        // writes still unread (any response tail moves it). With the
        // ring empty on top, "resolved(T)" is truthful: nothing at or
        // below T remains unpublished.
        if self.state == ST_STREAMING
            && self.caught_up
            && self.ring_len() == 0
            && self.link_up
            && self.safe_frontier > self.last_resolved_sent
            && now_ms.wrapping_sub(self.last_resolved_ms) >= self.resolved_interval_ms as u64
        {
            let mut scratch8 = [0u8; 8];
            let frontier = self.safe_frontier;
            let ev = resolved_event(self.feed_id as u64, frontier, &mut scratch8);
            if self.publish_event(io, &ev, true, 0, 0, ST_STREAMING) {
                self.last_resolved_sent = frontier;
                self.last_resolved_ms = now_ms;
                self.m_resolved = self.m_resolved.wrapping_add(1);
            }
        }

        // Retention claim cadence.
        if now_ms.wrapping_sub(self.last_claim_ms) >= self.claim_interval_ms as u64 {
            self.last_claim_ms = now_ms;
            self.publish_claim(io, now_ms);
        }
    }
}

/// The op body region of `scratch` (after envelope + request head).
const BODY_AT: usize = wire::ENVELOPE_HDR + 18;

/// Generation record payload:
/// `[state:u8][scan_cursor:u64][FeedCursor::WIRE_LEN bytes]`.
pub const GEN_RECORD_LEN: usize = 1 + 8 + db_ops::FeedCursor::WIRE_LEN;
