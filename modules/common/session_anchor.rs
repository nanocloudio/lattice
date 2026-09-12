//! session_anchor — the anchor half of SessionCtrlV1 for Lattice's
//! edge anchors (`etcd_edge_anchor`, `redis_edge_anchor`).
//!
//! An anchor owns the client-visible transport. This core owns the
//! sessions it fronts: their identity (minted here, at
//! `FIRST_EPOCH`), the worker each is bound to, the handoff state
//! machine that moves a session from one worker to another without
//! the client noticing, the delivery counters the cursor gate compares
//! against, the per-session ingress hold that keeps the anchor's side
//! of the cursor obligation, and the bridge to the session directory.
//!
//! Pure logic: frames in, frames out through a sink, and an
//! [`AnchorAction`] telling the module what to do with its own
//! transport state. The module never inspects a SessionCtrlV1 frame.
//!
//! ## Handoff
//!
//! ```text
//!   DRAIN → (worker) DRAINED → EXPORT_BEGIN ─cursor gate─┐
//!     relay BEGIN/CHUNK/END to standby                   │ mismatch
//!   (standby) IMPORT_BEGIN … IMPORT_END ok               │
//!   [directory EPOCH_BUMP → confirmed]                   ▼
//!   RESUME(epoch+1) → (standby) RESUMED → swap        RESUME(epoch)
//!   DETACH(old)                                       → (worker) RESUMED
//! ```
//!
//! A refused handoff commits nothing: the exporting worker is told to
//! RESUME at the session's *current* epoch and carries on with the
//! state it still holds; the standby is DETACHed and discards its
//! partial blob. Refusal is the safe direction and is what a cursor
//! mismatch, a corrupt transfer, a directory rejection, or a deadline
//! produces.
//!
//! ## Relocation
//!
//! A whole-worker move ([`SessionAnchor::relocate`]) hands every
//! session on the source worker to the target, one at a time under
//! its own epoch, then makes the target the worker new sessions
//! attach to. A refused session stays where it was and the move
//! carries on with the next; the module learns of each outcome.
//!
//! ## Directory
//!
//! When a `session.directory` is wired, the anchor speaks the contract's
//! directory verbs to it: ATTACH for each session when it is minted,
//! ATTACH again at the next epoch naming the standby before a handoff
//! commits — the authoritative rebind — and DETACH at teardown. The
//! directory answers after its own commit on the contract's frames, each
//! naming its session. A directory refusal refuses the handoff.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules; each consumer uses a subset"
)]

#[path = "session_core.rs"]
#[allow(
    clippy::duplicate_mod,
    reason = "dual-target core mounted per consumer; each uses a subset"
)]
pub mod session_core;

#[path = "session_handoff.rs"]
#[allow(
    clippy::duplicate_mod,
    reason = "platform core mounted per consumer; each uses a subset"
)]
pub mod session_handoff;

use session_core::session_ctrl as sc;
use session_core::{mint_session_id, next_epoch, AnchorId, SessionBinding, SessionId, WorkerId};
use session_handoff::{cursors_admit, SessionCursors, HANDOFF_OK};

// ── Capacities ────────────────────────────────────────────────────────

/// Workers an anchor is wired to: the active one and a standby.
pub const WORKERS: usize = 2;
/// No worker.
pub const NO_WORKER_IDX: u8 = 0xFF;

/// Per-session ingress hold: data-plane envelopes that arrived while
/// the session was not in service (attaching, or mid-handoff). Sized
/// for a burst of keepalives or a couple of watch control envelopes.
pub const HOLD_BYTES: usize = 192;

/// Ticks a handoff phase may wait before the anchor refuses it.
pub const HANDOFF_DEADLINE_TICKS: u32 = 2000;
/// `deadline_ms` carried on DRAIN.
pub const DRAIN_DEADLINE_MS: u32 = 1000;

/// Frame-building scratch: largest relayed frame is an EXPORT_CHUNK.
const FRAME_SCRATCH: usize = sc::SESSION_HEADER + 4 + 512;

// ── Phases ────────────────────────────────────────────────────────────

pub const APHASE_ATTACH_WAIT: u8 = 0;
pub const APHASE_ACTIVE: u8 = 1;
pub const APHASE_DRAIN_WAIT: u8 = 2;
pub const APHASE_IMPORT_WAIT: u8 = 3;
pub const APHASE_DIR_WAIT: u8 = 4;
pub const APHASE_RESUME_WAIT: u8 = 5;
pub const APHASE_REFUSE_WAIT: u8 = 6;
pub const APHASE_DETACH_WAIT: u8 = 7;

// ── Directory bridge ──────────────────────────────────────────────────
//
// The anchor speaks the contract's directory verbs (`session_ctrl`):
// ATTACH when a session is minted, ATTACH at the next epoch naming the
// standby before a handoff commits, DETACH at teardown. The directory
// answers after its own commit, on the contract's frames — ATTACHED,
// EPOCH_CONFIRMED, DETACHED, ERROR — each naming the session, which is
// how a reply finds its session here. The grant that follows a binding
// is the transport's and is not read.

/// Where a frame goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Worker(u8),
    Directory,
}

// ── Session ───────────────────────────────────────────────────────────

#[repr(C)]
#[derive(Clone, Copy)]
pub struct AnchorSession {
    pub binding: SessionBinding,
    pub in_use: bool,
    /// Module-defined class byte (watch / lease / subscriber).
    pub class: u8,
    /// The protocol handle the session was minted from.
    pub app_id: u64,
    /// Data-plane address on this anchor: connection slot and stream.
    pub slot: u16,
    pub stream: u32,
    /// Worker currently serving the session.
    pub worker: u8,
    /// Standby during a handoff, else `NO_WORKER_IDX`.
    pub standby: u8,
    /// Old worker awaiting DETACHED after a swap, else `NO_WORKER_IDX`.
    pub retiring: u8,
    pub phase: u8,
    /// Envelopes forwarded to the serving worker for this session.
    pub forwarded: u64,
    /// Envelopes relayed from the worker to the client for this session.
    pub relayed: u64,
    /// Ticks left in the current wait; 0 = no deadline armed.
    pub deadline: u32,
    /// Outstanding directory request, 0 = none.
    pub dir_req: u64,
    /// Detach reason to report once DETACHED lands.
    pub detach_reason: u8,
    /// Set once a relocation tried (and failed) to start this session's
    /// handoff, so the driver moves on; cleared when the move ends.
    reloc_skip: bool,
    hold_len: u16,
    hold: [u8; HOLD_BYTES],
}

impl AnchorSession {
    pub const fn free() -> Self {
        Self {
            binding: SessionBinding::empty(),
            in_use: false,
            class: 0,
            app_id: 0,
            slot: 0,
            stream: 0,
            worker: NO_WORKER_IDX,
            standby: NO_WORKER_IDX,
            retiring: NO_WORKER_IDX,
            phase: APHASE_ATTACH_WAIT,
            forwarded: 0,
            relayed: 0,
            deadline: 0,
            dir_req: 0,
            detach_reason: sc::DETACH_NORMAL,
            reloc_skip: false,
            hold_len: 0,
            hold: [0; HOLD_BYTES],
        }
    }

    pub fn session_id(&self) -> &SessionId {
        &self.binding.session_id
    }

    pub fn epoch(&self) -> u32 {
        self.binding.session_epoch
    }

    /// True iff data-plane envelopes flow straight through.
    pub fn in_service(&self) -> bool {
        self.in_use && self.phase == APHASE_ACTIVE
    }

    fn push_hold(&mut self, msg_type: u8, payload: &[u8]) -> bool {
        let need = 3 + payload.len();
        let at = self.hold_len as usize;
        if payload.len() > u16::MAX as usize || at + need > HOLD_BYTES {
            return false;
        }
        self.hold[at] = msg_type;
        self.hold[at + 1..at + 3].copy_from_slice(&(payload.len() as u16).to_le_bytes());
        self.hold[at + 3..at + need].copy_from_slice(payload);
        self.hold_len = (at + need) as u16;
        true
    }
}

// ── Actions ───────────────────────────────────────────────────────────

/// What the module must do with its transport state after an event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnchorAction {
    None,
    /// The session is in service; held ingress was forwarded.
    Attached(usize),
    /// The worker (or directory) refused the attach; the session is
    /// gone and the module closes the client stream.
    AttachFailed(usize, u8),
    /// The session now lives on `worker`; held ingress was forwarded.
    Swapped(usize, u8),
    /// A handoff was refused (`status`); the session stays on its
    /// worker and held ingress was forwarded there.
    Refused(usize, u8),
    /// The session is gone after a detach the module asked for.
    Detached(usize),
    /// The session is unrecoverable (its worker errored out of a
    /// refusal); the module closes the client stream.
    Lost(usize),
    /// A whole-worker relocation finished; `active` is the worker new
    /// sessions attach to. `moved` / `stayed` count the outcomes.
    RelocationDone(u8, u16, u16),
}

// ── Anchor ────────────────────────────────────────────────────────────

#[repr(C)]
pub struct SessionAnchor<const N: usize> {
    pub anchor_id: AnchorId,
    /// Worker ids learned from HELLO_ACK; zeros until then.
    pub workers: [WorkerId; WORKERS],
    pub worker_known: [bool; WORKERS],
    /// Whether a worker port is wired at all.
    pub worker_wired: [bool; WORKERS],
    /// Worker new sessions attach to.
    pub active_worker: u8,
    pub dir_wired: bool,
    pub sessions: [AnchorSession; N],
    /// Relocation target, or `NO_WORKER_IDX` when none is in progress.
    relocate_to: u8,
    relocate_moved: u16,
    relocate_stayed: u16,
    dir_seq: u64,
    scratch: [u8; FRAME_SCRATCH],
}

impl<const N: usize> SessionAnchor<N> {
    pub const fn new(anchor_id: AnchorId) -> Self {
        Self {
            anchor_id,
            workers: [[0; 8]; WORKERS],
            worker_known: [false; WORKERS],
            worker_wired: [false; WORKERS],
            active_worker: 0,
            dir_wired: false,
            sessions: [AnchorSession::free(); N],
            relocate_to: NO_WORKER_IDX,
            relocate_moved: 0,
            relocate_stayed: 0,
            dir_seq: 0,
            scratch: [0; FRAME_SCRATCH],
        }
    }

    pub fn init(&mut self, anchor_id: AnchorId, worker_wired: [bool; WORKERS], dir_wired: bool) {
        self.anchor_id = anchor_id;
        self.workers = [[0; 8]; WORKERS];
        self.worker_known = [false; WORKERS];
        self.worker_wired = worker_wired;
        self.active_worker = 0;
        self.dir_wired = dir_wired;
        let mut i = 0;
        while i < N {
            self.sessions[i] = AnchorSession::free();
            i += 1;
        }
        self.relocate_to = NO_WORKER_IDX;
        self.relocate_moved = 0;
        self.relocate_stayed = 0;
        self.dir_seq = 0;
    }

    pub fn find(&self, session_id: &SessionId) -> Option<usize> {
        self.sessions
            .iter()
            .position(|s| s.in_use && s.binding.is(session_id))
    }

    /// The session minted from `(class, app_id)`, if any.
    pub fn find_app(&self, class: u8, app_id: u64) -> Option<usize> {
        self.sessions
            .iter()
            .position(|s| s.in_use && s.class == class && s.app_id == app_id)
    }

    pub fn relocating(&self) -> bool {
        self.relocate_to != NO_WORKER_IDX
    }

    pub fn len(&self) -> usize {
        self.sessions.iter().filter(|s| s.in_use).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn find_free(&self) -> Option<usize> {
        self.sessions.iter().position(|s| !s.in_use)
    }

    // ── frame building ────────────────────────────────────────────

    fn send_session(
        &mut self,
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
        to: Target,
        msg: u8,
        sid: &SessionId,
        epoch: u32,
    ) -> bool {
        let n = sc::put_session_header(&mut self.scratch, sid, epoch);
        sink(to, msg, &self.scratch[..n])
    }

    fn send_status(
        &mut self,
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
        to: Target,
        msg: u8,
        sid: &SessionId,
        epoch: u32,
        status: u8,
    ) -> bool {
        sc::put_session_header(&mut self.scratch, sid, epoch);
        let n = sc::put_status(&mut self.scratch, status);
        sink(to, msg, &self.scratch[..n])
    }

    fn send_resume(
        &mut self,
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
        worker: u8,
        sid: &SessionId,
        new_epoch: u32,
    ) -> bool {
        self.send_session(
            sink,
            Target::Worker(worker),
            sc::CMD_SC_RESUME,
            sid,
            new_epoch,
        )
    }

    fn send_detach(
        &mut self,
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
        worker: u8,
        sid: &SessionId,
        epoch: u32,
        reason: u8,
    ) -> bool {
        self.send_status(
            sink,
            Target::Worker(worker),
            sc::CMD_SC_DETACH,
            sid,
            epoch,
            reason,
        )
    }

    fn next_dir_req(&mut self) -> u64 {
        self.dir_seq = self.dir_seq.wrapping_add(1);
        if self.dir_seq == 0 {
            self.dir_seq = 1;
        }
        self.dir_seq
    }

    fn dir_bind(&mut self, idx: usize, sink: &mut impl FnMut(Target, u8, &[u8]) -> bool) {
        if !self.dir_wired {
            return;
        }
        let req = self.next_dir_req();
        let s = self.sessions[idx];
        let worker = self.workers[s.worker as usize];
        // ATTACH [sid:16][anchor_id:8][epoch:4][cc:1][worker_id:8]
        let b = &mut self.scratch;
        b[0..16].copy_from_slice(s.session_id());
        b[16..24].copy_from_slice(&self.anchor_id);
        b[24..28].copy_from_slice(&s.epoch().to_le_bytes());
        b[28] = sc::CC_EDGE_ANCHORED;
        b[29..37].copy_from_slice(&worker);
        if sink(
            Target::Directory,
            sc::CMD_SC_ATTACH,
            &self.scratch[..sc::ATTACH_PAYLOAD_LEN],
        ) {
            self.sessions[idx].dir_req = req;
        }
    }

    /// The swap's authoritative rebind: ATTACH at `new_epoch` naming the
    /// standby, which the directory answers with a verdict and, on
    /// acceptance, a grant under the new generation.
    fn dir_rebind(
        &mut self,
        idx: usize,
        new_epoch: u32,
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
    ) -> bool {
        let req = self.next_dir_req();
        let s = self.sessions[idx];
        let worker = self.workers[s.standby as usize];
        let b = &mut self.scratch;
        b[0..16].copy_from_slice(s.session_id());
        b[16..24].copy_from_slice(&self.anchor_id);
        b[24..28].copy_from_slice(&new_epoch.to_le_bytes());
        b[28] = sc::CC_EDGE_ANCHORED;
        b[29..37].copy_from_slice(&worker);
        if sink(
            Target::Directory,
            sc::CMD_SC_ATTACH,
            &self.scratch[..sc::ATTACH_PAYLOAD_LEN],
        ) {
            self.sessions[idx].dir_req = req;
            true
        } else {
            false
        }
    }

    fn dir_unbind(&mut self, idx: usize, sink: &mut impl FnMut(Target, u8, &[u8]) -> bool) {
        if !self.dir_wired {
            return;
        }
        let s = self.sessions[idx];
        // DETACH [sid:16][epoch:4][reason:1]
        let b = &mut self.scratch;
        sc::put_session_header(b, s.session_id(), s.epoch());
        b[20] = sc::DETACH_NORMAL;
        let _ = sink(
            Target::Directory,
            sc::CMD_SC_DETACH,
            &self.scratch[..sc::DETACH_PAYLOAD_LEN],
        );
    }

    // ── lifecycle ─────────────────────────────────────────────────

    /// Say HELLO to every wired worker. Call once the channels exist.
    pub fn hello(&mut self, sink: &mut impl FnMut(Target, u8, &[u8]) -> bool) {
        let mut hello = [0u8; 1 + sc::ANCHOR_ID_BYTES + 1];
        hello[0] = sc::ROLE_ANCHOR;
        hello[1..9].copy_from_slice(&self.anchor_id);
        for w in 0..WORKERS {
            if self.worker_wired[w] {
                let _ = sink(Target::Worker(w as u8), sc::CMD_SC_HELLO, &hello);
            }
        }
    }

    /// Mint and attach a session for `(class, app_id)` at the data-plane
    /// address `(slot, stream)`. The session attaches to the active
    /// worker; until ATTACHED lands, [`SessionAnchor::forward`] holds
    /// its ingress. `Err(())` when the table is full, no worker is
    /// wired, or the identity is already in use.
    pub fn attach(
        &mut self,
        class: u8,
        app_id: u64,
        slot: u16,
        stream: u32,
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
    ) -> Result<usize, ()> {
        let w = self.active_worker as usize;
        if w >= WORKERS || !self.worker_wired[w] {
            return Err(());
        }
        let sid = mint_session_id(&self.anchor_id, app_id);
        if self.find(&sid).is_some() {
            return Err(());
        }
        let idx = self.find_free().ok_or(())?;
        let mut s = AnchorSession::free();
        s.binding = SessionBinding::attach(sid, self.anchor_id);
        s.in_use = true;
        s.class = class;
        s.app_id = app_id;
        s.slot = slot;
        s.stream = stream;
        s.worker = self.active_worker;
        s.phase = APHASE_ATTACH_WAIT;
        s.deadline = HANDOFF_DEADLINE_TICKS;
        self.sessions[idx] = s;

        let hint = if self.worker_known[w] {
            self.workers[w]
        } else {
            session_core::NO_WORKER
        };
        let n = sc::put_attach(
            &mut self.scratch,
            &sid,
            &self.anchor_id,
            session_core::FIRST_EPOCH,
            sc::CC_EDGE_ANCHORED,
            &hint,
        );
        if !sink(
            Target::Worker(self.active_worker),
            sc::CMD_SC_ATTACH,
            &self.scratch[..n],
        ) {
            self.sessions[idx] = AnchorSession::free();
            return Err(());
        }
        self.dir_bind(idx, sink);
        Ok(idx)
    }

    /// Forward a data-plane envelope for session `idx` to its worker,
    /// or hold it while the session is not in service. The envelope
    /// must begin with the session header (`[session_id:16][epoch:4]`);
    /// the anchor stamps the session's *current* epoch into it at the
    /// moment it is written, so an envelope held across a handoff goes
    /// out under the generation the new worker admits. Counts it as
    /// forwarded when written. `false` means neither happened (hold
    /// full, short payload, or the channel refused it) — the module
    /// decides what that costs the client.
    pub fn forward(
        &mut self,
        idx: usize,
        msg_type: u8,
        payload: &[u8],
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
    ) -> bool {
        if idx >= N || !self.sessions[idx].in_use || payload.len() < sc::SESSION_HEADER {
            return false;
        }
        if self.sessions[idx].phase != APHASE_ACTIVE {
            return self.sessions[idx].push_hold(msg_type, payload);
        }
        self.write_stamped(idx, msg_type, payload, sink)
    }

    /// Write `payload` to session `idx`'s worker with the current
    /// epoch stamped into its session header.
    fn write_stamped(
        &mut self,
        idx: usize,
        msg_type: u8,
        payload: &[u8],
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
    ) -> bool {
        if payload.len() > FRAME_SCRATCH {
            return false;
        }
        let s = &self.sessions[idx];
        let (w, sid, epoch) = (s.worker, *s.session_id(), s.epoch());
        self.scratch[..payload.len()].copy_from_slice(payload);
        sc::put_session_header(&mut self.scratch, &sid, epoch);
        if sink(Target::Worker(w), msg_type, &self.scratch[..payload.len()]) {
            self.sessions[idx].forwarded = self.sessions[idx].forwarded.wrapping_add(1);
            true
        } else {
            false
        }
    }

    /// Count one envelope relayed from the worker to the client for
    /// session `idx`. Call it for every worker-originated envelope the
    /// module consumes for the session, whether or not the client
    /// stream is still open — the worker counted it as produced.
    pub fn relayed(&mut self, idx: usize) {
        if idx < N && self.sessions[idx].in_use {
            self.sessions[idx].relayed = self.sessions[idx].relayed.wrapping_add(1);
        }
    }

    /// The worker serving session `idx`, for routing a worker-originated
    /// reply back to a stream by session.
    pub fn worker_of(&self, idx: usize) -> u8 {
        self.sessions[idx].worker
    }

    fn flush_hold(&mut self, idx: usize, sink: &mut impl FnMut(Target, u8, &[u8]) -> bool) {
        let len = self.sessions[idx].hold_len as usize;
        let mut at = 0usize;
        while at + 3 <= len {
            let mt = self.sessions[idx].hold[at];
            let l = u16::from_le_bytes([
                self.sessions[idx].hold[at + 1],
                self.sessions[idx].hold[at + 2],
            ]) as usize;
            if at + 3 + l > len {
                break;
            }
            let mut tmp = [0u8; HOLD_BYTES];
            tmp[..l].copy_from_slice(&self.sessions[idx].hold[at + 3..at + 3 + l]);
            let _ = self.write_stamped(idx, mt, &tmp[..l], sink);
            at += 3 + l;
        }
        self.sessions[idx].hold_len = 0;
    }

    /// Detach session `idx` with `reason`. The session is reported
    /// gone (`AnchorAction::Detached`) when DETACHED lands or the
    /// deadline passes.
    pub fn detach(
        &mut self,
        idx: usize,
        reason: u8,
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
    ) {
        if idx >= N || !self.sessions[idx].in_use {
            return;
        }
        let s = self.sessions[idx];
        let sid = *s.session_id();
        let epoch = s.epoch();
        if s.standby != NO_WORKER_IDX {
            self.send_detach(sink, s.standby, &sid, epoch, reason);
        }
        if s.retiring != NO_WORKER_IDX {
            self.send_detach(sink, s.retiring, &sid, epoch, reason);
        }
        self.send_detach(sink, s.worker, &sid, epoch, reason);
        self.dir_unbind(idx, sink);
        self.sessions[idx].phase = APHASE_DETACH_WAIT;
        self.sessions[idx].standby = NO_WORKER_IDX;
        self.sessions[idx].retiring = NO_WORKER_IDX;
        self.sessions[idx].detach_reason = reason;
        self.sessions[idx].deadline = HANDOFF_DEADLINE_TICKS;
        self.sessions[idx].hold_len = 0;
    }

    /// Begin moving every session on the other worker to `target`.
    /// `false` if `target` is not wired or a relocation is in progress.
    pub fn relocate(&mut self, target: u8) -> bool {
        let t = target as usize;
        if t >= WORKERS || !self.worker_wired[t] || self.relocating() {
            return false;
        }
        self.relocate_to = target;
        self.relocate_moved = 0;
        self.relocate_stayed = 0;
        true
    }

    /// Start a handoff of session `idx` to `standby`. The session must
    /// be in service.
    pub fn begin_handoff(
        &mut self,
        idx: usize,
        standby: u8,
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
    ) -> bool {
        if idx >= N || !self.sessions[idx].in_service() {
            return false;
        }
        let st = standby as usize;
        if st >= WORKERS || !self.worker_wired[st] || standby == self.sessions[idx].worker {
            return false;
        }
        let s = self.sessions[idx];
        let sid = *s.session_id();
        let epoch = s.epoch();
        sc::put_session_header(&mut self.scratch, &sid, epoch);
        let n = sc::put_u32_after_header(&mut self.scratch, DRAIN_DEADLINE_MS);
        if !sink(
            Target::Worker(s.worker),
            sc::CMD_SC_DRAIN,
            &self.scratch[..n],
        ) {
            return false;
        }
        self.sessions[idx].standby = standby;
        self.sessions[idx].phase = APHASE_DRAIN_WAIT;
        self.sessions[idx].deadline = HANDOFF_DEADLINE_TICKS;
        true
    }

    /// Refuse the handoff in progress on `idx`: the standby discards,
    /// the exporter is returned to service at the current epoch.
    fn refuse(
        &mut self,
        idx: usize,
        status: u8,
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
    ) -> AnchorAction {
        let s = self.sessions[idx];
        let sid = *s.session_id();
        let epoch = s.epoch();
        if s.standby != NO_WORKER_IDX {
            self.send_detach(sink, s.standby, &sid, epoch, sc::DETACH_ERROR);
        }
        self.sessions[idx].standby = NO_WORKER_IDX;
        self.sessions[idx].detach_reason = status;
        if s.phase == APHASE_DRAIN_WAIT {
            // The worker never declared DRAINED (or we never saw it):
            // it may be draining still, so RESUME at the current epoch
            // returns it to service either way.
        }
        self.send_resume(sink, s.worker, &sid, epoch);
        self.sessions[idx].phase = APHASE_REFUSE_WAIT;
        self.sessions[idx].deadline = HANDOFF_DEADLINE_TICKS;
        if self.relocating() {
            self.sessions[idx].reloc_skip = true;
            self.relocate_stayed = self.relocate_stayed.wrapping_add(1);
        }
        AnchorAction::None
    }

    /// One tick: age deadlines and drive a relocation. Returns at most
    /// one action.
    pub fn step(&mut self, sink: &mut impl FnMut(Target, u8, &[u8]) -> bool) -> AnchorAction {
        // Deadlines.
        for idx in 0..N {
            if !self.sessions[idx].in_use || self.sessions[idx].deadline == 0 {
                continue;
            }
            self.sessions[idx].deadline -= 1;
            if self.sessions[idx].deadline != 0 {
                continue;
            }
            match self.sessions[idx].phase {
                APHASE_ATTACH_WAIT => {
                    let reason = sc::STATUS_NOT_READY;
                    self.sessions[idx] = AnchorSession::free();
                    return AnchorAction::AttachFailed(idx, reason);
                }
                APHASE_DRAIN_WAIT | APHASE_IMPORT_WAIT | APHASE_DIR_WAIT | APHASE_RESUME_WAIT => {
                    return self.refuse(idx, sc::DETACH_DRAIN_TIMEOUT, sink);
                }
                APHASE_REFUSE_WAIT => {
                    // The exporter never came back: the session is lost.
                    self.sessions[idx] = AnchorSession::free();
                    return AnchorAction::Lost(idx);
                }
                APHASE_DETACH_WAIT => {
                    self.sessions[idx] = AnchorSession::free();
                    return AnchorAction::Detached(idx);
                }
                _ => {}
            }
        }

        // Relocation: one session in flight at a time.
        if self.relocating() {
            let target = self.relocate_to;
            let source = 1 - target;
            let busy = self
                .sessions
                .iter()
                .any(|s| s.in_use && s.standby != NO_WORKER_IDX);
            if !busy {
                let next = self.sessions.iter().position(|s| {
                    s.in_service()
                        && s.worker == source
                        && s.retiring == NO_WORKER_IDX
                        && !s.reloc_skip
                });
                match next {
                    Some(idx) => {
                        if !self.begin_handoff(idx, target, sink) {
                            // Could not even ask: count it as stayed and
                            // move on; the module sees the tally.
                            self.sessions[idx].reloc_skip = true;
                            self.relocate_stayed = self.relocate_stayed.wrapping_add(1);
                        }
                    }
                    None => {
                        // Nothing left on the source (sessions still
                        // attaching or detaching are not moved).
                        let pending = self.sessions.iter().any(|s| {
                            s.in_use
                                && s.worker == source
                                && matches!(s.phase, APHASE_REFUSE_WAIT | APHASE_RESUME_WAIT)
                        });
                        if !pending {
                            for s in self.sessions.iter_mut() {
                                s.reloc_skip = false;
                            }
                            self.relocate_to = NO_WORKER_IDX;
                            self.active_worker = target;
                            return AnchorAction::RelocationDone(
                                target,
                                self.relocate_moved,
                                self.relocate_stayed,
                            );
                        }
                    }
                }
            }
        }
        AnchorAction::None
    }

    // ── frames from workers ───────────────────────────────────────

    /// Handle one SessionCtrlV1 frame from worker `from`.
    pub fn on_frame(
        &mut self,
        from: u8,
        msg_type: u8,
        payload: &[u8],
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
    ) -> AnchorAction {
        if msg_type == sc::MSG_SC_HELLO_ACK {
            // [role:1][peer_id:8]
            if payload.len() > sc::WORKER_ID_BYTES
                && payload[0] == sc::ROLE_WORKER
                && (from as usize) < WORKERS
            {
                self.workers[from as usize].copy_from_slice(&payload[1..9]);
                self.worker_known[from as usize] = true;
            }
            return AnchorAction::None;
        }
        if payload.len() < sc::SESSION_HEADER {
            return AnchorAction::None;
        }
        let mut sid: SessionId = [0; 16];
        sid.copy_from_slice(sc::session_id(payload));
        let Some(idx) = self.find(&sid) else {
            return AnchorAction::None;
        };
        let s = self.sessions[idx];
        let status = sc::status(payload);
        let epoch = sc::epoch(payload);
        let from_worker = from == s.worker;
        let from_standby = s.standby != NO_WORKER_IDX && from == s.standby;

        match msg_type {
            sc::MSG_SC_ATTACHED if s.phase == APHASE_ATTACH_WAIT && from_worker => {
                if status == sc::STATUS_OK {
                    self.sessions[idx].phase = APHASE_ACTIVE;
                    self.sessions[idx].deadline = 0;
                    self.flush_hold(idx, sink);
                    AnchorAction::Attached(idx)
                } else {
                    self.dir_unbind(idx, sink);
                    self.sessions[idx] = AnchorSession::free();
                    AnchorAction::AttachFailed(idx, status)
                }
            }
            sc::MSG_SC_DRAINED if s.phase == APHASE_DRAIN_WAIT && from_worker => {
                self.sessions[idx].phase = APHASE_IMPORT_WAIT;
                self.sessions[idx].deadline = HANDOFF_DEADLINE_TICKS;
                AnchorAction::None
            }
            sc::CMD_SC_EXPORT_BEGIN if s.phase == APHASE_IMPORT_WAIT && from_worker => {
                let admit = match sc::export_cursors(payload).and_then(SessionCursors::decode) {
                    Some(c) => cursors_admit(&c, s.forwarded, s.relayed),
                    None => sc::STATUS_CURSOR_MISMATCH,
                };
                if admit != HANDOFF_OK {
                    return self.refuse(idx, sc::STATUS_CURSOR_MISMATCH, sink);
                }
                if !sink(Target::Worker(s.standby), msg_type, payload) {
                    return self.refuse(idx, sc::STATUS_NOT_READY, sink);
                }
                AnchorAction::None
            }
            sc::CMD_SC_EXPORT_CHUNK | sc::CMD_SC_EXPORT_END
                if s.phase == APHASE_IMPORT_WAIT && from_worker =>
            {
                if !sink(Target::Worker(s.standby), msg_type, payload) {
                    return self.refuse(idx, sc::STATUS_NOT_READY, sink);
                }
                AnchorAction::None
            }
            sc::MSG_SC_IMPORT_BEGIN if s.phase == APHASE_IMPORT_WAIT && from_standby => {
                if status != sc::STATUS_OK {
                    return self.refuse(idx, status, sink);
                }
                AnchorAction::None
            }
            sc::MSG_SC_IMPORT_END if s.phase == APHASE_IMPORT_WAIT && from_standby => {
                if status != sc::STATUS_OK {
                    return self.refuse(idx, status, sink);
                }
                let new_epoch = next_epoch(s.epoch());
                if self.dir_wired {
                    if self.dir_rebind(idx, new_epoch, sink) {
                        self.sessions[idx].phase = APHASE_DIR_WAIT;
                        self.sessions[idx].deadline = HANDOFF_DEADLINE_TICKS;
                        AnchorAction::None
                    } else {
                        self.refuse(idx, sc::STATUS_NOT_READY, sink)
                    }
                } else {
                    self.commit_resume(idx, new_epoch, sink)
                }
            }
            sc::MSG_SC_RESUMED if s.phase == APHASE_RESUME_WAIT && from_standby => {
                if !self.sessions[idx].binding.adopt_epoch(epoch) {
                    return self.refuse(idx, sc::STATUS_STALE_EPOCH, sink);
                }
                let old = s.worker;
                self.sessions[idx].worker = s.standby;
                self.sessions[idx].standby = NO_WORKER_IDX;
                self.sessions[idx].retiring = old;
                self.sessions[idx].phase = APHASE_ACTIVE;
                self.sessions[idx].deadline = 0;
                self.send_detach(sink, old, &sid, s.epoch(), sc::DETACH_NORMAL);
                self.flush_hold(idx, sink);
                if self.relocating() {
                    self.relocate_moved = self.relocate_moved.wrapping_add(1);
                }
                AnchorAction::Swapped(idx, self.sessions[idx].worker)
            }
            sc::MSG_SC_RESUMED if s.phase == APHASE_REFUSE_WAIT && from_worker => {
                self.sessions[idx].phase = APHASE_ACTIVE;
                self.sessions[idx].deadline = 0;
                self.flush_hold(idx, sink);
                AnchorAction::Refused(idx, s.detach_reason)
            }
            sc::MSG_SC_DETACHED => {
                if s.retiring != NO_WORKER_IDX && from == s.retiring {
                    self.sessions[idx].retiring = NO_WORKER_IDX;
                    return AnchorAction::None;
                }
                if s.phase == APHASE_DETACH_WAIT && from_worker {
                    self.sessions[idx] = AnchorSession::free();
                    return AnchorAction::Detached(idx);
                }
                AnchorAction::None
            }
            sc::MSG_SC_ERROR => {
                if s.phase == APHASE_REFUSE_WAIT && from_worker {
                    // The exporter cannot come back: nothing holds the
                    // session any more.
                    self.dir_unbind(idx, sink);
                    self.sessions[idx] = AnchorSession::free();
                    return AnchorAction::Lost(idx);
                }
                if matches!(
                    s.phase,
                    APHASE_DRAIN_WAIT | APHASE_IMPORT_WAIT | APHASE_DIR_WAIT | APHASE_RESUME_WAIT
                ) && (from_worker || from_standby)
                {
                    return self.refuse(idx, status, sink);
                }
                AnchorAction::None
            }
            _ => AnchorAction::None,
        }
    }

    fn commit_resume(
        &mut self,
        idx: usize,
        new_epoch: u32,
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
    ) -> AnchorAction {
        let s = self.sessions[idx];
        let sid = *s.session_id();
        if !self.send_resume(sink, s.standby, &sid, new_epoch) {
            return self.refuse(idx, sc::STATUS_NOT_READY, sink);
        }
        self.sessions[idx].phase = APHASE_RESUME_WAIT;
        self.sessions[idx].deadline = HANDOFF_DEADLINE_TICKS;
        AnchorAction::None
    }

    /// Handle one frame from the directory: a verdict names its session.
    /// Grants and HELLO_ACK are not verdicts and are not read here.
    pub fn on_dir_reply(
        &mut self,
        msg: u8,
        payload: &[u8],
        sink: &mut impl FnMut(Target, u8, &[u8]) -> bool,
    ) -> AnchorAction {
        if payload.len() < sc::SESSION_HEADER {
            return AnchorAction::None;
        }
        let ok = match msg {
            sc::MSG_SC_ATTACHED | sc::MSG_SC_ERROR => {
                payload.len() > sc::SESSION_HEADER && payload[sc::SESSION_HEADER] == sc::STATUS_OK
            }
            sc::MSG_SC_EPOCH_CONFIRMED | sc::MSG_SC_DETACHED => true,
            _ => return AnchorAction::None,
        };
        let sid = sc::session_id(payload);
        let Some(idx) = self
            .sessions
            .iter()
            .position(|s| s.in_use && s.dir_req != 0 && s.session_id() == sid)
        else {
            return AnchorAction::None;
        };
        self.sessions[idx].dir_req = 0;
        let s = self.sessions[idx];
        match s.phase {
            APHASE_DIR_WAIT => {
                // The rebind's verdict decides the handoff.
                if !ok {
                    return self.refuse(idx, sc::STATUS_STALE_EPOCH, sink);
                }
                let new_epoch = next_epoch(s.epoch());
                self.commit_resume(idx, new_epoch, sink)
            }
            _ => {
                if !ok && msg == sc::MSG_SC_ATTACHED {
                    // The directory refused the binding: the session
                    // cannot be fronted. Tear it down wherever it got to.
                    let sid = *s.session_id();
                    self.send_detach(sink, s.worker, &sid, s.epoch(), sc::DETACH_ERROR);
                    self.sessions[idx] = AnchorSession::free();
                    return AnchorAction::AttachFailed(idx, sc::STATUS_STALE_EPOCH);
                }
                AnchorAction::None
            }
        }
    }
}
