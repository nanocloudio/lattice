//! session_worker — the worker half of SessionCtrlV1 for Lattice's
//! continuity workers (`watch_registry`, `lease_manager`,
//! `pubsub_worker`).
//!
//! A worker owns movable session state. This core owns everything
//! about a session that is *not* the application record: its
//! identity and epoch (`session_core::SessionBinding`), its
//! continuity phase, its delivery cursors, and the chunked
//! export/import transfer. The module keeps the application record
//! and supplies the blob; the two meet through [`WorkerAction`].
//!
//! Pure logic over caller-owned buffers: the module reads a frame off
//! its control channel and hands it to [`SessionWorker::handle_frame`]
//! with a sink that writes reply frames back. Nothing here touches a
//! channel.
//!
//! ## Epoch rules
//!
//! The anchor mints a session at `FIRST_EPOCH` and is the authority
//! for its generation. A worker admits a command only at the session's
//! current epoch; stale and future epochs are refused with
//! `STATUS_STALE_EPOCH`. A `CMD_SC_RESUME` at the current epoch on a
//! drained session is a *refusal* — the handoff was abandoned and the
//! worker returns to service with the state it still holds. A
//! `CMD_SC_RESUME` above the imported epoch commits an import.
//!
//! ## Cursors
//!
//! The unit is whole envelopes on the anchor/worker seam: the module
//! calls [`SessionWorker::consumed`] once per data-plane envelope it
//! accepts for a session and [`SessionWorker::produced`] once per
//! envelope it emits toward the anchor for that session. Both sides
//! count the same thing, which is all the contract asks.

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
use session_core::{AnchorId, SessionBinding, SessionId, WorkerId, NO_WORKER};
use session_handoff::{
    handoff_crc32, HandoffExport, HandoffImport, SessionCursors, CURSOR_PAIR_LEN, HANDOFF_OK,
};

// ── Phases ────────────────────────────────────────────────────────────

/// Bound and in service.
pub const WPHASE_ACTIVE: u8 = 0;
/// `CMD_SC_DRAIN` received; the module declares drained once settled.
pub const WPHASE_DRAINING: u8 = 1;
/// `MSG_SC_DRAINED` sent and state exported; consuming nothing.
pub const WPHASE_DRAINED: u8 = 2;
/// Import in progress (EXPORT_BEGIN accepted, chunks arriving).
pub const WPHASE_IMPORTING: u8 = 3;
/// Import committed (EXPORT_END verified); dormant until RESUME.
pub const WPHASE_IMPORTED: u8 = 4;

/// Largest blob a worker imports. Sized for the pub/sub subscriber
/// record (the largest of the three) with headroom.
pub const IMPORT_BUF_LEN: usize = 2048;

/// Bytes per `CMD_SC_EXPORT_CHUNK`. Fits a channel frame with room
/// for the session header.
pub const EXPORT_CHUNK_LEN: u32 = 512;

/// Frame-building scratch: session header + chunk.
const FRAME_SCRATCH: usize = sc::SESSION_HEADER + 4 + EXPORT_CHUNK_LEN as usize;

// ── Slot ──────────────────────────────────────────────────────────────

#[repr(C)]
#[derive(Clone, Copy)]
pub struct WorkerSlot {
    pub binding: SessionBinding,
    pub phase: u8,
    pub in_use: bool,
    /// Data-plane envelopes consumed for this session.
    pub in_consumed: u64,
    /// Envelopes emitted toward the anchor for this session.
    pub out_produced: u64,
    /// Epoch an import was offered at; adopted on RESUME.
    pub import_epoch: u32,
}

impl WorkerSlot {
    pub const fn free() -> Self {
        Self {
            binding: SessionBinding::empty(),
            phase: WPHASE_ACTIVE,
            in_use: false,
            in_consumed: 0,
            out_produced: 0,
            import_epoch: 0,
        }
    }

    pub fn session_id(&self) -> &SessionId {
        &self.binding.session_id
    }

    pub fn epoch(&self) -> u32 {
        self.binding.session_epoch
    }

    /// True iff the session may consume and produce.
    pub fn in_service(&self) -> bool {
        self.in_use && self.phase == WPHASE_ACTIVE
    }

    pub fn cursors(&self) -> SessionCursors {
        SessionCursors::new(self.in_consumed, self.out_produced)
    }
}

// ── Actions ───────────────────────────────────────────────────────────

/// What the module must do after a frame was handled. Session-scoped
/// actions carry the slot index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerAction {
    /// Nothing for the module.
    None,
    /// A session attached (or re-attached idempotently).
    Attached(usize),
    /// A session detached; the module drops its record. `reason` is
    /// the wire `DETACH_*`.
    Detached(usize, u8),
    /// Drain requested; stop producing for the session. The module
    /// calls [`SessionWorker::declare_drained`] once settled, then
    /// [`SessionWorker::export`] with the blob.
    Drain(usize),
    /// An import completed and is dormant; the module applies the
    /// blob (`import_blob()`) as a dormant record.
    Imported(usize),
    /// `CMD_SC_RESUME` committed the imported session at `new_epoch`;
    /// the module brings its dormant record into service.
    Resumed(usize, u32),
    /// A refused handoff returned the exporting session to service at
    /// its unchanged epoch.
    Reinstated(usize),
    /// The directory advanced the session's epoch (`CMD_SC_EPOCH_BUMP`).
    EpochBumped(usize, u32),
    /// A partial import was discarded (detach or corrupt transfer).
    ImportDiscarded(usize),
}

// ── Worker ────────────────────────────────────────────────────────────

#[repr(C)]
pub struct SessionWorker<const N: usize> {
    pub worker_id: WorkerId,
    /// The anchor that said HELLO, or zeros before it did.
    pub anchor_id: AnchorId,
    pub slots: [WorkerSlot; N],
    /// One import in flight at a time.
    import: HandoffImport,
    import_slot: usize,
    import_buf: [u8; IMPORT_BUF_LEN],
    scratch: [u8; FRAME_SCRATCH],
}

impl<const N: usize> SessionWorker<N> {
    pub const fn new(worker_id: WorkerId) -> Self {
        Self {
            worker_id,
            anchor_id: [0; 8],
            slots: [WorkerSlot::free(); N],
            import: HandoffImport::new(),
            import_slot: usize::MAX,
            import_buf: [0; IMPORT_BUF_LEN],
            scratch: [0; FRAME_SCRATCH],
        }
    }

    pub fn init(&mut self, worker_id: WorkerId) {
        self.worker_id = worker_id;
        self.anchor_id = [0; 8];
        let mut i = 0;
        while i < N {
            self.slots[i] = WorkerSlot::free();
            i += 1;
        }
        self.import.reset();
        self.import_slot = usize::MAX;
    }

    pub fn find(&self, session_id: &SessionId) -> Option<usize> {
        self.slots
            .iter()
            .position(|s| s.in_use && s.binding.is(session_id))
    }

    fn find_free(&self) -> Option<usize> {
        self.slots.iter().position(|s| !s.in_use)
    }

    /// Drop a session without a wire exchange: the module ended it on
    /// its own authority (a refused replay the anchor learns of from
    /// the data plane). A later DETACH for it is answered idempotently.
    pub fn forget(&mut self, idx: usize) {
        if idx < N && self.slots[idx].in_use {
            self.free_slot(idx);
        }
    }

    /// The committed import blob for `WorkerAction::Imported`.
    pub fn import_blob(&self) -> &[u8] {
        let n = self.import.total_len() as usize;
        &self.import_buf[..n.min(IMPORT_BUF_LEN)]
    }

    /// Admit a data-plane envelope for `session_id` at `epoch`: the
    /// slot index when the session is in service at exactly that
    /// epoch, else `None` (stale, future, unknown, or not in service).
    pub fn admit(&self, session_id: &SessionId, epoch: u32) -> Option<usize> {
        let idx = self.find(session_id)?;
        let s = &self.slots[idx];
        if !s.in_service() || s.binding.admit(epoch) != session_core::EpochCheck::Current {
            return None;
        }
        Some(idx)
    }

    /// Count one consumed data-plane envelope for slot `idx`.
    pub fn consumed(&mut self, idx: usize) {
        if idx < N {
            self.slots[idx].in_consumed = self.slots[idx].in_consumed.wrapping_add(1);
        }
    }

    /// Count one produced envelope for slot `idx`.
    pub fn produced(&mut self, idx: usize) {
        if idx < N {
            self.slots[idx].out_produced = self.slots[idx].out_produced.wrapping_add(1);
        }
    }

    // ── frame building ────────────────────────────────────────────

    fn send_session(
        &mut self,
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
        msg: u8,
        sid: &SessionId,
        epoch: u32,
    ) -> bool {
        let n = sc::put_session_header(&mut self.scratch, sid, epoch);
        sink(msg, &self.scratch[..n])
    }

    fn send_status(
        &mut self,
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
        msg: u8,
        sid: &SessionId,
        epoch: u32,
        status: u8,
    ) -> bool {
        sc::put_session_header(&mut self.scratch, sid, epoch);
        let n = sc::put_status(&mut self.scratch, status);
        sink(msg, &self.scratch[..n])
    }

    fn send_error(
        &mut self,
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
        sid: &SessionId,
        epoch: u32,
        status: u8,
    ) -> bool {
        self.send_status(sink, sc::MSG_SC_ERROR, sid, epoch, status)
    }

    fn free_slot(&mut self, idx: usize) {
        self.slots[idx] = WorkerSlot::free();
        if self.import_slot == idx {
            self.import.reset();
            self.import_slot = usize::MAX;
        }
    }

    // ── the contract ──────────────────────────────────────────────

    /// Handle one SessionCtrlV1 frame from the anchor. Replies go to
    /// `sink(msg_type, payload)`; the returned action is the module's
    /// share of the work. Frames outside `0x70..=0x9F` are ignored.
    pub fn handle_frame(
        &mut self,
        msg_type: u8,
        payload: &[u8],
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
    ) -> WorkerAction {
        match msg_type {
            sc::CMD_SC_HELLO => self.on_hello(payload, sink),
            sc::CMD_SC_ATTACH => self.on_attach(payload, sink),
            sc::CMD_SC_DETACH => self.on_detach(payload, sink),
            sc::CMD_SC_DRAIN => self.on_drain(payload, sink),
            sc::CMD_SC_EXPORT_BEGIN => self.on_export_begin(payload, sink),
            sc::CMD_SC_EXPORT_CHUNK => self.on_export_chunk(payload, sink),
            sc::CMD_SC_EXPORT_END => self.on_export_end(payload, sink),
            sc::CMD_SC_RESUME => self.on_resume(payload, sink),
            sc::CMD_SC_EPOCH_BUMP => self.on_epoch_bump(payload, sink),
            sc::CMD_SC_RELOCATE => {
                // Directory → anchor traffic; a worker has no binding
                // to move. Answer so the sender is not left waiting.
                let sid = sid_of(payload);
                let epoch = sc::epoch(payload);
                self.send_status(
                    sink,
                    sc::MSG_SC_RELOCATED,
                    &sid,
                    epoch,
                    sc::STATUS_UNSUPPORTED,
                );
                WorkerAction::None
            }
            _ => WorkerAction::None,
        }
    }

    fn on_hello(
        &mut self,
        payload: &[u8],
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
    ) -> WorkerAction {
        // [role:1][self_id:8][flags:1]
        if payload.len() > sc::ANCHOR_ID_BYTES && payload[0] == sc::ROLE_ANCHOR {
            self.anchor_id
                .copy_from_slice(&payload[1..1 + sc::ANCHOR_ID_BYTES]);
        }
        let mut ack = [0u8; 1 + sc::WORKER_ID_BYTES];
        ack[0] = sc::ROLE_WORKER;
        ack[1..].copy_from_slice(&self.worker_id);
        sink(sc::MSG_SC_HELLO_ACK, &ack);
        WorkerAction::None
    }

    fn on_attach(
        &mut self,
        payload: &[u8],
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
    ) -> WorkerAction {
        if payload.len() < sc::ATTACH_PAYLOAD_LEN {
            return WorkerAction::None;
        }
        let sid = sid_of(payload);
        let mut anchor: AnchorId = [0; 8];
        anchor.copy_from_slice(sc::attach_anchor_id(payload));
        let epoch = sc::attach_epoch(payload);
        let hint_at = sc::SESSION_ID_BYTES + sc::ANCHOR_ID_BYTES + sc::EPOCH_BYTES + 1;
        let hint = &payload[hint_at..hint_at + sc::WORKER_ID_BYTES];
        if hint != NO_WORKER && hint != self.worker_id {
            // Addressed to another worker on a shared channel.
            return WorkerAction::None;
        }
        if let Some(idx) = self.find(&sid) {
            let cur = self.slots[idx].epoch();
            let status = if cur == epoch && self.slots[idx].phase == WPHASE_ACTIVE {
                sc::STATUS_OK // idempotent re-attach after a lost reply
            } else {
                sc::STATUS_STALE_EPOCH
            };
            self.send_status(sink, sc::MSG_SC_ATTACHED, &sid, cur, status);
            return if status == sc::STATUS_OK {
                WorkerAction::Attached(idx)
            } else {
                WorkerAction::None
            };
        }
        let Some(binding) = SessionBinding::imported(sid, anchor, epoch) else {
            self.send_status(
                sink,
                sc::MSG_SC_ATTACHED,
                &sid,
                epoch,
                sc::STATUS_STALE_EPOCH,
            );
            return WorkerAction::None;
        };
        let Some(idx) = self.find_free() else {
            self.send_status(
                sink,
                sc::MSG_SC_ATTACHED,
                &sid,
                epoch,
                sc::STATUS_NO_CAPACITY,
            );
            return WorkerAction::None;
        };
        self.slots[idx] = WorkerSlot {
            binding,
            phase: WPHASE_ACTIVE,
            in_use: true,
            in_consumed: 0,
            out_produced: 0,
            import_epoch: 0,
        };
        self.send_status(sink, sc::MSG_SC_ATTACHED, &sid, epoch, sc::STATUS_OK);
        WorkerAction::Attached(idx)
    }

    fn on_detach(
        &mut self,
        payload: &[u8],
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
    ) -> WorkerAction {
        if payload.len() < sc::SESSION_HEADER {
            return WorkerAction::None;
        }
        let sid = sid_of(payload);
        let epoch = sc::epoch(payload);
        let reason = if payload.len() > sc::SESSION_HEADER {
            payload[sc::SESSION_HEADER]
        } else {
            sc::DETACH_NORMAL
        };
        let Some(idx) = self.find(&sid) else {
            // Idempotent: already gone.
            self.send_session(sink, sc::MSG_SC_DETACHED, &sid, epoch);
            return WorkerAction::None;
        };
        let was_import = matches!(self.slots[idx].phase, WPHASE_IMPORTING | WPHASE_IMPORTED);
        let cur = self.slots[idx].epoch();
        self.free_slot(idx);
        self.send_session(sink, sc::MSG_SC_DETACHED, &sid, cur);
        if was_import {
            WorkerAction::ImportDiscarded(idx)
        } else {
            WorkerAction::Detached(idx, reason)
        }
    }

    fn on_drain(
        &mut self,
        payload: &[u8],
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
    ) -> WorkerAction {
        if payload.len() < sc::SESSION_HEADER {
            return WorkerAction::None;
        }
        let sid = sid_of(payload);
        let epoch = sc::epoch(payload);
        let Some(idx) = self.find(&sid) else {
            self.send_error(sink, &sid, epoch, sc::STATUS_UNKNOWN_SESSION);
            return WorkerAction::None;
        };
        let cur = self.slots[idx].epoch();
        if cur != epoch || !matches!(self.slots[idx].phase, WPHASE_ACTIVE | WPHASE_DRAINING) {
            self.send_error(sink, &sid, cur, sc::STATUS_STALE_EPOCH);
            return WorkerAction::None;
        }
        self.slots[idx].phase = WPHASE_DRAINING;
        WorkerAction::Drain(idx)
    }

    /// The module's declaration that slot `idx` has settled: every
    /// envelope it emitted for the session has been accounted for and
    /// its inbound tail is dry. Sends `MSG_SC_DRAINED`; from here the
    /// session consumes nothing until told to. Returns `false` when
    /// the slot is not draining.
    pub fn declare_drained(
        &mut self,
        idx: usize,
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
    ) -> bool {
        if idx >= N || !self.slots[idx].in_use || self.slots[idx].phase != WPHASE_DRAINING {
            return false;
        }
        let sid = *self.slots[idx].session_id();
        let epoch = self.slots[idx].epoch();
        self.slots[idx].phase = WPHASE_DRAINED;
        self.send_session(sink, sc::MSG_SC_DRAINED, &sid, epoch)
    }

    /// Export slot `idx`'s state: `EXPORT_BEGIN` carrying the delivery
    /// cursors, then chunks, then `EXPORT_END` with the CRC. The slot
    /// must be drained. Returns `false` if any frame was refused by the
    /// sink (the anchor will time the handoff out and refuse it).
    pub fn export(
        &mut self,
        idx: usize,
        blob: &[u8],
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
    ) -> bool {
        if idx >= N || !self.slots[idx].in_use || self.slots[idx].phase != WPHASE_DRAINED {
            return false;
        }
        if blob.len() > u32::MAX as usize {
            return false;
        }
        let sid = *self.slots[idx].session_id();
        let epoch = self.slots[idx].epoch();
        let cursors = self.slots[idx].cursors();

        // EXPORT_BEGIN: [sid:16][epoch:4][total_len:4][in:8][out:8]
        sc::put_session_header(&mut self.scratch, &sid, epoch);
        let n = sc::put_u32_after_header(&mut self.scratch, blob.len() as u32);
        let mut c = [0u8; CURSOR_PAIR_LEN];
        cursors.encode(&mut c);
        self.scratch[n..n + CURSOR_PAIR_LEN].copy_from_slice(&c);
        if !sink(
            sc::CMD_SC_EXPORT_BEGIN,
            &self.scratch[..sc::EXPORT_BEGIN_LEN],
        ) {
            return false;
        }

        let mut walk = HandoffExport::new(blob.len() as u32);
        while let Some((off, len)) = walk.next_chunk(EXPORT_CHUNK_LEN) {
            sc::put_session_header(&mut self.scratch, &sid, epoch);
            let n = sc::put_u32_after_header(&mut self.scratch, off);
            let (o, l) = (off as usize, len as usize);
            self.scratch[n..n + l].copy_from_slice(&blob[o..o + l]);
            if !sink(sc::CMD_SC_EXPORT_CHUNK, &self.scratch[..n + l]) {
                return false;
            }
            walk.advance(len);
        }

        sc::put_session_header(&mut self.scratch, &sid, epoch);
        let n = sc::put_u32_after_header(&mut self.scratch, handoff_crc32(blob));
        sink(sc::CMD_SC_EXPORT_END, &self.scratch[..n])
    }

    fn on_export_begin(
        &mut self,
        payload: &[u8],
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
    ) -> WorkerAction {
        if payload.len() < sc::EXPORT_BEGIN_LEN {
            return WorkerAction::None;
        }
        let sid = sid_of(payload);
        let epoch = sc::epoch(payload);
        let total = sc::u32_after_header(payload);
        if self.find(&sid).is_some() {
            // We hold this session: an import of it here is a misbound
            // handoff, never something to admit.
            self.send_status(
                sink,
                sc::MSG_SC_IMPORT_BEGIN,
                &sid,
                epoch,
                sc::STATUS_STALE_EPOCH,
            );
            return WorkerAction::None;
        }
        if self.import_slot != usize::MAX {
            self.send_status(
                sink,
                sc::MSG_SC_IMPORT_BEGIN,
                &sid,
                epoch,
                sc::STATUS_NOT_READY,
            );
            return WorkerAction::None;
        }
        let Some(cursors) = sc::export_cursors(payload).and_then(SessionCursors::decode) else {
            self.send_status(
                sink,
                sc::MSG_SC_IMPORT_BEGIN,
                &sid,
                epoch,
                sc::STATUS_CORRUPT,
            );
            return WorkerAction::None;
        };
        let Some(idx) = self.find_free() else {
            self.send_status(
                sink,
                sc::MSG_SC_IMPORT_BEGIN,
                &sid,
                epoch,
                sc::STATUS_NO_CAPACITY,
            );
            return WorkerAction::None;
        };
        let status = self.import.begin(total, IMPORT_BUF_LEN as u32);
        if status != HANDOFF_OK {
            self.send_status(sink, sc::MSG_SC_IMPORT_BEGIN, &sid, epoch, status);
            return WorkerAction::None;
        }
        // The exporter's anchor is learned at RESUME through the
        // binding it carried; until then the session belongs to the
        // anchor that said HELLO.
        let anchor = self.anchor_id;
        let Some(binding) = SessionBinding::imported(sid, anchor, epoch) else {
            self.import.reset();
            self.send_status(
                sink,
                sc::MSG_SC_IMPORT_BEGIN,
                &sid,
                epoch,
                sc::STATUS_STALE_EPOCH,
            );
            return WorkerAction::None;
        };
        self.slots[idx] = WorkerSlot {
            binding,
            phase: WPHASE_IMPORTING,
            in_use: true,
            in_consumed: cursors.in_consumed,
            out_produced: cursors.out_produced,
            import_epoch: epoch,
        };
        self.import_slot = idx;
        self.send_status(sink, sc::MSG_SC_IMPORT_BEGIN, &sid, epoch, sc::STATUS_OK);
        WorkerAction::None
    }

    fn importing_slot(&self, sid: &SessionId) -> Option<usize> {
        let idx = self.import_slot;
        if idx < N && self.slots[idx].in_use && self.slots[idx].binding.is(sid) {
            Some(idx)
        } else {
            None
        }
    }

    fn on_export_chunk(
        &mut self,
        payload: &[u8],
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
    ) -> WorkerAction {
        if payload.len() < sc::SESSION_HEADER + 4 {
            return WorkerAction::None;
        }
        let sid = sid_of(payload);
        let epoch = sc::epoch(payload);
        let Some(idx) = self.importing_slot(&sid) else {
            self.send_status(
                sink,
                sc::MSG_SC_IMPORT_END,
                &sid,
                epoch,
                sc::STATUS_NOT_READY,
            );
            return WorkerAction::None;
        };
        if self.slots[idx].import_epoch != epoch || self.slots[idx].phase != WPHASE_IMPORTING {
            self.send_status(
                sink,
                sc::MSG_SC_IMPORT_END,
                &sid,
                epoch,
                sc::STATUS_STALE_EPOCH,
            );
            return WorkerAction::None;
        }
        let offset = sc::u32_after_header(payload);
        let data = sc::export_chunk_data(payload);
        // `HandoffImport` is `Copy` and the buffer lives beside it, so
        // the copy-out / copy-back keeps the borrows disjoint.
        let mut dest = self.import_buf;
        let mut import = self.import;
        let status = import.chunk(offset, data, &mut dest);
        self.import = import;
        self.import_buf = dest;
        if status != HANDOFF_OK {
            self.free_slot(idx);
            self.send_status(sink, sc::MSG_SC_IMPORT_END, &sid, epoch, status);
            return WorkerAction::ImportDiscarded(idx);
        }
        sc::put_session_header(&mut self.scratch, &sid, epoch);
        let n = sc::put_u32_after_header(&mut self.scratch, offset);
        sink(sc::MSG_SC_IMPORT_CHUNK, &self.scratch[..n]);
        WorkerAction::None
    }

    fn on_export_end(
        &mut self,
        payload: &[u8],
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
    ) -> WorkerAction {
        if payload.len() < sc::SESSION_HEADER + 4 {
            return WorkerAction::None;
        }
        let sid = sid_of(payload);
        let epoch = sc::epoch(payload);
        let Some(idx) = self.importing_slot(&sid) else {
            self.send_status(
                sink,
                sc::MSG_SC_IMPORT_END,
                &sid,
                epoch,
                sc::STATUS_NOT_READY,
            );
            return WorkerAction::None;
        };
        if self.slots[idx].import_epoch != epoch || self.slots[idx].phase != WPHASE_IMPORTING {
            self.send_status(
                sink,
                sc::MSG_SC_IMPORT_END,
                &sid,
                epoch,
                sc::STATUS_STALE_EPOCH,
            );
            return WorkerAction::None;
        }
        let crc = sc::u32_after_header(payload);
        let status = self.import.end(crc);
        if status != HANDOFF_OK {
            self.free_slot(idx);
            self.send_status(sink, sc::MSG_SC_IMPORT_END, &sid, epoch, status);
            return WorkerAction::ImportDiscarded(idx);
        }
        self.slots[idx].phase = WPHASE_IMPORTED;
        self.send_status(sink, sc::MSG_SC_IMPORT_END, &sid, epoch, sc::STATUS_OK);
        WorkerAction::Imported(idx)
    }

    /// Release the import buffer after the module applied the blob
    /// (`WorkerAction::Imported`). The slot stays dormant until RESUME.
    pub fn import_applied(&mut self, idx: usize) {
        if self.import_slot == idx {
            self.import.reset();
            self.import_slot = usize::MAX;
        }
    }

    fn on_resume(
        &mut self,
        payload: &[u8],
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
    ) -> WorkerAction {
        if payload.len() < sc::RESUME_PAYLOAD_LEN {
            return WorkerAction::None;
        }
        let sid = sid_of(payload);
        let new_epoch = sc::epoch(payload);
        let Some(idx) = self.find(&sid) else {
            self.send_error(sink, &sid, new_epoch, sc::STATUS_UNKNOWN_SESSION);
            return WorkerAction::None;
        };
        let slot = self.slots[idx];
        match slot.phase {
            WPHASE_DRAINING | WPHASE_DRAINED => {
                // A refusal: back into service at the unchanged epoch.
                if new_epoch == slot.epoch() {
                    self.slots[idx].phase = WPHASE_ACTIVE;
                    self.send_session(sink, sc::MSG_SC_RESUMED, &sid, new_epoch);
                    WorkerAction::Reinstated(idx)
                } else {
                    self.send_error(sink, &sid, slot.epoch(), sc::STATUS_STALE_EPOCH);
                    WorkerAction::None
                }
            }
            WPHASE_IMPORTED => {
                if new_epoch > slot.import_epoch {
                    self.slots[idx].binding.adopt_epoch(new_epoch);
                    self.slots[idx].phase = WPHASE_ACTIVE;
                    if self.import_slot == idx {
                        self.import.reset();
                        self.import_slot = usize::MAX;
                    }
                    self.send_session(sink, sc::MSG_SC_RESUMED, &sid, new_epoch);
                    WorkerAction::Resumed(idx, new_epoch)
                } else {
                    self.send_error(sink, &sid, slot.import_epoch, sc::STATUS_STALE_EPOCH);
                    WorkerAction::None
                }
            }
            WPHASE_ACTIVE if new_epoch == slot.epoch() => {
                // Idempotent (a lost RESUMED).
                self.send_session(sink, sc::MSG_SC_RESUMED, &sid, new_epoch);
                WorkerAction::None
            }
            _ => {
                self.send_error(sink, &sid, slot.epoch(), sc::STATUS_STALE_EPOCH);
                WorkerAction::None
            }
        }
    }

    fn on_epoch_bump(
        &mut self,
        payload: &[u8],
        sink: &mut impl FnMut(u8, &[u8]) -> bool,
    ) -> WorkerAction {
        if payload.len() < sc::EPOCH_BUMP_PAYLOAD_LEN {
            return WorkerAction::None;
        }
        let sid = sid_of(payload);
        let old = sc::epoch(payload);
        let new = sc::u32_after_header(payload);
        let Some(idx) = self.find(&sid) else {
            self.send_error(sink, &sid, old, sc::STATUS_UNKNOWN_SESSION);
            return WorkerAction::None;
        };
        let cur = self.slots[idx].epoch();
        if old != cur || !self.slots[idx].binding.adopt_epoch(new) {
            self.send_error(sink, &sid, cur, sc::STATUS_STALE_EPOCH);
            return WorkerAction::None;
        }
        self.send_session(sink, sc::MSG_SC_EPOCH_CONFIRMED, &sid, new);
        WorkerAction::EpochBumped(idx, new)
    }
}

/// The session id at the head of a session-scoped payload (zeros on a
/// short payload — every caller has already length-checked).
pub fn sid_of(payload: &[u8]) -> SessionId {
    let mut s = [0u8; sc::SESSION_ID_BYTES];
    let src = sc::session_id(payload);
    if src.len() == sc::SESSION_ID_BYTES {
        s.copy_from_slice(src);
    }
    s
}
