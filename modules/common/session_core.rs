//! session_core — Lattice's use of Fluxor's session identity.
//!
//! One identity, one epoch, defined by the platform contract
//! (`contracts/net/session_ctrl.rs`, mounted below as
//! [`session_ctrl`]). This file holds the pieces every
//! continuity-bearing module shares:
//!
//! - the identity triple `session_id` / `anchor_id` / `session_epoch`
//!   and the binding that carries it, with presence tracked apart from
//!   value so no anchor id is a sentinel;
//! - two distinct epochs, kept separate: a **session epoch** is per
//!   session and advances by one on every authoritative rebind of
//!   *that* session; a **placement epoch** is cluster-wide,
//!   substrate-driven, and is never stamped into a session record;
//! - session-id minting, so a data-plane address is recoverable from
//!   the identity itself (`[anchor_id:8][app_id:8 BE]`).
//!
//! Dual-target: compiles into the no_std PIC modules and is
//! `#[path]`-mounted into the host contract suites.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules; each consumer uses a subset of the surface"
)]

/// The single mount of Fluxor's SessionCtrlV1 contract for this tree.
/// Consumers reach it as `session_core::session_ctrl` rather than
/// mounting the file a second time, which rustc would treat as an
/// unrelated module.
#[path = "../../target/fluxor/fluxor-abi/sdk/contracts/net/session_ctrl.rs"]
#[allow(
    dead_code,
    clippy::duplicate_mod,
    reason = "platform contract mounted wholesale; each consumer uses a subset"
)]
#[rustfmt::skip]
pub mod session_ctrl;

pub use session_ctrl::{ANCHOR_ID_BYTES, SESSION_ID_BYTES, WORKER_ID_BYTES};

pub type SessionId = [u8; SESSION_ID_BYTES];
pub type AnchorId = [u8; ANCHOR_ID_BYTES];
pub type WorkerId = [u8; WORKER_ID_BYTES];

/// The all-zero worker id: "no hint" on ATTACH, never a real worker.
pub const NO_WORKER: WorkerId = [0; WORKER_ID_BYTES];

/// The epoch a session is minted at. The contract fixes zero as
/// "never bound"; the first authoritative bind is generation one.
pub const FIRST_EPOCH: u32 = 1;

/// The next epoch after `epoch`. Saturates rather than wrapping: a
/// wrap would hand a session a *lower* generation, which is the one
/// thing an epoch must never do. Four billion rebinds of one session
/// is not a budget any deployment reaches.
pub const fn next_epoch(epoch: u32) -> u32 {
    if epoch == u32::MAX {
        u32::MAX
    } else {
        epoch + 1
    }
}

/// Result of checking a frame's epoch against a binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpochCheck {
    /// The session's current generation: admit.
    Current,
    /// Below the current generation: a frame from before a rebind.
    Stale,
    /// Above the current generation: nobody issued it. Refused too —
    /// epochs advance only through an authoritative rebind.
    Future,
}

// ── Binding ───────────────────────────────────────────────────────────

/// A session's binding to the anchor that fronts it.
///
/// `bound` is the presence bit. It is separate from `anchor_id` for
/// the same reason the connection tables keep presence apart from the
/// id: every value of the id is a legitimate anchor, including all
/// zeros and all ones. An unbound session keeps its identity and its
/// epoch — the anchor dropped, the client may resume — and only loses
/// presence.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionBinding {
    pub session_id: SessionId,
    pub anchor_id: AnchorId,
    /// Monotonic per session. Advances on every authoritative rebind
    /// of this session and on nothing else.
    pub session_epoch: u32,
    pub bound: bool,
}

impl SessionBinding {
    /// No session: zero identity, epoch zero, unbound.
    pub const fn empty() -> Self {
        Self {
            session_id: [0; SESSION_ID_BYTES],
            anchor_id: [0; ANCHOR_ID_BYTES],
            session_epoch: 0,
            bound: false,
        }
    }

    /// First attach: the session is minted at [`FIRST_EPOCH`] on
    /// `anchor_id`.
    pub const fn attach(session_id: SessionId, anchor_id: AnchorId) -> Self {
        Self {
            session_id,
            anchor_id,
            session_epoch: FIRST_EPOCH,
            bound: true,
        }
    }

    /// A binding restored from a handoff: identity and epoch are the
    /// exporter's, not fresh. Refused (returns `None`) at epoch zero,
    /// which no bound session ever carries.
    pub const fn imported(session_id: SessionId, anchor_id: AnchorId, epoch: u32) -> Option<Self> {
        if epoch == 0 {
            return None;
        }
        Some(Self {
            session_id,
            anchor_id,
            session_epoch: epoch,
            bound: true,
        })
    }

    /// Authoritative rebind onto `anchor_id`. The session epoch
    /// advances by one; the new epoch is returned. This is the only
    /// writer of `session_epoch` after minting.
    pub fn rebind(&mut self, anchor_id: AnchorId) -> u32 {
        self.session_epoch = next_epoch(self.session_epoch);
        self.anchor_id = anchor_id;
        self.bound = true;
        self.session_epoch
    }

    /// Adopt an epoch the directory advanced to (`CMD_SC_RESUME` /
    /// `CMD_SC_EPOCH_BUMP` carrying `new_epoch`). Honoured only when it
    /// advances; a stale or equal value leaves the binding untouched
    /// and returns `false`.
    pub fn adopt_epoch(&mut self, new_epoch: u32) -> bool {
        if new_epoch <= self.session_epoch {
            return false;
        }
        self.session_epoch = new_epoch;
        true
    }

    /// Drop presence, keep identity and epoch.
    pub fn unbind(&mut self) {
        self.bound = false;
    }

    /// Check a frame's epoch against this binding.
    pub fn admit(&self, epoch: u32) -> EpochCheck {
        if epoch == self.session_epoch {
            EpochCheck::Current
        } else if epoch < self.session_epoch {
            EpochCheck::Stale
        } else {
            EpochCheck::Future
        }
    }

    /// True iff `session_id` names this binding.
    pub fn is(&self, session_id: &SessionId) -> bool {
        self.session_id == *session_id
    }
}

// ── Placement epoch ───────────────────────────────────────────────────

/// The cluster-wide placement epoch as the substrate reports it.
/// Held once per module, never stamped into a session: a placement
/// event tells the directory that placements may have moved, and the
/// directory drives a per-session rebind for each session whose
/// placement did move. A session that did not move is not fenced.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlacementEpoch {
    pub current: u32,
}

impl PlacementEpoch {
    /// Initial value 1 covers the pre-substrate single-node case, until
    /// the first placement event lands.
    pub const fn new() -> Self {
        Self { current: 1 }
    }

    /// Advance to `new`. Monotonic: a duplicate or out-of-order event
    /// is ignored and returns `false`.
    pub fn advance(&mut self, new: u32) -> bool {
        if new <= self.current {
            return false;
        }
        self.current = new;
        true
    }
}

impl Default for PlacementEpoch {
    fn default() -> Self {
        Self::new()
    }
}

/// The substrate's placement-epoch envelope, `control_plane.epoch_events`:
/// `[kpg_id:u16 LE][epoch:u32 LE][reason:u8]`. The declaration is
/// clustor's (`wire.rs`); this is the one shape a placement event has.
pub const MSG_PLACEMENT_EPOCH_EVENT: u8 = 0xD5;
/// Payload bytes of a placement-epoch event.
pub const PLACEMENT_EPOCH_EVENT_LEN: usize = 2 + 4 + 1;

/// The new placement epoch a placement event carries, or `None` for
/// anything that is not one.
pub fn placement_event_epoch(msg_type: u8, payload: &[u8]) -> Option<u32> {
    if msg_type != MSG_PLACEMENT_EPOCH_EVENT || payload.len() < PLACEMENT_EPOCH_EVENT_LEN {
        return None;
    }
    Some(u32::from_le_bytes([
        payload[2], payload[3], payload[4], payload[5],
    ]))
}

// ── Identity minting ──────────────────────────────────────────────────

/// Mint a session id: `[anchor_id:8][app_id:8 BE]`. `app_id` is the
/// protocol-visible handle the anchor already owns (an etcd watch id
/// or lease id, a subscriber's connection generation), so the
/// data-plane address is recoverable from the identity — which is the
/// only correlation the contract carries.
pub fn mint_session_id(anchor_id: &AnchorId, app_id: u64) -> SessionId {
    let mut id = [0u8; SESSION_ID_BYTES];
    id[..ANCHOR_ID_BYTES].copy_from_slice(anchor_id);
    id[ANCHOR_ID_BYTES..].copy_from_slice(&app_id.to_be_bytes());
    id
}

/// The anchor that minted `session_id`.
pub fn session_anchor(session_id: &SessionId) -> AnchorId {
    let mut a = [0u8; ANCHOR_ID_BYTES];
    a.copy_from_slice(&session_id[..ANCHOR_ID_BYTES]);
    a
}

/// The application handle `session_id` was minted from.
pub fn session_app_id(session_id: &SessionId) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&session_id[ANCHOR_ID_BYTES..]);
    u64::from_be_bytes(b)
}

/// A worker's 8-byte identity from its small configured ordinal:
/// `b"LWRK"` + class byte + ordinal, so a monitor line reads.
pub const fn worker_id(class: u8, ordinal: u16) -> WorkerId {
    let o = ordinal.to_be_bytes();
    [b'L', b'W', b'R', b'K', class, 0, o[0], o[1]]
}

/// An anchor's 8-byte identity: `b"LA"` + protocol + port + ordinal.
pub const fn anchor_id(protocol: u8, port: u16, ordinal: u16) -> AnchorId {
    let p = port.to_be_bytes();
    let o = ordinal.to_be_bytes();
    [b'L', b'A', protocol, p[0], p[1], 0, o[0], o[1]]
}

/// Continuity-worker class bytes carried in [`worker_id`].
pub const CLASS_WATCH: u8 = b'W';
pub const CLASS_LEASE: u8 = b'L';
pub const CLASS_PUBSUB: u8 = b'P';
