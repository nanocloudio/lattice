//! pubsub_worker — Redis pub/sub session state as a movable worker.
//!
//! Redis pub/sub has no cursor, so a message published while a
//! subscriber is disconnected is lost and undetectable. Holding the
//! subscription state in a worker that can be handed off — not in the
//! anchor's per-connection slot — is what lets it survive a worker
//! move. A subscriber connection is a session
//! (`session_id = [anchor_id:8][conn_generation:8]`); its subscription
//! list crosses a worker move in the handoff blob, so a maintenance
//! window does not drop the subscriber's messages.
//!
//! The anchor keeps the transport, the RESP codec and the ingress hold
//! buffer; command traffic on the same connection is unaffected and
//! stays `drain_only`.
//!
//! Ports:
//! - **`ctrl_in`** — from the redis anchor: SessionCtrlV1 frames
//!   (`0x70..=0x9F`), the session-scoped `MSG_PUBSUB_CTRL`
//!   subscribe/unsubscribe, and the connectionless `MSG_PUBSUB_PUBLISH`.
//! - **`ctrl_out`** — everything back to the anchor on one ordered
//!   channel: SessionCtrlV1 replies (`MSG_SC_*`), `MSG_PUBSUB_MSG`
//!   pushes toward subscribers, and `MSG_PUBSUB_PUBLISHED` receiver
//!   counts back to publishers. One channel keeps message pushes
//!   ordered ahead of a handoff's `EXPORT_BEGIN`, which the
//!   delivery-cursor check relies on.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    clippy::duplicate_mod,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    clippy::too_many_arguments,
    reason = "fluxor module ABI: raw-pointer entry points are the contract, and ABI fns carry a fixed arity"
)]

use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/pubsub_core.rs"]
mod pubsub_core;

#[path = "../../common/session_worker.rs"]
mod session_worker;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use pubsub_core::SubTable;
use session_worker::session_core::session_ctrl as sc;
use session_worker::session_core::{worker_id, SessionId, CLASS_PUBSUB};
use session_worker::{SessionWorker, WorkerAction};
use wire::{
    MSG_PUBSUB_CTRL, MSG_PUBSUB_MSG, MSG_PUBSUB_PUBLISH, MSG_PUBSUB_PUBLISHED,
    PUBSUB_CTRL_PSUBSCRIBE, PUBSUB_CTRL_PUNSUBSCRIBE, PUBSUB_CTRL_SUBSCRIBE,
    PUBSUB_CTRL_UNSUBSCRIBE, PUBSUB_KIND_MESSAGE, PUBSUB_KIND_PMESSAGE, PUBSUB_KIND_PSUBSCRIBE,
    PUBSUB_KIND_PUNSUBSCRIBE, PUBSUB_KIND_SUBSCRIBE, PUBSUB_KIND_UNSUBSCRIBE,
};

const SCRATCH_BUF_SIZE: usize = 4096;
const MAX_SESSIONS: usize = 128;

#[repr(C)]
struct PubsubState {
    syscalls: *const SyscallTable,

    ctrl_in: i32,
    ctrl_out: i32,
    messages_out: i32,
    metrics_out: i32,

    worker_ordinal: u8,

    m_subs: u64,
    m_published: u64,
    m_delivered: u64,
    step_ctr: u64,

    subs: [SubTable; MAX_SESSIONS],
    sw: SessionWorker<MAX_SESSIONS>,
    scratch: [u8; SCRATCH_BUF_SIZE],
}

define_params! {
    PubsubState;

    1, worker_id, u8, 0
        => |s, d, len| { s.worker_ordinal = p_u8(d, len, 0, 0); };
}

impl PubsubState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.ctrl_in = -1;
        self.ctrl_out = -1;
        self.messages_out = -1;
        self.metrics_out = -1;
        self.worker_ordinal = 0;
        self.m_subs = 0;
        self.m_published = 0;
        self.m_delivered = 0;
        self.step_ctr = 0;
        let mut i = 0;
        while i < MAX_SESSIONS {
            self.subs[i] = SubTable::new();
            i += 1;
        }
        self.sw.init(worker_id(CLASS_PUBSUB, 0));
    }
}

unsafe fn read_envelope(sys: &SyscallTable, chan: i32, scratch: &mut [u8]) -> Option<(u8, usize)> {
    if chan < 0 {
        return None;
    }
    let poll = (sys.channel_poll)(chan, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return None;
    }
    let mut hdr = [0u8; 3];
    let n = (sys.channel_read)(chan, hdr.as_mut_ptr(), 3);
    if n < 3 {
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
    let n2 = (sys.channel_read)(chan, scratch.as_mut_ptr(), payload_len);
    if (n2 as usize) < payload_len {
        return None;
    }
    Some((msg_type, payload_len))
}

unsafe fn write_envelope(sys: &SyscallTable, chan: i32, msg_type: u8, payload: &[u8]) -> bool {
    if chan < 0 || payload.len() > u16::MAX as usize {
        return false;
    }
    let mut buf = [0u8; SCRATCH_BUF_SIZE + 3];
    let total = 3 + payload.len();
    if total > buf.len() {
        return false;
    }
    buf[0] = msg_type;
    buf[1] = (payload.len() & 0xFF) as u8;
    buf[2] = ((payload.len() >> 8) & 0xFF) as u8;
    buf[3..total].copy_from_slice(payload);
    (sys.channel_write)(chan, buf.as_mut_ptr(), total) == total as i32
}

macro_rules! ctrl_sink {
    ($st:expr) => {{
        let sys = $st.syscalls;
        let chan = $st.ctrl_out;
        move |msg: u8, payload: &[u8]| -> bool {
            if sys.is_null() {
                return false;
            }
            unsafe { write_envelope(&*sys, chan, msg, payload) }
        }
    }};
}

// ── Session control ───────────────────────────────────────────────────

fn handle_session_frame(st: &mut PubsubState, msg_type: u8, payload: &[u8]) {
    let mut sink = ctrl_sink!(st);
    let action = st.sw.handle_frame(msg_type, payload, &mut sink);
    match action {
        WorkerAction::Attached(idx) => {
            st.subs[idx] = SubTable::new();
        }
        WorkerAction::Detached(idx, _) | WorkerAction::ImportDiscarded(idx) => {
            st.subs[idx] = SubTable::new();
        }
        WorkerAction::Drain(idx) => {
            let mut blob = [0u8; SCRATCH_BUF_SIZE];
            let n = st.subs[idx].export(&mut blob);
            let mut sink = ctrl_sink!(st);
            if st.sw.declare_drained(idx, &mut sink) {
                let _ = st.sw.export(idx, &blob[..n], &mut sink);
            }
        }
        WorkerAction::Imported(idx) => {
            match SubTable::import(st.sw.import_blob()) {
                Some(t) => st.subs[idx] = t,
                None => st.sw.forget(idx),
            }
            st.sw.import_applied(idx);
        }
        WorkerAction::None
        | WorkerAction::Resumed(_, _)
        | WorkerAction::Reinstated(_)
        | WorkerAction::EpochBumped(_, _) => {}
    }
}

// ── Data plane ────────────────────────────────────────────────────────

fn handle_ctrl(st: &mut PubsubState, payload: &[u8]) {
    // [session_id:16][epoch:4][ctrl:1][count:1] then count × [len:2][name…]
    if payload.len() < sc::SESSION_HEADER + 2 {
        return;
    }
    let sid = session_worker::sid_of(payload);
    let epoch = sc::epoch(payload);
    let Some(idx) = st.sw.admit(&sid, epoch) else {
        return; // fenced
    };
    st.sw.consumed(idx);
    let ctrl = payload[sc::SESSION_HEADER];
    let count = payload[sc::SESSION_HEADER + 1] as usize;
    let is_pattern = matches!(ctrl, PUBSUB_CTRL_PSUBSCRIBE | PUBSUB_CTRL_PUNSUBSCRIBE);
    let is_sub = matches!(ctrl, PUBSUB_CTRL_SUBSCRIBE | PUBSUB_CTRL_PSUBSCRIBE);
    let ack_kind = match ctrl {
        PUBSUB_CTRL_SUBSCRIBE => PUBSUB_KIND_SUBSCRIBE,
        PUBSUB_CTRL_UNSUBSCRIBE => PUBSUB_KIND_UNSUBSCRIBE,
        PUBSUB_CTRL_PSUBSCRIBE => PUBSUB_KIND_PSUBSCRIBE,
        PUBSUB_CTRL_PUNSUBSCRIBE => PUBSUB_KIND_PUNSUBSCRIBE,
        _ => return,
    };

    if count == 0 && !is_sub {
        // Unsubscribe-all of this kind.
        let n = st.subs[idx].remove_all(is_pattern);
        emit_ack(st, idx, &sid, epoch, ack_kind, n as u32, &[]);
        return;
    }

    let mut at = sc::SESSION_HEADER + 2;
    let mut i = 0;
    while i < count {
        if at + 2 > payload.len() {
            break;
        }
        let len = u16::from_le_bytes([payload[at], payload[at + 1]]) as usize;
        at += 2;
        if at + len > payload.len() {
            break;
        }
        let name = &payload[at..at + len];
        at += len;
        let after = if is_sub {
            st.subs[idx]
                .add(name, is_pattern)
                .unwrap_or_else(|| st.subs[idx].count())
        } else {
            st.subs[idx].remove(name, is_pattern)
        };
        if is_sub {
            st.m_subs = st.m_subs.wrapping_add(1);
        }
        // One acknowledgement per name, carrying the running count —
        // exactly Redis's per-channel subscribe/unsubscribe reply.
        emit_ack(st, idx, &sid, epoch, ack_kind, after as u32, name);
        i += 1;
    }
}

fn emit_ack(
    st: &mut PubsubState,
    idx: usize,
    sid: &SessionId,
    epoch: u32,
    kind: u8,
    count: u32,
    channel: &[u8],
) {
    st.sw.produced(idx);
    let body = build_msg(&mut st.scratch, sid, epoch, kind, count, channel, &[], &[]);
    unsafe {
        let sys = st.syscalls;
        if !sys.is_null() {
            let _ = write_envelope(&*sys, st.messages_out, MSG_PUBSUB_MSG, body);
        }
    }
}

fn handle_publish(st: &mut PubsubState, payload: &[u8]) {
    // [corr:8][channel_len:2][channel…][message_len:2][message…]
    if payload.len() < 8 + 2 {
        return;
    }
    let corr = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    let clen = u16::from_le_bytes([payload[8], payload[9]]) as usize;
    if 10 + clen + 2 > payload.len() {
        return;
    }
    let mut channel = [0u8; pubsub_core::SUB_NAME_MAX];
    let cc = clen.min(channel.len());
    channel[..cc].copy_from_slice(&payload[10..10 + cc]);
    let mlen_off = 10 + clen;
    let mlen = u16::from_le_bytes([payload[mlen_off], payload[mlen_off + 1]]) as usize;
    let msg_off = mlen_off + 2;
    if msg_off + mlen > payload.len() {
        return;
    }
    let mut message = [0u8; 1024];
    let mm = mlen.min(message.len());
    message[..mm].copy_from_slice(&payload[msg_off..msg_off + mm]);

    st.m_published = st.m_published.wrapping_add(1);
    let mut receivers = 0u32;
    let mut i = 0;
    while i < MAX_SESSIONS {
        let deliver = if st.sw.slots[i].in_service() {
            st.subs[i].delivery_for(&channel[..cc]).map(|p| {
                let mut pat = [0u8; pubsub_core::SUB_NAME_MAX];
                let pl = p.len().min(pat.len());
                pat[..pl].copy_from_slice(&p[..pl]);
                (pl, pat)
            })
        } else {
            None
        };
        if let Some((pl, pat)) = deliver {
            let sid = *st.sw.slots[i].session_id();
            let epoch = st.sw.slots[i].epoch();
            st.sw.produced(i);
            let (kind, pattern): (u8, &[u8]) = if pl == 0 {
                (PUBSUB_KIND_MESSAGE, &[])
            } else {
                (PUBSUB_KIND_PMESSAGE, &pat[..pl])
            };
            let body = build_msg(
                &mut st.scratch,
                &sid,
                epoch,
                kind,
                0,
                &channel[..cc],
                pattern,
                &message[..mm],
            );
            unsafe {
                let sys = st.syscalls;
                if !sys.is_null() {
                    let _ = write_envelope(&*sys, st.messages_out, MSG_PUBSUB_MSG, body);
                }
            }
            st.m_delivered = st.m_delivered.wrapping_add(1);
            receivers += 1;
        }
        i += 1;
    }

    // Receiver count back to the publisher.
    let mut rbody = [0u8; 12];
    rbody[0..8].copy_from_slice(&corr.to_le_bytes());
    rbody[8..12].copy_from_slice(&receivers.to_le_bytes());
    unsafe {
        let sys = st.syscalls;
        if !sys.is_null() {
            let _ = write_envelope(&*sys, st.messages_out, MSG_PUBSUB_PUBLISHED, &rbody);
        }
    }
}

/// Build a `MSG_PUBSUB_MSG` body into `buf`, returning the slice.
///   [session_id:16][epoch:4][kind:1][count:4]
///   [channel_len:2][channel…][pattern_len:2][pattern…]
///   [message_len:2][message…]
fn build_msg<'a>(
    buf: &'a mut [u8],
    sid: &SessionId,
    epoch: u32,
    kind: u8,
    count: u32,
    channel: &[u8],
    pattern: &[u8],
    message: &[u8],
) -> &'a [u8] {
    sc::put_session_header(buf, sid, epoch);
    let mut n = sc::SESSION_HEADER;
    buf[n] = kind;
    n += 1;
    buf[n..n + 4].copy_from_slice(&count.to_le_bytes());
    n += 4;
    for field in [channel, pattern, message] {
        let l = field.len().min(u16::MAX as usize);
        buf[n..n + 2].copy_from_slice(&(l as u16).to_le_bytes());
        n += 2;
        buf[n..n + l].copy_from_slice(&field[..l]);
        n += l;
    }
    &buf[..n]
}

// ── Module ABI ────────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<PubsubState>() as u32
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
    if state_size < core::mem::size_of::<PubsubState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let st = unsafe { &mut *state.cast::<PubsubState>() };
    st.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(st, params, params_len) };
    }
    st.sw
        .init(worker_id(CLASS_PUBSUB, u16::from(st.worker_ordinal)));

    // inputs: ctrl_in[0]; outputs: ctrl_out[0] (= messages), metrics[1].
    // SessionCtrlV1 frames AND message pushes share one channel so the
    // anchor relays every message before it sees EXPORT_BEGIN — the
    // cursor gate depends on that ordering.
    st.ctrl_in = in_chan;
    st.ctrl_out = out_chan;
    st.messages_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        st.metrics_out = dev_channel_port(sys, 1, 1);
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let st = unsafe { &mut *state.cast::<PubsubState>() };
    unsafe {
        let sys_ptr = st.syscalls;
        if sys_ptr.is_null() {
            return 0;
        }
        let sys = &*sys_ptr;
        if let Some((mt, len)) = read_envelope(sys, st.ctrl_in, &mut st.scratch) {
            let mut tmp = [0u8; SCRATCH_BUF_SIZE];
            tmp[..len].copy_from_slice(&st.scratch[..len]);
            if (0x70..=0x9F).contains(&mt) {
                handle_session_frame(st, mt, &tmp[..len]);
            } else if mt == MSG_PUBSUB_CTRL {
                handle_ctrl(st, &tmp[..len]);
            } else if mt == MSG_PUBSUB_PUBLISH {
                handle_publish(st, &tmp[..len]);
            }
        }

        st.step_ctr = st.step_ctr.wrapping_add(1);
        if st.step_ctr.is_multiple_of(5000) && !st.syscalls.is_null() {
            telemetry::emit_counters(
                &*st.syscalls,
                st.metrics_out,
                &[st.m_subs, st.m_published, st.m_delivered],
            );
        }
    }
    0
}
