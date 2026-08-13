//! cdc_pump — CDC egress pump. One instance, one feed.
//!
//! This file is deliberately thin: channels, params, the syscall
//! table, and metrics. The ENTIRE feed protocol — checkpoint
//! generation-pointer discipline, frozen-bound streaming windows,
//! backfill lifecycle, the unacked replay ring, resolved watermarks,
//! compaction-lapse relatch — lives in `modules/common/cdc_feed.rs`
//! ([`cdc_feed::FeedCore`]), which the host conformance suite drives
//! against a simulated provider and sink
//! (`tests/harness/tests/contract_cdc_feed.rs`). Fix protocol bugs
//! THERE; this shell only moves bytes between channels and the core.
//!
//! The pump is an ordinary KV client on a router anchor pair: it reads
//! change windows with `KV_OP_SCAN_VERSIONS`, backfills with
//! `KV_OP_SNAPSHOT_VERSIONS`, and checkpoints with `KV_OP_GET` /
//! `KV_OP_PUT` / `KV_OP_CAS` — the same surface any external feed
//! consumer sees, with no privileged read path. Events leave as
//! `cdc_wire::CdcEvent` envelopes wrapped in `SinkPublish` frames; the
//! sink answers with `SinkAck`s, and the DURABLE checkpoint advances
//! only past the contiguous acknowledged prefix.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
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

#[path = "../../common/cdc_feed.rs"]
mod cdc_feed;

#[path = "../../common/hex_core.rs"]
mod hex_core;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use cdc_feed::cdc_wire::{SinkAck, MSG_CDC_ACK};
use cdc_feed::wire::{FenceTail, MSG_KV_RESPONSE};
use cdc_feed::{FeedCore, FeedIo, KEY_MAX, SCRATCH};

const EMIT_EVERY: u64 = 2000;

#[repr(C)]
struct PumpState {
    syscalls: *const SyscallTable,
    kv_in: i32,
    ack_in: i32,
    kv_out: i32,
    publish_out: i32,
    retention_out: i32,
    metrics_out: i32,
    step_ctr: u64,
    core: FeedCore,
}

/// [`FeedIo`] over the kernel channel/log syscalls. Borrowed per step;
/// holds only what the core's side effects need.
struct ShellIo<'a> {
    sys: &'a SyscallTable,
    kv_out: i32,
    publish_out: i32,
    retention_out: i32,
}

impl ShellIo<'_> {
    fn write_chan(&mut self, chan: i32, frame: &mut [u8]) -> bool {
        if chan < 0 {
            return false;
        }
        // SAFETY: `frame` is a live, exclusively borrowed buffer of
        // exactly `frame.len()` bytes; the kernel write only reads it.
        unsafe {
            (self.sys.channel_write)(chan, frame.as_mut_ptr(), frame.len()) == frame.len() as i32
        }
    }
}

impl FeedIo for ShellIo<'_> {
    fn send_kv(&mut self, frame: &mut [u8]) -> bool {
        self.write_chan(self.kv_out, frame)
    }

    fn ship(&mut self, frame: &mut [u8]) -> bool {
        self.write_chan(self.publish_out, frame)
    }

    fn send_claim(&mut self, frame: &mut [u8]) -> bool {
        self.write_chan(self.retention_out, frame)
    }

    /// One tagged value (state moves, CAS outcomes, wedge ticks) at
    /// warn level so a stuck pump is diagnosable from the boot log
    /// alone.
    fn log(&mut self, tag: &[u8; 4], value: u64) {
        let mut m = *b"[cdc] xxxx=00000000000000000000";
        m[6..10].copy_from_slice(tag);
        let mut n = 11usize;
        let mut x = value;
        let mut digits = [0u8; 20];
        let mut d = 0;
        loop {
            digits[d] = b'0' + (x % 10) as u8;
            x /= 10;
            d += 1;
            if x == 0 {
                break;
            }
        }
        while d > 0 {
            d -= 1;
            m[n] = digits[d];
            n += 1;
        }
        unsafe {
            dev_log(self.sys, 2, m.as_ptr(), n);
        }
    }
}

define_params! {
    PumpState;

    // Feed identity (checkpoint key, envelope field, metrics label).
    1, feed_id, u32, 1
        => |s, d, len| { s.core.feed_id = p_u32(d, len, 0, 1); };

    // Envelope table_id; 0 for raw-KV feeds.
    2, table_id, u32, 0
        => |s, d, len| { s.core.table_id = p_u32(d, len, 0, 0); };

    // Keyrange prefix as hex (configs carry text, not raw bytes).
    3, prefix, str, 0 => |s, d, len| {
        let mut hexbuf = [0u8; KEY_MAX * 2];
        let n = len.min(KEY_MAX * 2);
        let mut i = 0usize;
        while i < n {
            hexbuf[i] = *d.add(i);
            i += 1;
        }
        let mut out = [0u8; KEY_MAX];
        if let Some(k) = hex_core::hex_decode(&hexbuf[..n], &mut out) {
            s.core.prefix[..k].copy_from_slice(&out[..k]);
            s.core.prefix_len = k as u16;
        }
    };

    // Protocol byte matching the wired anchor pair (reply routing).
    4, proto, u32, 1
        => |s, d, len| { s.core.proto = p_u32(d, len, 0, 1); };

    // Static owner epoch.
    5, owner_epoch, u32, 1
        => |s, d, len| { s.core.owner_epoch = p_u32(d, len, 0, 1); };

    // on_lapse policy: 1 = backfill, 0 = halt.
    6, on_lapse_backfill, u32, 1
        => |s, d, len| { s.core.on_lapse_backfill = p_u32(d, len, 0, 1); };

    // Backfill when enabling on an existing keyrange.
    7, backfill_on_create, u32, 1
        => |s, d, len| { s.core.backfill_on_create = p_u32(d, len, 0, 1); };

    8, checkpoint_interval_events, u32, 32
        => |s, d, len| { s.core.checkpoint_interval_events = p_u32(d, len, 0, 32); };

    9, checkpoint_interval_ms, u32, 1000
        => |s, d, len| { s.core.checkpoint_interval_ms = p_u32(d, len, 0, 1000); };

    10, resolved_interval_ms, u32, 1000
        => |s, d, len| { s.core.resolved_interval_ms = p_u32(d, len, 0, 1000); };

    11, claim_interval_ms, u32, 1000
        => |s, d, len| { s.core.claim_interval_ms = p_u32(d, len, 0, 1000); };

    // Retention budget, as claim-expiry age. GC-side lapse needs no
    // pump cooperation: an expired claim simply stops protecting.
    12, retention_budget_ms, u32, 60000
        => |s, d, len| { s.core.retention_budget_ms = p_u32(d, len, 0, 60000); };

    13, stall_grace_ms, u32, 2000
        => |s, d, len| { s.core.stall_grace_ms = p_u32(d, len, 0, 2000); };

    // Feed-page request size, clamped to 1..=16 by validate_config.
    // Production leaves the default; conformance tests shrink it to
    // force multi-page windows deterministically.
    14, page_limit, u32, 16
        => |s, d, len| { s.core.page_limit = p_u32(d, len, 0, 16) as u16; };
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
    let mut hdr = [0u8; 3];
    if (sys.channel_read)(chan, hdr.as_mut_ptr(), 3) < 3 {
        return None;
    }
    let mt = hdr[0];
    let len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if len > scratch.len() {
        return None;
    }
    if len == 0 {
        return Some((mt, 0));
    }
    if ((sys.channel_read)(chan, scratch.as_mut_ptr(), len) as usize) < len {
        return None;
    }
    Some((mt, len))
}

// ── module ABI ────────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<PumpState>() as u32
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
    if state_size < core::mem::size_of::<PumpState>() {
        return -1;
    }
    unsafe {
        let s = &mut *state.cast::<PumpState>();
        let sys = &*syscalls.cast::<SyscallTable>();
        s.syscalls = sys;
        s.step_ctr = 0;
        s.core.init();
        s.kv_in = in_chan;
        s.kv_out = out_chan;
        s.ack_in = dev_channel_port(sys, 0, 1);
        s.publish_out = dev_channel_port(sys, 1, 1);
        s.retention_out = dev_channel_port(sys, 1, 2);
        s.metrics_out = dev_channel_port(sys, 1, 3);
        parse_tlv(s, params, params_len);
        // Refuses a system-prefix keyrange (self-amplifying loop) and
        // clamps page_limit.
        s.core.validate_config();
    }
    0
}

// ── step ──────────────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<PumpState>() };
    let sys_ptr = s.syscalls;
    if sys_ptr.is_null() {
        return 0;
    }
    let sys = unsafe { &*sys_ptr };
    let now = unsafe { dev_millis(sys) };
    let mut io = ShellIo {
        sys,
        kv_out: s.kv_out,
        publish_out: s.publish_out,
        retention_out: s.retention_out,
    };

    // 1. Drain acks (bounded).
    for _ in 0..8 {
        let mut buf = [0u8; 32];
        let Some((mt, len)) = (unsafe { read_envelope(sys, s.ack_in, &mut buf) }) else {
            break;
        };
        if mt != MSG_CDC_ACK {
            continue;
        }
        let Some(ack) = SinkAck::decode(&buf[..len]) else {
            continue;
        };
        s.core.on_sink_ack(&mut io, &ack);
    }

    // 2. Drain the KV response (one outstanding at a time).
    {
        let mut buf = [0u8; SCRATCH];
        if let Some((mt, len)) = unsafe { read_envelope(sys, s.kv_in, &mut buf) } {
            if mt == MSG_KV_RESPONSE && len >= 20 {
                let corr = u64::from_le_bytes(buf[0..8].try_into().unwrap_or([0; 8]));
                let result = buf[9];
                let revision = u64::from_le_bytes(buf[10..18].try_into().unwrap_or([0; 8]));
                let blen = u16::from_le_bytes([buf[18], buf[19]]) as usize;
                if corr == s.core.kv_corr && len >= 20 + blen {
                    // Fence tail: the commit frontier rides every
                    // response.
                    let tail_at = 20 + blen;
                    let frontier = buf
                        .get(tail_at..tail_at + FenceTail::LEN)
                        .and_then(FenceTail::decode)
                        .map_or(0, |t| t.commit_frontier);
                    s.core.on_kv_response(
                        &mut io,
                        now,
                        result,
                        revision,
                        frontier,
                        &buf[20..20 + blen],
                    );
                }
            }
        }
    }

    // 3–5. Progress + resolved watermark + retention claim.
    s.core.step(&mut io, now);

    emit_metrics(s, sys);
    0
}

fn emit_metrics(s: &mut PumpState, sys: &SyscallTable) {
    s.step_ctr = s.step_ctr.wrapping_add(1);
    if s.step_ctr.is_multiple_of(EMIT_EVERY) {
        unsafe {
            telemetry::emit_counters(
                sys,
                s.metrics_out,
                &[
                    s.core.m_published,
                    s.core.m_acked,
                    s.core.m_refusals,
                    s.core.m_replays,
                    s.core.m_checkpoints,
                    s.core.m_resolved,
                    s.core.m_backfill_rows,
                    s.core.m_compacted,
                    u64::from(s.core.state),
                    u64::from(s.core.ring_len()),
                    s.core.cur_revision,
                    s.core.frontier,
                ],
            );
        }
    }
}
