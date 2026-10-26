//! route_catalogue_front — the route catalogue as an exchange provider.
//!
//! This file is deliberately thin: channels, params, the syscall table.
//! The requests, statuses and the publication, snapshot and
//! garbage-collection machines live in `modules/common/route_catalogue_front.rs`
//! ([`route_catalogue_front::Front`]) and `modules/common/route_catalogue.rs`,
//! which the host suite drives against a real store
//! (`tests/harness/tests/contract_route_catalogue.rs`).
//!
//! # Ports
//!
//! The exchange provider pair `request_in` (`ExchangeRequest`) /
//! `response_out` (`ExchangeResponse`), and a router anchor pair: `kv_out`
//! carries `MSG_KV_REQUEST` (stamped with the `proto` param) into a free
//! client pair's input of `kv_request_router`, `kv_in` takes
//! `MSG_KV_RESPONSE` from the same pair's output. `metrics` carries the
//! counters. A module with the router pair unwired, `request_in` wired and
//! `response_out` not, or a `proto` that names no client pair, refuses to
//! construct.
//!
//! Any of the router's four client pairs carries the front, each the same
//! KV surface: the router answers a request on the pair it came in by.
//!
//! | Pair | `kv_out` to | `kv_in` from | `proto` |
//! |---|---|---|---|
//! | etcd | `etcd_in` (input 0) | `etcd_out` (output 3) | 1 |
//! | redis | `redis_in` (input 1) | `redis_out` (output 4) | 2 |
//! | memcached | `memcached_in` (input 2) | `memcached_out` (output 5) | 3 |
//! | memcached_udp | `memcached_udp_in` (input 3) | `memcached_udp_out` (output 6) | 4 |
//!
//! The pair must be free: a protocol anchor on the same pair would share
//! its answers. The router's internal caller pairs (`internal_*`) are not
//! client pairs: each belongs to its own caller.
//!
//! # Params
//!
//! | Tag | Name | Type | Default | Description |
//! |---|---|---|---|---|
//! | 1 | proto | u32 | 1 | Protocol byte of the wired client pair (1 etcd, 2 redis, 3 memcached, 4 memcached_udp); any other value refuses construction |

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

#[path = "../../common/route_catalogue.rs"]
mod route_catalogue;

#[path = "../../common/route_catalogue_front.rs"]
mod route_catalogue_front;
use route_catalogue_front::exchange::RECORD_MAX;
use route_catalogue_front::{Front, FrontIo};

#[path = "../../common/wire.rs"]
mod wire;
use wire::{FRAME_PAYLOAD_MAX, MSG_KV_REQUEST, MSG_KV_RESPONSE};

use route_catalogue::types::{PROTO_ETCD, PROTO_MEMCACHED_UDP};

#[path = "../../common/envelope.rs"]
mod envelope;
use envelope::{read_envelope, Inbox, ENVELOPE_HDR};

#[path = "../../common/telemetry.rs"]
mod telemetry;

/// The `MSG_KV_RESPONSE` head: corr, conn, result, revision, body length.
const KV_RESP_HEAD: usize = 8 + 1 + 1 + 8 + 2;
/// The `MSG_KV_REQUEST` head: corr, protocol, tenant, conn, consistency,
/// op, body length.
const KV_REQ_HEAD: usize = 18;
/// One incoming KV response, whole.
const KV_IN_MAX: usize = ENVELOPE_HDR + FRAME_PAYLOAD_MAX;
/// One outgoing KV request: the largest body the machines send.
const KV_OUT_MAX: usize = ENVELOPE_HDR + KV_REQ_HEAD + route_catalogue::KV_BODY_MAX;
// The router takes a request whose payload fits its ingress and whose
// re-headed command fits the same scratch; it discards a larger one
// unanswered, which would hold `in_flight` forever. The machines refuse a
// body over `KV_BODY_MAX` before sending, so every request they send is
// carried.
const _: () = assert!(KV_REQ_HEAD + route_catalogue::KV_BODY_MAX <= wire::KV_REQUEST_PAYLOAD_MAX);
const _: () = assert!(
    ENVELOPE_HDR + wire::KvCommandHead::LEN + route_catalogue::KV_BODY_MAX
        <= wire::KV_REQUEST_PAYLOAD_MAX
);
/// Milliseconds between metric emissions.
const METRICS_EVERY_MS: u64 = 1000;

#[repr(C)]
struct FrontState {
    syscalls: *const SyscallTable,
    request_in: i32,
    response_out: i32,
    kv_in: i32,
    kv_out: i32,
    metrics_out: i32,
    kv_inbox: Inbox,
    draining: u8,
    proto: u8,
    corr: u64,
    /// The store request in flight's correlation (0: none), and the
    /// front's epoch it was sent under.
    in_flight: u64,
    in_flight_epoch: u32,
    last_metrics_ms: u64,
    outbox: ExchangeOutbox,
    front: Front,
    kv_buf: [u8; KV_IN_MAX],
    kv_frame: [u8; KV_OUT_MAX],
    rec: [u8; RECORD_MAX],
    out: [u8; RECORD_MAX],
}

define_params! {
    FrontState;

    1, proto, u32, 1
        => |s, d, len| { s.proto = u8::try_from(p_u32(d, len, 0, 1)).unwrap_or(0); };
}

/// [`FrontIo`] over the kernel channel syscalls: one `MSG_KV_REQUEST` per
/// request, under a fresh correlation.
struct ShellIo<'a> {
    sys: &'a SyscallTable,
    kv_out: i32,
    proto: u8,
    corr: &'a mut u64,
    in_flight: &'a mut u64,
    in_flight_epoch: &'a mut u32,
    /// The front's epoch while it steps: the request it sends for.
    epoch: u32,
    frame: &'a mut [u8; KV_OUT_MAX],
}

impl FrontIo for ShellIo<'_> {
    fn send_kv(&mut self, op: u8, body: &[u8]) -> bool {
        if *self.in_flight != 0 {
            return false;
        }
        let e = ENVELOPE_HDR;
        let payload = KV_REQ_HEAD + body.len();
        if e + payload > self.frame.len() || payload > u16::MAX as usize {
            return false;
        }
        *self.corr = self.corr.wrapping_add(1).max(1);
        let corr = *self.corr;
        let f = &mut *self.frame;
        f[0] = MSG_KV_REQUEST;
        f[1..3].copy_from_slice(&(payload as u16).to_le_bytes());
        f[e..e + 8].copy_from_slice(&corr.to_le_bytes());
        f[e + 8] = self.proto;
        f[e + 9..e + 13].copy_from_slice(&0u32.to_le_bytes());
        f[e + 13] = 0;
        f[e + 14] = 0;
        f[e + 15] = op;
        f[e + 16..e + 18].copy_from_slice(&(body.len() as u16).to_le_bytes());
        f[e + 18..e + payload].copy_from_slice(body);
        let n = e + payload;
        // SAFETY: `frame` is a live, exclusively borrowed buffer of at least
        // `n` bytes; the kernel write only reads it.
        let ok = unsafe { (self.sys.channel_write)(self.kv_out, f.as_mut_ptr(), n) == n as i32 };
        if ok {
            *self.in_flight = corr;
            *self.in_flight_epoch = self.epoch;
        }
        ok
    }
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<FrontState>() as u32
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
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<FrontState>() {
            return -2;
        }
        let s = &mut *(state as *mut FrontState);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        // inputs: request_in[0], kv_in[1]; outputs: response_out[0],
        // kv_out[1], metrics[2]
        s.request_in = in_chan;
        s.response_out = out_chan;
        s.kv_in = dev_channel_port(sys, 0, 1);
        s.kv_out = dev_channel_port(sys, 1, 1);
        s.metrics_out = dev_channel_port(sys, 1, 2);
        s.kv_inbox = Inbox::new();
        s.draining = 0;
        s.proto = 1;
        s.corr = 0;
        s.in_flight = 0;
        s.in_flight_epoch = 0;
        s.last_metrics_ms = 0;
        s.outbox = ExchangeOutbox::new();
        s.front.reset();
        parse_tlv(s, params, params_len);
        if s.kv_in < 0 || s.kv_out < 0 {
            let m = b"[route_catalogue_front] refusing to construct: kv_in and kv_out must both be wired to a kv_request_router pair";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        if !(PROTO_ETCD..=PROTO_MEMCACHED_UDP).contains(&s.proto) {
            let m = b"[route_catalogue_front] refusing to construct: proto must name a client pair of kv_request_router (1 etcd, 2 redis, 3 memcached, 4 memcached_udp)";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        if s.request_in >= 0 && s.response_out < 0 {
            let m = b"[route_catalogue_front] refusing to construct: request_in is wired but response_out is not";
            dev_log(sys, 2, m.as_ptr(), m.len());
            return -1;
        }
        dev_log(sys, 3, b"[route_catalogue_front] init".as_ptr(), 28);
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_drain"]
pub extern "C" fn module_drain(state: *mut u8) -> i32 {
    unsafe {
        (*(state as *mut FrontState)).draining = 1;
        0
    }
}

/// Place the record owed, if the port has room.
unsafe fn flush_records(s: &mut FrontState) {
    let sys = &*s.syscalls;
    if s.response_out < 0 {
        return;
    }
    if !s.outbox.flush(sys, s.response_out, &s.out) {
        return;
    }
    let Some(n) = s.front.next_record(&mut s.out) else {
        return;
    };
    let _ = s.outbox.send(sys, s.response_out, &s.out, n);
}

/// Read request records while the front can take them.
unsafe fn read_requests(s: &mut FrontState, now: u64) {
    let sys = &*s.syscalls;
    if s.request_in < 0 {
        return;
    }
    while s.front.can_take() {
        let poll = (sys.channel_poll)(s.request_in, POLL_IN);
        if poll <= 0 || (poll as u32 & POLL_IN) == 0 {
            return;
        }
        let n = (sys.channel_read)(s.request_in, s.rec.as_mut_ptr(), RECORD_MAX);
        if n <= 0 {
            return;
        }
        s.front.accept(&s.rec[..n as usize], now);
    }
}

/// Hand the router's answer to the front: the one in flight, by its
/// correlation. An answer under any other correlation is dropped, and so is
/// the one in flight when the request it was sent for has ended (aborted,
/// or another taken since): it frees the store slot for the next request
/// but is never handed to that request's machine.
unsafe fn read_answers(s: &mut FrontState) {
    let sys = &*s.syscalls;
    while let Some((mt, len)) = read_envelope(&mut s.kv_inbox, sys, s.kv_in, &mut s.kv_buf) {
        if mt != MSG_KV_RESPONSE || len < KV_RESP_HEAD {
            continue;
        }
        let b = &s.kv_buf[..len];
        let corr = u64::from_le_bytes(b[0..8].try_into().unwrap_or([0; 8]));
        if corr != s.in_flight {
            continue;
        }
        let result = b[9];
        let revision = u64::from_le_bytes(b[10..18].try_into().unwrap_or([0; 8]));
        let body_len = u16::from_le_bytes([b[18], b[19]]) as usize;
        let Some(body) = b.get(KV_RESP_HEAD..KV_RESP_HEAD + body_len) else {
            continue;
        };
        s.in_flight = 0;
        if s.in_flight_epoch != s.front.epoch() {
            continue;
        }
        s.front.on_kv_response(result, revision, body);
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        let s = &mut *(state as *mut FrontState);
        let sys = &*s.syscalls;
        let now = dev_millis(sys);

        // 1. The record owed first.
        flush_records(s);
        if s.draining == 1 {
            s.front.drain();
        }

        // 2. Requests in, the router's answer in, the next store request out.
        read_requests(s, now);
        read_answers(s);
        {
            let epoch = s.front.epoch();
            let FrontState {
                kv_out,
                proto,
                corr,
                in_flight,
                in_flight_epoch,
                kv_frame,
                front,
                ..
            } = s;
            let mut io = ShellIo {
                sys,
                kv_out: *kv_out,
                proto: *proto,
                corr,
                in_flight,
                in_flight_epoch,
                epoch,
                frame: kv_frame,
            };
            front.step(&mut io, now);
        }
        flush_records(s);

        // 3. Metrics on a wall-clock cadence.
        if s.metrics_out >= 0 && now.saturating_sub(s.last_metrics_ms) >= METRICS_EVERY_MS {
            s.last_metrics_ms = now;
            telemetry::emit_counters(
                sys,
                s.metrics_out,
                &[
                    s.front.m_activated,
                    s.front.m_refused,
                    s.front.m_pages,
                    s.front.m_gc,
                ],
            );
        }

        // 4. Drain: once nothing is held or owed, report Done.
        if s.draining == 1 && s.front.idle() && !s.outbox.holding() {
            return 1;
        }
        0
    }
}
