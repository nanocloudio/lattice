//! ttl_scheduler — deterministic expiry service.
//!
//! Phase 4 module. Consumes deterministic-time tick envelopes
//! from the substrate (or, in single-node mode, from a graph-
//! local emitter wired to `tick_in`), maintains a deadline queue
//! of lease + KV-record expiries via the dual-target
//! `ttl_scheduler::TtlQueue` facade, and emits expiration events
//! back into the KV path / lease manager.
//!
//! Two-way contract:
//!
//! - `tick_in` carries `MSG_LEASE_TICK { tick_ms: u64 }`. The
//!   module updates its `now_ms` and drains any expired entries.
//! - `tick_out` re-emits the same tick to `lease_manager` so the
//!   lease module can advance its own deadline scan.
//! - `lease_deadlines` carries `MSG_LEASE_CTRL` envelopes —
//!   lease_manager forwards its grant/keepalive deadlines here so
//!   the central queue knows about them. (Lease_manager keeps its
//!   own per-record bookkeeping for the wire response; ttl_scheduler
//!   keeps the centralized priority queue.)
//! - `kv_expiry` carries KV-record expiry registrations from
//!   `kv_state_worker` (SET with EX/PX): body is
//!   `[deadline_ms:u64 LE][key_hash:u64 LE]`.
//! - `expire_out` emits expired-record events back into the KV
//!   path. Body: `[kind:u8][payload:u64 LE]` where kind matches
//!   `EXPIRY_KIND_*`.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface"
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

#[path = "../../common/ttl_scheduler.rs"]
mod ttl_scheduler;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use ttl_scheduler::{ExpiryEntry, TtlQueue, EXPIRY_KIND_KV, EXPIRY_KIND_LEASE};
use wire::{MSG_LEASE_CTRL, MSG_LEASE_TICK};

const SCRATCH_BUF_SIZE: usize = 4096;
const QUEUE_CAP: usize = 1024;

/// Wire constant for `expire_out` body shape — re-use the
/// `MSG_RETENTION_FLOOR`-adjacent block in `wire.rs` (0xE0..). We
/// re-purpose 0xE2 for now since `MSG_COMPACTION_FLOOR` is 0xE1
/// and we don't want to collide.
const MSG_TTL_EXPIRED: u8 = 0xE2;

#[repr(C)]
struct TtlState {
    syscalls: *const SyscallTable,

    tick_in: i32,
    lease_deadlines_in: i32,
    kv_expiry_in: i32,
    expire_out: i32,
    tick_out: i32,
    metrics_out: i32,

    /// Deterministic time advanced by the most recent tick.
    now_ms: u64,

    // Phase-14 telemetry. Monotonic counters emitted on `metrics_out` at
    // a coarse step cadence; ids follow the manifest `[observability]
    // metrics` order (0=scheduled, 1=expired).
    m_scheduled: u64,
    m_expired: u64,
    step_ctr: u64,

    queue: TtlQueue<QUEUE_CAP>,
    scratch: [u8; SCRATCH_BUF_SIZE],
}

impl TtlState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.tick_in = -1;
        self.lease_deadlines_in = -1;
        self.kv_expiry_in = -1;
        self.expire_out = -1;
        self.tick_out = -1;
        self.metrics_out = -1;
        self.now_ms = 0;
        self.m_scheduled = 0;
        self.m_expired = 0;
        self.step_ctr = 0;
        self.queue.init();
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

fn handle_tick(sched: &mut TtlState, payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let tick_ms = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    if tick_ms > sched.now_ms {
        sched.now_ms = tick_ms;
    }
    // Forward the tick to lease_manager so its own scan runs at
    // the same `now_ms`.
    unsafe {
        let sys = sched.syscalls;
        if !sys.is_null() {
            let mut body = [0u8; 8];
            body.copy_from_slice(&sched.now_ms.to_le_bytes());
            let _ = write_envelope(&*sys, sched.tick_out, MSG_LEASE_TICK, &body);
        }
    }
    // Drain expired entries.
    let mut fired: [ExpiryEntry; 32] = [ExpiryEntry::new(0, 0, 0); 32];
    let mut count = 0usize;
    sched.queue.drain_due(sched.now_ms, |entry| {
        if count < fired.len() {
            fired[count] = entry;
            count += 1;
        }
    });
    let mut i = 0;
    while i < count {
        emit_expired(sched, fired[i]);
        i += 1;
    }
}

fn emit_expired(sched: &mut TtlState, entry: ExpiryEntry) {
    sched.m_expired += 1;
    // [kind:1][payload:8] = 9 bytes
    let mut body = [0u8; 9];
    body[0] = entry.kind;
    body[1..9].copy_from_slice(&entry.payload.to_le_bytes());
    unsafe {
        let sys = sched.syscalls;
        if !sys.is_null() {
            let _ = write_envelope(&*sys, sched.expire_out, MSG_TTL_EXPIRED, &body);
        }
    }
}

fn handle_lease_deadline(sched: &mut TtlState, payload: &[u8]) {
    // Lease manager forwards its grant/keepalive: same body as
    // `MSG_LEASE_CTRL`. We register the deadline in our central
    // queue so we can drive the scan from one place. For grant /
    // keepalive (`ctrl 0 / 2`) insert; for revoke (`ctrl 1`)
    // remove.
    if payload.len() < 17 {
        return;
    }
    let ctrl = payload[0];
    let lease_id = u64::from_le_bytes([
        payload[1], payload[2], payload[3], payload[4], payload[5], payload[6], payload[7],
        payload[8],
    ]);
    let ttl_ms = u32::from_le_bytes([payload[9], payload[10], payload[11], payload[12]]);
    match ctrl {
        0 | 2 => {
            // Grant or keepalive: (re)insert with `now + ttl`.
            let _ = sched.queue.remove(EXPIRY_KIND_LEASE, lease_id);
            let deadline = sched.now_ms.saturating_add(u64::from(ttl_ms));
            let _ = sched
                .queue
                .insert(ExpiryEntry::new(deadline, EXPIRY_KIND_LEASE, lease_id));
            sched.m_scheduled += 1;
        }
        1 => {
            let _ = sched.queue.remove(EXPIRY_KIND_LEASE, lease_id);
        }
        _ => {}
    }
}

fn handle_kv_expiry(sched: &mut TtlState, payload: &[u8]) {
    // [deadline_ms:8][key_hash:8]
    if payload.len() < 16 {
        return;
    }
    let deadline = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    let key_hash = u64::from_le_bytes([
        payload[8],
        payload[9],
        payload[10],
        payload[11],
        payload[12],
        payload[13],
        payload[14],
        payload[15],
    ]);
    if deadline == 0 {
        // 0 means "cancel any pending expiry for this key".
        let _ = sched.queue.remove(EXPIRY_KIND_KV, key_hash);
    } else {
        let _ = sched.queue.remove(EXPIRY_KIND_KV, key_hash);
        let _ = sched
            .queue
            .insert(ExpiryEntry::new(deadline, EXPIRY_KIND_KV, key_hash));
        sched.m_scheduled += 1;
    }
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<TtlState>() as u32
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
    _params: *const u8,
    _params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    if state.is_null() || syscalls.is_null() {
        return -1;
    }
    if state_size < core::mem::size_of::<TtlState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let sched = unsafe { &mut *state.cast::<TtlState>() };
    sched.init(sys_ptr);

    // inputs: tick_in[0], lease_deadlines[1], kv_expiry[2]
    // outputs: expire_out[0], tick_out[1], metrics[2]
    sched.tick_in = in_chan;
    sched.expire_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        sched.lease_deadlines_in = dev_channel_port(sys, 0, 1);
        sched.kv_expiry_in = dev_channel_port(sys, 0, 2);
        sched.tick_out = dev_channel_port(sys, 1, 1);
        sched.metrics_out = dev_channel_port(sys, 1, 2);
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let sched = unsafe { &mut *state.cast::<TtlState>() };
    unsafe {
        let sys_ptr = sched.syscalls;
        if sys_ptr.is_null() {
            return 0;
        }
        let sys = &*sys_ptr;
        if let Some((mt, len)) = read_envelope(sys, sched.tick_in, &mut sched.scratch) {
            if mt == MSG_LEASE_TICK {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&sched.scratch[..len]);
                handle_tick(sched, &tmp[..len]);
            }
        }
        if let Some((mt, len)) = read_envelope(sys, sched.lease_deadlines_in, &mut sched.scratch) {
            if mt == MSG_LEASE_CTRL {
                let mut tmp = [0u8; SCRATCH_BUF_SIZE];
                tmp[..len].copy_from_slice(&sched.scratch[..len]);
                handle_lease_deadline(sched, &tmp[..len]);
            }
        }
        if let Some((_mt, len)) = read_envelope(sys, sched.kv_expiry_in, &mut sched.scratch) {
            let mut tmp = [0u8; SCRATCH_BUF_SIZE];
            tmp[..len].copy_from_slice(&sched.scratch[..len]);
            handle_kv_expiry(sched, &tmp[..len]);
        }

        // Phase-14: emit module-scope counters on `metrics_out` at a
        // coarse cadence (no-op until the port is wired). ids follow the
        // manifest `[observability] metrics` order.
        sched.step_ctr = sched.step_ctr.wrapping_add(1);
        if sched.step_ctr.is_multiple_of(5000) && !sched.syscalls.is_null() {
            telemetry::emit_counters(
                &*sched.syscalls,
                sched.metrics_out,
                &[sched.m_scheduled, sched.m_expired],
            );
        }
    }
    0
}
