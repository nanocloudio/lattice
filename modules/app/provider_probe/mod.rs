//! provider_probe — writes and reads through the store contracts the
//! graph gives it (see manifest.toml).
//!
//! Each write is built in this module's state (key, value, fence), so the
//! request it asks again with names the same buffers with the same
//! contents, as a pending write requires.

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

#[path = "../../common/held_read.rs"]
mod held_read;
use held_read::{HeldRead, Read};

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_CHANGES: u32 = 0x1307;
const EINPROGRESS: i32 = -36;
const ENXIO: i32 = -6;
/// A read's "not yet": the store answers it later, and nothing may be
/// concluded from it (never absent, never empty, never short).
const EAGAIN: i32 = -11;

/// What a write answered: decided, or not yet (ask again).
enum WriteAnswer {
    Decided(i32),
    Pending,
}

fn write_answer(rc: i32) -> WriteAnswer {
    if rc == EINPROGRESS {
        WriteAnswer::Pending
    } else {
        WriteAnswer::Decided(rc)
    }
}

const FENCE_CAP: usize = 64;
const KEY_CAP: usize = 32;
const VAL_CAP: usize = 32;
/// LIST and CHANGES answers, and one large value read back.
const PAGE: usize = 256 * 1024;
const BIG_MAX: usize = 4 << 20;
/// Steps between checks of the store.
const VERIFY_EVERY: u64 = 500;

/// The page for a large value of `big` bytes: a LIST or CHANGES answer, and
/// any node's large value read back whole.
const fn page_bytes(big: usize) -> usize {
    if big > PAGE {
        big
    } else {
        PAGE
    }
}

/// The heap: the page and the large value written, which live together for
/// the module's life, so they are ONE allocation carved in two. The heap may
/// be elastic, growing in segments an allocation cannot span: taken one by
/// one, the second would find the first's segment tail too short and the
/// cap reached. The cap is the largest configuration's allocation and the
/// allocator's rounding.
const ARENA_BYTES: usize = page_bytes(BIG_MAX) + BIG_MAX + 64 * 1024;
declare_module_arena_bytes!(ARENA_BYTES);

const PHASE_WRITE: u8 = 0;
const PHASE_BIG: u8 = 1;
const PHASE_VERIFY: u8 = 2;

#[repr(C)]
struct ProbeState {
    syscalls: *const SyscallTable,
    // ── params ──
    node: u8,
    nodes: u8,
    writes: u16,
    big: u32,
    // ── progress ──
    phase: u8,
    /// The write being asked (index), and whether its request is built.
    at: u16,
    built: bool,
    ok: u16,
    failed: u16,
    /// The first decided write's fence tag.
    fence_tag: u8,
    key_len: u8,
    val_len: u8,
    key: [u8; KEY_CAP],
    val: [u8; VAL_CAP],
    fence: [u8; FENCE_CAP],
    /// Heap: the large value written, and a page / read-back buffer.
    big_buf: *mut u8,
    page: *mut u8,
    step: u64,
    /// The last check's `(keys, changes, large values whole)`, logged
    /// only when it changes.
    last: u64,
    /// Each node's large value as last read here: its length, or the
    /// errno; and the nodes whose bytes were not its pattern.
    reads: [i32; MAX_NODES],
    wrong: u8,
    /// The nodes whose large values have read back whole (each is read
    /// until it has, and not after: a read is the whole value, expanded
    /// from its chunks in one step).
    whole: u8,
    /// The key count at the last check, and the change count last read.
    keys_seen: i32,
    changes_seen: i32,
    /// A check a read made wait, resumed next step rather than at the next
    /// `VERIFY_EVERY`: what it has read so far, and the large value read it
    /// holds (for node `read_node`).
    resume: bool,
    pass_keys: i32,
    keys_done: bool,
    changes_done: bool,
    read_node: u8,
    read: HeldRead,
}

/// Nodes whose large values are read back, at most.
const MAX_NODES: usize = 8;

define_params! {
    ProbeState;

    // This node's number: its keys are `/probe/n<node>/…`.
    1, node, u8, 0
        => |s, d, len| { s.node = p_u8(d, len, 0, 0); };

    // Writes of its own.
    2, writes, u16, 32
        => |s, d, len| { s.writes = p_u16(d, len, 0, 32); };

    // One large value of this many bytes (0: none).
    3, big, u32, 0
        => |s, d, len| { s.big = p_u32(d, len, 0, 0); };

    // Nodes whose large values are read back.
    4, nodes, u8, 3
        => |s, d, len| { s.nodes = p_u8(d, len, 0, 3); };
}

/// Decimal `v` at `buf[at..]`: the new end.
fn put_dec(buf: &mut [u8], at: usize, v: u64) -> usize {
    let mut digits = [0u8; 20];
    let mut d = 0;
    let mut x = v;
    loop {
        digits[d] = b'0' + (x % 10) as u8;
        x /= 10;
        d += 1;
        if x == 0 {
            break;
        }
    }
    let mut p = at;
    while d > 0 && p < buf.len() {
        d -= 1;
        buf[p] = digits[d];
        p += 1;
    }
    p
}

fn put_bytes(buf: &mut [u8], at: usize, src: &[u8]) -> usize {
    let mut p = at;
    for b in src {
        if p >= buf.len() {
            break;
        }
        buf[p] = *b;
        p += 1;
    }
    p
}

/// A large value's byte `i`, for node `node`.
fn big_byte(node: u8, i: usize) -> u8 {
    (i as u8)
        .wrapping_mul(31)
        .wrapping_add(node.wrapping_mul(7))
        ^ (i >> 9) as u8
}

/// `/probe/n<node>/` then `tail`.
fn probe_key(out: &mut [u8], node: u8, tail: &[u8]) -> usize {
    let mut n = put_bytes(out, 0, b"/probe/n");
    n = put_dec(out, n, u64::from(node));
    n = put_bytes(out, n, b"/");
    put_bytes(out, n, tail)
}

/// `/probe-big/n<node>`: a large value, apart from the keys counted (a
/// change read carries values).
fn big_key(out: &mut [u8], node: u8) -> usize {
    let n = put_bytes(out, 0, b"/probe-big/n");
    put_dec(out, n, u64::from(node))
}

unsafe fn log(sys: &SyscallTable, buf: &[u8], n: usize) {
    dev_log(sys, 2, buf.as_ptr(), n.min(buf.len()));
}

/// Ask PUT `key` = `value` (precondition ANY), the fence into `fence`.
unsafe fn put(
    sys: &SyscallTable,
    key: &[u8],
    value: &[u8],
    fence: &mut [u8; FENCE_CAP],
) -> WriteAnswer {
    let mut arg = [0u8; 96];
    let mut p = 0usize;
    arg[0..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    arg[p] = 0; // no content type
    p += 1;
    arg[p..p + 8].copy_from_slice(&(value.as_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 8].copy_from_slice(&(value.len() as u64).to_le_bytes());
    p += 8;
    arg[p] = 0; // precondition ANY
    arg[p + 1] = 0;
    p += 2;
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&(FENCE_CAP as u16).to_le_bytes());
    p += 2;
    write_answer((sys.provider_call)(-1, OBJ_PUT, arg.as_mut_ptr(), p))
}

/// The module's `provider_call`, as a held read calls it.
fn caller(sys: &SyscallTable) -> impl FnMut(i32, u32, &mut [u8]) -> i32 + '_ {
    move |h, op, arg: &mut [u8]| unsafe { (sys.provider_call)(h, op, arg.as_mut_ptr(), arg.len()) }
}

/// The keys under `prefix` (one LIST page into `page`), or a negative
/// errno.
unsafe fn count_keys(sys: &SyscallTable, prefix: &[u8], page: &mut [u8]) -> i32 {
    let mut arg = [0u8; 64];
    let mut p = 0usize;
    arg[0..2].copy_from_slice(&(prefix.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + prefix.len()].copy_from_slice(prefix);
    p += prefix.len();
    p += 2; // no cursor
    arg[p..p + 8].copy_from_slice(&(page.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 4].copy_from_slice(&(page.len() as u32).to_le_bytes());
    p += 4;
    p += 10; // no fence
    let n = (sys.provider_call)(-1, NS_LIST, arg.as_mut_ptr(), p);
    if n < 0 {
        return n;
    }
    let mut count = 0i32;
    let mut at = 0usize;
    let end = n as usize;
    while at + 1 < end {
        if page[at] == 0xFF && page[at + 1] == 0xFF {
            // The trailer: a cursor means the page did not hold them all.
            return if page.get(at + 2) == Some(&0) {
                count
            } else {
                -12
            };
        }
        at += 2 + page[at] as usize;
        count += 1;
    }
    -5
}

/// The changes under `prefix` after revision `since`, or a negative errno.
unsafe fn count_changes(sys: &SyscallTable, prefix: &[u8], since: u64, page: &mut [u8]) -> i32 {
    let mut arg = [0u8; 64];
    let mut p = 0usize;
    arg[0..2].copy_from_slice(&(prefix.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + prefix.len()].copy_from_slice(prefix);
    p += prefix.len();
    arg[p..p + 8].copy_from_slice(&since.to_le_bytes());
    p += 8;
    arg[p..p + 8].copy_from_slice(&(page.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 4].copy_from_slice(&(page.len() as u32).to_le_bytes());
    p += 4;
    p += 10; // no fence
    let n = (sys.provider_call)(-1, NS_CHANGES, arg.as_mut_ptr(), p);
    if n < 5 {
        return if n < 0 { n } else { -5 };
    }
    if page[0] != 0 {
        return -32; // the history no longer reaches it
    }
    u32::from_le_bytes([page[1], page[2], page[3], page[4]]) as i32
}

/// Build write `at`'s request: `/probe/n<node>/<at>` = `v<node>-<at>`.
fn build(s: &mut ProbeState) {
    let mut tail = [0u8; 8];
    let tn = put_dec(&mut tail, 0, u64::from(s.at));
    s.key_len = probe_key(&mut s.key, s.node, &tail[..tn]) as u8;
    let mut n = put_bytes(&mut s.val, 0, b"v");
    n = put_dec(&mut s.val, n, u64::from(s.node));
    n = put_bytes(&mut s.val, n, b"-");
    s.val_len = put_dec(&mut s.val, n, u64::from(s.at)) as u8;
    s.built = true;
}

/// One write's answer, counted.
unsafe fn decided(s: &mut ProbeState, sys: &SyscallTable, rc: i32) {
    if rc == 0 {
        s.ok += 1;
        if s.fence_tag == 0 {
            s.fence_tag = s.fence[0];
            let mut m = [0u8; 48];
            let mut n = put_bytes(&mut m, 0, b"[probe] n");
            n = put_dec(&mut m, n, u64::from(s.node));
            n = put_bytes(&mut m, n, b" fence=");
            n = put_dec(&mut m, n, u64::from(s.fence_tag));
            log(sys, &m, n);
        }
    } else {
        s.failed += 1;
        let mut m = [0u8; 64];
        let mut n = put_bytes(&mut m, 0, b"[probe] n");
        n = put_dec(&mut m, n, u64::from(s.node));
        n = put_bytes(&mut m, n, b" write failed rc=-");
        n = put_dec(&mut m, n, u64::from(rc.unsigned_abs()));
        log(sys, &m, n);
    }
}

unsafe fn step_write(s: &mut ProbeState, sys: &SyscallTable) {
    if s.at >= s.writes {
        let mut m = [0u8; 64];
        let mut n = put_bytes(&mut m, 0, b"[probe] n");
        n = put_dec(&mut m, n, u64::from(s.node));
        n = put_bytes(&mut m, n, b" wrote ok=");
        n = put_dec(&mut m, n, u64::from(s.ok));
        n = put_bytes(&mut m, n, b" failed=");
        n = put_dec(&mut m, n, u64::from(s.failed));
        log(sys, &m, n);
        s.phase = if s.big > 0 { PHASE_BIG } else { PHASE_VERIFY };
        return;
    }
    if !s.built {
        build(s);
    }
    let key = core::slice::from_raw_parts(s.key.as_ptr(), s.key_len as usize);
    let val = core::slice::from_raw_parts(s.val.as_ptr(), s.val_len as usize);
    match put(sys, key, val, &mut s.fence) {
        WriteAnswer::Pending => {}
        WriteAnswer::Decided(rc) => {
            decided(s, sys, rc);
            s.at += 1;
            s.built = false;
        }
    }
}

unsafe fn step_big(s: &mut ProbeState, sys: &SyscallTable) {
    if s.big_buf.is_null() {
        s.phase = PHASE_VERIFY;
        return;
    }
    let mut key = [0u8; KEY_CAP];
    let kn = big_key(&mut key, s.node);
    let val = core::slice::from_raw_parts(s.big_buf, s.big as usize);
    match put(sys, &key[..kn], val, &mut s.fence) {
        WriteAnswer::Pending => {}
        WriteAnswer::Decided(rc) => {
            decided(s, sys, rc);
            s.phase = PHASE_VERIFY;
        }
    }
}

/// Every node's large value that is present, read back whole: how many
/// were, or `None` while a read waits (it is held, and this pass resumes at
/// its node next step).
unsafe fn big_values_whole(s: &mut ProbeState, sys: &SyscallTable, page: &mut [u8]) -> Option<u32> {
    let nodes = s.nodes.min(MAX_NODES as u8);
    let first = if s.read.held() { s.read_node } else { 0 };
    if first == 0 {
        s.wrong = 0;
    }
    let mut call = caller(sys);
    for m in first..nodes {
        if s.whole & (1 << m) != 0 {
            continue;
        }
        let mut key = [0u8; KEY_CAP];
        let kn = big_key(&mut key, m);
        let n = match s.read.step(&mut call, &mut key[..kn], page) {
            Read::Pending => {
                s.read_node = m;
                return None;
            }
            Read::Done(n) => n as i32,
            Read::Failed(e) => e,
        };
        s.reads[m as usize] = n;
        if n != s.big as i32 {
            continue;
        }
        if page[..n as usize]
            .iter()
            .enumerate()
            .all(|(i, b)| *b == big_byte(m, i))
        {
            s.whole |= 1 << m;
        } else {
            s.wrong |= 1 << m;
        }
    }
    Some((s.whole & ((1u16 << nodes) - 1) as u8).count_ones())
}

unsafe fn step_verify(s: &mut ProbeState, sys: &SyscallTable) {
    if (!s.resume && !s.step.is_multiple_of(VERIFY_EVERY)) || s.page.is_null() {
        return;
    }
    // A read that answers EAGAIN decides nothing: the check is held where
    // it stopped (what it has read kept) and resumed next step.
    s.resume = true;
    let page = core::slice::from_raw_parts_mut(s.page, page_bytes(s.big as usize));
    if !s.keys_done {
        let keys = count_keys(sys, b"/probe/", page);
        if keys == EAGAIN {
            return;
        }
        s.pass_keys = keys;
        s.keys_done = true;
    }
    let keys = s.pass_keys;
    // Every change since the first revision: a write applied twice is a
    // key with two. A read of the whole history is the costliest call
    // here, so it is made once the keys have stopped arriving.
    if !s.changes_done && keys == s.keys_seen && keys > 0 {
        let changes = count_changes(sys, b"/probe/", 1, page);
        if changes == EAGAIN {
            return;
        }
        s.changes_seen = changes;
    }
    s.changes_done = true;
    let whole = if s.big > 0 {
        match big_values_whole(s, sys, page) {
            Some(w) => w,
            None => return,
        }
    } else {
        0
    };
    // The check is whole: the next starts afresh.
    s.resume = false;
    s.keys_done = false;
    s.changes_done = false;
    s.keys_seen = keys;
    let changes = s.changes_seen;
    // Logged when anything it reports has changed.
    let mut now =
        (u64::from(keys as u32) << 32) ^ u64::from(changes as u32) ^ (u64::from(whole) << 20);
    now ^= u64::from(s.wrong) << 40;
    for (k, r) in s.reads.iter().enumerate() {
        now = now.rotate_left(7) ^ u64::from(*r as u32) ^ k as u64;
    }
    if now == s.last {
        return;
    }
    s.last = now;
    let mut m = [0u8; 224];
    let mut n = put_bytes(&mut m, 0, b"[probe] n");
    n = put_dec(&mut m, n, u64::from(s.node));
    n = put_bytes(&mut m, n, b" keys=");
    n = if keys < 0 {
        let k = put_bytes(&mut m, n, b"-");
        put_dec(&mut m, k, u64::from(keys.unsigned_abs()))
    } else {
        put_dec(&mut m, n, keys as u64)
    };
    n = put_bytes(&mut m, n, b" changes=");
    n = if changes < 0 {
        let k = put_bytes(&mut m, n, b"-");
        put_dec(&mut m, k, u64::from(changes.unsigned_abs()))
    } else {
        put_dec(&mut m, n, changes as u64)
    };
    n = put_bytes(&mut m, n, b" big_whole=");
    n = put_dec(&mut m, n, u64::from(whole));
    n = put_bytes(&mut m, n, b" own_ok=");
    n = put_dec(&mut m, n, u64::from(s.ok));
    if s.big > 0 {
        n = put_bytes(&mut m, n, b" reads=");
        for k in 0..s.nodes.min(MAX_NODES as u8) as usize {
            let r = s.reads[k];
            if r < 0 {
                n = put_bytes(&mut m, n, b"-");
            }
            n = put_dec(&mut m, n, u64::from(r.unsigned_abs()));
            if s.wrong & (1 << k) != 0 {
                n = put_bytes(&mut m, n, b"!");
            }
            n = put_bytes(&mut m, n, b" ");
        }
    }
    log(sys, &m, n);
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<ProbeState>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    _in_chan: i32,
    _out_chan: i32,
    _ctrl_chan: i32,
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    if state.is_null() || syscalls.is_null() || state_size < core::mem::size_of::<ProbeState>() {
        return -1;
    }
    unsafe {
        let s = &mut *state.cast::<ProbeState>();
        let sys = &*syscalls.cast::<SyscallTable>();
        core::ptr::write_bytes(state, 0, core::mem::size_of::<ProbeState>());
        s.syscalls = sys;
        s.writes = 32;
        s.nodes = 3;
        parse_tlv(s, params, params_len);
        let big = (s.big as usize).min(BIG_MAX);
        s.big = big as u32;
        let page = page_bytes(big);
        // The page, then the large value: one allocation.
        let base = (sys.heap_alloc)((page + big) as u32);
        if base.is_null() {
            let m = b"[probe] heap refused";
            log(sys, m, m.len());
            return -1;
        }
        s.page = base;
        if big > 0 {
            s.big_buf = base.add(page);
            for i in 0..big {
                *s.big_buf.add(i) = big_byte(s.node, i);
            }
        }
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let s = unsafe { &mut *state.cast::<ProbeState>() };
    if s.syscalls.is_null() {
        return 0;
    }
    let sys = unsafe { &*s.syscalls };
    s.step = s.step.wrapping_add(1);
    unsafe {
        if s.phase == PHASE_WRITE {
            step_write(s, sys);
        } else if s.phase == PHASE_BIG {
            step_big(s, sys);
        } else {
            step_verify(s, sys);
        }
    }
    0
}
