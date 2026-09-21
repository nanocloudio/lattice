//! Prometheus-compatible metrics anchor — remote-write ingest and metrics
//! queries over the Prometheus HTTP API, one dialect surface over the
//! canonical KV store's time-series keyspace. Query languages are parsed and
//! lowered by front-ends (`metricsql_front` for PromQL/MetricsQL, `kql_front`
//! for KQL) onto the language-agnostic `tsquery_core`; this module owns only
//! the HTTP transport and the KV plumbing that resolves, folds, and
//! aggregates series.
//!
//! Routes:
//!   `POST /api/v1/write`        — Snappy-framed protobuf `WriteRequest`;
//!                                 each series' identity + samples become a
//!                                 series-directory record and sample puts in
//!                                 one transaction.
//!   `GET  /api/v1/query_range`  — PromQL/MetricsQL `query` over a
//!                                 `start`/`end`/`step` grid; resolves
//!                                 matching series through the directory,
//!                                 folds each series' window per step
//!                                 (cross-series aggregation included), and
//!                                 streams the Prometheus `matrix`.
//!   `GET  /api/v1/query`        — instant form: evaluates at `time` and
//!                                 streams a `vector`.
//!   `GET  /api/v1/kql`          — a KQL `query` over the same grid
//!                                 parameters; the tabular pipe lowers onto
//!                                 the same core.
//!   `GET  /api/v1/labels`,
//!   `GET  /api/v1/series`       — discovery over the series directory.
//!   `GET  /-/healthy`           — liveness probe.
//!
//! A query outside a front-end's subset is refused by name rather than
//! mis-served — every other lattice surface holds the same rule.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    clippy::duplicate_mod,
    reason = "PIC build path-mounts the fluxor SDK wholesale and shares common files via #[path]; each module consumes only a subset of the ABI surface"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    clippy::too_many_arguments,
    reason = "fluxor module ABI: raw-pointer entry points are the contract and ABI fns carry a fixed arity"
)]
use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/http_min.rs"]
mod http_min;
#[path = "../../common/models.rs"]
mod models;
#[path = "../../common/prom_remote_write.rs"]
mod prom_remote_write;
#[path = "../../common/snappy_min.rs"]
mod snappy_min;
#[path = "../../common/telemetry.rs"]
mod telemetry;
#[path = "../../common/types.rs"]
mod types;
#[path = "../../common/wire.rs"]
mod wire;
// MetricsQL carries the PromQL front-end as `metricsql_front::promql`; the
// anchor uses that one copy for PromQL/MetricsQL and its core (`tq`).
#[path = "../../common/metricsql_front.rs"]
mod metricsql_front;
use metricsql_front::promql as promql_front;
#[path = "../../common/kql_front.rs"]
mod kql_front;
#[path = "../../common/prom_result.rs"]
mod prom_result;

use abi::contracts::net::net_proto::{
    conn_id, CMD_BIND as NET_CMD_BIND, CMD_CLOSE as NET_CMD_CLOSE, CMD_SEND as NET_CMD_SEND,
    CONN_ID_LEN, MSG_ACCEPTED as NET_MSG_ACCEPTED, MSG_BOUND as NET_MSG_BOUND,
    MSG_CLOSED as NET_MSG_CLOSED, MSG_DATA as NET_MSG_DATA, MSG_ERROR as NET_MSG_ERROR,
};
use types::{
    KV_OP_RANGE_SCAN, KV_OP_TXN, KV_RESULT_OK, KV_RESULT_RANGE, KV_RESULT_TXN, PROTO_MEMCACHED,
};
use wire::{MSG_KV_REQUEST, MSG_KV_RESPONSE};

// Core types via one of the mounted front-ends (all mount the same source).
use promql_front::core as tq;

// ── Capacities ───────────────────────────────────────────────────────

const MAX_CONNS: usize = 8;
const RECV_BUF: usize = 16384;
const SEND_BUF: usize = 20480;
const SCRATCH_BUF: usize = 32768;
const RESP_BUF: usize = 20480;
const SLOT_FREE: u16 = 0xFFFF;
const DEFAULT_LISTEN_PORT: u16 = 9090;
const KEY_MAX: usize = 512;

/// Bounded live query dimensions — a query exceeding any is refused.
const MAX_SERIES_Q: usize = 32;
const MAX_STEPS_Q: usize = 128;
const MAX_SAMPLES_Q: usize = 2048;
const MATCHER_BUF: usize = 512;
const SERIES_LBL_BUF: usize = 4096;

const S_READY: u8 = 0;
const S_BUSY: u8 = 1;
const S_CLOSING: u8 = 2;

const D_IDLE: u8 = 0;
const D_WRITE: u8 = 1; // ingest TXN awaiting ack
const D_DIR: u8 = 2; // series-directory scan page
const D_SAMP: u8 = 3; // per-series sample-window scan page
const D_LABELS: u8 = 4; // full-directory scan collecting distinct label names

const PHASE_INIT: u8 = 0;
const PHASE_WAIT_BOUND: u8 = 1;
const PHASE_LISTENING: u8 = 2;

// Temporal op tags stored in owned query state.
const T_LAST: u8 = 0;
const T_RATE: u8 = 1;

#[repr(C)]
struct Slot {
    conn_id: u16,
    state: u8,
    recv: [u8; RECV_BUF],
    recv_len: usize,
    send: [u8; SEND_BUF],
    send_len: usize,
}

impl Slot {
    const fn free() -> Self {
        Slot {
            conn_id: SLOT_FREE,
            state: S_READY,
            recv: [0; RECV_BUF],
            recv_len: 0,
            send: [0; SEND_BUF],
            send_len: 0,
        }
    }
}

define_params! {
    AnchorState;

    1, listen_port, u16, 9090
        => |s, d, len| { s.listen_port = p_u16(d, len, 0, 9090); };
}

#[repr(C)]
struct AnchorState {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    kv_in: i32,
    kv_out: i32,
    metrics_out: i32,

    listen_port: u16,
    phase: u8,
    server_conn_id: u16,
    slots: [Slot; MAX_CONNS],

    d_phase: u8,
    d_slot: u8,
    kv_corr: u64,
    cursor: u64,
    scan_start: [u8; KEY_MAX],
    scan_start_len: u16,
    scan_end: [u8; KEY_MAX],
    scan_end_len: u16,

    // Owned query context (survives the async scans).
    q_metric_id: u32,
    q_metric_name: [u8; 128],
    q_metric_name_len: u8,
    q_matchers: [u8; MATCHER_BUF], // [nlen u8][name][op u8][vlen u8][val]…
    q_matchers_n: u8,
    q_temporal: u8,
    q_instant: bool,
    q_series_meta: bool, // /api/v1/series: render matched series' labels, no samples
    q_range_ms: u64,
    q_start_ms: u64,
    q_end_ms: u64,
    q_step_ms: u64,
    q_steps: u16,

    // Series collected from the directory scan.
    series_id: [u64; MAX_SERIES_Q],
    series_lbl: [u8; SERIES_LBL_BUF], // per series: preimage bytes
    series_lbl_off: [u16; MAX_SERIES_Q + 1],
    series_n: u16,
    cur_series: u16,

    // Cross-series aggregation (`sum by (...)`).
    q_has_grouping: bool,
    q_without: bool,
    q_combine: u8,      // 0 Sum, 1 Min, 2 Max, 3 Avg, 4 Count
    q_group: [u8; 256], // group label NAMES: [nlen u8][name]…
    q_group_len: u16,
    q_group_n: u8,
    series_group: [u16; MAX_SERIES_Q],
    group_lbl: [u8; SERIES_LBL_BUF], // per group: output label bytes
    group_off: [u16; MAX_SERIES_Q + 1],
    group_n: u16,

    // /api/v1/labels: distinct label names collected during a full scan,
    // serialized `[nlen u8][name]…`.
    lbl_names: [u8; 2048],
    lbl_names_len: u16,

    // Current series' fetched samples.
    samp_ts: [u64; MAX_SAMPLES_Q],
    samp_val: [f64; MAX_SAMPLES_Q],
    samp_n: u16,

    // Computed step matrix: value + presence per (series, step).
    step_val: [[f64; MAX_STEPS_Q]; MAX_SERIES_Q],
    step_present: [[bool; MAX_STEPS_Q]; MAX_SERIES_Q],

    env: [u8; SCRATCH_BUF],
    scratch: [u8; SCRATCH_BUF],
    m_sessions: u64,
    m_commands: u64,
    m_errors: u64,
    step_ctr: u64,
}

impl AnchorState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.net_in = -1;
        self.net_out = -1;
        self.kv_in = -1;
        self.kv_out = -1;
        self.metrics_out = -1;
        self.listen_port = DEFAULT_LISTEN_PORT;
        self.phase = PHASE_INIT;
        self.server_conn_id = SLOT_FREE;
        self.slots = [const { Slot::free() }; MAX_CONNS];
        self.d_phase = D_IDLE;
        self.d_slot = 0;
        self.kv_corr = 0;
        self.cursor = 0;
        self.scan_start = [0; KEY_MAX];
        self.scan_start_len = 0;
        self.scan_end = [0; KEY_MAX];
        self.scan_end_len = 0;
        self.q_metric_id = 0;
        self.q_metric_name = [0; 128];
        self.q_metric_name_len = 0;
        self.q_matchers = [0; MATCHER_BUF];
        self.q_matchers_n = 0;
        self.q_temporal = T_LAST;
        self.q_instant = false;
        self.q_series_meta = false;
        self.q_range_ms = 0;
        self.q_start_ms = 0;
        self.q_end_ms = 0;
        self.q_step_ms = 0;
        self.q_steps = 0;
        self.series_id = [0; MAX_SERIES_Q];
        self.series_lbl = [0; SERIES_LBL_BUF];
        self.series_lbl_off = [0; MAX_SERIES_Q + 1];
        self.series_n = 0;
        self.cur_series = 0;
        self.q_has_grouping = false;
        self.q_without = false;
        self.q_combine = 0;
        self.q_group = [0; 256];
        self.q_group_len = 0;
        self.q_group_n = 0;
        self.series_group = [0; MAX_SERIES_Q];
        self.group_lbl = [0; SERIES_LBL_BUF];
        self.group_off = [0; MAX_SERIES_Q + 1];
        self.group_n = 0;
        self.lbl_names = [0; 2048];
        self.lbl_names_len = 0;
        self.samp_ts = [0; MAX_SAMPLES_Q];
        self.samp_val = [0.0; MAX_SAMPLES_Q];
        self.samp_n = 0;
        self.step_val = [[0.0; MAX_STEPS_Q]; MAX_SERIES_Q];
        self.step_present = [[false; MAX_STEPS_Q]; MAX_SERIES_Q];
        self.env = [0; SCRATCH_BUF];
        self.scratch = [0; SCRATCH_BUF];
        self.m_sessions = 0;
        self.m_commands = 0;
        self.m_errors = 0;
        self.step_ctr = 0;
    }

    fn find_slot(&self, conn_id: u16) -> Option<usize> {
        self.slots
            .iter()
            .position(|s| s.conn_id == conn_id && s.conn_id != SLOT_FREE)
    }

    fn alloc_slot(&mut self, conn_id: u16) -> Option<usize> {
        let idx = self.slots.iter().position(|s| s.conn_id == SLOT_FREE)?;
        self.slots[idx] = Slot::free();
        self.slots[idx].conn_id = conn_id;
        self.m_sessions = self.m_sessions.wrapping_add(1);
        Some(idx)
    }

    fn free_slot(&mut self, idx: usize) {
        if self.d_phase != D_IDLE && self.d_slot as usize == idx {
            self.d_phase = D_IDLE;
        }
        self.slots[idx] = Slot::free();
    }
}

// ── Net + KV plumbing (mirrors the established anchor pattern) ─────────

unsafe fn net_send(anchor: &mut AnchorState, cmd: u8, conn_id: u16, data: &[u8]) -> bool {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.net_out < 0 {
        return false;
    }
    let payload_len = CONN_ID_LEN + data.len();
    if payload_len + NET_FRAME_HDR > SCRATCH_BUF {
        return false;
    }
    let id = conn_id.to_le_bytes();
    let scratch = anchor.scratch.as_mut_ptr();
    *scratch = cmd;
    *scratch.add(1) = (payload_len & 0xFF) as u8;
    *scratch.add(2) = ((payload_len >> 8) & 0xFF) as u8;
    *scratch.add(NET_FRAME_HDR) = id[0];
    *scratch.add(NET_FRAME_HDR + 1) = id[1];
    if !data.is_empty() {
        core::ptr::copy_nonoverlapping(
            data.as_ptr(),
            scratch.add(NET_FRAME_HDR + CONN_ID_LEN),
            data.len(),
        );
    }
    let total = NET_FRAME_HDR + payload_len;
    ((*sys).channel_write)(anchor.net_out, scratch, total) == total as i32
}

unsafe fn net_bind(anchor: &mut AnchorState) -> bool {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.net_out < 0 {
        return false;
    }
    let port = anchor.listen_port.to_le_bytes();
    let scratch = anchor.scratch.as_mut_ptr();
    *scratch = NET_CMD_BIND;
    *scratch.add(1) = 2;
    *scratch.add(2) = 0;
    *scratch.add(NET_FRAME_HDR) = port[0];
    *scratch.add(NET_FRAME_HDR + 1) = port[1];
    let total = NET_FRAME_HDR + 2;
    ((*sys).channel_write)(anchor.net_out, scratch, total) == total as i32
}

fn send_to_slot(anchor: &mut AnchorState, idx: usize, data: &[u8]) {
    let slot = &mut anchor.slots[idx];
    if slot.send_len + data.len() > SEND_BUF {
        slot.state = S_CLOSING;
        return;
    }
    let at = slot.send_len;
    slot.send[at..at + data.len()].copy_from_slice(data);
    slot.send_len += data.len();
    flush_slot(anchor, idx);
}

fn flush_slot(anchor: &mut AnchorState, idx: usize) {
    let (conn_id, len) = (anchor.slots[idx].conn_id, anchor.slots[idx].send_len);
    if len == 0 {
        return;
    }
    let mut buf = [0u8; SEND_BUF];
    buf[..len].copy_from_slice(&anchor.slots[idx].send[..len]);
    let sent = unsafe { net_send(anchor, NET_CMD_SEND, conn_id, &buf[..len]) };
    if sent {
        anchor.slots[idx].send_len = 0;
    }
}

const BODY_AT: usize = wire::ENVELOPE_HDR + 18;

fn kv_send(anchor: &mut AnchorState, op: u8, body_len: usize, next: u8) -> bool {
    anchor.kv_corr = anchor.kv_corr.wrapping_add(1).max(1);
    const REQ_HEAD: usize = 18;
    let at = wire::ENVELOPE_HDR;
    if at + REQ_HEAD + body_len > anchor.env.len() {
        return false;
    }
    anchor.env[at..at + 8].copy_from_slice(&anchor.kv_corr.to_le_bytes());
    anchor.env[at + 8] = PROTO_MEMCACHED;
    anchor.env[at + 9..at + 13].copy_from_slice(&0u32.to_le_bytes());
    anchor.env[at + 13] = 0;
    anchor.env[at + 14] = 0;
    anchor.env[at + 15] = op;
    anchor.env[at + 16..at + 18].copy_from_slice(&(body_len as u16).to_le_bytes());
    let sent = unsafe {
        let sys = anchor.syscalls;
        !sys.is_null()
            && write_envelope(
                &*sys,
                anchor.kv_out,
                MSG_KV_REQUEST,
                REQ_HEAD + body_len,
                &mut anchor.env,
            )
    };
    if sent {
        anchor.d_phase = next;
    }
    sent
}

fn stage_scan_bounds(anchor: &mut AnchorState) -> Option<usize> {
    let sn = anchor.scan_start_len as usize;
    let en = anchor.scan_end_len as usize;
    let cursor = anchor.cursor;
    let need = 2 + sn + 2 + en + 10;
    if BODY_AT + need > anchor.env.len() {
        return None;
    }
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&(sn as u16).to_le_bytes());
    p += 2;
    anchor.env[p..p + sn].copy_from_slice(&anchor.scan_start[..sn]);
    p += sn;
    anchor.env[p..p + 2].copy_from_slice(&(en as u16).to_le_bytes());
    p += 2;
    anchor.env[p..p + en].copy_from_slice(&anchor.scan_end[..en]);
    p += en;
    anchor.env[p..p + 8].copy_from_slice(&cursor.to_le_bytes());
    p += 8;
    anchor.env[p..p + 2].copy_from_slice(&64u16.to_le_bytes());
    Some(need)
}

fn user_key(out: &mut [u8], keyspace: u32, body: &[u8]) -> Option<usize> {
    let need = 4 + body.len();
    if out.len() < need {
        return None;
    }
    out[0..4].copy_from_slice(&keyspace.to_be_bytes());
    out[4..need].copy_from_slice(body);
    Some(need)
}

fn prefix_successor(prefix: &[u8], out: &mut [u8]) -> Option<usize> {
    if prefix.len() > out.len() {
        return None;
    }
    let mut n = prefix.len();
    while n > 0 {
        if prefix[n - 1] != 0xFF {
            out[..n].copy_from_slice(&prefix[..n]);
            out[n - 1] += 1;
            return Some(n);
        }
        n -= 1;
    }
    None
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

unsafe fn read_one_envelope<'a>(
    sys: &SyscallTable,
    chan: i32,
    scratch: &'a mut [u8],
) -> Option<(u8, &'a [u8])> {
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
    let payload_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if payload_len > scratch.len() {
        return None;
    }
    if payload_len > 0
        && ((sys.channel_read)(chan, scratch.as_mut_ptr(), payload_len) as usize) < payload_len
    {
        return None;
    }
    Some((hdr[0], &scratch[..payload_len]))
}

// ── HTTP replies ─────────────────────────────────────────────────────

fn reply_http(anchor: &mut AnchorState, status: u16, reason: &[u8], ctype: &[u8], body: &[u8]) {
    let idx = anchor.d_slot as usize;
    anchor.d_phase = D_IDLE;
    if anchor.slots[idx].conn_id != SLOT_FREE && anchor.slots[idx].state == S_BUSY {
        anchor.slots[idx].state = S_READY;
    }
    let mut out = [0u8; SEND_BUF];
    if let Some(n) = http_min::write_response(&mut out, status, reason, ctype, body) {
        send_to_slot(anchor, idx, &out[..n]);
    }
    // We advertise `Connection: close`; close the connection once the
    // response has been queued so the client sees EOF and no request is
    // served on a half-closed socket.
    if anchor.slots[idx].conn_id != SLOT_FREE {
        anchor.slots[idx].state = S_CLOSING;
    }
}

fn reply_error(anchor: &mut AnchorState, status: u16, reason: &[u8], msg: &[u8]) {
    anchor.m_errors = anchor.m_errors.wrapping_add(1);
    // Prometheus error envelope.
    let mut body = [0u8; 256];
    let mut n = 0;
    let pre = b"{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"";
    body[n..n + pre.len()].copy_from_slice(pre);
    n += pre.len();
    let m = msg.len().min(body.len() - n - 3);
    body[n..n + m].copy_from_slice(&msg[..m]);
    n += m;
    body[n..n + 3].copy_from_slice(b"\"}}");
    n += 3;
    let mut f = [0u8; 256];
    f[..n].copy_from_slice(&body[..n]);
    reply_http(anchor, status, reason, b"application/json", &f[..n]);
}

// ── Request dispatch ─────────────────────────────────────────────────

fn begin_request(anchor: &mut AnchorState, idx: usize, req: &http_min::Request<'_>) {
    anchor.d_slot = idx as u8;
    anchor.slots[idx].state = S_BUSY;
    anchor.m_commands = anchor.m_commands.wrapping_add(1);

    if req.method == b"POST" && req.path == b"/api/v1/write" {
        handle_write(anchor, req);
        return;
    }
    if req.method == b"GET" && (req.path == b"/api/v1/query_range" || req.path == b"/api/v1/query")
    {
        handle_query(anchor, req);
        return;
    }
    if req.method == b"GET" && req.path == b"/api/v1/kql" {
        handle_kql(anchor, req);
        return;
    }
    if req.method == b"GET" && req.path == b"/api/v1/labels" {
        handle_labels(anchor);
        return;
    }
    if req.method == b"GET" && req.path == b"/api/v1/series" {
        handle_series(anchor, req);
        return;
    }
    if req.method == b"GET" && req.path == b"/-/healthy" {
        reply_http(anchor, 200, b"OK", b"text/plain", b"ok");
        return;
    }
    reply_error(anchor, 404, b"Not Found", b"unknown route");
}

// ── Ingest: remote-write → series directory + sample puts ─────────────

fn handle_write(anchor: &mut AnchorState, req: &http_min::Request<'_>) {
    // Snappy-decompress if the body is encoded (Prometheus always is).
    let mut raw = [0u8; SCRATCH_BUF / 2];
    let body: &[u8] = if req.content_encoding == b"snappy"
        || req.content_encoding.is_empty() && looks_snappy(req.body)
    {
        match snappy_min::decode(req.body, &mut raw) {
            Some(n) => &raw[..n],
            None => {
                // Maybe it was not actually compressed; fall back to raw.
                req.body
            }
        }
    } else {
        req.body
    };

    // Build one TXN: [cmp=0][then=N][puts…][else=0]. Count puts as we go.
    let end_guard = anchor.env.len() - 4;
    let mut p = BODY_AT;
    anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // cmp
    p += 2;
    let then_at = p;
    p += 2; // then count backfilled
    let mut puts: u16 = 0;

    let mut reader = prom_remote_write::Reader::new(body);
    let mut labels = [prom_remote_write::core::Label {
        name: b"",
        value: b"",
    }; tq::MAX_LABELS];
    let mut samples = [prom_remote_write::Sample {
        ts_ms: 0,
        value: 0.0,
    }; 256];
    loop {
        match reader.next_series(&mut labels, &mut samples) {
            Ok(Some((nl, ns))) => {
                // Canonicalise: find __name__, sort the rest by name.
                let mut order = [0u8; tq::MAX_LABELS];
                let (metric, mo, mn) = canonicalize(&labels[..nl], &mut order);
                if metric.is_empty() {
                    continue;
                }
                let mid = prom_remote_write::core::metric_id(metric);
                // Digest over the sorted non-name labels.
                let mut sorted = [prom_remote_write::core::Label {
                    name: b"",
                    value: b"",
                }; tq::MAX_LABELS];
                for i in 0..mn {
                    sorted[i] = labels[order[i] as usize];
                }
                let dig = prom_remote_write::core::labels_digest(&sorted[..mn]);
                let sid = prom_remote_write::core::series_id(mid, &dig);
                let _ = mo;

                // Series-directory record: key [mid][dig], value = preimage.
                let mut skbody = [0u8; models::TIMESERIES_SERIES_KEY_LEN];
                let Some(skn) = models::encode_timeseries_series_key(&mut skbody, mid, &dig) else {
                    continue;
                };
                let mut skey = [0u8; KEY_MAX];
                let Some(skn2) = user_key(&mut skey, models::KS_TIMESERIES_SERIES, &skbody[..skn])
                else {
                    continue;
                };
                let mut preimage = [0u8; 512];
                let pn = encode_preimage(&sorted[..mn], &mut preimage);
                match put_op(anchor, p, end_guard, &skey[..skn2], &preimage[..pn]) {
                    Some(np) => {
                        p = np;
                        puts += 1;
                    }
                    None => break,
                }

                // Sample puts under KS_TIMESERIES_SAMPLE[sid][0][ts].
                for s in &samples[..ns] {
                    let ts = if s.ts_ms < 0 { 0u64 } else { s.ts_ms as u64 };
                    let mut kbody = [0u8; models::TIMESERIES_SAMPLE_KEY_LEN];
                    let Some(kn) = models::encode_timeseries_sample_key(&mut kbody, sid, 0, ts)
                    else {
                        continue;
                    };
                    let mut key = [0u8; KEY_MAX];
                    let Some(kn2) = user_key(&mut key, models::KS_TIMESERIES_SAMPLE, &kbody[..kn])
                    else {
                        continue;
                    };
                    let val = s.value.to_le_bytes();
                    match put_op(anchor, p, end_guard, &key[..kn2], &val) {
                        Some(np) => {
                            p = np;
                            puts += 1;
                        }
                        None => break,
                    }
                }
            }
            Ok(None) => break,
            Err(_) => {
                reply_error(anchor, 400, b"Bad Request", b"malformed remote_write");
                return;
            }
        }
    }

    anchor.env[then_at..then_at + 2].copy_from_slice(&puts.to_le_bytes());
    anchor.env[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // else
    p += 2;
    if puts == 0 {
        reply_http(anchor, 204, b"No Content", b"text/plain", b"");
        return;
    }
    if !kv_send(anchor, KV_OP_TXN, p - BODY_AT, D_WRITE) {
        reply_error(anchor, 500, b"Internal Server Error", b"store unavailable");
    }
}

/// Append one PUT into a TXN body at `p`: [op][oplen u16][klen u16][key][vlen u32][value][flags u8][ttl u64].
fn put_op(
    anchor: &mut AnchorState,
    mut p: usize,
    end_guard: usize,
    key: &[u8],
    value: &[u8],
) -> Option<usize> {
    let put_len = 2 + key.len() + 4 + value.len() + 1 + 8;
    if p + 3 + put_len > end_guard {
        return None;
    }
    anchor.env[p] = types::KV_OP_PUT;
    p += 1;
    anchor.env[p..p + 2].copy_from_slice(&(put_len as u16).to_le_bytes());
    p += 2;
    anchor.env[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    anchor.env[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    anchor.env[p..p + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
    p += 4;
    anchor.env[p..p + value.len()].copy_from_slice(value);
    p += value.len();
    anchor.env[p] = 0;
    p += 1;
    anchor.env[p..p + 8].copy_from_slice(&0u64.to_le_bytes());
    p += 8;
    Some(p)
}

// ── Query: PromQL → directory scan → sample scans → matrix ───────────

fn handle_query(anchor: &mut AnchorState, req: &http_min::Request<'_>) {
    // Decode the query params.
    let mut qbuf = [0u8; 1024];
    let query = match http_min::find_param(req.query, b"query") {
        Some(raw) => match http_min::percent_decode(raw, &mut qbuf) {
            Some(n) => &qbuf[..n],
            None => {
                reply_error(anchor, 400, b"Bad Request", b"bad query encoding");
                return;
            }
        },
        None => {
            reply_error(anchor, 400, b"Bad Request", b"missing query");
            return;
        }
    };
    // An instant query (`/api/v1/query`) evaluates at a single `time` and
    // returns a `vector`; a range query sweeps `start..end` by `step` and
    // returns a `matrix`.
    let instant = req.path == b"/api/v1/query";
    let (start_ms, end_ms, step_ms) = if instant {
        let t = param_seconds_to_ms(req.query, b"time", 0);
        (t, t, 1)
    } else {
        let start = param_seconds_to_ms(req.query, b"start", 0);
        let end = param_seconds_to_ms(req.query, b"end", start);
        let step = {
            let s = param_seconds_to_ms(req.query, b"step", 15_000);
            if s == 0 {
                15_000
            } else {
                s
            }
        };
        (start, end, step)
    };

    // Parse + lower via MetricsQL — a PromQL superset, so this endpoint
    // accepts both PromQL and MetricsQL over the same core.
    let mut scratch = [0u8; 1024];
    let mut mb = [promql_front::core::Matcher {
        name: b"",
        op: promql_front::core::MatchOp::Eq,
        value: b"",
    }; tq::MAX_MATCHERS];
    let mut gb: [&[u8]; tq::MAX_LABELS] = [b""; tq::MAX_LABELS];
    let parsed = match metricsql_front::lower(query, &mut scratch, &mut mb, &mut gb) {
        Ok(p) => p,
        Err(_) => {
            reply_error(anchor, 400, b"Bad Request", b"unsupported PromQL/MetricsQL");
            return;
        }
    };
    let q = parsed.into_query(&mb, &gb, start_ms, end_ms, step_ms);
    let steps = match q.step_count() {
        Ok(n) if n <= MAX_STEPS_Q => n,
        _ => {
            reply_error(anchor, 400, b"Bad Request", b"query grid too large");
            return;
        }
    };
    // Own the grouping (cross-series aggregation), if any.
    anchor.q_has_grouping = false;
    anchor.q_group_n = 0;
    anchor.q_group_len = 0;
    if let Some(g) = q.grouping {
        anchor.q_has_grouping = true;
        anchor.q_without = g.without;
        anchor.q_combine = match g.combine {
            promql_front::core::SpatialOp::Sum => 0,
            promql_front::core::SpatialOp::Min => 1,
            promql_front::core::SpatialOp::Max => 2,
            promql_front::core::SpatialOp::Avg => 3,
            promql_front::core::SpatialOp::Count => 4,
        };
        let mut gp = 0usize;
        for name in g.group {
            if gp + 1 + name.len() > anchor.q_group.len() {
                reply_error(anchor, 400, b"Bad Request", b"grouping too large");
                return;
            }
            anchor.q_group[gp] = name.len() as u8;
            gp += 1;
            anchor.q_group[gp..gp + name.len()].copy_from_slice(name);
            gp += name.len();
            anchor.q_group_n += 1;
        }
        anchor.q_group_len = gp as u16;
    }

    // Own the query context across the async scans.
    anchor.q_metric_id = q.metric_id;
    // Recover the metric name from the selector: it is the leading ident of
    // the query (before `{` / `(`), needed to render __name__.
    let mn = extract_metric_name(query);
    let ml = mn.len().min(anchor.q_metric_name.len());
    anchor.q_metric_name[..ml].copy_from_slice(&mn[..ml]);
    anchor.q_metric_name_len = ml as u8;
    // Serialize matchers into owned bytes.
    let mut mp = 0usize;
    for m in q.matchers {
        let op = match m.op {
            promql_front::core::MatchOp::Eq => 0u8,
            promql_front::core::MatchOp::Ne => 1u8,
        };
        if mp + 3 + m.name.len() + m.value.len() > MATCHER_BUF {
            reply_error(anchor, 400, b"Bad Request", b"selector too large");
            return;
        }
        anchor.q_matchers[mp] = m.name.len() as u8;
        mp += 1;
        anchor.q_matchers[mp..mp + m.name.len()].copy_from_slice(m.name);
        mp += m.name.len();
        anchor.q_matchers[mp] = op;
        mp += 1;
        anchor.q_matchers[mp] = m.value.len() as u8;
        mp += 1;
        anchor.q_matchers[mp..mp + m.value.len()].copy_from_slice(m.value);
        mp += m.value.len();
    }
    anchor.q_matchers_n = q.matchers.len() as u8;
    anchor.q_temporal = match q.temporal {
        promql_front::core::TemporalOp::Rate { .. } => T_RATE,
        _ => T_LAST,
    };
    anchor.q_range_ms = q.range_ms;
    anchor.q_instant = instant;
    anchor.q_series_meta = false;
    // An instant bare selector returns the latest sample within a staleness
    // window before `time` (Prometheus default 5m), not a 1ms step.
    if instant && anchor.q_temporal == T_LAST {
        anchor.q_range_ms = 300_000;
    }
    anchor.q_start_ms = start_ms;
    anchor.q_end_ms = end_ms;
    anchor.q_step_ms = step_ms;
    anchor.q_steps = steps as u16;
    begin_dir_scan(anchor);
}

/// Reset series collection and kick off the directory scan over
/// `KS_TIMESERIES_SERIES` with the (already-stored) `q_metric_id` prefix.
/// Shared by every language front-end once it has stored the owned query.
fn begin_dir_scan(anchor: &mut AnchorState) {
    anchor.series_n = 0;
    anchor.cur_series = 0;
    anchor.series_lbl_off[0] = 0;
    let mid_be = anchor.q_metric_id.to_be_bytes();
    let mut startk = [0u8; KEY_MAX];
    let Some(sn) = user_key(&mut startk, models::KS_TIMESERIES_SERIES, &mid_be) else {
        reply_error(anchor, 500, b"Internal Server Error", b"key");
        return;
    };
    let mut endk = [0u8; KEY_MAX];
    let Some(en) = prefix_successor(&startk[..sn], &mut endk) else {
        reply_error(anchor, 500, b"Internal Server Error", b"key");
        return;
    };
    anchor.scan_start[..sn].copy_from_slice(&startk[..sn]);
    anchor.scan_start_len = sn as u16;
    anchor.scan_end[..en].copy_from_slice(&endk[..en]);
    anchor.scan_end_len = en as u16;
    anchor.cursor = 0;
    let Some(bn) = stage_scan_bounds(anchor) else {
        reply_error(anchor, 500, b"Internal Server Error", b"scan");
        return;
    };
    if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_DIR) {
        reply_error(anchor, 500, b"Internal Server Error", b"scan");
    }
}

/// KQL query handler — parses the Kusto pipe form and lowers it onto the same
/// owned query + directory scan every language shares. Endpoint: `/api/v1/kql`.
fn handle_kql(anchor: &mut AnchorState, req: &http_min::Request<'_>) {
    let mut qbuf = [0u8; 1024];
    let query = match http_min::find_param(req.query, b"query") {
        Some(raw) => match http_min::percent_decode(raw, &mut qbuf) {
            Some(n) => &qbuf[..n],
            None => {
                reply_error(anchor, 400, b"Bad Request", b"bad query encoding");
                return;
            }
        },
        None => {
            reply_error(anchor, 400, b"Bad Request", b"missing query");
            return;
        }
    };
    let start_ms = param_seconds_to_ms(req.query, b"start", 0);
    let end_ms = param_seconds_to_ms(req.query, b"end", start_ms);
    let step_ms = {
        let s = param_seconds_to_ms(req.query, b"step", 15_000);
        if s == 0 {
            15_000
        } else {
            s
        }
    };

    let mut mb = [kql_front::core::Matcher {
        name: b"",
        op: kql_front::core::MatchOp::Eq,
        value: b"",
    }; 16];
    let mut gb: [&[u8]; 32] = [b""; 32];
    let parsed = match kql_front::lower(query, &mut mb, &mut gb) {
        Ok(p) => p,
        Err(_) => {
            reply_error(anchor, 400, b"Bad Request", b"unsupported KQL");
            return;
        }
    };
    let q = parsed.into_query(&mb, &gb, start_ms, end_ms, step_ms);
    let steps = match q.step_count() {
        Ok(n) if n <= MAX_STEPS_Q => n,
        _ => {
            reply_error(anchor, 400, b"Bad Request", b"query grid too large");
            return;
        }
    };

    // Grouping.
    anchor.q_has_grouping = false;
    anchor.q_group_n = 0;
    anchor.q_group_len = 0;
    if let Some(g) = q.grouping {
        anchor.q_has_grouping = true;
        anchor.q_without = g.without;
        anchor.q_combine = match g.combine {
            kql_front::core::SpatialOp::Sum => 0,
            kql_front::core::SpatialOp::Min => 1,
            kql_front::core::SpatialOp::Max => 2,
            kql_front::core::SpatialOp::Avg => 3,
            kql_front::core::SpatialOp::Count => 4,
        };
        let mut gp = 0usize;
        for name in g.group {
            if gp + 1 + name.len() > anchor.q_group.len() {
                reply_error(anchor, 400, b"Bad Request", b"grouping too large");
                return;
            }
            anchor.q_group[gp] = name.len() as u8;
            gp += 1;
            anchor.q_group[gp..gp + name.len()].copy_from_slice(name);
            gp += name.len();
            anchor.q_group_n += 1;
        }
        anchor.q_group_len = gp as u16;
    }

    // The KQL table name is the metric; render it as __name__ for non-agg.
    anchor.q_metric_id = q.metric_id;
    let mn = kql_table_name(query);
    let ml = mn.len().min(anchor.q_metric_name.len());
    anchor.q_metric_name[..ml].copy_from_slice(&mn[..ml]);
    anchor.q_metric_name_len = ml as u8;

    let mut mp = 0usize;
    for m in q.matchers {
        let op = match m.op {
            kql_front::core::MatchOp::Eq => 0u8,
            kql_front::core::MatchOp::Ne => 1u8,
        };
        if mp + 3 + m.name.len() + m.value.len() > MATCHER_BUF {
            reply_error(anchor, 400, b"Bad Request", b"selector too large");
            return;
        }
        anchor.q_matchers[mp] = m.name.len() as u8;
        mp += 1;
        anchor.q_matchers[mp..mp + m.name.len()].copy_from_slice(m.name);
        mp += m.name.len();
        anchor.q_matchers[mp] = op;
        mp += 1;
        anchor.q_matchers[mp] = m.value.len() as u8;
        mp += 1;
        anchor.q_matchers[mp..mp + m.value.len()].copy_from_slice(m.value);
        mp += m.value.len();
    }
    anchor.q_matchers_n = q.matchers.len() as u8;
    anchor.q_temporal = T_LAST; // KQL v1 lowers to instant/last
    anchor.q_instant = false; // KQL endpoint serves ranged matrices
    anchor.q_series_meta = false;
    anchor.q_range_ms = q.range_ms;
    anchor.q_start_ms = start_ms;
    anchor.q_end_ms = end_ms;
    anchor.q_step_ms = step_ms;
    anchor.q_steps = steps as u16;
    begin_dir_scan(anchor);
}

/// The leading table identifier of a KQL query (before the first `|`).
fn kql_table_name(query: &[u8]) -> &[u8] {
    let mut i = 0;
    while i < query.len() && (query[i] == b' ' || query[i] == b'\t') {
        i += 1;
    }
    let start = i;
    while i < query.len() && (query[i].is_ascii_alphanumeric() || query[i] == b'_') {
        i += 1;
    }
    &query[start..i]
}

fn on_kv_response(anchor: &mut AnchorState, result: u8, body: &[u8]) {
    match anchor.d_phase {
        D_WRITE => match result {
            KV_RESULT_TXN if !body.is_empty() && body[0] == 1 => {
                reply_http(anchor, 204, b"No Content", b"text/plain", b"")
            }
            KV_RESULT_OK => reply_http(anchor, 204, b"No Content", b"text/plain", b""),
            _ => reply_error(
                anchor,
                500,
                b"Internal Server Error",
                b"store rejected write",
            ),
        },
        D_DIR => on_dir_page(anchor, result, body),
        D_SAMP => on_samp_page(anchor, result, body),
        D_LABELS => on_labels_page(anchor, result, body),
        _ => {}
    }
}

/// `GET /api/v1/labels` — scan the whole series directory and return the
/// distinct label names (plus the reserved `__name__`).
fn handle_labels(anchor: &mut AnchorState) {
    anchor.lbl_names_len = 0;
    let mut startk = [0u8; KEY_MAX];
    let Some(sn) = user_key(&mut startk, models::KS_TIMESERIES_SERIES, &[]) else {
        reply_error(anchor, 500, b"Internal Server Error", b"key");
        return;
    };
    let mut endk = [0u8; KEY_MAX];
    let Some(en) = prefix_successor(&startk[..sn], &mut endk) else {
        reply_error(anchor, 500, b"Internal Server Error", b"key");
        return;
    };
    anchor.scan_start[..sn].copy_from_slice(&startk[..sn]);
    anchor.scan_start_len = sn as u16;
    anchor.scan_end[..en].copy_from_slice(&endk[..en]);
    anchor.scan_end_len = en as u16;
    anchor.cursor = 0;
    let Some(bn) = stage_scan_bounds(anchor) else {
        reply_error(anchor, 500, b"Internal Server Error", b"scan");
        return;
    };
    if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_LABELS) {
        reply_error(anchor, 500, b"Internal Server Error", b"scan");
    }
}

fn on_labels_page(anchor: &mut AnchorState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        reply_error(anchor, 500, b"Internal Server Error", b"labels scan");
        return;
    }
    anchor.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    let mut at = 10usize;
    for _ in 0..count {
        let Some((_kbody, value, next)) = read_kv(body, at) else {
            reply_error(anchor, 500, b"Internal Server Error", b"labels page");
            return;
        };
        at = next;
        // Preimage: `[nlen][name][vlen][val]…` — collect each distinct name.
        let mut p = 0usize;
        while p < value.len() {
            let nl = value[p] as usize;
            p += 1;
            if p + nl > value.len() {
                break;
            }
            let name = &value[p..p + nl];
            p += nl;
            if p >= value.len() {
                break;
            }
            let vl = value[p] as usize;
            p += 1;
            if p + vl > value.len() {
                break;
            }
            p += vl;
            add_label_name(anchor, name);
        }
    }
    if anchor.cursor != 0 {
        let Some(bn) = stage_scan_bounds(anchor) else {
            reply_error(anchor, 500, b"Internal Server Error", b"scan");
            return;
        };
        if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_LABELS) {
            reply_error(anchor, 500, b"Internal Server Error", b"scan");
        }
        return;
    }
    render_labels(anchor);
}

fn add_label_name(anchor: &mut AnchorState, name: &[u8]) {
    // Already present?
    let mut p = 0usize;
    while p < anchor.lbl_names_len as usize {
        let nl = anchor.lbl_names[p] as usize;
        p += 1;
        if &anchor.lbl_names[p..p + nl] == name {
            return;
        }
        p += nl;
    }
    let at = anchor.lbl_names_len as usize;
    if name.len() > 255 || at + 1 + name.len() > anchor.lbl_names.len() {
        return;
    }
    anchor.lbl_names[at] = name.len() as u8;
    anchor.lbl_names[at + 1..at + 1 + name.len()].copy_from_slice(name);
    anchor.lbl_names_len = (at + 1 + name.len()) as u16;
}

fn render_labels(anchor: &mut AnchorState) {
    let mut out = [0u8; RESP_BUF];
    let mut o = 0usize;
    let pre = b"{\"status\":\"success\",\"data\":[\"__name__\"";
    out[o..o + pre.len()].copy_from_slice(pre);
    o += pre.len();
    let mut p = 0usize;
    while p < anchor.lbl_names_len as usize {
        let nl = anchor.lbl_names[p] as usize;
        p += 1;
        let name = &anchor.lbl_names[p..p + nl];
        p += nl;
        if o + 3 + nl > out.len() {
            break;
        }
        out[o] = b',';
        out[o + 1] = b'"';
        o += 2;
        out[o..o + nl].copy_from_slice(name);
        o += nl;
        out[o] = b'"';
        o += 1;
    }
    if o + 2 <= out.len() {
        out[o..o + 2].copy_from_slice(b"]}");
        o += 2;
    }
    let mut f = [0u8; RESP_BUF];
    f[..o].copy_from_slice(&out[..o]);
    reply_http(anchor, 200, b"OK", b"application/json", &f[..o]);
}

fn on_dir_page(anchor: &mut AnchorState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        reply_error(anchor, 500, b"Internal Server Error", b"dir scan");
        return;
    }
    anchor.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    let mut at = 10usize;
    for _ in 0..count {
        let Some((kbody, value, next)) = read_kv(body, at) else {
            reply_error(anchor, 500, b"Internal Server Error", b"dir page");
            return;
        };
        at = next;
        let Some((mid, dig)) = models::decode_timeseries_series_key(kbody) else {
            continue;
        };
        // Check matchers against the preimage labels. A query that
        // matches more series than the anchor can materialise is refused
        // whole rather than answered from a silently truncated set —
        // an answer missing series is wrong, not merely partial.
        if preimage_matches(anchor, value) {
            if (anchor.series_n as usize) >= MAX_SERIES_Q {
                reply_error(anchor, 422, b"Unprocessable Entity", b"too many series");
                return;
            }
            let sid = tq::series_id(mid, &dig);
            let i = anchor.series_n as usize;
            let off = anchor.series_lbl_off[i] as usize;
            if off + value.len() > SERIES_LBL_BUF {
                reply_error(
                    anchor,
                    422,
                    b"Unprocessable Entity",
                    b"series labels too large",
                );
                return;
            }
            anchor.series_id[i] = sid;
            anchor.series_lbl[off..off + value.len()].copy_from_slice(value);
            anchor.series_lbl_off[i + 1] = (off + value.len()) as u16;
            anchor.series_n += 1;
        }
    }
    if anchor.cursor != 0 {
        let Some(bn) = stage_scan_bounds(anchor) else {
            reply_error(anchor, 500, b"Internal Server Error", b"scan");
            return;
        };
        if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_DIR) {
            reply_error(anchor, 500, b"Internal Server Error", b"scan");
        }
        return;
    }
    // /api/v1/series: the matched series' label sets are the answer — no
    // sample scans needed.
    if anchor.q_series_meta {
        render_series_meta(anchor);
        return;
    }
    // Directory done → begin per-series sample scans (or render empty).
    anchor.cur_series = 0;
    if anchor.series_n == 0 {
        render_matrix(anchor);
        return;
    }
    if anchor.q_has_grouping && !compute_groups(anchor) {
        return;
    }
    start_sample_scan(anchor);
}

/// `GET /api/v1/series?match[]=<selector>` — the label sets of series matching
/// the selector (Prometheus series metadata).
fn handle_series(anchor: &mut AnchorState, req: &http_min::Request<'_>) {
    let mut qbuf = [0u8; 1024];
    let sel = match http_min::find_param(req.query, b"match[]")
        .or_else(|| http_min::find_param(req.query, b"match"))
    {
        Some(raw) => match http_min::percent_decode(raw, &mut qbuf) {
            Some(n) => &qbuf[..n],
            None => {
                reply_error(anchor, 400, b"Bad Request", b"bad match encoding");
                return;
            }
        },
        None => {
            reply_error(anchor, 400, b"Bad Request", b"missing match[]");
            return;
        }
    };
    let mut scratch = [0u8; 1024];
    let mut mb = [promql_front::core::Matcher {
        name: b"",
        op: promql_front::core::MatchOp::Eq,
        value: b"",
    }; tq::MAX_MATCHERS];
    let mut gb: [&[u8]; tq::MAX_LABELS] = [b""; tq::MAX_LABELS];
    let parsed = match metricsql_front::lower(sel, &mut scratch, &mut mb, &mut gb) {
        Ok(p) => p,
        Err(_) => {
            reply_error(anchor, 400, b"Bad Request", b"unsupported match selector");
            return;
        }
    };
    let q = parsed.into_query(&mb, &gb, 0, 0, 1);

    anchor.q_metric_id = q.metric_id;
    let mn = extract_metric_name(sel);
    let ml = mn.len().min(anchor.q_metric_name.len());
    anchor.q_metric_name[..ml].copy_from_slice(&mn[..ml]);
    anchor.q_metric_name_len = ml as u8;
    let mut mp = 0usize;
    for m in q.matchers {
        let op = match m.op {
            promql_front::core::MatchOp::Eq => 0u8,
            promql_front::core::MatchOp::Ne => 1u8,
        };
        if mp + 3 + m.name.len() + m.value.len() > MATCHER_BUF {
            reply_error(anchor, 400, b"Bad Request", b"selector too large");
            return;
        }
        anchor.q_matchers[mp] = m.name.len() as u8;
        mp += 1;
        anchor.q_matchers[mp..mp + m.name.len()].copy_from_slice(m.name);
        mp += m.name.len();
        anchor.q_matchers[mp] = op;
        mp += 1;
        anchor.q_matchers[mp] = m.value.len() as u8;
        mp += 1;
        anchor.q_matchers[mp..mp + m.value.len()].copy_from_slice(m.value);
        mp += m.value.len();
    }
    anchor.q_matchers_n = q.matchers.len() as u8;
    anchor.q_has_grouping = false;
    anchor.q_series_meta = true;
    begin_dir_scan(anchor);
}

/// Render matched series' label sets: `{"status":"success","data":[{"__name__":
/// "up","job":"api"}, …]}`.
fn render_series_meta(anchor: &mut AnchorState) {
    let name_len = anchor.q_metric_name_len as usize;
    let mut out = [0u8; RESP_BUF];
    let mut o = 0usize;
    let pre = b"{\"status\":\"success\",\"data\":[";
    out[..pre.len()].copy_from_slice(pre);
    o += pre.len();
    for i in 0..anchor.series_n as usize {
        if i > 0 && o < out.len() {
            out[o] = b',';
            o += 1;
        }
        // `{"__name__":"<name>"` + each preimage label.
        let head = b"{\"__name__\":\"";
        if o + head.len() + name_len + 1 > out.len() {
            break;
        }
        out[o..o + head.len()].copy_from_slice(head);
        o += head.len();
        out[o..o + name_len].copy_from_slice(&anchor.q_metric_name[..name_len]);
        o += name_len;
        out[o] = b'"';
        o += 1;
        let lo = anchor.series_lbl_off[i] as usize;
        let hi = anchor.series_lbl_off[i + 1] as usize;
        let preimage = &anchor.series_lbl[lo..hi];
        let mut p = 0usize;
        while p < preimage.len() {
            let nl = preimage[p] as usize;
            p += 1;
            if p + nl > preimage.len() {
                break;
            }
            let lname = &preimage[p..p + nl];
            p += nl;
            if p >= preimage.len() {
                break;
            }
            let vl = preimage[p] as usize;
            p += 1;
            if p + vl > preimage.len() {
                break;
            }
            let lval = &preimage[p..p + vl];
            p += vl;
            if o + 4 + nl + vl > out.len() {
                break;
            }
            out[o] = b',';
            out[o + 1] = b'"';
            o += 2;
            out[o..o + nl].copy_from_slice(lname);
            o += nl;
            let mid = b"\":\"";
            out[o..o + mid.len()].copy_from_slice(mid);
            o += mid.len();
            out[o..o + vl].copy_from_slice(lval);
            o += vl;
            out[o] = b'"';
            o += 1;
        }
        if o < out.len() {
            out[o] = b'}';
            o += 1;
        }
    }
    if o + 2 <= out.len() {
        out[o..o + 2].copy_from_slice(b"]}");
        o += 2;
    }
    let mut f = [0u8; RESP_BUF];
    f[..o].copy_from_slice(&out[..o]);
    reply_http(anchor, 200, b"OK", b"application/json", &f[..o]);
}

/// Assign each collected series to an output group (its group-label identity),
/// building the distinct group list. Groups are the output identity: two series
/// with identical output labels aggregate together.
/// Assign each resolved series to an aggregation group. False = the
/// query was refused (the error reply has been sent).
fn compute_groups(anchor: &mut AnchorState) -> bool {
    anchor.group_n = 0;
    anchor.group_off[0] = 0;
    for i in 0..anchor.series_n as usize {
        let mut sig = [0u8; 256];
        let sn = build_group_labels(anchor, i, &mut sig);
        // Find an existing identical group.
        let mut found = None;
        for g in 0..anchor.group_n as usize {
            let lo = anchor.group_off[g] as usize;
            let hi = anchor.group_off[g + 1] as usize;
            if anchor.group_lbl[lo..hi] == sig[..sn] {
                found = Some(g as u16);
                break;
            }
        }
        let gidx = match found {
            Some(g) => g,
            None => {
                let g = anchor.group_n as usize;
                let off = anchor.group_off[g] as usize;
                if off + sn > SERIES_LBL_BUF || g >= MAX_SERIES_Q {
                    // Unreachable while the series-resolution caps hold
                    // (groups ≤ series, group bytes ≤ series bytes), but
                    // refuse rather than fold distinct groups together.
                    reply_error(anchor, 422, b"Unprocessable Entity", b"grouping too large");
                    return false;
                }
                anchor.group_lbl[off..off + sn].copy_from_slice(&sig[..sn]);
                anchor.group_off[g + 1] = (off + sn) as u16;
                anchor.group_n += 1;
                g as u16
            }
        };
        anchor.series_group[i] = gidx;
    }
    true
}

/// Build a series' output labels (the `[nlen][name][vlen][val]…` a grouped
/// result renders) into `out`, returning the length. For `by(G)` the output is
/// the values of the labels in `G`; for `without(G)` it is every preimage label
/// whose name is not in `G`.
fn build_group_labels(anchor: &AnchorState, series_idx: usize, out: &mut [u8]) -> usize {
    let lo = anchor.series_lbl_off[series_idx] as usize;
    let hi = anchor.series_lbl_off[series_idx + 1] as usize;
    let preimage = &anchor.series_lbl[lo..hi];
    let mut o = 0usize;
    if anchor.q_without {
        // Every preimage label whose name is not in the group set.
        let mut at = 0usize;
        while at < preimage.len() {
            let nl = preimage[at] as usize;
            at += 1;
            if at + nl > preimage.len() {
                break;
            }
            let name = &preimage[at..at + nl];
            at += nl;
            if at >= preimage.len() {
                break;
            }
            let vl = preimage[at] as usize;
            at += 1;
            if at + vl > preimage.len() {
                break;
            }
            let value = &preimage[at..at + vl];
            at += vl;
            if !group_contains(anchor, name) {
                o = push_label(out, o, name, value);
            }
        }
    } else {
        // The value of each `by` label, in group order.
        let mut gp = 0usize;
        for _ in 0..anchor.q_group_n {
            let nl = anchor.q_group[gp] as usize;
            gp += 1;
            let name = &anchor.q_group[gp..gp + nl];
            gp += nl;
            let value = preimage_value(preimage, name);
            o = push_label(out, o, name, value);
        }
    }
    o
}

fn group_contains(anchor: &AnchorState, name: &[u8]) -> bool {
    let mut gp = 0usize;
    for _ in 0..anchor.q_group_n {
        let nl = anchor.q_group[gp] as usize;
        gp += 1;
        if &anchor.q_group[gp..gp + nl] == name {
            return true;
        }
        gp += nl;
    }
    false
}

fn preimage_value<'a>(preimage: &'a [u8], name: &[u8]) -> &'a [u8] {
    let mut at = 0usize;
    while at < preimage.len() {
        let nl = preimage[at] as usize;
        at += 1;
        if at + nl > preimage.len() {
            break;
        }
        let n = &preimage[at..at + nl];
        at += nl;
        if at >= preimage.len() {
            break;
        }
        let vl = preimage[at] as usize;
        at += 1;
        if at + vl > preimage.len() {
            break;
        }
        let v = &preimage[at..at + vl];
        at += vl;
        if n == name {
            return v;
        }
    }
    &[]
}

fn push_label(out: &mut [u8], mut o: usize, name: &[u8], value: &[u8]) -> usize {
    if o + 2 + name.len() + value.len() > out.len() || name.len() > 255 || value.len() > 255 {
        return o;
    }
    out[o] = name.len() as u8;
    o += 1;
    out[o..o + name.len()].copy_from_slice(name);
    o += name.len();
    out[o] = value.len() as u8;
    o += 1;
    out[o..o + value.len()].copy_from_slice(value);
    o += value.len();
    o
}

fn start_sample_scan(anchor: &mut AnchorState) {
    let i = anchor.cur_series as usize;
    let sid = anchor.series_id[i];
    let from = anchor.q_start_ms.saturating_sub(anchor.q_range_ms);
    let to = anchor.q_end_ms.saturating_add(1);
    let mut sb = [0u8; models::TIMESERIES_SAMPLE_KEY_LEN];
    let mut eb = [0u8; models::TIMESERIES_SAMPLE_KEY_LEN];
    let (Some(sbn), Some(ebn)) = (
        models::encode_timeseries_sample_key(&mut sb, sid, 0, from),
        models::encode_timeseries_sample_key(&mut eb, sid, 0, to),
    ) else {
        reply_error(anchor, 500, b"Internal Server Error", b"key");
        return;
    };
    let mut sk = [0u8; KEY_MAX];
    let mut ek = [0u8; KEY_MAX];
    let (Some(sn), Some(en)) = (
        user_key(&mut sk, models::KS_TIMESERIES_SAMPLE, &sb[..sbn]),
        user_key(&mut ek, models::KS_TIMESERIES_SAMPLE, &eb[..ebn]),
    ) else {
        reply_error(anchor, 500, b"Internal Server Error", b"key");
        return;
    };
    anchor.scan_start[..sn].copy_from_slice(&sk[..sn]);
    anchor.scan_start_len = sn as u16;
    anchor.scan_end[..en].copy_from_slice(&ek[..en]);
    anchor.scan_end_len = en as u16;
    anchor.cursor = 0;
    anchor.samp_n = 0;
    let Some(bn) = stage_scan_bounds(anchor) else {
        reply_error(anchor, 500, b"Internal Server Error", b"scan");
        return;
    };
    if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_SAMP) {
        reply_error(anchor, 500, b"Internal Server Error", b"scan");
    }
}

fn on_samp_page(anchor: &mut AnchorState, result: u8, body: &[u8]) {
    if result != KV_RESULT_RANGE || body.len() < 10 {
        reply_error(anchor, 500, b"Internal Server Error", b"samp scan");
        return;
    }
    anchor.cursor = u64::from_le_bytes(body[0..8].try_into().unwrap_or([0; 8]));
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    let mut at = 10usize;
    for _ in 0..count {
        let Some((kbody, value, next)) = read_kv(body, at) else {
            reply_error(anchor, 500, b"Internal Server Error", b"samp page");
            return;
        };
        at = next;
        let Some((_sid, _bucket, ts)) = models::decode_timeseries_sample_key(kbody) else {
            continue;
        };
        if value.len() == 8 {
            // A window denser than the sample buffer is refused whole:
            // folding a truncated window would silently understate every
            // aggregate. Narrow the range or coarsen the step instead.
            if (anchor.samp_n as usize) >= MAX_SAMPLES_Q {
                reply_error(anchor, 422, b"Unprocessable Entity", b"window too dense");
                return;
            }
            let v = f64::from_le_bytes(value.try_into().unwrap_or([0; 8]));
            let n = anchor.samp_n as usize;
            anchor.samp_ts[n] = ts;
            anchor.samp_val[n] = v;
            anchor.samp_n += 1;
        }
    }
    if anchor.cursor != 0 {
        let Some(bn) = stage_scan_bounds(anchor) else {
            reply_error(anchor, 500, b"Internal Server Error", b"scan");
            return;
        };
        if !kv_send(anchor, KV_OP_RANGE_SCAN, bn, D_SAMP) {
            reply_error(anchor, 500, b"Internal Server Error", b"scan");
        }
        return;
    }
    // This series' samples are complete → compute its step values.
    compute_series_steps(anchor);
    anchor.cur_series += 1;
    if (anchor.cur_series as usize) < anchor.series_n as usize {
        start_sample_scan(anchor);
    } else {
        render_matrix(anchor);
    }
}

fn compute_series_steps(anchor: &mut AnchorState) {
    let i = anchor.cur_series as usize;
    let n = anchor.samp_n as usize;
    // Samples arrive ascending by ts (the sample key sorts by ts).
    let mut samples = [tq::Sample { ts: 0, value: 0.0 }; MAX_SAMPLES_Q];
    for (k, s) in samples.iter_mut().enumerate().take(n) {
        *s = tq::Sample {
            ts: anchor.samp_ts[k],
            value: anchor.samp_val[k],
        };
    }
    let temporal = match anchor.q_temporal {
        T_RATE => tq::TemporalOp::Rate {
            range_secs: (anchor.q_range_ms as f64) / 1000.0,
        },
        _ => tq::TemporalOp::Last,
    };
    for s in 0..anchor.q_steps as usize {
        let ts = anchor.q_start_ms + (s as u64) * anchor.q_step_ms;
        let win = tq::window_slice(&samples[..n], ts, anchor.q_range_ms);
        match tq::temporal_fold(temporal, win) {
            Some(v) => {
                anchor.step_val[i][s] = v;
                anchor.step_present[i][s] = true;
            }
            None => anchor.step_present[i][s] = false,
        }
    }
}

fn render_matrix(anchor: &mut AnchorState) {
    let mut out = [0u8; RESP_BUF];
    let name_len = anchor.q_metric_name_len as usize;
    let mut name = [0u8; 128];
    name[..name_len].copy_from_slice(&anchor.q_metric_name[..name_len]);
    let combine = match anchor.q_combine {
        1 => tq::SpatialOp::Min,
        2 => tq::SpatialOp::Max,
        3 => tq::SpatialOp::Avg,
        4 => tq::SpatialOp::Count,
        _ => tq::SpatialOp::Sum,
    };

    // Instant queries return a `vector`: each series' single value at the
    // one evaluated step.
    if anchor.q_instant {
        let ts = anchor.q_start_ms;
        let res = (|| -> Option<usize> {
            let mut w = prom_result::ResultWriter::begin_vector(&mut out)?;
            if anchor.q_has_grouping {
                for g in 0..anchor.group_n as usize {
                    let mut vals = [0.0f64; MAX_SERIES_Q];
                    let mut vn = 0usize;
                    for i in 0..anchor.series_n as usize {
                        if anchor.series_group[i] as usize == g && anchor.step_present[i][0] {
                            vals[vn] = anchor.step_val[i][0];
                            vn += 1;
                        }
                    }
                    if vn > 0 {
                        let lo = anchor.group_off[g] as usize;
                        let hi = anchor.group_off[g + 1] as usize;
                        let mut lbuf = [prom_result::core::Label {
                            name: b"",
                            value: b"",
                        }; tq::MAX_LABELS];
                        let nl = decode_preimage(&anchor.group_lbl[lo..hi], &mut lbuf);
                        w.vector_series_labels_only(
                            &lbuf[..nl],
                            ts,
                            tq::spatial_combine(combine, &vals[..vn]),
                        )?;
                    }
                }
            } else {
                for i in 0..anchor.series_n as usize {
                    if anchor.step_present[i][0] {
                        let lo = anchor.series_lbl_off[i] as usize;
                        let hi = anchor.series_lbl_off[i + 1] as usize;
                        let mut lbuf = [prom_result::core::Label {
                            name: b"",
                            value: b"",
                        }; tq::MAX_LABELS];
                        let nl = decode_preimage(&anchor.series_lbl[lo..hi], &mut lbuf);
                        w.vector_series(&name[..name_len], &lbuf[..nl], ts, anchor.step_val[i][0])?;
                    }
                }
            }
            w.finish()
        })();
        match res {
            Some(n) => {
                let mut f = [0u8; RESP_BUF];
                f[..n].copy_from_slice(&out[..n]);
                reply_http(anchor, 200, b"OK", b"application/json", &f[..n]);
            }
            None => reply_error(anchor, 500, b"Internal Server Error", b"result too large"),
        }
        return;
    }

    let res = (|| -> Option<usize> {
        let mut w = prom_result::ResultWriter::begin_matrix(&mut out)?;
        if anchor.q_has_grouping {
            // One output series per group; each step combines the present
            // member series' values across the group.
            for g in 0..anchor.group_n as usize {
                let lo = anchor.group_off[g] as usize;
                let hi = anchor.group_off[g + 1] as usize;
                let mut lbuf = [prom_result::core::Label {
                    name: b"",
                    value: b"",
                }; tq::MAX_LABELS];
                let nl = decode_preimage(&anchor.group_lbl[lo..hi], &mut lbuf);
                w.begin_series_labels_only(&lbuf[..nl])?;
                for s in 0..anchor.q_steps as usize {
                    let mut vals = [0.0f64; MAX_SERIES_Q];
                    let mut vn = 0usize;
                    for i in 0..anchor.series_n as usize {
                        if anchor.series_group[i] as usize == g && anchor.step_present[i][s] {
                            vals[vn] = anchor.step_val[i][s];
                            vn += 1;
                        }
                    }
                    if vn > 0 {
                        let ts = anchor.q_start_ms + (s as u64) * anchor.q_step_ms;
                        w.point(ts, tq::spatial_combine(combine, &vals[..vn]))?;
                    }
                }
                w.end_series()?;
            }
        } else {
            for i in 0..anchor.series_n as usize {
                let lo = anchor.series_lbl_off[i] as usize;
                let hi = anchor.series_lbl_off[i + 1] as usize;
                let mut lbuf = [prom_result::core::Label {
                    name: b"",
                    value: b"",
                }; tq::MAX_LABELS];
                let nl = decode_preimage(&anchor.series_lbl[lo..hi], &mut lbuf);
                w.begin_series(&name[..name_len], &lbuf[..nl])?;
                for s in 0..anchor.q_steps as usize {
                    if anchor.step_present[i][s] {
                        let ts = anchor.q_start_ms + (s as u64) * anchor.q_step_ms;
                        w.point(ts, anchor.step_val[i][s])?;
                    }
                }
                w.end_series()?;
            }
        }
        w.finish()
    })();

    match res {
        Some(n) => {
            let mut f = [0u8; RESP_BUF];
            f[..n].copy_from_slice(&out[..n]);
            reply_http(anchor, 200, b"OK", b"application/json", &f[..n]);
        }
        None => reply_error(anchor, 500, b"Internal Server Error", b"result too large"),
    }
}

// ── Small helpers ────────────────────────────────────────────────────

/// Read one `[klen u16][key][vlen u32][value]` record; returns (kbody, value,
/// next) where kbody strips the 4-byte keyspace prefix.
fn read_kv(body: &[u8], at: usize) -> Option<(&[u8], &[u8], usize)> {
    let klen = body
        .get(at..at + 2)
        .map(|b| u16::from_le_bytes([b[0], b[1]]) as usize)?;
    let koff = at + 2;
    let voff = koff + klen;
    let vlen = body
        .get(voff..voff + 4)
        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)?;
    let vend = voff + 4 + vlen;
    if vend > body.len() || klen < 4 {
        return None;
    }
    Some((&body[koff + 4..koff + klen], &body[voff + 4..vend], vend))
}

/// Find `__name__` and produce a name-sorted order of the remaining labels.
/// Returns (metric_name, order-for-all(unused), count-of-non-name).
fn canonicalize<'a>(
    labels: &[prom_remote_write::core::Label<'a>],
    order: &mut [u8; tq::MAX_LABELS],
) -> (&'a [u8], usize, usize) {
    let mut metric: &[u8] = &[];
    let mut n = 0usize;
    for (i, l) in labels.iter().enumerate() {
        if l.name == b"__name__" {
            metric = l.value;
        } else if n < order.len() {
            order[n] = i as u8;
            n += 1;
        }
    }
    // Insertion sort `order[..n]` by label name.
    let mut i = 1;
    while i < n {
        let mut j = i;
        while j > 0 {
            let a = labels[order[j - 1] as usize].name;
            let b = labels[order[j] as usize].name;
            if name_gt(a, b) {
                order.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
        i += 1;
    }
    (metric, 0, n)
}

fn name_gt(a: &[u8], b: &[u8]) -> bool {
    let m = a.len().min(b.len());
    let mut i = 0;
    while i < m {
        if a[i] != b[i] {
            return a[i] > b[i];
        }
        i += 1;
    }
    a.len() > b.len()
}

/// Encode sorted labels as `[nlen u8][name][vlen u8][value]…`.
fn encode_preimage(labels: &[prom_remote_write::core::Label<'_>], out: &mut [u8]) -> usize {
    let mut n = 0usize;
    for l in labels {
        if n + 2 + l.name.len() + l.value.len() > out.len()
            || l.name.len() > 255
            || l.value.len() > 255
        {
            break;
        }
        out[n] = l.name.len() as u8;
        n += 1;
        out[n..n + l.name.len()].copy_from_slice(l.name);
        n += l.name.len();
        out[n] = l.value.len() as u8;
        n += 1;
        out[n..n + l.value.len()].copy_from_slice(l.value);
        n += l.value.len();
    }
    n
}

/// Decode `[nlen][name][vlen][value]…` into prom_result labels borrowing the
/// buffer. Returns the count.
fn decode_preimage<'a>(buf: &'a [u8], out: &mut [prom_result::core::Label<'a>]) -> usize {
    let mut n = 0usize;
    let mut at = 0usize;
    while at < buf.len() && n < out.len() {
        let nl = buf[at] as usize;
        at += 1;
        if at + nl > buf.len() {
            break;
        }
        let name = &buf[at..at + nl];
        at += nl;
        if at >= buf.len() {
            break;
        }
        let vl = buf[at] as usize;
        at += 1;
        if at + vl > buf.len() {
            break;
        }
        let value = &buf[at..at + vl];
        at += vl;
        out[n] = prom_result::core::Label { name, value };
        n += 1;
    }
    n
}

/// Check the owned matchers against a series' preimage label bytes.
fn preimage_matches(anchor: &AnchorState, preimage: &[u8]) -> bool {
    let mut lbuf = [promql_front::core::Label {
        name: b"",
        value: b"",
    }; tq::MAX_LABELS];
    let nl = decode_preimage_pf(preimage, &mut lbuf);
    // Walk the owned matcher bytes.
    let mut at = 0usize;
    let mut checked = 0u8;
    while checked < anchor.q_matchers_n {
        let nlen = anchor.q_matchers[at] as usize;
        at += 1;
        let name = &anchor.q_matchers[at..at + nlen];
        at += nlen;
        let op = if anchor.q_matchers[at] == 0 {
            promql_front::core::MatchOp::Eq
        } else {
            promql_front::core::MatchOp::Ne
        };
        at += 1;
        let vlen = anchor.q_matchers[at] as usize;
        at += 1;
        let value = &anchor.q_matchers[at..at + vlen];
        at += vlen;
        let m = promql_front::core::Matcher { name, op, value };
        if !promql_front::core::matcher_matches(&m, &lbuf[..nl]) {
            return false;
        }
        checked += 1;
    }
    true
}

fn decode_preimage_pf<'a>(buf: &'a [u8], out: &mut [promql_front::core::Label<'a>]) -> usize {
    let mut n = 0usize;
    let mut at = 0usize;
    while at < buf.len() && n < out.len() {
        let nl = buf[at] as usize;
        at += 1;
        if at + nl > buf.len() {
            break;
        }
        let name = &buf[at..at + nl];
        at += nl;
        if at >= buf.len() {
            break;
        }
        let vl = buf[at] as usize;
        at += 1;
        if at + vl > buf.len() {
            break;
        }
        let value = &buf[at..at + vl];
        at += vl;
        out[n] = promql_front::core::Label { name, value };
        n += 1;
    }
    n
}

/// The leading metric-name identifier of a query expression, for rendering
/// `__name__`. Skips a leading aggregator/function to the innermost ident.
fn extract_metric_name(query: &[u8]) -> &[u8] {
    // Find the last identifier that precedes a `{` or is followed by `[`/`)`/end
    // — good enough for the v1 subset (bare selector or rate(sel[..])).
    // Simple approach: the ident immediately before the first `{`, else the
    // last ident token.
    let mut i = 0;
    // Skip to inside the innermost `(` if present.
    // Find first `{`.
    while i < query.len() && query[i] != b'{' {
        i += 1;
    }
    if i < query.len() {
        // walk back over the ident before `{`.
        let end = i;
        let mut s = end;
        while s > 0 && is_ident(query[s - 1]) {
            s -= 1;
        }
        return &query[s..end];
    }
    // No braces: take the last ident run.
    let mut end = query.len();
    while end > 0 && !is_ident(query[end - 1]) {
        end -= 1;
    }
    let mut s = end;
    while s > 0 && is_ident(query[s - 1]) {
        s -= 1;
    }
    &query[s..end]
}

fn is_ident(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b':'
}

fn looks_snappy(_body: &[u8]) -> bool {
    // Prometheus always Snappy-frames remote-write; assume compressed when the
    // Content-Encoding header is absent but a body is present.
    false
}

/// Read a query param as a seconds(.fraction) value → milliseconds.
fn param_seconds_to_ms(query: &[u8], name: &[u8], default_ms: u64) -> u64 {
    let raw = match http_min::find_param(query, name) {
        Some(r) => r,
        None => return default_ms,
    };
    let mut buf = [0u8; 32];
    let n = match http_min::percent_decode(raw, &mut buf) {
        Some(n) => n,
        None => return default_ms,
    };
    parse_seconds_ms(&buf[..n]).unwrap_or(default_ms)
}

/// Parse `<int>[.<frac>]` seconds into milliseconds (millisecond precision).
fn parse_seconds_ms(s: &[u8]) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    let mut int_part: u64 = 0;
    let mut i = 0;
    while i < s.len() && s[i].is_ascii_digit() {
        int_part = int_part
            .checked_mul(10)?
            .checked_add((s[i] - b'0') as u64)?;
        i += 1;
    }
    let mut ms = int_part.checked_mul(1000)?;
    if i < s.len() && s[i] == b'.' {
        i += 1;
        let mut frac = 0u64;
        let mut scale = 100u64;
        let mut k = 0;
        while i < s.len() && s[i].is_ascii_digit() && k < 3 {
            frac += (s[i] - b'0') as u64 * scale;
            scale /= 10;
            i += 1;
            k += 1;
        }
        ms = ms.checked_add(frac)?;
    }
    Some(ms)
}

// ── Net dispatch + HTTP drain ────────────────────────────────────────

fn drain_slot(anchor: &mut AnchorState, idx: usize) {
    loop {
        let len = anchor.slots[idx].recv_len;
        if len == 0 {
            return;
        }
        let mut buf = [0u8; RECV_BUF];
        buf[..len].copy_from_slice(&anchor.slots[idx].recv[..len]);
        match http_min::frame(&buf[..len]) {
            http_min::Frame::Incomplete => return,
            http_min::Frame::Error => {
                anchor.slots[idx].state = S_CLOSING;
                return;
            }
            http_min::Frame::Ready { consumed, req } => {
                {
                    let slot = &mut anchor.slots[idx];
                    slot.recv.copy_within(consumed..len, 0);
                    slot.recv_len = len - consumed;
                }
                begin_request(anchor, idx, &req);
                if anchor.slots[idx].state != S_READY {
                    return;
                }
            }
        }
    }
}

unsafe fn dispatch_net(anchor: &mut AnchorState, msg_type: u8, payload: &[u8]) {
    match msg_type {
        NET_MSG_BOUND => {
            if anchor.phase == PHASE_WAIT_BOUND && payload.len() >= 4 {
                let port = u16::from_le_bytes([payload[2], payload[3]]);
                if port == anchor.listen_port {
                    anchor.server_conn_id = conn_id(payload);
                    anchor.phase = PHASE_LISTENING;
                }
            } else if anchor.phase == PHASE_WAIT_BOUND && payload.len() >= CONN_ID_LEN {
                anchor.server_conn_id = conn_id(payload);
                anchor.phase = PHASE_LISTENING;
            }
        }
        NET_MSG_ACCEPTED => {
            if payload.len() >= 4 {
                let port = u16::from_le_bytes([payload[2], payload[3]]);
                if port != anchor.listen_port {
                    return;
                }
            }
            if payload.len() >= CONN_ID_LEN {
                let new_id = conn_id(payload);
                if anchor.alloc_slot(new_id).is_none() {
                    let _ = net_send(anchor, NET_CMD_CLOSE, new_id, &[]);
                }
            }
        }
        NET_MSG_DATA => {
            if payload.len() > CONN_ID_LEN {
                let conn_id = conn_id(payload);
                let data = &payload[CONN_ID_LEN..];
                if let Some(idx) = anchor.find_slot(conn_id) {
                    let slot = &mut anchor.slots[idx];
                    let room = RECV_BUF - slot.recv_len;
                    if data.len() > room {
                        slot.state = S_CLOSING;
                    } else {
                        let at = slot.recv_len;
                        slot.recv[at..at + data.len()].copy_from_slice(data);
                        slot.recv_len += data.len();
                        drain_slot(anchor, idx);
                    }
                }
            }
        }
        NET_MSG_CLOSED | NET_MSG_ERROR => {
            if payload.len() >= CONN_ID_LEN {
                let conn_id = conn_id(payload);
                if let Some(idx) = anchor.find_slot(conn_id) {
                    anchor.free_slot(idx);
                }
            }
        }
        _ => {}
    }
}

// ── Module ABI ───────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<AnchorState>() as u32
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
    if state_size < core::mem::size_of::<AnchorState>() {
        return -1;
    }
    let sys_ptr = syscalls.cast::<SyscallTable>();
    let anchor = unsafe { &mut *state.cast::<AnchorState>() };
    anchor.init(sys_ptr);
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(anchor, params, params_len) };
    }
    anchor.net_in = in_chan;
    anchor.net_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        anchor.kv_in = dev_channel_port(sys, 0, 1);
        anchor.kv_out = dev_channel_port(sys, 1, 1);
        anchor.metrics_out = dev_channel_port(sys, 1, 2);
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let anchor = unsafe { &mut *state.cast::<AnchorState>() };
    let sys_ptr = anchor.syscalls;
    if sys_ptr.is_null() {
        return -1;
    }
    if anchor.phase == PHASE_INIT && unsafe { net_bind(anchor) } {
        anchor.phase = PHASE_WAIT_BOUND;
    }
    unsafe {
        let sys = &*sys_ptr;
        for _ in 0..8 {
            let mut env = [0u8; SCRATCH_BUF];
            let Some((msg, payload)) = read_one_envelope(sys, anchor.net_in, &mut env) else {
                break;
            };
            dispatch_net(anchor, msg, payload);
        }
        let mut env = [0u8; SCRATCH_BUF];
        if let Some((msg, payload)) = read_one_envelope(sys, anchor.kv_in, &mut env) {
            if msg == MSG_KV_RESPONSE && payload.len() >= 20 {
                let corr = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
                let result = payload[9];
                let blen = u16::from_le_bytes([payload[18], payload[19]]) as usize;
                if corr == anchor.kv_corr && payload.len() >= 20 + blen {
                    let mut body = [0u8; SCRATCH_BUF];
                    body[..blen].copy_from_slice(&payload[20..20 + blen]);
                    on_kv_response(anchor, result, &body[..blen]);
                }
            }
        }
    }
    for idx in 0..MAX_CONNS {
        if anchor.slots[idx].conn_id == SLOT_FREE {
            continue;
        }
        if anchor.slots[idx].state == S_CLOSING {
            let conn = anchor.slots[idx].conn_id;
            let _ = unsafe { net_send(anchor, NET_CMD_CLOSE, conn, &[]) };
            anchor.free_slot(idx);
            continue;
        }
        flush_slot(anchor, idx);
        if anchor.slots[idx].state == S_READY && anchor.slots[idx].recv_len > 0 {
            drain_slot(anchor, idx);
        }
    }
    anchor.step_ctr = anchor.step_ctr.wrapping_add(1);
    if anchor.step_ctr.is_multiple_of(5000) {
        unsafe {
            telemetry::emit_counters(
                &*sys_ptr,
                anchor.metrics_out,
                &[anchor.m_sessions, anchor.m_commands, anchor.m_errors],
            );
        }
    }
    0
}
