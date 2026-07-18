//! pg_edge_anchor — PostgreSQL v3 wire connector (RFC database
//! foundation §14.3).
//!
//! ## What this module owns
//!
//! Exactly what §14.3 assigns a connector: the wire handshake,
//! authentication negotiation, the simple query flow, session variables,
//! the dialect surface, error mapping, and RESULT ENCODING. Everything
//! about how PostgreSQL specifically spells things lives here.
//!
//! ## What it must not own
//!
//! Parsing, binding, catalogs of record, table formats, transaction
//! management, persistence. §14.3 forbids connector-specific versions of
//! all of those, and the reason is concrete rather than architectural
//! tidiness: if this module parsed SQL, it and the MySQL connector would
//! eventually disagree about what `INSERT INTO t VALUES (1)` means for a
//! two-column table, and nothing would surface the disagreement until
//! someone compared the ports. So statement TEXT goes to
//! `relational_executor` and TYPED values come back. The only decision
//! this module makes about a value is how to render it — which is why a
//! boolean can be `t`/`f` here and `1`/`0` in the MySQL connector with
//! neither one being a special case.
//!
//! ## The two protocol details that hang a client when wrong
//!
//! - **`ReadyForQuery` ends every cycle.** A client blocks until it
//!   arrives, so an error path that forgets it does not fail the client,
//!   it HANGS it. Every reply path here ends with one.
//! - **Message length counts itself, not the type byte.** Written in one
//!   place, in `pg_server_codec::Writer::finish`, and never by hand.
//!
//! ## Extended query protocol
//!
//! `Parse`/`Bind`/`Execute` are answered with a named refusal rather than
//! ignored. A driver that sends them is using a prepared statement with
//! parameters, and the executor has no parameter binding: pretending to
//! prepare and then executing something else would be the silent
//! approximation §14.3 forbids. `psql` uses the simple query path for
//! typed input, so the interactive path works; a parameterised driver
//! gets a clear "feature not supported" instead of a wrong answer.

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface. unreachable_patterns: defensive `_ =>` arms are intentional so a new variant cannot silently bypass the error path"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    clippy::too_many_arguments,
    clippy::duplicate_mod,
    reason = "fluxor module ABI: raw-pointer entry points are the contract, ABI fns carry a fixed arity, and the PIC build #[path]-remounts shared SDK/common code"
)]
#![allow(
    clippy::manual_memcpy,
    clippy::needless_range_loop,
    reason = "hand-written index loops build wire envelopes byte-by-byte throughout these modules; the explicit form is the module idiom"
)]
use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/net_proto.rs"]
mod net_proto;

// Only `pg_server_codec` is mounted: it mounts `sql_exec`, which mounts
// `sql_core` and `relational`, so this module sees exactly one of each.
#[path = "../../common/pg_server_codec.rs"]
mod pg;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use net_proto::{
    NET_CMD_BIND, NET_CMD_CLOSE, NET_CMD_SEND, NET_MSG_ACCEPTED, NET_MSG_BOUND, NET_MSG_CLOSED,
    NET_MSG_DATA, NET_MSG_ERROR,
};
use pg::relational::{LogicalType, Value};
use pg::sql_exec;
use wire::{MSG_SQL_REQUEST, MSG_SQL_RESPONSE};

// ── Capacities ────────────────────────────────────────────────────────

/// Concurrent sessions. A SQL session is heavier than a Redis connection
/// (it holds a receive buffer big enough for a statement), so this is
/// smaller than the Redis anchor's slot count by design.
const MAX_CONNS: usize = 16;

/// Per-slot receive buffer. Must hold one whole statement, since the
/// simple query protocol frames by length and a statement cannot be
/// executed in halves.
const RECV_BUF: usize = 8192;

/// Per-slot send buffer. Sized for one result-set response plus framing.
const SEND_BUF: usize = 32768;

/// Envelope scratch. Must hold the largest `MSG_SQL_RESPONSE`.
const SCRATCH_BUF: usize = 32768;

const SLOT_FREE: u8 = 0xFF;
const DEFAULT_LISTEN_PORT: u16 = 5432;

/// Longest rendered cell. A text value is capped by the logical type's
/// own bound; hex-encoded binary doubles it.
const CELL_BUF: usize = 2 + 2 * pg::relational::MAX_TEXT_LEN + 8;

/// Longest configured password.
const PASSWORD_MAX: usize = 64;

// ── Session slot ──────────────────────────────────────────────────────

/// Where a session is in the protocol. The distinction that matters is
/// `WaitStartup` vs `Ready`: before startup completes, bytes are
/// startup-framed (no type byte), and after it they are
/// `[type][len][payload]`. Reading one as the other desyncs the stream.
const S_WAIT_STARTUP: u8 = 0;
const S_READY: u8 = 1;
/// A statement is with the executor; further client bytes buffer but are
/// not parsed, because the simple query protocol is strictly
/// request/response and a second query cannot begin before the first
/// finishes.
const S_BUSY: u8 = 2;
/// Password expected (cleartext auth configured).
const S_WAIT_PASSWORD: u8 = 3;
const S_CLOSING: u8 = 4;

#[repr(C)]
#[derive(Clone, Copy)]
struct Slot {
    conn_id: u8,
    state: u8,
    /// `true` once a `BEGIN` has been seen and not yet ended. Reported in
    /// `ReadyForQuery`, because a client may change behaviour on it and a
    /// false claim would mislead it.
    in_transaction: bool,
    /// The correlation id of the statement in flight, so a reply lands on
    /// the slot that asked.
    corr_id: u64,
    recv_len: usize,
    send_len: usize,
    recv: [u8; RECV_BUF],
    send: [u8; SEND_BUF],
}

impl Slot {
    const fn free() -> Self {
        Self {
            conn_id: SLOT_FREE,
            state: S_WAIT_STARTUP,
            in_transaction: false,
            corr_id: 0,
            recv_len: 0,
            send_len: 0,
            recv: [0; RECV_BUF],
            send: [0; SEND_BUF],
        }
    }

    fn status_byte(&self) -> u8 {
        if self.in_transaction {
            pg::STATUS_IN_TRANSACTION
        } else {
            pg::STATUS_IDLE
        }
    }
}

const PHASE_INIT: u8 = 0;
const PHASE_WAIT_BOUND: u8 = 1;
const PHASE_LISTENING: u8 = 2;

#[repr(C)]
struct AnchorState {
    syscalls: *const SyscallTable,

    net_in: i32,
    net_out: i32,
    sql_out: i32,
    sql_in: i32,
    metrics_out: i32,

    phase: u8,
    server_conn_id: u8,
    listen_port: u16,
    /// Configured password. Empty means an open server, exactly as the
    /// Redis anchor's `requirepass` does.
    password: [u8; PASSWORD_MAX],
    password_len: usize,

    corr_seq: u64,

    slots: [Slot; MAX_CONNS],

    m_sessions: u64,
    m_queries: u64,
    m_rows_sent: u64,
    m_errors: u64,
    m_net_errors: u64,
    step_ctr: u64,

    scratch: [u8; SCRATCH_BUF],
}

define_params! {
    AnchorState;

    1, listen_port, u16, 5432
        => |s, d, len| { s.listen_port = p_u16(d, len, 0, 5432); };

    // Cleartext password. Empty (the default) means no authentication,
    // matching the Redis anchor's empty `requirepass`. Cleartext rather
    // than SCRAM because a SCRAM exchange this server cannot complete
    // correctly would be WORSE than one it does not offer — a client
    // would believe its password had been verified against a challenge.
    2, password, blob, 0
        => |s, d, len| {
            let take = len.min(PASSWORD_MAX);
            for i in 0..take { s.password[i] = *d.add(i); }
            s.password_len = take;
        };
}

impl AnchorState {
    fn init(&mut self, syscalls: *const SyscallTable) {
        self.syscalls = syscalls;
        self.net_in = -1;
        self.net_out = -1;
        self.sql_out = -1;
        self.sql_in = -1;
        self.metrics_out = -1;
        self.phase = PHASE_INIT;
        self.server_conn_id = 0;
        self.listen_port = DEFAULT_LISTEN_PORT;
        self.password_len = 0;
        self.corr_seq = 0;
        let mut i = 0;
        while i < MAX_CONNS {
            self.slots[i] = Slot::free();
            i += 1;
        }
        self.m_sessions = 0;
        self.m_queries = 0;
        self.m_rows_sent = 0;
        self.m_errors = 0;
        self.m_net_errors = 0;
        self.step_ctr = 0;
    }

    fn alloc_slot(&mut self, conn_id: u8) -> Option<usize> {
        for (i, s) in self.slots.iter_mut().enumerate() {
            if s.conn_id == SLOT_FREE {
                *s = Slot::free();
                s.conn_id = conn_id;
                return Some(i);
            }
        }
        None
    }

    fn find_slot(&self, conn_id: u8) -> Option<usize> {
        self.slots.iter().position(|s| s.conn_id == conn_id)
    }

    fn find_by_corr(&self, corr_id: u64) -> Option<usize> {
        self.slots
            .iter()
            .position(|s| s.conn_id != SLOT_FREE && s.state == S_BUSY && s.corr_id == corr_id)
    }

    fn free_slot(&mut self, idx: usize) {
        self.slots[idx] = Slot::free();
    }
}

// ── Slot output ───────────────────────────────────────────────────────

/// Append backend messages to a slot's send buffer through a `Writer`.
///
/// The closure form exists so the length-tracking `Writer` never has to
/// be stored: it borrows the send buffer, and holding it across steps
/// would pin the slot table.
fn write_to_slot(slot: &mut Slot, f: impl FnOnce(&mut pg::Writer<'_>)) -> bool {
    let at = slot.send_len;
    let mut w = pg::Writer::new(&mut slot.send[at..]);
    f(&mut w);
    match w.finished() {
        Some(bytes) => {
            let n = bytes.len();
            slot.send_len += n;
            true
        }
        // A truncated or unfinished message would desync the stream
        // permanently, since the client frames by length. Drop the
        // partial bytes rather than send them.
        None => false,
    }
}

/// Emit an `ErrorResponse` followed by `ReadyForQuery`.
///
/// The pairing is the point: a client blocks on `ReadyForQuery`, so an
/// error without one hangs the session instead of failing the statement.
fn send_error(anchor: &mut AnchorState, idx: usize, code: u8) {
    let status = anchor.slots[idx].status_byte();
    let sqlstate = pg::sqlstate(code);
    let message = pg::error_message(code);
    let ok = write_to_slot(&mut anchor.slots[idx], |w| {
        pg::error_response(w, b"ERROR", sqlstate, message);
        pg::ready_for_query(w, status);
    });
    if !ok {
        // Even the error did not fit: the session cannot be resynchronised,
        // so close it rather than leave the client waiting.
        anchor.slots[idx].state = S_CLOSING;
    }
    anchor.m_errors = anchor.m_errors.wrapping_add(1);
}

// ── Startup ───────────────────────────────────────────────────────────

/// Complete the startup sequence: authentication, the parameters a
/// client reads before it will proceed, and the first `ReadyForQuery`.
fn send_startup_complete(anchor: &mut AnchorState, idx: usize) {
    let conn = anchor.slots[idx].conn_id;
    let ok = write_to_slot(&mut anchor.slots[idx], |w| {
        pg::auth_ok(w);
        // A client reads several of these and some will not present a
        // prompt without them. `server_version` in particular gates
        // psql's own feature detection.
        pg::parameter_status(w, b"server_version", b"15.0 (lattice)");
        pg::parameter_status(w, b"server_encoding", b"UTF8");
        pg::parameter_status(w, b"client_encoding", b"UTF8");
        pg::parameter_status(w, b"DateStyle", b"ISO, MDY");
        pg::parameter_status(w, b"integer_datetimes", b"on");
        pg::parameter_status(w, b"standard_conforming_strings", b"on");
        // The cancel key. A cancel this server cannot honour is still
        // better announced than omitted: the startup sequence expects the
        // message, and a client that issues a CancelRequest gets a
        // connection that closes rather than one that never replies.
        pg::backend_key_data(w, 1, i32::from_le_bytes([conn, 0, 0, 0]));
        pg::ready_for_query(w, pg::STATUS_IDLE);
    });
    if ok {
        anchor.slots[idx].state = S_READY;
        anchor.m_sessions = anchor.m_sessions.wrapping_add(1);
    } else {
        anchor.slots[idx].state = S_CLOSING;
    }
}

/// Consume startup-phase bytes. Returns how many were consumed.
fn feed_startup(anchor: &mut AnchorState, idx: usize) -> usize {
    let mut buf = [0u8; RECV_BUF];
    let n = anchor.slots[idx].recv_len;
    buf[..n].copy_from_slice(&anchor.slots[idx].recv[..n]);

    match pg::parse_startup(&buf[..n]) {
        Err(()) => {
            anchor.slots[idx].state = S_CLOSING;
            n
        }
        Ok(None) => 0, // incomplete: wait for more bytes
        Ok(Some((msg, used))) => {
            match msg {
                pg::Startup::SslRequest | pg::Startup::GssEncRequest => {
                    // Refuse encryption negotiation with a single byte and
                    // stay in the startup phase — the client will follow
                    // with a real StartupMessage, or disconnect if it
                    // requires encryption. Either is correct; a silent
                    // downgrade would not be.
                    let ok = {
                        let slot = &mut anchor.slots[idx];
                        if slot.send_len < SEND_BUF {
                            slot.send[slot.send_len] = pg::SSL_REFUSED;
                            slot.send_len += 1;
                            true
                        } else {
                            false
                        }
                    };
                    if !ok {
                        anchor.slots[idx].state = S_CLOSING;
                    }
                }
                pg::Startup::CancelRequest => {
                    // Nothing to cancel: statements here complete or fail,
                    // and there is no interruptible long-running work yet.
                    // Close, which is what a client expects of a cancel
                    // connection.
                    anchor.slots[idx].state = S_CLOSING;
                }
                pg::Startup::Params { protocol, params } => {
                    if protocol != pg::PROTOCOL_3_0 {
                        // A version this connector does not speak. Say so
                        // rather than attempt it — 08P01 is the protocol
                        // violation class, which tells the client not to
                        // retry.
                        let ok = write_to_slot(&mut anchor.slots[idx], |w| {
                            pg::error_response(
                                w,
                                b"FATAL",
                                b"0A000",
                                b"unsupported frontend protocol version",
                            );
                        });
                        let _ = ok;
                        anchor.slots[idx].state = S_CLOSING;
                    } else if anchor.password_len == 0 {
                        let _ = params;
                        send_startup_complete(anchor, idx);
                    } else {
                        let ok = write_to_slot(&mut anchor.slots[idx], pg::auth_cleartext);
                        if ok {
                            anchor.slots[idx].state = S_WAIT_PASSWORD;
                        } else {
                            anchor.slots[idx].state = S_CLOSING;
                        }
                    }
                }
            }
            used
        }
    }
}

// ── Query dispatch ────────────────────────────────────────────────────

/// Forward a statement to the executor.
fn forward_statement(anchor: &mut AnchorState, idx: usize, sql: &[u8]) {
    anchor.corr_seq = anchor.corr_seq.wrapping_add(1);
    let corr = anchor.corr_seq;
    let conn = anchor.slots[idx].conn_id;
    let mut payload = [0u8; SCRATCH_BUF];
    let Some(n) =
        sql_exec::encode_sql_request(&mut payload, corr, sql_exec::DIALECT_POSTGRES, 0, conn, sql)
    else {
        send_error(anchor, idx, sql_exec::ERR_TOO_LONG);
        return;
    };
    let sent = unsafe {
        let sys = anchor.syscalls;
        !sys.is_null()
            && envelope_write(
                &*sys,
                anchor.sql_out,
                MSG_SQL_REQUEST,
                &payload[..n],
                &mut anchor.scratch,
            )
    };
    if sent {
        anchor.slots[idx].corr_id = corr;
        anchor.slots[idx].state = S_BUSY;
        anchor.m_queries = anchor.m_queries.wrapping_add(1);
    } else {
        // The executor is unreachable. `58030` is an I/O condition: a
        // retry may succeed, which is the right instruction here.
        send_error(anchor, idx, sql_exec::ERR_STORE);
    }
}

/// Consume one regular frontend message. Returns bytes consumed, or 0
/// when more input is needed.
fn feed_ready(anchor: &mut AnchorState, idx: usize) -> usize {
    let mut buf = [0u8; RECV_BUF];
    let n = anchor.slots[idx].recv_len;
    buf[..n].copy_from_slice(&anchor.slots[idx].recv[..n]);

    let (msg_kind, used, payload_at, payload_len) = match pg::parse_frontend(&buf[..n]) {
        Err(()) => {
            anchor.slots[idx].state = S_CLOSING;
            return n;
        }
        Ok(None) => return 0,
        Ok(Some((m, used))) => (m.kind, used, 5usize, m.payload.len()),
    };
    let payload = &buf[payload_at..payload_at + payload_len];

    match msg_kind {
        pg::F_QUERY => {
            let Some(sql) = pg::query_sql(payload) else {
                // An unterminated string is not a statement. Refusing is
                // the only option: executing whatever bytes followed
                // would run something the client did not send.
                send_error(anchor, idx, sql_exec::ERR_UNTERMINATED_LITERAL);
                return used;
            };
            let mut sqlbuf = [0u8; RECV_BUF];
            let sl = sql.len().min(sqlbuf.len());
            sqlbuf[..sl].copy_from_slice(&sql[..sl]);
            forward_statement(anchor, idx, &sqlbuf[..sl]);
        }
        pg::F_TERMINATE => anchor.slots[idx].state = S_CLOSING,
        // A `Sync` outside the extended protocol just wants a
        // ReadyForQuery, and answering it costs nothing.
        pg::F_SYNC | pg::F_FLUSH => {
            let status = anchor.slots[idx].status_byte();
            let _ = write_to_slot(&mut anchor.slots[idx], |w| pg::ready_for_query(w, status));
        }
        // Extended query protocol. Refused by name rather than
        // half-implemented: a driver sending these is using parameters,
        // and the executor has no parameter binding. Pretending to
        // prepare and then executing something else is exactly the
        // silent approximation §14.3 forbids.
        pg::F_PARSE | pg::F_BIND | pg::F_DESCRIBE | pg::F_EXECUTE | pg::F_CLOSE => {
            send_error(anchor, idx, sql_exec::ERR_UNSUPPORTED);
        }
        pg::F_PASSWORD => {
            // A password outside the auth phase is a protocol error.
            send_error(anchor, idx, sql_exec::ERR_UNSUPPORTED);
        }
        _ => send_error(anchor, idx, sql_exec::ERR_UNSUPPORTED),
    }
    used
}

/// Consume a `PasswordMessage` during authentication.
fn feed_password(anchor: &mut AnchorState, idx: usize) -> usize {
    let mut buf = [0u8; RECV_BUF];
    let n = anchor.slots[idx].recv_len;
    buf[..n].copy_from_slice(&anchor.slots[idx].recv[..n]);
    let (kind, payload_len, used) = match pg::parse_frontend(&buf[..n]) {
        Err(()) => {
            anchor.slots[idx].state = S_CLOSING;
            return n;
        }
        Ok(None) => return 0,
        Ok(Some((m, used))) => (m.kind, m.payload.len(), used),
    };
    if kind != pg::F_PASSWORD {
        anchor.slots[idx].state = S_CLOSING;
        return used;
    }
    // The payload is a NUL-terminated password.
    let supplied = {
        let p = &buf[5..5 + payload_len];
        let end = p.iter().position(|&b| b == 0).unwrap_or(p.len());
        &p[..end]
    };
    // Constant-time-ish: compare the whole configured length regardless of
    // where the first difference is. Not a hardening claim — the network
    // dominates any timing signal here — but it costs nothing and avoids
    // an early-return that leaks the matching prefix length.
    let expected = &anchor.password[..anchor.password_len];
    let mut diff = supplied.len() ^ expected.len();
    for i in 0..expected.len() {
        diff |= usize::from(expected[i] ^ supplied.get(i).copied().unwrap_or(0));
    }
    if diff == 0 {
        send_startup_complete(anchor, idx);
    } else {
        let _ = write_to_slot(&mut anchor.slots[idx], |w| {
            pg::error_response(w, b"FATAL", b"28P01", b"password authentication failed");
        });
        anchor.slots[idx].state = S_CLOSING;
    }
    used
}

/// Drive a slot's receive buffer as far as it will go.
fn drain_slot(anchor: &mut AnchorState, idx: usize) {
    loop {
        if anchor.slots[idx].conn_id == SLOT_FREE {
            return;
        }
        let state = anchor.slots[idx].state;
        // A busy slot must not parse further input: the simple query
        // protocol is strictly request/response, so a second query cannot
        // begin before the first finishes. The bytes stay buffered.
        if state == S_BUSY || state == S_CLOSING {
            return;
        }
        let used = match state {
            S_WAIT_STARTUP => feed_startup(anchor, idx),
            S_WAIT_PASSWORD => feed_password(anchor, idx),
            S_READY => feed_ready(anchor, idx),
            _ => 0,
        };
        if used == 0 {
            return;
        }
        let slot = &mut anchor.slots[idx];
        let remaining = slot.recv_len.saturating_sub(used);
        if remaining > 0 {
            slot.recv.copy_within(used..slot.recv_len, 0);
        }
        slot.recv_len = remaining;
        if remaining == 0 {
            return;
        }
    }
}

// ── Executor replies ──────────────────────────────────────────────────

/// Render a `MSG_SQL_RESPONSE` onto the originating session.
fn on_sql_response(anchor: &mut AnchorState, payload: &[u8]) {
    let Some(h) = sql_exec::decode_response_header(payload) else {
        return;
    };
    let Some(idx) = anchor.find_by_corr(h.corr_id) else {
        // A reply for a session that has gone. Dropping it is correct —
        // there is nobody to render it for — and it must not be applied
        // to whatever slot now holds that conn_id.
        return;
    };

    if h.outcome != sql_exec::OUTCOME_OK {
        send_error(anchor, idx, h.outcome);
        anchor.slots[idx].state = S_READY;
        return;
    }

    // Transaction status is tracked here because `ReadyForQuery` reports
    // it and a client may branch on it. BEGIN/COMMIT are NOT
    // transactional in the executor yet, so what this tracks is the
    // client's own view of its block — which is the honest thing to
    // report, since claiming `I` inside a client's BEGIN would be a
    // different lie from claiming `T`.
    match h.tag {
        sql_exec::TAG_BEGIN => anchor.slots[idx].in_transaction = true,
        sql_exec::TAG_COMMIT | sql_exec::TAG_ROLLBACK => anchor.slots[idx].in_transaction = false,
        _ => {}
    }

    if h.col_count == 0 {
        let (tag, affected) = (h.tag, h.affected);
        let status = anchor.slots[idx].status_byte();
        let ok = write_to_slot(&mut anchor.slots[idx], |w| {
            if tag == sql_exec::TAG_EMPTY {
                pg::empty_query_response(w);
            } else {
                let mut tagbuf = [0u8; 32];
                if let Some(n) = pg::command_tag(tag, affected, 0, &mut tagbuf) {
                    pg::command_complete(w, &tagbuf[..n]);
                }
            }
            pg::ready_for_query(w, status);
        });
        if !ok {
            anchor.slots[idx].state = S_CLOSING;
            return;
        }
        anchor.slots[idx].state = S_READY;
        return;
    }

    render_result_set(anchor, idx, payload, &h);
}

/// Render `RowDescription`, every `DataRow`, `CommandComplete` and
/// `ReadyForQuery` for one result set.
fn render_result_set(
    anchor: &mut AnchorState,
    idx: usize,
    payload: &[u8],
    h: &sql_exec::ResponseHeader,
) {
    // Column metadata first: names and types, copied out so the slot can
    // be borrowed mutably while rendering.
    const MAX_COLS: usize = 32;
    let mut types = [LogicalType::Null; MAX_COLS];
    let mut names = [[0u8; 64]; MAX_COLS];
    let mut name_lens = [0usize; MAX_COLS];
    let mut ncols = 0usize;
    let rows_at = {
        let (t, n, nl, cnt) = (&mut types, &mut names, &mut name_lens, &mut ncols);
        sql_exec::walk_response_columns(payload, h.col_count, |c| {
            if *cnt < MAX_COLS {
                t[*cnt] = c.ty;
                let ln = c.name.len().min(64);
                n[*cnt][..ln].copy_from_slice(&c.name[..ln]);
                nl[*cnt] = ln;
                *cnt += 1;
            }
        })
    };
    let Some(rows_at) = rows_at else {
        send_error(anchor, idx, sql_exec::ERR_CORRUPT);
        anchor.slots[idx].state = S_READY;
        return;
    };
    if ncols != h.col_count as usize {
        // More columns than this connector can describe. Refusing beats
        // sending a RowDescription whose arity disagrees with the
        // DataRows, which would desync the client's parser.
        send_error(anchor, idx, sql_exec::ERR_TOO_MANY_ITEMS);
        anchor.slots[idx].state = S_READY;
        return;
    }

    let ok = write_to_slot(&mut anchor.slots[idx], |w| {
        // `FieldDesc` borrows its name, so it cannot be `Copy` and the
        // array cannot be built by repetition. Filled per index instead.
        let mut fields: [pg::FieldDesc<'_>; MAX_COLS] = core::array::from_fn(|i| pg::FieldDesc {
            name: if i < ncols {
                &names[i][..name_lens[i]]
            } else {
                b""
            },
            type_oid: pg::type_oid(types[i]),
            type_size: pg::type_size(types[i]),
            type_mod: pg::type_mod(types[i]),
        });
        let _ = &mut fields;
        pg::row_description(w, &fields[..ncols]);
    });
    if !ok {
        anchor.slots[idx].state = S_CLOSING;
        return;
    }

    // Now the rows. Cells are decoded from the shared plain encoding and
    // rendered as PostgreSQL text — the connector half of the split.
    let mut failed = false;
    let mut current_col = 0u16;
    let rows = {
        let slot = &mut anchor.slots[idx];
        sql_exec::walk_response_rows(payload, rows_at, h.col_count, |_r, c, cell| {
            if failed {
                return;
            }
            if c == 0 {
                // A DataRow header per row. `walk_response_rows` reports
                // cells, so the row boundary is `c == 0`.
                let at = slot.send_len;
                let mut w = pg::Writer::new(&mut slot.send[at..]);
                pg::data_row_begin(&mut w, h.col_count as i16);
                // The header stays OPEN until the last cell, so the length
                // is backfilled after them — which is why the writer's
                // result is discarded here. Written directly instead: type
                // byte, placeholder length, count.
                let _ = w.finished();
                // Write the DataRow header by hand because its length
                // covers cells appended afterwards.
                let need = 1 + 4 + 2;
                if at + need > SEND_BUF {
                    failed = true;
                    return;
                }
                slot.send[at] = pg::B_DATA_ROW;
                // Length is backfilled once the row's cells are written.
                slot.send[at + 1..at + 5].copy_from_slice(&0i32.to_be_bytes());
                slot.send[at + 5..at + 7].copy_from_slice(&(h.col_count as i16).to_be_bytes());
                slot.send_len = at + need;
                row_start_stash(slot, at);
            }
            current_col = c;
            let mut cellbuf = [0u8; CELL_BUF];
            let rendered: Option<usize> = match cell {
                None => None,
                Some(bytes) => match pg::relational::decode_value_plain(bytes, types[c as usize]) {
                    Some((v, _)) => match pg::render_value(types[c as usize], v, &mut cellbuf) {
                        Some(n) => Some(n),
                        None => {
                            failed = true;
                            return;
                        }
                    },
                    None => {
                        failed = true;
                        return;
                    }
                },
            };
            let at = slot.send_len;
            match rendered {
                None => {
                    // SQL NULL is length -1, distinct from an empty value
                    // at length 0.
                    if at + 4 > SEND_BUF {
                        failed = true;
                        return;
                    }
                    slot.send[at..at + 4].copy_from_slice(&(-1i32).to_be_bytes());
                    slot.send_len = at + 4;
                }
                Some(n) => {
                    if at + 4 + n > SEND_BUF {
                        failed = true;
                        return;
                    }
                    slot.send[at..at + 4].copy_from_slice(&(n as i32).to_be_bytes());
                    slot.send[at + 4..at + 4 + n].copy_from_slice(&cellbuf[..n]);
                    slot.send_len = at + 4 + n;
                }
            }
            if c + 1 == h.col_count {
                row_finish(slot);
            }
        })
    };
    let _ = current_col;

    if failed || rows.is_none() {
        // The rows did not fit or did not decode. The RowDescription is
        // already on the wire, so the session cannot be resynchronised by
        // an error alone: close it rather than leave the client parsing
        // a truncated result set as though it were complete.
        anchor.slots[idx].state = S_CLOSING;
        anchor.m_errors = anchor.m_errors.wrapping_add(1);
        return;
    }
    let rows = rows.unwrap_or(0);
    anchor.m_rows_sent = anchor.m_rows_sent.wrapping_add(u64::from(rows));

    let status = anchor.slots[idx].status_byte();
    let ok = write_to_slot(&mut anchor.slots[idx], |w| {
        let mut tagbuf = [0u8; 32];
        if let Some(n) = pg::command_tag(sql_exec::TAG_SELECT, 0, rows, &mut tagbuf) {
            pg::command_complete(w, &tagbuf[..n]);
        }
        pg::ready_for_query(w, status);
    });
    if ok {
        anchor.slots[idx].state = S_READY;
    } else {
        anchor.slots[idx].state = S_CLOSING;
    }
}

/// Remember where the current `DataRow` began, so its length can be
/// backfilled once its cells are written. One slot is enough: rows are
/// rendered strictly one at a time.
fn row_start_stash(slot: &mut Slot, at: usize) {
    slot.corr_id = (slot.corr_id & 0xFFFF_FFFF_0000_0000) | (at as u64 & 0xFFFF_FFFF);
}

fn row_finish(slot: &mut Slot) {
    let at = (slot.corr_id & 0xFFFF_FFFF) as usize;
    let body = slot.send_len - at - 1;
    if at + 5 <= slot.send_len {
        slot.send[at + 1..at + 5].copy_from_slice(&(body as i32).to_be_bytes());
    }
}

// ── Channel helpers ───────────────────────────────────────────────────

unsafe fn envelope_read(sys: &SyscallTable, chan: i32, scratch: &mut [u8]) -> Option<(u8, usize)> {
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
    let len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if len > scratch.len() {
        return None;
    }
    if len == 0 {
        return Some((hdr[0], 0));
    }
    if ((sys.channel_read)(chan, scratch.as_mut_ptr(), len) as usize) < len {
        return None;
    }
    Some((hdr[0], len))
}

unsafe fn envelope_write(
    sys: &SyscallTable,
    chan: i32,
    msg_type: u8,
    payload: &[u8],
    scratch: &mut [u8],
) -> bool {
    if chan < 0 || payload.len() > u16::MAX as usize || 3 + payload.len() > scratch.len() {
        return false;
    }
    scratch[0] = msg_type;
    scratch[1] = (payload.len() & 0xFF) as u8;
    scratch[2] = ((payload.len() >> 8) & 0xFF) as u8;
    scratch[3..3 + payload.len()].copy_from_slice(payload);
    let total = 3 + payload.len();
    (sys.channel_write)(chan, scratch.as_mut_ptr(), total) == total as i32
}

unsafe fn net_send(anchor: &mut AnchorState, cmd: u8, conn_id: u8, data: &[u8]) -> bool {
    let sys = anchor.syscalls;
    if sys.is_null() || anchor.net_out < 0 {
        return false;
    }
    let payload_len = 1 + data.len();
    if payload_len + NET_FRAME_HDR > SCRATCH_BUF {
        return false;
    }
    let scratch = anchor.scratch.as_mut_ptr();
    *scratch = cmd;
    *scratch.add(1) = (payload_len & 0xFF) as u8;
    *scratch.add(2) = ((payload_len >> 8) & 0xFF) as u8;
    *scratch.add(NET_FRAME_HDR) = conn_id;
    if !data.is_empty() {
        core::ptr::copy_nonoverlapping(data.as_ptr(), scratch.add(NET_FRAME_HDR + 1), data.len());
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
    net_write_frame(
        &*sys,
        anchor.net_out,
        NET_CMD_BIND,
        port.as_ptr(),
        2,
        scratch,
        SCRATCH_BUF,
    ) > 0
}

// ── Net frame dispatch ────────────────────────────────────────────────

unsafe fn dispatch_net(anchor: &mut AnchorState, msg_type: u8, payload: &[u8]) {
    match msg_type {
        NET_MSG_BOUND => {
            // `net_out` is a broadcast channel in multi-anchor graphs, so
            // BOUND arrives at every anchor. Claim only the listener whose
            // port is ours.
            if anchor.phase == PHASE_WAIT_BOUND && payload.len() >= 3 {
                let port = u16::from_le_bytes([payload[1], payload[2]]);
                if port == anchor.listen_port {
                    anchor.server_conn_id = payload[0];
                    anchor.phase = PHASE_LISTENING;
                }
            } else if anchor.phase == PHASE_WAIT_BOUND && !payload.is_empty() {
                anchor.server_conn_id = payload[0];
                anchor.phase = PHASE_LISTENING;
            }
        }
        NET_MSG_ACCEPTED => {
            // Same per-port filter: ACCEPTED carries the parent listener's
            // port, so a connection for another anchor is not claimed.
            if payload.len() >= 3 {
                let port = u16::from_le_bytes([payload[1], payload[2]]);
                if port != anchor.listen_port {
                    return;
                }
            }
            if !payload.is_empty() {
                let new_id = payload[0];
                if anchor.alloc_slot(new_id).is_none() {
                    let _ = net_send(anchor, NET_CMD_CLOSE, new_id, &[]);
                }
            }
        }
        NET_MSG_DATA => {
            if payload.len() >= 2 {
                let conn_id = payload[0];
                let data = &payload[1..];
                if let Some(idx) = anchor.find_slot(conn_id) {
                    let slot = &mut anchor.slots[idx];
                    let room = RECV_BUF - slot.recv_len;
                    if data.len() > room {
                        // A statement larger than the buffer cannot be
                        // executed in halves, and silently dropping bytes
                        // would execute a DIFFERENT statement. Close.
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
        NET_MSG_CLOSED => {
            if !payload.is_empty() {
                if let Some(idx) = anchor.find_slot(payload[0]) {
                    anchor.free_slot(idx);
                }
            }
        }
        NET_MSG_ERROR => {
            // Broadcast, so this also carries errors for connections other
            // modules own. React only to ours.
            if !payload.is_empty() {
                if let Some(idx) = anchor.find_slot(payload[0]) {
                    anchor.free_slot(idx);
                    anchor.m_net_errors = anchor.m_net_errors.wrapping_add(1);
                }
            }
        }
        _ => {}
    }
}

/// Ship each slot's pending bytes, and close slots that asked to close
/// once their final bytes are out.
unsafe fn flush_slots(anchor: &mut AnchorState) {
    let mut i = 0;
    while i < MAX_CONNS {
        let (conn_id, send_len, state) = {
            let s = &anchor.slots[i];
            (s.conn_id, s.send_len, s.state)
        };
        if conn_id == SLOT_FREE {
            i += 1;
            continue;
        }
        if send_len > 0 {
            let mut out = [0u8; SEND_BUF];
            out[..send_len].copy_from_slice(&anchor.slots[i].send[..send_len]);
            if net_send(anchor, NET_CMD_SEND, conn_id, &out[..send_len]) {
                anchor.slots[i].send_len = 0;
            }
        }
        // Close only after the buffer has drained, so a FATAL error
        // message actually reaches the client before the socket goes.
        if state == S_CLOSING && anchor.slots[i].send_len == 0 {
            let _ = net_send(anchor, NET_CMD_CLOSE, conn_id, &[]);
            anchor.free_slot(i);
        }
        i += 1;
    }
}

// ── Module ABI ────────────────────────────────────────────────────────

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

    // inputs:  net_in[0], sql_in[1]
    // outputs: net_out[0], sql_out[1], metrics[2]
    anchor.net_in = in_chan;
    anchor.net_out = out_chan;
    unsafe {
        let sys = &*sys_ptr;
        anchor.sql_in = dev_channel_port(sys, 0, 1);
        anchor.sql_out = dev_channel_port(sys, 1, 1);
        anchor.metrics_out = dev_channel_port(sys, 1, 2);
    }
    // Without an executor this connector can accept sessions and answer
    // nothing. Refuse to start rather than accept connections it cannot
    // serve — a client that connects and then hangs is worse than one
    // that cannot connect.
    if anchor.sql_out < 0 || anchor.sql_in < 0 {
        return -1;
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
    unsafe {
        let sys_ptr = anchor.syscalls;
        if sys_ptr.is_null() {
            return 0;
        }
        let sys = &*sys_ptr;

        if anchor.phase == PHASE_INIT {
            if net_bind(anchor) {
                anchor.phase = PHASE_WAIT_BOUND;
            }
            return 0;
        }

        // Executor replies first: they unblock busy slots, and a slot that
        // finished this step can then parse the next pipelined statement.
        if let Some((mt, len)) = envelope_read(sys, anchor.sql_in, &mut anchor.scratch) {
            if mt == MSG_SQL_RESPONSE {
                let mut tmp = [0u8; SCRATCH_BUF];
                tmp[..len].copy_from_slice(&anchor.scratch[..len]);
                on_sql_response(anchor, &tmp[..len]);
                // The slot may have buffered another statement while busy.
                let mut i = 0;
                while i < MAX_CONNS {
                    if anchor.slots[i].conn_id != SLOT_FREE
                        && anchor.slots[i].state == S_READY
                        && anchor.slots[i].recv_len > 0
                    {
                        drain_slot(anchor, i);
                    }
                    i += 1;
                }
            }
        }

        // One net frame per step keeps the step bounded; the scheduler
        // calls back immediately while work remains.
        let buf = anchor.scratch.as_mut_ptr();
        let (msg_type, payload_len) = net_read_frame(sys, anchor.net_in, buf, SCRATCH_BUF);
        if msg_type != 0 || payload_len != 0 {
            let mut tmp = [0u8; SCRATCH_BUF];
            let copy = payload_len.min(SCRATCH_BUF);
            core::ptr::copy_nonoverlapping(buf.add(NET_FRAME_HDR), tmp.as_mut_ptr(), copy);
            dispatch_net(anchor, msg_type, &tmp[..copy]);
        }

        flush_slots(anchor);

        anchor.step_ctr = anchor.step_ctr.wrapping_add(1);
        if anchor.step_ctr.is_multiple_of(5000) {
            telemetry::emit_counters(
                sys,
                anchor.metrics_out,
                &[
                    anchor.m_sessions,
                    anchor.m_queries,
                    anchor.m_rows_sent,
                    anchor.m_errors,
                    anchor.m_net_errors,
                ],
            );
        }
    }
    0
}
