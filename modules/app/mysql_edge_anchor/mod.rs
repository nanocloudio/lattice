//! mysql_edge_anchor — MySQL/MariaDB wire connector (RFC database
//! foundation §14.3).
//!
//! The counterpart to `pg_edge_anchor`, and deliberately its structural
//! twin: the same slot table, the same "text out, typed values back"
//! boundary, the same refusal discipline. What differs is entirely
//! MySQL's spelling of things, which is exactly the division §14.3
//! draws — a connector owns wire, auth, session, error mapping and
//! result encoding, and owns nothing about what a statement MEANS.
//!
//! The clearest evidence the split is real: a boolean renders `1`/`0`
//! here and `t`/`f` on the PostgreSQL port, and neither is a special
//! case, because the executor never produced text at all.
//!
//! ## The two details that desync a MySQL client
//!
//! - **The sequence number is protocol state.** It resets to 0 per
//!   command and increments per packet; a client that sees a gap hangs
//!   up. Each reply is therefore written with one `Writer` seeded from
//!   the command's sequence, and the writer hands back where it ended.
//! - **A result set is a PACKET SEQUENCE, not a message.** Column count,
//!   then one definition per column, then EOF, then one packet per row,
//!   then EOF. Omitting either EOF leaves the client waiting — and this
//!   connector does not advertise `CLIENT_DEPRECATE_EOF` precisely so
//!   that framing stays unambiguous.
//!
//! ## Authentication
//!
//! `mysql_native_password` is advertised and a scramble is sent, because
//! the handshake layout requires both before a client can learn whether
//! a password is needed. With no password configured the response is
//! accepted whatever it contains — an open server, the same posture the
//! Redis anchor's empty `requirepass` takes.
//!
//! With a password configured the response IS verified: the client sends
//! `SHA1(password) XOR SHA1(scramble ‖ SHA1(SHA1(password)))`, and the
//! server recomputes it. That is checkable without storing the password
//! in the clear on the wire, which is why MySQL's scheme is worth
//! implementing here where PostgreSQL's SCRAM was not.

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

// Only `mysql_server_codec` is mounted: it mounts `sql_exec`, which
// mounts `sql_core` and `relational`, so this module sees one of each.
#[path = "../../common/mysql_server_codec.rs"]
mod my;

// The native-password digest needs SHA-1. `mysql_core` is the CLIENT
// side of this protocol, but the hash is the same computation in both
// directions, so it is reused rather than reimplemented — one SHA-1 in
// the tree, not two.
#[path = "../../common/mysql_core.rs"]
mod mysql_core;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use my::relational::{LogicalType, Value};
use my::sql_exec;
use net_proto::{
    NET_CMD_BIND, NET_CMD_CLOSE, NET_CMD_SEND, NET_MSG_ACCEPTED, NET_MSG_BOUND, NET_MSG_CLOSED,
    NET_MSG_DATA, NET_MSG_ERROR,
};
use wire::{MSG_SQL_REQUEST, MSG_SQL_RESPONSE};

/// Keep the SDK's ABI-surface attestation reachable.
///
/// `runtime.rs` declares `FLUXOR_ABI_SURFACE` as a plain `pub static`, and
/// the packer reads it out of the ELF to prove which ABI the module was
/// compiled against. Nothing in a module's own code reads it, so with
/// section GC the linker is free to drop the data while leaving the
/// symbol pointing into merged `.rodata` — at which point the packer
/// reads 32 bytes of something else and rejects the module for an "ABI
/// mismatch" that is really a dangling symbol. Observed exactly that:
/// this module was rejected with a digest that no build ever produced.
/// `#[used]` keeps the data anchored.
#[used]
static ABI_SURFACE_ANCHOR: &[u8; 32] = &FLUXOR_ABI_SURFACE;

// ── Capacities ────────────────────────────────────────────────────────

const MAX_CONNS: usize = 16;
const RECV_BUF: usize = 8192;
const SEND_BUF: usize = 32768;
const SCRATCH_BUF: usize = 32768;
const SLOT_FREE: u8 = 0xFF;
const DEFAULT_LISTEN_PORT: u16 = 3306;
const PASSWORD_MAX: usize = 64;

/// Longest rendered cell.
const CELL_BUF: usize = 2 * my::relational::MAX_TEXT_LEN + 8;

/// Columns this connector can describe in one result set.
const MAX_COLS: usize = 32;

// ── Session slot ──────────────────────────────────────────────────────

const S_WAIT_HANDSHAKE: u8 = 0;
const S_READY: u8 = 1;
const S_BUSY: u8 = 2;
const S_CLOSING: u8 = 3;

#[repr(C)]
#[derive(Clone, Copy)]
struct Slot {
    conn_id: u8,
    state: u8,
    in_transaction: bool,
    /// The sequence number the NEXT reply packet must carry. Reset to
    /// the command's sequence plus one on every command, because MySQL
    /// restarts the count per command and a gap makes the client hang up.
    next_seq: u8,
    corr_id: u64,
    /// Per-session auth challenge. Kept because verification happens on
    /// the response packet, one round trip later.
    scramble: [u8; 20],
    recv_len: usize,
    send_len: usize,
    recv: [u8; RECV_BUF],
    send: [u8; SEND_BUF],
}

impl Slot {
    const fn free() -> Self {
        Self {
            conn_id: SLOT_FREE,
            state: S_WAIT_HANDSHAKE,
            in_transaction: false,
            next_seq: 0,
            corr_id: 0,
            scramble: [0; 20],
            recv_len: 0,
            send_len: 0,
            recv: [0; RECV_BUF],
            send: [0; SEND_BUF],
        }
    }

    fn status(&self) -> u16 {
        if self.in_transaction {
            my::SERVER_STATUS_AUTOCOMMIT | my::SERVER_STATUS_IN_TRANS
        } else {
            my::SERVER_STATUS_AUTOCOMMIT
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
    password: [u8; PASSWORD_MAX],
    password_len: usize,

    corr_seq: u64,
    /// Counter feeding the per-session challenge. Not a CSPRNG, and the
    /// module says so: with no password configured the challenge is
    /// unused, and with one configured the deployment posture is a
    /// trusted network or TLS termination in front — the same posture
    /// the PostgreSQL connector's cleartext auth assumes. A predictable
    /// challenge would matter on a hostile network, and that is exactly
    /// where neither connector should be exposed.
    challenge_ctr: u64,

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

    1, listen_port, u16, 3306
        => |s, d, len| { s.listen_port = p_u16(d, len, 0, 3306); };

    // Empty (the default) means no authentication, matching the Redis
    // anchor's empty `requirepass`.
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
        self.challenge_ctr = 0x9E37_79B9_7F4A_7C15;
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

    /// Next challenge. Bytes are kept in the printable ASCII range
    /// because some clients treat the scramble as a C string and stop at
    /// a NUL, which would silently shorten it.
    fn next_scramble(&mut self) -> [u8; 20] {
        let mut out = [0u8; 20];
        let mut x = self.challenge_ctr;
        for b in out.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = 0x21 + (x % 0x5D) as u8;
        }
        self.challenge_ctr = x;
        out
    }
}

// ── Slot output ───────────────────────────────────────────────────────

/// Append packets to a slot's send buffer, threading the sequence.
fn write_to_slot(slot: &mut Slot, f: impl FnOnce(&mut my::Writer<'_>)) -> bool {
    let at = slot.send_len;
    let mut w = my::Writer::new(&mut slot.send[at..], slot.next_seq);
    f(&mut w);
    let end_seq = w.seq();
    match w.finished() {
        Some(bytes) => {
            let n = bytes.len();
            slot.send_len += n;
            slot.next_seq = end_seq;
            true
        }
        // A truncated packet desyncs the client permanently. Drop the
        // partial bytes rather than send them.
        None => false,
    }
}

fn send_error(anchor: &mut AnchorState, idx: usize, code: u8) {
    let number = my::error_number(code);
    let state = my::sqlstate(code);
    let message = error_text(code);
    let ok = write_to_slot(&mut anchor.slots[idx], |w| {
        my::err_packet(w, number, state, message);
    });
    if !ok {
        anchor.slots[idx].state = S_CLOSING;
    }
    anchor.m_errors = anchor.m_errors.wrapping_add(1);
}

/// Human message for an outcome code. Deliberately terse; the error
/// NUMBER is what a driver branches on.
fn error_text(code: u8) -> &'static [u8] {
    use sql_exec as x;
    match code {
        x::ERR_TOO_LONG => b"Statement too long",
        x::ERR_UNEXPECTED_END => b"Unexpected end of statement",
        x::ERR_SYNTAX => b"You have an error in your SQL syntax",
        x::ERR_NAME_TOO_LONG => b"Identifier name is too long",
        x::ERR_LITERAL_TOO_LONG => b"Data too long for column",
        x::ERR_TOO_MANY_ITEMS => b"Too many items in statement",
        x::ERR_UNTERMINATED_LITERAL => b"Unterminated string literal",
        x::ERR_UNKNOWN_TYPE => b"Unknown column type",
        x::ERR_UNKNOWN_COLUMN => b"Unknown column",
        x::ERR_TYPE_MISMATCH => b"Incorrect value for column",
        x::ERR_ARITY_MISMATCH => b"Column count doesn't match value count",
        x::ERR_NOT_NULL => b"Column cannot be null",
        x::ERR_MISSING_PRIMARY_KEY => b"This table requires a primary key",
        x::ERR_DUPLICATE_COLUMN => b"Duplicate column name",
        x::ERR_UNSUPPORTED => b"This version does not yet support that",
        x::ERR_NO_SUCH_TABLE => b"Table doesn't exist",
        x::ERR_TABLE_EXISTS => b"Table already exists",
        x::ERR_STORE => b"Storage unavailable",
        x::ERR_RESULT_TOO_LARGE => b"The SELECT would examine too many rows",
        x::ERR_BUSY => b"Too many statements in flight",
        x::ERR_DUPLICATE_KEY => b"Duplicate entry for key 'PRIMARY'",
        x::ERR_CORRUPT => b"Stored object could not be decoded",
        _ => b"Unknown error",
    }
}

// ── Handshake ─────────────────────────────────────────────────────────

fn send_handshake(anchor: &mut AnchorState, idx: usize) {
    let scramble = anchor.next_scramble();
    anchor.slots[idx].scramble = scramble;
    let conn = u32::from(anchor.slots[idx].conn_id);
    // The handshake is always sequence 0 — it opens the exchange.
    anchor.slots[idx].next_seq = 0;
    let ok = write_to_slot(&mut anchor.slots[idx], |w| {
        my::handshake(w, conn, &scramble, b"8.0.0-lattice");
    });
    if !ok {
        anchor.slots[idx].state = S_CLOSING;
    }
}

/// Verify a `HandshakeResponse41` and reply OK or ERR.
fn feed_handshake(anchor: &mut AnchorState, idx: usize) -> usize {
    let mut buf = [0u8; RECV_BUF];
    let n = anchor.slots[idx].recv_len;
    buf[..n].copy_from_slice(&anchor.slots[idx].recv[..n]);
    let (seq, payload_at, payload_len, used) = match my::parse_packet(&buf[..n]) {
        Err(()) => {
            anchor.slots[idx].state = S_CLOSING;
            return n;
        }
        Ok(None) => return 0,
        Ok(Some((p, used))) => (p.seq, my::PACKET_HEADER, p.payload.len(), used),
    };
    let payload = &buf[payload_at..payload_at + payload_len];
    // The reply continues the client's sequence.
    anchor.slots[idx].next_seq = seq.wrapping_add(1);

    let Some(caps) = my::response_capabilities(payload) else {
        anchor.slots[idx].state = S_CLOSING;
        return used;
    };
    if caps & my::CLIENT_PROTOCOL_41 == 0 {
        // The pre-4.1 handshake is a different protocol, not a subset.
        send_error(anchor, idx, sql_exec::ERR_UNSUPPORTED);
        anchor.slots[idx].state = S_CLOSING;
        return used;
    }

    if anchor.password_len > 0 && !auth_ok(anchor, idx, payload) {
        let ok = write_to_slot(&mut anchor.slots[idx], |w| {
            my::err_packet(w, 1045, b"28000", b"Access denied for user");
        });
        let _ = ok;
        anchor.slots[idx].state = S_CLOSING;
        return used;
    }

    let status = anchor.slots[idx].status();
    let ok = write_to_slot(&mut anchor.slots[idx], |w| {
        my::ok_packet(w, 0, 0, status);
    });
    if ok {
        anchor.slots[idx].state = S_READY;
        anchor.m_sessions = anchor.m_sessions.wrapping_add(1);
    } else {
        anchor.slots[idx].state = S_CLOSING;
    }
    used
}

/// Check the client's `mysql_native_password` token.
///
/// The client sends `SHA1(pw) XOR SHA1(scramble ‖ SHA1(SHA1(pw)))`. The
/// server recomputes the same value from the password it holds and the
/// scramble it issued, so the password never crosses the wire — which is
/// why this scheme is implemented here where PostgreSQL's SCRAM was not:
/// it is verifiable with the primitives already in the tree.
fn auth_ok(anchor: &AnchorState, idx: usize, payload: &[u8]) -> bool {
    // Layout after the fixed header: `[user\0][auth_len:u8][auth…]`.
    const USER_AT: usize = 4 + 4 + 1 + 23;
    let Some(rest) = payload.get(USER_AT..) else {
        return false;
    };
    let Some(user_end) = rest.iter().position(|&b| b == 0) else {
        return false;
    };
    let after_user = &rest[user_end + 1..];
    let Some(&auth_len) = after_user.first() else {
        return false;
    };
    let Some(supplied) = after_user.get(1..1 + auth_len as usize) else {
        return false;
    };
    if supplied.len() != 20 {
        return false;
    }
    let expected = mysql_core::mysql_native_token(
        &anchor.password[..anchor.password_len],
        &anchor.slots[idx].scramble,
    );
    // Compare every byte regardless of where the first difference is.
    // Not a hardening claim — the network dominates any timing signal —
    // but an early return would leak the matching prefix length for no
    // benefit.
    let mut diff = 0u8;
    for i in 0..20 {
        diff |= expected[i] ^ supplied[i];
    }
    diff == 0
}

// ── Commands ──────────────────────────────────────────────────────────

fn forward_statement(anchor: &mut AnchorState, idx: usize, sql: &[u8]) {
    anchor.corr_seq = anchor.corr_seq.wrapping_add(1);
    let corr = anchor.corr_seq;
    let conn = anchor.slots[idx].conn_id;
    let mut payload = [0u8; SCRATCH_BUF];
    let Some(n) =
        sql_exec::encode_sql_request(&mut payload, corr, sql_exec::DIALECT_MYSQL, 0, conn, sql)
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
        send_error(anchor, idx, sql_exec::ERR_STORE);
    }
}

fn feed_command(anchor: &mut AnchorState, idx: usize) -> usize {
    let mut buf = [0u8; RECV_BUF];
    let n = anchor.slots[idx].recv_len;
    buf[..n].copy_from_slice(&anchor.slots[idx].recv[..n]);
    let (seq, payload_at, payload_len, used) = match my::parse_packet(&buf[..n]) {
        Err(()) => {
            anchor.slots[idx].state = S_CLOSING;
            return n;
        }
        Ok(None) => return 0,
        Ok(Some((p, used))) => (p.seq, my::PACKET_HEADER, p.payload.len(), used),
    };
    let payload = &buf[payload_at..payload_at + payload_len];
    // Every command restarts the sequence; the reply continues from it.
    anchor.slots[idx].next_seq = seq.wrapping_add(1);

    let Some(&cmd) = payload.first() else {
        anchor.slots[idx].state = S_CLOSING;
        return used;
    };
    match cmd {
        my::COM_QUIT => anchor.slots[idx].state = S_CLOSING,
        my::COM_PING => {
            let status = anchor.slots[idx].status();
            let _ = write_to_slot(&mut anchor.slots[idx], |w| my::ok_packet(w, 0, 0, status));
        }
        // `USE <db>`: accepted because there is one database and the
        // client is asking for it. Refusing would stop `mariadb -D` from
        // connecting at all, and accepting is truthful — the session
        // does end up pointed at the only database there is.
        my::COM_INIT_DB => {
            let status = anchor.slots[idx].status();
            let _ = write_to_slot(&mut anchor.slots[idx], |w| my::ok_packet(w, 0, 0, status));
        }
        my::COM_QUERY => {
            let sql = &payload[1..];
            let mut sqlbuf = [0u8; RECV_BUF];
            let sl = sql.len().min(sqlbuf.len());
            sqlbuf[..sl].copy_from_slice(&sql[..sl]);
            forward_statement(anchor, idx, &sqlbuf[..sl]);
        }
        // Prepared statements need parameter binding the executor does
        // not have. Named refusal rather than a fake prepare that
        // executed something else.
        my::COM_STMT_PREPARE | my::COM_STMT_EXECUTE | my::COM_STMT_CLOSE | my::COM_FIELD_LIST => {
            send_error(anchor, idx, sql_exec::ERR_UNSUPPORTED);
        }
        _ => send_error(anchor, idx, sql_exec::ERR_UNSUPPORTED),
    }
    used
}

fn drain_slot(anchor: &mut AnchorState, idx: usize) {
    loop {
        if anchor.slots[idx].conn_id == SLOT_FREE {
            return;
        }
        let state = anchor.slots[idx].state;
        if state == S_BUSY || state == S_CLOSING {
            return;
        }
        let used = match state {
            S_WAIT_HANDSHAKE => feed_handshake(anchor, idx),
            S_READY => feed_command(anchor, idx),
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

fn on_sql_response(anchor: &mut AnchorState, payload: &[u8]) {
    let Some(h) = sql_exec::decode_response_header(payload) else {
        return;
    };
    let Some(idx) = anchor.find_by_corr(h.corr_id) else {
        return;
    };

    if h.outcome != sql_exec::OUTCOME_OK {
        send_error(anchor, idx, h.outcome);
        anchor.slots[idx].state = S_READY;
        return;
    }

    match h.tag {
        sql_exec::TAG_BEGIN => anchor.slots[idx].in_transaction = true,
        sql_exec::TAG_COMMIT | sql_exec::TAG_ROLLBACK => anchor.slots[idx].in_transaction = false,
        _ => {}
    }

    if h.col_count == 0 {
        let affected = h.affected;
        let status = anchor.slots[idx].status();
        let ok = write_to_slot(&mut anchor.slots[idx], |w| {
            // Every non-row statement is an OK packet carrying its
            // affected-row count. MySQL has no per-verb completion
            // string, so the executor's tag is used only for the count.
            my::ok_packet(w, affected, 0, status);
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

/// Emit the full result-set packet sequence: column count, definitions,
/// EOF, rows, EOF.
fn render_result_set(
    anchor: &mut AnchorState,
    idx: usize,
    payload: &[u8],
    h: &sql_exec::ResponseHeader,
) {
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
        // A column count that disagrees with the definitions that follow
        // would desync the client's parser. Refuse instead.
        send_error(anchor, idx, sql_exec::ERR_TOO_MANY_ITEMS);
        anchor.slots[idx].state = S_READY;
        return;
    }

    let status = anchor.slots[idx].status();
    let header_ok = write_to_slot(&mut anchor.slots[idx], |w| {
        my::column_count(w, u64::from(h.col_count));
        for i in 0..ncols {
            my::column_definition(
                w,
                &my::ColumnDef {
                    name: &names[i][..name_lens[i]],
                    table: b"",
                    column_type: my::column_type(types[i]),
                    charset: my::column_charset(types[i]),
                    length: my::column_length(types[i]),
                    flags: 0,
                    decimals: 0,
                },
            );
        }
        // Not advertising CLIENT_DEPRECATE_EOF means this EOF is
        // required; without it the client waits for more definitions.
        my::eof_packet(w, status);
    });
    if !header_ok {
        anchor.slots[idx].state = S_CLOSING;
        return;
    }

    // One packet per row, each holding one length-encoded string per
    // column.
    let mut failed = false;
    let mut rows_emitted = 0u16;
    {
        let slot = &mut anchor.slots[idx];
        let mut row_cells = [[0u8; CELL_BUF]; MAX_COLS];
        let mut row_lens = [0usize; MAX_COLS];
        let mut row_null = [false; MAX_COLS];
        let walked = sql_exec::walk_response_rows(payload, rows_at, h.col_count, |_r, c, cell| {
            if failed {
                return;
            }
            match cell {
                None => {
                    row_null[c as usize] = true;
                    row_lens[c as usize] = 0;
                }
                Some(bytes) => match my::relational::decode_value_plain(bytes, types[c as usize]) {
                    Some((v, _)) => {
                        match my::render_value(types[c as usize], v, &mut row_cells[c as usize]) {
                            Some(n) => {
                                row_null[c as usize] = false;
                                row_lens[c as usize] = n;
                            }
                            None => {
                                failed = true;
                                return;
                            }
                        }
                    }
                    None => {
                        failed = true;
                        return;
                    }
                },
            }
            // The row is complete on its last cell: emit it as one
            // packet, which is also what keeps the sequence numbering
            // right (one increment per row, not per cell).
            if c + 1 == h.col_count {
                let at = slot.send_len;
                let mut w = my::Writer::new(&mut slot.send[at..], slot.next_seq);
                w.begin();
                for i in 0..h.col_count as usize {
                    if row_null[i] {
                        w.null_cell();
                    } else {
                        w.lenenc_str(&row_cells[i][..row_lens[i]]);
                    }
                }
                w.end();
                let end_seq = w.seq();
                match w.finished() {
                    Some(bytes) => {
                        let n = bytes.len();
                        slot.send_len += n;
                        slot.next_seq = end_seq;
                        rows_emitted = rows_emitted.wrapping_add(1);
                    }
                    None => failed = true,
                }
            }
        });
        if walked.is_none() {
            failed = true;
        }
    }

    if failed {
        // The column definitions are already on the wire, so an error
        // packet cannot resynchronise the session. Close it rather than
        // leave the client reading a truncated result set as complete.
        anchor.slots[idx].state = S_CLOSING;
        anchor.m_errors = anchor.m_errors.wrapping_add(1);
        return;
    }
    anchor.m_rows_sent = anchor.m_rows_sent.wrapping_add(u64::from(rows_emitted));

    let ok = write_to_slot(&mut anchor.slots[idx], |w| my::eof_packet(w, status));
    if ok {
        anchor.slots[idx].state = S_READY;
    } else {
        anchor.slots[idx].state = S_CLOSING;
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
            if payload.len() >= 3 {
                let port = u16::from_le_bytes([payload[1], payload[2]]);
                if port != anchor.listen_port {
                    return;
                }
            }
            if !payload.is_empty() {
                let new_id = payload[0];
                match anchor.alloc_slot(new_id) {
                    // MySQL is a server-speaks-first protocol: the
                    // handshake goes out on accept, before the client
                    // sends anything.
                    Some(idx) => send_handshake(anchor, idx),
                    None => {
                        let _ = net_send(anchor, NET_CMD_CLOSE, new_id, &[]);
                    }
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
        // Close only once the buffer has drained, so an ERR packet
        // reaches the client before the socket goes.
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
    // Without an executor this connector would complete a handshake and
    // then answer nothing. Refuse to start: a client that connects and
    // hangs is worse than one that cannot connect.
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

        if let Some((mt, len)) = envelope_read(sys, anchor.sql_in, &mut anchor.scratch) {
            if mt == MSG_SQL_RESPONSE {
                let mut tmp = [0u8; SCRATCH_BUF];
                tmp[..len].copy_from_slice(&anchor.scratch[..len]);
                on_sql_response(anchor, &tmp[..len]);
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
