//! The SQL front's answer: one executor `MSG_SQL_RESPONSE` rendered as the
//! exchange answer `pg_client` gives for the same statement.
//!
//! `pg_client` answers a query with the first column of every row, as
//! PostgreSQL text, newline-joined; with the server's message text when the
//! server refused the statement; and refuses a result set past its answer
//! buffer whole. Against lattice's own PostgreSQL connector the server's
//! text form of a value is `pg_server_codec::render_value` and its message
//! is `pg_server_codec::error_message`, so rendering the executor's typed
//! answer through those two here gives the front byte-identical answers to
//! `pg_client` talking to `pg_edge_anchor` over the wire — which is what lets
//! an application swap one for the other by wiring alone.
//!
//! Dual-target facade: `#[path]`-mounted by `modules/app/sql_front` and by
//! the host suite (`tests/harness/tests/contract_sql_front.rs`). It does no
//! I/O.

#![allow(
    dead_code,
    reason = "shared via #[path] into the sql_front module and host tests; each consumer uses a subset"
)]

// `pg_server_codec` mounts `sql_exec`, which mounts `sql_core` and
// `relational`: one tree, so the types the codec renders are the types the
// response walk hands back.
#[path = "pg_server_codec.rs"]
pub mod pg;

pub use pg::relational;
pub use pg::sql_exec;

// The newline-joined accumulation `pg_client` builds its answer with.
include!("pg_reply.rs");

use relational::LogicalType;

/// Largest answer body: `pg_client`'s result buffer. A result set past it
/// is refused whole, as `pg_client` refuses one.
pub const ANSWER_MAX: usize = 8192;

/// Longest rendered cell: a text value at its type's bound, hex-encoded
/// binary doubling it, and a sign, point and digits.
pub const CELL_MAX: usize = 2 + 2 * relational::MAX_TEXT_LEN + 8;

/// What one executor response answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    /// 200: the statement completed; the body is the first `len` bytes.
    Rows(usize),
    /// 400: the executor refused the statement; the body is its message.
    Refused(usize),
    /// 502: the result set does not fit `ANSWER_MAX`, refused whole.
    TooLarge,
    /// 502: the response does not decode; nothing true can be answered.
    Undecodable,
}

/// Render the `MSG_SQL_RESPONSE` payload `payload` into `out` (at least
/// [`ANSWER_MAX`] bytes; a smaller one bounds the answer instead).
pub fn answer(payload: &[u8], out: &mut [u8]) -> Answer {
    let Some(h) = sql_exec::decode_response_header(payload) else {
        return Answer::Undecodable;
    };
    let room = out.len().min(ANSWER_MAX);
    let out = &mut out[..room];
    if h.outcome != sql_exec::OUTCOME_OK {
        let msg = pg::error_message(h.outcome);
        let n = msg.len().min(room);
        out[..n].copy_from_slice(&msg[..n]);
        return Answer::Refused(n);
    }
    if h.col_count == 0 {
        // A statement with no rows — DDL, a write, BEGIN/COMMIT, an empty
        // query — answers an empty body.
        return Answer::Rows(0);
    }
    let mut first = LogicalType::Null;
    let mut seen = false;
    let Some(rows_at) = sql_exec::walk_response_columns(payload, h.col_count, |c| {
        if !seen {
            first = c.ty;
            seen = true;
        }
    }) else {
        return Answer::Undecodable;
    };
    let mut len = 0usize;
    let mut have = false;
    let mut overflow = false;
    let mut bad = false;
    let walked = sql_exec::walk_response_rows(payload, rows_at, h.col_count, |_r, c, cell| {
        if c != 0 || overflow || bad {
            return;
        }
        // A NULL first column contributes no row.
        let Some(bytes) = cell else {
            return;
        };
        let Some((v, _)) = relational::decode_value_plain(bytes, first) else {
            bad = true;
            return;
        };
        let mut text = [0u8; CELL_MAX];
        let Some(n) = pg::render_value(first, v, &mut text) else {
            bad = true;
            return;
        };
        match pg_reply_append(out, len, have, &text[..n]) {
            Some(next) => {
                len = next;
                have = true;
            }
            None => overflow = true,
        }
    });
    if walked.is_none() || bad {
        return Answer::Undecodable;
    }
    if overflow {
        return Answer::TooLarge;
    }
    Answer::Rows(len)
}
