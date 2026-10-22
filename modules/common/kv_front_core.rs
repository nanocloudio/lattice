//! The KV front's request and answer halves: one structured command in, the
//! answer `redis_client` gives for it out.
//!
//! `redis_client` takes a STRUCTURED command (`[nargs:u8]` then per argument
//! `[len:u16 LE][bytes]`), builds its RESP form, and answers with the
//! server's reply mapped onto a status and a body. The front takes the same
//! body and builds the same RESP form, then performs it the way lattice's
//! Redis connector does — `redis_codec` parses it and turns it into the
//! typed KV operation, and turns the store's answer back into a RESP reply —
//! and maps that reply with `redis_client`'s own mapping (`resp_classify`).
//! The answer is therefore byte-identical to `redis_client` talking to
//! `redis_edge_anchor` over the wire.
//!
//! What a connection carries — AUTH, SELECT, MULTI/EXEC, pub/sub — has no
//! meaning on a front that serves one command per exchange and checks no
//! authority (access is the graph's wiring); those commands are answered as
//! a server error naming that.
//!
//! Dual-target facade: `#[path]`-mounted by `modules/app/kv_front` and by
//! the host suite (`tests/harness/tests/contract_kv_front.rs`). It does no
//! I/O.

#![allow(
    dead_code,
    reason = "shared via #[path] into the kv_front module and host tests; each consumer uses a subset"
)]

#[path = "types.rs"]
pub mod types;

#[path = "redis_codec.rs"]
pub mod redis_codec;

// Mounted beside `types` (it reaches it as `super::types`).
#[path = "kv_large.rs"]
pub mod kv_large;

// `redis_client`'s RESP framing and reply classification, verbatim.
include!("resp_core.rs");
include!("redis_core.rs");

use redis_codec::{
    build_op_body, enc_array_header, enc_bulk, enc_error, enc_raw, enc_simple_str, eq_ascii_ci,
    parse_one, Built, ParseStep,
};

/// Longest structured command one request carries (`redis_client`'s).
pub const COMMAND_MAX: usize = 4096;
/// Longest RESP form of a command (`redis_client`'s request buffer).
pub const RESP_COMMAND_MAX: usize = 8192;
/// Longest RESP reply answered (`redis_client`'s reply buffer); a longer one
/// is answered 502, as `redis_client` answers one.
pub const REPLY_MAX: usize = 8192;
/// The typed KV operation body built from one command: the command's
/// arguments and the operation's fixed fields.
pub const OP_BODY_MAX: usize = COMMAND_MAX + 256;
/// Room a RESP reply is rendered into before it is judged against
/// [`REPLY_MAX`]: a store answer at its bound, every element framed.
pub const REPLY_RENDER_MAX: usize = 2 * REPLY_MAX + 64;

/// Whether `name` is a command whose meaning is a connection's state. A
/// chain of comparisons, not a table: a table of slices is a table of
/// pointers, which a PIC module cannot relocate.
fn is_session_command(name: &[u8]) -> bool {
    eq_ascii_ci(name, b"AUTH")
        || eq_ascii_ci(name, b"HELLO")
        || eq_ascii_ci(name, b"SELECT")
        || eq_ascii_ci(name, b"CLIENT")
        || eq_ascii_ci(name, b"QUIT")
        || eq_ascii_ci(name, b"RESET")
        || eq_ascii_ci(name, b"MULTI")
        || eq_ascii_ci(name, b"EXEC")
        || eq_ascii_ci(name, b"DISCARD")
        || eq_ascii_ci(name, b"WATCH")
        || eq_ascii_ci(name, b"UNWATCH")
        || eq_ascii_ci(name, b"SUBSCRIBE")
        || eq_ascii_ci(name, b"PSUBSCRIBE")
        || eq_ascii_ci(name, b"UNSUBSCRIBE")
        || eq_ascii_ci(name, b"PUNSUBSCRIBE")
        || eq_ascii_ci(name, b"PUBLISH")
}

/// What a request body asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Plan {
    /// 400 with an empty body: not exactly one structured command.
    BadRequest,
    /// 413 with an empty body: its RESP form is past [`RESP_COMMAND_MAX`].
    TooLarge,
    /// Answered without the store: the RESP reply is `reply[..len]`.
    Reply(usize),
    /// The typed operation `op` over `body[..len]`, read at `consistency`.
    Forward { op: u8, consistency: u8, len: usize },
}

/// Plan the structured command `cmd`. `resp` takes its RESP form (the
/// argument views point into it), `body` the operation body, `reply` an
/// answer given without the store.
pub fn plan(
    cmd: &[u8],
    resp: &mut [u8; RESP_COMMAND_MAX],
    body: &mut [u8; OP_BODY_MAX],
    reply: &mut [u8; REPLY_RENDER_MAX],
) -> Plan {
    if cmd.first().copied().unwrap_or(0) == 0 || structured_len(cmd) != Some(cmd.len()) {
        return Plan::BadRequest;
    }
    let Some(n) = resp_cmd_structured(cmd, resp) else {
        return Plan::TooLarge;
    };
    let argv = match parse_one(&resp[..n]) {
        ParseStep::Ready { argv, .. } => argv,
        ParseStep::Error(line) => {
            let mut len = 0usize;
            let _ = enc_raw(reply, &mut len, line);
            return Plan::Reply(len);
        }
        ParseStep::Incomplete => return Plan::BadRequest,
    };
    let resp = &resp[..n];
    let name = argv.arg(resp, 0);
    let mut len = 0usize;
    if eq_ascii_ci(name, b"PING") {
        if argv.count >= 2 {
            let _ = enc_bulk(reply, &mut len, argv.arg(resp, 1));
        } else {
            let _ = enc_simple_str(reply, &mut len, b"PONG");
        }
        return Plan::Reply(len);
    }
    if eq_ascii_ci(name, b"ECHO") {
        if argv.count < 2 {
            let _ = enc_error(reply, &mut len, b"ERR wrong number of arguments for 'echo'");
        } else {
            let _ = enc_bulk(reply, &mut len, argv.arg(resp, 1));
        }
        return Plan::Reply(len);
    }
    if eq_ascii_ci(name, b"COMMAND") {
        let _ = enc_array_header(reply, &mut len, 0);
        return Plan::Reply(len);
    }
    if is_session_command(name) {
        let _ = enc_raw(reply, &mut len, b"-ERR '");
        let _ = enc_raw(reply, &mut len, name);
        let _ = enc_raw(
            reply,
            &mut len,
            b"' needs a connection; the KV front serves one command per exchange\r\n",
        );
        return Plan::Reply(len);
    }
    match build_op_body(resp, name, &argv, body) {
        Built::Ok {
            op,
            consistency,
            body_len,
        } => Plan::Forward {
            op,
            consistency,
            len: body_len,
        },
        Built::BadArgs(msg) => {
            let _ = enc_error(reply, &mut len, msg);
            Plan::Reply(len)
        }
        Built::Unknown => {
            let _ = enc_raw(reply, &mut len, b"-ERR unknown command '");
            let _ = enc_raw(reply, &mut len, name);
            let _ = enc_raw(reply, &mut len, b"'\r\n");
            Plan::Reply(len)
        }
    }
}

/// Render the store's answer (`result`, `body`) as the RESP reply lattice's
/// Redis connector sends for it: its length in `reply`.
pub fn render(result: u8, body: &[u8], reply: &mut [u8; REPLY_RENDER_MAX]) -> usize {
    let mut len = 0usize;
    redis_codec::encode_kv_reply(reply, &mut len, result, body);
    len
}

/// How a RESP reply answers, by `redis_client`'s mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    /// 200, body `reply[start..end]`.
    Ok { start: usize, end: usize },
    /// 400 (a `-ERR` reply), body `reply[start..end]`.
    Refused { start: usize, end: usize },
    /// 502: the reply is past [`REPLY_MAX`], or is not one whole reply.
    TooLarge,
}

/// Map the RESP reply `reply` onto a status and body as `redis_client` does:
/// a simple string's text, an integer's digits, a bulk string's data, a nil
/// as empty, an array whole, an error's text.
pub fn answer(reply: &[u8]) -> Answer {
    if reply.len() > REPLY_MAX {
        return Answer::TooLarge;
    }
    let r = resp_classify(reply);
    if r.kind == RespKind::Incomplete || r.total != reply.len() || r.val_end < r.val_start {
        return Answer::TooLarge;
    }
    if r.is_error() {
        Answer::Refused {
            start: r.val_start,
            end: r.val_end,
        }
    } else {
        Answer::Ok {
            start: r.val_start,
            end: r.val_end,
        }
    }
}
