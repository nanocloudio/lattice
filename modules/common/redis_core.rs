// Bounded, no_std, no-alloc REDIS protocol core. Like the other `*_core.rs`
// files it carries no inner attributes and no test module, so it is `include!`d
// verbatim by both this crate (for host tests) and the on-device `redis` .fmod —
// one source of truth. Builds on `resp_core.rs`.
//
// This is the piece that makes a GENUINE connector, not a codec: it owns RESP
// command construction, reply classification, AND the connection state machine —
// a persistent connection with a multi-round-trip `AUTH` handshake. That
// handshake (send AUTH, branch on the server's `+OK`/`-ERR`, only then serve
// requests) is exactly what a stateless encode/decode bytecode program cannot
// express: it is reply-dependent and spans round trips. Keeping it here as a pure
// function makes the protocol logic host-testable; the `.fmod` is just the I/O
// pump that maps actions to net_proto frames.

/// Encode a RESP command from a STRUCTURED command frame — `[nargs:u8]` then per
/// arg `[len:u16 LE][bytes]` — into `out` as `*<n>\r\n$<len>\r\n<arg>\r\n…`.
/// Upstream sends structured args and the connector owns the wire encoding, so
/// the protocol lives in the module (compiled), not in a param. `None` on a
/// malformed frame or insufficient output space.
pub fn resp_cmd_structured(cmd: &[u8], out: &mut [u8]) -> Option<usize> {
    let nargs = *cmd.first()? as usize;
    let mut pos = 0usize;
    resp_put(out, &mut pos, b"*")?;
    let mut num = [0u8; 20];
    let ln = itoa(nargs as i64, &mut num)?;
    resp_put(out, &mut pos, &num[..ln])?;
    resp_put(out, &mut pos, b"\r\n")?;
    let mut ip = 1usize;
    for _ in 0..nargs {
        let lo = *cmd.get(ip)?;
        let hi = *cmd.get(ip + 1)?;
        let alen = u16::from_le_bytes([lo, hi]) as usize;
        ip += 2;
        let arg = cmd.get(ip..ip + alen)?;
        ip += alen;
        resp_bulk(out, &mut pos, arg)?;
    }
    Some(pos)
}

/// Encode a RESP `AUTH <password>` command into `out`.
pub fn resp_auth(password: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut pos = 0usize;
    resp_put(out, &mut pos, b"*2\r\n$4\r\nAUTH\r\n")?;
    resp_bulk(out, &mut pos, password)?;
    Some(pos)
}

/// The shape of a complete RESP reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespKind {
    Simple,     // +OK
    Error,      // -ERR …
    Int,        // :N
    Bulk,       // $len\r\n…\r\n
    Nil,        // $-1
    Array,      // *N (surfaced whole; not decomposed here)
    Incomplete, // need more bytes
}

/// A classified reply. `val` is the meaningful payload byte range (simple/error
/// text, or bulk data); `int` holds an integer reply; `total` is the reply's byte
/// length (0 when incomplete).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RespReply {
    pub kind: RespKind,
    pub val_start: usize,
    pub val_end: usize,
    pub int: i64,
    pub total: usize,
}

impl RespReply {
    fn incomplete() -> Self {
        Self {
            kind: RespKind::Incomplete,
            val_start: 0,
            val_end: 0,
            int: 0,
            total: 0,
        }
    }
    /// True for a `-ERR` reply (server-side error).
    pub fn is_error(&self) -> bool {
        matches!(self.kind, RespKind::Error)
    }
}

/// Classify the first complete reply in `buf`. Returns `Incomplete` (via
/// `resp_reply_len`) if more bytes are needed. Never panics on malformed bytes.
pub fn resp_classify(buf: &[u8]) -> RespReply {
    let total = match resp_reply_len(buf) {
        Some(t) => t,
        None => return RespReply::incomplete(),
    };
    match buf[0] {
        b'+' => RespReply {
            kind: RespKind::Simple,
            val_start: 1,
            val_end: total - 2,
            int: 0,
            total,
        },
        b'-' => RespReply {
            kind: RespKind::Error,
            val_start: 1,
            val_end: total - 2,
            int: 0,
            total,
        },
        b':' => {
            let v = resp_parse_int(&buf[1..total - 2]).unwrap_or(0);
            RespReply {
                kind: RespKind::Int,
                val_start: 1,
                val_end: total - 2,
                int: v,
                total,
            }
        }
        b'$' => {
            let header_end = match resp_crlf(buf, 1) {
                Some(h) => h,
                None => return RespReply::incomplete(),
            };
            let n = resp_parse_int(&buf[1..header_end]).unwrap_or(-1);
            if n < 0 {
                RespReply {
                    kind: RespKind::Nil,
                    val_start: 0,
                    val_end: 0,
                    int: -1,
                    total,
                }
            } else {
                let start = header_end + 2;
                RespReply {
                    kind: RespKind::Bulk,
                    val_start: start,
                    val_end: start + n as usize,
                    int: n,
                    total,
                }
            }
        }
        b'*' => RespReply {
            kind: RespKind::Array,
            val_start: 0,
            val_end: total,
            int: 0,
            total,
        },
        _ => RespReply {
            kind: RespKind::Simple,
            val_start: 0,
            val_end: total,
            int: 0,
            total,
        },
    }
}

/// Connection lifecycle. A persistent connection: `Ready` holds an OPEN socket
/// between requests; `Authing` gates `Ready` on a successful `AUTH`. `#[repr(u8)]`
/// so it can live in a module's `#[repr(C)]` state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Phase {
    Disconnected = 0,
    Connecting = 1,
    Authing = 2,
    Ready = 3,
    Awaiting = 4,
}

/// Events the I/O layer feeds the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ev {
    HaveRequest, // a command is queued to send
    Connected,   // the socket connected
    ReplyOk,     // a complete reply arrived (not a `-ERR`)
    ReplyErr,    // a complete `-ERR` reply arrived
    PeerClosed,  // the server closed the connection
    NetError,    // a transport error
}

/// Actions the state machine asks the I/O layer to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Act {
    None,
    Connect,      // open the socket
    SendAuth,     // send AUTH <password>
    SendRequest,  // send the queued command
    DeliverReply, // emit the reply downstream, connection stays open
    FailRequest,  // drop the in-flight request and reset the connection
}

/// The pure Redis connection state machine. Given the current `phase`, an `ev`,
/// and whether a password is configured (`needs_auth`), return the action to
/// perform and the next phase.
///
/// The two properties a codec cannot have live here:
///  * **persistence** — a delivered reply returns to `Ready` with the socket
///    still open, so the next request reuses it (no reconnect per call);
///  * **the AUTH handshake** — `Connecting → Connected` sends AUTH first when a
///    password is set, and `Ready` is only reached after the server's `+OK`; a
///    `-ERR` to AUTH fails closed rather than serving requests unauthenticated.
pub fn redis_transition(phase: Phase, ev: Ev, needs_auth: bool) -> (Act, Phase) {
    use Act::*;
    use Ev::*;
    use Phase::*;
    match (phase, ev) {
        // A request while disconnected opens the connection.
        (Disconnected, HaveRequest) => (Connect, Connecting),
        // Connected: authenticate first if a password is set, else send now.
        (Connecting, Connected) => {
            if needs_auth {
                (SendAuth, Authing)
            } else {
                (SendRequest, Awaiting)
            }
        }
        // AUTH reply gates readiness: +OK → serve the queued request; -ERR → fail.
        (Authing, ReplyOk) => (SendRequest, Awaiting),
        (Authing, ReplyErr) => (FailRequest, Disconnected),
        // A command reply (ok OR -ERR) is delivered; the socket stays open.
        (Awaiting, ReplyOk) | (Awaiting, ReplyErr) => (DeliverReply, Ready),
        // Ready with a queued request → reuse the open socket, no reconnect.
        (Ready, HaveRequest) => (SendRequest, Awaiting),
        // Transport failure: fail an in-flight request, otherwise just reset.
        (Awaiting, PeerClosed) | (Awaiting, NetError) => (FailRequest, Disconnected),
        (Authing, PeerClosed) | (Authing, NetError) => (FailRequest, Disconnected),
        (_, PeerClosed) | (_, NetError) => (None, Disconnected),
        _ => (None, phase),
    }
}
