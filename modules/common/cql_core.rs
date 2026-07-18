// Bounded, no_std, no-alloc Cassandra CQL native-protocol core (v4) — frame
// framing, the STARTUP/AUTH_RESPONSE builders, and the connection state machine.
// `include!`d by the host crate (tests) and the `cassandra` .fmod.
//
// CQL is a binary FRAME protocol with per-frame stream multiplexing: every frame
// carries a stream id so many in-flight queries share one connection, and the
// connection opens with a negotiation — the client sends STARTUP (offering a CQL
// version) and the server answers READY (no auth) or AUTHENTICATE (then a PLAIN
// AUTH_RESPONSE / AUTH_SUCCESS exchange). A staged negotiation with a
// reply-dependent branch (ready vs authenticate) over a stream-tagged binary
// frame layer is a stateful session, not request/reply.
//
// Frame: [version:1][flags:1][stream:i16 BE][opcode:1][length:u32 BE][body]
//   request version 0x04, response version 0x84.

pub mod cql_op {
    pub const ERROR: u8 = 0x00;
    pub const STARTUP: u8 = 0x01;
    pub const READY: u8 = 0x02;
    pub const AUTHENTICATE: u8 = 0x03;
    pub const OPTIONS: u8 = 0x05;
    pub const SUPPORTED: u8 = 0x06;
    pub const QUERY: u8 = 0x07;
    pub const RESULT: u8 = 0x08;
    pub const AUTH_CHALLENGE: u8 = 0x0E;
    pub const AUTH_RESPONSE: u8 = 0x0F;
    pub const AUTH_SUCCESS: u8 = 0x10;
}

pub const CQL_VERSION_REQUEST: u8 = 0x04;

fn cput(out: &mut [u8], pos: &mut usize, b: &[u8]) -> Option<()> {
    if *pos + b.len() > out.len() {
        return None;
    }
    out[*pos..*pos + b.len()].copy_from_slice(b);
    *pos += b.len();
    Some(())
}

/// Build a CQL request frame `[0x04][flags=0][stream][opcode][len:u32][body]`.
pub fn cql_frame(opcode: u8, stream: i16, body: &[u8], out: &mut [u8]) -> Option<usize> {
    let total = 9 + body.len();
    if total > out.len() {
        return None;
    }
    out[0] = CQL_VERSION_REQUEST;
    out[1] = 0; // flags
    out[2..4].copy_from_slice(&stream.to_be_bytes());
    out[4] = opcode;
    out[5..9].copy_from_slice(&(body.len() as u32).to_be_bytes());
    out[9..total].copy_from_slice(body);
    Some(total)
}

/// STARTUP body: a `[string map]` `{"CQL_VERSION": "3.0.0"}`.
/// string map = `[n:u16]` then per entry `[klen:u16][k][vlen:u16][v]`.
pub fn cql_startup(out: &mut [u8]) -> Option<usize> {
    let mut body = [0u8; 64];
    let mut p = 0;
    cput(&mut body, &mut p, &1u16.to_be_bytes())?; // one entry
    cput(&mut body, &mut p, &11u16.to_be_bytes())?;
    cput(&mut body, &mut p, b"CQL_VERSION")?;
    cput(&mut body, &mut p, &5u16.to_be_bytes())?;
    cput(&mut body, &mut p, b"3.0.0")?;
    cql_frame(cql_op::STARTUP, 1, &body[..p], out)
}

/// AUTH_RESPONSE body for the PasswordAuthenticator: a `[bytes]`
/// (`[len:i32 BE][data]`) whose data is `\0user\0pass` (SASL PLAIN).
pub fn cql_auth_response(user: &[u8], pass: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut body = [0u8; 256];
    let mut p = 0;
    let token_len = 1 + user.len() + 1 + pass.len();
    cput(&mut body, &mut p, &(token_len as i32).to_be_bytes())?;
    cput(&mut body, &mut p, &[0])?;
    cput(&mut body, &mut p, user)?;
    cput(&mut body, &mut p, &[0])?;
    cput(&mut body, &mut p, pass)?;
    cql_frame(cql_op::AUTH_RESPONSE, 1, &body[..p], out)
}

/// A parsed frame view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CqlFrame {
    pub version: u8,
    pub stream: i16,
    pub opcode: u8,
    pub body_start: usize,
    pub body_end: usize,
    pub total: usize,
}

/// Frame the first complete CQL frame in `buf`, or `None` if truncated. Never
/// panics.
pub fn cql_parse_frame(buf: &[u8]) -> Option<CqlFrame> {
    if buf.len() < 9 {
        return None;
    }
    let version = buf[0];
    let stream = i16::from_be_bytes([buf[2], buf[3]]);
    let opcode = buf[4];
    let len = u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]) as usize;
    let total = 9 + len;
    if buf.len() < total {
        return None;
    }
    Some(CqlFrame {
        version,
        stream,
        opcode,
        body_start: 9,
        body_end: total,
        total,
    })
}

// ---- state machine ----------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CPhase {
    Disconnected = 0,
    Connecting = 1,
    /// STARTUP sent; awaiting READY or AUTHENTICATE.
    AwaitReady = 2,
    /// AUTH_RESPONSE sent; awaiting AUTH_SUCCESS.
    AwaitAuthSuccess = 3,
    /// Session established.
    Ready = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CEv {
    Start,
    Connected,
    GotReady,
    GotAuthenticate,
    GotAuthSuccess,
    GotError,
    PeerClosed,
    NetError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CAct {
    None,
    Connect,
    SendStartup,
    SendAuthResponse,
    Fail,
}

/// Map a received opcode to the state-machine event.
pub fn cql_classify(opcode: u8) -> Option<CEv> {
    match opcode {
        cql_op::READY => Some(CEv::GotReady),
        cql_op::AUTHENTICATE => Some(CEv::GotAuthenticate),
        cql_op::AUTH_SUCCESS => Some(CEv::GotAuthSuccess),
        cql_op::ERROR => Some(CEv::GotError),
        _ => None,
    }
}

/// The CQL connection state machine — pure and host-testable. The reply-dependent
/// branch is `AwaitReady`: the server answers STARTUP with either READY (open) or
/// AUTHENTICATE (which forces the PLAIN AUTH_RESPONSE/AUTH_SUCCESS exchange).
pub fn cql_transition(phase: CPhase, ev: CEv) -> (CAct, CPhase) {
    use CAct::*;
    use CEv::*;
    use CPhase::*;
    match (phase, ev) {
        (Disconnected, Start) => (Connect, Connecting),
        (Connecting, Connected) => (SendStartup, AwaitReady),
        (AwaitReady, GotReady) => (None, Ready),
        (AwaitReady, GotAuthenticate) => (SendAuthResponse, AwaitAuthSuccess),
        (AwaitAuthSuccess, GotAuthSuccess) => (None, Ready),
        (_, GotError) | (_, PeerClosed) | (_, NetError) => (Fail, Disconnected),
        _ => (None, phase),
    }
}
