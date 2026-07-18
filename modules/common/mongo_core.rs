// Bounded, no_std, no-alloc MongoDB protocol core — a minimal BSON writer/reader,
// OP_MSG framing, and the SCRAM-SHA-256 authentication flow. `include!`d by the
// host crate (tests) and the `mongo` .fmod.
//
// The point of this connector is REUSE: MongoDB authenticates with SCRAM-SHA-256,
// so it drives the SAME `scram_core` the Postgres connector does — one crypto
// core (SHA-256/HMAC/PBKDF2, client proof over the server's random salt/nonce),
// two entirely unrelated wire protocols (Postgres' typed message stream vs
// MongoDB's BSON/OP_MSG). That a stateless codec cannot do the crypto is already
// settled by pg; MongoDB shows the compiled-module protocol logic composing with
// a shared, host-verified core rather than re-implementing it.
//
// OP_MSG (opcode 2013): [len:i32][requestID:i32][responseTo:i32][opCode:i32]
//                       [flagBits:u32][section kind:u8 = 0][BSON document]
// Auth is two round trips of the `saslStart`/`saslContinue` commands, each an
// OP_MSG whose BSON body carries the SCRAM message as a binary field.

// ---- minimal BSON -----------------------------------------------------------

pub mod bson {
    pub const DOUBLE: u8 = 0x01;
    pub const STRING: u8 = 0x02;
    pub const DOC: u8 = 0x03;
    pub const ARRAY: u8 = 0x04;
    pub const BINARY: u8 = 0x05;
    pub const BOOL: u8 = 0x08;
    pub const INT32: u8 = 0x10;
    pub const INT64: u8 = 0x12;
}

fn bput(out: &mut [u8], pos: &mut usize, b: &[u8]) -> Option<()> {
    if *pos + b.len() > out.len() {
        return None;
    }
    out[*pos..*pos + b.len()].copy_from_slice(b);
    *pos += b.len();
    Some(())
}

/// Write an element header `[type][name\0]`.
fn bson_elem(out: &mut [u8], pos: &mut usize, ty: u8, name: &[u8]) -> Option<()> {
    bput(out, pos, &[ty])?;
    bput(out, pos, name)?;
    bput(out, pos, &[0])
}

pub fn bson_i32(out: &mut [u8], pos: &mut usize, name: &[u8], v: i32) -> Option<()> {
    bson_elem(out, pos, bson::INT32, name)?;
    bput(out, pos, &v.to_le_bytes())
}

/// Append an element with a pre-encoded value: `[type][name\0][value…]`.
/// Used to copy an element verbatim from one document to another.
pub fn bson_append_raw(
    out: &mut [u8],
    pos: &mut usize,
    ty: u8,
    name: &[u8],
    value: &[u8],
) -> Option<()> {
    bson_elem(out, pos, ty, name)?;
    bput(out, pos, value)
}

pub fn bson_bool(out: &mut [u8], pos: &mut usize, name: &[u8], v: bool) -> Option<()> {
    bson_elem(out, pos, bson::BOOL, name)?;
    bput(out, pos, &[u8::from(v)])
}

pub fn bson_str(out: &mut [u8], pos: &mut usize, name: &[u8], s: &[u8]) -> Option<()> {
    bson_elem(out, pos, bson::STRING, name)?;
    bput(out, pos, &((s.len() + 1) as i32).to_le_bytes())?;
    bput(out, pos, s)?;
    bput(out, pos, &[0])
}

/// Binary, subtype 0 (generic). SCRAM payloads travel this way.
pub fn bson_bin(out: &mut [u8], pos: &mut usize, name: &[u8], data: &[u8]) -> Option<()> {
    bson_elem(out, pos, bson::BINARY, name)?;
    bput(out, pos, &(data.len() as i32).to_le_bytes())?;
    bput(out, pos, &[0])?; // subtype 0
    bput(out, pos, data)
}

/// Open a document: reserve the 4-byte length and return its offset.
pub fn bson_open(out: &mut [u8], pos: &mut usize) -> Option<usize> {
    let start = *pos;
    bput(out, pos, &[0, 0, 0, 0])?;
    Some(start)
}

/// Close a document opened at `start`: write the `0x00` terminator and backfill
/// the total length. Returns the document's byte length.
pub fn bson_close(out: &mut [u8], pos: &mut usize, start: usize) -> Option<usize> {
    bput(out, pos, &[0])?;
    let len = *pos - start;
    out[start..start + 4].copy_from_slice(&(len as i32).to_le_bytes());
    Some(len)
}

/// Find element `name` in a BSON `doc`; returns `(type, value_offset)` where the
/// value bytes begin. Bounds-checked; `None` if absent or malformed.
pub fn bson_find(doc: &[u8], name: &[u8]) -> Option<(u8, usize)> {
    if doc.len() < 5 {
        return None;
    }
    let mut i = 4; // skip length
    while i < doc.len() {
        let ty = doc[i];
        if ty == 0 {
            break;
        }
        i += 1;
        let nstart = i;
        while i < doc.len() && doc[i] != 0 {
            i += 1;
        }
        if i >= doc.len() {
            return None;
        }
        let ename = &doc[nstart..i];
        i += 1; // skip name NUL
        let vstart = i;
        // advance past the value to the next element
        let vlen = bson_value_len(doc, ty, vstart)?;
        if ename == name {
            return Some((ty, vstart));
        }
        i = vstart + vlen;
    }
    None
}

/// Byte length of a value of type `ty` starting at `at`.
fn bson_value_len(doc: &[u8], ty: u8, at: usize) -> Option<usize> {
    match ty {
        bson::DOUBLE | bson::INT64 => Some(8),
        bson::INT32 => Some(4),
        bson::BOOL => Some(1),
        bson::STRING => {
            let n = i32::from_le_bytes(doc.get(at..at + 4)?.try_into().ok()?);
            Some(4 + n as usize)
        }
        bson::DOC => {
            let n = i32::from_le_bytes(doc.get(at..at + 4)?.try_into().ok()?);
            Some(n as usize)
        }
        bson::BINARY => {
            let n = i32::from_le_bytes(doc.get(at..at + 4)?.try_into().ok()?);
            Some(4 + 1 + n as usize) // len + subtype + bytes
        }
        _ => None,
    }
}

pub fn bson_get_i32(doc: &[u8], name: &[u8]) -> Option<i32> {
    let (ty, at) = bson_find(doc, name)?;
    if ty != bson::INT32 {
        return None;
    }
    Some(i32::from_le_bytes(doc.get(at..at + 4)?.try_into().ok()?))
}

pub fn bson_get_bool(doc: &[u8], name: &[u8]) -> Option<bool> {
    let (ty, at) = bson_find(doc, name)?;
    if ty != bson::BOOL {
        return None;
    }
    Some(*doc.get(at)? != 0)
}

/// A double or int32 field as f64/i64 truthiness — MongoDB's `ok` is often a
/// double 1.0. Returns true when the value is non-zero.
pub fn bson_ok(doc: &[u8]) -> bool {
    if let Some(v) = bson_get_i32(doc, b"ok") {
        return v != 0;
    }
    if let Some((bson::DOUBLE, at)) = bson_find(doc, b"ok") {
        if let Some(b) = doc.get(at..at + 8) {
            let d = f64::from_le_bytes(b.try_into().unwrap());
            return d != 0.0;
        }
    }
    false
}

/// The `payload` binary field's byte range (the SCRAM message from the server).
pub fn bson_get_bin(doc: &[u8], name: &[u8]) -> Option<(usize, usize)> {
    let (ty, at) = bson_find(doc, name)?;
    if ty != bson::BINARY {
        return None;
    }
    let n = i32::from_le_bytes(doc.get(at..at + 4)?.try_into().ok()?) as usize;
    let start = at + 5; // len(4) + subtype(1)
    let end = start + n;
    if end > doc.len() {
        return None;
    }
    Some((start, end))
}

// ---- OP_MSG framing ---------------------------------------------------------

/// Frame a BSON command body as an OP_MSG. `request_id` correlates the reply.
pub fn mongo_op_msg(request_id: i32, body: &[u8], out: &mut [u8]) -> Option<usize> {
    let total = 16 + 4 + 1 + body.len();
    if total > out.len() {
        return None;
    }
    out[0..4].copy_from_slice(&(total as i32).to_le_bytes());
    out[4..8].copy_from_slice(&request_id.to_le_bytes());
    out[8..12].copy_from_slice(&0i32.to_le_bytes()); // responseTo
    out[12..16].copy_from_slice(&2013i32.to_le_bytes()); // opCode OP_MSG
    out[16..20].copy_from_slice(&0u32.to_le_bytes()); // flagBits
    out[20] = 0; // section kind 0 (body)
    out[21..21 + body.len()].copy_from_slice(body);
    Some(total)
}

/// Length of the first complete OP_MSG in `buf` (the leading i32), or `None`.
pub fn mongo_reply_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < 4 {
        return None;
    }
    let n = i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if n < 21 {
        return None;
    }
    let total = n as usize;
    if buf.len() < total {
        return None;
    }
    Some(total)
}

/// The BSON body range of a complete OP_MSG reply (skip 16 header + 4 flags + 1
/// section kind).
pub fn mongo_reply_body(buf: &[u8]) -> Option<(usize, usize)> {
    let total = mongo_reply_len(buf)?;
    let start = 21;
    if start > total {
        return None;
    }
    Some((start, total))
}

// ---- SCRAM-over-Mongo commands ----------------------------------------------

/// Build a `saslStart` OP_MSG carrying the SCRAM `client-first-message` as the
/// `payload` binary. `client_first` comes from `scram_client_first`.
pub fn mongo_sasl_start(
    request_id: i32,
    database: &[u8],
    client_first: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let mut body = [0u8; 512];
    let mut p = 0;
    let start = bson_open(&mut body, &mut p)?;
    bson_i32(&mut body, &mut p, b"saslStart", 1)?;
    bson_str(&mut body, &mut p, b"mechanism", b"SCRAM-SHA-256")?;
    bson_bin(&mut body, &mut p, b"payload", client_first)?;
    // options: { skipEmptyExchange: true } — finish SCRAM in two round trips
    // (the server sets done:true on the saslContinue response) instead of
    // demanding a third, empty saslContinue.
    bson_elem(&mut body, &mut p, bson::DOC, b"options")?;
    let ostart = bson_open(&mut body, &mut p)?;
    bson_bool(&mut body, &mut p, b"skipEmptyExchange", true)?;
    bson_close(&mut body, &mut p, ostart)?;
    bson_i32(&mut body, &mut p, b"autoAuthorize", 1)?;
    bson_str(&mut body, &mut p, b"$db", database)?;
    bson_close(&mut body, &mut p, start)?;
    mongo_op_msg(request_id, &body[..p], out)
}

/// Build a `saslContinue` OP_MSG carrying the SCRAM `client-final-message`.
pub fn mongo_sasl_continue(
    request_id: i32,
    database: &[u8],
    conversation_id: i32,
    client_final: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let mut body = [0u8; 512];
    let mut p = 0;
    let start = bson_open(&mut body, &mut p)?;
    bson_i32(&mut body, &mut p, b"saslContinue", 1)?;
    bson_i32(&mut body, &mut p, b"conversationId", conversation_id)?;
    bson_bin(&mut body, &mut p, b"payload", client_final)?;
    bson_str(&mut body, &mut p, b"$db", database)?;
    bson_close(&mut body, &mut p, start)?;
    mongo_op_msg(request_id, &body[..p], out)
}

/// Build a simple `{ping: 1}` command (used to confirm the authed connection).
pub fn mongo_ping(request_id: i32, database: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut body = [0u8; 128];
    let mut p = 0;
    let start = bson_open(&mut body, &mut p)?;
    bson_i32(&mut body, &mut p, b"ping", 1)?;
    bson_str(&mut body, &mut p, b"$db", database)?;
    bson_close(&mut body, &mut p, start)?;
    mongo_op_msg(request_id, &body[..p], out)
}

/// Build an `insert` command OP_MSG: `{ insert: <collection>, documents: [ {
/// value: <value> } ], $db: <database> }`. A genuine MongoDB write over the
/// authenticated connection, in BSON/OP_MSG — the insert-sink analogue of the
/// redis/pg/kafka producers. `documents` is a BSON array whose one element is a
/// sub-document; the nested length-prefixed framing is what a stateless codec
/// cannot assemble.
pub fn mongo_insert_body(
    request_id: i32,
    database: &[u8],
    collection: &[u8],
    value: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let mut body = [0u8; 512];
    let mut p = 0usize;
    let cmd = bson_open(&mut body, &mut p)?;
    bson_str(&mut body, &mut p, b"insert", collection)?;
    // documents : ARRAY [ "0" : DOC { value: <value> } ]
    bson_elem(&mut body, &mut p, bson::ARRAY, b"documents")?;
    let arr = bson_open(&mut body, &mut p)?;
    bson_elem(&mut body, &mut p, bson::DOC, b"0")?;
    let doc0 = bson_open(&mut body, &mut p)?;
    bson_str(&mut body, &mut p, b"value", value)?;
    bson_close(&mut body, &mut p, doc0)?;
    bson_close(&mut body, &mut p, arr)?;
    bson_str(&mut body, &mut p, b"$db", database)?;
    bson_close(&mut body, &mut p, cmd)?;
    mongo_op_msg(request_id, &body[..p], out)
}

// ---- auth state machine -----------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MPhase {
    Disconnected = 0,
    Connecting = 1,
    /// saslStart sent; awaiting the server-first payload.
    AwaitSaslStart = 2,
    /// saslContinue sent; awaiting the server-final (done) payload.
    AwaitSaslContinue = 3,
    /// Authenticated; the connection is usable.
    Ready = 4,
    /// A post-auth command is in flight.
    AwaitCommand = 5,
    /// INSERT-sink mode: authenticated, waiting for the next `request_in` value.
    InsertIdle = 6,
    /// INSERT-sink mode: an insert command is in flight, awaiting the ack.
    InsertWait = 7,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MEv {
    Start,
    Connected,
    SaslContinueNeeded, // server-first arrived, ok, not done
    SaslDone,           // server-final arrived, done
    CommandReply,
    AuthFailed,
    PeerClosed,
    NetError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MAct {
    None,
    Connect,
    SendSaslStart,
    SendSaslContinue,
    DeliverReply,
    Fail,
}

/// The MongoDB auth + command state machine — pure and host-testable. The auth
/// phases mirror pg's SCRAM exchange; the difference is entirely in the wire
/// encoding (BSON/OP_MSG here, typed messages there) — the crypto is shared.
pub fn mongo_transition(phase: MPhase, ev: MEv) -> (MAct, MPhase) {
    use MAct::*;
    use MEv::*;
    use MPhase::*;
    match (phase, ev) {
        (Disconnected, Start) => (Connect, Connecting),
        (Connecting, Connected) => (SendSaslStart, AwaitSaslStart),
        (AwaitSaslStart, SaslContinueNeeded) => (SendSaslContinue, AwaitSaslContinue),
        (AwaitSaslContinue, SaslDone) => (None, Ready),
        (AwaitCommand, CommandReply) => (DeliverReply, Ready),
        (_, AuthFailed) => (Fail, Disconnected),
        (_, PeerClosed) | (_, NetError) => (Fail, Disconnected),
        _ => (None, phase),
    }
}
