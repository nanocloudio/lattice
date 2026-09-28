//! Server-side MongoDB wire framing for the document capability.
//!
//! The document capability's first connector speaks the MongoDB wire
//! protocol — OP_MSG (opcode 2013) carrying BSON command documents —
//! because that is what real document clients speak, and capabilities
//! are proven against real clients. This file owns the CONNECTOR half
//! of the dialect/executor split the SQL connectors follow:
//! framing, the handshake surface, reply encoding, error mapping. It
//! does NOT own key composition (`models.rs`), value semantics, or
//! anything a second document connector would have to agree on.
//!
//! BSON primitives come from `mongo_core.rs` — the same writer/reader
//! the OUTBOUND mongo client uses, mounted through one chain so the
//! two sides can never disagree about an encoding. This file adds only
//! what a SERVER needs and the client side never did: a complete element
//! reader (every type a document may carry, the rest refused by type),
//! and the reply shapes (`hello`, cursor batches, write results, command
//! errors).
//!
//! ## What is not here
//!
//! Document meaning: identity, filters, updates, projections — the
//! shared `document_executor` owns them (`doc_exec.rs`, `doc_ops.rs`), and
//! the translation from MongoDB's spellings is the dialect's
//! (`mongo_dialect.rs`). This file frames and reads BSON and builds the
//! reply shapes; it decides nothing about a document.

#![allow(
    dead_code,
    reason = "shared via #[path] into the anchor module and host tests; each consumer uses a subset"
)]

// One mount chain: the BSON primitives are the client connector's, so
// the two sides share one encoding by construction.
#[path = "mongo_core.rs"]
pub mod mongo_core;

use mongo_core::{bson, bson_bool, bson_close, bson_i32, bson_open, bson_str};

/// OP_MSG opcode.
pub const OP_MSG: i32 = 2013;

/// Wire versions advertised in `hello`. 17 = MongoDB 6.0's wire
/// version: high enough that modern drivers pick plain OP_MSG paths,
/// low enough to promise nothing exotic.
pub const MAX_WIRE_VERSION: i32 = 17;
pub const MIN_WIRE_VERSION: i32 = 0;

/// Largest BSON document accepted or produced.
pub const MAX_BSON: usize = 4096;

/// One parsed inbound OP_MSG.
pub struct Request<'a> {
    pub request_id: i32,
    /// The kind-0 body section's BSON document.
    pub body: &'a [u8],
}

/// Parse one complete OP_MSG from `buf`. `Ok(None)` = incomplete,
/// `Err` = malformed (close the connection: framing is unrecoverable).
pub fn parse_op_msg(buf: &[u8]) -> Result<Option<(Request<'_>, usize)>, ()> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = i32::from_le_bytes(buf[0..4].try_into().map_err(|_| ())?) as usize;
    if !(21..=MAX_BSON + 21).contains(&len) {
        return Err(()); // header(16) + flags(4) + kind(1) + minimal doc
    }
    if buf.len() < len {
        return Ok(None);
    }
    let request_id = i32::from_le_bytes(buf[4..8].try_into().map_err(|_| ())?);
    let opcode = i32::from_le_bytes(buf[12..16].try_into().map_err(|_| ())?);
    if opcode != OP_MSG {
        return Err(());
    }
    let flags = u32::from_le_bytes(buf[16..20].try_into().map_err(|_| ())?);
    // checksumPresent / moreToCome / exhaustAllowed are the defined
    // bits; anything REQUIRED (bit 16..) refuses. moreToCome from a
    // client is a fire-and-forget write — refuse rather than silently
    // not replying (unsupported, not approximated).
    if flags & 0xFFFF_0002 != 0 {
        return Err(());
    }
    if buf[20] != 0 {
        return Err(()); // kind-1 document sequences: not accepted yet
    }
    let doc = &buf[21..len];
    if doc.len() < 5 {
        return Err(());
    }
    let dlen = i32::from_le_bytes(doc[0..4].try_into().map_err(|_| ())?) as usize;
    if dlen != doc.len() {
        return Err(());
    }
    Ok(Some((
        Request {
            request_id,
            body: doc,
        },
        len,
    )))
}

/// The command is the FIRST element's name, lower-cased into `out`
/// (commands are case-insensitive at the shell level; `isMaster` and
/// `ismaster` are the same probe). Returns the name length.
pub fn command_name(body: &[u8], out: &mut [u8; 32]) -> Option<usize> {
    if body.len() < 6 {
        return None;
    }
    let mut n = 0usize;
    let mut at = 5; // past doc length + first element's type byte
    while at < body.len() && body[at] != 0 {
        if n >= out.len() {
            return None;
        }
        out[n] = body[at].to_ascii_lowercase();
        n += 1;
        at += 1;
    }
    if n == 0 {
        None
    } else {
        Some(n)
    }
}

// ── Reading BSON ─────────────────────────────────────────────────────

/// Types beyond `mongo_core::bson`'s.
pub const B_OBJECT_ID: u8 = 0x07;
pub const B_DATETIME: u8 = 0x09;
pub const B_NULL: u8 = 0x0A;

/// Why a BSON document could not be read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BsonError {
    /// Lengths or terminators that do not frame.
    Malformed,
    /// A well-formed element of a type this server does not read
    /// (regex, javascript, decimal128, the internal timestamp, minKey,
    /// maxKey, the deprecated types).
    Unsupported,
}

fn rd_i32(b: &[u8], at: usize) -> Option<i32> {
    Some(i32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

fn rd_i64(b: &[u8], at: usize) -> Option<i64> {
    Some(i64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

/// The length of a value of a readable type at the start of `b`.
fn value_len(b: &[u8], ty: u8) -> Result<usize, BsonError> {
    let n = |x: Option<i32>| -> Result<usize, BsonError> {
        usize::try_from(x.ok_or(BsonError::Malformed)?).map_err(|_| BsonError::Malformed)
    };
    Ok(match ty {
        bson::DOUBLE | bson::INT64 | B_DATETIME => 8,
        bson::INT32 => 4,
        bson::BOOL => 1,
        B_NULL => 0,
        B_OBJECT_ID => 12,
        bson::STRING => 4 + n(rd_i32(b, 0))?,
        bson::DOC | bson::ARRAY => n(rd_i32(b, 0))?,
        bson::BINARY => 5 + n(rd_i32(b, 0))?,
        0x06 | 0x0B | 0x0C | 0x0D | 0x0E | 0x0F | 0x11 | 0x13 | 0x7F | 0xFF => {
            return Err(BsonError::Unsupported)
        }
        _ => return Err(BsonError::Malformed),
    })
}

/// The elements of a document (or array): `(name, type, value bytes)`.
pub struct Elements<'a> {
    doc: &'a [u8],
    at: usize,
}

/// Read a document's elements; the document's own framing is checked
/// here, each element's as it is reached.
pub fn elements(doc: &[u8]) -> Result<Elements<'_>, BsonError> {
    let len = usize::try_from(rd_i32(doc, 0).ok_or(BsonError::Malformed)?)
        .map_err(|_| BsonError::Malformed)?;
    if len < 5 || len != doc.len() || doc[len - 1] != 0 {
        return Err(BsonError::Malformed);
    }
    Ok(Elements { doc, at: 4 })
}

impl<'a> Iterator for Elements<'a> {
    type Item = Result<(&'a [u8], u8, &'a [u8]), BsonError>;
    fn next(&mut self) -> Option<Self::Item> {
        let ty = *self.doc.get(self.at)?;
        if ty == 0 {
            return None;
        }
        let ns = self.at + 1;
        let mut e = ns;
        while e < self.doc.len() && self.doc[e] != 0 {
            e += 1;
        }
        // Any failure ends the walk: framing past it is unknowable.
        let end = self.doc.len();
        if e >= end {
            self.at = end;
            return Some(Err(BsonError::Malformed));
        }
        let name = &self.doc[ns..e];
        let vs = e + 1;
        let rest = self.doc.get(vs..).unwrap_or(&[]);
        let vl = match value_len(rest, ty) {
            Ok(n) => n,
            Err(err) => {
                self.at = end;
                return Some(Err(err));
            }
        };
        let Some(v) = rest.get(..vl) else {
            self.at = end;
            return Some(Err(BsonError::Malformed));
        };
        self.at = vs + vl;
        Some(Ok((name, ty, v)))
    }
}

/// A top-level element by name.
pub fn find<'a>(doc: &'a [u8], name: &[u8]) -> Result<Option<(u8, &'a [u8])>, BsonError> {
    for e in elements(doc)? {
        let (n, ty, v) = e?;
        if n == name {
            return Ok(Some((ty, v)));
        }
    }
    Ok(None)
}

/// A string value's text (length-prefixed, NUL-terminated).
pub fn str_value(v: &[u8]) -> Option<&[u8]> {
    let n = usize::try_from(rd_i32(v, 0)?).ok()?;
    if n == 0 || v.len() != 4 + n || v[3 + n] != 0 {
        return None;
    }
    v.get(4..3 + n)
}

/// A number value as an i64: an int32, an int64, or an integral double.
pub fn int_value(ty: u8, v: &[u8]) -> Option<i64> {
    match ty {
        bson::INT32 => rd_i32(v, 0).map(i64::from),
        bson::INT64 => rd_i64(v, 0),
        bson::DOUBLE => {
            let f = f64::from_bits(rd_i64(v, 0)? as u64);
            let i = f as i64;
            (i as f64 == f).then_some(i)
        }
        _ => None,
    }
}

/// MongoDB's truthiness of an option value.
pub fn truthy(ty: u8, v: &[u8]) -> bool {
    match ty {
        bson::BOOL => v.first() == Some(&1),
        bson::INT32 | bson::INT64 | bson::DOUBLE => int_value(ty, v).is_none_or(|i| i != 0),
        B_NULL => false,
        _ => true,
    }
}

/// A top-level string element by name.
pub fn get_str<'a>(doc: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    match find(doc, name).ok()?? {
        (bson::STRING, v) => str_value(v),
        _ => None,
    }
}

/// A top-level document or array element by name: its whole bytes.
pub fn get_doc<'a>(doc: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    match find(doc, name).ok()?? {
        (bson::DOC | bson::ARRAY, v) => Some(v),
        _ => None,
    }
}

// ── Reply building ───────────────────────────────────────────────────

/// Frame one OP_MSG REPLY around a BSON body already built in
/// `body[..body_len]`, writing the complete message into `out`.
/// `response_to` is the request's id.
pub fn frame_reply(response_to: i32, body: &[u8], out: &mut [u8]) -> Option<usize> {
    let total = 16 + 4 + 1 + body.len();
    if out.len() < total {
        return None;
    }
    out[0..4].copy_from_slice(&(total as i32).to_le_bytes());
    out[4..8].copy_from_slice(&0i32.to_le_bytes()); // server request id
    out[8..12].copy_from_slice(&response_to.to_le_bytes());
    out[12..16].copy_from_slice(&OP_MSG.to_le_bytes());
    out[16..20].copy_from_slice(&0u32.to_le_bytes()); // flags
    out[20] = 0; // kind 0
    out[21..21 + body.len()].copy_from_slice(body);
    Some(total)
}

/// `hello` / `isMaster` reply body.
pub fn hello_body(out: &mut [u8]) -> Option<usize> {
    let mut pos = 0usize;
    let start = bson_open(out, &mut pos)?;
    bson_bool(out, &mut pos, b"helloOk", true)?;
    bson_bool(out, &mut pos, b"ismaster", true)?;
    bson_bool(out, &mut pos, b"isWritablePrimary", true)?;
    bson_i32(out, &mut pos, b"maxBsonObjectSize", MAX_BSON as i32)?;
    bson_i32(
        out,
        &mut pos,
        b"maxMessageSizeBytes",
        (MAX_BSON + 64) as i32,
    )?;
    bson_i32(out, &mut pos, b"maxWriteBatchSize", 8)?;
    bson_i32(out, &mut pos, b"minWireVersion", MIN_WIRE_VERSION)?;
    bson_i32(out, &mut pos, b"maxWireVersion", MAX_WIRE_VERSION)?;
    bson_i32(out, &mut pos, b"ok", 1)?;
    bson_close(out, &mut pos, start)
}

/// `{ok: 1}` — `ping` and friends.
pub fn ok_body(out: &mut [u8]) -> Option<usize> {
    let mut pos = 0usize;
    let start = bson_open(out, &mut pos)?;
    bson_i32(out, &mut pos, b"ok", 1)?;
    bson_close(out, &mut pos, start)
}

/// Command error: `{ok: 0, errmsg, code}`.
pub fn error_body(code: i32, msg: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut pos = 0usize;
    let start = bson_open(out, &mut pos)?;
    bson_i32(out, &mut pos, b"ok", 0)?;
    bson_str(out, &mut pos, b"errmsg", msg)?;
    bson_i32(out, &mut pos, b"code", code)?;
    bson_close(out, &mut pos, start)
}

/// Write `[type][name\0]`.
fn element_head(out: &mut [u8], pos: &mut usize, ty: u8, name: &[u8]) -> Option<()> {
    *out.get_mut(*pos)? = ty;
    *pos += 1;
    out.get_mut(*pos..*pos + name.len())?.copy_from_slice(name);
    *pos += name.len();
    *out.get_mut(*pos)? = 0;
    *pos += 1;
    Some(())
}

/// An array element's name: its decimal index.
fn index_name(i: u32, buf: &mut [u8; 10]) -> usize {
    let mut tmp = [0u8; 10];
    let mut v = i;
    let mut k = 0;
    loop {
        tmp[k] = b'0' + (v % 10) as u8;
        v /= 10;
        k += 1;
        if v == 0 {
            break;
        }
    }
    for j in 0..k {
        buf[j] = tmp[k - 1 - j];
    }
    k
}

/// One write error of a write result.
pub struct WriteError<'a> {
    pub index: i32,
    pub code: i32,
    pub msg: &'a [u8],
}

/// A write command's answer: `{n, nModified?, upserted?, writeErrors?,
/// ok: 1}`. `upserted` is the array's elements, already encoded
/// (`{index, _id}` documents named `0`, `1`, …).
pub fn write_result_body(
    n: i32,
    n_modified: Option<i32>,
    upserted: &[u8],
    error: Option<WriteError<'_>>,
    out: &mut [u8],
) -> Option<usize> {
    let mut pos = 0usize;
    let root = bson_open(out, &mut pos)?;
    bson_i32(out, &mut pos, b"n", n)?;
    if let Some(m) = n_modified {
        bson_i32(out, &mut pos, b"nModified", m)?;
    }
    if !upserted.is_empty() {
        element_head(out, &mut pos, bson::ARRAY, b"upserted")?;
        let arr = bson_open(out, &mut pos)?;
        out.get_mut(pos..pos + upserted.len())?
            .copy_from_slice(upserted);
        pos += upserted.len();
        bson_close(out, &mut pos, arr)?;
    }
    if let Some(e) = error {
        element_head(out, &mut pos, bson::ARRAY, b"writeErrors")?;
        let arr = bson_open(out, &mut pos)?;
        element_head(out, &mut pos, bson::DOC, b"0")?;
        let d = bson_open(out, &mut pos)?;
        bson_i32(out, &mut pos, b"index", e.index)?;
        bson_i32(out, &mut pos, b"code", e.code)?;
        bson_str(out, &mut pos, b"errmsg", e.msg)?;
        bson_close(out, &mut pos, d)?;
        bson_close(out, &mut pos, arr)?;
    }
    bson_i32(out, &mut pos, b"ok", 1)?;
    bson_close(out, &mut pos, root)
}

/// One `upserted` element: `{index, _id}` named by its position, the
/// `_id` element written by `write_id` (its type and value).
pub fn upserted_element<F: FnOnce(&mut [u8], &mut usize) -> Option<()>>(
    position: u32,
    index: i32,
    write_id: F,
    out: &mut [u8],
    pos: &mut usize,
) -> Option<()> {
    let mut nb = [0u8; 10];
    let nl = index_name(position, &mut nb);
    element_head(out, pos, bson::DOC, &nb[..nl])?;
    let d = bson_open(out, pos)?;
    bson_i32(out, pos, b"index", index)?;
    write_id(out, pos)?;
    bson_close(out, pos, d).map(|_| ())
}

/// `killCursors`' answer: `{cursorsKilled: [ids], ok: 1}`.
pub fn kill_cursors_body(killed: &[i64], out: &mut [u8]) -> Option<usize> {
    let mut pos = 0usize;
    let root = bson_open(out, &mut pos)?;
    element_head(out, &mut pos, bson::ARRAY, b"cursorsKilled")?;
    let arr = bson_open(out, &mut pos)?;
    for (i, id) in killed.iter().enumerate() {
        let mut nb = [0u8; 10];
        let nl = index_name(i as u32, &mut nb);
        element_head(out, &mut pos, bson::INT64, &nb[..nl])?;
        out.get_mut(pos..pos + 8)?
            .copy_from_slice(&id.to_le_bytes());
        pos += 8;
    }
    bson_close(out, &mut pos, arr)?;
    bson_i32(out, &mut pos, b"ok", 1)?;
    bson_close(out, &mut pos, root)
}

/// Incremental cursor reply builder: `{cursor: {firstBatch|nextBatch:
/// [...], id, ns}, ok: 1}`. Documents append between `new` and `finish`.
pub struct FindReply<'a> {
    out: &'a mut [u8],
    pos: usize,
    root: usize,
    cursor: usize,
    batch: usize,
    n: u32,
}

impl<'a> FindReply<'a> {
    /// A first batch (`find`), or a next batch (`getMore`).
    pub fn new(out: &'a mut [u8], next_batch: bool) -> Option<Self> {
        let mut pos = 0usize;
        let root = bson_open(out, &mut pos)?;
        element_head(out, &mut pos, bson::DOC, b"cursor")?;
        let cursor = bson_open(out, &mut pos)?;
        // Each branch writes its own literal: PIC relocates no table of
        // slices.
        if next_batch {
            element_head(out, &mut pos, bson::ARRAY, b"nextBatch")?;
        } else {
            element_head(out, &mut pos, bson::ARRAY, b"firstBatch")?;
        }
        let batch = bson_open(out, &mut pos)?;
        Some(Self {
            out,
            pos,
            root,
            cursor,
            batch,
            n: 0,
        })
    }

    /// Append one raw BSON document to the batch.
    pub fn push(&mut self, doc: &[u8]) -> Option<()> {
        self.push_with(|out, pos| {
            out.get_mut(*pos..*pos + doc.len())?.copy_from_slice(doc);
            *pos += doc.len();
            Some(())
        })
    }

    /// Append one document the caller writes (`write` puts a whole BSON
    /// document at `pos`). Nothing is kept of a document that does not
    /// fit.
    pub fn push_with<F: FnOnce(&mut [u8], &mut usize) -> Option<()>>(
        &mut self,
        write: F,
    ) -> Option<()> {
        let mark = self.pos;
        let mut nb = [0u8; 10];
        let nl = index_name(self.n, &mut nb);
        let r = element_head(self.out, &mut self.pos, bson::DOC, &nb[..nl])
            .and_then(|_| write(self.out, &mut self.pos));
        if r.is_none() {
            self.pos = mark;
            return None;
        }
        self.n += 1;
        Some(())
    }

    /// Documents in the batch.
    pub fn len(&self) -> u32 {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Close the batch with the cursor's id (0: exhausted) and namespace;
    /// the body's length.
    pub fn finish(mut self, ns: &[u8], cursor_id: i64) -> Option<usize> {
        let batch = self.batch;
        bson_close(self.out, &mut self.pos, batch)?;
        element_head(self.out, &mut self.pos, bson::INT64, b"id")?;
        self.out
            .get_mut(self.pos..self.pos + 8)?
            .copy_from_slice(&cursor_id.to_le_bytes());
        self.pos += 8;
        bson_str(self.out, &mut self.pos, b"ns", ns)?;
        let cursor = self.cursor;
        bson_close(self.out, &mut self.pos, cursor)?;
        bson_i32(self.out, &mut self.pos, b"ok", 1)?;
        let root = self.root;
        bson_close(self.out, &mut self.pos, root)
    }
}
