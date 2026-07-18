//! Server-side MongoDB wire framing for the document capability
//! (RFC database foundation §14.16, Phase 8).
//!
//! The document capability's first connector speaks the MongoDB wire
//! protocol — OP_MSG (opcode 2013) carrying BSON command documents —
//! because that is what real document clients speak, and this
//! engagement proves capabilities against real clients. This file owns
//! the CONNECTOR half per the §14.3 split the SQL connectors follow:
//! framing, the handshake surface, reply encoding, error mapping. It
//! does NOT own key composition (`models.rs`), value semantics, or
//! anything a second document connector would have to agree on.
//!
//! BSON primitives come from `mongo_core.rs` — the same writer/reader
//! the OUTBOUND mongo client uses, mounted through one chain so the
//! two sides can never disagree about an encoding. This file adds only
//! what a SERVER needs and the client side never did: embedded
//! document/array extraction (an `insert`'s `documents` array), and
//! the reply shapes (`hello`, cursor batches, command errors).
//!
//! ## The `_id` rule
//!
//! A document's identity key component is the RAW BSON element value —
//! type byte + value bytes, name stripped ([`extract_id`]). Exact and
//! type-faithful: an `_id: 5` (int32) and an `_id: "5"` (string) are
//! DIFFERENT documents, exactly as MongoDB treats them, because the
//! type byte participates in the bytes. Documents without `_id` are
//! refused — the server does not mint identities (drivers already
//! generate ObjectIds client-side, and a server-minted id would make
//! retry idempotency unknowable).

#![allow(
    dead_code,
    reason = "shared via #[path] into the anchor module and host tests; each consumer uses a subset"
)]

// One mount chain: the BSON primitives are the client connector's, so
// the two sides share one encoding by construction.
#[path = "mongo_core.rs"]
pub mod mongo_core;

#[path = "models.rs"]
pub mod models;

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

// ── BSON extraction the server needs ─────────────────────────────────

/// Byte length of one element's VALUE at `at` given its type.
fn value_len(doc: &[u8], ty: u8, at: usize) -> Option<usize> {
    Some(match ty {
        bson::DOUBLE | bson::INT64 | 0x11 /* timestamp */ | 0x09 /* datetime */ => 8,
        bson::INT32 => 4,
        bson::BOOL => 1,
        0x0A /* null */ => 0,
        0x07 /* ObjectId */ => 12,
        bson::STRING | 0x0D /* JS code */ => {
            4 + i32::from_le_bytes(doc.get(at..at + 4)?.try_into().ok()?) as usize
        }
        bson::DOC | bson::ARRAY => {
            i32::from_le_bytes(doc.get(at..at + 4)?.try_into().ok()?) as usize
        }
        bson::BINARY => {
            5 + i32::from_le_bytes(doc.get(at..at + 4)?.try_into().ok()?) as usize
        }
        _ => return None, // decimal128, regex, …: refuse, never skip blind
    })
}

/// Walk `doc`'s elements, calling `f(name, type, value_range)`.
/// Returns `None` on any malformed element — fail closed.
fn walk<'a, F: FnMut(&'a [u8], u8, core::ops::Range<usize>) -> bool>(
    doc: &'a [u8],
    mut f: F,
) -> Option<()> {
    if doc.len() < 5 {
        return None;
    }
    let mut at = 4usize;
    while at < doc.len() - 1 {
        let ty = doc[at];
        if ty == 0 {
            break;
        }
        at += 1;
        let name_start = at;
        while at < doc.len() && doc[at] != 0 {
            at += 1;
        }
        let name = doc.get(name_start..at)?;
        at += 1;
        let vlen = value_len(doc, ty, at)?;
        let range = at..at.checked_add(vlen)?;
        if range.end > doc.len() {
            return None;
        }
        if !f(name, ty, range.clone()) {
            return Some(());
        }
        at = range.end;
    }
    Some(())
}

/// The RAW identity bytes of a document: `[type][value…]` of its `_id`
/// element. `None` when `_id` is absent or of an unsupported type.
pub fn extract_id(doc: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut found: Option<usize> = None;
    walk(doc, |name, ty, range| {
        if name == b"_id" {
            let vlen = range.len();
            if vlen < out.len() {
                out[0] = ty;
                // Byte loop: slice copy_from_slice pulls the panicking
                // len-mismatch symbol into the PIC link.
                for (i, b) in doc[range].iter().enumerate() {
                    out[1 + i] = *b;
                }
                found = Some(1 + vlen);
            }
            return false;
        }
        true
    })?;
    found
}

/// A numeric BSON value as `f64`, for cross-type numeric comparison.
fn num_of(ty: u8, v: &[u8]) -> Option<f64> {
    match ty {
        bson::INT32 => Some(i32::from_le_bytes(v.get(0..4)?.try_into().ok()?) as f64),
        bson::INT64 => Some(i64::from_le_bytes(v.get(0..8)?.try_into().ok()?) as f64),
        bson::DOUBLE => Some(f64::from_le_bytes(v.get(0..8)?.try_into().ok()?)),
        _ => None,
    }
}

/// The payload bytes of a BSON string (its `[len][bytes][NUL]`), stripped
/// to the content.
fn str_of(v: &[u8]) -> Option<&[u8]> {
    let slen = i32::from_le_bytes(v.get(0..4)?.try_into().ok()?) as usize;
    if slen == 0 || 4 + slen > v.len() {
        return None;
    }
    v.get(4..4 + slen - 1)
}

/// Are two BSON values equal? Numbers compare across int/long/double;
/// everything else is same-type byte equality.
fn values_equal(dty: u8, dval: &[u8], fty: u8, fval: &[u8]) -> bool {
    if let (Some(a), Some(b)) = (num_of(dty, dval), num_of(fty, fval)) {
        return a == b;
    }
    dty == fty && dval == fval
}

/// Order two BSON values — numeric or string — for range operators.
fn values_cmp(dty: u8, dval: &[u8], fty: u8, fval: &[u8]) -> Option<core::cmp::Ordering> {
    if let (Some(a), Some(b)) = (num_of(dty, dval), num_of(fty, fval)) {
        return a.partial_cmp(&b);
    }
    if dty == bson::STRING && fty == bson::STRING {
        return Some(str_of(dval)?.cmp(str_of(fval)?));
    }
    None
}

/// Does a document field satisfy an operator sub-document like
/// `{$gt: 30, $lte: 50}`? Every operator must hold (their conjunction).
fn matches_ops(dty: u8, dval: &[u8], opdoc: &[u8]) -> bool {
    use core::cmp::Ordering::{Equal, Greater, Less};
    let mut ok = true;
    let _ = walk(opdoc, |op, oty, orange| {
        let oval = &opdoc[orange.clone()];
        let pass = match op {
            b"$eq" => values_equal(dty, dval, oty, oval),
            b"$ne" => !values_equal(dty, dval, oty, oval),
            b"$gt" => values_cmp(dty, dval, oty, oval) == Some(Greater),
            b"$gte" => matches!(values_cmp(dty, dval, oty, oval), Some(Greater | Equal)),
            b"$lt" => values_cmp(dty, dval, oty, oval) == Some(Less),
            b"$lte" => matches!(values_cmp(dty, dval, oty, oval), Some(Less | Equal)),
            // An operator this slice does not implement must not silently
            // pass; `filter_is_supported` rejects it upstream, so reaching
            // here is a fail-closed guard.
            _ => false,
        };
        if !pass {
            ok = false;
            return false;
        }
        true
    });
    ok
}

/// Does `doc` satisfy every field of `filter` (their conjunction)? A field
/// value is either a literal (equality) or an operator sub-document. A
/// field the document lacks fails the filter.
pub fn doc_matches_filter(doc: &[u8], filter: &[u8]) -> bool {
    let mut ok = true;
    let _ = walk(filter, |name, fty, frange| {
        let fval = &filter[frange.clone()];
        let Some((dty, drange)) = find_element(doc, name) else {
            ok = false;
            return false;
        };
        let dval = &doc[drange];
        let pass = if fty == bson::DOC {
            matches_ops(dty, dval, fval)
        } else {
            values_equal(dty, dval, fty, fval)
        };
        if !pass {
            ok = false;
            return false;
        }
        true
    });
    ok
}

/// Append a numeric element preserving the ORIGINAL field's type, for
/// `$inc` (an increment keeps int fields int, doubles double).
fn append_number(out: &mut [u8], pos: &mut usize, ty: u8, name: &[u8], v: f64) -> Option<()> {
    match ty {
        bson::INT32 => mongo_core::bson_append_raw(out, pos, ty, name, &(v as i32).to_le_bytes()),
        bson::INT64 => mongo_core::bson_append_raw(out, pos, ty, name, &(v as i64).to_le_bytes()),
        bson::DOUBLE => mongo_core::bson_append_raw(out, pos, ty, name, &v.to_le_bytes()),
        _ => None,
    }
}

/// Build the updated document from `orig` under `$set` / `$inc` operator
/// sub-documents. `$set` replaces or adds a field; `$inc` adds to a numeric
/// field (creating it from zero if absent). `_id` is preserved because it
/// is copied like any other field and update filters never name it in an
/// operator. Returns the new document's length in `out`.
pub fn apply_update_ops(
    orig: &[u8],
    set_doc: Option<&[u8]>,
    inc_doc: Option<&[u8]>,
    out: &mut [u8],
) -> Option<usize> {
    let mut pos = 0usize;
    let start = mongo_core::bson_open(out, &mut pos)?;
    let mut fail = false;
    // Existing fields, modified in place.
    let _ = walk(orig, |name, ty, range| {
        let oval = &orig[range.clone()];
        if let Some(id) = inc_doc {
            if let Some((ity, ir)) = find_element(id, name) {
                let base = num_of(ty, oval).unwrap_or(0.0);
                let delta = num_of(ity, &id[ir]).unwrap_or(0.0);
                if append_number(out, &mut pos, ty, name, base + delta).is_none() {
                    fail = true;
                    return false;
                }
                return true;
            }
        }
        if let Some(sd) = set_doc {
            if let Some((sty, sr)) = find_element(sd, name) {
                if mongo_core::bson_append_raw(out, &mut pos, sty, name, &sd[sr]).is_none() {
                    fail = true;
                    return false;
                }
                return true;
            }
        }
        if mongo_core::bson_append_raw(out, &mut pos, ty, name, oval).is_none() {
            fail = true;
            return false;
        }
        true
    });
    if fail {
        return None;
    }
    // `$set` fields the document did not already have.
    if let Some(sd) = set_doc {
        let _ = walk(sd, |name, sty, sr| {
            if find_element(orig, name).is_none()
                && mongo_core::bson_append_raw(out, &mut pos, sty, name, &sd[sr]).is_none()
            {
                fail = true;
                return false;
            }
            true
        });
    }
    // `$inc` fields the document did not already have (increment from 0).
    if let Some(id) = inc_doc {
        let _ = walk(id, |name, ity, ir| {
            if find_element(orig, name).is_none() {
                let v = num_of(ity, &id[ir]).unwrap_or(0.0);
                if append_number(out, &mut pos, ity, name, v).is_none() {
                    fail = true;
                    return false;
                }
            }
            true
        });
    }
    if fail {
        return None;
    }
    mongo_core::bson_close(out, &mut pos, start)
}

/// Is a query filter one this slice can evaluate? Each field is a literal
/// (equality) or an operator doc using only supported operators. `_id`
/// filters take the point-read path and are checked elsewhere.
pub fn filter_is_supported(filter: &[u8]) -> bool {
    let mut ok = true;
    let _ = walk(filter, |_name, fty, frange| {
        if fty == bson::DOC {
            let sub = &filter[frange.clone()];
            let mut sub_ok = true;
            let _ = walk(sub, |op, _oty, _r| {
                if !matches!(op, b"$eq" | b"$ne" | b"$gt" | b"$gte" | b"$lt" | b"$lte") {
                    sub_ok = false;
                    return false;
                }
                true
            });
            if !sub_ok {
                ok = false;
                return false;
            }
        }
        true
    });
    ok
}

/// The value range of a top-level element by name, with its type.
pub fn find_element(doc: &[u8], target: &[u8]) -> Option<(u8, core::ops::Range<usize>)> {
    let mut found: Option<(u8, core::ops::Range<usize>)> = None;
    walk(doc, |name, ty, range| {
        if name == target {
            found = Some((ty, range));
            return false;
        }
        true
    })?;
    found
}

/// UTF-8 string element by name (BSON strings carry a length + NUL).
pub fn get_str<'a>(doc: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    let (ty, range) = find_element(doc, name)?;
    if ty != bson::STRING {
        return None;
    }
    let v = &doc[range];
    let slen = i32::from_le_bytes(v.get(0..4)?.try_into().ok()?) as usize;
    if slen == 0 || 4 + slen > v.len() {
        return None;
    }
    v.get(4..4 + slen - 1) // strip the trailing NUL
}

/// Embedded document/array element by name: the full `[len][...]` doc
/// bytes.
pub fn get_doc<'a>(doc: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    let (ty, range) = find_element(doc, name)?;
    if ty != bson::DOC && ty != bson::ARRAY {
        return None;
    }
    Some(&doc[range])
}

/// Iterate an ARRAY's element values (documents, for `insert`'s
/// `documents`), calling `f(element_doc)`. Non-document elements fail
/// closed.
pub fn each_array_doc<F: FnMut(&[u8]) -> bool>(arr: &[u8], mut f: F) -> Option<()> {
    walk(arr, |_name, ty, range| {
        if ty != bson::DOC {
            return false;
        }
        f(&arr[range.clone()])
    })
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

/// `insert` acknowledgement: `{n: <count>, ok: 1}`.
pub fn insert_body(n: i32, out: &mut [u8]) -> Option<usize> {
    let mut pos = 0usize;
    let start = bson_open(out, &mut pos)?;
    bson_i32(out, &mut pos, b"n", n)?;
    bson_i32(out, &mut pos, b"ok", 1)?;
    bson_close(out, &mut pos, start)
}

/// `delete` acknowledgement: `{n: <deleted>, ok: 1}`.
pub fn delete_body(n: i32, out: &mut [u8]) -> Option<usize> {
    let mut pos = 0usize;
    let start = bson_open(out, &mut pos)?;
    bson_i32(out, &mut pos, b"n", n)?;
    bson_i32(out, &mut pos, b"ok", 1)?;
    bson_close(out, &mut pos, start)
}

/// `update` acknowledgement: `{n: <matched>, nModified: <modified>, ok: 1}`.
pub fn update_body(n: i32, n_modified: i32, out: &mut [u8]) -> Option<usize> {
    let mut pos = 0usize;
    let start = bson_open(out, &mut pos)?;
    bson_i32(out, &mut pos, b"n", n)?;
    bson_i32(out, &mut pos, b"nModified", n_modified)?;
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

/// Incremental `find` reply builder: `{cursor: {firstBatch: [...],
/// id: 0, ns: "<db.coll>"}, ok: 1}`. Documents append between `new`
/// and `finish`; cursor id is always 0 — every result fits one batch
/// or the find refuses upstream (bounded, like the SQL staging buffer).
pub struct FindReply<'a> {
    out: &'a mut [u8],
    pos: usize,
    root: usize,
    cursor: usize,
    batch: usize,
    n: u32,
}

impl<'a> FindReply<'a> {
    pub fn new(out: &'a mut [u8]) -> Option<Self> {
        let mut pos = 0usize;
        let root = bson_open(out, &mut pos)?;
        // cursor: { firstBatch: [ ...
        if pos + 8 > out.len() {
            return None;
        }
        out[pos] = bson::DOC;
        pos += 1;
        out[pos..pos + 7].copy_from_slice(b"cursor\0");
        pos += 7;
        let cursor = bson_open(out, &mut pos)?;
        if pos + 12 > out.len() {
            return None;
        }
        out[pos] = bson::ARRAY;
        pos += 1;
        out[pos..pos + 11].copy_from_slice(b"firstBatch\0");
        pos += 11;
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

    /// Append one raw BSON document to the batch. Array keys are the
    /// decimal element index, as BSON arrays require.
    pub fn push(&mut self, doc: &[u8]) -> Option<()> {
        let mut namebuf = [0u8; 10];
        let mut n = 0usize;
        let mut v = self.n;
        if v == 0 {
            namebuf[0] = b'0';
            n = 1;
        } else {
            let mut digits = [0u8; 10];
            let mut d = 0;
            while v > 0 {
                digits[d] = b'0' + (v % 10) as u8;
                v /= 10;
                d += 1;
            }
            while d > 0 {
                d -= 1;
                namebuf[n] = digits[d];
                n += 1;
            }
        }
        let need = 1 + n + 1 + doc.len();
        if self.pos + need > self.out.len() {
            return None;
        }
        self.out[self.pos] = bson::DOC;
        self.pos += 1;
        self.out[self.pos..self.pos + n].copy_from_slice(&namebuf[..n]);
        self.pos += n;
        self.out[self.pos] = 0;
        self.pos += 1;
        self.out[self.pos..self.pos + doc.len()].copy_from_slice(doc);
        self.pos += doc.len();
        self.n += 1;
        Some(())
    }

    /// Close the arrays/documents and return the body length.
    pub fn finish(mut self, ns: &[u8]) -> Option<usize> {
        let batch = self.batch;
        bson_close(self.out, &mut self.pos, batch)?;
        // id: int64 0, ns: string
        if self.pos + 12 > self.out.len() {
            return None;
        }
        self.out[self.pos] = bson::INT64;
        self.pos += 1;
        self.out[self.pos..self.pos + 3].copy_from_slice(b"id\0");
        self.pos += 3;
        self.out[self.pos..self.pos + 8].copy_from_slice(&0i64.to_le_bytes());
        self.pos += 8;
        bson_str(self.out, &mut self.pos, b"ns", ns)?;
        let cursor = self.cursor;
        bson_close(self.out, &mut self.pos, cursor)?;
        bson_i32(self.out, &mut self.pos, b"ok", 1)?;
        let root = self.root;
        bson_close(self.out, &mut self.pos, root)
    }
}
