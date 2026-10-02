//! Large values at a module's KV edge: the layer between a module that
//! reads and writes whole values and a KV path whose writes and frames
//! are one replicated entry.
//!
//! A module builds its requests and parses its replies as it always did.
//! Through this layer:
//!
//! - an outgoing `KV_OP_PUT`, or a `KV_OP_TXN` whose `PUT`s carry a value
//!   larger than one data chunk (`kv_chunks::DATA_MAX`), is held: each such
//!   value's chunks are uploaded (`KV_OP_PUT_CHUNK`, one bounded request
//!   each), then the request goes out rewritten — each large `PUT` a
//!   `KV_OP_PUT_MANIFEST` of its value's manifest — compared exactly as
//!   the module wrote it;
//! - a `KV_OP_TXN_STAGED` — a transaction as its writer stages it, each
//!   branch op framed with a `u32` so a `PUT` may carry a whole record —
//!   is always held, its large values uploaded as above, and goes out the
//!   `KV_OP_TXN` it stands for;
//! - an outgoing `KV_OP_RANGE_META` or `KV_OP_SCAN_VERSIONS` is marked as
//!   reading manifests, and its reply is EXPANDED before the module sees
//!   it: each manifest entry's value fetched (`KV_OP_GET_CHUNK`, one
//!   chunk per request) and put in its place, the flag bytes stripped —
//!   the reply the module asked for, as if the value were inline. A page
//!   whose expansion outgrows the module's buffer is cut short and marked
//!   partial (the module pages on); a `KV_RESULT_MANIFEST` answer to a
//!   `KV_OP_GET` becomes the value, `KV_RESULT_OK`.
//!
//! The module supplies the buffers (its policy sizes them) and drives the
//! exchanges with its own send and receive; this file holds no I/O.
//! Mounted beside `types.rs` (reached as `super::types`).

#[path = "kv_chunks.rs"]
pub mod kv_chunks;

use super::types::{
    DELETE_META_FLAG_MANIFESTS, ETCD_OP_DELETE, ETCD_OP_PUT, ETCD_OP_PUT_MANIFEST, ETCD_OP_RANGE,
    ETCD_OP_TXN, ETCD_TXN_DEPTH_MAX, KVS_MORE_PARTIAL, KV_OP_DELETE_META, KV_OP_ETCD_TXN,
    KV_OP_GET, KV_OP_GET_CHUNK, KV_OP_PUT, KV_OP_PUT_CHUNK, KV_OP_PUT_MANIFEST, KV_OP_RANGE_META,
    KV_OP_SCAN_VERSIONS, KV_OP_TXN, KV_OP_TXN_STAGED, KV_RESULT_ETCD_TXN, KV_RESULT_KVS,
    KV_RESULT_MANIFEST, KV_RESULT_OK, KV_RESULT_TOO_LARGE, KV_RESULT_VERSIONS, PUT_FLAG2_MANIFESTS,
    PUT_FLAG_GET, PUT_FLAG_LEASE, PUT_FLAG_PREV_META, RANGE_META_FLAG_MANIFESTS,
    RECORD_FLAG_MANIFEST, VERSIONS_FLAG_MANIFESTS, VERSIONS_FLAG_META, VERSION_KIND_PUT,
};
use super::types::{
    KV_ARRAY_ELEMENT_NULL, KV_OP_MGET, KV_OP_MSET, KV_RESULT_ARRAY, MGET_FLAG_MANIFESTS,
};
use kv_chunks::{
    Builder, Hash, Manifest, Reader, Step, CHUNK_MAX, DATA_MAX, HASH_LEN, MANIFEST_LEN,
};

/// Large values one request may carry (a TXN's `PUT`s).
pub const MAX_LARGE_PER_REQUEST: usize = 64;

/// A `KV_OP_PUT_CHUNK` body: hash, length, chunk.
pub const CHUNK_REQUEST_MAX: usize = HASH_LEN + 2 + CHUNK_MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LargeError {
    /// A request or reply that is not the shape its op says.
    Malformed,
    /// A buffer is full, or a request holds more large values than
    /// [`MAX_LARGE_PER_REQUEST`].
    Overflow,
    /// A chunk that does not read back as the manifest names it.
    BadChunk,
    /// The answer holds a large value, to be fetched (see [`unmark`]).
    Large,
}

fn rd_u16(b: &[u8], at: usize) -> Option<usize> {
    Some(u16::from_le_bytes([*b.get(at)?, *b.get(at + 1)?]) as usize)
}

fn rd_u32(b: &[u8], at: usize) -> Option<usize> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?) as usize)
}

fn rd_u64(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

fn put(out: &mut [u8], at: &mut usize, b: &[u8]) -> Result<(), LargeError> {
    let d = out
        .get_mut(*at..*at + b.len())
        .ok_or(LargeError::Overflow)?;
    for (x, y) in d.iter_mut().zip(b) {
        *x = *y;
    }
    *at += b.len();
    Ok(())
}

/// `b[from..to]`, or malformed.
fn sl(b: &[u8], from: usize, to: usize) -> Result<&[u8], LargeError> {
    b.get(from..to).ok_or(LargeError::Malformed)
}

/// Write `b` at `out[at..]` (a length laid out after what it measures).
fn put_at(out: &mut [u8], at: usize, b: &[u8]) -> Result<(), LargeError> {
    put(out, &mut { at }, b)
}

/// Set `bits` in `out[at]`.
fn or_at(out: &mut [u8], at: usize, bits: u8) -> Result<(), LargeError> {
    let x = out.get_mut(at).ok_or(LargeError::Malformed)?;
    *x |= bits;
    Ok(())
}

// ── Writes ────────────────────────────────────────────────────────────

/// A `PUT` body's value: `(offset, length)` in the body.
fn put_value(body: &[u8]) -> Option<(usize, usize)> {
    let kl = rd_u16(body, 0)?;
    let vl = rd_u32(body, 2 + kl)?;
    let off = 2 + kl + 4;
    body.get(off..off + vl)?;
    Some((off, vl))
}

/// Each value of an MSET body: `(offset, length)`. `None` when malformed.
fn mset_values(body: &[u8], mut f: impl FnMut(usize, usize) -> bool) -> Option<usize> {
    let n = rd_u16(body, 0)?;
    let mut at = 2usize;
    for _ in 0..n {
        let kl = rd_u16(body, at)?;
        at += 2 + kl;
        let vl = rd_u32(body, at)?;
        body.get(at + 4..at + 4 + vl)?;
        if !f(at + 4, vl) {
            return None;
        }
        at += 4 + vl;
    }
    Some(at)
}

/// A TXN body's branch op at `at`: `(op, header length, body length)` —
/// its length a `u16` on the wire, a `u32` staged (`KV_OP_TXN_STAGED`).
fn txn_op_at(body: &[u8], at: usize, staged: bool) -> Option<(u8, usize, usize)> {
    let op = *body.get(at)?;
    if staged {
        Some((op, 5, rd_u32(body, at + 1)?))
    } else {
        Some((op, 3, rd_u16(body, at + 1)?))
    }
}

/// The `PUT` sub-ops of a TXN body (`staged`: a `KV_OP_TXN_STAGED`):
/// `(op offset, body offset, body len)` each, both branches.
fn txn_puts(
    body: &[u8],
    staged: bool,
    mut f: impl FnMut(usize, usize, usize) -> bool,
) -> Option<()> {
    let n = rd_u16(body, 0)?;
    let mut at = 2usize;
    for _ in 0..n {
        let kl = rd_u16(body, at + 1)?;
        at += 1 + 2 + kl + 8;
    }
    for _branch in 0..2 {
        let n = rd_u16(body, at)?;
        at += 2;
        for _ in 0..n {
            let (op, h, bl) = txn_op_at(body, at, staged)?;
            body.get(at + h..at + h + bl)?;
            if op == KV_OP_PUT && !f(at, at + h, bl) {
                return None;
            }
            at += h + bl;
        }
    }
    Some(())
}

/// Uploads a request's large values, then rewrites it.
pub struct Uploader {
    op: u8,
    /// Each large value: its offset and length in the held request.
    values: [(u32, u32); MAX_LARGE_PER_REQUEST],
    count: usize,
    /// The value being cut, and how much of it the builder has taken.
    current: usize,
    fed: usize,
    builder: Option<Builder>,
    manifests: [Manifest; MAX_LARGE_PER_REQUEST],
    /// Values longer than this go as chunks ([`cut_for`]).
    cut: usize,
}

const NO_MANIFEST: Manifest = Manifest {
    total: 0,
    depth: 0,
    root: [0; HASH_LEN],
};

impl Uploader {
    pub const fn new() -> Self {
        Uploader {
            op: 0,
            values: [(0, 0); MAX_LARGE_PER_REQUEST],
            count: 0,
            current: 0,
            fed: 0,
            builder: None,
            manifests: [NO_MANIFEST; MAX_LARGE_PER_REQUEST],
            cut: DATA_MAX,
        }
    }

    /// Look at an outgoing request (`op`, `body`, which the caller holds
    /// unchanged until [`Uploader::rewrite`]): does it carry values that
    /// go as chunks — every one past a data chunk, and, when the request
    /// would still not fit `room` bytes (one replicated entry's command,
    /// [`ENTRY_BODY_MAX`]), its largest others until it does?
    /// `Ok(false)`: send it as it is. A `KV_OP_TXN_STAGED` is always
    /// `Ok(true)`: it goes out only rewritten.
    pub fn start(&mut self, op: u8, body: &[u8], room: usize) -> Result<bool, LargeError> {
        self.op = op;
        self.count = 0;
        self.current = 0;
        self.fed = 0;
        self.builder = None;
        self.cut = cut_for(op, body, room)?;
        let (cut, count, values) = (self.cut, &mut self.count, &mut self.values);
        each_value(op, body, &mut |off, len| {
            if len > cut {
                if *count == MAX_LARGE_PER_REQUEST {
                    return false;
                }
                values[*count] = (off as u32, len as u32);
                *count += 1;
            }
            true
        })?;
        Ok(self.count > 0 || op == KV_OP_TXN_STAGED)
    }

    /// The next `KV_OP_PUT_CHUNK` body into `out`: its length, `None` when
    /// every chunk is uploaded. `body` is the held request.
    pub fn next_chunk(
        &mut self,
        body: &[u8],
        out: &mut [u8; CHUNK_REQUEST_MAX],
    ) -> Result<Option<usize>, LargeError> {
        let mut chunk = [0u8; CHUNK_MAX];
        loop {
            if self.current == self.count {
                return Ok(None);
            }
            let (off, len) = self.values[self.current];
            let (off, len) = (off as usize, len as usize);
            if self.builder.is_none() {
                self.builder = Some(Builder::new(len as u64).ok_or(LargeError::Malformed)?);
                self.fed = 0;
            }
            let b = self.builder.as_mut().ok_or(LargeError::Malformed)?;
            if let Some((n, h)) = b.next(&mut chunk) {
                out[..HASH_LEN].copy_from_slice(&h);
                out[HASH_LEN..HASH_LEN + 2].copy_from_slice(&(n as u16).to_le_bytes());
                out[HASH_LEN + 2..HASH_LEN + 2 + n].copy_from_slice(&chunk[..n]);
                return Ok(Some(HASH_LEN + 2 + n));
            }
            if self.fed < len {
                let take = b.room().min(len - self.fed);
                let v = body
                    .get(off + self.fed..off + self.fed + take)
                    .ok_or(LargeError::Malformed)?;
                b.feed(v).ok_or(LargeError::Malformed)?;
                self.fed += take;
                continue;
            }
            // This value is whole: its manifest; on to the next.
            self.manifests[self.current] = b.manifest().ok_or(LargeError::Malformed)?;
            self.builder = None;
            self.current += 1;
        }
    }

    /// The request to send once every chunk is stored: `(op, length)`
    /// written into `out` — each large `PUT` a `KV_OP_PUT_MANIFEST` of its
    /// value's manifest.
    pub fn rewrite(&self, body: &[u8], out: &mut [u8]) -> Result<(u8, usize), LargeError> {
        let mut at = 0usize;
        match self.op {
            KV_OP_PUT => {
                let m = self.manifests[0].encode();
                rewrite_put(body, &m, out, &mut at)?;
                Ok((KV_OP_PUT_MANIFEST, at))
            }
            KV_OP_TXN | KV_OP_TXN_STAGED => {
                // Copy the compares whole; rewrite the branches op by op,
                // each framed as the wire frames it.
                let staged = self.op == KV_OP_TXN_STAGED;
                let n = rd_u16(body, 0).ok_or(LargeError::Malformed)?;
                let mut p = 2usize;
                for _ in 0..n {
                    let kl = rd_u16(body, p + 1).ok_or(LargeError::Malformed)?;
                    p += 1 + 2 + kl + 8;
                }
                put(out, &mut at, body.get(..p).ok_or(LargeError::Malformed)?)?;
                let mut large = 0usize;
                for _branch in 0..2 {
                    let n = rd_u16(body, p).ok_or(LargeError::Malformed)?;
                    put(out, &mut at, &(n as u16).to_le_bytes())?;
                    p += 2;
                    for _ in 0..n {
                        let (op, h, bl) =
                            txn_op_at(body, p, staged).ok_or(LargeError::Malformed)?;
                        let sub = body.get(p + h..p + h + bl).ok_or(LargeError::Malformed)?;
                        let is_large = op == KV_OP_PUT
                            && put_value(sub).is_some_and(|(_, len)| len > self.cut);
                        if is_large {
                            let m = self.manifests[large].encode();
                            large += 1;
                            let head = at;
                            put(out, &mut at, &[KV_OP_PUT_MANIFEST, 0, 0])?;
                            let start = at;
                            rewrite_put(sub, &m, out, &mut at)?;
                            let len =
                                u16::try_from(at - start).map_err(|_| LargeError::Overflow)?;
                            out[head + 1..head + 3].copy_from_slice(&len.to_le_bytes());
                        } else {
                            let len = u16::try_from(bl).map_err(|_| LargeError::Overflow)?;
                            put(out, &mut at, &[op])?;
                            put(out, &mut at, &len.to_le_bytes())?;
                            put(out, &mut at, sub)?;
                        }
                        p += h + bl;
                    }
                }
                put(out, &mut at, body.get(p..).unwrap_or(&[]))?;
                Ok((KV_OP_TXN, at))
            }
            KV_OP_ETCD_TXN => {
                let mut large = 0usize;
                self.rewrite_etcd_txn(body, out, &mut at, &mut large, 0)?;
                Ok((KV_OP_ETCD_TXN, at))
            }
            KV_OP_MSET => {
                // The pairs, each large value its manifest; then the marks.
                let n = rd_u16(body, 0).ok_or(LargeError::Malformed)?;
                put(out, &mut at, sl(body, 0, 2)?)?;
                let mut p = 2usize;
                let mut large = 0usize;
                for _ in 0..n {
                    let kl = rd_u16(body, p).ok_or(LargeError::Malformed)?;
                    put(out, &mut at, sl(body, p, p + 2 + kl)?)?;
                    p += 2 + kl;
                    let vl = rd_u32(body, p).ok_or(LargeError::Malformed)?;
                    if vl > self.cut {
                        let m = self
                            .manifests
                            .get(large)
                            .ok_or(LargeError::Malformed)?
                            .encode();
                        large += 1;
                        put(out, &mut at, &(m.len() as u32).to_le_bytes())?;
                        put(out, &mut at, &m)?;
                    } else {
                        put(out, &mut at, sl(body, p, p + 4 + vl)?)?;
                    }
                    p += 4 + vl;
                }
                let mut q = 2usize;
                for _ in 0..n {
                    q += 2 + rd_u16(body, q).ok_or(LargeError::Malformed)?;
                    let vl = rd_u32(body, q).ok_or(LargeError::Malformed)?;
                    put(out, &mut at, &[u8::from(vl > self.cut)])?;
                    q += 4 + vl;
                }
                Ok((KV_OP_MSET, at))
            }
            _ => Err(LargeError::Malformed),
        }
    }
}

impl Uploader {
    /// An etcd Txn body with each large Put a Put of its manifest
    /// (`ETCD_OP_PUT_MANIFEST`), nested Txns too, in the order
    /// [`etcd_txn_puts`] found them.
    fn rewrite_etcd_txn(
        &self,
        body: &[u8],
        out: &mut [u8],
        at: &mut usize,
        large: &mut usize,
        depth: usize,
    ) -> Result<(), LargeError> {
        use LargeError::Malformed;
        if depth > ETCD_TXN_DEPTH_MAX {
            return Err(Malformed);
        }
        let mut p = etcd_txn_branches(body).ok_or(Malformed)?;
        put(out, at, sl(body, 0, p)?)?;
        for _branch in 0..2 {
            let n = *body.get(p).ok_or(Malformed)?;
            put(out, at, &[n])?;
            p += 1;
            for _ in 0..n {
                let (kind, len) = txn_op(body, p).ok_or(Malformed)?;
                let sub = body.get(p + 5..p + 5 + len).ok_or(Malformed)?;
                let is_large =
                    kind == ETCD_OP_PUT && put_value(sub).is_some_and(|(_, len)| len > self.cut);
                let head = *at;
                put(
                    out,
                    at,
                    &[
                        if is_large { ETCD_OP_PUT_MANIFEST } else { kind },
                        0,
                        0,
                        0,
                        0,
                    ],
                )?;
                let start = *at;
                if is_large {
                    let m = self.manifests.get(*large).ok_or(Malformed)?.encode();
                    *large += 1;
                    rewrite_put(sub, &m, out, at)?;
                } else if kind == ETCD_OP_TXN {
                    self.rewrite_etcd_txn(sub, out, at, large, depth + 1)?;
                } else {
                    put(out, at, sub)?;
                }
                let n = u32::try_from(*at - start).map_err(|_| LargeError::Overflow)?;
                put_at(out, head + 1, &n.to_le_bytes())?;
                p += 5 + len;
            }
        }
        put(out, at, body.get(p..).unwrap_or(&[]))
    }
}

/// Each Put value of an etcd Txn body (both branches, nested Txns in
/// place): `f(offset, length)` in the whole body, `base` where this body
/// starts in it. `None` when malformed or `f` refuses.
fn etcd_txn_puts<F: FnMut(usize, usize) -> bool>(
    body: &[u8],
    base: usize,
    depth: usize,
    f: &mut F,
) -> Option<()> {
    if depth > ETCD_TXN_DEPTH_MAX {
        return None;
    }
    let mut p = etcd_txn_branches(body)?;
    for _branch in 0..2 {
        let n = *body.get(p)?;
        p += 1;
        for _ in 0..n {
            let (kind, len) = txn_op(body, p)?;
            let sub = body.get(p + 5..p + 5 + len)?;
            match kind {
                ETCD_OP_PUT => {
                    let (off, vl) = put_value(sub)?;
                    if !f(base + p + 5 + off, vl) {
                        return None;
                    }
                }
                ETCD_OP_TXN => etcd_txn_puts(sub, base + p + 5, depth + 1, f)?,
                _ => {}
            }
            p += 5 + len;
        }
    }
    Some(())
}

impl Default for Uploader {
    fn default() -> Self {
        Self::new()
    }
}

/// A `PUT` body with its value replaced by `value`.
fn rewrite_put(
    body: &[u8],
    value: &[u8],
    out: &mut [u8],
    at: &mut usize,
) -> Result<(), LargeError> {
    let (off, len) = put_value(body).ok_or(LargeError::Malformed)?;
    put(out, at, body.get(..off - 4).ok_or(LargeError::Malformed)?)?;
    put(out, at, &(value.len() as u32).to_le_bytes())?;
    put(out, at, value)?;
    put(out, at, body.get(off + len..).ok_or(LargeError::Malformed)?)
}

// ── Reads ─────────────────────────────────────────────────────────────

/// A `PUT` body's flags byte: its offset, when the body has one.
fn put_flags_at(body: &[u8]) -> Option<usize> {
    let (off, len) = put_value(body)?;
    body.get(off + len)?;
    Some(off + len)
}

/// Does a `PUT` body read a previous value (`PUT_FLAG_GET`,
/// `PUT_FLAG_PREV_META`)? What [`mark_read`] marks.
fn put_reads(body: &[u8]) -> Option<u8> {
    let f = *body.get(put_flags_at(body)?)?;
    Some(f & (PUT_FLAG_GET | PUT_FLAG_PREV_META))
}

/// A `PUT` body marked as reading a previous manifest, into `out`: the
/// expiry (and lease) made explicit, then the second flags byte.
fn mark_put(body: &[u8], out: &mut [u8], at: &mut usize) -> Result<(), LargeError> {
    use LargeError::Malformed;
    let f_at = put_flags_at(body).ok_or(Malformed)?;
    put(out, at, sl(body, 0, f_at + 1)?)?;
    let flags = *body.get(f_at).ok_or(Malformed)?;
    let mut p = f_at + 1;
    let ttl = body.get(p..p + 8).unwrap_or(&[0; 8]);
    put(out, at, ttl)?;
    p += 8;
    if flags & PUT_FLAG_LEASE != 0 {
        put(out, at, body.get(p..p + 8).ok_or(Malformed)?)?;
        p += 8;
    }
    let flags2 = body.get(p).copied().unwrap_or(0) | PUT_FLAG2_MANIFESTS;
    put(out, at, &[flags2])
}

/// `[kind:u8][len:u32 LE]` of the etcd Txn op at `b[p..]`.
fn txn_op(b: &[u8], p: usize) -> Option<(u8, usize)> {
    Some((*b.get(p)?, rd_u32(b, p + 1)?))
}

/// Where an etcd Txn body's branches start: past its compares.
fn etcd_txn_branches(body: &[u8]) -> Option<usize> {
    let n = *body.first()? as usize;
    let mut p = 1usize;
    for _ in 0..n {
        // [target][result][key][end][operand:8][vlen:4][value]
        let kl = rd_u16(body, p + 2)?;
        p += 4 + kl;
        let el = rd_u16(body, p)?;
        p += 2 + el + 8;
        let vl = rd_u32(body, p)?;
        p += 4 + vl;
    }
    body.get(..p)?;
    Some(p)
}

/// An etcd Txn body with every read marked (its Ranges, its DeleteRanges
/// with prev_kv, its Puts that read a previous value), nested Txns too.
/// Marked whole, a Txn never meets a manifest it would refuse halfway.
fn mark_etcd_txn(
    body: &[u8],
    out: &mut [u8],
    at: &mut usize,
    depth: usize,
) -> Result<(), LargeError> {
    use LargeError::Malformed;
    if depth > ETCD_TXN_DEPTH_MAX {
        return Err(Malformed);
    }
    let mut p = etcd_txn_branches(body).ok_or(Malformed)?;
    put(out, at, sl(body, 0, p)?)?;
    for _branch in 0..2 {
        let n = *body.get(p).ok_or(Malformed)?;
        put(out, at, &[n])?;
        p += 1;
        for _ in 0..n {
            let (kind, len) = txn_op(body, p).ok_or(Malformed)?;
            let sub = body.get(p + 5..p + 5 + len).ok_or(Malformed)?;
            let head = *at;
            put(out, at, &[kind, 0, 0, 0, 0])?;
            let start = *at;
            match kind {
                ETCD_OP_RANGE => {
                    put(out, at, sub)?;
                    let f = range_flags_at(sub).ok_or(Malformed)?;
                    or_at(out, start + f, RANGE_META_FLAG_MANIFESTS)?;
                }
                ETCD_OP_PUT | ETCD_OP_PUT_MANIFEST if put_reads(sub).ok_or(Malformed)? != 0 => {
                    mark_put(sub, out, at)?
                }
                ETCD_OP_DELETE => {
                    put(out, at, sub)?;
                    if sub.last().ok_or(Malformed)? & 1 != 0 {
                        or_at(out, *at - 1, DELETE_META_FLAG_MANIFESTS)?;
                    }
                }
                ETCD_OP_TXN => mark_etcd_txn(sub, out, at, depth + 1)?,
                _ => put(out, at, sub)?,
            }
            let n = u32::try_from(*at - start).map_err(|_| LargeError::Overflow)?;
            put_at(out, head + 1, &n.to_le_bytes())?;
            p += 5 + len;
        }
    }
    put(out, at, body.get(p..).unwrap_or(&[]))
}

/// A `KV_OP_RANGE_META` body's flags byte: its offset.
fn range_flags_at(body: &[u8]) -> Option<usize> {
    let e = 2 + rd_u16(body, 0)?;
    let f = e + 2 + rd_u16(body, e)? + 8 + 4;
    body.get(f)?;
    Some(f)
}

/// Most bytes [`mark_read`] adds to a request: a `PUT`'s expiry and
/// second flags byte, for each `PUT` a Txn holds.
pub const fn mark_growth(puts: usize) -> usize {
    puts * 9
}

/// Room a request of up to `len` bytes needs marked: as many `PUT`s as
/// fit it (each at least a Txn op head and an empty key and value)
/// growing by [`mark_growth`].
pub const fn mark_room(len: usize) -> usize {
    len + mark_growth(len / 12)
}

/// Does an outgoing write carry a value that goes as chunks — one past a
/// data chunk, or one the request needs chunked to fit `room` bytes
/// ([`Uploader::start`])? Such a request needs the layer to upload it.
pub fn carries_large(op: u8, body: &[u8], room: usize) -> bool {
    if op == KV_OP_TXN_STAGED {
        // Staged, it goes out only through the layer.
        return true;
    }
    let Ok(cut) = cut_for(op, body, room) else {
        return false;
    };
    let mut any = false;
    let _ = each_value(op, body, &mut |_, len| {
        any |= len > cut;
        true
    });
    any
}

pub use kv_chunks::ENTRY_BODY_MAX;

/// Each value an outgoing write puts (a `PUT`'s, a Txn's or an etcd Txn's
/// `PUT`s, an MSET's): `f(offset, length)` in `body`. `f` answering
/// `false` is [`LargeError::Overflow`]. `Ok(false)`: the op puts none.
fn each_value<F: FnMut(usize, usize) -> bool>(
    op: u8,
    body: &[u8],
    f: &mut F,
) -> Result<bool, LargeError> {
    let mut over = false;
    let mut g = |off: usize, len: usize| {
        let ok = f(off, len);
        over |= !ok;
        ok
    };
    let walked =
        match op {
            KV_OP_PUT => put_value(body).map(|(off, len)| {
                g(off, len);
            }),
            KV_OP_TXN | KV_OP_TXN_STAGED => txn_puts(body, op == KV_OP_TXN_STAGED, |_, b, bl| {
                match body.get(b..b + bl).and_then(put_value) {
                    Some((off, len)) => g(b + off, len),
                    None => false,
                }
            })
            .map(|_| ()),
            KV_OP_MSET => mset_values(body, &mut g).map(|_| ()),
            KV_OP_ETCD_TXN => etcd_txn_puts(body, 0, 0, &mut |off, len| g(off, len)).map(|_| ()),
            _ => return Ok(false),
        };
    match walked {
        Some(()) if !over => Ok(true),
        _ if over => Err(LargeError::Overflow),
        _ => Err(LargeError::Malformed),
    }
}

/// The length past which a write's values go as chunks: a data chunk's,
/// lowered past the largest values that remain inline while the request
/// as rewritten would not fit `room` bytes. Each chunked value leaves a
/// manifest in its place, and a rewritten MSET gains one mark byte per
/// pair ([`Uploader::rewrite`]); every other rewrite is no longer than
/// that accounting (a staged Txn's op heads shrink). A value no longer
/// than a manifest is never chunked: it would not shrink the request.
fn cut_for(op: u8, body: &[u8], room: usize) -> Result<usize, LargeError> {
    let marks = if op == KV_OP_MSET {
        rd_u16(body, 0).ok_or(LargeError::Malformed)?
    } else {
        0
    };
    let mut cut = DATA_MAX;
    loop {
        let mut saved = 0usize;
        let mut chunked = false;
        let mut next = 0usize;
        each_value(op, body, &mut |_, len| {
            if len > cut {
                saved += len - MANIFEST_LEN;
                chunked = true;
            } else if len > next {
                next = len;
            }
            true
        })?;
        let rewritten = if chunked {
            body.len().saturating_sub(saved) + marks
        } else {
            body.len()
        };
        if rewritten <= room || next <= MANIFEST_LEN {
            return Ok(cut);
        }
        cut = next - 1;
    }
}

/// Does an etcd Txn read values (a Range, a Put's previous value, a
/// DeleteRange's prev_kv), nested Txns included? Such a Txn must go
/// marked: unmarked, a manifest refused midway would leave the ops before
/// it applied.
pub fn etcd_txn_reads(body: &[u8]) -> bool {
    etcd_txn_reads_at(body, 0).unwrap_or(true)
}

fn etcd_txn_reads_at(body: &[u8], depth: usize) -> Option<bool> {
    if depth > ETCD_TXN_DEPTH_MAX {
        return None;
    }
    let mut p = etcd_txn_branches(body)?;
    for _branch in 0..2 {
        let n = *body.get(p)?;
        p += 1;
        for _ in 0..n {
            let (kind, len) = txn_op(body, p)?;
            let sub = body.get(p + 5..p + 5 + len)?;
            let reads = match kind {
                ETCD_OP_RANGE => true,
                ETCD_OP_PUT | ETCD_OP_PUT_MANIFEST => put_reads(sub)? != 0,
                ETCD_OP_DELETE => sub.last()? & 1 != 0,
                ETCD_OP_TXN => etcd_txn_reads_at(sub, depth + 1)?,
                _ => false,
            };
            if reads {
                return Some(true);
            }
            p += 5 + len;
        }
    }
    Some(false)
}

/// Mark an outgoing read as reading manifests, into `out` (room for the
/// body and [`mark_growth`]): its length, or `None` when the op needs no
/// mark (it is sent as it is).
pub fn mark_read(op: u8, body: &[u8], out: &mut [u8]) -> Option<usize> {
    let skip_key = |at: usize| -> Option<usize> { Some(at + 2 + rd_u16(body, at)?) };
    let n = body.len();
    match op {
        KV_OP_RANGE_META => {
            let f = range_flags_at(body)?;
            put_at(out, 0, body).ok()?;
            or_at(out, f, RANGE_META_FLAG_MANIFESTS).ok()?;
            Some(n)
        }
        KV_OP_SCAN_VERSIONS => {
            // [start][end][from:8][to:8][cursor:8][limit:2] then an
            // optional flags byte.
            let flags_at = skip_key(skip_key(0)?)? + 8 + 8 + 8 + 2;
            put_at(out, 0, body).ok()?;
            if n == flags_at {
                put_at(out, n, &[VERSIONS_FLAG_MANIFESTS]).ok()?;
                Some(n + 1)
            } else {
                or_at(out, flags_at, VERSIONS_FLAG_MANIFESTS).ok()?;
                Some(n)
            }
        }
        KV_OP_PUT | KV_OP_PUT_MANIFEST if put_reads(body)? != 0 => {
            let mut at = 0;
            mark_put(body, out, &mut at).ok()?;
            Some(at)
        }
        KV_OP_DELETE_META if body.last()? & 1 != 0 => {
            put_at(out, 0, body).ok()?;
            or_at(out, n.checked_sub(1)?, DELETE_META_FLAG_MANIFESTS).ok()?;
            Some(n)
        }
        KV_OP_ETCD_TXN => {
            let mut at = 0;
            mark_etcd_txn(body, out, &mut at, 0).ok()?;
            Some(at)
        }
        KV_OP_MGET => {
            // The flags byte after the keys.
            let mut at = 2usize;
            for _ in 0..rd_u16(body, 0)? {
                at += 2 + rd_u16(body, at)?;
            }
            put_at(out, 0, body.get(..at)?).ok()?;
            put_at(out, at, &[MGET_FLAG_MANIFESTS]).ok()?;
            Some(at + 1)
        }
        _ => None,
    }
}

/// What a read's reply needs next.
#[derive(Debug, PartialEq, Eq)]
pub enum Need {
    /// Fetch this chunk (`KV_OP_GET_CHUNK`) and [`Expander::take`] it.
    Chunk(Hash),
    /// The expanded reply is complete: `(result, length)` in the output.
    Done(u8, usize),
}

/// Most manifests one reply expands. A page holding more is cut before
/// the first that does not fit; any other answer is too large.
pub const MAX_LARGE_PER_REPLY: usize = 64;

/// A value to fetch into the laid-out answer: where, and whose.
#[derive(Clone, Copy)]
struct Fill {
    at: usize,
    m: Manifest,
}

const NO_FILL: Fill = Fill {
    at: 0,
    m: NO_MANIFEST,
};

/// The expanded answer being laid out: bytes written, and the values
/// reserved for fetching.
struct Lay<'o> {
    out: &'o mut [u8],
    at: usize,
    /// Where values to fetch are reserved; empty: none may be (a manifest
    /// is [`LargeError::Large`]).
    fills: &'o mut [Fill],
    n: usize,
}

impl Lay<'_> {
    /// Is there room for `bytes` more, `fills` of them fetched?
    fn room(&self, bytes: usize, fills: usize) -> bool {
        self.at + bytes <= self.out.len() && self.n + fills <= self.fills.len()
    }

    /// May a value be fetched into this answer at all?
    fn fetchable(&self, m: Option<Manifest>) -> Result<(), LargeError> {
        if m.is_some() && self.fills.is_empty() {
            return Err(LargeError::Large);
        }
        Ok(())
    }

    fn put(&mut self, b: &[u8]) -> Result<(), LargeError> {
        put(self.out, &mut self.at, b)
    }

    /// A value, inline or (a manifest) reserved for its fetch; its length
    /// first.
    fn value(&mut self, v: &[u8], m: Option<Manifest>) -> Result<(), LargeError> {
        match m {
            None => {
                self.put(&(v.len() as u32).to_le_bytes())?;
                self.put(v)
            }
            Some(m) => {
                self.fetchable(Some(m))?;
                let total = usize::try_from(m.total).map_err(|_| LargeError::Overflow)?;
                self.put(&(total as u32).to_le_bytes())?;
                if !self.room(total, 1) {
                    return Err(LargeError::Overflow);
                }
                *self.fills.get_mut(self.n).ok_or(LargeError::Overflow)? = Fill { at: self.at, m };
                self.n += 1;
                self.at += total;
                Ok(())
            }
        }
    }
}

/// One entry of a KVS answer (`[klen:2][key][vlen:4][value][meta:32]`,
/// then `[flags:1]` when `marked`): where it ends, its value, and the
/// manifest its value is.
struct KvsEntry {
    end: usize,
    value_off: usize,
    value_len: usize,
    m: Option<Manifest>,
}

fn kvs_entry(reply: &[u8], e: usize, marked: bool) -> Option<KvsEntry> {
    let kl = rd_u16(reply, e)?;
    let vl = rd_u32(reply, e + 2 + kl)?;
    let value_off = e + 2 + kl + 4;
    let end = value_off + vl + 32 + usize::from(marked);
    reply.get(..end)?;
    Some(KvsEntry {
        end,
        value_off,
        value_len: vl,
        m: entry_manifest(reply, marked, end, value_off, vl)?,
    })
}

/// The manifest a marked entry's value is (`Some(None)`: none).
fn entry_manifest(
    reply: &[u8],
    marked: bool,
    end: usize,
    value_off: usize,
    vl: usize,
) -> Option<Option<Manifest>> {
    // A keys-only entry carries the flag but no value.
    if !marked || reply.get(end - 1)? & RECORD_FLAG_MANIFEST == 0 || vl == 0 {
        return Some(None);
    }
    Manifest::parse(reply.get(value_off..value_off + vl)?).map(Some)
}

/// An entry's expanded size.
fn grown(e: usize, end: usize, value_len: usize, marked: bool, m: Option<Manifest>) -> usize {
    end - e - usize::from(marked) - value_len + m.map_or(value_len, |m| m.total as usize)
}

/// A KVS answer laid out: `[total:8][more:1][count:2]` and its entries.
/// A `pageable` answer that does not fit is cut (the entries that fit,
/// `more` = partial); any other is too large.
fn lay_kvs(lay: &mut Lay, reply: &[u8], marked: bool, pageable: bool) -> Result<bool, LargeError> {
    use LargeError::Malformed;
    let count = rd_u16(reply, 9).ok_or(Malformed)?;
    let head = lay.at;
    lay.put(reply.get(..11).ok_or(Malformed)?)?;
    let mut e = 11usize;
    for i in 0..count {
        let x = kvs_entry(reply, e, marked).ok_or(Malformed)?;
        lay.fetchable(x.m)?;
        if !lay.room(
            grown(e, x.end, x.value_len, marked, x.m),
            usize::from(x.m.is_some()),
        ) {
            if pageable && i > 0 {
                put_at(lay.out, head + 8, &[KVS_MORE_PARTIAL])?;
                put_at(lay.out, head + 9, &(i as u16).to_le_bytes())?;
                return Ok(true);
            }
            return Err(LargeError::Overflow);
        }
        let v_end = x.value_off + x.value_len;
        lay.put(sl(reply, e, x.value_off - 4)?)?;
        lay.value(sl(reply, x.value_off, v_end)?, x.m)?;
        lay.put(sl(reply, v_end, x.end - usize::from(marked))?)?;
        e = x.end;
    }
    Ok(false)
}

/// A VERSIONS page laid out: `[next_cursor:8][count:2]` then entries
/// `[rev:8][ts:8][kind:1][klen:2][key][vlen:4][value]` (`[meta:24]` on a
/// put when `meta`) `[flags:1]`; cut as a pageable KVS answer is, the next
/// page starting after the entries kept.
fn lay_versions(lay: &mut Lay, reply: &[u8], meta: bool, cursor: u64) -> Result<bool, LargeError> {
    use LargeError::Malformed;
    let count = rd_u16(reply, 8).ok_or(Malformed)?;
    let head = lay.at;
    lay.put(reply.get(..10).ok_or(Malformed)?)?;
    let mut e = 10usize;
    for i in 0..count {
        let kl = rd_u16(reply, e + 17).ok_or(Malformed)?;
        let vl = rd_u32(reply, e + 19 + kl).ok_or(Malformed)?;
        let value_off = e + 19 + kl + 4;
        let put_meta = meta && *reply.get(e + 16).ok_or(Malformed)? == VERSION_KIND_PUT;
        let end = value_off + vl + if put_meta { 24 } else { 0 } + 1;
        let m = entry_manifest(reply, true, end, value_off, vl).ok_or(Malformed)?;
        lay.fetchable(m)?;
        if !lay.room(grown(e, end, vl, true, m), usize::from(m.is_some())) {
            if i > 0 {
                let next = cursor + i as u64;
                put_at(lay.out, head, &next.to_le_bytes())?;
                put_at(lay.out, head + 8, &(i as u16).to_le_bytes())?;
                return Ok(true);
            }
            return Err(LargeError::Overflow);
        }
        lay.put(sl(reply, e, value_off - 4)?)?;
        lay.value(sl(reply, value_off, value_off + vl)?, m)?;
        lay.put(sl(reply, value_off + vl, end - 1)?)?;
        e = end;
    }
    Ok(false)
}

/// A `PUT`'s answer: with `PUT_FLAG_PREV_META` read marked, `[present:1]`
/// and one marked entry; else as it is.
fn lay_put(lay: &mut Lay, reply: &[u8], req: &[u8]) -> Result<(), LargeError> {
    use LargeError::Malformed;
    let reads = put_reads(req).ok_or(Malformed)?;
    if reads & PUT_FLAG_PREV_META == 0 || reply.first() != Some(&1) {
        return lay.put(reply);
    }
    lay.put(&[1])?;
    let x = kvs_entry(reply, 1, true).ok_or(Malformed)?;
    let v_end = x.value_off + x.value_len;
    lay.put(sl(reply, 1, x.value_off - 4)?)?;
    lay.value(sl(reply, x.value_off, v_end)?, x.m)?;
    lay.put(sl(reply, v_end, x.end - 1)?)
}

/// A marked MGET answer, `[count:2]{[len:4][value][flags:1]}` (`len`
/// null for a missing key), laid out unmarked.
fn lay_array(lay: &mut Lay, reply: &[u8]) -> Result<(), LargeError> {
    use LargeError::Malformed;
    let count = rd_u16(reply, 0).ok_or(Malformed)?;
    lay.put(sl(reply, 0, 2)?)?;
    let mut e = 2usize;
    for _ in 0..count {
        let len = rd_u32(reply, e).ok_or(Malformed)?;
        if len as u32 == KV_ARRAY_ELEMENT_NULL {
            lay.put(sl(reply, e, e + 4)?)?;
            e += 5;
            continue;
        }
        let v = sl(reply, e + 4, e + 4 + len)?;
        let flags = *reply.get(e + 4 + len).ok_or(Malformed)?;
        let m = if flags & RECORD_FLAG_MANIFEST != 0 {
            Some(Manifest::parse(v).ok_or(Malformed)?)
        } else {
            None
        };
        lay.value(v, m)?;
        e += 5 + len;
    }
    Ok(())
}

/// An etcd Txn's answer, `[succeeded:1][count:1]` then per op of the
/// branch taken `[kind:1][result:1][len:4][answer]`: each answer laid out
/// as the request's op marked it (see [`mark_etcd_txn`]), its length
/// rewritten.
fn lay_etcd_txn(lay: &mut Lay, reply: &[u8], req: &[u8], depth: usize) -> Result<(), LargeError> {
    use LargeError::Malformed;
    if depth > ETCD_TXN_DEPTH_MAX {
        return Err(Malformed);
    }
    let succeeded = *reply.first().ok_or(Malformed)? != 0;
    let count = *reply.get(1).ok_or(Malformed)?;
    lay.put(sl(reply, 0, 2)?)?;
    // The branch taken, in the request.
    let mut q = etcd_txn_branches(req).ok_or(Malformed)?;
    if !succeeded {
        let n = *req.get(q).ok_or(Malformed)?;
        q += 1;
        for _ in 0..n {
            q += 5 + txn_op(req, q).ok_or(Malformed)?.1;
        }
    }
    if *req.get(q).ok_or(Malformed)? != count {
        return Err(Malformed);
    }
    q += 1;
    let mut p = 2usize;
    for _ in 0..count {
        let (kind, rq) = txn_op(req, q).ok_or(Malformed)?;
        let sub_req = req.get(q + 5..q + 5 + rq).ok_or(Malformed)?;
        q += 5 + rq;
        let result = *reply.get(p + 1).ok_or(Malformed)?;
        let len = rd_u32(reply, p + 2).ok_or(Malformed)?;
        let sub = reply.get(p + 6..p + 6 + len).ok_or(Malformed)?;
        // A large Put went as a Put of its manifest.
        let answered = *reply.get(p).ok_or(Malformed)?;
        let same = answered == kind || (kind == ETCD_OP_PUT && answered == ETCD_OP_PUT_MANIFEST);
        if !same {
            return Err(Malformed);
        }
        // Answered as the module asked: a large Put is its Put.
        lay.put(&[kind, result, 0, 0, 0, 0])?;
        let start = lay.at;
        match kind {
            ETCD_OP_RANGE => {
                lay_kvs(lay, sub, true, false)?;
            }
            ETCD_OP_PUT | ETCD_OP_PUT_MANIFEST => lay_put(lay, sub, sub_req)?,
            ETCD_OP_DELETE => {
                let prev_kv = sub_req.last().ok_or(Malformed)? & 1 != 0;
                lay_kvs(lay, sub, prev_kv, false)?;
            }
            ETCD_OP_TXN => lay_etcd_txn(lay, sub, sub_req, depth + 1)?,
            _ => lay.put(sub)?,
        }
        let n = u32::try_from(lay.at - start).map_err(|_| LargeError::Overflow)?;
        put_at(lay.out, start - 4, &n.to_le_bytes())?;
        p += 6 + len;
    }
    Ok(())
}

/// Lay an answer out (see [`Expander::start`]): the answer's result (a
/// value read as a manifest answers `KV_RESULT_OK`) and whether a page was
/// cut.
fn layout(
    op: u8,
    req: &[u8],
    result: u8,
    reply: &[u8],
    lay: &mut Lay,
) -> Result<(u8, bool), LargeError> {
    use LargeError::Malformed;
    match (op, result) {
        (KV_OP_GET | KV_OP_PUT, KV_RESULT_MANIFEST) => {
            let m = Manifest::parse(reply).ok_or(Malformed)?;
            lay.fetchable(Some(m))?;
            // The value alone, without a length before it.
            match usize::try_from(m.total) {
                Ok(total) if lay.room(total, 1) => {
                    *lay.fills.first_mut().ok_or(Malformed)? = Fill { at: 0, m };
                    lay.n = 1;
                    lay.at = total;
                    Ok((KV_RESULT_OK, false))
                }
                _ => Err(LargeError::Overflow),
            }
        }
        (KV_OP_PUT, _) => lay_put(lay, reply, req).map(|()| (result, false)),
        (KV_OP_RANGE_META, _) => lay_kvs(lay, reply, true, true).map(|c| (result, c)),
        (KV_OP_DELETE_META, _) => {
            let prev_kv = req.last().ok_or(Malformed)? & 1 != 0;
            lay_kvs(lay, reply, prev_kv, false).map(|c| (result, c))
        }
        (KV_OP_SCAN_VERSIONS, _) => {
            let skip = |at: usize| -> Option<usize> { Some(at + 2 + rd_u16(req, at)?) };
            let after_end = skip(skip(0).ok_or(Malformed)?).ok_or(Malformed)?;
            let cursor = rd_u64(req, after_end + 16).ok_or(Malformed)?;
            let meta = req
                .get(after_end + 26)
                .is_some_and(|f| f & VERSIONS_FLAG_META != 0);
            lay_versions(lay, reply, meta, cursor).map(|c| (result, c))
        }
        (KV_OP_ETCD_TXN, _) => lay_etcd_txn(lay, reply, req, 0).map(|()| (result, false)),
        (KV_OP_MGET, _) => lay_array(lay, reply).map(|()| (result, false)),
        _ => Err(Malformed),
    }
}

/// A marked answer with no large value in it, unmarked into `out`:
/// `Ok(Some((result, length)))`; `Ok(None)` when it holds one, to be
/// expanded by an [`Expander`] (or a [`Session`]). An answer larger than
/// `out` is `KV_RESULT_TOO_LARGE`. Needs no fetch state, so a module can
/// answer the common case without holding a session.
pub fn unmark(
    op: u8,
    req: &[u8],
    result: u8,
    reply: &[u8],
    out: &mut [u8],
) -> Result<Option<(u8, usize)>, LargeError> {
    let mut lay = Lay {
        out,
        at: 0,
        fills: &mut [],
        n: 0,
    };
    match layout(op, req, result, reply, &mut lay) {
        Ok((r, _)) => Ok(Some((r, lay.at))),
        Err(LargeError::Large) => Ok(None),
        Err(LargeError::Overflow) => Ok(Some((KV_RESULT_TOO_LARGE, 0))),
        Err(e) => Err(e),
    }
}

/// Expands a reply's manifests into their values. The answer is laid out
/// first — every manifest states its value's length, so each lands at a
/// known place and every length (a Txn's nested ones too) is written
/// before a byte is fetched — then the values are fetched in.
pub struct Expander {
    result: u8,
    out_len: usize,
    fills: [Fill; MAX_LARGE_PER_REPLY],
    n: usize,
    /// The value being fetched, and how much of it has landed.
    current: usize,
    written: usize,
    reader: Option<Reader>,
    /// A page cut short: entries kept.
    cut: bool,
}

impl Expander {
    pub const fn new() -> Self {
        Expander {
            result: 0,
            out_len: 0,
            fills: [NO_FILL; MAX_LARGE_PER_REPLY],
            n: 0,
            current: 0,
            written: 0,
            reader: None,
            cut: false,
        }
    }

    /// Does this reply need expanding (the request marked by
    /// [`mark_read`], or a `KV_OP_GET`)?
    pub fn wants(op: u8, result: u8) -> bool {
        matches!(
            (op, result),
            (KV_OP_GET | KV_OP_PUT, KV_RESULT_MANIFEST)
                | (KV_OP_PUT, KV_RESULT_OK)
                | (KV_OP_RANGE_META | KV_OP_DELETE_META, KV_RESULT_KVS)
                | (KV_OP_SCAN_VERSIONS, KV_RESULT_VERSIONS)
                | (KV_OP_ETCD_TXN, KV_RESULT_ETCD_TXN)
                | (KV_OP_MGET, KV_RESULT_ARRAY)
        )
    }

    /// Lay the expanded answer out into `out`. `op` and `req` are the
    /// request as the module wrote it (before [`mark_read`]); `result` and
    /// `reply` its answer. Then [`Expander::step`] until done.
    pub fn start(
        &mut self,
        op: u8,
        req: &[u8],
        result: u8,
        reply: &[u8],
        out: &mut [u8],
    ) -> Result<(), LargeError> {
        *self = Expander::new();
        self.result = result;
        let mut lay = Lay {
            out,
            at: 0,
            fills: &mut self.fills,
            n: 0,
        };
        let laid = layout(op, req, result, reply, &mut lay);
        match laid {
            Ok((r, cut)) => {
                self.result = r;
                self.cut = cut;
                self.out_len = lay.at;
                self.n = lay.n;
            }
            // More than the module's answer holds: too large, honestly.
            Err(LargeError::Overflow) => {
                self.result = KV_RESULT_TOO_LARGE;
                self.out_len = 0;
                self.n = 0;
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }

    /// What is needed next: a chunk of a value being fetched, or the
    /// answer, whole.
    pub fn step(&mut self) -> Result<Need, LargeError> {
        while self.current < self.n {
            let m = self.fills.get(self.current).ok_or(LargeError::Malformed)?.m;
            let r = self.reader.get_or_insert_with(|| Reader::new(m));
            if let Some(h) = r.want() {
                return Ok(Need::Chunk(h));
            }
            if !r.complete() || self.written as u64 != m.total {
                return Err(LargeError::BadChunk);
            }
            self.reader = None;
            self.written = 0;
            self.current += 1;
        }
        Ok(Need::Done(self.result, self.out_len))
    }

    /// Hand over the chunk [`Need::Chunk`] asked for.
    pub fn take(&mut self, chunk: &[u8], out: &mut [u8]) -> Result<(), LargeError> {
        let f = *self.fills.get(self.current).ok_or(LargeError::Malformed)?;
        let r = self.reader.as_mut().ok_or(LargeError::Malformed)?;
        if let Step::Data(d) = r.take(chunk).ok_or(LargeError::BadChunk)? {
            if (self.written + d.len()) as u64 > f.m.total {
                return Err(LargeError::BadChunk);
            }
            let mut at = f.at + self.written;
            put(out, &mut at, d)?;
            self.written += d.len();
        }
        Ok(())
    }

    /// Was the page cut short?
    pub fn was_cut(&self) -> bool {
        self.cut
    }
}

impl Default for Expander {
    fn default() -> Self {
        Self::new()
    }
}

/// A `KV_OP_GET_CHUNK` body.
pub fn chunk_request(hash: &Hash) -> [u8; HASH_LEN] {
    *hash
}

/// The op a module sends to fetch or store a chunk.
pub const FETCH_OP: u8 = KV_OP_GET_CHUNK;
pub const UPLOAD_OP: u8 = KV_OP_PUT_CHUNK;

// ── A session: one request through the layer ──────────────────────────

/// What a [`Session`] needs done next.
#[derive(Debug, PartialEq, Eq)]
pub enum Act {
    /// Send `op` with the body the session wrote into `send[..len]`, and
    /// hand its reply to [`Session::reply`].
    Send(u8, usize),
    /// The answer: `result`, its body in `out[..len]`.
    Done(u8, usize),
    /// The answer is the reply as it arrived, unchanged.
    AsIs,
}

const S_IDLE: u8 = 0;
const S_UPLOAD: u8 = 1;
const S_REQUEST: u8 = 2;
const S_FETCH: u8 = 3;

/// One request through the layer, whatever it needs: its large values
/// uploaded, its reads marked, its answer expanded. The module supplies
/// every buffer (its policy sizes them) and does the sending:
///
/// - `req`: the request as the module wrote it, held unchanged until the
///   session is done;
/// - `work`: the request as it goes out (marked), held likewise — room
///   for `req` and [`mark_growth`];
/// - `send`: each request to send (a chunk, the request, a fetch) — room
///   for the rewritten request and [`CHUNK_REQUEST_MAX`];
/// - `out`: the answer, expanded.
pub struct Session {
    up: Uploader,
    ex: Expander,
    op: u8,
    state: u8,
    work_len: usize,
    /// The reply was expanded (else it is answered as it came).
    reads: bool,
}

impl Session {
    pub const fn new() -> Self {
        Session {
            up: Uploader::new(),
            ex: Expander::new(),
            op: 0,
            state: S_IDLE,
            work_len: 0,
            reads: false,
        }
    }

    /// Is a request in flight?
    pub fn busy(&self) -> bool {
        self.state != S_IDLE
    }

    /// Is the next reply the request's own (not a chunk's)? Its revision
    /// is the answer's.
    pub fn awaits_request(&self) -> bool {
        self.state == S_REQUEST
    }

    /// Begin `op` over `req`: the first thing to send.
    pub fn begin(
        &mut self,
        op: u8,
        req: &[u8],
        work: &mut [u8],
        send: &mut [u8],
    ) -> Result<Act, LargeError> {
        self.op = op;
        self.state = S_IDLE;
        // Marked first: the upload rewrites the marked request.
        let marked = mark_read(op, req, work);
        self.reads = marked.is_some() || op == KV_OP_GET;
        self.work_len = match marked {
            Some(n) => n,
            None => {
                put(work, &mut 0, req)?;
                req.len()
            }
        };
        let work = sl(work, 0, self.work_len)?;
        if self.up.start(op, work, ENTRY_BODY_MAX)? {
            self.state = S_UPLOAD;
            return self.next_upload(work, send);
        }
        self.state = S_REQUEST;
        put(send, &mut 0, work)?;
        Ok(Act::Send(op, work.len()))
    }

    fn next_upload(&mut self, work: &[u8], send: &mut [u8]) -> Result<Act, LargeError> {
        let mut chunk = [0u8; CHUNK_REQUEST_MAX];
        match self.up.next_chunk(work, &mut chunk)? {
            Some(n) => {
                put(send, &mut 0, sl(&chunk, 0, n)?)?;
                Ok(Act::Send(UPLOAD_OP, n))
            }
            None => {
                let (op, n) = self.up.rewrite(work, send)?;
                self.state = S_REQUEST;
                Ok(Act::Send(op, n))
            }
        }
    }

    /// The reply (`result`, `body`) to the last [`Act::Send`].
    pub fn reply(
        &mut self,
        req: &[u8],
        work: &[u8],
        result: u8,
        body: &[u8],
        send: &mut [u8],
        out: &mut [u8],
    ) -> Result<Act, LargeError> {
        match self.state {
            S_UPLOAD => {
                if result != KV_RESULT_OK {
                    // The chunk was refused: so is the request.
                    self.state = S_IDLE;
                    return Ok(Act::Done(result, 0));
                }
                let work = work.get(..self.work_len).ok_or(LargeError::Malformed)?;
                self.next_upload(work, send)
            }
            S_REQUEST => {
                if !(self.reads && Expander::wants(self.op, result)) {
                    self.state = S_IDLE;
                    return Ok(Act::AsIs);
                }
                self.ex.start(self.op, req, result, body, out)?;
                self.state = S_FETCH;
                self.next_fetch(send)
            }
            S_FETCH => {
                if result != KV_RESULT_OK {
                    // A chunk the manifest names is gone: the value does
                    // not read back.
                    self.state = S_IDLE;
                    return Err(LargeError::BadChunk);
                }
                self.ex.take(body, out)?;
                self.next_fetch(send)
            }
            _ => Err(LargeError::Malformed),
        }
    }

    fn next_fetch(&mut self, send: &mut [u8]) -> Result<Act, LargeError> {
        match self.ex.step()? {
            Need::Chunk(h) => {
                put(send, &mut 0, &h)?;
                Ok(Act::Send(FETCH_OP, HASH_LEN))
            }
            Need::Done(r, n) => {
                self.state = S_IDLE;
                Ok(Act::Done(r, n))
            }
        }
    }

    /// Expand an answer to a request the module sent itself (marked with
    /// [`mark_read`], or a `KV_OP_GET`): the first chunk to fetch, or the
    /// answer. `req` is the request as the module wrote it, held until
    /// done; `reply` is needed only for this call.
    pub fn expand(
        &mut self,
        op: u8,
        req: &[u8],
        result: u8,
        reply: &[u8],
        send: &mut [u8],
        out: &mut [u8],
    ) -> Result<Act, LargeError> {
        self.op = op;
        self.reads = true;
        self.ex.start(op, req, result, reply, out)?;
        self.state = S_FETCH;
        self.next_fetch(send)
    }

    /// Abandon the request in flight.
    pub fn reset(&mut self) {
        self.state = S_IDLE;
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}
