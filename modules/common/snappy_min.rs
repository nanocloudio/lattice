//! Minimal Snappy raw-block decompressor — Prometheus `remote_write` frames
//! its protobuf body as a single Snappy raw block. Pure logic, `no_std`, no
//! allocation: it decodes into a caller-provided output buffer and refuses
//! (returns `None`) on any malformed or over-long input rather than growing.
//!
//! This is the "raw" Snappy block format (not the streaming framing format):
//! a varint uncompressed-length header followed by a sequence of elements,
//! each a literal run or a back-reference copy. Reference:
//! google/snappy `format_description.txt`.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC anchor module and host tests; each consumer uses a subset of the surface"
)]

/// Decode a Snappy raw block from `src` into `dst`, returning the number of
/// bytes written. Returns `None` if the input is malformed, references data
/// before the output start, or would exceed `dst`.
pub fn decode(src: &[u8], dst: &mut [u8]) -> Option<usize> {
    let (expect_len, mut i) = read_varint(src)?;
    if expect_len > dst.len() {
        return None;
    }
    let mut o = 0usize;
    while i < src.len() {
        let tag = src[i];
        i += 1;
        match tag & 0x03 {
            0 => {
                // Literal. Upper 6 bits are (length-1), or a 1..4 byte
                // extension count when ≥ 60.
                let mut len = (tag >> 2) as usize;
                if len >= 60 {
                    let extra = len - 59; // 60→1, 61→2, 62→3, 63→4
                    if i + extra > src.len() {
                        return None;
                    }
                    let mut l = 0usize;
                    for k in 0..extra {
                        l |= (src[i + k] as usize) << (8 * k);
                    }
                    i += extra;
                    len = l;
                }
                len += 1;
                if i + len > src.len() || o + len > dst.len() {
                    return None;
                }
                dst[o..o + len].copy_from_slice(&src[i..i + len]);
                i += len;
                o += len;
            }
            1 => {
                // Copy with 1-byte offset. Length 4..11 in bits 2..4, high
                // 3 offset bits in bits 5..7, low 8 in the next byte.
                if i >= src.len() {
                    return None;
                }
                let len = 4 + ((tag >> 2) & 0x07) as usize;
                let off = (((tag >> 5) as usize) << 8) | src[i] as usize;
                i += 1;
                o = copy_from(dst, o, off, len)?;
            }
            2 => {
                // Copy with 2-byte little-endian offset.
                if i + 2 > src.len() {
                    return None;
                }
                let len = 1 + (tag >> 2) as usize;
                let off = src[i] as usize | ((src[i + 1] as usize) << 8);
                i += 2;
                o = copy_from(dst, o, off, len)?;
            }
            _ => {
                // Copy with 4-byte little-endian offset.
                if i + 4 > src.len() {
                    return None;
                }
                let len = 1 + (tag >> 2) as usize;
                let off = src[i] as usize
                    | ((src[i + 1] as usize) << 8)
                    | ((src[i + 2] as usize) << 16)
                    | ((src[i + 3] as usize) << 24);
                i += 4;
                o = copy_from(dst, o, off, len)?;
            }
        }
    }
    if o != expect_len {
        return None;
    }
    Some(o)
}

/// A back-reference copy: append `len` bytes taken from `off` bytes before
/// the current output position. Overlapping copies are byte-by-byte (the
/// Snappy self-referential run), so this cannot use `copy_from_slice`.
fn copy_from(dst: &mut [u8], mut o: usize, off: usize, len: usize) -> Option<usize> {
    if off == 0 || off > o {
        return None;
    }
    if o + len > dst.len() {
        return None;
    }
    let mut src_pos = o - off;
    for _ in 0..len {
        dst[o] = dst[src_pos];
        o += 1;
        src_pos += 1;
    }
    Some(o)
}

/// Read a base-128 varint, returning (value, next-index). Bounded to the
/// 5 bytes a 32-bit length needs.
fn read_varint(src: &[u8]) -> Option<(usize, usize)> {
    let mut val: usize = 0;
    let mut shift = 0;
    let mut i = 0;
    while i < src.len() && i < 5 {
        let b = src[i];
        val |= ((b & 0x7f) as usize) << shift;
        i += 1;
        if b & 0x80 == 0 {
            return Some((val, i));
        }
        shift += 7;
    }
    None
}
