// Bounded, no_std, no-alloc HEX codec. Like the other `*_core.rs` files it has
// NO inner attributes and NO test module, so it is `include!`d verbatim by both
// this crate (`lib.rs`) and the param-driven Fluxor modules — one source of
// truth, host and device.
//
// Param-driven modules receive their artefact bytecode as a `str` module param
// (a config carries text, not raw bytes), hex-encoded. `hex_decode` turns that
// back into bytecode on device; `hex_encode` produces the string a config sets.

/// Decode ASCII hex into `out`. Returns the byte count, or `None` on odd length,
/// a non-hex digit, or insufficient output space.
pub fn hex_decode(hex: &[u8], out: &mut [u8]) -> Option<usize> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let n = hex.len() / 2;
    if n > out.len() {
        return None;
    }
    let mut i = 0;
    while i < n {
        let hi = nibble(hex[2 * i])?;
        let lo = nibble(hex[2 * i + 1])?;
        out[i] = (hi << 4) | lo;
        i += 1;
    }
    Some(n)
}

/// Encode `bytes` as lowercase ASCII hex into `out`. Returns the char count, or
/// `None` if `out` is too small.
pub fn hex_encode(bytes: &[u8], out: &mut [u8]) -> Option<usize> {
    let need = bytes.len() * 2;
    if need > out.len() {
        return None;
    }
    const D: &[u8; 16] = b"0123456789abcdef";
    let mut i = 0;
    while i < bytes.len() {
        out[2 * i] = D[(bytes[i] >> 4) as usize];
        out[2 * i + 1] = D[(bytes[i] & 0x0f) as usize];
        i += 1;
    }
    Some(need)
}

fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}
