// Reply accumulation for the pg_client connector — the pure halves of the
// first-column, newline-joined result contract. `include!`d by the `pg`
// .fmod and `#[path]`-mounted by `tests/harness/tests/contract_pg_reply.rs`.
// Split from `pg_core.rs` (the wire framing) so each consumer mounts exactly
// the layer it uses.

/// Append one row's column value to a newline-joined reply accumulation in
/// `buf` (current content length `len`; `have_prior` = a row is already
/// present, so a separator is owed). Returns the new length, or `None` when
/// the row would overflow `buf` — the caller MUST then refuse the whole
/// reply rather than truncate: a prefix of a result set is indistinguishable
/// from a complete one downstream.
pub fn pg_reply_append(buf: &mut [u8], len: usize, have_prior: bool, col: &[u8]) -> Option<usize> {
    let sep = usize::from(have_prior);
    if len + sep + col.len() > buf.len() {
        return None;
    }
    if sep == 1 {
        buf[len] = b'\n';
    }
    buf[len + sep..len + sep + col.len()].copy_from_slice(col);
    Some(len + sep + col.len())
}

/// The first column value of a DataRow (`D`) body
/// (`[ncols:i16 BE]` then per column `[len:i32 BE (-1 = NULL)][bytes]`), as a
/// byte range into `body`. `None` if absent, NULL, or truncated.
pub fn pg_datarow_col0(body: &[u8]) -> Option<(usize, usize)> {
    if body.len() < 2 {
        return None;
    }
    let ncols = i16::from_be_bytes([body[0], body[1]]);
    if ncols < 1 || body.len() < 6 {
        return None;
    }
    let collen = i32::from_be_bytes([body[2], body[3], body[4], body[5]]);
    if collen < 0 {
        return None; // NULL column
    }
    let start = 6;
    let end = start + collen as usize;
    if end > body.len() {
        return None;
    }
    Some((start, end))
}
