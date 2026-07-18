// Bounded, no_std, no-alloc Postgres v3 wire framing — StartupMessage, the SASL
// authentication messages, simple Query, and message parsing. `include!`d by the
// host crate (tests) and the `pg` .fmod. The SCRAM computation lives in
// scram_core.rs; the connection state machine lives in the module. Message
// building + parsing only here, so it is pure and host-testable.

fn pg_put(out: &mut [u8], pos: &mut usize, bytes: &[u8]) -> Option<()> {
    if *pos + bytes.len() > out.len() {
        return None;
    }
    out[*pos..*pos + bytes.len()].copy_from_slice(bytes);
    *pos += bytes.len();
    Some(())
}

/// Build a StartupMessage: `[len:i32 BE][0x00030000][user\0<u>\0database\0<d>\0\0]`.
/// No type byte (only the startup packet lacks one). Returns the length.
pub fn pg_startup(user: &[u8], database: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut o = 4; // reserve the length prefix
    pg_put(out, &mut o, &[0x00, 0x03, 0x00, 0x00])?; // protocol 3.0
    pg_put(out, &mut o, b"user\0")?;
    pg_put(out, &mut o, user)?;
    pg_put(out, &mut o, b"\0")?;
    pg_put(out, &mut o, b"database\0")?;
    pg_put(out, &mut o, database)?;
    pg_put(out, &mut o, b"\0")?;
    pg_put(out, &mut o, b"\0")?; // terminating empty key
    out[0..4].copy_from_slice(&(o as i32).to_be_bytes());
    Some(o)
}

/// Build a type-tagged message `[tag][len:i32 BE = 4 + body][body]`.
fn pg_tagged(tag: u8, body: &[u8], out: &mut [u8]) -> Option<usize> {
    let len = 4 + body.len();
    if 1 + len > out.len() {
        return None;
    }
    out[0] = tag;
    out[1..5].copy_from_slice(&(len as i32).to_be_bytes());
    out[5..5 + body.len()].copy_from_slice(body);
    Some(1 + len)
}

/// SASLInitialResponse (`p`): `mechanism\0 [initial-len:i32 BE] client-first`.
pub fn pg_sasl_initial(mechanism: &[u8], client_first: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut body = [0u8; 320];
    let mut b = 0;
    pg_put(&mut body, &mut b, mechanism)?;
    pg_put(&mut body, &mut b, b"\0")?;
    pg_put(
        &mut body,
        &mut b,
        &(client_first.len() as i32).to_be_bytes(),
    )?;
    pg_put(&mut body, &mut b, client_first)?;
    pg_tagged(b'p', &body[..b], out)
}

/// SASLResponse (`p`): the client-final-message as the whole body.
pub fn pg_sasl_response(client_final: &[u8], out: &mut [u8]) -> Option<usize> {
    pg_tagged(b'p', client_final, out)
}

/// Simple Query (`Q`): `<sql>\0`.
pub fn pg_query(sql: &[u8], out: &mut [u8]) -> Option<usize> {
    let len = 4 + sql.len() + 1;
    if 1 + len > out.len() {
        return None;
    }
    out[0] = b'Q';
    out[1..5].copy_from_slice(&(len as i32).to_be_bytes());
    out[5..5 + sql.len()].copy_from_slice(sql);
    out[5 + sql.len()] = 0;
    Some(1 + len)
}

/// One framed server message.
pub struct PgMsg {
    pub tag: u8,
    pub body_start: usize,
    pub body_end: usize,
    pub total: usize,
}

/// Frame the first complete type-tagged message in `buf` (`[tag][len:i32][body]`),
/// or `None` if fewer than `total` bytes are present (need more) or the length is
/// malformed. Never panics.
pub fn pg_next_msg(buf: &[u8]) -> Option<PgMsg> {
    if buf.len() < 5 {
        return None;
    }
    let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    if len < 4 {
        return None;
    }
    let total = 1 + len as usize;
    if buf.len() < total {
        return None;
    }
    Some(PgMsg {
        tag: buf[0],
        body_start: 5,
        body_end: total,
        total,
    })
}

/// The authentication sub-type of an `R` message body (`[auth_type:i32 BE][data]`).
/// 0 = Ok, 10 = SASL, 11 = SASLContinue, 12 = SASLFinal.
pub fn pg_auth_type(body: &[u8]) -> Option<i32> {
    if body.len() < 4 {
        return None;
    }
    Some(i32::from_be_bytes([body[0], body[1], body[2], body[3]]))
}

/// True if `R`-body advertises SCRAM-SHA-256 among its SASL mechanisms.
pub fn pg_sasl_has_scram_sha256(body: &[u8]) -> bool {
    // After the auth_type, a list of NUL-terminated mechanism names.
    let mut i = 4;
    while i < body.len() {
        let start = i;
        while i < body.len() && body[i] != 0 {
            i += 1;
        }
        if &body[start..i] == b"SCRAM-SHA-256" {
            return true;
        }
        i += 1; // skip the NUL
        if start == i - 1 {
            break; // empty name terminates the list
        }
    }
    false
}

/// The ReadyForQuery (`Z`) transaction-status byte (`I`/`T`/`E`).
pub fn pg_ready_status(body: &[u8]) -> Option<u8> {
    body.first().copied()
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
