// Bounded, no_std, no-alloc MySQL protocol core — SHA-1, the
// `mysql_native_password` challenge-response, packet framing, the handshake
// parse/build, and the auth state machine. `include!`d by the host crate (tests)
// and the `mysql` .fmod.
//
// MySQL adds an auth CLASS the other connectors don't have. `mysql_native_password`
// is a SHA-1 challenge-response: the server sends a random 20-byte scramble in its
// initial handshake, and the client proves knowledge of the password with
//   token = SHA1(pw) XOR SHA1( scramble ++ SHA1(SHA1(pw)) )
// — a reply-dependent computation over a server-chosen nonce (like SCRAM, but a
// different construction and its own binary packet framing). Neither the SHA-1
// nor the scramble dependence is expressible in a stateless codec.

// Crypto primitives are fluxor-SDK-owned. Consumers reach this file two ways and
// both work: an `include!` (the `.fmod` I/O pumps, which include! the SDK crypto
// alongside it) or a `#[path] mod` mount (`mysql_edge_anchor`), which gets its
// own copy of the primitives from the `sdk` module below. `cfg(not(...))` is not
// needed — a duplicate definition in the include! case is shadowed by the outer
// scope, so the mount is inert there.
#[allow(
    dead_code,
    unused_imports,
    reason = "shared SDK surface across modules: include! consumers already carry the crypto primitives in the outer scope, so this copy is dead there; the #[path] mod mount is the case that needs it"
)]
mod sdk {
    include!("../../target/fluxor/fluxor-abi/sdk/crypto/sha1.rs");
    include!("../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
}
#[allow(
    unused_imports,
    reason = "shadowed by the outer scope at include! sites; live only for the #[path] mod mount"
)]
use sdk::{sha1, sha256};

// SHA-1 comes from the fluxor SDK (`sdk/crypto/sha1.rs`), include!d by the
// consuming module before this file — primitives are SDK-owned.

/// The `mysql_native_password` auth token:
/// `SHA1(pw) XOR SHA1( scramble ++ SHA1(SHA1(pw)) )`. Empty password → empty token.
pub fn mysql_native_token(password: &[u8], scramble: &[u8; 20]) -> [u8; 20] {
    if password.is_empty() {
        return [0u8; 20];
    }
    let h1 = sha1(password);
    let h2 = sha1(&h1);
    let mut cat = [0u8; 40];
    cat[..20].copy_from_slice(scramble);
    cat[20..].copy_from_slice(&h2);
    let h3 = sha1(&cat);
    let mut token = [0u8; 20];
    let mut i = 0;
    while i < 20 {
        token[i] = h1[i] ^ h3[i];
        i += 1;
    }
    token
}

// ---- packet framing ---------------------------------------------------------

/// `(payload_start, payload_end, sequence_id, total)` of the first complete MySQL
/// packet (`[len:3 LE][seq:1][payload]`), or `None` if truncated.
pub fn mysql_packet(buf: &[u8]) -> Option<(usize, usize, u8, usize)> {
    if buf.len() < 4 {
        return None;
    }
    let len = (buf[0] as usize) | ((buf[1] as usize) << 8) | ((buf[2] as usize) << 16);
    let total = 4 + len;
    if buf.len() < total {
        return None;
    }
    Some((4, total, buf[3], total))
}

/// The kind of a server response packet by its first payload byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MyReply {
    Ok,         // 0x00
    Err,        // 0xff
    AuthSwitch, // 0xfe (server wants a different auth plugin)
    MoreData,   // 0x01 (AuthMoreData: caching_sha2 fast-auth result byte follows)
    Other,
}

pub fn mysql_reply_kind(payload: &[u8]) -> MyReply {
    match payload.first() {
        Some(0x00) => MyReply::Ok,
        Some(0xff) => MyReply::Err,
        Some(0xfe) => MyReply::AuthSwitch,
        Some(0x01) => MyReply::MoreData,
        _ => MyReply::Other,
    }
}

/// The auth plugin a handshake / AuthSwitch names. Both deployed MySQL auth
/// classes are supported: `mysql_native_password` (SHA-1 challenge-response)
/// and `caching_sha2_password`'s FAST path (SHA-256 scramble against the
/// server's credential cache). The sha2 FULL path (cold cache) requires the
/// password over a secure transport (TLS) or RSA — out of scope here, so it is
/// surfaced as a distinct failure, never silently downgraded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MyAuthPlugin {
    Native,
    CachingSha2,
    Other,
}

fn plugin_from_name(name: &[u8]) -> MyAuthPlugin {
    if name == b"mysql_native_password" {
        MyAuthPlugin::Native
    } else if name == b"caching_sha2_password" {
        MyAuthPlugin::CachingSha2
    } else {
        MyAuthPlugin::Other
    }
}

/// Parse the initial Handshake v10 packet payload: the 20-byte scramble
/// (auth-plugin-data part 1 ++ part 2) and the server's advertised auth
/// plugin, so the response can speak the server's dialect from the start.
/// `None` if not protocol 10 or the layout is truncated.
pub fn mysql_parse_handshake(payload: &[u8]) -> Option<([u8; 20], MyAuthPlugin)> {
    if payload.first() != Some(&10) {
        return None;
    }
    let mut i = 1;
    while i < payload.len() && payload[i] != 0 {
        i += 1; // server_version
    }
    i += 1; // NUL
    i += 4; // thread_id
    let part1 = payload.get(i..i + 8)?;
    i += 8;
    i += 1; // filler
    i += 2 + 1 + 2 + 2; // cap_lower, charset, status, cap_upper
    let apd_len = *payload.get(i)? as usize;
    i += 1;
    i += 10; // reserved
    let part2_len = if apd_len > 8 { apd_len - 8 } else { 13 };
    let avail = payload.len().saturating_sub(i);
    let part2 = payload.get(i..i + part2_len.min(avail))?;
    let mut scramble = [0u8; 20];
    scramble[..8].copy_from_slice(part1);
    let take = part2.len().min(12);
    scramble[8..8 + take].copy_from_slice(&part2[..take]);
    // The auth plugin name follows auth-plugin-data, NUL-terminated.
    i += part2_len.min(avail);
    let mut e = i;
    while e < payload.len() && payload[e] != 0 {
        e += 1;
    }
    let plugin = plugin_from_name(payload.get(i..e).unwrap_or(&[]));
    Some((scramble, plugin))
}

/// The `caching_sha2_password` FAST-path token:
/// `SHA256(pw) XOR SHA256( SHA256(SHA256(pw)) ++ nonce )`. Empty password →
/// empty token. (SHA-256 comes from the SDK — `sdk/crypto/sha256.rs`.)
pub fn mysql_sha2_token(password: &[u8], nonce: &[u8; 20]) -> [u8; 32] {
    let mut token = [0u8; 32];
    if password.is_empty() {
        return token;
    }
    let h1 = sha256(password);
    let h2 = sha256(&h1);
    let mut cat = [0u8; 52];
    cat[..32].copy_from_slice(&h2);
    cat[32..].copy_from_slice(nonce);
    let h3 = sha256(&cat);
    let mut i = 0;
    while i < 32 {
        token[i] = h1[i] ^ h3[i];
        i += 1;
    }
    token
}

/// Parse an AuthSwitchRequest (0xfe): the new plugin name and its 20-byte
/// nonce (the trailing NUL of the auth data is stripped).
pub fn mysql_parse_auth_switch(payload: &[u8]) -> Option<(MyAuthPlugin, [u8; 20])> {
    if payload.first() != Some(&0xfe) {
        return None;
    }
    let mut i = 1;
    let start = i;
    while i < payload.len() && payload[i] != 0 {
        i += 1;
    }
    let plugin = plugin_from_name(payload.get(start..i)?);
    i += 1; // NUL
    let data = payload.get(i..)?;
    let take = data.len().min(20);
    let mut nonce = [0u8; 20];
    nonce[..take].copy_from_slice(&data[..take]);
    Some((plugin, nonce))
}

/// Build the bare AuthSwitchResponse packet: just the token for `plugin` over
/// `nonce`, with the given sequence id. Returns the total length.
pub fn mysql_auth_switch_response(
    password: &[u8],
    plugin: MyAuthPlugin,
    nonce: &[u8; 20],
    seq: u8,
    out: &mut [u8],
) -> Option<usize> {
    let mut p = 4usize;
    match plugin {
        MyAuthPlugin::Native => {
            let token = mysql_native_token(password, nonce);
            myput(out, &mut p, &token)?;
        }
        MyAuthPlugin::CachingSha2 => {
            let token = mysql_sha2_token(password, nonce);
            myput(out, &mut p, &token)?;
        }
        MyAuthPlugin::Other => return None,
    }
    let plen = p - 4;
    out[0] = (plen & 0xff) as u8;
    out[1] = ((plen >> 8) & 0xff) as u8;
    out[2] = ((plen >> 16) & 0xff) as u8;
    out[3] = seq;
    Some(p)
}

fn myput(out: &mut [u8], pos: &mut usize, b: &[u8]) -> Option<()> {
    if *pos + b.len() > out.len() {
        return None;
    }
    out[*pos..*pos + b.len()].copy_from_slice(b);
    *pos += b.len();
    Some(())
}

/// Build the Handshake Response 41 packet speaking `plugin`'s dialect,
/// sequence id 1 (it always follows the seq-0 server handshake). Returns length.
pub fn mysql_handshake_response(
    user: &[u8],
    password: &[u8],
    database: &[u8],
    scramble: &[u8; 20],
    plugin: MyAuthPlugin,
    out: &mut [u8],
) -> Option<usize> {
    // CLIENT_LONG_PASSWORD | CLIENT_CONNECT_WITH_DB | CLIENT_PROTOCOL_41 |
    // CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
    const CAPS: u32 = 0x0000_0001 | 0x0000_0008 | 0x0000_0200 | 0x0000_8000 | 0x0008_0000;
    let mut p = 4usize; // reserve the 4-byte packet header
    myput(out, &mut p, &CAPS.to_le_bytes())?;
    myput(out, &mut p, &0x0100_0000u32.to_le_bytes())?; // max packet 16MB
    myput(out, &mut p, &[33])?; // charset utf8_general_ci
    myput(out, &mut p, &[0u8; 23])?; // filler
    myput(out, &mut p, user)?;
    myput(out, &mut p, &[0])?;
    match plugin {
        MyAuthPlugin::Native => {
            let token = mysql_native_token(password, scramble);
            myput(out, &mut p, &[token.len() as u8])?; // length-encoded auth response
            myput(out, &mut p, &token)?;
        }
        MyAuthPlugin::CachingSha2 => {
            let token = mysql_sha2_token(password, scramble);
            myput(out, &mut p, &[token.len() as u8])?;
            myput(out, &mut p, &token)?;
        }
        MyAuthPlugin::Other => return None,
    }
    myput(out, &mut p, database)?;
    myput(out, &mut p, &[0])?;
    match plugin {
        MyAuthPlugin::Native => myput(out, &mut p, b"mysql_native_password")?,
        MyAuthPlugin::CachingSha2 => myput(out, &mut p, b"caching_sha2_password")?,
        MyAuthPlugin::Other => return None,
    }
    myput(out, &mut p, &[0])?;
    let plen = p - 4;
    out[0] = (plen & 0xff) as u8;
    out[1] = ((plen >> 8) & 0xff) as u8;
    out[2] = ((plen >> 16) & 0xff) as u8;
    out[3] = 1; // sequence id
    Some(p)
}

// ---- auth state machine -----------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MyPhase {
    Disconnected = 0,
    Connecting = 1,
    /// Connected; awaiting the server's initial handshake packet.
    AwaitHandshake = 2,
    /// Handshake response sent; awaiting OK/ERR.
    AwaitAuthResult = 3,
    /// Authenticated.
    Ready = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MyEv {
    Start,
    Connected,
    GotHandshake,
    AuthOk,
    AuthErr,
    PeerClosed,
    NetError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MyAct {
    None,
    Connect,
    SendHandshakeResponse,
    Fail,
}

/// The MySQL native-auth state machine — pure and host-testable.
pub fn mysql_transition(phase: MyPhase, ev: MyEv) -> (MyAct, MyPhase) {
    use MyAct::*;
    use MyEv::*;
    use MyPhase::*;
    match (phase, ev) {
        (Disconnected, Start) => (Connect, Connecting),
        (Connecting, Connected) => (None, AwaitHandshake),
        (AwaitHandshake, GotHandshake) => (SendHandshakeResponse, AwaitAuthResult),
        (AwaitAuthResult, AuthOk) => (None, Ready),
        (AwaitAuthResult, AuthErr) => (Fail, Disconnected),
        (_, PeerClosed) | (_, NetError) => (Fail, Disconnected),
        _ => (None, phase),
    }
}
