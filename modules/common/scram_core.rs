// Bounded, no_std, no-alloc SCRAM-SHA-256 client core. `include!`d by the
// on-device `pg`/`mongo` .fmods — one source of truth. Carries the SCRAM-SHA-256
// proof computation (RFC 5802 / RFC 7677) plus its KDF chain
// (PBKDF2-HMAC-SHA-256) and a multi-part HMAC built on the SDK's hash.
//
// Crypto PRIMITIVES are owned by the fluxor SDK (`sdk/crypto/`): the including
// module must `include!` `sdk/crypto/sha256.rs` (for `sha256` / `Sha256`) and
// `sdk/crypto/b64.rs` (for `b64_encode`/`b64_decode`) BEFORE this file. Only the
// RFC 2104 padding glue (`hmac_parts`, over the SDK's streaming `Sha256`) and
// the SCRAM/PBKDF2 protocol math live here — no compression functions, no
// alphabet tables. One owner per primitive, protocol logic with the protocol.
//
// This is also the concrete proof of the codec-vs-protocol boundary: the client proof
// depends on the server's *random* salt and nonce and requires PBKDF2 (thousands
// of HMAC iterations) and an HMAC/XOR chain — reply-dependent, multi-round-trip
// computation a stateless encode/decode bytecode program cannot express. It is a
// state machine + crypto, so it must live in a compiled module.

// ---- HMAC-SHA-256 -----------------------------------------------------------

/// HMAC-SHA-256 over the concatenation of `parts` (no joining buffer needed).
fn hmac_parts(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&sha256(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    let mut i = 0;
    while i < 64 {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
        i += 1;
    }
    let mut inner = Sha256::new();
    inner.update(&ipad);
    for p in parts {
        inner.update(p);
    }
    let ih = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(&opad);
    outer.update(&ih);
    outer.finalize()
}

/// HMAC-SHA-256 over a single message.
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    hmac_parts(key, &[msg])
}

/// PBKDF2-HMAC-SHA-256 producing exactly one 32-byte block (SCRAM's
/// `SaltedPassword`, since HashLen == dkLen == 32).
pub fn pbkdf2_sha256_32(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut u = hmac_parts(password, &[salt, &1u32.to_be_bytes()]);
    let mut out = u;
    let mut c = 1;
    while c < iterations {
        u = hmac_sha256(password, &u);
        let mut i = 0;
        while i < 32 {
            out[i] ^= u[i];
            i += 1;
        }
        c += 1;
    }
    out
}

// ---- SCRAM-SHA-256 proof ----------------------------------------------------

/// The SCRAM-SHA-256 client computation. Given the account `password`, the
/// server's decoded `salt` and `iterations`, and the assembled `auth_message`
/// (`client-first-bare , server-first , client-final-without-proof`), fill the
/// `ClientProof` (to Base64 into the client-final `p=`) and the expected
/// `ServerSignature` (to verify the server-final `v=`).
///
/// SaltedPassword = PBKDF2(password, salt, i)
/// ClientKey      = HMAC(SaltedPassword, "Client Key")
/// StoredKey      = SHA256(ClientKey)
/// ClientProof    = ClientKey XOR HMAC(StoredKey, AuthMessage)
/// ServerSig      = HMAC(HMAC(SaltedPassword, "Server Key"), AuthMessage)
pub fn scram_client_proof(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    auth_message: &[u8],
    proof_out: &mut [u8; 32],
    server_sig_out: &mut [u8; 32],
) {
    let salted = pbkdf2_sha256_32(password, salt, iterations);
    let client_key = hmac_sha256(&salted, b"Client Key");
    let stored_key = sha256(&client_key);
    let client_sig = hmac_sha256(&stored_key, auth_message);
    let mut i = 0;
    while i < 32 {
        proof_out[i] = client_key[i] ^ client_sig[i];
        i += 1;
    }
    let server_key = hmac_sha256(&salted, b"Server Key");
    *server_sig_out = hmac_sha256(&server_key, auth_message);
}

// ---- SCRAM message orchestration --------------------------------------------

fn sc_push(out: &mut [u8], pos: &mut usize, bytes: &[u8]) -> Option<()> {
    if *pos + bytes.len() > out.len() {
        return None;
    }
    out[*pos..*pos + bytes.len()].copy_from_slice(bytes);
    *pos += bytes.len();
    Some(())
}

/// Build the SCRAM `client-first-message`: `n,,n=<user>,r=<nonce>`. The
/// `client-first-bare` (needed for the AuthMessage) is the slice after the 3-byte
/// `n,,` GS2 header, i.e. `out[3..len]`.
pub fn scram_client_first(user: &[u8], nonce: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut p = 0;
    sc_push(out, &mut p, b"n,,n=")?;
    sc_push(out, &mut p, user)?;
    sc_push(out, &mut p, b",r=")?;
    sc_push(out, &mut p, nonce)?;
    Some(p)
}

/// The parsed `server-first-message` fields.
pub struct ScramServerFirst<'a> {
    pub full_nonce: &'a [u8],
    pub salt_b64: &'a [u8],
    pub iterations: u32,
}

/// Parse a SCRAM `server-first-message` (`r=…,s=…,i=…`). `None` if a field is
/// missing or the iteration count is not a positive integer.
pub fn scram_parse_server_first(body: &[u8]) -> Option<ScramServerFirst<'_>> {
    let mut nonce: Option<&[u8]> = None;
    let mut salt: Option<&[u8]> = None;
    let mut iters: Option<u32> = None;
    for tok in body.split(|&c| c == b',') {
        if tok.len() >= 2 && tok[1] == b'=' {
            let v = &tok[2..];
            match tok[0] {
                b'r' => nonce = Some(v),
                b's' => salt = Some(v),
                b'i' => {
                    if v.is_empty() {
                        return None;
                    }
                    let mut n: u32 = 0;
                    for &c in v {
                        if !c.is_ascii_digit() {
                            return None;
                        }
                        n = n.wrapping_mul(10).wrapping_add((c - b'0') as u32);
                    }
                    iters = Some(n);
                }
                _ => {}
            }
        }
    }
    Some(ScramServerFirst {
        full_nonce: nonce?,
        salt_b64: salt?,
        iterations: iters?,
    })
}

/// Build the SCRAM `client-final-message` (`c=biws,r=<nonce>,p=<proof>`) from the
/// `client-first-bare` and the raw `server-first-message`, and write the expected
/// `ServerSignature` for verifying the server-final. Returns the client-final
/// length, or `None` on a malformed server-first or insufficient scratch/output.
pub fn scram_build_client_final(
    client_first_bare: &[u8],
    server_first: &[u8],
    password: &[u8],
    out: &mut [u8],
    server_sig_out: &mut [u8; 32],
) -> Option<usize> {
    let sf = scram_parse_server_first(server_first)?;
    let mut salt = [0u8; 64];
    let sn = b64_decode(sf.salt_b64, &mut salt)?;

    // client-final-without-proof = "c=biws,r=<full_nonce>"  (biws = base64("n,,")).
    let mut cfwp = [0u8; 160];
    let mut c = 0;
    sc_push(&mut cfwp, &mut c, b"c=biws,r=")?;
    sc_push(&mut cfwp, &mut c, sf.full_nonce)?;

    // AuthMessage = client-first-bare , server-first , client-final-without-proof.
    let mut am = [0u8; 512];
    let mut a = 0;
    sc_push(&mut am, &mut a, client_first_bare)?;
    sc_push(&mut am, &mut a, b",")?;
    sc_push(&mut am, &mut a, server_first)?;
    sc_push(&mut am, &mut a, b",")?;
    sc_push(&mut am, &mut a, &cfwp[..c])?;

    let mut proof = [0u8; 32];
    scram_client_proof(
        password,
        &salt[..sn],
        sf.iterations,
        &am[..a],
        &mut proof,
        server_sig_out,
    );
    let mut pb64 = [0u8; 48];
    let pn = b64_encode(&proof, &mut pb64)?;

    let mut o = 0;
    sc_push(out, &mut o, &cfwp[..c])?;
    sc_push(out, &mut o, b",p=")?;
    sc_push(out, &mut o, &pb64[..pn])?;
    Some(o)
}
