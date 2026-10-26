//! route_catalogue — Wormhole's route and endpoint catalogues in Lattice:
//! staged publication, sealed manifests, compare-and-swap activation,
//! bounded garbage collection and revision-pinned paged snapshots; and
//! beside the generations, declarations and gap-free status streams.
//!
//! Lattice stores and distributes the records; it does not interpret
//! them. A route or endpoint record is an opaque WHR1 envelope checked
//! structurally only ([`validate_envelope`]): the magic, a known kind, no
//! reserved bit set, lengths that agree, the SHA-256 content digest, and
//! the 4,096-byte envelope ceiling. Everything Lattice writes itself — the
//! endpoint-set manifest, the sealed route-generation manifest, the active
//! pointer, the staging marker — is a small fixed record of its own.
//!
//! # Key layout
//!
//! Every component is fixed-width binary, so a key parses without
//! delimiters and nothing is ever truncated to fit. The longest key is 73
//! bytes, under the store provider's 255-byte key and the 256-byte watch
//! key (`watch_hub::WATCH_KEY_MAX`), and a whole service's catalogue sits
//! under one 37-byte prefix, so one watch on it covers the catalogue.
//!
//! A generation carries up to [`ROUTES_MAX`] routes (a service's ports by
//! listener family), each in a directory of its own named by its index
//! `<r u8>` (`0..routes`, contiguous): its route record, its endpoint
//! records and its endpoint-set manifest.
//!
//! ```text
//! rc/<tenant 16>/<service 16>/gen/<generation u64 BE>/pol/<kind u16 BE><id 16>
//!                                                                         72 bytes  a policy of the generation (placed WHR1)
//! rc/<tenant 16>/<service 16>/gen/<generation u64 BE>/r/<r u8>/ep/<endpoint 16>
//!                                                                         73 bytes  route r's endpoint record (WHR1)
//! rc/<tenant 16>/<service 16>/gen/<generation u64 BE>/r/<r u8>/manifest  62 bytes  route r's endpoint-set manifest (LCM1)
//! rc/<tenant 16>/<service 16>/gen/<generation u64 BE>/r/<r u8>/route     59 bytes  route r's route record (WHR1)
//! rc/<tenant 16>/<service 16>/gen/<generation u64 BE>/sealed             56 bytes  route-generation manifest (LCS1)
//! rc/<tenant 16>/<service 16>/staging/<generation u64 BE>                53 bytes  staging marker (LCG1: when and by whom)
//! rc/<tenant 16>/<service 16>/active                                     43 bytes  the active-generation pointer
//! rc/<tenant 16>/<service 16>/decl/<kind u16 BE><id 16>                  60 bytes  a declaration (placed WHR1)
//! rc/<tenant 16>/<service 16>/op/<operation 16>/<seq u64 BE>             65 bytes  a status (placed WHR1)
//! rc/<tenant 16>/<service 16>/op/<operation 16>/head                     61 bytes  the stream's head (LSH1)
//! ```
//!
//! The generation is big-endian so generations list in order. Within one
//! generation the records sort `pol/…`, then each route's directory in
//! index order — `ep/…`, `manifest`, `route` — then `sealed`. A policy's id
//! is its content address ([`content_address`]), so a generation's policies
//! are immutable and a route names each by the address it was stored
//! under; one policy serves every route of the generation that names it.
//!
//! A generation with no route at all is the withdrawn form: a service
//! that has nothing to publish (its last port gone, or no longer handed to
//! this catalogue) is published as the next generation, sealed over no
//! records (`routes`, `endpoints` and `records` 0, the all-zero chain), and
//! activated like any other. Generations never restart, so a service that
//! returns continues from it.
//!
//! # Placed records
//!
//! A policy, a declaration and a status are WHR1 envelopes too, checked by
//! [`validate_record`]: the same structure, and any kind but the reserved
//! zero and the three a generation's fixed places hold. Their kinds are
//! the publisher's (Wormhole's dial, attachment, chain, egress, address,
//! failure-domain and region families among them); a key names its
//! record's kind and a reader checks the two agree. Lattice reads the
//! envelope's kind and generation, and never a payload.
//!
//! # Records Lattice writes
//!
//! ```text
//! LCM1 manifest : "LCM1" | count u32 LE | digest [32]                    a route's endpoint-set manifest
//! LCS1 sealed   : "LCS1" | routes u32 LE | endpoints u32 LE | records u32 LE | digest [32]
//!                                                                        route-generation manifest
//! active        : generation u64 LE | sealed_digest [32] | revision u64 LE
//! LCG1 marker   : "LCG1" | started_ms u64 LE | publisher [16]
//! LSH1 head     : "LSH1" | seq u64 LE | flags u32 LE (bit 0 closed) | chain [32]
//! ```
//!
//! A manifest digest is a streaming SHA-256 chain over lexicographically
//! ordered `(record_key, record_content_digest)` pairs
//! ([`ManifestDigest`]): `chain = SHA-256(chain ‖ key_len u16 LE ‖ key ‖
//! digest)` from an all-zero chain, where a record's content digest is
//! the SHA-256 of its stored bytes. A chain folds one record at a time
//! and is 32 bytes of state, so it streams through pages and travels in a
//! snapshot's continuation. A route's endpoint-set manifest covers that
//! route's endpoint records; the sealed manifest covers every record of the
//! generation but itself (policies, and each route's endpoints, manifest
//! and route) and counts its routes, its endpoints and its records. A
//! status head's chain is the same chain over the stream's statuses.
//!
//! # Publication
//!
//! [`Publisher`] drives one generation through the store in order: the
//! staging marker, created only if absent (`KV_OP_CAS` on witness 0), so
//! a generation number belongs to one publisher and a generation already
//! begun — the active one included — is never written over; each route's
//! endpoint records and route record, and the generation's policies, under
//! the staging generation; then the seal — for each route its endpoints
//! are read back in key order (every one its manifest will count exists,
//! each a valid envelope) and its manifest written, then every record of
//! the generation read back and digested (each route present, with its
//! manifest) and the sealed manifest written; then the activation — the
//! active pointer
//! read, its generation compared with the expected predecessor, and the
//! pointer advanced by `KV_OP_CAS` on the record's `mod_revision` (a
//! create when there is none). Two publishers racing on one predecessor
//! cannot both win: the second CAS fails and its generation stays staged
//! and invisible. The revision watchers see is the CAS's. A generation
//! that fails or is abandoned is never named by the pointer and is
//! reclaimed by [`Gc`].
//!
//! A record is written by one `KV_OP_PUT` through the router, whose
//! request ceiling ([`KV_BODY_MAX`]) is below a put of a 4,096-byte
//! envelope: a record whose put the router cannot carry is refused
//! [`EnvelopeError::TooLarge`] before it is sent, never left unanswered.
//!
//! # Snapshot-then-watch
//!
//! [`Snapshot`] pages every record under a service at one revision
//! (`KV_OP_RANGE_META` with `at_rev`), each page carrying an opaque
//! continuation bound to that revision and service (the revision, the
//! count and digest chain so far, the last key) and the last page the
//! manifest `(count, digest)`. A page ceiling never truncates: a page
//! that stops — the store's limit, or the caller's page full — says
//! `more` and carries where to go on. The consumer's side — install the
//! snapshot whole, then consume changes strictly after its revision — is
//! `route_catalogue_reader.rs`.
//!
//! # Declarations and status streams
//!
//! [`Declarer`] keeps one record per `(kind, id)`, replaced only by a
//! later `record_generation` through `KV_OP_CAS` on the held record's
//! witness — a region's or the global manifest, an address or
//! failure-domain policy — so a watcher sees each declaration in order
//! and never one going back. [`StatusLog`] appends an operation's statuses
//! gap free: each created under the next number, then the head advanced
//! over it by CAS, so the published statuses are exactly `1..=seq` and a
//! reader verifies them by the head's chain; the last closes the stream,
//! and [`Reclaim`] withdraws a closed one head first.
//!
//! # Garbage collection
//!
//! [`Gc`] lists the staging markers and removes a generation that is
//! neither active nor the active's predecessor when it is older than the
//! active by more than `keep` generations, or its marker is older than
//! `retention_ms`; a generation's records are deleted a bounded batch at
//! a time, its marker last, so an interrupted pass leaves nothing the next
//! cannot finish. An active pointer that does not decode stops the pass:
//! without it no generation can be told apart from the live one.
//!
//! Every machine is a KV client: it hands one request body at a time to
//! [`KvIo::request`] and is given the answer by `on_response`; the module
//! shell (`modules/app/route_catalogue_front`) wraps that in
//! `MSG_KV_REQUEST` frames, the host suite applies it to a store at once.
//! No allocation, no panic path, no float, SHA-256 from the SDK.

#![allow(
    dead_code,
    reason = "shared via #[path] into the front module and the host suite; each uses a subset of the surface"
)]

#[path = "types.rs"]
pub mod types;

/// SHA-256 from the SDK.
#[allow(dead_code, reason = "the SDK hasher is mounted whole")]
pub mod sha {
    include!("../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
}

use types::{
    KV_OP_CAS, KV_OP_DELETE, KV_OP_PUT, KV_OP_RANGE_META, KV_OP_SCAN_VERSIONS,
    KV_RESULT_CAS_FAILED, KV_RESULT_COMPACTED, KV_RESULT_FUTURE_REV, KV_RESULT_INTEGER,
    KV_RESULT_KVS, KV_RESULT_OK, KV_RESULT_VERSIONS,
};

// ── The envelope (Wormhole's, checked here, never interpreted) ────────

pub const ENVELOPE_MAGIC: [u8; 4] = *b"WHR1";
/// `magic | kind u16 | flags u16 | record_len u32 | record_generation u64
/// | operation_id [16]`.
pub const ENVELOPE_HEADER: usize = 4 + 2 + 2 + 4 + 8 + 16;
pub const DIGEST_LEN: usize = 32;
/// The smallest envelope: a header and a digest around an empty payload.
pub const ENVELOPE_MIN: usize = ENVELOPE_HEADER + DIGEST_LEN;
/// The envelope ceiling: a record larger is refused, never chunked. A
/// record must also fit one routed put ([`KV_BODY_MAX`]).
pub const RECORD_MAX: usize = 4096;
pub const KIND_ROUTE: u16 = 1;
pub const KIND_ENDPOINT: u16 = 2;
pub const KIND_ENDPOINT_MANIFEST: u16 = 3;
/// Routes one generation carries: a service's eight ports by two
/// listener families.
pub const ROUTES_MAX: usize = 16;

/// What a structural check of an envelope found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Envelope {
    pub kind: u16,
    pub generation: u64,
    pub payload_len: usize,
    pub content_digest: [u8; DIGEST_LEN],
}

/// Why an envelope was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvelopeError {
    /// Over [`RECORD_MAX`].
    TooLarge(usize),
    /// Shorter than a header and a digest.
    Short,
    Magic,
    /// A kind this catalogue does not store.
    Kind(u16),
    /// A reserved bit set (every flag bit is reserved).
    ReservedBits(u16),
    /// `record_len` and the bytes disagree (trailing bytes or a payload
    /// cut short).
    Length,
    /// The content digest does not cover the header and payload.
    Digest,
}

/// Check an envelope structurally: magic, kind, reserved bits, length
/// agreement, digest, ceiling. The payload is not read. The kind is one a
/// generation's fixed places hold: route, endpoint or endpoint manifest.
pub fn validate_envelope(v: &[u8]) -> Result<Envelope, EnvelopeError> {
    check_envelope(v, |k| {
        matches!(k, KIND_ROUTE | KIND_ENDPOINT | KIND_ENDPOINT_MANIFEST)
    })
}

/// Check a placed record — a generation's policy, a declaration, a status
/// — structurally, as [`validate_envelope`] does, admitting any kind but
/// the reserved zero and the three a generation's fixed places hold. The
/// kind is the publisher's; its key names it, and a reader checks the two
/// agree. The payload is not read.
pub fn validate_record(v: &[u8]) -> Result<Envelope, EnvelopeError> {
    check_envelope(v, |k| k > KIND_ENDPOINT_MANIFEST)
}

/// A placed record's content address: the first 16 bytes of the SHA-256
/// of its payload, the identity a generation's policy is keyed by (and a
/// route names it by). `None` for bytes that are not an envelope.
pub fn content_address(v: &[u8]) -> Option<[u8; ID_LEN]> {
    let len = u32::from_le_bytes(v.get(8..12)?.try_into().ok()?) as usize;
    let payload = v.get(ENVELOPE_HEADER..ENVELOPE_HEADER.checked_add(len)?)?;
    let d = sha::sha256(payload);
    let mut id = [0u8; ID_LEN];
    id.copy_from_slice(&d[..ID_LEN]);
    Some(id)
}

fn check_envelope(v: &[u8], admit: impl Fn(u16) -> bool) -> Result<Envelope, EnvelopeError> {
    if v.len() > RECORD_MAX {
        return Err(EnvelopeError::TooLarge(v.len()));
    }
    if v.len() < ENVELOPE_MIN {
        return Err(EnvelopeError::Short);
    }
    if v[0..4] != ENVELOPE_MAGIC {
        return Err(EnvelopeError::Magic);
    }
    let kind = u16::from_le_bytes([v[4], v[5]]);
    if !admit(kind) {
        return Err(EnvelopeError::Kind(kind));
    }
    let flags = u16::from_le_bytes([v[6], v[7]]);
    if flags != 0 {
        return Err(EnvelopeError::ReservedBits(flags));
    }
    let record_len = u32::from_le_bytes([v[8], v[9], v[10], v[11]]) as usize;
    // No overflow: both sides are bounded by RECORD_MAX and u32.
    if ENVELOPE_HEADER + record_len + DIGEST_LEN != v.len() {
        return Err(EnvelopeError::Length);
    }
    let covered = &v[..ENVELOPE_HEADER + record_len];
    let want = &v[ENVELOPE_HEADER + record_len..];
    let got = sha::sha256(covered);
    if got[..] != *want {
        return Err(EnvelopeError::Digest);
    }
    let mut generation = [0u8; 8];
    generation.copy_from_slice(&v[12..20]);
    let mut content_digest = [0u8; DIGEST_LEN];
    content_digest.copy_from_slice(want);
    Ok(Envelope {
        kind,
        generation: u64::from_le_bytes(generation),
        payload_len: record_len,
        content_digest,
    })
}

/// Build an envelope (encoders write reserved bits as zero) — for
/// publishers and tests; Lattice never builds one for the store.
pub fn encode_envelope(
    kind: u16,
    generation: u64,
    operation_id: &[u8; 16],
    payload: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let n = ENVELOPE_HEADER + payload.len() + DIGEST_LEN;
    if n > RECORD_MAX {
        return None;
    }
    let o = out.get_mut(..n)?;
    o[0..4].copy_from_slice(&ENVELOPE_MAGIC);
    o[4..6].copy_from_slice(&kind.to_le_bytes());
    o[6..8].copy_from_slice(&0u16.to_le_bytes());
    o[8..12].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    o[12..20].copy_from_slice(&generation.to_le_bytes());
    o[20..36].copy_from_slice(operation_id);
    o[36..36 + payload.len()].copy_from_slice(payload);
    let d = sha::sha256(&o[..36 + payload.len()]);
    o[36 + payload.len()..].copy_from_slice(&d);
    Some(n)
}

// ── Keys ───────────────────────────────────────────────────────────────

pub const ID_LEN: usize = 16;
/// `rc/<tenant>/<service>/`: the service prefix a watch covers.
pub const SERVICE_PREFIX_LEN: usize = 3 + ID_LEN + 1 + ID_LEN + 1;
/// `…/gen/<generation>/`: a generation's prefix.
pub const GEN_PREFIX_LEN: usize = SERVICE_PREFIX_LEN + 4 + 8 + 1;
/// `…/gen/<generation>/r/<r u8>/`: a route's directory.
pub const ROUTE_DIR_LEN: usize = GEN_PREFIX_LEN + 2 + 1 + 1;
pub const EP_KEY_LEN: usize = ROUTE_DIR_LEN + 3 + ID_LEN;
pub const MANIFEST_KEY_LEN: usize = ROUTE_DIR_LEN + 8;
pub const ROUTE_KEY_LEN: usize = ROUTE_DIR_LEN + 5;
pub const SEALED_KEY_LEN: usize = GEN_PREFIX_LEN + 6;
pub const STAGING_KEY_LEN: usize = SERVICE_PREFIX_LEN + 8 + 8;
pub const ACTIVE_KEY_LEN: usize = SERVICE_PREFIX_LEN + 6;
/// `…/gen/<generation>/pol/<kind u16 BE><content address 16>`.
pub const POLICY_KEY_LEN: usize = GEN_PREFIX_LEN + 4 + 2 + ID_LEN;
/// `…/decl/<kind u16 BE><id 16>`.
pub const DECLARATION_KEY_LEN: usize = SERVICE_PREFIX_LEN + 5 + 2 + ID_LEN;
/// `…/op/<operation 16>/<seq u64 BE>` and `…/op/<operation 16>/head`.
pub const STATUS_KEY_LEN: usize = SERVICE_PREFIX_LEN + 3 + ID_LEN + 1 + 8;
pub const STATUS_HEAD_KEY_LEN: usize = SERVICE_PREFIX_LEN + 3 + ID_LEN + 1 + 4;
/// The longest catalogue key.
pub const KEY_MAX: usize = EP_KEY_LEN;
const _: () = assert!(KEY_MAX == 73);
const _: () = assert!(
    POLICY_KEY_LEN <= KEY_MAX
        && DECLARATION_KEY_LEN <= KEY_MAX
        && STATUS_KEY_LEN <= KEY_MAX
        && STATUS_HEAD_KEY_LEN <= KEY_MAX
);
// Key lengths travel in a byte (a digest chain's last key, a continuation).
const _: () = assert!(KEY_MAX <= u8::MAX as usize);

/// A catalogue service: the tenant and service identities.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Service {
    pub tenant: [u8; ID_LEN],
    pub service: [u8; ID_LEN],
}

/// Key builders. Each writes into `out` and answers the length; `out`
/// must hold [`KEY_MAX`] bytes.
pub mod key {
    use super::{
        Service, ACTIVE_KEY_LEN, GEN_PREFIX_LEN, ID_LEN, ROUTE_DIR_LEN, SERVICE_PREFIX_LEN,
    };

    fn service_prefix(s: &Service, out: &mut [u8]) -> usize {
        out[0..3].copy_from_slice(b"rc/");
        out[3..3 + ID_LEN].copy_from_slice(&s.tenant);
        out[3 + ID_LEN] = b'/';
        out[4 + ID_LEN..4 + 2 * ID_LEN].copy_from_slice(&s.service);
        out[4 + 2 * ID_LEN] = b'/';
        SERVICE_PREFIX_LEN
    }

    /// `rc/<tenant>/<service>/`.
    pub fn service(s: &Service, out: &mut [u8]) -> usize {
        service_prefix(s, out)
    }

    /// `…/gen/<generation>/`.
    pub fn generation(s: &Service, g: u64, out: &mut [u8]) -> usize {
        let n = service_prefix(s, out);
        out[n..n + 4].copy_from_slice(b"gen/");
        out[n + 4..n + 12].copy_from_slice(&g.to_be_bytes());
        out[n + 12] = b'/';
        GEN_PREFIX_LEN
    }

    /// `…/gen/<generation>/r/`: every route of the generation.
    pub fn routes(s: &Service, g: u64, out: &mut [u8]) -> usize {
        under(s, g, b"r/", out)
    }

    /// `…/gen/<generation>/r/<r>/`: route `r`'s directory.
    pub fn route_dir(s: &Service, g: u64, r: u8, out: &mut [u8]) -> usize {
        let n = routes(s, g, out);
        out[n] = r;
        out[n + 1] = b'/';
        ROUTE_DIR_LEN
    }

    /// `…/gen/<generation>/r/<r>/ep/`.
    pub fn endpoints(s: &Service, g: u64, r: u8, out: &mut [u8]) -> usize {
        let n = route_dir(s, g, r, out);
        out[n..n + 3].copy_from_slice(b"ep/");
        n + 3
    }

    /// `…/gen/<generation>/r/<r>/ep/<endpoint>`.
    pub fn endpoint(s: &Service, g: u64, r: u8, ep: &[u8; ID_LEN], out: &mut [u8]) -> usize {
        let n = endpoints(s, g, r, out);
        out[n..n + ID_LEN].copy_from_slice(ep);
        n + ID_LEN
    }

    fn under(s: &Service, g: u64, name: &[u8], out: &mut [u8]) -> usize {
        let n = generation(s, g, out);
        out[n..n + name.len()].copy_from_slice(name);
        n + name.len()
    }

    fn in_route(s: &Service, g: u64, r: u8, name: &[u8], out: &mut [u8]) -> usize {
        let n = route_dir(s, g, r, out);
        out[n..n + name.len()].copy_from_slice(name);
        n + name.len()
    }

    /// `…/gen/<generation>/r/<r>/manifest`.
    pub fn manifest(s: &Service, g: u64, r: u8, out: &mut [u8]) -> usize {
        in_route(s, g, r, b"manifest", out)
    }

    /// `…/gen/<generation>/r/<r>/route`.
    pub fn route(s: &Service, g: u64, r: u8, out: &mut [u8]) -> usize {
        in_route(s, g, r, b"route", out)
    }

    pub fn sealed(s: &Service, g: u64, out: &mut [u8]) -> usize {
        under(s, g, b"sealed", out)
    }

    /// `…/staging/` and `…/staging/<generation>`.
    pub fn stagings(s: &Service, out: &mut [u8]) -> usize {
        let n = service_prefix(s, out);
        out[n..n + 8].copy_from_slice(b"staging/");
        n + 8
    }

    pub fn staging(s: &Service, g: u64, out: &mut [u8]) -> usize {
        let n = stagings(s, out);
        out[n..n + 8].copy_from_slice(&g.to_be_bytes());
        n + 8
    }

    /// `…/active`.
    pub fn active(s: &Service, out: &mut [u8]) -> usize {
        let n = service_prefix(s, out);
        out[n..n + 6].copy_from_slice(b"active");
        ACTIVE_KEY_LEN
    }

    /// `…/gen/<generation>/pol/`.
    pub fn policies(s: &Service, g: u64, out: &mut [u8]) -> usize {
        under(s, g, b"pol/", out)
    }

    /// `…/gen/<generation>/pol/<kind><id>`: a policy of generation `g`,
    /// keyed by its kind and content address.
    pub fn policy(s: &Service, g: u64, kind: u16, id: &[u8; ID_LEN], out: &mut [u8]) -> usize {
        let n = policies(s, g, out);
        out[n..n + 2].copy_from_slice(&kind.to_be_bytes());
        out[n + 2..n + 2 + ID_LEN].copy_from_slice(id);
        n + 2 + ID_LEN
    }

    /// `…/decl/`.
    pub fn declarations(s: &Service, out: &mut [u8]) -> usize {
        let n = service_prefix(s, out);
        out[n..n + 5].copy_from_slice(b"decl/");
        n + 5
    }

    /// `…/decl/<kind><id>`: the declaration of kind `kind` the publisher
    /// names `id`.
    pub fn declaration(s: &Service, kind: u16, id: &[u8; ID_LEN], out: &mut [u8]) -> usize {
        let n = declarations(s, out);
        out[n..n + 2].copy_from_slice(&kind.to_be_bytes());
        out[n + 2..n + 2 + ID_LEN].copy_from_slice(id);
        n + 2 + ID_LEN
    }

    /// `…/op/<operation>/`: one operation's status stream.
    pub fn statuses(s: &Service, operation: &[u8; ID_LEN], out: &mut [u8]) -> usize {
        let n = service_prefix(s, out);
        out[n..n + 3].copy_from_slice(b"op/");
        out[n + 3..n + 3 + ID_LEN].copy_from_slice(operation);
        out[n + 3 + ID_LEN] = b'/';
        n + 4 + ID_LEN
    }

    /// `…/op/<operation>/<seq>`: the stream's `seq`th status (from 1).
    pub fn status(s: &Service, operation: &[u8; ID_LEN], seq: u64, out: &mut [u8]) -> usize {
        let n = statuses(s, operation, out);
        out[n..n + 8].copy_from_slice(&seq.to_be_bytes());
        n + 8
    }

    /// `…/op/<operation>/head`: how much of the stream is published.
    pub fn status_head(s: &Service, operation: &[u8; ID_LEN], out: &mut [u8]) -> usize {
        let n = statuses(s, operation, out);
        out[n..n + 4].copy_from_slice(b"head");
        n + 4
    }
}

/// What a key under a service names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Parsed {
    /// Route `route`'s endpoint record.
    Endpoint {
        generation: u64,
        route: u8,
        endpoint: [u8; ID_LEN],
    },
    /// Route `route`'s endpoint-set manifest.
    Manifest {
        generation: u64,
        route: u8,
    },
    /// Route `route`'s route record.
    Route {
        generation: u64,
        route: u8,
    },
    Sealed {
        generation: u64,
    },
    Staging {
        generation: u64,
    },
    Active,
    /// A policy of a generation: its kind and content address.
    Policy {
        generation: u64,
        kind: u16,
        id: [u8; ID_LEN],
    },
    Declaration {
        kind: u16,
        id: [u8; ID_LEN],
    },
    Status {
        operation: [u8; ID_LEN],
        seq: u64,
    },
    StatusHead {
        operation: [u8; ID_LEN],
    },
}

/// A `<kind u16 BE><id 16>` key component.
fn kind_id(b: &[u8]) -> Option<(u16, [u8; ID_LEN])> {
    if b.len() != 2 + ID_LEN {
        return None;
    }
    let mut id = [0u8; ID_LEN];
    id.copy_from_slice(&b[2..]);
    Some((u16::from_be_bytes([b[0], b[1]]), id))
}

/// Parse a key under `s`'s prefix; `None` for any other key.
pub fn parse_key(s: &Service, k: &[u8]) -> Option<Parsed> {
    let mut p = [0u8; KEY_MAX];
    let n = key::service(s, &mut p);
    let rest = k.strip_prefix(&p[..n])?;
    if rest == b"active" {
        return Some(Parsed::Active);
    }
    if let Some(g) = rest.strip_prefix(b"staging/") {
        let g: [u8; 8] = g.try_into().ok()?;
        return Some(Parsed::Staging {
            generation: u64::from_be_bytes(g),
        });
    }
    if let Some(d) = rest.strip_prefix(b"decl/") {
        let (kind, id) = kind_id(d)?;
        return Some(Parsed::Declaration { kind, id });
    }
    if let Some(o) = rest.strip_prefix(b"op/") {
        let operation: [u8; ID_LEN] = o.get(..ID_LEN)?.try_into().ok()?;
        let tail = o.get(ID_LEN..)?.strip_prefix(b"/")?;
        if tail == b"head" {
            return Some(Parsed::StatusHead { operation });
        }
        let seq: [u8; 8] = tail.try_into().ok()?;
        return Some(Parsed::Status {
            operation,
            seq: u64::from_be_bytes(seq),
        });
    }
    let rest = rest.strip_prefix(b"gen/")?;
    let g: [u8; 8] = rest.get(..8)?.try_into().ok()?;
    let generation = u64::from_be_bytes(g);
    let rest = rest.get(8..)?.strip_prefix(b"/")?;
    if let Some(r) = rest.strip_prefix(b"r/") {
        let route = *r.first()?;
        if usize::from(route) >= ROUTES_MAX {
            return None;
        }
        let rest = r.get(1..)?.strip_prefix(b"/")?;
        if let Some(ep) = rest.strip_prefix(b"ep/") {
            let endpoint: [u8; ID_LEN] = ep.try_into().ok()?;
            return Some(Parsed::Endpoint {
                generation,
                route,
                endpoint,
            });
        }
        return match rest {
            b"manifest" => Some(Parsed::Manifest { generation, route }),
            b"route" => Some(Parsed::Route { generation, route }),
            _ => None,
        };
    }
    if let Some(p) = rest.strip_prefix(b"pol/") {
        let (kind, id) = kind_id(p)?;
        return Some(Parsed::Policy {
            generation,
            kind,
            id,
        });
    }
    match rest {
        b"sealed" => Some(Parsed::Sealed { generation }),
        _ => None,
    }
}

// ── Manifest digests ──────────────────────────────────────────────────

/// A streaming SHA-256 chain over lexicographically ordered `(key,
/// digest)` pairs: `chain = SHA-256(chain ‖ key_len u16 LE ‖ key ‖
/// digest)` from an all-zero chain. Refuses a key not strictly after the
/// last.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ManifestDigest {
    pub chain: [u8; DIGEST_LEN],
    pub count: u32,
    last_len: u8,
    last: [u8; KEY_MAX],
}

impl Default for ManifestDigest {
    fn default() -> Self {
        Self::new()
    }
}

impl ManifestDigest {
    pub const fn new() -> Self {
        ManifestDigest {
            chain: [0; DIGEST_LEN],
            count: 0,
            last_len: 0,
            last: [0; KEY_MAX],
        }
    }

    /// Resume a chain from a continuation: its state so far and the key
    /// it stands after.
    pub fn resume(chain: [u8; DIGEST_LEN], count: u32, last: &[u8]) -> Option<Self> {
        if last.len() > KEY_MAX {
            return None;
        }
        let mut d = ManifestDigest::new();
        d.chain = chain;
        d.count = count;
        d.last_len = last.len() as u8;
        d.last[..last.len()].copy_from_slice(last);
        Some(d)
    }

    /// The key the chain stands after.
    pub fn last_key(&self) -> &[u8] {
        &self.last[..usize::from(self.last_len)]
    }

    /// Fold one pair in. `false` when `key` is not strictly after the
    /// last (a disorder or a duplicate), or longer than a key.
    pub fn absorb(&mut self, key: &[u8], digest: &[u8; DIGEST_LEN]) -> bool {
        if key.len() > KEY_MAX || (self.count > 0 && key <= self.last_key()) {
            return false;
        }
        let mut h = sha::Sha256::new();
        h.update(&self.chain);
        h.update(&(key.len() as u16).to_le_bytes());
        h.update(key);
        h.update(digest);
        self.chain = h.finalize();
        self.count = self.count.wrapping_add(1);
        self.last_len = key.len() as u8;
        self.last[..key.len()].copy_from_slice(key);
        true
    }

    /// Fold a record in by its stored bytes.
    pub fn absorb_record(&mut self, key: &[u8], value: &[u8]) -> bool {
        self.absorb(key, &sha::sha256(value))
    }
}

// ── Lattice's own records ─────────────────────────────────────────────

pub const MANIFEST_MAGIC: [u8; 4] = *b"LCM1";
pub const MANIFEST_LEN: usize = 4 + 4 + DIGEST_LEN;
pub const SEALED_MAGIC: [u8; 4] = *b"LCS1";
pub const SEALED_LEN: usize = 4 + 4 + 4 + 4 + DIGEST_LEN;
pub const ACTIVE_LEN: usize = 8 + DIGEST_LEN + 8;
pub const MARKER_MAGIC: [u8; 4] = *b"LCG1";
pub const MARKER_LEN: usize = 4 + 8 + 16;
pub const HEAD_MAGIC: [u8; 4] = *b"LSH1";
pub const HEAD_LEN: usize = 4 + 8 + 4 + DIGEST_LEN;
/// [`StatusHead`] flag: the stream's last status is published; nothing
/// is appended after it.
pub const HEAD_CLOSED: u32 = 1;

/// The endpoint-set manifest: how many endpoint records, and their digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub count: u32,
    pub digest: [u8; DIGEST_LEN],
}

impl Manifest {
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let o = out.get_mut(..MANIFEST_LEN)?;
        o[0..4].copy_from_slice(&MANIFEST_MAGIC);
        o[4..8].copy_from_slice(&self.count.to_le_bytes());
        o[8..].copy_from_slice(&self.digest);
        Some(MANIFEST_LEN)
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != MANIFEST_LEN || b[0..4] != MANIFEST_MAGIC {
            return None;
        }
        let mut digest = [0u8; DIGEST_LEN];
        digest.copy_from_slice(&b[8..]);
        Some(Manifest {
            count: u32::from_le_bytes([b[4], b[5], b[6], b[7]]),
            digest,
        })
    }
}

/// The sealed route-generation manifest: the route count, the endpoint
/// count (every route's), the record count (policies, and each route's
/// endpoints, manifest and route) and the digest over them. A generation of
/// no routes (all three 0, the all-zero chain) is the withdrawn form.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sealed {
    pub routes: u32,
    pub endpoints: u32,
    pub records: u32,
    pub digest: [u8; DIGEST_LEN],
}

impl Sealed {
    /// The withdrawn form: no routes, nothing sealed.
    pub fn withdrawn(&self) -> bool {
        self.routes == 0
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let o = out.get_mut(..SEALED_LEN)?;
        o[0..4].copy_from_slice(&SEALED_MAGIC);
        o[4..8].copy_from_slice(&self.routes.to_le_bytes());
        o[8..12].copy_from_slice(&self.endpoints.to_le_bytes());
        o[12..16].copy_from_slice(&self.records.to_le_bytes());
        o[16..].copy_from_slice(&self.digest);
        Some(SEALED_LEN)
    }

    /// Decode; a route count past [`ROUTES_MAX`], or a withdrawn form that
    /// counts any record or carries a chain, does not decode.
    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != SEALED_LEN || b[0..4] != SEALED_MAGIC {
            return None;
        }
        let mut digest = [0u8; DIGEST_LEN];
        digest.copy_from_slice(&b[16..]);
        let s = Sealed {
            routes: u32::from_le_bytes([b[4], b[5], b[6], b[7]]),
            endpoints: u32::from_le_bytes([b[8], b[9], b[10], b[11]]),
            records: u32::from_le_bytes([b[12], b[13], b[14], b[15]]),
            digest,
        };
        if s.routes as usize > ROUTES_MAX
            || (s.routes == 0
                && (s.endpoints != 0 || s.records != 0 || s.digest != [0; DIGEST_LEN]))
        {
            return None;
        }
        Some(s)
    }
}

/// The active-generation pointer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Active {
    pub generation: u64,
    pub sealed_digest: [u8; DIGEST_LEN],
    /// The revision the sealed manifest was written at.
    pub revision: u64,
}

impl Active {
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let o = out.get_mut(..ACTIVE_LEN)?;
        o[0..8].copy_from_slice(&self.generation.to_le_bytes());
        o[8..40].copy_from_slice(&self.sealed_digest);
        o[40..48].copy_from_slice(&self.revision.to_le_bytes());
        Some(ACTIVE_LEN)
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != ACTIVE_LEN {
            return None;
        }
        let mut g = [0u8; 8];
        g.copy_from_slice(&b[0..8]);
        let mut sealed_digest = [0u8; DIGEST_LEN];
        sealed_digest.copy_from_slice(&b[8..40]);
        let mut r = [0u8; 8];
        r.copy_from_slice(&b[40..48]);
        Some(Active {
            generation: u64::from_le_bytes(g),
            sealed_digest,
            revision: u64::from_le_bytes(r),
        })
    }
}

/// A staging marker: when the generation was begun, and by whom.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Marker {
    pub started_ms: u64,
    pub publisher: [u8; 16],
}

impl Marker {
    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let o = out.get_mut(..MARKER_LEN)?;
        o[0..4].copy_from_slice(&MARKER_MAGIC);
        o[4..12].copy_from_slice(&self.started_ms.to_le_bytes());
        o[12..].copy_from_slice(&self.publisher);
        Some(MARKER_LEN)
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != MARKER_LEN || b[0..4] != MARKER_MAGIC {
            return None;
        }
        let mut s = [0u8; 8];
        s.copy_from_slice(&b[4..12]);
        let mut publisher = [0u8; 16];
        publisher.copy_from_slice(&b[12..]);
        Some(Marker {
            started_ms: u64::from_le_bytes(s),
            publisher,
        })
    }
}

/// A status stream's head: how many statuses are published (`seq`, the
/// last one's number), whether the stream is closed, and the digest chain
/// over the published statuses' `(key, content digest)` pairs in order —
/// the same chain a manifest is, so a reader recomputes it from the
/// records it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct StatusHead {
    pub seq: u64,
    pub flags: u32,
    pub chain: [u8; DIGEST_LEN],
}

impl StatusHead {
    pub fn closed(&self) -> bool {
        self.flags & HEAD_CLOSED != 0
    }

    pub fn encode(&self, out: &mut [u8]) -> Option<usize> {
        let o = out.get_mut(..HEAD_LEN)?;
        o[0..4].copy_from_slice(&HEAD_MAGIC);
        o[4..12].copy_from_slice(&self.seq.to_le_bytes());
        o[12..16].copy_from_slice(&self.flags.to_le_bytes());
        o[16..].copy_from_slice(&self.chain);
        Some(HEAD_LEN)
    }

    /// A head whose flags name no bit but [`HEAD_CLOSED`], and whose
    /// count fits the chain's.
    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != HEAD_LEN || b[0..4] != HEAD_MAGIC {
            return None;
        }
        let mut q = [0u8; 8];
        q.copy_from_slice(&b[4..12]);
        let seq = u64::from_le_bytes(q);
        let flags = u32::from_le_bytes([b[12], b[13], b[14], b[15]]);
        if flags & !HEAD_CLOSED != 0 || seq == 0 || seq > u64::from(u32::MAX) {
            return None;
        }
        let mut chain = [0u8; DIGEST_LEN];
        chain.copy_from_slice(&b[16..]);
        Some(StatusHead { seq, flags, chain })
    }
}

// ── KV plumbing shared by the machines ────────────────────────────────

/// How a machine reaches the store: one request at a time, answered by
/// `on_response`. `false` is backpressure: the machine asks again.
pub trait KvIo {
    fn request(&mut self, op: u8, body: &[u8]) -> bool;
}

/// Entries one `KV_OP_RANGE_META` page asks for: a record is at most
/// [`RECORD_MAX`] bytes with its key and metadata, so a page of eight
/// holds eight small records or one large one within the worker's answer.
pub const PAGE_LIMIT: u32 = 8;
/// Keys one delete batch names.
pub const DELETE_BATCH: usize = 32;
/// A request body: a put of the largest record with its key, or a delete
/// batch.
pub const BODY_MAX: usize = 2 + KEY_MAX + 4 + RECORD_MAX + 1 + 8;
const _: () = assert!(BODY_MAX >= 2 + DELETE_BATCH * (2 + KEY_MAX));
/// The largest request body the router carries to a worker: its 4,160-byte
/// ingress (`wire::KV_REQUEST_PAYLOAD_MAX`) less the 3-byte envelope and the
/// 35-byte `wire::KvCommandHead` it re-heads a request with. The router
/// discards a larger request without an answer, so a put over it is
/// refused before it is sent: an endpoint record over 4,038 bytes, a route
/// record over 4,052. The module shell asserts the arithmetic against
/// `wire.rs`.
pub const KV_BODY_MAX: usize = 4160 - 3 - 35;
const _: () = assert!(KV_BODY_MAX >= 2 + DELETE_BATCH * (2 + KEY_MAX));
const _: () = assert!(KV_BODY_MAX < BODY_MAX);

fn put16(o: &mut [u8], at: &mut usize, b: &[u8]) -> Option<()> {
    let n = *at + b.len();
    o.get_mut(*at..n)?.copy_from_slice(b);
    *at = n;
    Some(())
}

/// A `KV_OP_PUT` body (no flags, no expiry), within [`KV_BODY_MAX`].
fn put_body(key: &[u8], value: &[u8], out: &mut [u8]) -> Option<usize> {
    if 2 + key.len() + 4 + value.len() + 1 + 8 > KV_BODY_MAX {
        return None;
    }
    let mut at = 0;
    put16(out, &mut at, &(key.len() as u16).to_le_bytes())?;
    put16(out, &mut at, key)?;
    put16(out, &mut at, &(value.len() as u32).to_le_bytes())?;
    put16(out, &mut at, value)?;
    put16(out, &mut at, &[0u8])?;
    put16(out, &mut at, &0u64.to_le_bytes())?;
    Some(at)
}

/// A `KV_OP_CAS` body.
fn cas_body(key: &[u8], witness: u64, value: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut at = 0;
    put16(out, &mut at, &(key.len() as u16).to_le_bytes())?;
    put16(out, &mut at, key)?;
    put16(out, &mut at, &witness.to_le_bytes())?;
    put16(out, &mut at, &(value.len() as u32).to_le_bytes())?;
    put16(out, &mut at, value)?;
    Some(at)
}

/// A `KV_OP_RANGE_META` body over `[start, end)` at `at_rev` (0: now),
/// `limit` entries, key order; `keys_only` leaves the values out.
fn range_body(
    start: &[u8],
    end: &[u8],
    at_rev: u64,
    limit: u32,
    keys_only: bool,
    out: &mut [u8],
) -> Option<usize> {
    let mut at = 0;
    put16(out, &mut at, &(start.len() as u16).to_le_bytes())?;
    put16(out, &mut at, start)?;
    put16(out, &mut at, &(end.len() as u16).to_le_bytes())?;
    put16(out, &mut at, end)?;
    put16(out, &mut at, &at_rev.to_le_bytes())?;
    put16(out, &mut at, &limit.to_le_bytes())?;
    put16(out, &mut at, &[u8::from(keys_only)])?;
    put16(out, &mut at, &[0u8, 0u8])?;
    put16(out, &mut at, &[0u8; 32])?;
    Some(at)
}

/// The key just after `prefix`'s last key: `prefix` with its last byte
/// incremented (a catalogue prefix ends in `/`, never `0xFF`).
fn prefix_end(prefix: &[u8], out: &mut [u8]) -> Option<usize> {
    let n = prefix.len();
    out.get_mut(..n)?.copy_from_slice(prefix);
    let last = out.get_mut(n - 1)?;
    *last = last.checked_add(1)?;
    Some(n)
}

/// The key just after `k`: `k` followed by `0x00`.
fn successor(k: &[u8], out: &mut [u8]) -> Option<usize> {
    let n = k.len() + 1;
    out.get_mut(..k.len())?.copy_from_slice(k);
    *out.get_mut(k.len())? = 0;
    Some(n)
}

/// One entry of a `KV_RESULT_KVS` answer: `(key, value, mod_rev, next)`.
fn kvs_entry(b: &[u8], at: usize) -> Option<(&[u8], &[u8], u64, usize)> {
    let kl = usize::from(u16::from_le_bytes([*b.get(at)?, *b.get(at + 1)?]));
    let key = b.get(at + 2..at + 2 + kl)?;
    let p = at + 2 + kl;
    let vl =
        u32::from_le_bytes([*b.get(p)?, *b.get(p + 1)?, *b.get(p + 2)?, *b.get(p + 3)?]) as usize;
    let value = b.get(p + 4..p + 4 + vl)?;
    let m = p + 4 + vl + 8;
    let mut modr = [0u8; 8];
    modr.copy_from_slice(b.get(m..m + 8)?);
    Some((key, value, u64::from_le_bytes(modr), p + 4 + vl + 32))
}

/// A `KV_RESULT_KVS` head: `(more, count)`.
fn kvs_head(b: &[u8]) -> Option<(u8, usize)> {
    Some((
        *b.get(8)?,
        usize::from(u16::from_le_bytes([*b.get(9)?, *b.get(10)?])),
    ))
}

// ── Publication ───────────────────────────────────────────────────────

/// Why a publication did not activate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishError {
    /// A record refused structurally.
    Envelope(EnvelopeError),
    /// A record of the wrong kind for its place.
    Kind,
    /// The store refused an operation (its result code).
    Store(u8),
    /// The generation's staging marker already exists: another publisher
    /// holds the generation, or it was published (or begun) before.
    Exists,
    /// Route `route`'s endpoints read back are not the endpoints declared
    /// for it (`found` of `declared`): a referential gap.
    Endpoints {
        route: u8,
        declared: u32,
        found: u32,
    },
    /// A declared route has no route record (or a route was staged that is
    /// not declared, or past [`ROUTES_MAX`]); or a generation declaring no
    /// route — the withdrawn form — had endpoints or policies staged.
    NoRoute,
    /// The policies read back are not the policies staged (`found` of
    /// `staged`): a referential gap.
    Policies { staged: u32, found: u32 },
    /// The active pointer is not at the expected predecessor (`actual`
    /// is 0 for none): another publisher won, or the caller is stale.
    Predecessor { expected: u64, actual: u64 },
    /// The CAS lost to a racing publisher.
    Raced,
    /// A call out of order (a stage after the seal, a stage or a seal
    /// while a write is in flight).
    Sequence,
    /// A page the store answered that cannot be read.
    Answer,
}

/// Where a publication stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishPhase {
    Idle,
    /// The staging marker's create in flight.
    Marker,
    /// Ready for a stage or the seal; nothing in flight.
    Staging,
    /// A record's write in flight.
    Writing,
    /// Reading the endpoints back.
    SealEndpoints,
    SealManifest,
    /// Reading every record back.
    SealRecords,
    SealWrite,
    ActivateRead,
    ActivateCas,
    Activated,
    Failed,
}

/// One route generation's publication, in order.
pub struct Publisher {
    pub service: Service,
    pub generation: u64,
    pub predecessor: u64,
    pub phase: PublishPhase,
    pub error: Option<PublishError>,
    /// The revision the CAS applied at (the watcher-visible one).
    pub revision: u64,
    /// Endpoints (every route's) and policies staged by this publisher,
    /// and the routes staged (bit `r`: route `r`'s record).
    pub staged_endpoints: u32,
    pub staged_policies: u32,
    routes_staged: u32,
    policies_seen: u32,
    /// The seal: the routes declared and each one's endpoint count, the
    /// route being read back, the endpoint-set digest, the generation's
    /// digest, and where the walk resumes.
    routes: u8,
    declared: [u32; ROUTES_MAX],
    cur: u8,
    ep_digest: ManifestDigest,
    all_digest: ManifestDigest,
    routes_seen: u32,
    manifests_seen: u32,
    endpoints_seen: u32,
    manifests: [Manifest; ROUTES_MAX],
    sealed: Sealed,
    sealed_revision: u64,
    /// The active pointer as read (and its witness), for the CAS.
    witness: u64,
    /// The request being (re)offered until the store takes it.
    pending: bool,
    op: u8,
    body_len: usize,
    body: [u8; BODY_MAX],
}

impl Default for Publisher {
    fn default() -> Self {
        Self::new()
    }
}

impl Publisher {
    pub const fn new() -> Self {
        Publisher {
            service: Service {
                tenant: [0; ID_LEN],
                service: [0; ID_LEN],
            },
            generation: 0,
            predecessor: 0,
            phase: PublishPhase::Idle,
            error: None,
            revision: 0,
            staged_endpoints: 0,
            staged_policies: 0,
            routes_staged: 0,
            policies_seen: 0,
            routes: 0,
            declared: [0; ROUTES_MAX],
            cur: 0,
            ep_digest: ManifestDigest::new(),
            all_digest: ManifestDigest::new(),
            routes_seen: 0,
            manifests_seen: 0,
            endpoints_seen: 0,
            manifests: [Manifest {
                count: 0,
                digest: [0; DIGEST_LEN],
            }; ROUTES_MAX],
            sealed: Sealed {
                routes: 0,
                endpoints: 0,
                records: 0,
                digest: [0; DIGEST_LEN],
            },
            sealed_revision: 0,
            witness: 0,
            pending: false,
            op: 0,
            body_len: 0,
            body: [0; BODY_MAX],
        }
    }

    /// Whether a stage or the seal may be asked now.
    pub fn ready(&self) -> bool {
        self.phase == PublishPhase::Staging && !self.pending
    }

    pub fn done(&self) -> bool {
        matches!(self.phase, PublishPhase::Activated | PublishPhase::Failed)
    }

    fn fail(&mut self, e: PublishError) {
        self.phase = PublishPhase::Failed;
        self.error = Some(e);
        self.pending = false;
    }

    fn stage(&mut self, op: u8, len: usize, phase: PublishPhase) {
        self.op = op;
        self.body_len = len;
        self.pending = true;
        self.phase = phase;
    }

    /// Begin generation `generation` of `service`, expecting the active
    /// pointer at `predecessor` (0: none) when it is activated: creates
    /// the staging marker, which fails [`PublishError::Exists`] when the
    /// generation is already begun.
    pub fn begin(
        &mut self,
        service: Service,
        generation: u64,
        predecessor: u64,
        now_ms: u64,
        publisher: [u8; 16],
    ) -> bool {
        if !matches!(
            self.phase,
            PublishPhase::Idle | PublishPhase::Activated | PublishPhase::Failed
        ) {
            return false;
        }
        *self = Publisher::new();
        self.service = service;
        self.generation = generation;
        self.predecessor = predecessor;
        let mut k = [0u8; KEY_MAX];
        let kn = key::staging(&service, generation, &mut k);
        let mut v = [0u8; MARKER_LEN];
        let m = Marker {
            started_ms: now_ms,
            publisher,
        };
        let vn = m.encode(&mut v).unwrap_or(0);
        let Some(n) = cas_body(&k[..kn], 0, &v[..vn], &mut self.body) else {
            return false;
        };
        self.stage(KV_OP_CAS, n, PublishPhase::Marker);
        true
    }

    /// Stage endpoint `endpoint`'s record (an endpoint envelope) in route
    /// `route` (`0..ROUTES_MAX`).
    pub fn stage_endpoint(
        &mut self,
        route: u8,
        endpoint: &[u8; ID_LEN],
        record: &[u8],
    ) -> Result<(), PublishError> {
        if !self.ready() {
            return Err(PublishError::Sequence);
        }
        if usize::from(route) >= ROUTES_MAX {
            return Err(PublishError::NoRoute);
        }
        let e = validate_envelope(record).map_err(PublishError::Envelope)?;
        if e.kind != KIND_ENDPOINT {
            return Err(PublishError::Kind);
        }
        let mut k = [0u8; KEY_MAX];
        let kn = key::endpoint(&self.service, self.generation, route, endpoint, &mut k);
        let n = put_body(&k[..kn], record, &mut self.body).ok_or(PublishError::Envelope(
            EnvelopeError::TooLarge(record.len()),
        ))?;
        self.staged_endpoints = self.staged_endpoints.wrapping_add(1);
        self.stage(KV_OP_PUT, n, PublishPhase::Writing);
        Ok(())
    }

    /// Stage route `route`'s record (a route envelope), `0..ROUTES_MAX`.
    pub fn stage_route(&mut self, route: u8, record: &[u8]) -> Result<(), PublishError> {
        if !self.ready() {
            return Err(PublishError::Sequence);
        }
        if usize::from(route) >= ROUTES_MAX {
            return Err(PublishError::NoRoute);
        }
        let e = validate_envelope(record).map_err(PublishError::Envelope)?;
        if e.kind != KIND_ROUTE {
            return Err(PublishError::Kind);
        }
        let mut k = [0u8; KEY_MAX];
        let kn = key::route(&self.service, self.generation, route, &mut k);
        let n = put_body(&k[..kn], record, &mut self.body).ok_or(PublishError::Envelope(
            EnvelopeError::TooLarge(record.len()),
        ))?;
        self.routes_staged |= 1 << route;
        self.stage(KV_OP_PUT, n, PublishPhase::Writing);
        Ok(())
    }

    /// Stage a policy of the generation: any placed kind
    /// ([`validate_record`]), keyed by its kind and content address, and
    /// covered by the sealed manifest like every record of the generation.
    /// Answers the content address, the id a route names the policy by.
    /// Each policy is staged once: the seal reads back exactly as many as
    /// were staged.
    pub fn stage_policy(&mut self, record: &[u8]) -> Result<[u8; ID_LEN], PublishError> {
        if !self.ready() {
            return Err(PublishError::Sequence);
        }
        let e = validate_record(record).map_err(PublishError::Envelope)?;
        let id = content_address(record).ok_or(PublishError::Kind)?;
        let mut k = [0u8; KEY_MAX];
        let kn = key::policy(&self.service, self.generation, e.kind, &id, &mut k);
        let n = put_body(&k[..kn], record, &mut self.body).ok_or(PublishError::Envelope(
            EnvelopeError::TooLarge(record.len()),
        ))?;
        self.staged_policies = self.staged_policies.wrapping_add(1);
        self.stage(KV_OP_PUT, n, PublishPhase::Writing);
        Ok(id)
    }

    /// Seal: for each route, read its endpoints back (`declared[r]` of
    /// them must exist) and write its manifest; then read every record
    /// back and write the sealed manifest; then activate by CAS.
    /// `declared` names the generation's routes, `0..declared.len()`, each
    /// staged, and the count each one's manifest will carry — the caller's
    /// own counts, so a staged record that did not land is a refusal, not a
    /// silent shortfall. An empty `declared` with nothing staged seals the
    /// withdrawn form (no routes, no records).
    pub fn seal(&mut self, declared: &[u32]) -> Result<(), PublishError> {
        if !self.ready() {
            return Err(PublishError::Sequence);
        }
        let n = declared.len();
        let want = if n >= 32 { u32::MAX } else { (1u32 << n) - 1 };
        if n > ROUTES_MAX || self.routes_staged != want {
            return Err(PublishError::NoRoute);
        }
        if n == 0 && (self.staged_endpoints != 0 || self.staged_policies != 0) {
            return Err(PublishError::NoRoute);
        }
        self.routes = n as u8;
        self.declared = [0; ROUTES_MAX];
        self.declared[..n].copy_from_slice(declared);
        self.cur = 0;
        if n == 0 {
            self.begin_record_walk();
            return Ok(());
        }
        self.ep_digest = ManifestDigest::new();
        self.issue_endpoint_page();
        Ok(())
    }

    fn begin_record_walk(&mut self) {
        self.all_digest = ManifestDigest::new();
        self.routes_seen = 0;
        self.manifests_seen = 0;
        self.endpoints_seen = 0;
        self.policies_seen = 0;
        self.issue_record_page();
    }

    fn issue_endpoint_page(&mut self) {
        let mut start = [0u8; KEY_MAX + 1];
        let mut end = [0u8; KEY_MAX + 1];
        let mut p = [0u8; KEY_MAX];
        let pn = key::endpoints(&self.service, self.generation, self.cur, &mut p);
        let sn = if self.ep_digest.count == 0 {
            start[..pn].copy_from_slice(&p[..pn]);
            pn
        } else {
            successor(self.ep_digest.last_key(), &mut start).unwrap_or(0)
        };
        let en = prefix_end(&p[..pn], &mut end).unwrap_or(0);
        match range_body(
            &start[..sn],
            &end[..en],
            0,
            PAGE_LIMIT,
            false,
            &mut self.body,
        ) {
            Some(n) => self.stage(KV_OP_RANGE_META, n, PublishPhase::SealEndpoints),
            None => self.fail(PublishError::Answer),
        }
    }

    fn issue_record_page(&mut self) {
        let mut start = [0u8; KEY_MAX + 1];
        let mut end = [0u8; KEY_MAX + 1];
        let mut p = [0u8; KEY_MAX];
        let pn = key::generation(&self.service, self.generation, &mut p);
        let sn = if self.all_digest.count == 0 {
            start[..pn].copy_from_slice(&p[..pn]);
            pn
        } else {
            successor(self.all_digest.last_key(), &mut start).unwrap_or(0)
        };
        let en = prefix_end(&p[..pn], &mut end).unwrap_or(0);
        match range_body(
            &start[..sn],
            &end[..en],
            0,
            PAGE_LIMIT,
            false,
            &mut self.body,
        ) {
            Some(n) => self.stage(KV_OP_RANGE_META, n, PublishPhase::SealRecords),
            None => self.fail(PublishError::Answer),
        }
    }

    /// Offer the pending request, if any.
    pub fn step(&mut self, io: &mut impl KvIo) {
        if self.pending && io.request(self.op, &self.body[..self.body_len]) {
            self.pending = false;
        }
    }

    /// Whether a request is in flight (offered, unanswered).
    pub fn in_flight(&self) -> bool {
        !self.pending
            && matches!(
                self.phase,
                PublishPhase::Marker
                    | PublishPhase::Writing
                    | PublishPhase::SealEndpoints
                    | PublishPhase::SealManifest
                    | PublishPhase::SealRecords
                    | PublishPhase::SealWrite
                    | PublishPhase::ActivateRead
                    | PublishPhase::ActivateCas
            )
    }

    /// The store's answer to the request in flight.
    pub fn on_response(&mut self, result: u8, revision: u64, body: &[u8]) {
        match self.phase {
            PublishPhase::Marker => match result {
                KV_RESULT_OK => self.phase = PublishPhase::Staging,
                KV_RESULT_CAS_FAILED => self.fail(PublishError::Exists),
                r => self.fail(PublishError::Store(r)),
            },
            PublishPhase::Writing => {
                if result != KV_RESULT_OK {
                    return self.fail(PublishError::Store(result));
                }
                self.phase = PublishPhase::Staging;
            }
            PublishPhase::SealEndpoints => self.endpoint_page(result, body),
            PublishPhase::SealManifest => {
                if result != KV_RESULT_OK {
                    return self.fail(PublishError::Store(result));
                }
                // The next route's endpoints, or every record once each
                // route has its manifest.
                self.cur += 1;
                if self.cur < self.routes {
                    self.ep_digest = ManifestDigest::new();
                    return self.issue_endpoint_page();
                }
                self.begin_record_walk();
            }
            PublishPhase::SealRecords => self.record_page(result, body),
            PublishPhase::SealWrite => {
                if result != KV_RESULT_OK {
                    return self.fail(PublishError::Store(result));
                }
                self.sealed_revision = revision;
                let mut k = [0u8; KEY_MAX];
                let kn = key::active(&self.service, &mut k);
                match range_body(&k[..kn], &[], 0, 1, false, &mut self.body) {
                    Some(n) => self.stage(KV_OP_RANGE_META, n, PublishPhase::ActivateRead),
                    None => self.fail(PublishError::Answer),
                }
            }
            PublishPhase::ActivateRead => self.active_read(result, body),
            PublishPhase::ActivateCas => match result {
                KV_RESULT_OK => {
                    self.revision = revision;
                    self.phase = PublishPhase::Activated;
                }
                KV_RESULT_CAS_FAILED => self.fail(PublishError::Raced),
                r => self.fail(PublishError::Store(r)),
            },
            _ => {}
        }
    }

    fn endpoint_page(&mut self, result: u8, body: &[u8]) {
        if result != KV_RESULT_KVS {
            return self.fail(PublishError::Store(result));
        }
        let Some((more, count)) = kvs_head(body) else {
            return self.fail(PublishError::Answer);
        };
        let mut at = 11;
        for _ in 0..count {
            let Some((k, v, _, next)) = kvs_entry(body, at) else {
                return self.fail(PublishError::Answer);
            };
            at = next;
            match validate_envelope(v) {
                Ok(e) if e.kind == KIND_ENDPOINT => {}
                Ok(_) => return self.fail(PublishError::Kind),
                Err(e) => return self.fail(PublishError::Envelope(e)),
            }
            if !self.ep_digest.absorb_record(k, v) {
                return self.fail(PublishError::Answer);
            }
        }
        if more != 0 {
            if count == 0 {
                return self.fail(PublishError::Answer);
            }
            return self.issue_endpoint_page();
        }
        let r = usize::from(self.cur);
        if self.ep_digest.count != self.declared[r] {
            return self.fail(PublishError::Endpoints {
                route: self.cur,
                declared: self.declared[r],
                found: self.ep_digest.count,
            });
        }
        self.manifests[r] = Manifest {
            count: self.ep_digest.count,
            digest: self.ep_digest.chain,
        };
        let mut k = [0u8; KEY_MAX];
        let kn = key::manifest(&self.service, self.generation, self.cur, &mut k);
        let mut v = [0u8; MANIFEST_LEN];
        let vn = self.manifests[r].encode(&mut v).unwrap_or(0);
        match put_body(&k[..kn], &v[..vn], &mut self.body) {
            Some(n) => self.stage(KV_OP_PUT, n, PublishPhase::SealManifest),
            None => self.fail(PublishError::Answer),
        }
    }

    fn record_page(&mut self, result: u8, body: &[u8]) {
        if result != KV_RESULT_KVS {
            return self.fail(PublishError::Store(result));
        }
        let Some((more, count)) = kvs_head(body) else {
            return self.fail(PublishError::Answer);
        };
        let mut at = 11;
        for _ in 0..count {
            let Some((k, v, _, next)) = kvs_entry(body, at) else {
                return self.fail(PublishError::Answer);
            };
            at = next;
            match parse_key(&self.service, k) {
                Some(Parsed::Endpoint { route, .. }) if route < self.routes => {
                    self.endpoints_seen = self.endpoints_seen.wrapping_add(1);
                }
                Some(Parsed::Manifest { route, .. }) if route < self.routes => {
                    if Manifest::decode(v) != Some(self.manifests[usize::from(route)]) {
                        return self.fail(PublishError::Answer);
                    }
                    self.manifests_seen |= 1 << route;
                }
                Some(Parsed::Route { route, .. }) if route < self.routes => {
                    match validate_envelope(v) {
                        Ok(e) if e.kind == KIND_ROUTE => {}
                        _ => return self.fail(PublishError::Kind),
                    }
                    self.routes_seen |= 1 << route;
                }
                Some(Parsed::Policy { kind, id, .. }) => {
                    match validate_record(v) {
                        Ok(e) if e.kind == kind && content_address(v) == Some(id) => {}
                        _ => return self.fail(PublishError::Kind),
                    }
                    self.policies_seen = self.policies_seen.wrapping_add(1);
                }
                // A sealed manifest already there (a seal run twice)
                // is not part of what it covers.
                Some(Parsed::Sealed { .. }) => continue,
                _ => return self.fail(PublishError::Answer),
            }
            if !self.all_digest.absorb_record(k, v) {
                return self.fail(PublishError::Answer);
            }
        }
        if more != 0 {
            if count == 0 {
                return self.fail(PublishError::Answer);
            }
            return self.issue_record_page();
        }
        let want = self.routes_staged;
        if self.routes_seen != want {
            return self.fail(PublishError::NoRoute);
        }
        if self.policies_seen != self.staged_policies {
            return self.fail(PublishError::Policies {
                staged: self.staged_policies,
                found: self.policies_seen,
            });
        }
        let endpoints: u32 = self.declared.iter().sum();
        let routes = u32::from(self.routes);
        if self.manifests_seen != want
            || self.endpoints_seen != endpoints
            || self.all_digest.count != endpoints + 2 * routes + self.policies_seen
        {
            return self.fail(PublishError::Answer);
        }
        self.sealed = Sealed {
            routes,
            endpoints,
            records: self.all_digest.count,
            digest: self.all_digest.chain,
        };
        let mut k = [0u8; KEY_MAX];
        let kn = key::sealed(&self.service, self.generation, &mut k);
        let mut v = [0u8; SEALED_LEN];
        let vn = self.sealed.encode(&mut v).unwrap_or(0);
        match put_body(&k[..kn], &v[..vn], &mut self.body) {
            Some(n) => self.stage(KV_OP_PUT, n, PublishPhase::SealWrite),
            None => self.fail(PublishError::Answer),
        }
    }

    fn active_read(&mut self, result: u8, body: &[u8]) {
        if result != KV_RESULT_KVS {
            return self.fail(PublishError::Store(result));
        }
        let Some((_, count)) = kvs_head(body) else {
            return self.fail(PublishError::Answer);
        };
        let (actual, witness) = if count == 0 {
            (0, 0)
        } else {
            let Some((_, v, modr, _)) = kvs_entry(body, 11) else {
                return self.fail(PublishError::Answer);
            };
            let Some(a) = Active::decode(v) else {
                return self.fail(PublishError::Answer);
            };
            (a.generation, modr)
        };
        if actual != self.predecessor {
            return self.fail(PublishError::Predecessor {
                expected: self.predecessor,
                actual,
            });
        }
        self.witness = witness;
        let mut k = [0u8; KEY_MAX];
        let kn = key::active(&self.service, &mut k);
        let mut v = [0u8; ACTIVE_LEN];
        let a = Active {
            generation: self.generation,
            sealed_digest: self.sealed.digest,
            revision: self.sealed_revision,
        };
        let vn = a.encode(&mut v).unwrap_or(0);
        match cas_body(&k[..kn], witness, &v[..vn], &mut self.body) {
            Some(n) => self.stage(KV_OP_CAS, n, PublishPhase::ActivateCas),
            None => self.fail(PublishError::Answer),
        }
    }
}

// ── Snapshot ──────────────────────────────────────────────────────────

/// A snapshot continuation: the revision it is bound to, the records so
/// far and their digest chain, the key the next page starts after (a key
/// of the snapshot's service, which binds the continuation to it).
/// Opaque to the consumer; `[at_rev u64 LE][count u32 LE][chain 32]
/// [key_len u8][key]`.
pub const TOKEN_MAX: usize = 8 + 4 + DIGEST_LEN + 1 + KEY_MAX;

/// Why a snapshot stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotError {
    /// The revision is compacted: no snapshot can be taken at it.
    Compacted,
    /// The revision is past the store.
    Future,
    /// A continuation not of this snapshot's revision or service, or
    /// malformed.
    Token,
    Store(u8),
    Answer,
    /// The caller's page cannot hold even the first record.
    PageTooSmall,
}

/// One page of a snapshot: the records under a service at one revision
/// from a continuation on, into the caller's page.
pub struct Snapshot {
    pub service: Service,
    pub at_rev: u64,
    digest: ManifestDigest,
    pending: bool,
    body_len: usize,
    body: [u8; 2 * (KEY_MAX + 1) + 64],
}

/// A finished page: records were written to the caller's page; `More`
/// with the continuation, or `Last` with the manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotPage {
    More {
        token_len: usize,
    },
    Last {
        count: u32,
        digest: [u8; DIGEST_LEN],
    },
}

impl Default for Snapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl Snapshot {
    pub const fn new() -> Self {
        Snapshot {
            service: Service {
                tenant: [0; ID_LEN],
                service: [0; ID_LEN],
            },
            at_rev: 0,
            digest: ManifestDigest::new(),
            pending: false,
            body_len: 0,
            body: [0; 2 * (KEY_MAX + 1) + 64],
        }
    }

    /// Begin a page: at `at_rev` (0: the store's revision — the first
    /// page's answer pins it), from `token` (empty: the start).
    pub fn begin(
        &mut self,
        service: Service,
        at_rev: u64,
        token: &[u8],
    ) -> Result<(), SnapshotError> {
        self.service = service;
        let mut p = [0u8; KEY_MAX];
        let pn = key::service(&service, &mut p);
        if token.is_empty() {
            self.at_rev = at_rev;
            self.digest = ManifestDigest::new();
        } else {
            let (rev, count, chain, last) = decode_token(token).ok_or(SnapshotError::Token)?;
            if (at_rev != 0 && at_rev != rev)
                || rev == 0
                || count == 0
                || last.len() <= pn
                || !last.starts_with(&p[..pn])
            {
                return Err(SnapshotError::Token);
            }
            self.at_rev = rev;
            self.digest = ManifestDigest::resume(chain, count, last).ok_or(SnapshotError::Token)?;
        }
        let mut start = [0u8; KEY_MAX + 1];
        let mut end = [0u8; KEY_MAX + 1];
        let sn = if self.digest.count == 0 {
            start[..pn].copy_from_slice(&p[..pn]);
            pn
        } else {
            successor(self.digest.last_key(), &mut start).ok_or(SnapshotError::Token)?
        };
        let en = prefix_end(&p[..pn], &mut end).ok_or(SnapshotError::Answer)?;
        self.body_len = range_body(
            &start[..sn],
            &end[..en],
            self.at_rev,
            PAGE_LIMIT,
            false,
            &mut self.body,
        )
        .ok_or(SnapshotError::Answer)?;
        self.pending = true;
        Ok(())
    }

    pub fn step(&mut self, io: &mut impl KvIo) {
        if self.pending && io.request(KV_OP_RANGE_META, &self.body[..self.body_len]) {
            self.pending = false;
        }
    }

    /// The store's answer: the page's records into `page` as `[klen u16
    /// LE][key][vlen u32 LE][value]` each (`written` bytes), and the token
    /// into `token` when there is more. Records go in while they fit; a
    /// page that fills before the store's answer is done stops after the
    /// last record that fit and continues from it, so a page is never cut
    /// and never refused because the store answered more than it holds.
    pub fn on_response(
        &mut self,
        result: u8,
        revision: u64,
        body: &[u8],
        page: &mut [u8],
        token: &mut [u8; TOKEN_MAX],
    ) -> Result<(usize, SnapshotPage), SnapshotError> {
        match result {
            KV_RESULT_KVS => {}
            KV_RESULT_COMPACTED => return Err(SnapshotError::Compacted),
            KV_RESULT_FUTURE_REV => return Err(SnapshotError::Future),
            r => return Err(SnapshotError::Store(r)),
        }
        if self.at_rev == 0 {
            // The first page pins the snapshot to the revision it read at.
            self.at_rev = revision;
        }
        let (more, count) = kvs_head(body).ok_or(SnapshotError::Answer)?;
        let mut at = 11;
        let mut written = 0usize;
        let mut full = false;
        for i in 0..count {
            let (k, v, _, next) = kvs_entry(body, at).ok_or(SnapshotError::Answer)?;
            at = next;
            let mut w = written;
            let fits = put16(page, &mut w, &(k.len() as u16).to_le_bytes())
                .and_then(|()| put16(page, &mut w, k))
                .and_then(|()| put16(page, &mut w, &(v.len() as u32).to_le_bytes()))
                .and_then(|()| put16(page, &mut w, v))
                .is_some();
            if !fits {
                if i == 0 {
                    return Err(SnapshotError::PageTooSmall);
                }
                full = true;
                break;
            }
            if !self.digest.absorb_record(k, v) {
                return Err(SnapshotError::Answer);
            }
            written = w;
        }
        if more != 0 || full {
            if count == 0 {
                return Err(SnapshotError::Answer);
            }
            let n = encode_token(self.at_rev, &self.digest, token);
            return Ok((written, SnapshotPage::More { token_len: n }));
        }
        Ok((
            written,
            SnapshotPage::Last {
                count: self.digest.count,
                digest: self.digest.chain,
            },
        ))
    }
}

fn encode_token(at_rev: u64, d: &ManifestDigest, out: &mut [u8; TOKEN_MAX]) -> usize {
    out[0..8].copy_from_slice(&at_rev.to_le_bytes());
    out[8..12].copy_from_slice(&d.count.to_le_bytes());
    out[12..44].copy_from_slice(&d.chain);
    let k = d.last_key();
    out[44] = k.len() as u8;
    out[45..45 + k.len()].copy_from_slice(k);
    45 + k.len()
}

fn decode_token(t: &[u8]) -> Option<(u64, u32, [u8; DIGEST_LEN], &[u8])> {
    if t.len() < 45 {
        return None;
    }
    let mut r = [0u8; 8];
    r.copy_from_slice(&t[0..8]);
    let count = u32::from_le_bytes([t[8], t[9], t[10], t[11]]);
    let mut chain = [0u8; DIGEST_LEN];
    chain.copy_from_slice(&t[12..44]);
    let kl = usize::from(t[44]);
    let key = t.get(45..45 + kl)?;
    if t.len() != 45 + kl || kl > KEY_MAX {
        return None;
    }
    Some((u64::from_le_bytes(r), count, chain, key))
}

/// Parse one snapshot page as the consumer reads it: `f(key, value)` per
/// record; `false` for a page that is not whole.
pub fn each_snapshot_record(page: &[u8], mut f: impl FnMut(&[u8], &[u8])) -> bool {
    let mut at = 0usize;
    while at < page.len() {
        let Some(kl) = page
            .get(at..at + 2)
            .map(|b| usize::from(u16::from_le_bytes([b[0], b[1]])))
        else {
            return false;
        };
        let Some(k) = page.get(at + 2..at + 2 + kl) else {
            return false;
        };
        let p = at + 2 + kl;
        let Some(vl) = page
            .get(p..p + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
        else {
            return false;
        };
        let Some(v) = page.get(p + 4..p + 4 + vl) else {
            return false;
        };
        f(k, v);
        at = p + 4 + vl;
    }
    true
}

// ── Changes: the ordered tail, each revision counted ─────────────────

/// Changes one window asks the store for: a garbage-collection or reclaim
/// batch is [`DELETE_BATCH`] deletes at one revision, so a window always
/// holds at least one whole revision of the catalogue's own writes.
pub const CHANGES_LIMIT: usize = 64;
const _: () = assert!(CHANGES_LIMIT >= DELETE_BATCH && CHANGES_LIMIT <= u16::MAX as usize);
/// A change as the page carries it, before its key and value:
/// `[revision u64][index u16][count u16][kind u8][klen u16]`, then the
/// key, `[vlen u32]` and the value.
pub const CHANGE_HEAD: usize = 8 + 2 + 2 + 1 + 2;
/// A change is a write (`0`) or a delete (`1`, no value): the store's
/// `VERSION_KIND_*`.
pub const CHANGE_PUT: u8 = 0;
pub const CHANGE_DELETE: u8 = 1;

/// Why a changes page could not be served.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangesError {
    /// `after` is below what the store retains: take a snapshot.
    Compacted,
    /// `after` is past the store.
    Future,
    /// One revision's changes do not fit one page (or one store answer).
    TooLarge,
    Store(u8),
    Answer,
}

/// A served page: `written` bytes of changes, every revision in
/// `(after, through]` whole and in order, `count` changes; `revision` is
/// the store's revision when the request was taken (`through <
/// revision`: there is more to ask for, from `through`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChangesPage {
    pub written: usize,
    pub through: u64,
    pub revision: u64,
    pub count: u32,
}

/// The changes under a service strictly after a revision, as whole
/// revisions: every change carries its revision, its place among that
/// revision's changes and how many the revision holds, so a consumer that
/// installed a snapshot at `after` applies a revision only once all of its
/// changes are in hand, and knows when they are.
///
/// One `KV_OP_SCAN_VERSIONS` window `(after, to]` over the service's span
/// is asked for (`to` 0 the first time: the store pins its revision and
/// reports it). The store walks a window key by key, so only a window it
/// answered whole says how many changes each revision holds: a window
/// that did not fit one answer, or whose changes do not fit the caller's
/// page, is halved (`to` brought down toward `after`) and asked again,
/// until it fits. A window of one revision that still does not fit is
/// `TooLarge`. The answer is sorted by revision, then key, and numbered.
pub struct Changes {
    pub service: Service,
    pub after: u64,
    /// The window's ceiling asked for (0: the store's revision).
    pub to: u64,
    /// The store's revision, pinned by the first answer.
    pub revision: u64,
    pending: bool,
    body_len: usize,
    body: [u8; 2 * (KEY_MAX + 1) + 64],
    /// The answer's entries, as offsets into it, while it is sorted.
    order: [u32; CHANGES_LIMIT],
}

impl Default for Changes {
    fn default() -> Self {
        Self::new()
    }
}

/// One `KV_RESULT_VERSIONS` entry: `(revision, kind, key, value, next)`.
fn version_entry(b: &[u8], at: usize) -> Option<(u64, u8, &[u8], &[u8], usize)> {
    let mut r = [0u8; 8];
    r.copy_from_slice(b.get(at..at + 8)?);
    let kind = *b.get(at + 16)?;
    let kl = usize::from(u16::from_le_bytes([*b.get(at + 17)?, *b.get(at + 18)?]));
    let key = b.get(at + 19..at + 19 + kl)?;
    let p = at + 19 + kl;
    let vl =
        u32::from_le_bytes([*b.get(p)?, *b.get(p + 1)?, *b.get(p + 2)?, *b.get(p + 3)?]) as usize;
    let value = b.get(p + 4..p + 4 + vl)?;
    Some((u64::from_le_bytes(r), kind, key, value, p + 4 + vl))
}

/// `(revision, key)` order between two entries of one answer.
fn change_before(b: &[u8], x: u32, y: u32) -> bool {
    match (version_entry(b, x as usize), version_entry(b, y as usize)) {
        (Some((rx, _, kx, ..)), Some((ry, _, ky, ..))) => (rx, kx) < (ry, ky),
        _ => false,
    }
}

impl Changes {
    pub const fn new() -> Self {
        Changes {
            service: Service {
                tenant: [0; ID_LEN],
                service: [0; ID_LEN],
            },
            after: 0,
            to: 0,
            revision: 0,
            pending: false,
            body_len: 0,
            body: [0; 2 * (KEY_MAX + 1) + 64],
            order: [0; CHANGES_LIMIT],
        }
    }

    /// Begin a page of the changes under `service` strictly after `after`.
    pub fn begin(&mut self, service: Service, after: u64) -> Result<(), ChangesError> {
        self.service = service;
        self.after = after;
        self.to = 0;
        self.revision = 0;
        self.stage()
    }

    fn stage(&mut self) -> Result<(), ChangesError> {
        let mut p = [0u8; KEY_MAX];
        let pn = key::service(&self.service, &mut p);
        let mut end = [0u8; KEY_MAX + 1];
        let en = prefix_end(&p[..pn], &mut end).ok_or(ChangesError::Answer)?;
        let mut at = 0usize;
        let b = &mut self.body;
        put16(b, &mut at, &(pn as u16).to_le_bytes())
            .and_then(|()| put16(b, &mut at, &p[..pn]))
            .and_then(|()| put16(b, &mut at, &(en as u16).to_le_bytes()))
            .and_then(|()| put16(b, &mut at, &end[..en]))
            .and_then(|()| put16(b, &mut at, &self.after.to_le_bytes()))
            .and_then(|()| put16(b, &mut at, &self.to.to_le_bytes()))
            .and_then(|()| put16(b, &mut at, &0u64.to_le_bytes()))
            .and_then(|()| put16(b, &mut at, &(CHANGES_LIMIT as u16).to_le_bytes()))
            .and_then(|()| put16(b, &mut at, &[0u8]))
            .ok_or(ChangesError::Answer)?;
        self.body_len = at;
        self.pending = true;
        Ok(())
    }

    pub fn step(&mut self, io: &mut impl KvIo) {
        if self.pending && io.request(KV_OP_SCAN_VERSIONS, &self.body[..self.body_len]) {
            self.pending = false;
        }
    }

    /// Whether a request is staged and not yet sent.
    pub fn pending(&self) -> bool {
        self.pending
    }

    /// Halve the window and ask again; a window of one revision cannot be.
    fn narrow(&mut self, hi: u64) -> Result<Option<ChangesPage>, ChangesError> {
        if hi <= self.after + 1 {
            return Err(ChangesError::TooLarge);
        }
        self.to = self.after + (hi - self.after) / 2;
        self.stage().map(|()| None)
    }

    /// The store's answer: `Some(page)` with the changes written into
    /// `page`, or `None` when the window was narrowed and asked again.
    pub fn on_response(
        &mut self,
        result: u8,
        revision: u64,
        body: &[u8],
        page: &mut [u8],
    ) -> Result<Option<ChangesPage>, ChangesError> {
        match result {
            KV_RESULT_VERSIONS => {}
            KV_RESULT_COMPACTED => return Err(ChangesError::Compacted),
            KV_RESULT_FUTURE_REV => return Err(ChangesError::Future),
            r => return Err(ChangesError::Store(r)),
        }
        // The window's ceiling: the one asked for, or the store's revision
        // the first answer pinned.
        let hi = if self.to != 0 { self.to } else { revision };
        if self.revision == 0 {
            self.revision = hi;
        }
        if self.after > hi {
            return Err(ChangesError::Future);
        }
        if body.len() < 10 {
            return Err(ChangesError::Answer);
        }
        let next = u64::from_le_bytes([
            body[0], body[1], body[2], body[3], body[4], body[5], body[6], body[7],
        ]);
        let count = usize::from(u16::from_le_bytes([body[8], body[9]]));
        if count > self.order.len() {
            return Err(ChangesError::Answer);
        }
        if next != 0 {
            // The store holds more of this window than one answer: how many
            // changes its revisions hold is not known from this one.
            return self.narrow(hi);
        }
        let mut at = 10usize;
        for i in 0..count {
            let (rev, kind, _, value, nx) = version_entry(body, at).ok_or(ChangesError::Answer)?;
            if rev <= self.after
                || rev > hi
                || kind > CHANGE_DELETE
                || (kind == CHANGE_DELETE && !value.is_empty())
            {
                return Err(ChangesError::Answer);
            }
            self.order[i] = at as u32;
            at = nx;
        }
        // Revision, then key: insertion sort over at most CHANGES_LIMIT.
        for i in 1..count {
            let mut j = i;
            while j > 0 && change_before(body, self.order[j], self.order[j - 1]) {
                self.order.swap(j, j - 1);
                j -= 1;
            }
        }
        let mut written = 0usize;
        let mut i = 0usize;
        while i < count {
            let (rev, ..) =
                version_entry(body, self.order[i] as usize).ok_or(ChangesError::Answer)?;
            let mut n = 1usize;
            while i + n < count
                && version_entry(body, self.order[i + n] as usize).map(|e| e.0) == Some(rev)
            {
                n += 1;
            }
            for idx in 0..n {
                let (_, kind, k, v, _) = version_entry(body, self.order[i + idx] as usize)
                    .ok_or(ChangesError::Answer)?;
                let mut w = written;
                let fits = put16(page, &mut w, &rev.to_le_bytes())
                    .and_then(|()| put16(page, &mut w, &(idx as u16).to_le_bytes()))
                    .and_then(|()| put16(page, &mut w, &(n as u16).to_le_bytes()))
                    .and_then(|()| put16(page, &mut w, &[kind]))
                    .and_then(|()| put16(page, &mut w, &(k.len() as u16).to_le_bytes()))
                    .and_then(|()| put16(page, &mut w, k))
                    .and_then(|()| put16(page, &mut w, &(v.len() as u32).to_le_bytes()))
                    .and_then(|()| put16(page, &mut w, v))
                    .is_some();
                if !fits {
                    // The window's changes do not fit the page: a smaller
                    // window is asked for.
                    return self.narrow(hi);
                }
                written = w;
            }
            i += n;
        }
        Ok(Some(ChangesPage {
            written,
            through: hi,
            revision: self.revision,
            count: count as u32,
        }))
    }
}

/// One change of a changes page, as the consumer reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageChange<'a> {
    pub revision: u64,
    pub index: u16,
    pub count: u16,
    pub kind: u8,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

/// Parse one changes page: `f(change)` per change; `false` for a page
/// that is not whole.
pub fn each_change(page: &[u8], mut f: impl FnMut(PageChange<'_>)) -> bool {
    let mut at = 0usize;
    while at < page.len() {
        let Some(h) = page.get(at..at + CHANGE_HEAD) else {
            return false;
        };
        let mut r = [0u8; 8];
        r.copy_from_slice(&h[0..8]);
        let kl = usize::from(u16::from_le_bytes([h[13], h[14]]));
        let Some(k) = page.get(at + CHANGE_HEAD..at + CHANGE_HEAD + kl) else {
            return false;
        };
        let p = at + CHANGE_HEAD + kl;
        let Some(vl) = page
            .get(p..p + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
        else {
            return false;
        };
        let Some(v) = page.get(p + 4..p + 4 + vl) else {
            return false;
        };
        f(PageChange {
            revision: u64::from_le_bytes(r),
            index: u16::from_le_bytes([h[8], h[9]]),
            count: u16::from_le_bytes([h[10], h[11]]),
            kind: h[12],
            key: k,
            value: v,
        });
        at = p + 4 + vl;
    }
    true
}

// ── Garbage collection ────────────────────────────────────────────────

/// Why a generation is reclaimed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcReason {
    /// Older than the active generation by more than `keep`.
    Superseded,
    /// Its marker is older than the retention.
    Expired,
}

/// Decide a staged generation's fate: never the active or its
/// predecessor; else reclaimed when older than the active by more than
/// `keep` generations, or when its marker is older than `retention_ms`.
pub fn gc_decision(
    generation: u64,
    started_ms: u64,
    active: Option<Active>,
    predecessor: u64,
    keep: u64,
    now_ms: u64,
    retention_ms: u64,
) -> Option<GcReason> {
    if let Some(a) = active {
        if generation == a.generation || generation == predecessor {
            return None;
        }
        if generation < a.generation && a.generation - generation > keep {
            return Some(GcReason::Superseded);
        }
    }
    if now_ms.saturating_sub(started_ms) > retention_ms {
        return Some(GcReason::Expired);
    }
    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcPhase {
    Idle,
    /// Reading the active pointer.
    Active,
    /// Listing the staging markers.
    Markers,
    /// Listing a doomed generation's keys.
    Keys,
    /// Deleting a batch of them.
    Delete,
    /// Deleting the marker.
    Marker,
    Done,
    Failed,
}

/// Generations one pass reclaims at most; the rest wait for the next pass
/// (each lists from the first marker, so none is skipped).
pub const GC_MARKERS: usize = 64;

/// One bounded garbage-collection pass over a service.
pub struct Gc {
    pub service: Service,
    pub phase: GcPhase,
    /// Why a pass failed: the store's result code, or `0xFF` for an answer
    /// that cannot be read (an active pointer that does not decode among
    /// them).
    pub error: u8,
    /// Generations reclaimed, and records deleted, this pass.
    pub reclaimed: u32,
    pub deleted: u32,
    keep: u64,
    retention_ms: u64,
    now_ms: u64,
    active: Option<Active>,
    predecessor: u64,
    doomed: [u64; GC_MARKERS],
    doomed_n: usize,
    doomed_at: usize,
    last_marker_len: u8,
    last_marker: [u8; KEY_MAX],
    pending: bool,
    op: u8,
    body_len: usize,
    body: [u8; BODY_MAX],
}

impl Default for Gc {
    fn default() -> Self {
        Self::new()
    }
}

impl Gc {
    pub const fn new() -> Self {
        Gc {
            service: Service {
                tenant: [0; ID_LEN],
                service: [0; ID_LEN],
            },
            phase: GcPhase::Idle,
            error: 0,
            reclaimed: 0,
            deleted: 0,
            keep: 0,
            retention_ms: 0,
            now_ms: 0,
            active: None,
            predecessor: 0,
            doomed: [0; GC_MARKERS],
            doomed_n: 0,
            doomed_at: 0,
            last_marker_len: 0,
            last_marker: [0; KEY_MAX],
            pending: false,
            op: 0,
            body_len: 0,
            body: [0; BODY_MAX],
        }
    }

    fn stage(&mut self, op: u8, len: usize, phase: GcPhase) {
        self.op = op;
        self.body_len = len;
        self.pending = true;
        self.phase = phase;
    }

    fn fail(&mut self, result: u8) {
        self.phase = GcPhase::Failed;
        self.error = result;
        self.pending = false;
    }

    /// Begin a pass: `predecessor` the active's predecessor (kept with
    /// the active), `keep` generations behind the active kept, markers
    /// older than `retention_ms` reclaimed.
    pub fn begin(
        &mut self,
        service: Service,
        predecessor: u64,
        keep: u64,
        retention_ms: u64,
        now_ms: u64,
    ) -> bool {
        if !matches!(self.phase, GcPhase::Idle | GcPhase::Done | GcPhase::Failed) {
            return false;
        }
        *self = Gc::new();
        self.service = service;
        self.predecessor = predecessor;
        self.keep = keep;
        self.retention_ms = retention_ms;
        self.now_ms = now_ms;
        let mut k = [0u8; KEY_MAX];
        let kn = key::active(&service, &mut k);
        match range_body(&k[..kn], &[], 0, 1, false, &mut self.body) {
            Some(n) => {
                self.stage(KV_OP_RANGE_META, n, GcPhase::Active);
                true
            }
            None => false,
        }
    }

    pub fn step(&mut self, io: &mut impl KvIo) {
        if self.pending && io.request(self.op, &self.body[..self.body_len]) {
            self.pending = false;
        }
    }

    pub fn done(&self) -> bool {
        matches!(self.phase, GcPhase::Done | GcPhase::Failed)
    }

    fn issue_markers(&mut self) {
        let mut start = [0u8; KEY_MAX + 1];
        let mut end = [0u8; KEY_MAX + 1];
        let mut p = [0u8; KEY_MAX];
        let pn = key::stagings(&self.service, &mut p);
        let sn = if self.last_marker_len == 0 {
            start[..pn].copy_from_slice(&p[..pn]);
            pn
        } else {
            successor(
                &self.last_marker[..usize::from(self.last_marker_len)],
                &mut start,
            )
            .unwrap_or(0)
        };
        let en = prefix_end(&p[..pn], &mut end).unwrap_or(0);
        match range_body(
            &start[..sn],
            &end[..en],
            0,
            PAGE_LIMIT,
            false,
            &mut self.body,
        ) {
            Some(n) => self.stage(KV_OP_RANGE_META, n, GcPhase::Markers),
            None => self.fail(0xFF),
        }
    }

    fn issue_keys(&mut self) {
        let Some(&g) = self.doomed.get(self.doomed_at) else {
            return self.phase_done();
        };
        if self.doomed_at >= self.doomed_n {
            return self.phase_done();
        }
        let mut p = [0u8; KEY_MAX];
        let pn = key::generation(&self.service, g, &mut p);
        let mut end = [0u8; KEY_MAX + 1];
        let en = prefix_end(&p[..pn], &mut end).unwrap_or(0);
        match range_body(
            &p[..pn],
            &end[..en],
            0,
            DELETE_BATCH as u32,
            true,
            &mut self.body,
        ) {
            Some(n) => self.stage(KV_OP_RANGE_META, n, GcPhase::Keys),
            None => self.fail(0xFF),
        }
    }

    fn phase_done(&mut self) {
        self.phase = GcPhase::Done;
        self.pending = false;
    }

    pub fn on_response(&mut self, result: u8, body: &[u8]) {
        match self.phase {
            GcPhase::Active => {
                if result != KV_RESULT_KVS {
                    return self.fail(result);
                }
                let Some((_, count)) = kvs_head(body) else {
                    return self.fail(0xFF);
                };
                if count > 0 {
                    let Some((_, v, _, _)) = kvs_entry(body, 11) else {
                        return self.fail(0xFF);
                    };
                    // A pointer that does not decode names no generation
                    // to keep: reclaiming on `None` could take the live one.
                    let Some(a) = Active::decode(v) else {
                        return self.fail(0xFF);
                    };
                    self.active = Some(a);
                }
                self.issue_markers();
            }
            GcPhase::Markers => {
                if result != KV_RESULT_KVS {
                    return self.fail(result);
                }
                let Some((more, count)) = kvs_head(body) else {
                    return self.fail(0xFF);
                };
                let mut at = 11;
                for _ in 0..count {
                    let Some((k, v, _, next)) = kvs_entry(body, at) else {
                        return self.fail(0xFF);
                    };
                    at = next;
                    self.last_marker_len = k.len() as u8;
                    self.last_marker[..k.len()].copy_from_slice(k);
                    let Some(Parsed::Staging { generation }) = parse_key(&self.service, k) else {
                        continue;
                    };
                    // A marker that does not decode is reclaimed as expired:
                    // a torn write names no generation worth keeping.
                    let started = Marker::decode(v).map_or(0, |m| m.started_ms);
                    let doom = gc_decision(
                        generation,
                        started,
                        self.active,
                        self.predecessor,
                        self.keep,
                        self.now_ms,
                        self.retention_ms,
                    );
                    if doom.is_some() && self.doomed_n < GC_MARKERS {
                        self.doomed[self.doomed_n] = generation;
                        self.doomed_n += 1;
                    }
                }
                if more != 0 && count > 0 && self.doomed_n < GC_MARKERS {
                    return self.issue_markers();
                }
                self.issue_keys();
            }
            GcPhase::Keys => {
                if result != KV_RESULT_KVS {
                    return self.fail(result);
                }
                let Some((_, count)) = kvs_head(body) else {
                    return self.fail(0xFF);
                };
                if count == 0 {
                    // Nothing left under it: the marker goes last.
                    let Some(&g) = self.doomed.get(self.doomed_at) else {
                        return self.fail(0xFF);
                    };
                    let mut k = [0u8; KEY_MAX];
                    let kn = key::staging(&self.service, g, &mut k);
                    let mut d = [0u8; BODY_MAX];
                    let mut at = 0;
                    if put16(&mut d, &mut at, &1u16.to_le_bytes()).is_none()
                        || put16(&mut d, &mut at, &(kn as u16).to_le_bytes()).is_none()
                        || put16(&mut d, &mut at, &k[..kn]).is_none()
                    {
                        return self.fail(0xFF);
                    }
                    self.body[..at].copy_from_slice(&d[..at]);
                    return self.stage(KV_OP_DELETE, at, GcPhase::Marker);
                }
                // A delete batch of the keys listed.
                let mut d = [0u8; BODY_MAX];
                let mut at = 2;
                let mut n = 0u16;
                let mut e = 11;
                for _ in 0..count {
                    let Some((k, _, _, next)) = kvs_entry(body, e) else {
                        return self.fail(0xFF);
                    };
                    e = next;
                    if put16(&mut d, &mut at, &(k.len() as u16).to_le_bytes()).is_none()
                        || put16(&mut d, &mut at, k).is_none()
                    {
                        return self.fail(0xFF);
                    }
                    n += 1;
                }
                d[0..2].copy_from_slice(&n.to_le_bytes());
                self.body[..at].copy_from_slice(&d[..at]);
                self.deleted = self.deleted.wrapping_add(u32::from(n));
                self.stage(KV_OP_DELETE, at, GcPhase::Delete);
            }
            // A delete answers the count deleted (`KV_RESULT_INTEGER`).
            GcPhase::Delete => {
                if result != KV_RESULT_INTEGER && result != KV_RESULT_OK {
                    return self.fail(result);
                }
                self.issue_keys();
            }
            GcPhase::Marker => {
                if result != KV_RESULT_INTEGER && result != KV_RESULT_OK {
                    return self.fail(result);
                }
                self.reclaimed = self.reclaimed.wrapping_add(1);
                self.doomed_at += 1;
                self.issue_keys();
            }
            _ => {}
        }
    }
}

// ── Declarations ──────────────────────────────────────────────────────

/// Why a declaration was not written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeclareError {
    /// The record refused structurally (a generation kind among them).
    Envelope(EnvelopeError),
    /// The declaration held is of this generation or a later one
    /// (`held`): declarations only advance.
    Stale { held: u64 },
    /// The declaration held is of this generation with other bytes: one
    /// generation names one record.
    Conflict,
    /// The CAS lost to a racing declaration.
    Raced,
    /// The store refused an operation (its result code).
    Store(u8),
    /// The store answered what cannot be read (the record held among it).
    Answer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeclarePhase {
    Idle,
    /// The record held, read with its `mod_revision`.
    Read,
    /// The CAS on that witness.
    Cas,
    Declared,
    Failed,
}

/// One declaration: a placed record of a kind the publisher names, under
/// an id the publisher names (`…/decl/<kind><id>`), replaced only by a
/// record of a later `record_generation` — read with its `mod_revision`
/// and written by `KV_OP_CAS` on that witness (a create when there is
/// none), so of two racing declarations one wins and the store's record
/// never goes back a generation. The same record again is answered
/// declared, unchanged. Lattice reads the envelope's generation and
/// nothing of its payload.
pub struct Declarer {
    pub service: Service,
    pub kind: u16,
    pub id: [u8; ID_LEN],
    pub generation: u64,
    pub phase: DeclarePhase,
    pub error: Option<DeclareError>,
    /// The revision the record stands at: the CAS's, or, unchanged, the
    /// revision the same record was already written at.
    pub revision: u64,
    pub unchanged: bool,
    record_len: usize,
    record: [u8; RECORD_MAX],
    pending: bool,
    op: u8,
    body_len: usize,
    body: [u8; BODY_MAX],
}

impl Default for Declarer {
    fn default() -> Self {
        Self::new()
    }
}

/// A `KV_OP_RANGE_META` body for the one key `k`.
fn read_one_body(k: &[u8], out: &mut [u8]) -> Option<usize> {
    range_body(k, &[], 0, 1, false, out)
}

/// A `KV_OP_DELETE` body for the one key `k`.
fn delete_one_body(k: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut at = 0;
    put16(out, &mut at, &1u16.to_le_bytes())?;
    put16(out, &mut at, &(k.len() as u16).to_le_bytes())?;
    put16(out, &mut at, k)?;
    Some(at)
}

/// The one entry of a single-key `KV_RESULT_KVS` answer, if any:
/// `Err(())` for an answer that cannot be read.
fn kvs_one(body: &[u8]) -> Result<Option<(&[u8], u64)>, ()> {
    let (_, count) = kvs_head(body).ok_or(())?;
    if count == 0 {
        return Ok(None);
    }
    let (_, v, modr, _) = kvs_entry(body, 11).ok_or(())?;
    Ok(Some((v, modr)))
}

impl Declarer {
    pub const fn new() -> Self {
        Declarer {
            service: Service {
                tenant: [0; ID_LEN],
                service: [0; ID_LEN],
            },
            kind: 0,
            id: [0; ID_LEN],
            generation: 0,
            phase: DeclarePhase::Idle,
            error: None,
            revision: 0,
            unchanged: false,
            record_len: 0,
            record: [0; RECORD_MAX],
            pending: false,
            op: 0,
            body_len: 0,
            body: [0; BODY_MAX],
        }
    }

    pub fn done(&self) -> bool {
        matches!(self.phase, DeclarePhase::Declared | DeclarePhase::Failed)
    }

    fn fail(&mut self, e: DeclareError) {
        self.phase = DeclarePhase::Failed;
        self.error = Some(e);
        self.pending = false;
    }

    fn stage(&mut self, op: u8, len: usize, phase: DeclarePhase) {
        self.op = op;
        self.body_len = len;
        self.pending = true;
        self.phase = phase;
    }

    /// Declare `record` (a placed record) under `id`. `Err` for a record
    /// refused structurally, or one whose put the router cannot carry;
    /// nothing is sent then. `Ok(false)` while a declaration is still in
    /// flight.
    pub fn begin(
        &mut self,
        service: Service,
        id: [u8; ID_LEN],
        record: &[u8],
    ) -> Result<bool, DeclareError> {
        if !matches!(
            self.phase,
            DeclarePhase::Idle | DeclarePhase::Declared | DeclarePhase::Failed
        ) {
            return Ok(false);
        }
        let e = validate_record(record).map_err(DeclareError::Envelope)?;
        if 2 + DECLARATION_KEY_LEN + 8 + 4 + record.len() > KV_BODY_MAX {
            return Err(DeclareError::Envelope(EnvelopeError::TooLarge(
                record.len(),
            )));
        }
        *self = Declarer::new();
        self.service = service;
        self.kind = e.kind;
        self.id = id;
        self.generation = e.generation;
        self.record[..record.len()].copy_from_slice(record);
        self.record_len = record.len();
        let mut k = [0u8; KEY_MAX];
        let kn = key::declaration(&service, e.kind, &id, &mut k);
        match read_one_body(&k[..kn], &mut self.body) {
            Some(n) => self.stage(KV_OP_RANGE_META, n, DeclarePhase::Read),
            None => return Err(DeclareError::Answer),
        }
        Ok(true)
    }

    pub fn step(&mut self, io: &mut impl KvIo) {
        if self.pending && io.request(self.op, &self.body[..self.body_len]) {
            self.pending = false;
        }
    }

    pub fn on_response(&mut self, result: u8, revision: u64, body: &[u8]) {
        match self.phase {
            DeclarePhase::Read => {
                if result != KV_RESULT_KVS {
                    return self.fail(DeclareError::Store(result));
                }
                let witness = match kvs_one(body) {
                    Err(()) => return self.fail(DeclareError::Answer),
                    Ok(None) => 0,
                    Ok(Some((v, modr))) => {
                        let Ok(held) = validate_record(v) else {
                            return self.fail(DeclareError::Answer);
                        };
                        if held.generation == self.generation {
                            if v == &self.record[..self.record_len] {
                                self.revision = modr;
                                self.unchanged = true;
                                self.phase = DeclarePhase::Declared;
                                return;
                            }
                            return self.fail(DeclareError::Conflict);
                        }
                        if held.generation > self.generation {
                            return self.fail(DeclareError::Stale {
                                held: held.generation,
                            });
                        }
                        modr
                    }
                };
                let mut k = [0u8; KEY_MAX];
                let kn = key::declaration(&self.service, self.kind, &self.id, &mut k);
                match cas_body(
                    &k[..kn],
                    witness,
                    &self.record[..self.record_len],
                    &mut self.body,
                ) {
                    Some(n) => self.stage(KV_OP_CAS, n, DeclarePhase::Cas),
                    None => self.fail(DeclareError::Answer),
                }
            }
            DeclarePhase::Cas => match result {
                KV_RESULT_OK => {
                    self.revision = revision;
                    self.phase = DeclarePhase::Declared;
                }
                KV_RESULT_CAS_FAILED => self.fail(DeclareError::Raced),
                r => self.fail(DeclareError::Store(r)),
            },
            _ => {}
        }
    }
}

// ── Status streams ────────────────────────────────────────────────────

/// Why a status was not appended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusError {
    Envelope(EnvelopeError),
    /// The stream's last status is published: nothing follows it.
    Closed,
    /// The next number already holds another status: another appender
    /// writes this stream (one appender per operation).
    Conflict,
    /// The head's CAS lost to a racing append.
    Raced,
    /// The head does not decode: the stream cannot be continued.
    Head,
    Store(u8),
    Answer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusPhase {
    Idle,
    /// The head, read with its `mod_revision`.
    ReadHead,
    /// The status's create under the next number.
    Create,
    /// A status already under that number, read back.
    ReadBack,
    /// The head's CAS.
    Advance,
    Appended,
    Failed,
}

/// One operation's status stream, appended a status at a time and gap
/// free: each status is created only if absent under the next number
/// (`…/op/<operation>/<seq>`, from 1), then the head (`…/op/<operation>/
/// head`, `LSH1`) advanced to it by `KV_OP_CAS` on the head's witness,
/// its chain extended by the status — so what is published is exactly the
/// statuses `1..=seq`, and a reader verifies it holds every one of them
/// by the head's chain. A status above the head is staged, not published.
/// An append interrupted before its head moved is finished by appending
/// the same status again (found under its number, byte for byte); another
/// status found there is a [`StatusError::Conflict`]. The last status
/// closes the stream ([`HEAD_CLOSED`]). Lattice reads the envelope and
/// nothing of the payload: which status is last is the appender's word.
pub struct StatusLog {
    pub service: Service,
    pub operation: [u8; ID_LEN],
    pub phase: StatusPhase,
    pub error: Option<StatusError>,
    /// The number the status was published under, and the head's
    /// revision (the watcher-visible one).
    pub seq: u64,
    pub revision: u64,
    last: bool,
    witness: u64,
    chain: ManifestDigest,
    record_len: usize,
    record: [u8; RECORD_MAX],
    pending: bool,
    op: u8,
    body_len: usize,
    body: [u8; BODY_MAX],
}

impl Default for StatusLog {
    fn default() -> Self {
        Self::new()
    }
}

impl StatusLog {
    pub const fn new() -> Self {
        StatusLog {
            service: Service {
                tenant: [0; ID_LEN],
                service: [0; ID_LEN],
            },
            operation: [0; ID_LEN],
            phase: StatusPhase::Idle,
            error: None,
            seq: 0,
            revision: 0,
            last: false,
            witness: 0,
            chain: ManifestDigest::new(),
            record_len: 0,
            record: [0; RECORD_MAX],
            pending: false,
            op: 0,
            body_len: 0,
            body: [0; BODY_MAX],
        }
    }

    pub fn done(&self) -> bool {
        matches!(self.phase, StatusPhase::Appended | StatusPhase::Failed)
    }

    fn fail(&mut self, e: StatusError) {
        self.phase = StatusPhase::Failed;
        self.error = Some(e);
        self.pending = false;
    }

    fn stage(&mut self, op: u8, len: usize, phase: StatusPhase) {
        self.op = op;
        self.body_len = len;
        self.pending = true;
        self.phase = phase;
    }

    /// Append `record` (a placed record) to `operation`'s stream; `last`
    /// closes it. `Err` for a record refused structurally or too large to
    /// route; `Ok(false)` while an append is still in flight.
    pub fn begin(
        &mut self,
        service: Service,
        operation: [u8; ID_LEN],
        record: &[u8],
        last: bool,
    ) -> Result<bool, StatusError> {
        if !matches!(
            self.phase,
            StatusPhase::Idle | StatusPhase::Appended | StatusPhase::Failed
        ) {
            return Ok(false);
        }
        validate_record(record).map_err(StatusError::Envelope)?;
        if 2 + STATUS_KEY_LEN + 8 + 4 + record.len() > KV_BODY_MAX {
            return Err(StatusError::Envelope(EnvelopeError::TooLarge(record.len())));
        }
        *self = StatusLog::new();
        self.service = service;
        self.operation = operation;
        self.last = last;
        self.record[..record.len()].copy_from_slice(record);
        self.record_len = record.len();
        let mut k = [0u8; KEY_MAX];
        let kn = key::status_head(&service, &operation, &mut k);
        match read_one_body(&k[..kn], &mut self.body) {
            Some(n) => self.stage(KV_OP_RANGE_META, n, StatusPhase::ReadHead),
            None => return Err(StatusError::Answer),
        }
        Ok(true)
    }

    pub fn step(&mut self, io: &mut impl KvIo) {
        if self.pending && io.request(self.op, &self.body[..self.body_len]) {
            self.pending = false;
        }
    }

    fn status_key(&self, seq: u64, out: &mut [u8; KEY_MAX]) -> usize {
        key::status(&self.service, &self.operation, seq, out)
    }

    fn advance(&mut self) {
        let mut k = [0u8; KEY_MAX];
        let kn = self.status_key(self.seq, &mut k);
        if !self
            .chain
            .absorb_record(&k[..kn], &self.record[..self.record_len])
        {
            return self.fail(StatusError::Answer);
        }
        let head = StatusHead {
            seq: self.seq,
            flags: if self.last { HEAD_CLOSED } else { 0 },
            chain: self.chain.chain,
        };
        let mut v = [0u8; HEAD_LEN];
        let vn = head.encode(&mut v).unwrap_or(0);
        let kn = key::status_head(&self.service, &self.operation, &mut k);
        match cas_body(&k[..kn], self.witness, &v[..vn], &mut self.body) {
            Some(n) => self.stage(KV_OP_CAS, n, StatusPhase::Advance),
            None => self.fail(StatusError::Answer),
        }
    }

    pub fn on_response(&mut self, result: u8, revision: u64, body: &[u8]) {
        match self.phase {
            StatusPhase::ReadHead => {
                if result != KV_RESULT_KVS {
                    return self.fail(StatusError::Store(result));
                }
                match kvs_one(body) {
                    Err(()) => return self.fail(StatusError::Answer),
                    Ok(None) => {
                        self.seq = 1;
                        self.witness = 0;
                        self.chain = ManifestDigest::new();
                    }
                    Ok(Some((v, modr))) => {
                        let Some(h) = StatusHead::decode(v) else {
                            return self.fail(StatusError::Head);
                        };
                        if h.closed() {
                            return self.fail(StatusError::Closed);
                        }
                        let mut k = [0u8; KEY_MAX];
                        let kn = self.status_key(h.seq, &mut k);
                        let Some(chain) = ManifestDigest::resume(h.chain, h.seq as u32, &k[..kn])
                        else {
                            return self.fail(StatusError::Head);
                        };
                        self.chain = chain;
                        self.seq = h.seq + 1;
                        self.witness = modr;
                    }
                }
                let mut k = [0u8; KEY_MAX];
                let kn = self.status_key(self.seq, &mut k);
                match cas_body(&k[..kn], 0, &self.record[..self.record_len], &mut self.body) {
                    Some(n) => self.stage(KV_OP_CAS, n, StatusPhase::Create),
                    None => self.fail(StatusError::Answer),
                }
            }
            StatusPhase::Create => match result {
                KV_RESULT_OK => self.advance(),
                KV_RESULT_CAS_FAILED => {
                    let mut k = [0u8; KEY_MAX];
                    let kn = self.status_key(self.seq, &mut k);
                    match read_one_body(&k[..kn], &mut self.body) {
                        Some(n) => self.stage(KV_OP_RANGE_META, n, StatusPhase::ReadBack),
                        None => self.fail(StatusError::Answer),
                    }
                }
                r => self.fail(StatusError::Store(r)),
            },
            StatusPhase::ReadBack => {
                if result != KV_RESULT_KVS {
                    return self.fail(StatusError::Store(result));
                }
                match kvs_one(body) {
                    Ok(Some((v, _))) if v == &self.record[..self.record_len] => self.advance(),
                    Ok(_) => self.fail(StatusError::Conflict),
                    Err(()) => self.fail(StatusError::Answer),
                }
            }
            StatusPhase::Advance => match result {
                KV_RESULT_OK => {
                    self.revision = revision;
                    self.phase = StatusPhase::Appended;
                }
                KV_RESULT_CAS_FAILED => self.fail(StatusError::Raced),
                r => self.fail(StatusError::Store(r)),
            },
            _ => {}
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReclaimPhase {
    Idle,
    ReadHead,
    /// The head's delete: the stream is withdrawn first.
    DeleteHead,
    /// Listing a batch of the statuses left.
    Keys,
    Delete,
    Done,
    Failed,
}

/// Reclaim one closed status stream: the head is deleted first, so the
/// stream is withdrawn whole at one revision (a reader holding it sees
/// the head go and nothing half-published), then its statuses a
/// [`DELETE_BATCH`] at a time; an interrupted pass is finished by the
/// next, which finds no head and deletes what is left. A stream still
/// open is refused (`error` `0xFE`): which operation to reclaim, and
/// when, is the caller's retention to decide.
pub struct Reclaim {
    pub service: Service,
    pub operation: [u8; ID_LEN],
    pub phase: ReclaimPhase,
    /// The store's result code, `0xFE` for a stream still open, `0xFF`
    /// for an answer that cannot be read.
    pub error: u8,
    pub deleted: u32,
    pending: bool,
    op: u8,
    body_len: usize,
    body: [u8; BODY_MAX],
}

impl Default for Reclaim {
    fn default() -> Self {
        Self::new()
    }
}

impl Reclaim {
    pub const fn new() -> Self {
        Reclaim {
            service: Service {
                tenant: [0; ID_LEN],
                service: [0; ID_LEN],
            },
            operation: [0; ID_LEN],
            phase: ReclaimPhase::Idle,
            error: 0,
            deleted: 0,
            pending: false,
            op: 0,
            body_len: 0,
            body: [0; BODY_MAX],
        }
    }

    pub fn done(&self) -> bool {
        matches!(self.phase, ReclaimPhase::Done | ReclaimPhase::Failed)
    }

    fn fail(&mut self, result: u8) {
        self.phase = ReclaimPhase::Failed;
        self.error = result;
        self.pending = false;
    }

    fn stage(&mut self, op: u8, len: usize, phase: ReclaimPhase) {
        self.op = op;
        self.body_len = len;
        self.pending = true;
        self.phase = phase;
    }

    pub fn begin(&mut self, service: Service, operation: [u8; ID_LEN]) -> bool {
        if !matches!(
            self.phase,
            ReclaimPhase::Idle | ReclaimPhase::Done | ReclaimPhase::Failed
        ) {
            return false;
        }
        *self = Reclaim::new();
        self.service = service;
        self.operation = operation;
        let mut k = [0u8; KEY_MAX];
        let kn = key::status_head(&service, &operation, &mut k);
        match read_one_body(&k[..kn], &mut self.body) {
            Some(n) => {
                self.stage(KV_OP_RANGE_META, n, ReclaimPhase::ReadHead);
                true
            }
            None => false,
        }
    }

    pub fn step(&mut self, io: &mut impl KvIo) {
        if self.pending && io.request(self.op, &self.body[..self.body_len]) {
            self.pending = false;
        }
    }

    fn issue_keys(&mut self) {
        let mut p = [0u8; KEY_MAX];
        let pn = key::statuses(&self.service, &self.operation, &mut p);
        let mut end = [0u8; KEY_MAX + 1];
        let en = prefix_end(&p[..pn], &mut end).unwrap_or(0);
        match range_body(
            &p[..pn],
            &end[..en],
            0,
            DELETE_BATCH as u32,
            true,
            &mut self.body,
        ) {
            Some(n) => self.stage(KV_OP_RANGE_META, n, ReclaimPhase::Keys),
            None => self.fail(0xFF),
        }
    }

    pub fn on_response(&mut self, result: u8, body: &[u8]) {
        match self.phase {
            ReclaimPhase::ReadHead => {
                if result != KV_RESULT_KVS {
                    return self.fail(result);
                }
                match kvs_one(body) {
                    Err(()) => self.fail(0xFF),
                    Ok(None) => self.issue_keys(),
                    Ok(Some((v, _))) => {
                        // A head that does not decode is no open stream's:
                        // it is withdrawn like a closed one.
                        if StatusHead::decode(v).is_some_and(|h| !h.closed()) {
                            return self.fail(0xFE);
                        }
                        let mut k = [0u8; KEY_MAX];
                        let kn = key::status_head(&self.service, &self.operation, &mut k);
                        match delete_one_body(&k[..kn], &mut self.body) {
                            Some(n) => self.stage(KV_OP_DELETE, n, ReclaimPhase::DeleteHead),
                            None => self.fail(0xFF),
                        }
                    }
                }
            }
            ReclaimPhase::DeleteHead | ReclaimPhase::Delete => {
                if result != KV_RESULT_INTEGER && result != KV_RESULT_OK {
                    return self.fail(result);
                }
                self.issue_keys();
            }
            ReclaimPhase::Keys => {
                if result != KV_RESULT_KVS {
                    return self.fail(result);
                }
                let Some((_, count)) = kvs_head(body) else {
                    return self.fail(0xFF);
                };
                if count == 0 {
                    self.phase = ReclaimPhase::Done;
                    return;
                }
                let mut d = [0u8; BODY_MAX];
                let mut at = 2;
                let mut n = 0u16;
                let mut e = 11;
                for _ in 0..count {
                    let Some((k, _, _, next)) = kvs_entry(body, e) else {
                        return self.fail(0xFF);
                    };
                    e = next;
                    if put16(&mut d, &mut at, &(k.len() as u16).to_le_bytes()).is_none()
                        || put16(&mut d, &mut at, k).is_none()
                    {
                        return self.fail(0xFF);
                    }
                    n += 1;
                }
                d[0..2].copy_from_slice(&n.to_le_bytes());
                self.body[..at].copy_from_slice(&d[..at]);
                self.deleted = self.deleted.wrapping_add(u32::from(n));
                self.stage(KV_OP_DELETE, at, ReclaimPhase::Delete);
            }
            _ => {}
        }
    }
}

// ── Shards across key/partition groups ───────────────────────────────

/// The range boundary that begins catalogue shard `shard` of `tenant`:
/// `rc/<tenant>/<shard>`. A service's catalogue lies under its service
/// prefix, whose leading service byte is its shard, so an ordered range
/// map whose boundaries are only these keys (and the tenant's own prefix)
/// places every shard — every key a service's catalogue holds, its
/// snapshot's one span and its watch's one prefix — in one range, and so
/// in one key/partition group. A boundary anywhere else may split a
/// service, whose snapshot is then refused `KV_RESULT_CROSS_RANGE`, never
/// answered from one side. Writes `SERVICE_PREFIX_LEN - ID_LEN` bytes.
pub fn shard_boundary(tenant: &[u8; ID_LEN], shard: u8, out: &mut [u8]) -> Option<usize> {
    let n = 3 + ID_LEN + 2;
    let o = out.get_mut(..n)?;
    o[0..3].copy_from_slice(b"rc/");
    o[3..3 + ID_LEN].copy_from_slice(tenant);
    o[3 + ID_LEN] = b'/';
    o[4 + ID_LEN] = shard;
    Some(n)
}
