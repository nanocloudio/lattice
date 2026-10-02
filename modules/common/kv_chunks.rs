//! Large values: a hash tree of content-addressed chunks and the small
//! record that names its root — the contract the store worker and every
//! value producer share.
//!
//! No component holds a whole large value, and nothing that travels is
//! large. A value that does not fit one replicated write is cut into DATA
//! chunks; NODE chunks list up to [`FANOUT`] children's hashes; the value's
//! key holds a constant-size manifest naming the root. Each chunk is its
//! own write (`KV_OP_PUT_CHUNK`): the worker checks the bytes against their
//! SHA-256, stores them once, and accepts a NODE only when every child it
//! names is present — so by induction a present root is a whole value, and
//! the record's compared write checks one key. A reader fetches the tree
//! one bounded answer at a time.
//!
//! Chunks are immutable and named by their content, so a retry, a
//! duplicate or a concurrent writer of the same bytes is harmless.
//! References are replicated state: a record holds its root, a node its
//! children; a manifest a write supersedes queues its root's release at
//! that revision, and a replicated sweep releases it once the committed
//! compaction floor passes (history may still read it until then),
//! cascading down the tree a bounded amount per command.
//!
//! Contracts only — no ports, no I/O. The chunk records live under the
//! store's own identity keyspaces (`models::KS_CHUNK_*`) beside the
//! writer's tenant and database: no request may name one (the worker
//! refuses it), so no ordinary key can collide with a chunk record, and
//! only the chunk ops, which bind those identities themselves, write them.
//!
//! ## Chunks
//!
//! `[kind:u8][payload]`, at most [`CHUNK_MAX`] bytes: the largest payload
//! one replicated `KV_OP_PUT_CHUNK` carries (the proposal body
//! `wire::CONSENSUS_PROPOSAL_BODY_MAX` 2040, less the entry tag 1, the
//! command head 35, and the op's hash 32 and length 2). `KIND_DATA`: up to
//! [`DATA_MAX`] bytes of the value. `KIND_NODE`: up to [`FANOUT`] child
//! hashes, in value order.
//!
//! ## Manifest
//!
//! `[MANIFEST_MAGIC:2][MANIFEST_V1][total:u64 LE][depth:u8][root:32]` —
//! [`MANIFEST_LEN`] bytes whatever the value's size. `depth` counts NODE
//! levels above the data (a value of at most `FANOUT` data chunks has
//! depth 1). A value within [`DATA_MAX`] bytes is stored inline and costs
//! what it always did.

pub mod sha {
    include!("../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
}

use sha::Sha256;

pub const HASH_LEN: usize = 32;
pub type Hash = [u8; HASH_LEN];

/// The largest chunk (2040 − 1 − 35 − 32 − 2). A contract test holds it
/// to the derivation.
pub const CHUNK_MAX: usize = 1970;
/// A data chunk's share of the value.
pub const DATA_MAX: usize = CHUNK_MAX - 1;
/// The largest one replicated write's command body may be: the proposal
/// bound (`wire::CONSENSUS_PROPOSAL_BODY_MAX`, 2040) less the entry tag
/// and the command head — exactly a chunk upload. A write whose request
/// is larger goes with values as chunks until it is not.
pub const ENTRY_BODY_MAX: usize = HASH_LEN + 2 + CHUNK_MAX;
/// Children one node names.
pub const FANOUT: usize = (CHUNK_MAX - 1) / HASH_LEN;
/// Levels of nodes a value may have: enough for any value a u64 total can
/// state at 61-way fanout (61^9 data chunks) — a producer's and a
/// reader's per-level state (one node each) is bounded by it.
pub const MAX_DEPTH: usize = 9;

pub const KIND_DATA: u8 = 0;
pub const KIND_NODE: u8 = 1;

/// A manifest's first bytes, so a value that is one is recognised.
pub const MANIFEST_MAGIC: [u8; 2] = [0xC4, b'M'];
pub const MANIFEST_V1: u8 = 1;
pub const MANIFEST_LEN: usize = 2 + 1 + 8 + 1 + HASH_LEN;

/// The store's own identity keyspaces for chunk records (mirrors
/// `models::KS_CHUNK_*`; a contract test holds them equal).
pub const KS_CHUNK_DATA: u32 = 0x8008_0001;
pub const KS_CHUNK_REF: u32 = 0x8008_0002;
pub const KS_CHUNK_RETIRE: u32 = 0x8008_0003;
pub const KS_CHUNK_ZERO: u32 = 0x8008_0004;

/// Is `keyspace` one of the store's own chunk keyspaces, which no request
/// may name?
pub fn is_chunk_keyspace(keyspace: u32) -> bool {
    (KS_CHUNK_DATA..=KS_CHUNK_ZERO).contains(&keyspace)
}

pub fn sha256(data: &[u8]) -> Hash {
    sha256_of(&[data])
}

/// The SHA-256 of `parts`, one after another.
pub fn sha256_of(parts: &[&[u8]]) -> Hash {
    let mut h = Sha256::new();
    for p in parts {
        h.update(p);
    }
    h.finalize()
}

/// Can a value of `len` bytes be stored inline? (A writer whose entry
/// has no room for it chunks it anyway.)
pub fn fits_inline(len: usize) -> bool {
    len <= DATA_MAX
}

/// How many data chunks a value of `total` bytes takes.
pub fn data_chunks(total: u64) -> u64 {
    total.div_ceil(DATA_MAX as u64)
}

/// Node levels above `leaves` data chunks (at least 1).
pub fn depth_for(leaves: u64) -> u8 {
    let mut d = 1u8;
    let mut span = FANOUT as u64;
    while span < leaves {
        span = span.saturating_mul(FANOUT as u64);
        d += 1;
    }
    d
}

// ── Chunks ────────────────────────────────────────────────────────────

/// A stored chunk's kind and payload, validated for shape.
pub fn parse_chunk(c: &[u8]) -> Option<(u8, &[u8])> {
    let (&kind, payload) = c.split_first()?;
    if c.len() > CHUNK_MAX || payload.is_empty() {
        return None;
    }
    match kind {
        KIND_DATA => Some((kind, payload)),
        KIND_NODE if payload.len() % HASH_LEN == 0 => Some((kind, payload)),
        _ => None,
    }
}

/// A node's children.
pub fn children(payload: &[u8]) -> impl Iterator<Item = Hash> + '_ {
    payload
        .chunks_exact(HASH_LEN)
        .filter_map(|h| h.try_into().ok())
}

// ── Keys ──────────────────────────────────────────────────────────────

/// A chunk's key (in `KS_CHUNK_DATA`, and its references' in
/// `KS_CHUNK_REF`): its hash.
pub fn hash_key(hash: &Hash) -> &[u8] {
    hash
}

/// The chunk queues (`KS_CHUNK_RETIRE`, `KS_CHUNK_ZERO`) are the store's,
/// not an identity's: they live at this one tenant and database and name
/// the identity each entry belongs to, so one sweep finds everything due
/// in the store.
pub const QUEUE_TENANT: u32 = 0;
pub const QUEUE_DATABASE: u32 = 0;

/// A queued chunk's key (a release in `KS_CHUNK_RETIRE`, an unreferenced
/// chunk in `KS_CHUNK_ZERO`): `[rev BE][tenant BE][database BE][hash]`,
/// so the queue sorts by the revision it waits for.
pub const QUEUED_KEY_LEN: usize = 8 + 4 + 4 + HASH_LEN;

pub fn queued_key_of(rev: u64, tenant: u32, database: u32, hash: &Hash) -> [u8; QUEUED_KEY_LEN] {
    let mut k = [0u8; QUEUED_KEY_LEN];
    k[..8].copy_from_slice(&rev.to_be_bytes());
    k[8..12].copy_from_slice(&tenant.to_be_bytes());
    k[12..16].copy_from_slice(&database.to_be_bytes());
    k[16..].copy_from_slice(hash);
    k
}

/// One queue entry: the revision it waits for, the identity its chunk
/// belongs to, and the chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Queued {
    pub rev: u64,
    pub tenant: u32,
    pub database: u32,
    pub hash: Hash,
}

/// A queued chunk key's entry.
pub fn queued_key(k: &[u8]) -> Option<Queued> {
    if k.len() != QUEUED_KEY_LEN {
        return None;
    }
    Some(Queued {
        rev: u64::from_be_bytes(k.get(..8)?.try_into().ok()?),
        tenant: u32::from_be_bytes(k.get(8..12)?.try_into().ok()?),
        database: u32::from_be_bytes(k.get(12..16)?.try_into().ok()?),
        hash: k.get(16..)?.try_into().ok()?,
    })
}

/// An unreferenced chunk's queue value: `[queued_ms:u64]`, the committed
/// clock when it was queued. A sweep takes only chunks queued before the
/// bound it names, which is how a graph gives producers mid-upload and
/// readers mid-fetch their grace.
pub const ZERO_VALUE_LEN: usize = 8;

/// A chunk's references and the revision they last changed at (a chunk
/// at zero waits in ZERO under it): `[refs:u64][since:u64]`.
pub const REF_LEN: usize = 16;

pub fn encode_ref(refs: u64, since: u64) -> [u8; REF_LEN] {
    let mut b = [0u8; REF_LEN];
    b[..8].copy_from_slice(&refs.to_le_bytes());
    b[8..].copy_from_slice(&since.to_le_bytes());
    b
}

pub fn decode_ref(b: &[u8]) -> Option<(u64, u64)> {
    if b.len() != REF_LEN {
        return None;
    }
    Some((
        u64::from_le_bytes(b.get(..8)?.try_into().ok()?),
        u64::from_le_bytes(b.get(8..)?.try_into().ok()?),
    ))
}

// ── Manifests ─────────────────────────────────────────────────────────

/// A validated manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub total: u64,
    pub depth: u8,
    pub root: Hash,
}

impl Manifest {
    /// A value that is a manifest; `None` for any other value (inline).
    pub fn parse(b: &[u8]) -> Option<Self> {
        if b.len() != MANIFEST_LEN || b.get(..2)? != MANIFEST_MAGIC || *b.get(2)? != MANIFEST_V1 {
            return None;
        }
        let total = u64::from_le_bytes(b.get(3..11)?.try_into().ok()?);
        let depth = *b.get(11)?;
        let root: Hash = b.get(12..)?.try_into().ok()?;
        // Any length may be chunked: a writer chunks a value its entry has
        // no room for inline, which under replication is less than
        // `DATA_MAX` with a long key. The depth must be the one the length
        // implies.
        if total == 0 || depth != depth_for(data_chunks(total)) {
            return None;
        }
        Some(Manifest { total, depth, root })
    }

    pub fn encode(&self) -> [u8; MANIFEST_LEN] {
        let mut b = [0u8; MANIFEST_LEN];
        b[..2].copy_from_slice(&MANIFEST_MAGIC);
        b[2] = MANIFEST_V1;
        b[3..11].copy_from_slice(&self.total.to_le_bytes());
        b[11] = self.depth;
        b[12..].copy_from_slice(&self.root);
        b
    }
}

// ── Producing a value ─────────────────────────────────────────────────

/// Cuts a value into its tree, one chunk at a time in upload order —
/// every child before its parent — holding one partial node per level.
/// The producer feeds the value's bytes in order (it need not hold the
/// value whole) and uploads each chunk [`Builder::next`] yields. All zero
/// is an idle builder ([`Builder::IDLE`]), which [`Builder::start`] sets
/// going where it lies.
pub struct Builder {
    total: u64,
    depth: u8,
    fed: u64,
    data: [u8; DATA_MAX],
    data_len: usize,
    /// Per level (0 = the nodes above data): the child hashes gathered.
    levels: [[u8; FANOUT * HASH_LEN]; MAX_DEPTH],
    counts: [usize; MAX_DEPTH],
    /// The root, once the last node has closed (`rooted`).
    root: Hash,
    rooted: bool,
}

impl Builder {
    /// A builder with no value.
    pub const IDLE: Builder = Builder {
        total: 0,
        depth: 0,
        fed: 0,
        data: [0; DATA_MAX],
        data_len: 0,
        levels: [[0; FANOUT * HASH_LEN]; MAX_DEPTH],
        counts: [0; MAX_DEPTH],
        root: [0; HASH_LEN],
        rooted: false,
    };

    /// For a value of `total` bytes (at least one). A value that fits
    /// inline is chunked only when its writer's entry has no room for it.
    pub fn new(total: u64) -> Option<Self> {
        let mut b = Builder::IDLE;
        b.start(total).then_some(b)
    }

    /// Start a value of `total` bytes here, whatever this builder held:
    /// `false` (and idle) when no tree holds it.
    pub fn start(&mut self, total: u64) -> bool {
        let depth = depth_for(data_chunks(total));
        let ok = total != 0 && depth as usize <= MAX_DEPTH;
        self.total = if ok { total } else { 0 };
        self.depth = if ok { depth } else { 0 };
        self.fed = 0;
        self.data_len = 0;
        self.counts = [0; MAX_DEPTH];
        self.rooted = false;
        ok
    }

    /// The value's length (0: idle).
    pub fn total(&self) -> u64 {
        self.total
    }

    /// How many of the value's bytes have been fed.
    pub fn fed(&self) -> u64 {
        self.fed
    }

    /// How many of `bytes` to feed next: [`Builder::feed`] takes at most a
    /// data chunk's worth at a time.
    pub fn room(&self) -> usize {
        DATA_MAX - self.data_len
    }

    /// Feed the next value bytes (at most [`Builder::room`]).
    pub fn feed(&mut self, bytes: &[u8]) -> Option<()> {
        let d = self
            .data
            .get_mut(self.data_len..self.data_len + bytes.len())?;
        d.copy_from_slice(bytes);
        self.data_len += bytes.len();
        self.fed += bytes.len() as u64;
        (self.fed <= self.total).then_some(())
    }

    /// The next chunk to upload, when one is complete, into `out`: its
    /// length and hash. Call until `None` after each feed; at the end of
    /// the value the last nodes close and the root is known.
    pub fn next(&mut self, out: &mut [u8; CHUNK_MAX]) -> Option<(usize, Hash)> {
        let done = self.fed == self.total;
        // A full data chunk (or the value's last).
        if self.data_len == DATA_MAX || (done && self.data_len > 0) {
            out[0] = KIND_DATA;
            out[1..1 + self.data_len].copy_from_slice(&self.data[..self.data_len]);
            let n = 1 + self.data_len;
            self.data_len = 0;
            let h = sha256(&out[..n]);
            self.push(0, h);
            return Some((n, h));
        }
        // A full node (or, at the end, every open one, lowest first).
        for l in 0..self.depth as usize {
            let top = l + 1 == self.depth as usize;
            let full = self.counts[l] == FANOUT;
            let close = done && self.counts[l] > 0 && self.lower_empty(l);
            if full || close {
                let k = self.counts[l];
                out[0] = KIND_NODE;
                out[1..1 + k * HASH_LEN].copy_from_slice(&self.levels[l][..k * HASH_LEN]);
                let n = 1 + k * HASH_LEN;
                self.counts[l] = 0;
                let h = sha256(&out[..n]);
                if top {
                    self.root = h;
                    self.rooted = true;
                } else {
                    self.push(l + 1, h);
                }
                return Some((n, h));
            }
        }
        None
    }

    fn lower_empty(&self, l: usize) -> bool {
        self.data_len == 0 && self.counts[..l].iter().all(|&c| c == 0)
    }

    fn push(&mut self, l: usize, h: Hash) {
        let k = self.counts[l];
        self.levels[l][k * HASH_LEN..(k + 1) * HASH_LEN].copy_from_slice(&h);
        self.counts[l] = k + 1;
    }

    /// The manifest, once every chunk has been yielded.
    pub fn manifest(&self) -> Option<Manifest> {
        self.rooted.then_some(Manifest {
            total: self.total,
            depth: self.depth,
            root: self.root,
        })
    }
}

// ── Reading a value ───────────────────────────────────────────────────

/// Walks a manifest's tree depth-first, holding one node per level: it
/// names the next chunk to fetch, checks each fetched chunk against the
/// hash it was fetched by, and hands the value's bytes over in order —
/// checked against the manifest's total at the end.
pub struct Reader {
    m: Manifest,
    nodes: [[u8; FANOUT * HASH_LEN]; MAX_DEPTH],
    counts: [usize; MAX_DEPTH],
    at: [usize; MAX_DEPTH],
    /// Levels holding a node (0: none yet; the root is level depth − 1).
    held: usize,
    delivered: u64,
}

/// What a fetched chunk contributed.
#[derive(Debug, PartialEq, Eq)]
pub enum Step<'a> {
    /// Value bytes, in order.
    Data(&'a [u8]),
    /// A node: its children come next.
    Node,
}

impl Reader {
    pub fn new(m: Manifest) -> Self {
        Reader {
            m,
            nodes: [[0; FANOUT * HASH_LEN]; MAX_DEPTH],
            counts: [0; MAX_DEPTH],
            at: [0; MAX_DEPTH],
            held: 0,
            delivered: 0,
        }
    }

    /// The hash of the chunk to fetch next; `None` when the value is
    /// complete.
    pub fn want(&self) -> Option<Hash> {
        if self.held == 0 {
            return (self.delivered == 0).then_some(self.m.root);
        }
        let l = self.lowest();
        let i = self.at[l];
        if i >= self.counts[l] {
            return None;
        }
        self.nodes[l][i * HASH_LEN..(i + 1) * HASH_LEN]
            .try_into()
            .ok()
    }

    /// The level of the lowest node held.
    fn lowest(&self) -> usize {
        self.m.depth as usize - self.held
    }

    /// Take the chunk [`Reader::want`] named. `None`: it is not that
    /// chunk, or not the kind the tree has there.
    pub fn take<'c>(&mut self, chunk: &'c [u8]) -> Option<Step<'c>> {
        let want = self.want()?;
        if sha256(chunk) != want {
            return None;
        }
        let (kind, payload) = parse_chunk(chunk)?;
        let expect_node = self.held < self.m.depth as usize;
        match (kind, expect_node) {
            (KIND_NODE, true) => {
                let l = self.m.depth as usize - self.held - 1;
                let k = payload.len() / HASH_LEN;
                if k > FANOUT {
                    return None;
                }
                self.nodes[l][..payload.len()].copy_from_slice(payload);
                self.counts[l] = k;
                self.at[l] = 0;
                self.held += 1;
                Some(Step::Node)
            }
            (KIND_DATA, false) => {
                self.delivered += payload.len() as u64;
                if self.delivered > self.m.total {
                    return None;
                }
                self.advance();
                Some(Step::Data(payload))
            }
            _ => None,
        }
    }

    /// Past the child just delivered: next sibling, or up a level.
    fn advance(&mut self) {
        while self.held > 0 {
            let l = self.lowest();
            self.at[l] += 1;
            if self.at[l] < self.counts[l] {
                return;
            }
            // Level exhausted: drop it, and step past it in its parent.
            self.held -= 1;
        }
    }

    /// Was the whole value delivered?
    pub fn complete(&self) -> bool {
        self.want().is_none() && self.delivered == self.m.total
    }
}
