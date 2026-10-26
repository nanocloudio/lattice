//! route_catalogue_reader — the director's side of the route catalogue:
//! install a complete snapshot of one
//! service at revision `R`, verify its manifest, then consume changes
//! strictly after `R` in order, and expose a route generation only when
//! every record the sealed manifest covers is held and its digests agree.
//! Anything else — a snapshot whose digest disagrees, a change out of
//! order, a provider that says the window is gone, a pointer naming a
//! generation whose records are not all here, a table that cannot hold
//! the catalogue — is an explicit [`Reason`] to resync, never a partial
//! table and never a silently skipped change.
//!
//! The reader holds one entry per record (its key, revision and content
//! digest, and for the two manifests Lattice writes their decoded counts
//! and digest) in a table the caller provides, sorted by key, so a
//! generation's records are contiguous and a sealed digest is recomputed
//! by a single ordered walk. Keeping a manifest's decoded form in its own
//! entry means any number of staged or sealed generations beside the
//! active one cannot crowd it out. Record contents are not kept: the
//! director streams them into its own table builder as they arrive; this
//! reader decides only whether a generation may be exposed.
//!
//! The same table holds what the catalogue carries beside route
//! generations, each checked as it arrives and reported as an [`Event`]:
//! a generation's policies (members of it, covered by its sealed
//! manifest; each a placed record whose key names its kind and content
//! address), declarations (a placed record of the kind its key names,
//! never going back a generation: [`Event::Declared`]), and status streams
//! (a head moves only over statuses `1..=seq` all held and agreeing with
//! its chain: [`Event::Status`], else [`Reason::StatusGap`]).
//!
//! Readiness is explicit ([`Reader::readiness`]): unready until a snapshot
//! is installed and after any resync; fresh while the consumer has heard
//! the store say it holds nothing newer ([`Reader::caught_up`]) within the
//! freshness bound it names; stale otherwise — the generation exposed is
//! still whole, but the store has not been heard from.
//!
//! Driven by the host suites (`tests/harness/tests/contract_route_catalogue.rs`,
//! `contract_catalogue_records.rs`) with injected gaps, duplicates,
//! reorders, compaction and restarts. Mounted beside `route_catalogue.rs`
//! (reached as `super::route_catalogue`).

#![allow(
    dead_code,
    reason = "shared via #[path] into consumers and the host suite; each uses a subset of the surface"
)]

use super::route_catalogue::{
    content_address, key, parse_key, sha, validate_record, Active, Manifest, ManifestDigest,
    Parsed, Sealed, Service, StatusHead, DIGEST_LEN, ID_LEN, KEY_MAX,
};

/// One record held: its key, the revision it was last written at, its
/// content digest, what its key names, and — for a manifest or a sealed
/// manifest — the record decoded (`counts` the manifest's count, or the
/// sealed manifest's routes, endpoints and records; `chain` its digest). For a
/// declaration `generation` is its record generation; for a status and a
/// status head it is the status's number (the head's `counts[0]` its
/// flags, `chain` its chain).
#[derive(Clone, Copy)]
pub struct Entry {
    pub key_len: u8,
    pub key: [u8; KEY_MAX],
    pub rev: u64,
    pub digest: [u8; DIGEST_LEN],
    pub generation: u64,
    pub what: u8,
    pub counts: [u32; 3],
    pub chain: [u8; DIGEST_LEN],
}

const W_ENDPOINT: u8 = 1;
const W_MANIFEST: u8 = 2;
const W_ROUTE: u8 = 3;
const W_SEALED: u8 = 4;
const W_OTHER: u8 = 5;
const W_POLICY: u8 = 6;
const W_DECLARATION: u8 = 7;
const W_STATUS: u8 = 8;
const W_HEAD: u8 = 9;

impl Entry {
    pub const EMPTY: Entry = Entry {
        key_len: 0,
        key: [0; KEY_MAX],
        rev: 0,
        digest: [0; DIGEST_LEN],
        generation: 0,
        what: 0,
        counts: [0; 3],
        chain: [0; DIGEST_LEN],
    };

    pub fn key(&self) -> &[u8] {
        &self.key[..usize::from(self.key_len)]
    }
}

/// Why the reader must take a fresh snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    /// The snapshot's manifest does not match what was installed.
    SnapshotDigest,
    /// A snapshot record, or a change, out of key order within a page or
    /// with a revision below one already held.
    Reorder,
    /// The provider said the window is gone (LOST or RESYNC_REQUIRED).
    Compacted,
    /// The entry table cannot hold the catalogue.
    Overflow,
    /// The active pointer names a generation not wholly here, or whose
    /// digests disagree: records are missing (a gap) or wrong.
    Gap { generation: u64 },
    /// A record Lattice writes that does not decode, or a placed record
    /// whose envelope, kind or content address disagrees with its key.
    BadRecord,
    /// A call out of order (a change before the snapshot is done).
    Sequence,
    /// A declaration going back a generation, or another record at the
    /// generation held.
    Regression,
    /// A status head naming statuses not wholly here, or whose chain
    /// disagrees: the stream's head `seq`.
    StatusGap { seq: u64 },
}

/// Whether the director may trust what it holds (§13.2 explicit
/// stale/fresh readiness).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Readiness {
    /// No snapshot installed, or a resync owed: serve nothing from it.
    Unready,
    /// Installed, live, and the store heard within the freshness bound.
    Fresh,
    /// Installed and live, but the store not heard within the bound: what
    /// is exposed is whole, and nothing newer is known.
    Stale,
}

/// What a step produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    None,
    /// Generation `generation` is complete and may be exposed; it became
    /// visible at `revision` (the snapshot's, or the change's).
    Installed {
        generation: u64,
        revision: u64,
    },
    /// The active generation is the withdrawn form (sealed over no
    /// routes): the service publishes nothing from `revision` on.
    Withdrawn {
        generation: u64,
        revision: u64,
    },
    /// No active generation: the catalogue is empty at this revision.
    Empty {
        revision: u64,
    },
    /// A declaration of `kind` under `id` now stands at `generation`.
    Declared {
        kind: u16,
        id: [u8; ID_LEN],
        generation: u64,
        revision: u64,
    },
    /// A declaration withdrawn.
    Undeclared {
        kind: u16,
        id: [u8; ID_LEN],
        revision: u64,
    },
    /// `operation`'s stream is published through status `seq`, every one
    /// of them held (`closed`: its last).
    Status {
        operation: [u8; ID_LEN],
        seq: u64,
        closed: bool,
        revision: u64,
    },
    Resync(Reason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Installing,
    Live,
    Resync,
}

/// Change kinds as the store provider's CHANGES names them.
pub const KIND_ADDED: u8 = 0;
pub const KIND_MODIFIED: u8 = 1;
pub const KIND_DELETED: u8 = 2;
/// CHANGES page statuses (`store_provider::CH_*`).
pub const CH_OK: u8 = 0;

/// The reader of one service's catalogue.
pub struct Reader<'a> {
    pub service: Service,
    pub phase: Phase,
    /// The snapshot's revision; changes at or below it are already held.
    pub at_rev: u64,
    /// The last page's safe revision: pages must not go backwards.
    pub last_safe_rev: u64,
    /// The generation exposed, if any.
    pub exposed: Option<u64>,
    /// When the store was last heard to hold nothing newer, if ever.
    pub heard_ms: Option<u64>,
    entries: &'a mut [Entry],
    n: usize,
    snapshot: ManifestDigest,
    active: Option<Active>,
}

impl<'a> Reader<'a> {
    /// A reader over `entries` (the caller's table; its length bounds the
    /// records the catalogue may hold), installing a snapshot at `at_rev`.
    pub fn new(service: Service, at_rev: u64, entries: &'a mut [Entry]) -> Self {
        Reader {
            service,
            phase: Phase::Installing,
            at_rev,
            last_safe_rev: at_rev,
            exposed: None,
            heard_ms: None,
            entries,
            n: 0,
            snapshot: ManifestDigest::new(),
            active: None,
        }
    }

    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    fn resync(&mut self, r: Reason) -> Event {
        self.phase = Phase::Resync;
        self.heard_ms = None;
        Event::Resync(r)
    }

    fn find(&self, key: &[u8]) -> Result<usize, usize> {
        self.entries[..self.n].binary_search_by(|e| e.key().cmp(key))
    }

    /// The positions of the entries whose key starts with `prefix`.
    fn under(&self, prefix: &[u8]) -> core::ops::Range<usize> {
        let start = match self.find(prefix) {
            Ok(i) | Err(i) => i,
        };
        let mut end = start;
        while end < self.n && self.entries[end].key().starts_with(prefix) {
            end += 1;
        }
        start..end
    }

    /// The consumer heard the store hold nothing newer than what it has
    /// delivered — a snapshot's last page, or a change page that was not
    /// cut short — at `now_ms`.
    pub fn caught_up(&mut self, now_ms: u64) {
        if self.phase == Phase::Live {
            self.heard_ms = Some(now_ms);
        }
    }

    /// Whether what is held may be served: [`Readiness::Fresh`] when live
    /// and heard from within `fresh_ms` of `now_ms`.
    pub fn readiness(&self, now_ms: u64, fresh_ms: u64) -> Readiness {
        if self.phase != Phase::Live {
            return Readiness::Unready;
        }
        match self.heard_ms {
            Some(t) if now_ms.saturating_sub(t) <= fresh_ms => Readiness::Fresh,
            _ => Readiness::Stale,
        }
    }

    /// The declaration of `kind` under `id` held: its generation and the
    /// revision it was written at.
    pub fn declaration(&self, kind: u16, id: &[u8; ID_LEN]) -> Option<(u64, u64)> {
        let mut k = [0u8; KEY_MAX];
        let n = key::declaration(&self.service, kind, id, &mut k);
        let i = self.find(&k[..n]).ok()?;
        let e = &self.entries[i];
        Some((e.generation, e.rev))
    }

    /// How much of `operation`'s stream is published: its head's `seq`
    /// and whether it is closed (verified whenever the head moved).
    pub fn status(&self, operation: &[u8; ID_LEN]) -> Option<(u64, bool)> {
        let mut k = [0u8; KEY_MAX];
        let n = key::status_head(&self.service, operation, &mut k);
        let i = self.find(&k[..n]).ok()?;
        let e = &self.entries[i];
        Some((e.generation, e.counts[0] & 1 != 0))
    }

    /// `operation`'s published statuses are exactly `1..=seq`, each held,
    /// their chain the head's. A stream without a head is not published.
    fn verify_stream(&self, operation: &[u8; ID_LEN]) -> Result<(u64, bool), Reason> {
        let mut p = [0u8; KEY_MAX];
        let pn = key::statuses(&self.service, operation, &mut p);
        let mut head: Option<&Entry> = None;
        for e in &self.entries[self.under(&p[..pn])] {
            if e.what == W_HEAD {
                head = Some(e);
            }
        }
        let Some(h) = head else {
            return Ok((0, false));
        };
        let seq = h.generation;
        let mut chain = ManifestDigest::new();
        let mut want = 1u64;
        for e in &self.entries[self.under(&p[..pn])] {
            if e.what != W_STATUS || e.generation > seq {
                continue;
            }
            if e.generation != want || !chain.absorb(e.key(), &e.digest) {
                return Err(Reason::StatusGap { seq });
            }
            want += 1;
        }
        if u64::from(chain.count) != seq || chain.chain != h.chain {
            return Err(Reason::StatusGap { seq });
        }
        Ok((seq, h.counts[0] & 1 != 0))
    }

    /// Every stream with a head held verifies.
    fn verify_streams(&self) -> Result<(), Reason> {
        for e in &self.entries[..self.n] {
            if e.what != W_HEAD {
                continue;
            }
            if let Some(Parsed::StatusHead { operation }) = parse_key(&self.service, e.key()) {
                self.verify_stream(&operation)?;
            }
        }
        Ok(())
    }

    /// Put a record's entry in, by key: `Err(Overflow)` when the table is
    /// full, `Err(Reorder)` when `rev` is below the revision held,
    /// `Err(BadRecord)` for a manifest or sealed manifest that does not
    /// decode.
    fn upsert(&mut self, key: &[u8], rev: u64, value: &[u8]) -> Result<(), Reason> {
        let mut counts = [0u32; 3];
        let mut chain = [0u8; DIGEST_LEN];
        let what = match parse_key(&self.service, key) {
            Some(Parsed::Endpoint { generation, .. }) => (W_ENDPOINT, generation),
            Some(Parsed::Manifest { generation, .. }) => {
                let m = Manifest::decode(value).ok_or(Reason::BadRecord)?;
                counts[0] = m.count;
                chain = m.digest;
                (W_MANIFEST, generation)
            }
            Some(Parsed::Route { generation, .. }) => (W_ROUTE, generation),
            Some(Parsed::Sealed { generation }) => {
                let s = Sealed::decode(value).ok_or(Reason::BadRecord)?;
                counts = [s.routes, s.endpoints, s.records];
                chain = s.digest;
                (W_SEALED, generation)
            }
            Some(Parsed::Policy {
                generation,
                kind,
                id,
            }) => match validate_record(value) {
                Ok(e) if e.kind == kind && content_address(value) == Some(id) => {
                    (W_POLICY, generation)
                }
                _ => return Err(Reason::BadRecord),
            },
            Some(Parsed::Declaration { kind, .. }) => match validate_record(value) {
                Ok(e) if e.kind == kind => (W_DECLARATION, e.generation),
                _ => return Err(Reason::BadRecord),
            },
            Some(Parsed::Status { seq, .. }) => match validate_record(value) {
                Ok(_) => (W_STATUS, seq),
                Err(_) => return Err(Reason::BadRecord),
            },
            Some(Parsed::StatusHead { .. }) => {
                let h = StatusHead::decode(value).ok_or(Reason::BadRecord)?;
                counts[0] = h.flags;
                chain = h.chain;
                (W_HEAD, h.seq)
            }
            _ => (W_OTHER, 0),
        };
        let digest = sha::sha256(value);
        match self.find(key) {
            Ok(i) => {
                let e = &mut self.entries[i];
                if rev < e.rev {
                    return Err(Reason::Reorder);
                }
                if what.0 == W_DECLARATION && what.1 <= e.generation {
                    return Err(Reason::Regression);
                }
                e.rev = rev;
                e.digest = digest;
                e.generation = what.1;
                e.counts = counts;
                e.chain = chain;
                Ok(())
            }
            Err(i) => {
                if self.n >= self.entries.len() || key.len() > KEY_MAX {
                    return Err(Reason::Overflow);
                }
                self.entries.copy_within(i..self.n, i + 1);
                let e = &mut self.entries[i];
                *e = Entry::EMPTY;
                e.key_len = key.len() as u8;
                e.key[..key.len()].copy_from_slice(key);
                e.rev = rev;
                e.digest = digest;
                e.generation = what.1;
                e.what = what.0;
                e.counts = counts;
                e.chain = chain;
                self.n += 1;
                Ok(())
            }
        }
    }

    fn remove(&mut self, key: &[u8]) {
        if let Ok(i) = self.find(key) {
            self.entries.copy_within(i + 1..self.n, i);
            self.n -= 1;
        }
    }

    /// One snapshot record, in the page's order.
    pub fn install_record(&mut self, key: &[u8], value: &[u8]) -> Event {
        if self.phase != Phase::Installing {
            return self.resync(Reason::Sequence);
        }
        if !self.snapshot.absorb_record(key, value) {
            return self.resync(Reason::Reorder);
        }
        if let Err(r) = self.upsert(key, self.at_rev, value) {
            return self.resync(r);
        }
        self.note(key, value)
    }

    /// The active pointer, noted as it passes (the manifests are decoded
    /// into their entries by [`Reader::upsert`]).
    fn note(&mut self, key: &[u8], value: &[u8]) -> Event {
        if parse_key(&self.service, key) == Some(Parsed::Active) {
            match Active::decode(value) {
                Some(a) => self.active = Some(a),
                None => return self.resync(Reason::BadRecord),
            }
        }
        Event::None
    }

    /// The snapshot's last page: its manifest, verified against what was
    /// installed; then the active generation is verified whole.
    pub fn install_done(&mut self, count: u32, digest: &[u8; DIGEST_LEN]) -> Event {
        if self.phase != Phase::Installing {
            return self.resync(Reason::Sequence);
        }
        if self.snapshot.count != count || self.snapshot.chain != *digest {
            return self.resync(Reason::SnapshotDigest);
        }
        if let Err(r) = self.verify_streams() {
            return self.resync(r);
        }
        self.phase = Phase::Live;
        self.expose(self.at_rev)
    }

    /// Verify the active generation is whole and expose it.
    fn expose(&mut self, revision: u64) -> Event {
        let Some(a) = self.active else {
            self.exposed = None;
            return Event::Empty { revision };
        };
        match self.verify(&a) {
            Ok(s) => {
                self.exposed = Some(a.generation);
                if s.withdrawn() {
                    Event::Withdrawn {
                        generation: a.generation,
                        revision,
                    }
                } else {
                    Event::Installed {
                        generation: a.generation,
                        revision,
                    }
                }
            }
            Err(r) => self.resync(r),
        }
    }

    /// The sealed manifest of generation `g`'s records as held: every
    /// record of the generation but the sealed one, in key order — its
    /// policies, then each route's directory in index order (endpoints,
    /// manifest, route), each route's manifest over exactly its endpoints,
    /// the routes `0..routes` each present. The sealed manifest, when they
    /// agree.
    fn verify(&self, a: &Active) -> Result<Sealed, Reason> {
        let g = a.generation;
        let gap = Reason::Gap { generation: g };
        // The generation's entries are contiguous, in key order.
        let mut all = ManifestDigest::new();
        let mut eps = ManifestDigest::new();
        let mut sealed: Option<Sealed> = None;
        let mut endpoints = 0u32;
        let mut routes = 0u32;
        // The route whose directory the walk is in, and whether its
        // manifest (which follows its endpoints) has been seen.
        let mut cur: Option<u8> = None;
        let mut manifest_seen = false;
        for e in &self.entries[..self.n] {
            let member = matches!(
                e.what,
                W_ENDPOINT | W_MANIFEST | W_ROUTE | W_SEALED | W_POLICY
            );
            if e.generation != g || !member {
                continue;
            }
            if e.what == W_SEALED {
                sealed = Some(Sealed {
                    routes: e.counts[0],
                    endpoints: e.counts[1],
                    records: e.counts[2],
                    digest: e.chain,
                });
                continue;
            }
            let route = match parse_key(&self.service, e.key()) {
                Some(Parsed::Endpoint { route, .. })
                | Some(Parsed::Manifest { route, .. })
                | Some(Parsed::Route { route, .. }) => Some(route),
                _ => None,
            };
            if let Some(r) = route {
                if cur != Some(r) {
                    // A new route's directory: the one before it closed
                    // with its manifest and its route record; routes are
                    // numbered from 0 without a hole.
                    if cur.is_some() || u32::from(r) != routes {
                        return Err(gap);
                    }
                    cur = Some(r);
                    eps = ManifestDigest::new();
                    manifest_seen = false;
                }
            }
            match e.what {
                W_ENDPOINT => {
                    if manifest_seen || !eps.absorb(e.key(), &e.digest) {
                        return Err(gap);
                    }
                    endpoints = endpoints.wrapping_add(1);
                }
                W_MANIFEST => {
                    if e.counts[0] != eps.count || e.chain != eps.chain {
                        return Err(gap);
                    }
                    manifest_seen = true;
                }
                W_ROUTE => {
                    if !manifest_seen {
                        return Err(gap);
                    }
                    routes += 1;
                    cur = None;
                }
                _ => {
                    // A policy sorts before every route.
                    if routes != 0 || cur.is_some() {
                        return Err(gap);
                    }
                }
            }
            if !all.absorb(e.key(), &e.digest) {
                return Err(gap);
            }
        }
        let Some(sealed) = sealed else {
            return Err(gap);
        };
        if cur.is_some()
            || sealed.digest != a.sealed_digest
            || sealed.digest != all.chain
            || sealed.records != all.count
            || sealed.endpoints != endpoints
            || sealed.routes != routes
        {
            return Err(gap);
        }
        Ok(sealed)
    }

    /// One change record, as the store provider's CHANGES delivers it.
    pub fn change(&mut self, rev: u64, kind: u8, key: &[u8], value: &[u8]) -> Event {
        if self.phase != Phase::Live {
            return self.resync(Reason::Sequence);
        }
        // At or below the snapshot: already held (a page resumed inside a
        // revision, a history walk reporting twice).
        if rev <= self.at_rev {
            return Event::None;
        }
        if kind == KIND_DELETED {
            self.remove(key);
            return match parse_key(&self.service, key) {
                Some(Parsed::Active) => {
                    self.active = None;
                    self.expose(rev)
                }
                Some(Parsed::Declaration { kind, id }) => Event::Undeclared {
                    kind,
                    id,
                    revision: rev,
                },
                _ => Event::None,
            };
        }
        // The same write again: nothing.
        if let Ok(i) = self.find(key) {
            let e = &self.entries[i];
            if e.rev == rev && e.digest == sha::sha256(value) {
                return Event::None;
            }
        }
        if let Err(r) = self.upsert(key, rev, value) {
            return self.resync(r);
        }
        let ev = self.note(key, value);
        if ev != Event::None {
            return ev;
        }
        match parse_key(&self.service, key) {
            Some(Parsed::Active) => self.expose(rev),
            Some(Parsed::Declaration { kind, id }) => Event::Declared {
                kind,
                id,
                generation: self.declaration(kind, &id).map_or(0, |d| d.0),
                revision: rev,
            },
            Some(Parsed::StatusHead { operation }) => match self.verify_stream(&operation) {
                Ok((seq, closed)) => Event::Status {
                    operation,
                    seq,
                    closed,
                    revision: rev,
                },
                Err(r) => self.resync(r),
            },
            _ => Event::None,
        }
    }

    /// A CHANGES page's end: its status and safe revision. A status that
    /// is not a page is a resync; a safe revision below the last is a
    /// reorder.
    pub fn page_end(&mut self, status: u8, safe_rev: u64) -> Event {
        if self.phase != Phase::Live {
            return self.resync(Reason::Sequence);
        }
        if status != CH_OK {
            return self.resync(Reason::Compacted);
        }
        if safe_rev < self.last_safe_rev {
            return self.resync(Reason::Reorder);
        }
        self.last_safe_rev = safe_rev;
        Event::None
    }

    /// A status a change page carries for `operation`'s stream between
    /// head moves is staged, not published: whether the head names it.
    pub fn published(&self, operation: &[u8; ID_LEN], seq: u64) -> bool {
        self.status(operation).is_some_and(|(s, _)| seq <= s)
    }
}
