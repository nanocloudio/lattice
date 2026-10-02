//! Writes a replica originates into its group's log, applied exactly once.
//!
//! A replicated store provider (`store_provider.rs`) does not apply a write
//! where it is asked: it proposes it to the group's log and answers once
//! the write has come back, decided, through the apply stream. Clustor
//! delivers a proposal at least once — a follower's is relayed to the
//! leader, and one lost to a leader change is proposed again — so the
//! state machine is what makes it once.
//!
//! Each such entry carries its origin, `(replica, incarnation, seq)`, in
//! `KvCommandHead.corr_id`, and every replica applies it against the
//! origin's record in the store (keyspace [`KS_PROVIDER_ORIGINS`], one row
//! per replica):
//!
//! - **Incarnation.** An origin's incarnation is assigned by the log: it
//!   proposes [`KV_OP_ORIGIN_REGISTER`](super::types::KV_OP_ORIGIN_REGISTER)
//!   with a random nonce, and the entry's apply makes the record's
//!   incarnation one more than it was. Incarnations are therefore ordered
//!   and never reused, and every entry of an earlier one — still in flight
//!   from a process that has since restarted — is stale: applied as
//!   nothing.
//! - **Sequence.** Within an incarnation an origin numbers its writes from
//!   1 and proposes them in order. The record keeps the highest applied,
//!   and only its successor applies: an entry at or below it is a
//!   duplicate, one past it leaves a gap (an earlier entry was lost), and
//!   both apply as nothing. The origin, seeing the gap or hearing nothing,
//!   proposes again from the first undecided write.
//! - **Outcomes.** The record keeps the last [`RING`] outcomes, so an
//!   origin whose replica skipped the entries (a snapshot installed past
//!   them) can still answer its callers. An origin never has more than
//!   [`RING`] writes undecided.
//!
//! Chunks of a large value travel as
//! [`KV_OP_ORIGIN_CHUNK`](super::types::KV_OP_ORIGIN_CHUNK): content
//! addressed, so a duplicate is stored once. The origin offers its
//! manifest only once every chunk has applied; a manifest that still
//! finds one missing applies as a refusal the origin answers by uploading
//! again.
//!
//! Mounted beside `types.rs` (reached as `super::types`). No allocation and
//! no panic path: the worker that mounts it is a PIC module.

use super::types::{
    KV_OP_GET, KV_OP_ORIGIN_CHUNK, KV_OP_ORIGIN_IMPORT, KV_OP_ORIGIN_REGISTER, KV_OP_ORIGIN_WRITE,
    KV_OP_PUT, KV_OP_PUT_CHUNK, KV_OP_TXN, KV_RESULT_INTERNAL, KV_RESULT_NOT_FOUND, KV_RESULT_OK,
    KV_RESULT_TXN,
};

/// The provider's own records (an import's progress), apart from the keys
/// consumers see: the reserved family 0x800C (`models.rs`).
pub const KS_PROVIDER_META: u32 = 0x800C_0001;
/// The origin records: one per replica, keyed by its id (2 bytes, BE).
pub const KS_PROVIDER_ORIGINS: u32 = 0x800C_0002;
/// The key an import's progress is recorded under, in
/// [`KS_PROVIDER_META`]: `[offset:u64 LE][crc:u32 LE]`, how far the host
/// log is imported and the CRC state of those bytes. A replica's import is
/// of its own node's log, so a replicated store records each importing
/// replica's under this key and its id ([`imported_key`]).
pub const IMPORTED_KEY: &[u8] = b"store.log imported";
/// [`IMPORTED_KEY`] and a replica id (2 bytes, BE).
pub const IMPORTED_KEY_REPLICA_LEN: usize = IMPORTED_KEY.len() + 2;

/// The key replica `replica`'s import progress is recorded under.
pub fn imported_key(replica: u16) -> [u8; IMPORTED_KEY_REPLICA_LEN] {
    let mut k = [0u8; IMPORTED_KEY_REPLICA_LEN];
    for (d, s) in k
        .iter_mut()
        .zip(IMPORTED_KEY.iter().chain(replica.to_be_bytes().iter()))
    {
        *d = *s;
    }
    k
}
/// An import's marker, as [`KV_OP_ORIGIN_IMPORT`] carries it ahead of its
/// write and [`IMPORTED_KEY`] holds it.
pub const IMPORT_MARK_LEN: usize = 12;

/// The store provider's own family (`models.rs`): only the worker writes
/// it, never a command.
pub fn is_provider_keyspace(keyspace: u32) -> bool {
    keyspace & 0xFFFF_0000 == 0x800C_0000
}

/// Outcomes a record keeps, and so the most writes an origin has
/// undecided at once.
pub const RING: usize = 16;
/// One outcome: `[result u8][succeeded u8][revision u64 LE]`.
pub const OUTCOME_LEN: usize = 10;
/// A record: `[incarnation u16][nonce u64][hwm u32]`, then the ring.
pub const RECORD_LEN: usize = 2 + 8 + 4 + RING * OUTCOME_LEN;

/// `(replica, incarnation, seq)` as `KvCommandHead.corr_id`.
pub const fn corr(replica: u16, inc: u16, seq: u32) -> u64 {
    ((replica as u64) << 48) | ((inc as u64) << 32) | seq as u64
}

/// A `corr_id`'s `(replica, incarnation, seq)`.
pub const fn split(corr: u64) -> (u16, u16, u32) {
    ((corr >> 48) as u16, (corr >> 32) as u16, corr as u32)
}

/// What one write came to where it applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Outcome {
    /// The result of the write's op when its comparison held (a put's or
    /// a delete's: a manifest whose chunks are missing is refused there);
    /// the `KV_OP_TXN`'s own result when it did not.
    pub result: u8,
    /// Its comparison held (the answer's first byte).
    pub succeeded: bool,
    /// The store's revision once it applied.
    pub revision: u64,
}

pub const NO_OUTCOME: Outcome = Outcome {
    result: 0,
    succeeded: false,
    revision: 0,
};

/// An origin's record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Record {
    pub inc: u16,
    /// The registration that made `inc`.
    pub nonce: u64,
    /// The highest seq applied in `inc` (0: none).
    pub hwm: u32,
    /// Outcomes by `seq % RING`.
    pub ring: [Outcome; RING],
}

impl Record {
    pub fn encode(&self, out: &mut [u8; RECORD_LEN]) {
        out[0..2].copy_from_slice(&self.inc.to_le_bytes());
        out[2..10].copy_from_slice(&self.nonce.to_le_bytes());
        out[10..14].copy_from_slice(&self.hwm.to_le_bytes());
        for (i, o) in self.ring.iter().enumerate() {
            let at = 14 + i * OUTCOME_LEN;
            out[at] = o.result;
            out[at + 1] = u8::from(o.succeeded);
            out[at + 2..at + 10].copy_from_slice(&o.revision.to_le_bytes());
        }
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != RECORD_LEN {
            return None;
        }
        let mut ring = [NO_OUTCOME; RING];
        for (i, o) in ring.iter_mut().enumerate() {
            let at = 14 + i * OUTCOME_LEN;
            *o = Outcome {
                result: b[at],
                succeeded: b[at + 1] != 0,
                revision: u64::from_le_bytes(b[at + 2..at + 10].try_into().ok()?),
            };
        }
        Some(Record {
            inc: u16::from_le_bytes([b[0], b[1]]),
            nonce: u64::from_le_bytes(b[2..10].try_into().ok()?),
            hwm: u32::from_le_bytes(b[10..14].try_into().ok()?),
            ring,
        })
    }

    /// The outcome of `seq` in this incarnation, while the ring holds it.
    pub fn outcome(&self, seq: u32) -> Option<Outcome> {
        (seq != 0 && seq <= self.hwm && self.hwm - seq < RING as u32)
            .then(|| self.ring[seq as usize % RING])
    }
}

/// How an entry stands against its origin's record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The next write of the current incarnation: apply it.
    Apply,
    /// Already applied.
    Duplicate,
    /// An earlier write of the incarnation has not applied.
    Gap,
    /// From an incarnation that is not the current one.
    Stale,
}

pub fn verdict(rec: Option<&Record>, inc: u16, seq: u32) -> Verdict {
    match rec {
        Some(r) if r.inc == inc && seq != 0 => {
            if seq <= r.hwm {
                Verdict::Duplicate
            } else if seq == r.hwm.wrapping_add(1) {
                Verdict::Apply
            } else {
                Verdict::Gap
            }
        }
        _ => Verdict::Stale,
    }
}

/// The record a registration with `nonce` leaves: `None` when it is the
/// registration already in place (a registration proposed again).
pub fn register(rec: Option<&Record>, nonce: u64) -> Option<Record> {
    match rec {
        Some(r) if r.nonce == nonce => None,
        _ => Some(Record {
            inc: rec.map_or(1, |r| r.inc.wrapping_add(1)),
            nonce,
            hwm: 0,
            ring: [NO_OUTCOME; RING],
        }),
    }
}

/// The store an entry applies to: an op under the flat identity's
/// `keyspace`, as a command applies.
pub trait Store {
    /// `(result, answer length)`.
    fn apply(&mut self, keyspace: u32, op: u8, body: &[u8], out: &mut [u8]) -> (u8, usize);
    fn revision(&self) -> u64;
    /// A write applied: its `KV_OP_TXN` body, result and answer. Told
    /// before anything else applies, so the store's account of what the
    /// write changed is still the write's.
    fn wrote(&mut self, _txn: &[u8], _result: u8, _answer: &[u8]) {}
}

/// What applying an origin entry came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    /// A registration, now (or already) `inc` under `nonce`.
    Registered { replica: u16, inc: u16, nonce: u64 },
    /// The write applied: its `KV_OP_TXN` answer is in `out[..len]`.
    Wrote {
        replica: u16,
        inc: u16,
        seq: u32,
        outcome: Outcome,
        len: usize,
    },
    /// Nothing applied, by the record's verdict.
    Skipped {
        replica: u16,
        inc: u16,
        seq: u32,
        verdict: Verdict,
    },
    /// A chunk from `replica`, named by its hash: stored (or already
    /// stored), or refused (`result`).
    Chunk {
        replica: u16,
        hash: [u8; 32],
        result: u8,
    },
    /// Not an origin entry, or malformed: nothing applied.
    Refused,
    /// The store failed the entry (reading the record, applying the
    /// write, or recording it): this replica can no longer apply the log
    /// faithfully.
    Faulted,
}

/// `[key_len u16][key]` for `replica`'s record.
fn record_get(replica: u16) -> [u8; 4] {
    let k = replica.to_be_bytes();
    [2, 0, k[0], k[1]]
}

/// Read `replica`'s record: `Err` when the store fails the read.
pub fn read_record<S: Store>(s: &mut S, replica: u16) -> Result<Option<Record>, ()> {
    let mut val = [0u8; RECORD_LEN + 8];
    let (r, n) = s.apply(
        KS_PROVIDER_ORIGINS,
        KV_OP_GET,
        &record_get(replica),
        &mut val,
    );
    match r {
        KV_RESULT_OK => Record::decode(val.get(..n).ok_or(())?).map(Some).ok_or(()),
        KV_RESULT_NOT_FOUND => Ok(None),
        _ => Err(()),
    }
}

/// Record how far `replica`'s import has come (`mark`,
/// [`IMPORT_MARK_LEN`] bytes) under its [`imported_key`].
fn write_import_mark<S: Store>(s: &mut S, replica: u16, mark: &[u8; IMPORT_MARK_LEN]) -> bool {
    // `[key_len u16][key][value_len u32][value][flags u8][ttl u64]`.
    const V: usize = 2 + IMPORTED_KEY_REPLICA_LEN;
    let mut body = [0u8; V + 4 + IMPORT_MARK_LEN + 9];
    body[..2].copy_from_slice(&(IMPORTED_KEY_REPLICA_LEN as u16).to_le_bytes());
    body[2..V].copy_from_slice(&imported_key(replica));
    let v = V;
    body[v..v + 4].copy_from_slice(&(IMPORT_MARK_LEN as u32).to_le_bytes());
    body[v + 4..v + 4 + IMPORT_MARK_LEN].copy_from_slice(mark);
    let mut ans = [0u8; 16];
    s.apply(KS_PROVIDER_META, KV_OP_PUT, &body, &mut ans).0 == KV_RESULT_OK
}

fn write_record<S: Store>(s: &mut S, replica: u16, rec: &Record) -> bool {
    // `[key_len u16][key][value_len u32][value][flags u8][ttl u64]`.
    let mut body = [0u8; 4 + 4 + RECORD_LEN + 9];
    body[..4].copy_from_slice(&record_get(replica));
    body[4..8].copy_from_slice(&(RECORD_LEN as u32).to_le_bytes());
    let mut enc = [0u8; RECORD_LEN];
    rec.encode(&mut enc);
    body[8..8 + RECORD_LEN].copy_from_slice(&enc);
    let mut ans = [0u8; 16];
    s.apply(KS_PROVIDER_ORIGINS, KV_OP_PUT, &body, &mut ans).0 == KV_RESULT_OK
}

/// Apply one origin entry (`op`, `corr`, `body`) to `s`. A write's
/// `KV_OP_TXN` answers into `out`.
pub fn apply_entry<S: Store>(s: &mut S, op: u8, corr: u64, body: &[u8], out: &mut [u8]) -> Applied {
    let (replica, inc, seq) = split(corr);
    match op {
        KV_OP_ORIGIN_CHUNK => {
            let Some(hash) = body.get(..32).and_then(|h| h.try_into().ok()) else {
                return Applied::Refused;
            };
            Applied::Chunk {
                replica,
                hash,
                result: s.apply(0, KV_OP_PUT_CHUNK, body, out).0,
            }
        }
        KV_OP_ORIGIN_REGISTER => {
            let Some(nonce) = body.try_into().ok().map(u64::from_le_bytes) else {
                return Applied::Refused;
            };
            let Ok(rec) = read_record(s, replica) else {
                return Applied::Faulted;
            };
            match register(rec.as_ref(), nonce) {
                None => Applied::Registered {
                    replica,
                    inc: rec.map_or(0, |r| r.inc),
                    nonce,
                },
                Some(new) if write_record(s, replica, &new) => Applied::Registered {
                    replica,
                    inc: new.inc,
                    nonce,
                },
                Some(_) => Applied::Faulted,
            }
        }
        KV_OP_ORIGIN_WRITE | KV_OP_ORIGIN_IMPORT => {
            // An import's record carries how far the import has come once
            // it is in. The marker is written by the same entry's apply,
            // after the origin record: a replica never holds the marker
            // without the write it covers. A store fault between the two
            // faults the entry like any fault after its TXN, and leaves the
            // marker behind the write — the import then re-sends records it
            // already applied, which are puts of the same bytes.
            let (mark, body) = if op == KV_OP_ORIGIN_IMPORT {
                match body.split_first_chunk::<IMPORT_MARK_LEN>() {
                    Some((m, b)) => (Some(m), b),
                    None => return Applied::Refused,
                }
            } else {
                (None, body)
            };
            let Ok(rec) = read_record(s, replica) else {
                return Applied::Faulted;
            };
            let v = verdict(rec.as_ref(), inc, seq);
            let Some(mut rec) = rec.filter(|_| v == Verdict::Apply) else {
                return Applied::Skipped {
                    replica,
                    inc,
                    seq,
                    verdict: v,
                };
            };
            let (result, len) = s.apply(0, KV_OP_TXN, body, out);
            if result == KV_RESULT_INTERNAL {
                return Applied::Faulted;
            }
            s.wrote(body, result, out.get(..len).unwrap_or(&[]));
            // `[succeeded u8][op_count u16]`, then each op's `[result]
            // [len u16][answer]`: a write is one op.
            let succeeded = result == KV_RESULT_TXN && out.first() == Some(&1);
            let outcome = Outcome {
                result: if succeeded {
                    out.get(3).copied().unwrap_or(KV_RESULT_INTERNAL)
                } else {
                    result
                },
                succeeded,
                revision: s.revision(),
            };
            rec.hwm = seq;
            rec.ring[seq as usize % RING] = outcome;
            // Applied but not recorded: a proposal of it again would apply
            // it twice.
            if !write_record(s, replica, &rec) {
                return Applied::Faulted;
            }
            if let Some(m) = mark {
                if !write_import_mark(s, replica, m) {
                    return Applied::Faulted;
                }
            }
            Applied::Wrote {
                replica,
                inc,
                seq,
                outcome,
                len,
            }
        }
        _ => Applied::Refused,
    }
}
