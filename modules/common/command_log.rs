//! The command log — a state worker's own write-ahead log when nothing
//! orders its commands (no consensus in front of it: lattice's store
//! serving fluxor's store contracts, `kv_state_worker_store`).
//!
//! Where consensus orders a worker, clustor's WAL is the one
//! authoritative log and the disk store is a
//! materialization rebuilt from it. A worker nothing orders has no such
//! log, so it keeps this one: every op that is not a pure read is
//! appended and made durable before it is answered, with the clock it ran
//! under, and on restart the log's tail replays on top of the recovered
//! runs — each op recomputing the revision it was first assigned, the
//! writes the runs already hold suppressed by the materializer exactly as
//! a WAL replay's are. It is the fluxor host store's own model (an fsynced
//! append log in front of a materialization), and it keeps a store
//! provider's write synchronous and its `LocalDurable` fence honest.
//!
//! ## Files
//!
//! Segments `FileKind::Log` with increasing ids. A segment starts with a
//! header naming the revision the store stood at when it opened; every op
//! in it ran at or after that revision:
//!
//! ```text
//! header  [magic:u32 = "LCLG"][version:u16 = 1][start:u64][prev_end:u64][crc:u32]
//! record  [len:u32][crc:u32][payload]
//! payload [commit_ts:u64][now_ms:u64][tenant:u32][database:u32][keyspace:u32]
//!         [consistency:u8][op:u8][room:u32][body]
//! ```
//!
//! (little-endian; each crc is `crc32` of what precedes it in the header,
//! or of the payload.) `prev_end` is where the segment before this one
//! ends: its length when it was rotated out, or where a replay found its
//! torn tail (the append a crash interrupted, never answered — storage
//! cannot cut it off, so the next segment states where it stops). A
//! record that does not hold before that end is corruption; in the last
//! segment it is the torn tail, and the log ends there.
//!
//! ## Rotation, replay, reclamation
//!
//! A new segment opens whenever a flush freezes the memtable (its start is
//! the freeze revision) or the current one passes [`SEGMENT_BYTES`]. On
//! restart the store recovers its runs, whose high-water is `H`; replay
//! starts at the newest segment whose start is at most `H` — exactly `H`
//! when the last published flush opened it — with the revision clock
//! seeded at that start. Once a flush publishes `H`, every segment before
//! the newest one starting at or below `H` is no longer needed.
//!
//! ## Asking again
//!
//! No storage call waits on the device: any of them may answer `Again`
//! (see `run_storage.rs`). The log is therefore a step machine. An append
//! STAGES its record whole — header, fixed fields and body, in the log's
//! own buffer — and then writes it as one storage append (and, when
//! durable, fences it); a rotation is a fence of the segment being
//! closed, a create, a header append, a header fence and the new name's
//! publication (`fsync_name`). Whichever action
//! the device cannot take now stands, and [`CommandLog::settle`] (or the
//! next append) asks it again — the storage continues the bytes it already
//! took. [`LogError::Pending`] says the record is the log's and is not yet
//! where it was asked to be: the op it records must not be answered until
//! `settle` says so. While a record is pending the log takes no other.
//!
//! Mounted beside `kv_store` (reached as `super::kv_store::disk_store`).

use super::kv_store::disk_store::run_storage::{FileKind, RunStorage, StorageError};
use super::kv_store::disk_store::{crc32, crc32_finish, crc32_update, CRC32_INIT};

pub const SEGMENT_MAGIC: u32 = u32::from_le_bytes(*b"LCLG");
pub const SEGMENT_VERSION: u16 = 1;
pub const HEADER_LEN: usize = 26;
/// `[len:u32][crc:u32]` before each payload.
pub const RECORD_HEAD: usize = 8;
/// A payload's fixed fields before the op's body.
pub const PAYLOAD_FIXED: usize = 8 + 8 + 4 + 4 + 4 + 1 + 1 + 4;
/// The largest body an op carries: a channel frame's (a `u16` length).
pub const BODY_MAX: usize = 65536;
/// The largest payload.
pub const PAYLOAD_MAX: usize = PAYLOAD_FIXED + BODY_MAX;
/// A segment past this size is closed at the next op.
pub const SEGMENT_BYTES: u64 = 16 << 20;
/// Segments held at once (the ones not yet reclaimed).
pub const SEGMENTS: usize = 64;

/// Why the log refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogError {
    /// The storage refused (the store must not answer the op).
    Storage(StorageError),
    /// A segment other than the last does not read whole, or the segments
    /// do not reach back to the store's high-water.
    Corrupt,
    /// More segments than [`SEGMENTS`] are waiting to be reclaimed.
    Full,
    /// An op larger than [`BODY_MAX`].
    TooLarge,
    /// The record is taken (staged) but not yet written, or not yet
    /// durable when asked to be: [`CommandLog::settle`] on a later step
    /// finishes it. Not a failure — the op it records waits for it.
    Pending,
    /// A record is pending: this one is not taken. Settle, then ask again.
    Busy,
}

impl From<StorageError> for LogError {
    fn from(e: StorageError) -> Self {
        LogError::Storage(e)
    }
}

/// What the log is doing for the record in flight (`CommandLog::stage`).
const IDLE: u8 = 0;
/// Rotation: fencing the segment being closed.
const ROT_SYNC: u8 = 1;
/// Rotation: creating the new segment.
const ROT_CREATE: u8 = 2;
/// Rotation: its header.
const ROT_HEADER: u8 = 3;
/// Rotation: fencing the header.
const ROT_FENCE: u8 = 4;
/// The staged record: its one append.
const REC_WRITE: u8 = 5;
/// The staged record: its fence (a durable append).
const REC_FENCE: u8 = 6;
/// `sync`: fencing the newest segment's records.
const SYNC_FENCE: u8 = 7;
/// Rotation: publishing the new segment's NAME, so no record — and no
/// answer resting on one — lands in a segment a power cut could lose
/// whole by name.
const ROT_NAME: u8 = 8;

/// A whole staged record: its head and payload.
pub const STAGED_MAX: usize = RECORD_HEAD + PAYLOAD_MAX;

/// One logged op, as it ran.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Record<'a> {
    pub commit_ts: u64,
    pub now_ms: u64,
    pub identity: (u32, u32, u32),
    pub consistency: u8,
    pub op: u8,
    /// The room its answer had: an op the engine refused for want of
    /// room replays refused.
    pub room: u32,
    pub body: &'a [u8],
}

fn rd32(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

fn rd64(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

/// `src` into `dst` from `*at`, advancing it (as much as fits).
fn put(dst: &mut [u8], at: &mut usize, src: &[u8]) {
    for (x, y) in dst.iter_mut().skip(*at).zip(src) {
        *x = *y;
    }
    *at += src.len();
}

fn header(start: u64, prev_end: u64) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    let mut at = 0usize;
    put(&mut h, &mut at, &SEGMENT_MAGIC.to_le_bytes());
    put(&mut h, &mut at, &SEGMENT_VERSION.to_le_bytes());
    put(&mut h, &mut at, &start.to_le_bytes());
    put(&mut h, &mut at, &prev_end.to_le_bytes());
    let crc = crc32(h.get(..HEADER_CRC_AT).unwrap_or(&[]));
    put(&mut h, &mut at, &crc.to_le_bytes());
    h
}

const HEADER_CRC_AT: usize = HEADER_LEN - 4;

/// A payload's fixed fields.
fn fixed(r: &Record<'_>) -> [u8; PAYLOAD_FIXED] {
    let mut f = [0u8; PAYLOAD_FIXED];
    let (t, d, k) = r.identity;
    let mut at = 0usize;
    put(&mut f, &mut at, &r.commit_ts.to_le_bytes());
    put(&mut f, &mut at, &r.now_ms.to_le_bytes());
    put(&mut f, &mut at, &t.to_le_bytes());
    put(&mut f, &mut at, &d.to_le_bytes());
    put(&mut f, &mut at, &k.to_le_bytes());
    put(&mut f, &mut at, &[r.consistency, r.op]);
    put(&mut f, &mut at, &r.room.to_le_bytes());
    f
}

/// The segment being written, and the ones before it still needed.
pub struct CommandLog {
    /// Held segments, oldest first: `(id, start revision, where the one
    /// before it ends)`.
    segs: [(u32, u64, u64); SEGMENTS],
    count: usize,
    /// Bytes in the newest segment.
    len: u64,
    /// The newest segment is open for appends (a replayed one never is:
    /// its tail may be torn).
    open: bool,
    /// The newest segment holds records not yet made durable (appended
    /// with `durable` false — a bulk load's, synced at its end).
    dirty: bool,
    /// The action in flight (`IDLE` when none).
    stage: u8,
    /// The staged record is to be made durable.
    rec_durable: bool,
    /// The segment a rotation in flight opens: `(id, start, prev_end)`.
    rot: (u32, u64, u64),
    /// The staged record's length in `staged`.
    staged_len: usize,
    staged: [u8; STAGED_MAX],
    /// Recovery's listing, kept across asks: `listed` entries, once
    /// `have_list` (the zeroed log has none).
    ids: [u32; SEGMENTS],
    listed: usize,
    have_list: bool,
    /// Segment headers recovery has read (of the sorted listing).
    hdr_at: usize,
}

impl Default for CommandLog {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandLog {
    pub const fn new() -> Self {
        CommandLog {
            segs: [(0, 0, 0); SEGMENTS],
            count: 0,
            len: 0,
            open: false,
            dirty: false,
            stage: IDLE,
            rec_durable: false,
            rot: (0, 0, 0),
            staged_len: 0,
            staged: [0u8; STAGED_MAX],
            ids: [0u32; SEGMENTS],
            listed: 0,
            have_list: false,
            hdr_at: 0,
        }
    }

    /// The segments held, oldest first: `(id, start, previous end)`.
    pub fn segments(&self) -> &[(u32, u64, u64)] {
        self.segs.get(..self.count).unwrap_or(&[])
    }

    fn next_id(&self) -> u32 {
        self.segments()
            .last()
            .map_or(1, |s| s.0.wrapping_add(1).max(1))
    }

    /// Is a record (or a rotation, or a sync) part-way? While it is, the
    /// op it records is not to be answered, and the log takes no other
    /// record ([`LogError::Busy`]).
    pub fn pending(&self) -> bool {
        self.stage != IDLE
    }

    /// Ask the action in flight again: `Ok` once nothing is pending (the
    /// staged record written, and fenced when asked to be),
    /// `Err(Pending)` while the device still cannot take it, any other
    /// error as the storage gave it — the segment is then closed (its tail
    /// may be torn; the next segment states where it stops).
    pub fn settle<S: RunStorage>(&mut self, s: &mut S) -> Result<(), LogError> {
        loop {
            let step = match self.stage {
                IDLE => return Ok(()),
                ROT_SYNC | SYNC_FENCE => self
                    .newest_id()
                    .map_or(Ok(()), |id| s.fsync(FileKind::Log, id)),
                ROT_CREATE => match s.create(FileKind::Log, self.rot.0) {
                    // A create the provider decided while nobody was
                    // asking: the file is there, and empty.
                    Ok(()) | Err(StorageError::AlreadyExists) => Ok(()),
                    Err(e) => Err(e),
                },
                ROT_HEADER => s.append(FileKind::Log, self.rot.0, &header(self.rot.1, self.rot.2)),
                ROT_FENCE => s.fsync(FileKind::Log, self.rot.0),
                ROT_NAME => s.fsync_name(FileKind::Log, self.rot.0),
                REC_WRITE => match self.newest_id() {
                    Some(id) => s.append(
                        FileKind::Log,
                        id,
                        self.staged.get(..self.staged_len).unwrap_or(&[]),
                    ),
                    None => Err(StorageError::IoFault),
                },
                _ => self
                    .newest_id()
                    .map_or(Ok(()), |id| s.fsync(FileKind::Log, id)),
            };
            match step {
                Ok(()) => self.advance(),
                Err(StorageError::Again) => return Err(LogError::Pending),
                Err(e) => {
                    // Nothing is appended after a torn tail: the segment
                    // closes, and what was staged is dropped with it.
                    self.open = false;
                    self.stage = IDLE;
                    self.staged_len = 0;
                    return Err(LogError::Storage(e));
                }
            }
        }
    }

    /// The action in flight has landed: on to the next.
    fn advance(&mut self) {
        self.stage = match self.stage {
            ROT_SYNC => {
                self.dirty = false;
                ROT_CREATE
            }
            ROT_CREATE => ROT_HEADER,
            ROT_HEADER => ROT_FENCE,
            ROT_FENCE => ROT_NAME,
            ROT_NAME => {
                if let Some(slot) = self.segs.get_mut(self.count) {
                    *slot = self.rot;
                }
                self.count += 1;
                self.len = HEADER_LEN as u64;
                self.open = true;
                if self.staged_len > 0 {
                    REC_WRITE
                } else {
                    IDLE
                }
            }
            REC_WRITE => {
                self.len += self.staged_len as u64;
                self.staged_len = 0;
                if self.rec_durable {
                    REC_FENCE
                } else {
                    self.dirty = true;
                    IDLE
                }
            }
            REC_FENCE | SYNC_FENCE => {
                self.dirty = false;
                IDLE
            }
            _ => IDLE,
        };
    }

    fn newest_id(&self) -> Option<u32> {
        self.segments().last().map(|seg| seg.0)
    }

    /// Open a new segment starting at revision `start`: `Ok` once it is
    /// open, `Err(Pending)` while the device takes it (settle), or the
    /// refusal.
    pub fn rotate<S: RunStorage>(&mut self, s: &mut S, start: u64) -> Result<(), LogError> {
        if self.stage != IDLE {
            return Err(LogError::Busy);
        }
        self.begin_rotate(start)?;
        self.settle(s)
    }

    /// Stage a rotation to a segment starting at `start`.
    fn begin_rotate(&mut self, start: u64) -> Result<(), LogError> {
        if self.count == SEGMENTS {
            return Err(LogError::Full);
        }
        let id = self.next_id();
        // Where the segment this one follows stops: its length (or where a
        // replay found its torn tail).
        let prev_end = if self.count == 0 { 0 } else { self.len };
        self.rot = (id, start, prev_end);
        // The segment being closed is made durable first: nothing after it
        // may be durable while it is not.
        self.stage = if self.dirty { ROT_SYNC } else { ROT_CREATE };
        Ok(())
    }

    /// Make every record appended so far durable: `Ok` once it is,
    /// `Err(Pending)` while the fence is outstanding (settle).
    pub fn sync<S: RunStorage>(&mut self, s: &mut S) -> Result<(), LogError> {
        if self.stage != IDLE {
            // Whatever is in flight finishes first; a durable record's own
            // fence covers everything before it.
            self.settle(s)?;
        }
        if !self.dirty || self.newest_id().is_none() {
            return Ok(());
        }
        self.stage = SYNC_FENCE;
        self.settle(s)
    }

    /// Append `r` and — `durable` — make it (and everything before it)
    /// durable; otherwise it is durable at the next [`CommandLog::sync`],
    /// durable append or rotation. `revision` is the store's revision
    /// before `r` ran, and `frozen` the revision a flush last froze the
    /// memtable at: a freeze since the segment opened, or a full segment,
    /// opens a new one first (starting there), so a segment's ops all ran
    /// at or after its start.
    ///
    /// `Err(Pending)`: the record is taken — staged in the log — and the
    /// device has not finished with it; the op must not be answered until
    /// [`CommandLog::settle`] answers `Ok`. `Err(Busy)`: an earlier record
    /// is still pending, and this one is NOT taken.
    pub fn append<S: RunStorage>(
        &mut self,
        s: &mut S,
        r: &Record<'_>,
        revision: u64,
        frozen: u64,
        durable: bool,
    ) -> Result<(), LogError> {
        if self.stage != IDLE {
            return Err(LogError::Busy);
        }
        if r.body.len() > BODY_MAX {
            return Err(LogError::TooLarge);
        }
        let start = self.segments().last().map(|l| l.1);
        let rotate = match start {
            None => true,
            Some(st) => !self.open || frozen > st || self.len >= SEGMENT_BYTES,
        };
        // Stage the record whole: from here it is the log's, and every
        // ask of its append is the same bytes.
        let f = fixed(r);
        let crc = crc32_finish(crc32_update(crc32_update(CRC32_INIT, &f), r.body));
        let len = (PAYLOAD_FIXED + r.body.len()) as u32;
        let mut at = 0usize;
        put(&mut self.staged, &mut at, &len.to_le_bytes());
        put(&mut self.staged, &mut at, &crc.to_le_bytes());
        put(&mut self.staged, &mut at, &f);
        put(&mut self.staged, &mut at, r.body);
        self.staged_len = at;
        self.rec_durable = durable;
        if rotate {
            let at = if frozen > start.unwrap_or(0) && frozen <= revision {
                frozen
            } else {
                revision
            };
            if let Err(e) = self.begin_rotate(at) {
                self.staged_len = 0;
                return Err(e);
            }
        } else {
            self.stage = REC_WRITE;
        }
        self.settle(s)
    }

    /// A replay ended at `end` in the newest segment: the next segment
    /// states that the one before it stops there (see the module docs).
    pub fn replayed(&mut self, end: &Replay) {
        self.len = end.at;
        self.open = false;
    }

    /// A flush published revision `durable`: drop every segment before the
    /// newest one starting at or below it.
    pub fn reclaim<S: RunStorage>(&mut self, s: &mut S, durable: u64) -> Result<(), LogError> {
        let Some(keep) = self.first_at_or_below(durable) else {
            return Ok(());
        };
        for i in 0..keep {
            let id = self.segs.get(i).map_or(0, |seg| seg.0);
            match s.delete(FileKind::Log, id) {
                Ok(()) | Err(StorageError::NotFound) => {}
                Err(e) => {
                    // What was dropped stays dropped; the rest is kept.
                    self.shift(i);
                    return Err(LogError::Storage(e));
                }
            }
        }
        self.shift(keep);
        Ok(())
    }

    /// Where a replay for runs holding everything through `high` starts:
    /// the first segment opened at the latest start at or below `high`
    /// (segments opened at one revision — an op that changed nothing
    /// between — are all kept).
    fn first_at_or_below(&self, high: u64) -> Option<usize> {
        let best = self
            .segments()
            .iter()
            .map(|seg| seg.1)
            .filter(|st| *st <= high)
            .max()?;
        self.segments().iter().position(|seg| seg.1 == best)
    }

    fn shift(&mut self, n: usize) {
        let n = n.min(self.count);
        for i in 0..self.count - n {
            let moved = self.segs.get(i + n).copied().unwrap_or((0, 0, 0));
            if let Some(slot) = self.segs.get_mut(i) {
                *slot = moved;
            }
        }
        self.count -= n;
    }

    /// Recovery: find the segments on storage, and where replay starts for
    /// a store whose runs hold everything through `high`. Segments before
    /// that are dropped. `Ok(None)`: no log (a new store, or one that never
    /// logged); `Ok(Some(start))`: replay from [`Replay::new`] with the
    /// revision clock seeded at `start`.
    ///
    /// `Err(Storage(Again))`: the device deferred a listing or a read; ask
    /// again — the listing is kept and continues, the reads are made again.
    pub fn recover<S: RunStorage>(
        &mut self,
        s: &mut S,
        high: u64,
    ) -> Result<Option<u64>, LogError> {
        let r = self.recover_listed(s, high);
        if r != Err(LogError::Storage(StorageError::Again)) {
            self.have_list = false;
        }
        r
    }

    fn recover_listed<S: RunStorage>(
        &mut self,
        s: &mut S,
        high: u64,
    ) -> Result<Option<u64>, LogError> {
        // The listing continues into the same buffer when the FS answers
        // part-way (`Again`), and is not walked again once it is whole.
        if !self.have_list {
            match s.list(FileKind::Log, &mut self.ids) {
                Ok(n) => {
                    let n = n.min(SEGMENTS);
                    // Insertion sort: a handful of ids, and `sort_unstable`
                    // brings a panic path a module image cannot link.
                    let ids = self.ids.get_mut(..n).unwrap_or(&mut []);
                    for i in 1..ids.len() {
                        let mut j = i;
                        while j > 0 && ids.get(j - 1) > ids.get(j) {
                            ids.swap(j - 1, j);
                            j -= 1;
                        }
                    }
                    self.listed = n;
                    self.have_list = true;
                    self.count = 0;
                    self.hdr_at = 0;
                }
                Err(StorageError::Overflow) => return Err(LogError::Full),
                Err(e) => return Err(LogError::Storage(e)),
            }
        }
        let n = self.listed;
        let ids = self.ids;
        let ids = ids.get(..n).unwrap_or(&[]);
        let newest = ids.last().copied().unwrap_or(0);
        // Each header is read once: one the device defers is asked again
        // from where the reading stands (`hdr_at`).
        while let Some(&id) = ids.get(self.hdr_at) {
            let mut h = [0u8; HEADER_LEN];
            let got = read_full(s, id, 0, &mut h)?;
            self.hdr_at += 1;
            let ok = got == HEADER_LEN
                && rd32(&h, 0) == Some(SEGMENT_MAGIC)
                && h.get(4..6) == Some(&SEGMENT_VERSION.to_le_bytes()[..])
                && rd32(&h, HEADER_CRC_AT) == Some(crc32(h.get(..HEADER_CRC_AT).unwrap_or(&[])));
            if !ok {
                // A segment whose header never became durable holds no
                // record: the crash came as it opened.
                if id == newest {
                    let _ = s.delete(FileKind::Log, id);
                    continue;
                }
                return Err(LogError::Corrupt);
            }
            if let Some(slot) = self.segs.get_mut(self.count) {
                *slot = (id, rd64(&h, 6).unwrap_or(0), rd64(&h, 14).unwrap_or(0));
            }
            self.count += 1;
        }
        self.open = false;
        self.len = 0;
        self.stage = IDLE;
        if self.count == 0 {
            return Ok(None);
        }
        let Some(first) = self.first_at_or_below(high) else {
            // Every segment starts past what the runs hold: the ops
            // between are gone.
            return Err(LogError::Corrupt);
        };
        let start = self.segs.get(first).map_or(0, |seg| seg.1);
        for i in 0..first {
            let id = self.segs.get(i).map_or(0, |seg| seg.0);
            let _ = s.delete(FileKind::Log, id);
        }
        self.shift(first);
        Ok(Some(start))
    }
}

/// A replay's position: which held segment, and where in it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Replay {
    seg: usize,
    at: u64,
}

impl Replay {
    pub const fn new() -> Self {
        Replay {
            seg: 0,
            at: HEADER_LEN as u64,
        }
    }

    /// The next record into `buf` ([`PAYLOAD_MAX`] bytes): `Ok(None)` at
    /// the log's end (the last segment's end, or its torn tail).
    pub fn next<'b, S: RunStorage>(
        &mut self,
        log: &CommandLog,
        s: &S,
        buf: &'b mut [u8],
    ) -> Result<Option<Record<'b>>, LogError> {
        loop {
            let segs = log.segments();
            let Some(&(id, _, _)) = segs.get(self.seg) else {
                return Ok(None);
            };
            // Where this segment stops, when a later one says.
            let end = segs.get(self.seg + 1).map(|next| next.2);
            if end.is_some_and(|e| self.at >= e) {
                self.seg += 1;
                self.at = HEADER_LEN as u64;
                continue;
            }
            let mut head = [0u8; RECORD_HEAD];
            let got = read_full(s, id, self.at, &mut head)?;
            let len = rd32(&head, 0).unwrap_or(0) as usize;
            let whole = got == RECORD_HEAD
                && (PAYLOAD_FIXED..=PAYLOAD_MAX).contains(&len)
                && len <= buf.len()
                && end.is_none_or(|e| self.at + (RECORD_HEAD + len) as u64 <= e)
                && read_full(
                    s,
                    id,
                    self.at + RECORD_HEAD as u64,
                    buf.get_mut(..len).unwrap_or(&mut []),
                )? == len
                && rd32(&head, 4) == Some(crc32(buf.get(..len).unwrap_or(&[])));
            if !whole {
                // The last segment's torn tail ends the log; anywhere
                // else a record that does not hold is corruption.
                return if end.is_none() {
                    Ok(None)
                } else {
                    Err(LogError::Corrupt)
                };
            }
            self.at += (RECORD_HEAD + len) as u64;
            let whole_buf: &'b [u8] = buf;
            let p = whole_buf.get(..len).unwrap_or(&[]);
            return Ok(Some(Record {
                commit_ts: rd64(p, 0).unwrap_or(0),
                now_ms: rd64(p, 8).unwrap_or(0),
                identity: (
                    rd32(p, 16).unwrap_or(0),
                    rd32(p, 20).unwrap_or(0),
                    rd32(p, 24).unwrap_or(0),
                ),
                consistency: p.get(28).copied().unwrap_or(0),
                op: p.get(29).copied().unwrap_or(0),
                room: rd32(p, 30).unwrap_or(0),
                body: p.get(PAYLOAD_FIXED..).unwrap_or(&[]),
            }));
        }
    }
}

/// Read `out.len()` bytes at `at`, looping over short reads: the count
/// read (short only at the file's end).
fn read_full<S: RunStorage>(s: &S, id: u32, at: u64, out: &mut [u8]) -> Result<usize, LogError> {
    let mut got = 0usize;
    while got < out.len() {
        let n = s.read_at(
            FileKind::Log,
            id,
            at + got as u64,
            out.get_mut(got..).unwrap_or(&mut []),
        )?;
        if n == 0 {
            break;
        }
        got += n;
    }
    Ok(got)
}
