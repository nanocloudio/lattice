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
}

impl From<StorageError> for LogError {
    fn from(e: StorageError) -> Self {
        LogError::Storage(e)
    }
}

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

    /// Open a new segment starting at revision `start`.
    pub fn rotate<S: RunStorage>(&mut self, s: &mut S, start: u64) -> Result<(), LogError> {
        if self.count == SEGMENTS {
            return Err(LogError::Full);
        }
        // The segment being closed is made durable first: nothing after it
        // may be durable while it is not.
        self.sync(s)?;
        let id = self.next_id();
        // Where the segment this one follows stops: its length (or where a
        // replay found its torn tail).
        let prev_end = if self.count == 0 { 0 } else { self.len };
        s.create(FileKind::Log, id)?;
        s.append(FileKind::Log, id, &header(start, prev_end))?;
        s.fsync(FileKind::Log, id)?;
        if let Some(slot) = self.segs.get_mut(self.count) {
            *slot = (id, start, prev_end);
        }
        self.count += 1;
        self.len = HEADER_LEN as u64;
        self.open = true;
        Ok(())
    }

    /// Make every record appended so far durable.
    pub fn sync<S: RunStorage>(&mut self, s: &mut S) -> Result<(), LogError> {
        if !self.dirty {
            return Ok(());
        }
        let Some(&(id, _, _)) = self.segments().last() else {
            return Ok(());
        };
        if let Err(e) = s.fsync(FileKind::Log, id) {
            self.open = false;
            return Err(LogError::Storage(e));
        }
        self.dirty = false;
        Ok(())
    }

    /// Append `r` and — `durable` — make it (and everything before it)
    /// durable; otherwise it is durable at the next [`CommandLog::sync`],
    /// durable append or rotation. `revision` is the store's revision
    /// before `r` ran, and `frozen` the revision a flush last froze the
    /// memtable at: a freeze since the segment opened, or a full segment,
    /// opens a new one first (starting there), so a segment's ops all ran
    /// at or after its start.
    pub fn append<S: RunStorage>(
        &mut self,
        s: &mut S,
        r: &Record<'_>,
        revision: u64,
        frozen: u64,
        durable: bool,
    ) -> Result<(), LogError> {
        if r.body.len() > BODY_MAX {
            return Err(LogError::TooLarge);
        }
        let start = self.segments().last().map(|l| l.1);
        let rotate = match start {
            None => true,
            Some(st) => !self.open || frozen > st || self.len >= SEGMENT_BYTES,
        };
        if rotate {
            let at = if frozen > start.unwrap_or(0) && frozen <= revision {
                frozen
            } else {
                revision
            };
            self.rotate(s, at)?;
        }
        let Some(&(id, _, _)) = self.segments().last() else {
            return Err(LogError::Corrupt);
        };
        let f = fixed(r);
        let crc = crc32_finish(crc32_update(crc32_update(CRC32_INIT, &f), r.body));
        let len = (PAYLOAD_FIXED + r.body.len()) as u32;
        let mut head = [0u8; RECORD_HEAD];
        let mut at = 0usize;
        put(&mut head, &mut at, &len.to_le_bytes());
        put(&mut head, &mut at, &crc.to_le_bytes());
        // Once any byte of a record is out, the segment is closed on a
        // failure: its tail may be torn, and nothing is appended after a
        // torn tail.
        let written = s
            .append(FileKind::Log, id, &head)
            .and_then(|()| s.append(FileKind::Log, id, &f))
            .and_then(|()| s.append(FileKind::Log, id, r.body))
            .and_then(|()| {
                if durable {
                    s.fsync(FileKind::Log, id)
                } else {
                    Ok(())
                }
            });
        if let Err(e) = written {
            self.open = false;
            return Err(LogError::Storage(e));
        }
        self.dirty = !durable;
        self.len += (RECORD_HEAD as u64) + u64::from(len);
        Ok(())
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
    pub fn recover<S: RunStorage>(
        &mut self,
        s: &mut S,
        high: u64,
    ) -> Result<Option<u64>, LogError> {
        let mut ids = [0u32; SEGMENTS];
        let n = match s.list(FileKind::Log, &mut ids) {
            Ok(n) => n,
            Err(StorageError::Overflow) => return Err(LogError::Full),
            Err(e) => return Err(LogError::Storage(e)),
        };
        let ids = ids.get_mut(..n).unwrap_or(&mut []);
        // Insertion sort: a handful of ids, and `sort_unstable` brings a
        // panic path a module image cannot link.
        for i in 1..ids.len() {
            let mut j = i;
            while j > 0 && ids.get(j - 1) > ids.get(j) {
                ids.swap(j - 1, j);
                j -= 1;
            }
        }
        let newest = ids.last().copied().unwrap_or(0);
        self.count = 0;
        for &id in ids.iter() {
            let mut h = [0u8; HEADER_LEN];
            let got = s.read_at(FileKind::Log, id, 0, &mut h)?;
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
