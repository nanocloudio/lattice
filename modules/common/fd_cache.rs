//! A tiny fixed-size open-file-descriptor cache.
//!
//! ## Why this exists
//!
//! `FsRunStorage::read_at` performs a full **FS_OPEN + FS_SEEK +
//! FS_READ + FS_CLOSE** cycle per call. On FAT32 an `FS_OPEN` is a
//! LINEAR scan of the directory, and the KV run files accumulate one
//! per flush, so that scan gets longer as a benchmark runs. The rig
//! measured the consequence exactly:
//!
//! ```text
//! [kvw] drain n=1 us=125704 ... blocks=1 reads=3 rkb=4
//! ```
//!
//! Three reads moving four kilobytes, in 125.7 ms — **~42 ms per
//! `read_at`**. A 4 KiB read is not 42 ms on this NVMe; essentially all
//! of it is the open/close cycle around the read. Holding the
//! descriptor open across reads removes that cost entirely.
//!
//! ## Why it is deliberately tiny
//!
//! fat32's open-file table holds **8** entries and is SHARED with
//! durability's WAL segment, scan and snapshot handles. A cache sized
//! for the 16 possible runs would starve the WAL and turn a correct
//! system into an `ENFILE` one. `SLOTS` is therefore small, and the
//! cache never holds more than `SLOTS` descriptors: it evicts one of
//! its OWN entries before opening another, so it cannot grow into the
//! WAL's headroom no matter how many runs exist. A miss simply costs
//! what every read costs today — correct, just slower.
//!
//! "Slower" is NOT flat on FAT32, though: re-opening a file for
//! APPEND walks its whole FAT chain to rebuild the append cursor —
//! O(file length) per open. `SLOTS` must therefore cover the steady-
//! state compaction working set (RUN_MERGE_THRESHOLD = 3 input
//! runs + 1 output + the manifest = 5): at 3 slots, every output
//! append during a merge step evicted-and-reopened the growing
//! output run, and the finalize step's footer append crossed the
//! module step deadline on the rig (~140 ms observed) and got the
//! worker terminated. 5 + durability's ~3 handles meets the
//! 8-entry table exactly; the emergency MAX_RUNS-input merge still
//! thrashes reads by design (bounded, rare) rather than growing
//! into the WAL's share.
//!
//! ## The one way this can be wrong
//!
//! A descriptor that outlives its file. Run ids are reused after
//! compaction retires a run, so a stale fd on a reused id would read
//! ANOTHER RUN'S BYTES and answer confidently with them — silent
//! corruption, not an error. Every path that can invalidate a file
//! therefore funnels through `drop_entry`/`take_all` and gets its fd
//! back to close:
//!
//! - `delete` — before the unlink, which also avoids the provider's
//!   `EBUSY` on unlinking a file with an open handle;
//! - `create` — a create at an id means the bytes behind it are new;
//! - any write to the file — its content changed;
//! - any read error touching it — the handle is suspect;
//! - `init` — descriptors from a previous life are gone.
//!
//! The cache never closes a descriptor itself: it hands the caller the
//! `fd` to close, so the ownership transfer is explicit at every call
//! site rather than hidden in a `Drop`.

#![allow(
    dead_code,
    reason = "shared via #[path] into the PIC storage backend and host tests; each consumer uses a subset"
)]

/// Cached descriptors. Small on purpose — see the module docs on the
/// 8-entry fat32 table shared with the WAL, and on why this must
/// cover the compaction working set (3 inputs + output + manifest).
pub const SLOTS: usize = 5;

/// `fd < 0` means the slot is empty.
pub const EMPTY_FD: i32 = -1;

#[derive(Clone, Copy)]
pub struct FdSlot {
    pub fd: i32,
    pub kind_run: bool,
    pub id: u32,
    /// LRU stamp; larger is more recent.
    pub used: u32,
}

impl FdSlot {
    pub const fn empty() -> Self {
        Self {
            fd: EMPTY_FD,
            kind_run: false,
            id: 0,
            used: 0,
        }
    }
}

#[derive(Clone, Copy)]
pub struct FdCache {
    pub slots: [FdSlot; SLOTS],
    tick: u32,
}

impl FdCache {
    pub const fn new() -> Self {
        Self {
            slots: [FdSlot::empty(); SLOTS],
            tick: 0,
        }
    }

    /// Reset to empty WITHOUT closing anything. Only correct where the
    /// descriptors are already gone (a fresh `init`); everywhere else
    /// use `take_all` and close what it returns.
    pub fn reset(&mut self) {
        self.slots = [FdSlot::empty(); SLOTS];
        self.tick = 0;
    }

    /// A cached descriptor for this file, if present. Refreshes its LRU
    /// stamp.
    pub fn lookup(&mut self, kind_run: bool, id: u32) -> Option<i32> {
        self.tick = self.tick.wrapping_add(1);
        let tick = self.tick;
        for s in &mut self.slots {
            if s.fd >= 0 && s.kind_run == kind_run && s.id == id {
                s.used = tick;
                return Some(s.fd);
            }
        }
        None
    }

    /// Store `fd` for this file. Returns the descriptor of the entry it
    /// displaced, which the CALLER must close — the cache never closes
    /// anything itself.
    ///
    /// Never holds more than `SLOTS` descriptors: a full cache evicts
    /// its own least-recently-used entry rather than opening beyond its
    /// budget.
    pub fn insert(&mut self, kind_run: bool, id: u32, fd: i32) -> Option<i32> {
        self.tick = self.tick.wrapping_add(1);
        let tick = self.tick;
        // An existing entry for the same file is replaced, not
        // duplicated — two live descriptors on one file is exactly the
        // aliasing this cache must not create.
        let mut victim = usize::MAX;
        for (i, s) in self.slots.iter().enumerate() {
            if s.fd >= 0 && s.kind_run == kind_run && s.id == id {
                victim = i;
                break;
            }
        }
        if victim == usize::MAX {
            for (i, s) in self.slots.iter().enumerate() {
                if s.fd < 0 {
                    victim = i;
                    break;
                }
            }
        }
        if victim == usize::MAX {
            // LRU over a fixed set: oldest stamp loses.
            let mut oldest = 0usize;
            for (i, s) in self.slots.iter().enumerate() {
                if s.used < self.slots[oldest].used {
                    oldest = i;
                }
            }
            victim = oldest;
        }
        let evicted = self.slots[victim].fd;
        self.slots[victim] = FdSlot {
            fd,
            kind_run,
            id,
            used: tick,
        };
        if evicted >= 0 {
            Some(evicted)
        } else {
            None
        }
    }

    /// Forget this file, returning its descriptor for the caller to
    /// close. Call before ANY operation that changes what the id names.
    pub fn drop_entry(&mut self, kind_run: bool, id: u32) -> Option<i32> {
        for s in &mut self.slots {
            if s.fd >= 0 && s.kind_run == kind_run && s.id == id {
                let fd = s.fd;
                *s = FdSlot::empty();
                return Some(fd);
            }
        }
        None
    }

    /// Drain every descriptor into `out`, emptying the cache. Returns
    /// how many were written; `out` must hold `SLOTS`.
    pub fn take_all(&mut self, out: &mut [i32; SLOTS]) -> usize {
        let mut n = 0usize;
        for s in &mut self.slots {
            if s.fd >= 0 {
                out[n] = s.fd;
                n += 1;
                *s = FdSlot::empty();
            }
        }
        self.tick = 0;
        n
    }

    /// Descriptors currently held. Never exceeds `SLOTS`.
    pub fn len(&self) -> usize {
        self.slots.iter().filter(|s| s.fd >= 0).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for FdCache {
    fn default() -> Self {
        Self::new()
    }
}
