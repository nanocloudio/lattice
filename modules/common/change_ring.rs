//! The store provider's change log: a key-only ring of what changed, at
//! which revision, that subscriptions are rung from and `CHANGES` is
//! answered from — so neither ever scans the store's version history.
//!
//! Entries are `[rev u64][hint u8][kl u8][key]`, appended in apply order
//! (the order a command's keys were noted, `kv_store::KeyLog`), never in key
//! order: one revision may carry many entries, and the same key twice. A
//! position is named by `(rev, ordinal-within-rev)`, which is what a
//! `CHANGES` continuation carries.
//!
//! Two revisions bound what the ring can answer:
//!
//!   - the **frontier** F: every revision up to F has had its keys appended
//!     (or is known to have none). A command's keys are appended only once
//!     it stands — after its log record is durable — so a revision applied
//!     but not yet settled is beyond F, pending, never a gap;
//!   - the **base** B: the ring answers for every revision above B, and for
//!     nothing at or below it. Every way the ring can fail to hold a change —
//!     an entry evicted to make room, a command whose key list was not whole,
//!     an install, a restart, a revision that advanced with no commit — moves
//!     B up to the revision it reaches. A window starting below B is a gap:
//!     the caller walks history, or answers LOST.
//!
//! The ring's bytes are the caller's (one allocation from the provider's
//! heap); nothing here allocates or panics. A ring whose bytes are all zero
//! is a detached, empty one (the provider holding it is cleared in place).

/// Copy `src` into the front of `dst` (as much as fits). `copy_from_slice`
/// carries a panic path a PIC module cannot link.
fn copy(dst: &mut [u8], src: &[u8]) {
    for (d, s) in dst.iter_mut().zip(src) {
        *d = *s;
    }
}

/// An entry's bytes besides its key.
pub const ENTRY_FIXED: usize = 10;
/// The hint byte of a wrap filler: the rest of the buffer is unused.
const HINT_WRAP: u8 = 0xFF;
/// One checkpoint per this many entries: what a seek scans past at most.
pub const CHECK_EVERY: u32 = 32;
/// Checkpoints kept, a ring of its own.
pub const CHECKPOINTS: usize = 512;
/// The ordinal that sorts after every entry of a revision: a bare revision
/// `since` resumes at `(since, ORD_END)`.
pub const ORD_END: u32 = u32::MAX;

/// One entry, as read back.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Entry<'a> {
    pub rev: u64,
    pub ord: u32,
    pub hint: u8,
    pub key: &'a [u8],
}

#[derive(Clone, Copy)]
struct Check {
    at: u64,
    rev: u64,
    ord: u32,
}

const NO_CHECK: Check = Check {
    at: 0,
    rev: 0,
    ord: 0,
};

/// The ring. `buf` is set once ([`ChangeRing::attach`]); positions are
/// absolute byte offsets that only grow, so a cursor held across appends
/// can tell whether its entry was evicted (`at < tail`).
pub struct ChangeRing {
    buf: *mut u8,
    cap: usize,
    /// Absolute offsets: the oldest entry, and where the next is written.
    tail: u64,
    head: u64,
    /// The newest entry's revision and ordinal (the next entry of the same
    /// revision takes `last_ord + 1`).
    last_rev: u64,
    last_ord: u32,
    /// Entries ever appended (checkpoint cadence; 0: the ring has had no
    /// entry, so `last_rev` names none).
    appended: u64,
    base: u64,
    frontier: u64,
    checks: [Check; CHECKPOINTS],
    /// Checkpoints written ever (the newest is `checks[(n - 1) % N]`).
    n_checks: u32,
    /// Bumped whenever B moves for a reason other than eviction (a gap a
    /// subscriber must hear of whole): the pump compares it to its own.
    pub gaps: u32,
}

impl ChangeRing {
    pub const fn new() -> Self {
        ChangeRing {
            buf: core::ptr::null_mut(),
            cap: 0,
            tail: 0,
            head: 0,
            last_rev: 0,
            last_ord: 0,
            appended: 0,
            base: 0,
            frontier: 0,
            checks: [NO_CHECK; CHECKPOINTS],
            n_checks: 0,
            gaps: 0,
        }
    }

    /// Give the ring its bytes (`cap` of them at `buf`, writable for as long
    /// as the ring is used).
    ///
    /// # Safety
    /// `buf` must be valid for `cap` bytes for the ring's whole life.
    pub unsafe fn attach(&mut self, buf: *mut u8, cap: usize) {
        self.buf = buf;
        self.cap = cap;
        self.tail = 0;
        self.head = 0;
    }

    pub fn attached(&self) -> bool {
        !self.buf.is_null() && self.cap > ENTRY_FIXED
    }

    pub fn base(&self) -> u64 {
        self.base
    }

    pub fn frontier(&self) -> u64 {
        self.frontier
    }

    /// Bytes the ring holds (telemetry).
    pub fn used(&self) -> u64 {
        self.head - self.tail
    }

    /// Start empty at revision `rev`: nothing at or below it is answerable
    /// (boot, open: the ring never saw what came before).
    pub fn reset(&mut self, rev: u64) {
        self.tail = self.head;
        self.base = rev;
        self.frontier = rev;
        self.last_rev = rev;
        self.last_ord = 0;
        self.gaps = self.gaps.wrapping_add(1);
    }

    /// A gap at `rev`: changes up to it are not (all) in the ring. The ring
    /// empties and answers only for what follows (an install, a command
    /// whose key list was not whole, a revision that moved with no commit).
    pub fn gap(&mut self, rev: u64) {
        let rev = rev.max(self.frontier);
        self.reset(rev);
    }

    /// Every revision up to `rev` has had its keys appended.
    pub fn advance(&mut self, rev: u64) {
        if rev > self.frontier {
            self.frontier = rev;
        }
    }

    fn slice(&self) -> &[u8] {
        if self.buf.is_null() {
            return &[];
        }
        // SAFETY: `attach`'s contract: `buf` is valid for `cap` bytes.
        unsafe { core::slice::from_raw_parts(self.buf, self.cap) }
    }

    fn slice_mut(&mut self) -> &mut [u8] {
        if self.buf.is_null() {
            return &mut [];
        }
        // SAFETY: as `slice`, and the ring is the only writer.
        unsafe { core::slice::from_raw_parts_mut(self.buf, self.cap) }
    }

    fn phys(&self, at: u64) -> usize {
        // `checked_rem`: a detached ring (cap 0) has no panic path.
        at.checked_rem(self.cap as u64).unwrap_or(0) as usize
    }

    /// The entry at absolute offset `at`, or `None` at a filler or a torn
    /// offset; the offset of the entry after it.
    fn read(&self, at: u64) -> Option<(Entry<'_>, u64)> {
        let p = self.phys(at);
        let b = self.slice();
        if p + ENTRY_FIXED > self.cap {
            return None;
        }
        let hint = b[p + 8];
        if hint == HINT_WRAP {
            return None;
        }
        let rev = u64::from_le_bytes([
            b[p],
            b[p + 1],
            b[p + 2],
            b[p + 3],
            b[p + 4],
            b[p + 5],
            b[p + 6],
            b[p + 7],
        ]);
        let kl = b[p + 9] as usize;
        let key = b.get(p + ENTRY_FIXED..p + ENTRY_FIXED + kl)?;
        Some((
            Entry {
                rev,
                ord: 0,
                hint,
                key,
            },
            at + (ENTRY_FIXED + kl) as u64,
        ))
    }

    /// Skip a wrap filler (or the unusable tail of the buffer) at `at`.
    fn unwrap_at(&self, at: u64) -> u64 {
        let p = self.phys(at);
        if p + ENTRY_FIXED > self.cap {
            return at + (self.cap - p) as u64;
        }
        if self.slice()[p + 8] == HINT_WRAP {
            return at + (self.cap - p) as u64;
        }
        at
    }

    /// Drop the oldest entry: B rises to its revision (a window that needs
    /// it can no longer be answered here).
    fn evict(&mut self) {
        self.tail = self.unwrap_at(self.tail);
        if self.tail >= self.head {
            self.tail = self.head;
            return;
        }
        if let Some((e, next)) = self.read(self.tail) {
            if e.rev > self.base {
                self.base = e.rev;
            }
            self.tail = next;
        } else {
            self.tail = self.head;
        }
    }

    /// Append one entry. Room is made by evicting the oldest. `false` when
    /// the entry can never be held (the ring is smaller than it, or
    /// detached, or its hint is the filler's, which would read back as the
    /// end of the buffer): the caller takes that as a gap.
    pub fn append(&mut self, rev: u64, hint: u8, key: &[u8]) -> bool {
        let need = ENTRY_FIXED + key.len();
        if !self.attached() || hint == HINT_WRAP || key.len() > u8::MAX as usize || need > self.cap
        {
            return false;
        }
        // An entry never straddles the buffer's end: the rest is a filler.
        let p = self.phys(self.head);
        if p + need > self.cap {
            let fill = (self.cap - p) as u64;
            while self.tail < self.head
                && self.head + fill + need as u64 - self.tail > self.cap as u64
            {
                self.evict();
            }
            if p + ENTRY_FIXED <= self.cap {
                self.slice_mut()[p + 8] = HINT_WRAP;
            }
            self.head += fill;
        }
        while self.tail < self.head && self.head + need as u64 - self.tail > self.cap as u64 {
            self.evict();
        }
        let ord = if rev == self.last_rev && self.appended != 0 {
            self.last_ord.saturating_add(1)
        } else {
            0
        };
        let at = self.head;
        let p = self.phys(at);
        let b = self.slice_mut();
        copy(&mut b[p..p + 8], &rev.to_le_bytes());
        b[p + 8] = hint;
        b[p + 9] = key.len() as u8;
        copy(&mut b[p + ENTRY_FIXED..p + need], key);
        self.head += need as u64;
        self.last_rev = rev;
        self.last_ord = ord;
        if self.appended.is_multiple_of(u64::from(CHECK_EVERY)) {
            self.checks[self.n_checks as usize % CHECKPOINTS] = Check { at, rev, ord };
            self.n_checks = self.n_checks.wrapping_add(1);
        }
        self.appended = self.appended.wrapping_add(1);
        true
    }

    /// Whether the ring can answer from `(rev, ord)` on: `rev` above B, or
    /// at B from its end, and not past F.
    pub fn answers(&self, rev: u64, ord: u32) -> bool {
        (rev > self.base || (rev == self.base && ord == ORD_END)) && rev <= self.frontier
    }

    /// A cursor on the first entry at or after `(rev, ord)` — every entry
    /// of a later revision, or of `rev` with an ordinal `>= ord` — or at the
    /// tip when there is none yet. `None` when the ring cannot answer from
    /// there ([`answers`](Self::answers)).
    pub fn seek(&self, rev: u64, ord: u32) -> Option<Cursor> {
        if !self.answers(rev, ord) {
            return None;
        }
        // Start from the latest checkpoint still in the ring that sorts
        // before the target, else from the tail.
        let n = self.n_checks as usize;
        let kept = n.min(CHECKPOINTS);
        let nth = |i: usize| self.checks[(n - kept + i) % CHECKPOINTS];
        let (mut lo, mut hi) = (0usize, kept);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let c = nth(mid);
            if c.at < self.tail || (c.rev, c.ord) < (rev, ord) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let mut cur = self.oldest();
        if lo > 0 {
            let c = nth(lo - 1);
            if c.at >= self.tail && (c.rev, c.ord) < (rev, ord) {
                cur = Cursor {
                    at: c.at,
                    has_prev: false,
                    prev_rev: 0,
                    next_ord: c.ord,
                    lost: false,
                };
            }
        }
        loop {
            let before = cur;
            match self.next(&mut cur) {
                Some(e) if (e.rev, e.ord) >= (rev, ord) => return Some(before),
                Some(_) => {}
                None => return Some(cur),
            }
        }
    }

    /// A cursor on the oldest entry. Its ordinal is exact: an entry of a
    /// revision above B never had a sibling evicted (evicting one raises B
    /// to its revision), and the ordinals of B's own entries are never
    /// asked for.
    pub fn oldest(&self) -> Cursor {
        Cursor {
            at: self.tail,
            has_prev: false,
            prev_rev: 0,
            next_ord: 0,
            lost: false,
        }
    }

    /// A cursor past the newest entry: it reads what is appended from now on.
    pub fn tip(&self) -> Cursor {
        Cursor {
            at: self.head,
            has_prev: true,
            prev_rev: self.last_rev,
            next_ord: self.last_ord.saturating_add(1),
            lost: false,
        }
    }

    /// The entry under `cur`, advancing it; `None` at the tip. A cursor
    /// whose entry was evicted before it was read reads nothing more and is
    /// marked [`Cursor::lost`].
    pub fn next(&self, cur: &mut Cursor) -> Option<Entry<'_>> {
        if cur.lost || cur.at < self.tail {
            cur.lost = true;
            return None;
        }
        let at = self.unwrap_at(cur.at);
        if at >= self.head {
            return None;
        }
        let (mut e, next) = self.read(at)?;
        e.ord = if !cur.has_prev || cur.prev_rev == e.rev {
            cur.next_ord
        } else {
            0
        };
        cur.has_prev = true;
        cur.prev_rev = e.rev;
        cur.next_ord = e.ord.saturating_add(1);
        cur.at = next;
        Some(e)
    }
}

impl Default for ChangeRing {
    fn default() -> Self {
        Self::new()
    }
}

/// A read position in a [`ChangeRing`]: the next entry's offset, and what
/// its ordinal is.
#[derive(Clone, Copy, Debug)]
pub struct Cursor {
    pub at: u64,
    /// Whether an entry was read (or the tip stood) before `at`, and its
    /// revision: the next entry of the same revision takes `next_ord`.
    has_prev: bool,
    prev_rev: u64,
    next_ord: u32,
    /// The entry under the cursor was evicted before it was read.
    pub lost: bool,
}

impl Cursor {
    /// A cursor at offset 0 of an empty ring (all zero: a cleared holder's).
    pub const START: Cursor = Cursor {
        at: 0,
        has_prev: false,
        prev_rev: 0,
        next_ord: 0,
        lost: false,
    };
}
