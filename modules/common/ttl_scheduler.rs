//! Deterministic expiry queue for Lattice.
//!
//! Mirrors `modules/common/watch_hub.rs`: the same source compiles
//! into the no_std PIC `ttl_scheduler` / `lease_manager` modules
//! AND is `#[path]`-mounted into host tests. A regression in the
//! priority ordering, dedup-on-reinsert, or replay-safe ordering
//! is caught by `cargo test` before any ELF runs.
//!
//! ## Why a separate facade
//!
//! `lease_manager` needs to know when leases expire so it can emit
//! revoke envelopes; `kv_state_worker` needs to know when TTL-
//! bearing KV records expire so it can lazily evict them; and a
//! future `watch_idle_reaper` (or the `watch_registry`'s idle path)
//! needs to know when an unbound watch's session timeout elapses.
//! All three consume the same shape of `(deadline_ms, kind,
//! payload)` records and pop them in deadline order; sharing the
//! data structure means there's one priority queue to test, debug,
//! and snapshot.
//!
//! ## Replay safety
//!
//! The queue's deadline is **deterministic time** (driven by
//! `MSG_LEASE_TICK` envelopes from the apply pipeline) not wall
//! clock. After a snapshot install or worker rebind, the queue
//! state is reconstructed identically because the inputs that
//! shaped it (lease grants, KV PUTs with `expiry_ms`) replay in
//! commit order.
//!
//! ## Capacity model
//!
//! Backed by a fixed-cap sorted array. Insert/remove are O(N)
//! linear; pop-head is O(1). For the lease + KV-expiry workloads
//! we expect tens-to-thousands of pending deadlines, not millions,
//! and the linear cost is well under the per-tick budget at the
//! 250 µs apply tick.
//!
//! Heap-free, `no_std`-compatible, dual-target.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules; each consumer uses a subset of the surface so single-module rustc invocations see unused items"
)]

use core::mem::MaybeUninit;

// ── Expiry kinds (tagged so consumers can dispatch on payload) ────────

/// Lease deadline — payload is the `lease_id`.
pub const EXPIRY_KIND_LEASE: u8 = 0x01;
/// KV record expiry — payload is the kpg-scoped hash of the key
/// (the worker rehydrates the actual key from the hash via the
/// store).
pub const EXPIRY_KIND_KV: u8 = 0x02;
/// Watch session-idle timeout — payload is the `watch_id`.
pub const EXPIRY_KIND_WATCH_IDLE: u8 = 0x03;

// ── Entry ────────────────────────────────────────────────────────────

/// A single deadline record. Comparable by `deadline_ms` so the
/// internal array can stay sorted on insert.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpiryEntry {
    pub deadline_ms: u64,
    pub kind: u8,
    /// Payload semantics depend on `kind`. For `EXPIRY_KIND_LEASE`
    /// this is the lease_id; for KV it's the per-key handle; etc.
    pub payload: u64,
}

impl ExpiryEntry {
    pub const fn new(deadline_ms: u64, kind: u8, payload: u64) -> Self {
        Self {
            deadline_ms,
            kind,
            payload,
        }
    }
}

// ── TtlQueue ──────────────────────────────────────────────────────────

/// Errors a caller might want to differentiate. Capacity overflow
/// is the common one; duplicate-payload rejection is opt-in via
/// `insert_unique`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TtlError {
    /// Queue is at capacity. The caller should refuse the grant /
    /// SET-with-EX that triggered the insert.
    Full,
    /// `insert_unique` saw an existing entry with `(kind, payload)`.
    /// The caller should `remove(kind, payload)` first if they want
    /// to update the deadline.
    Duplicate,
}

/// Fixed-capacity priority queue keyed by `deadline_ms` ascending.
///
/// The implementation is a sorted array — insertion is O(N), pop-
/// head is O(1), `remove(kind, payload)` is O(N). The trade-off
/// versus a binary heap is intentional:
///
/// - **Replay determinism** wants stable iteration: a sorted array
///   has only one valid ordering for a given set of entries; a heap
///   has many. Two replicas observing the same input sequence
///   produce identical queue snapshots.
/// - **Snapshot export** is a single `slice` copy — the same memory
///   layout the queue uses at runtime is also the snapshot bytes
///   (modulo endianness, which is handled by the encoder).
/// - **Workload size** — for tens-to-thousands of leases / TTL
///   records the linear cost is dwarfed by the per-tick worker
///   step on bcm2712.
pub struct TtlQueue<const N: usize> {
    entries: [MaybeUninit<ExpiryEntry>; N],
    len: usize,
}

impl<const N: usize> TtlQueue<N> {
    pub const fn new() -> Self {
        // SAFETY: `MaybeUninit<T>` doesn't require initialisation.
        let entries =
            unsafe { MaybeUninit::<[MaybeUninit<ExpiryEntry>; N]>::uninit().assume_init() };
        Self { entries, len: 0 }
    }

    pub fn init(&mut self) {
        self.len = 0;
    }

    pub const fn capacity(&self) -> usize {
        N
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn is_full(&self) -> bool {
        self.len == N
    }

    /// Insert into the right sorted position. Duplicate `(kind,
    /// payload)` is allowed; use `insert_unique` to reject those.
    /// Returns `Err(Full)` when at capacity.
    pub fn insert(&mut self, entry: ExpiryEntry) -> Result<(), TtlError> {
        if self.len == N {
            return Err(TtlError::Full);
        }
        // Find insertion index: first slot whose deadline > new.
        let pos = self.find_insert_pos(entry.deadline_ms);
        // Shift right.
        let mut i = self.len;
        while i > pos {
            self.entries[i] = self.entries[i - 1];
            i -= 1;
        }
        self.entries[pos].write(entry);
        self.len += 1;
        Ok(())
    }

    /// Insert iff no existing entry has the same `(kind, payload)`.
    pub fn insert_unique(&mut self, entry: ExpiryEntry) -> Result<(), TtlError> {
        if self.find(entry.kind, entry.payload).is_some() {
            return Err(TtlError::Duplicate);
        }
        self.insert(entry)
    }

    /// Remove the first entry with `(kind, payload)`. Returns the
    /// removed entry, or `None` if not found.
    pub fn remove(&mut self, kind: u8, payload: u64) -> Option<ExpiryEntry> {
        let pos = self.find(kind, payload)?;
        // SAFETY: position came from `find`, so the slot is init.
        let removed = unsafe { self.entries[pos].assume_init_read() };
        let mut i = pos;
        while i + 1 < self.len {
            self.entries[i] = self.entries[i + 1];
            i += 1;
        }
        self.len -= 1;
        Some(removed)
    }

    /// Find the first entry index with `(kind, payload)`.
    pub fn find(&self, kind: u8, payload: u64) -> Option<usize> {
        for i in 0..self.len {
            // SAFETY: index is < len, so the slot is initialised.
            let e = unsafe { self.entries[i].assume_init_ref() };
            if e.kind == kind && e.payload == payload {
                return Some(i);
            }
        }
        None
    }

    /// Peek the next-to-expire entry without popping.
    pub fn peek(&self) -> Option<&ExpiryEntry> {
        if self.len == 0 {
            return None;
        }
        // SAFETY: len > 0 means slot 0 is initialised.
        Some(unsafe { self.entries[0].assume_init_ref() })
    }

    /// Remove the entry that expires last (the highest deadline).
    pub fn pop_latest(&mut self) -> Option<ExpiryEntry> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        // SAFETY: the slot was initialised while `len` covered it.
        Some(unsafe { self.entries[self.len].assume_init_read() })
    }

    /// Insert `entry`, keeping the soonest deadlines when full: the entry
    /// that expires last — `entry` itself, or the latest queued — is
    /// given up and returned. `None`: everything is kept.
    pub fn insert_keeping_soonest(&mut self, entry: ExpiryEntry) -> Option<ExpiryEntry> {
        if self.insert(entry).is_ok() {
            return None;
        }
        let Some(last) = self.len.checked_sub(1) else {
            return Some(entry);
        };
        // SAFETY: `last` < `len`, so the slot is initialised.
        let latest = unsafe { self.entries[last].assume_init_ref() }.deadline_ms;
        if latest <= entry.deadline_ms {
            return Some(entry);
        }
        let given_up = self.pop_latest();
        let _ = self.insert(entry);
        given_up
    }

    /// Pop the next-to-expire entry unconditionally.
    pub fn pop(&mut self) -> Option<ExpiryEntry> {
        if self.len == 0 {
            return None;
        }
        // SAFETY: len > 0.
        let removed = unsafe { self.entries[0].assume_init_read() };
        let mut i = 0;
        while i + 1 < self.len {
            self.entries[i] = self.entries[i + 1];
            i += 1;
        }
        self.len -= 1;
        Some(removed)
    }

    /// Fire entries with `deadline_ms <= now_ms`, in deadline order,
    /// popping each one only once `deliver` reports it delivered.
    /// Returns the count actually fired.
    ///
    /// Two bounds, both of which exist because an expiry that leaves
    /// the queue without being acted on is gone for good — the queue is
    /// the only record that the entry was ever due:
    ///
    /// - `limit` caps how many are popped in one call. A caller that
    ///   can emit at most N per step passes N, and entry N+1 stays
    ///   queued at its original deadline rather than being popped into
    ///   a full buffer and discarded.
    /// - `deliver` returning `false` (its output refused the event)
    ///   stops the drain with that entry still at the head, so the next
    ///   call — the next tick — offers it again.
    ///
    /// The head is always the earliest deadline, so stopping early
    /// never skips over an entry that was due.
    pub fn drain_due_while<F: FnMut(&ExpiryEntry) -> bool>(
        &mut self,
        now_ms: u64,
        limit: usize,
        mut deliver: F,
    ) -> usize {
        let mut count = 0;
        while count < limit {
            let Some(head) = self.peek() else {
                break;
            };
            if head.deadline_ms > now_ms {
                break;
            }
            // SAFETY: peek told us len > 0. The entry is COPIED, not
            // read out — it is only removed below, after delivery.
            let due = unsafe { *self.entries[0].assume_init_ref() };
            if !deliver(&due) {
                break;
            }
            let mut i = 0;
            while i + 1 < self.len {
                self.entries[i] = self.entries[i + 1];
                i += 1;
            }
            self.len -= 1;
            count += 1;
        }
        count
    }

    /// Pop every entry with `deadline_ms <= now_ms`, calling `sink` for
    /// each. Convenience wrapper over [`Self::drain_due_while`] for
    /// callers whose sink cannot fail and has no per-step bound.
    pub fn drain_due<F: FnMut(ExpiryEntry)>(&mut self, now_ms: u64, mut sink: F) -> usize {
        self.drain_due_while(now_ms, usize::MAX, |e| {
            sink(*e);
            true
        })
    }

    /// Iterate every entry in deadline order. Useful for snapshot
    /// export.
    pub fn iter(&self) -> TtlIter<'_, N> {
        TtlIter {
            queue: self,
            pos: 0,
        }
    }

    fn find_insert_pos(&self, deadline_ms: u64) -> usize {
        // Linear scan — adequate for the queue sizes we expect.
        for i in 0..self.len {
            // SAFETY: index is < len.
            let e = unsafe { self.entries[i].assume_init_ref() };
            if e.deadline_ms > deadline_ms {
                return i;
            }
        }
        self.len
    }
}

impl<const N: usize> Default for TtlQueue<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> Drop for TtlQueue<N> {
    fn drop(&mut self) {
        // ExpiryEntry is Copy + Trivial, so we don't need to drop
        // anything explicitly; the array slots will be reclaimed
        // with the struct. Kept as an explicit Drop impl in case
        // the entry type grows owning fields later.
    }
}

pub struct TtlIter<'a, const N: usize> {
    queue: &'a TtlQueue<N>,
    pos: usize,
}

impl<'a, const N: usize> Iterator for TtlIter<'a, N> {
    type Item = &'a ExpiryEntry;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.queue.len {
            return None;
        }
        // SAFETY: pos < len.
        let e = unsafe { self.queue.entries[self.pos].assume_init_ref() };
        self.pos += 1;
        Some(e)
    }
}
