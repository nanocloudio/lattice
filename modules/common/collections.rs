//! Fixed-capacity collections for Lattice modules.
//!
//! All structures here are `no_std` and heap-free. They are pulled into
//! each module via `#[path = "../../common/collections.rs"]` and into
//! the host-side test/bench harness via the same pattern.
//!
//! Mirrors `clustor/modules/common/collections.rs::RingBuf` for the
//! per-tenant queues, lease deadline lists, watch backlogs, and
//! correlation tables Lattice modules need.

#![allow(
    dead_code,
    reason = "shared via #[path] into multiple modules; each consumer uses a subset of the surface"
)]

use core::mem::MaybeUninit;

/// Fixed-capacity ring buffer. `N` is the slot count; the buffer holds
/// up to `N - 1` items at a time (one slot is reserved for the
/// full/empty distinction, mirroring the clustor pattern).
pub struct RingBuf<T, const N: usize> {
    slots: [MaybeUninit<T>; N],
    head: usize,
    tail: usize,
}

impl<T, const N: usize> Default for RingBuf<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, const N: usize> RingBuf<T, N> {
    /// Create an empty ring buffer.
    pub const fn new() -> Self {
        // SAFETY: An array of `MaybeUninit<T>` does not require
        // initialisation, by definition.
        let slots = unsafe { MaybeUninit::<[MaybeUninit<T>; N]>::uninit().assume_init() };
        Self {
            slots,
            head: 0,
            tail: 0,
        }
    }

    /// Number of items currently stored.
    pub fn len(&self) -> usize {
        if self.head >= self.tail {
            self.head - self.tail
        } else {
            N - self.tail + self.head
        }
    }

    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    pub fn is_full(&self) -> bool {
        (self.head + 1) % N == self.tail
    }

    pub fn capacity(&self) -> usize {
        N - 1
    }

    /// Push an item to the head. Returns `Err(item)` if full.
    pub fn push(&mut self, item: T) -> Result<(), T> {
        if self.is_full() {
            return Err(item);
        }
        self.slots[self.head].write(item);
        self.head = (self.head + 1) % N;
        Ok(())
    }

    /// Pop the oldest item. Returns `None` if empty.
    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        // SAFETY: slot was written by `push` and not yet read.
        let item = unsafe { self.slots[self.tail].assume_init_read() };
        self.tail = (self.tail + 1) % N;
        Some(item)
    }

    /// Peek the oldest item without removing it.
    pub fn peek(&self) -> Option<&T> {
        if self.is_empty() {
            return None;
        }
        // SAFETY: slot was written by `push` and not yet read.
        Some(unsafe { self.slots[self.tail].assume_init_ref() })
    }

    /// Drop all items.
    pub fn clear(&mut self) {
        while self.pop().is_some() {}
    }
}

impl<T, const N: usize> Drop for RingBuf<T, N> {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Fixed-capacity append-only ordered map keyed by `u64`. Used by
/// anchors to map `(corr_id) → (conn_id, request_metadata)` while
/// requests are in flight. Linear scan on lookup; intended for
/// in-flight tables of bounded size (typically `MAX_CONCURRENT_CONNS *
/// pipeline_depth`, on the order of 256–4096 entries).
pub struct InflightTable<V, const N: usize> {
    keys: [u64; N],
    vals: [MaybeUninit<V>; N],
    used: [bool; N],
    len: usize,
}

impl<V, const N: usize> Default for InflightTable<V, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V, const N: usize> InflightTable<V, N> {
    pub const fn new() -> Self {
        // SAFETY: see `RingBuf::new`.
        let vals = unsafe { MaybeUninit::<[MaybeUninit<V>; N]>::uninit().assume_init() };
        Self {
            keys: [0; N],
            vals,
            used: [false; N],
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn capacity(&self) -> usize {
        N
    }
    pub fn is_full(&self) -> bool {
        self.len == N
    }

    /// Insert or replace. Returns the previous value if the key already
    /// existed, else `None`. Returns `Err(value)` if the table is full
    /// AND the key is new.
    pub fn insert(&mut self, key: u64, value: V) -> Result<Option<V>, V> {
        if let Some(idx) = self.index_of(key) {
            // SAFETY: slot is marked used.
            let prev = unsafe { self.vals[idx].assume_init_read() };
            self.vals[idx].write(value);
            return Ok(Some(prev));
        }
        let Some(idx) = self.find_free() else {
            return Err(value);
        };
        self.keys[idx] = key;
        self.vals[idx].write(value);
        self.used[idx] = true;
        self.len += 1;
        Ok(None)
    }

    /// Remove and return the value associated with `key`, if any.
    pub fn remove(&mut self, key: u64) -> Option<V> {
        let idx = self.index_of(key)?;
        // SAFETY: slot is marked used.
        let value = unsafe { self.vals[idx].assume_init_read() };
        self.used[idx] = false;
        self.len -= 1;
        Some(value)
    }

    /// Look up without removing.
    pub fn get(&self, key: u64) -> Option<&V> {
        let idx = self.index_of(key)?;
        // SAFETY: slot is marked used.
        Some(unsafe { self.vals[idx].assume_init_ref() })
    }

    /// Iterate over live values (unordered). Used by consumers that must
    /// scan outstanding entries — e.g. the router's per-key dependency
    /// barrier checking same-conn cross-path conflicts.
    /// Iterate over live `(key, value)` pairs (unordered).
    ///
    /// Distinct from [`InflightTable::iter`] because a sweep that
    /// EXPIRES entries needs the key to answer the caller — the key is
    /// the correlation identity, and a reply without it reaches nobody.
    pub fn iter_entries(&self) -> impl Iterator<Item = (u64, &V)> + '_ {
        self.used.iter().enumerate().filter_map(move |(i, used)| {
            if *used {
                // SAFETY: slot is marked used.
                Some((self.keys[i], unsafe { self.vals[i].assume_init_ref() }))
            } else {
                None
            }
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = &V> + '_ {
        self.used.iter().enumerate().filter_map(move |(i, used)| {
            if *used {
                // SAFETY: slot is marked used.
                Some(unsafe { self.vals[i].assume_init_ref() })
            } else {
                None
            }
        })
    }

    fn index_of(&self, key: u64) -> Option<usize> {
        for (i, used) in self.used.iter().enumerate() {
            if *used && self.keys[i] == key {
                return Some(i);
            }
        }
        None
    }

    fn find_free(&self) -> Option<usize> {
        self.used.iter().position(|u| !u)
    }
}

impl<V, const N: usize> Drop for InflightTable<V, N> {
    fn drop(&mut self) {
        for (i, used) in self.used.iter().enumerate() {
            if *used {
                // SAFETY: slot is marked used.
                unsafe { self.vals[i].assume_init_drop() };
            }
        }
    }
}

// Host-side unit tests for the pure-logic collections. Gated off the
// no_std module build via `#[cfg(test)]`. This file is listed as an
// inline-tests exemption in fluxor.toml.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ringbuf_push_pop_fifo() {
        let mut rb: RingBuf<u32, 4> = RingBuf::new();
        assert!(rb.is_empty());
        assert!(rb.push(1).is_ok());
        assert!(rb.push(2).is_ok());
        assert!(rb.push(3).is_ok());
        assert!(rb.is_full());
        assert!(rb.push(4).is_err());
        assert_eq!(rb.pop(), Some(1));
        assert_eq!(rb.pop(), Some(2));
        assert!(rb.push(4).is_ok());
        assert_eq!(rb.pop(), Some(3));
        assert_eq!(rb.pop(), Some(4));
        assert_eq!(rb.pop(), None);
    }

    #[test]
    fn inflight_insert_get_remove() {
        let mut t: InflightTable<u32, 8> = InflightTable::new();
        assert!(t.insert(100, 1).unwrap().is_none());
        assert!(t.insert(200, 2).unwrap().is_none());
        assert_eq!(t.len(), 2);
        assert_eq!(t.get(100), Some(&1));
        assert_eq!(t.get(200), Some(&2));
        assert_eq!(t.get(300), None);
        let prev = t.insert(100, 11).unwrap();
        assert_eq!(prev, Some(1));
        assert_eq!(t.remove(100), Some(11));
        assert_eq!(t.len(), 1);
        assert_eq!(t.remove(100), None);
    }

    #[test]
    fn inflight_full_returns_err_for_new_keys() {
        let mut t: InflightTable<u32, 2> = InflightTable::new();
        assert!(t.insert(1, 10).is_ok());
        assert!(t.insert(2, 20).is_ok());
        let result = t.insert(3, 30);
        assert!(result.is_err());
        assert!(t.insert(1, 99).is_ok());
        assert_eq!(t.get(1), Some(&99));
    }
}
