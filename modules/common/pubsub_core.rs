//! pubsub_core — the subscription table for Lattice's pub/sub worker.
//!
//! Redis pub/sub has no cursor: a message published while a subscriber
//! is disconnected is gone, and the subscriber cannot detect the gap. Keeping the subscription state in a
//! *worker* — not in the anchor's per-connection slot — is what lets a
//! subscriber's session survive a worker move, so a maintenance window
//! is not silent data loss.
//!
//! This core owns the exact-channel and glob-pattern matching and the
//! per-session subscription list. It is the application record a pub/sub
//! session carries across a handoff; `session_worker` owns the session
//! lifecycle around it. Dual-target: host-tested here, compiled into the
//! `pubsub_worker` module.

#![allow(
    dead_code,
    reason = "shared via #[path] into the pubsub_worker module; host tests use the rest"
)]

/// Subscriptions per session (exact + pattern combined).
pub const MAX_SUBS_PER_SESSION: usize = 16;
/// Bytes per channel or pattern name.
pub const SUB_NAME_MAX: usize = 64;

/// One subscription: an exact channel or a glob pattern.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Sub {
    /// Name bytes in `buf[..len]`. `len == 0` marks a free entry.
    len: u16,
    /// True for a `PSUBSCRIBE` glob pattern, false for an exact channel.
    is_pattern: bool,
    buf: [u8; SUB_NAME_MAX],
}

impl Sub {
    pub const fn free() -> Self {
        Self {
            len: 0,
            is_pattern: false,
            buf: [0; SUB_NAME_MAX],
        }
    }

    pub fn in_use(&self) -> bool {
        self.len != 0
    }

    pub fn name(&self) -> &[u8] {
        self.buf.get(..self.len as usize).unwrap_or(&[])
    }

    pub fn is_pattern(&self) -> bool {
        self.is_pattern
    }
}

/// A session's subscription set.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SubTable {
    subs: [Sub; MAX_SUBS_PER_SESSION],
}

impl Default for SubTable {
    fn default() -> Self {
        Self::new()
    }
}

impl SubTable {
    pub const fn new() -> Self {
        Self {
            subs: [Sub::free(); MAX_SUBS_PER_SESSION],
        }
    }

    /// Active subscription count (exact + pattern).
    pub fn count(&self) -> usize {
        self.subs.iter().filter(|s| s.in_use()).count()
    }

    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    fn find(&self, name: &[u8], is_pattern: bool) -> Option<usize> {
        self.subs
            .iter()
            .position(|s| s.in_use() && s.is_pattern == is_pattern && s.name() == name)
    }

    /// Add a subscription. Idempotent; returns the count afterwards, or
    /// `None` if the name is too long or the table is full.
    pub fn add(&mut self, name: &[u8], is_pattern: bool) -> Option<usize> {
        if name.is_empty() || name.len() > SUB_NAME_MAX {
            return None;
        }
        if self.find(name, is_pattern).is_some() {
            return Some(self.count());
        }
        let idx = self.subs.iter().position(|s| !s.in_use())?;
        self.subs[idx].len = name.len() as u16;
        self.subs[idx].is_pattern = is_pattern;
        self.subs[idx].buf[..name.len()].copy_from_slice(name);
        Some(self.count())
    }

    /// Remove a subscription. Idempotent; returns the count afterwards.
    pub fn remove(&mut self, name: &[u8], is_pattern: bool) -> usize {
        if let Some(idx) = self.find(name, is_pattern) {
            self.subs[idx] = Sub::free();
        }
        self.count()
    }

    /// Remove every subscription of the given kind. Returns the count
    /// afterwards.
    pub fn remove_all(&mut self, is_pattern: bool) -> usize {
        for s in self.subs.iter_mut() {
            if s.in_use() && s.is_pattern == is_pattern {
                *s = Sub::free();
            }
        }
        self.count()
    }

    /// Does this session receive a message on `channel`? Returns the
    /// matching pattern for a pattern hit (`pmessage` carries it), or
    /// an empty slice for an exact hit, or `None` for no match.
    pub fn delivery_for<'a>(&'a self, channel: &[u8]) -> Option<&'a [u8]> {
        // Exact channels first.
        for s in self.subs.iter() {
            if s.in_use() && !s.is_pattern && s.name() == channel {
                return Some(&[]);
            }
        }
        for s in self.subs.iter() {
            if s.in_use() && s.is_pattern && glob_match(s.name(), channel) {
                return Some(s.name());
            }
        }
        None
    }

    /// Serialise for a handoff blob: `[count:2]` then, per sub,
    /// `[is_pattern:1][len:2][name…]`. Returns bytes written, or 0 if
    /// `out` is too short.
    pub fn export(&self, out: &mut [u8]) -> usize {
        let mut n = 2usize;
        let mut count = 0u16;
        for s in self.subs.iter() {
            if !s.in_use() {
                continue;
            }
            let nm = s.name();
            if n + 3 + nm.len() > out.len() {
                return 0;
            }
            out[n] = s.is_pattern as u8;
            n += 1;
            out[n..n + 2].copy_from_slice(&(nm.len() as u16).to_le_bytes());
            n += 2;
            out[n..n + nm.len()].copy_from_slice(nm);
            n += nm.len();
            count += 1;
        }
        out[0..2].copy_from_slice(&count.to_le_bytes());
        n
    }

    /// Inverse of `export`. `None` on a malformed blob.
    pub fn import(src: &[u8]) -> Option<Self> {
        if src.len() < 2 {
            return None;
        }
        let count = u16::from_le_bytes([src[0], src[1]]) as usize;
        if count > MAX_SUBS_PER_SESSION {
            return None;
        }
        let mut t = SubTable::new();
        let mut at = 2usize;
        for _ in 0..count {
            if at + 3 > src.len() {
                return None;
            }
            let is_pattern = src[at] != 0;
            let len = u16::from_le_bytes([src[at + 1], src[at + 2]]) as usize;
            at += 3;
            if len == 0 || len > SUB_NAME_MAX || at + len > src.len() {
                return None;
            }
            t.add(&src[at..at + len], is_pattern)?;
            at += len;
        }
        Some(t)
    }
}

/// Redis-style glob match: `*` (any run), `?` (one byte), `[…]` class
/// (with `^` negation and `a-z` ranges), `\` escape. Byte-oriented,
/// zero-alloc, matching redis `stringmatchlen`.
pub fn glob_match(pattern: &[u8], s: &[u8]) -> bool {
    glob_at(pattern, 0, s, 0)
}

fn glob_at(p: &[u8], mut pi: usize, s: &[u8], mut si: usize) -> bool {
    while pi < p.len() {
        match p[pi] {
            b'*' => {
                // Collapse consecutive stars.
                while pi + 1 < p.len() && p[pi + 1] == b'*' {
                    pi += 1;
                }
                if pi + 1 == p.len() {
                    return true; // trailing star matches the rest
                }
                let mut k = si;
                while k <= s.len() {
                    if glob_at(p, pi + 1, s, k) {
                        return true;
                    }
                    k += 1;
                }
                return false;
            }
            b'?' => {
                if si >= s.len() {
                    return false;
                }
                si += 1;
                pi += 1;
            }
            b'[' => {
                if si >= s.len() {
                    return false;
                }
                pi += 1;
                let mut negate = false;
                if pi < p.len() && p[pi] == b'^' {
                    negate = true;
                    pi += 1;
                }
                let mut matched = false;
                while pi < p.len() && p[pi] != b']' {
                    if p[pi] == b'\\' && pi + 1 < p.len() {
                        pi += 1;
                        if p[pi] == s[si] {
                            matched = true;
                        }
                        pi += 1;
                    } else if pi + 2 < p.len() && p[pi + 1] == b'-' && p[pi + 2] != b']' {
                        let (lo, hi) = (p[pi], p[pi + 2]);
                        let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
                        if s[si] >= lo && s[si] <= hi {
                            matched = true;
                        }
                        pi += 3;
                    } else {
                        if p[pi] == s[si] {
                            matched = true;
                        }
                        pi += 1;
                    }
                }
                if pi < p.len() {
                    pi += 1; // consume ']'
                }
                if matched == negate {
                    return false;
                }
                si += 1;
            }
            b'\\' if pi + 1 < p.len() => {
                pi += 1;
                if si >= s.len() || p[pi] != s[si] {
                    return false;
                }
                si += 1;
                pi += 1;
            }
            c => {
                if si >= s.len() || c != s[si] {
                    return false;
                }
                si += 1;
                pi += 1;
            }
        }
    }
    si == s.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_subscribe_and_match() {
        let mut t = SubTable::new();
        assert_eq!(t.add(b"news", false), Some(1));
        assert_eq!(t.add(b"news", false), Some(1), "idempotent");
        assert_eq!(t.add(b"sport", false), Some(2));
        assert_eq!(t.delivery_for(b"news"), Some(&[][..]));
        assert!(t.delivery_for(b"weather").is_none());
        assert_eq!(t.remove(b"news", false), 1);
        assert!(t.delivery_for(b"news").is_none());
    }

    #[test]
    fn pattern_subscribe_and_match() {
        let mut t = SubTable::new();
        t.add(b"news.*", true).unwrap();
        assert_eq!(t.delivery_for(b"news.sport"), Some(&b"news.*"[..]));
        assert!(t.delivery_for(b"weather.today").is_none());
        // An exact hit wins over a pattern hit for the returned marker.
        t.add(b"news.sport", false).unwrap();
        assert_eq!(t.delivery_for(b"news.sport"), Some(&[][..]));
    }

    #[test]
    fn glob_semantics() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[^ae]llo", b"hallo"));
        assert!(glob_match(b"news.[a-c]*", b"news.bulletin"));
        assert!(!glob_match(b"news.[a-c]*", b"news.zebra"));
        assert!(glob_match(b"a\\*b", b"a*b"));
        assert!(!glob_match(b"a\\*b", b"axb"));
        assert!(glob_match(b"**foo", b"foo"));
    }

    #[test]
    fn export_import_round_trips() {
        let mut t = SubTable::new();
        t.add(b"news", false).unwrap();
        t.add(b"sport.*", true).unwrap();
        let mut blob = [0u8; 512];
        let n = t.export(&mut blob);
        let back = SubTable::import(&blob[..n]).unwrap();
        assert_eq!(back.count(), 2);
        assert_eq!(back.delivery_for(b"news"), Some(&[][..]));
        assert_eq!(back.delivery_for(b"sport.hockey"), Some(&b"sport.*"[..]));
    }

    #[test]
    fn remove_all_by_kind() {
        let mut t = SubTable::new();
        t.add(b"a", false).unwrap();
        t.add(b"b", false).unwrap();
        t.add(b"p.*", true).unwrap();
        assert_eq!(t.remove_all(false), 1, "only the pattern remains");
        assert!(t.delivery_for(b"a").is_none());
        assert_eq!(t.delivery_for(b"p.x"), Some(&b"p.*"[..]));
    }
}
