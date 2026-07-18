//! Watch-replay codec (RFC database foundation §15, Phase-2 slice C).
//!
//! Dual-target facade — `no_std` inside the PIC module, host-testable
//! here — holding the three transformations a resume backfill needs:
//!
//! 1. decode a `MSG_WATCH_REPLAY_PLAN` into a bounded [`ReplayPlan`];
//! 2. build the `KV_OP_SCAN_VERSIONS` body for one page of it;
//! 3. turn each returned version into the frame a LIVE event would have
//!    produced.
//!
//! Step 3 carries the property the whole feature rests on: a resumed
//! stream and a continuous one must be indistinguishable to the client.
//! If a replayed frame differed in shape — a missing field, a different
//! event kind for a delete, a synthesised revision — a client could
//! tell that it had reconnected, and worse, could act on the difference.
//! So the frame builder here is the ONLY frame builder, and the live
//! path's shape is the shape it emits.
//!
//! The span logic in [`ReplayPlan::scan_body`] is the subtle part. A
//! watch is either single-key or a `[key, range_end)` range, but the
//! provider reads an EMPTY `end` as unbounded-above. A single-key watch
//! therefore cannot send its empty `range_end` through unchanged — that
//! would backfill the entire keyspace from that key onward and deliver
//! every other key's history to one watcher. It sends `key‖0x00`
//! instead, the immediate successor, which expresses "exactly this key"
//! as a half-open range.

// No crate-level attributes: this file is `#[path]`-mounted as a MODULE
// by both the PIC build and the host test crate, and `#![no_std]` is
// only legal at a crate root. The code is heap-free and `core`-only, so
// it needs no attribute to be no_std-compatible — the sibling facades
// (`watch_hub`, `ttl_scheduler`) follow the same rule.
// Unused-surface allows live at each crate ROOT that mounts this file
// (the PIC `mod.rs` and the host test crate), not here, for the same
// reason: a `#[path]`-mounted module cannot carry crate attributes.

/// Cap on a watch's key and range_end, each. Mirrors
/// `watch_hub::WATCH_KEY_MAX`, since a plan's span is copied out of a
/// `WatchRecord`.
pub const PLAN_KEY_MAX: usize = 96;

/// Fixed head of a `MSG_WATCH_REPLAY_PLAN`:
/// `[watch_id:8][from:8][to:8]`, then the two length-prefixed bounds.
const PLAN_HEAD: usize = 24;

/// Largest `KV_OP_SCAN_VERSIONS` body a plan can produce: two
/// length-prefixed bounds (the synthesised single-key end is one byte
/// longer than the key) plus `from`, `to`, `cursor`, `limit`.
pub const SCAN_BODY_MAX: usize = 2 + PLAN_KEY_MAX + 2 + (PLAN_KEY_MAX + 1) + 8 + 8 + 8 + 2;

/// Event kinds on the wire, mirroring `watch_hub`.
pub const WATCH_EVENT_PUT: u8 = 0;
pub const WATCH_EVENT_DELETE: u8 = 1;

/// Version kinds in a `KV_RESULT_VERSIONS` entry, mirroring `types`.
pub const VERSION_KIND_PUT: u8 = 0x00;
pub const VERSION_KIND_DELETE: u8 = 0x01;

/// One queued or in-flight backfill.
#[derive(Clone, Copy)]
pub struct ReplayPlan {
    pub watch_id: u64,
    /// Exclusive lower bound — the watch's `last_sent_revision`.
    pub from: u64,
    /// Inclusive upper bound; `0` means "up to now".
    pub to: u64,
    /// Provider cursor for the next page; `0` on the first request.
    pub cursor: u64,
    key_len: u16,
    end_len: u16,
    key_buf: [u8; PLAN_KEY_MAX * 2],
}

impl ReplayPlan {
    pub const fn empty() -> Self {
        Self {
            watch_id: 0,
            from: 0,
            to: 0,
            cursor: 0,
            key_len: 0,
            end_len: 0,
            key_buf: [0; PLAN_KEY_MAX * 2],
        }
    }

    pub fn key(&self) -> &[u8] {
        let n = self.key_len as usize;
        self.key_buf.get(..n).unwrap_or(&[])
    }

    pub fn range_end(&self) -> &[u8] {
        let a = self.key_len as usize;
        let b = a + self.end_len as usize;
        self.key_buf.get(a..b).unwrap_or(&[])
    }

    /// True when this plan asks for nothing: an empty span (the watch
    /// vanished before the plan was built) or an already-closed window.
    ///
    /// Both are legitimate no-ops rather than failures. Scanning for an
    /// empty span would read the whole keyspace to discover there is no
    /// watch to deliver to, and a client that reconnected inside one
    /// revision genuinely missed nothing.
    pub fn is_empty(&self) -> bool {
        if self.key_len == 0 && self.end_len == 0 {
            return true;
        }
        // `to == 0` is the live-tail sentinel and is never empty.
        self.to != 0 && self.from >= self.to
    }

    /// Build the `KV_OP_SCAN_VERSIONS` body for the next page:
    /// `[start_len:u16][start][end_len:u16][end][from:u64][to:u64]`
    /// `[cursor:u64][limit:u16]`, all little-endian.
    pub fn scan_body(&self, limit: u16, out: &mut [u8]) -> Option<usize> {
        let k = self.key();
        let e = self.range_end();
        // A single-key watch has no `range_end`, and an empty `end`
        // means unbounded to the provider. Sending it through unchanged
        // would backfill everything at or above the key. `key‖0x00` is
        // the immediate successor, so `[key, key‖0x00)` is exactly the
        // one key — the smallest half-open range that contains it.
        let mut single = [0u8; PLAN_KEY_MAX + 1];
        let end_bytes: &[u8] = if e.is_empty() {
            if k.len() >= single.len() {
                return None;
            }
            single[..k.len()].copy_from_slice(k);
            single[k.len()] = 0x00;
            &single[..k.len() + 1]
        } else {
            e
        };
        let need = 2 + k.len() + 2 + end_bytes.len() + 8 + 8 + 8 + 2;
        if out.len() < need {
            return None;
        }
        let mut n = 0usize;
        out[n..n + 2].copy_from_slice(&(k.len() as u16).to_le_bytes());
        n += 2;
        out[n..n + k.len()].copy_from_slice(k);
        n += k.len();
        out[n..n + 2].copy_from_slice(&(end_bytes.len() as u16).to_le_bytes());
        n += 2;
        out[n..n + end_bytes.len()].copy_from_slice(end_bytes);
        n += end_bytes.len();
        out[n..n + 8].copy_from_slice(&self.from.to_le_bytes());
        n += 8;
        out[n..n + 8].copy_from_slice(&self.to.to_le_bytes());
        n += 8;
        out[n..n + 8].copy_from_slice(&self.cursor.to_le_bytes());
        n += 8;
        out[n..n + 2].copy_from_slice(&limit.to_le_bytes());
        n += 2;
        Some(n)
    }
}

/// Decode a `MSG_WATCH_REPLAY_PLAN` payload:
/// `[watch_id:8][from:8][to:8][key_len:2][key][end_len:2][end]`.
///
/// Fails closed on anything malformed. A partially-decoded plan would
/// produce a backfill over the wrong span, which delivers another
/// watcher's history to this client — strictly worse than no backfill.
pub fn parse_replay_plan(payload: &[u8]) -> Option<ReplayPlan> {
    if payload.len() < PLAN_HEAD + 4 {
        return None;
    }
    let mut p = ReplayPlan::empty();
    p.watch_id = u64::from_le_bytes(sl8(payload, 0)?);
    p.from = u64::from_le_bytes(sl8(payload, 8)?);
    p.to = u64::from_le_bytes(sl8(payload, 16)?);
    let klen = u16::from_le_bytes([payload[PLAN_HEAD], payload[PLAN_HEAD + 1]]) as usize;
    if klen > PLAN_KEY_MAX || payload.len() < PLAN_HEAD + 2 + klen + 2 {
        return None;
    }
    let eo = PLAN_HEAD + 2 + klen;
    let elen = u16::from_le_bytes([payload[eo], payload[eo + 1]]) as usize;
    if elen > PLAN_KEY_MAX || payload.len() < eo + 2 + elen {
        return None;
    }
    p.key_buf[..klen].copy_from_slice(&payload[PLAN_HEAD + 2..PLAN_HEAD + 2 + klen]);
    p.key_buf[klen..klen + elen].copy_from_slice(&payload[eo + 2..eo + 2 + elen]);
    p.key_len = klen as u16;
    p.end_len = elen as u16;
    Some(p)
}

/// One decoded entry from a `KV_RESULT_VERSIONS` page.
pub struct VersionEntry<'a> {
    pub revision: u64,
    pub kind: u8,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

/// Walk a `KV_RESULT_VERSIONS` body, calling `f` per entry, and return
/// the page's `next_cursor`. `None` on a malformed body.
///
/// A malformed page abandons the whole backfill rather than delivering
/// the entries decoded so far: a truncated history is indistinguishable
/// from a complete one once it reaches the client.
pub fn walk_versions_page(body: &[u8], mut f: impl FnMut(VersionEntry<'_>)) -> Option<u64> {
    if body.len() < 10 {
        return None;
    }
    let next_cursor = u64::from_le_bytes(sl8(body, 0)?);
    let count = u16::from_le_bytes([body[8], body[9]]) as usize;
    let mut p = 10usize;
    for _ in 0..count {
        // [revision:8][kind:1][klen:2][key][vlen:4][value]
        if body.len() < p + 15 {
            return None;
        }
        let revision = u64::from_le_bytes(sl8(body, p)?);
        let kind = body[p + 8];
        let klen = u16::from_le_bytes([body[p + 9], body[p + 10]]) as usize;
        p += 11;
        if body.len() < p + klen + 4 {
            return None;
        }
        let key = &body[p..p + klen];
        p += klen;
        let vlen = u32::from_le_bytes([body[p], body[p + 1], body[p + 2], body[p + 3]]) as usize;
        p += 4;
        if body.len() < p + vlen {
            return None;
        }
        let value = &body[p..p + vlen];
        p += vlen;
        f(VersionEntry {
            revision,
            kind,
            key,
            value,
        });
    }
    Some(next_cursor)
}

/// Build the watch frame for one replayed version:
/// `[watch_id:8][kpg:2][rev:8][op:1][klen:2][key][vlen:2][value]`.
///
/// Byte-identical in shape to the frame the live path emits — that
/// identity is the contract, not an implementation detail. A client
/// must not be able to tell a replayed event from a streamed one.
pub fn build_frame(
    watch_id: u64,
    kpg_id: u16,
    entry: &VersionEntry<'_>,
    out: &mut [u8],
) -> Option<usize> {
    if entry.value.len() > u16::MAX as usize || entry.key.len() > u16::MAX as usize {
        return None;
    }
    let need = 8 + 2 + 8 + 1 + 2 + entry.key.len() + 2 + entry.value.len();
    if out.len() < need {
        return None;
    }
    out[0..8].copy_from_slice(&watch_id.to_le_bytes());
    out[8..10].copy_from_slice(&kpg_id.to_le_bytes());
    out[10..18].copy_from_slice(&entry.revision.to_le_bytes());
    // A tombstone becomes a DELETE event. Mapping it to a PUT with an
    // empty value would leave the client believing the key still exists
    // with no content, which is a different and wrong fact.
    out[18] = if entry.kind == VERSION_KIND_DELETE {
        WATCH_EVENT_DELETE
    } else {
        WATCH_EVENT_PUT
    };
    out[19..21].copy_from_slice(&(entry.key.len() as u16).to_le_bytes());
    out[21..21 + entry.key.len()].copy_from_slice(entry.key);
    let vo = 21 + entry.key.len();
    out[vo..vo + 2].copy_from_slice(&(entry.value.len() as u16).to_le_bytes());
    out[vo + 2..vo + 2 + entry.value.len()].copy_from_slice(entry.value);
    Some(need)
}

fn sl8(buf: &[u8], at: usize) -> Option<[u8; 8]> {
    let s = buf.get(at..at + 8)?;
    let mut out = [0u8; 8];
    out.copy_from_slice(s);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_bytes(watch_id: u64, from: u64, to: u64, key: &[u8], end: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&watch_id.to_le_bytes());
        b.extend_from_slice(&from.to_le_bytes());
        b.extend_from_slice(&to.to_le_bytes());
        b.extend_from_slice(&(key.len() as u16).to_le_bytes());
        b.extend_from_slice(key);
        b.extend_from_slice(&(end.len() as u16).to_le_bytes());
        b.extend_from_slice(end);
        b
    }

    /// Decode `scan_body` back into its fields so the tests can assert
    /// on meaning rather than on offsets.
    fn decode_scan_body(b: &[u8]) -> (Vec<u8>, Vec<u8>, u64, u64, u64, u16) {
        let klen = u16::from_le_bytes([b[0], b[1]]) as usize;
        let key = b[2..2 + klen].to_vec();
        let eo = 2 + klen;
        let elen = u16::from_le_bytes([b[eo], b[eo + 1]]) as usize;
        let end = b[eo + 2..eo + 2 + elen].to_vec();
        let mut p = eo + 2 + elen;
        let from = u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
        p += 8;
        let to = u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
        p += 8;
        let cursor = u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
        p += 8;
        let limit = u16::from_le_bytes([b[p], b[p + 1]]);
        assert_eq!(p + 2, b.len(), "scan body has trailing bytes");
        (key, end, from, to, cursor, limit)
    }

    #[test]
    fn plan_round_trips() {
        let bytes = plan_bytes(7, 100, 200, b"foo", b"fop");
        let p = parse_replay_plan(&bytes).unwrap();
        assert_eq!(p.watch_id, 7);
        assert_eq!(p.from, 100);
        assert_eq!(p.to, 200);
        assert_eq!(p.key(), b"foo");
        assert_eq!(p.range_end(), b"fop");
    }

    /// A truncated plan is refused rather than partially decoded — a
    /// half-read span would backfill the wrong keys, delivering another
    /// watcher's history to this client.
    #[test]
    fn truncated_plan_refuses() {
        let bytes = plan_bytes(7, 100, 200, b"foo", b"fop");
        for cut in 0..bytes.len() {
            assert!(
                parse_replay_plan(&bytes[..cut]).is_none(),
                "truncation to {cut} bytes was accepted"
            );
        }
    }

    /// An oversized span is refused, not clamped. Clamping would
    /// silently scan a different range than the watch covers.
    #[test]
    fn oversized_span_refuses() {
        let big = vec![b'k'; PLAN_KEY_MAX + 1];
        assert!(parse_replay_plan(&plan_bytes(1, 0, 0, &big, b"")).is_none());
        assert!(parse_replay_plan(&plan_bytes(1, 0, 0, b"k", &big)).is_none());
    }

    /// THE span bug this facade exists to prevent: a single-key watch
    /// must not send an empty `end`, because the provider reads that as
    /// unbounded and would replay every key at or above it.
    #[test]
    fn single_key_watch_scans_exactly_one_key() {
        let p = parse_replay_plan(&plan_bytes(1, 5, 0, b"foo", b"")).unwrap();
        let mut out = [0u8; SCAN_BODY_MAX];
        let n = p.scan_body(32, &mut out).unwrap();
        let (key, end, from, to, cursor, limit) = decode_scan_body(&out[..n]);
        assert_eq!(key, b"foo");
        assert_eq!(end, b"foo\x00", "single-key span must be [key, key\\0)");
        assert!(
            !end.is_empty(),
            "an empty end would scan the whole keyspace"
        );
        assert_eq!((from, to, cursor, limit), (5, 0, 0, 32));
    }

    /// A range watch passes its `range_end` through untouched.
    #[test]
    fn range_watch_keeps_its_bound() {
        let p = parse_replay_plan(&plan_bytes(1, 5, 9, b"a", b"b")).unwrap();
        let mut out = [0u8; SCAN_BODY_MAX];
        let n = p.scan_body(64, &mut out).unwrap();
        let (key, end, from, to, _, limit) = decode_scan_body(&out[..n]);
        assert_eq!(
            (key.as_slice(), end.as_slice()),
            (b"a".as_slice(), b"b".as_slice())
        );
        assert_eq!((from, to, limit), (5, 9, 64));
    }

    /// The cursor is what makes paging resumable, so it must reach the
    /// body rather than being reset per page.
    #[test]
    fn cursor_travels_into_the_body() {
        let mut p = parse_replay_plan(&plan_bytes(1, 5, 9, b"a", b"b")).unwrap();
        p.cursor = 4242;
        let mut out = [0u8; SCAN_BODY_MAX];
        let n = p.scan_body(32, &mut out).unwrap();
        assert_eq!(decode_scan_body(&out[..n]).4, 4242);
    }

    #[test]
    fn empty_plans_are_recognised() {
        // No span: the watch vanished before the plan was built.
        assert!(parse_replay_plan(&plan_bytes(1, 0, 0, b"", b""))
            .unwrap()
            .is_empty());
        // Closed window: the client missed nothing.
        assert!(parse_replay_plan(&plan_bytes(1, 9, 9, b"k", b""))
            .unwrap()
            .is_empty());
        assert!(parse_replay_plan(&plan_bytes(1, 9, 5, b"k", b""))
            .unwrap()
            .is_empty());
        // `to == 0` is live-tail, never empty.
        assert!(!parse_replay_plan(&plan_bytes(1, 9, 0, b"k", b""))
            .unwrap()
            .is_empty());
    }

    // ── Page walking and framing ─────────────────────────────────────

    fn versions_page(next: u64, entries: &[(u64, u8, &[u8], &[u8])]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&next.to_le_bytes());
        b.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        for (rev, kind, k, v) in entries {
            b.extend_from_slice(&rev.to_le_bytes());
            b.push(*kind);
            b.extend_from_slice(&(k.len() as u16).to_le_bytes());
            b.extend_from_slice(k);
            b.extend_from_slice(&(v.len() as u32).to_le_bytes());
            b.extend_from_slice(v);
        }
        b
    }

    #[test]
    fn page_walk_yields_every_entry_in_order() {
        let page = versions_page(
            77,
            &[
                (3, VERSION_KIND_PUT, b"k", b"v3"),
                (2, VERSION_KIND_DELETE, b"k", b""),
                (1, VERSION_KIND_PUT, b"k", b"v1"),
            ],
        );
        let mut seen = Vec::new();
        let next = walk_versions_page(&page, |e| seen.push((e.revision, e.kind, e.value.to_vec())))
            .unwrap();
        assert_eq!(next, 77);
        assert_eq!(
            seen,
            vec![
                (3, VERSION_KIND_PUT, b"v3".to_vec()),
                (2, VERSION_KIND_DELETE, Vec::new()),
                (1, VERSION_KIND_PUT, b"v1".to_vec()),
            ],
            "every version in the window, not just the survivor"
        );
    }

    /// A truncated page is refused outright. Delivering the entries
    /// decoded so far would hand the client a partial history it could
    /// not distinguish from a complete one.
    #[test]
    fn truncated_page_refuses() {
        let page = versions_page(0, &[(3, VERSION_KIND_PUT, b"k", b"v3")]);
        for cut in 0..page.len() {
            assert!(
                walk_versions_page(&page[..cut], |_| {}).is_none(),
                "truncation to {cut} bytes was accepted"
            );
        }
    }

    /// A replayed delete must reach the client AS a delete. Mapping it
    /// to an empty put would leave the client believing the key still
    /// exists with no content — a different, wrong fact.
    #[test]
    fn tombstones_frame_as_delete_events() {
        let entry = VersionEntry {
            revision: 9,
            kind: VERSION_KIND_DELETE,
            key: b"gone",
            value: b"",
        };
        let mut out = [0u8; 128];
        let n = build_frame(42, 0, &entry, &mut out).unwrap();
        assert_eq!(u64::from_le_bytes(out[0..8].try_into().unwrap()), 42);
        assert_eq!(u64::from_le_bytes(out[10..18].try_into().unwrap()), 9);
        assert_eq!(out[18], WATCH_EVENT_DELETE);
        assert_eq!(&out[21..25], b"gone");
        assert_eq!(n, 8 + 2 + 8 + 1 + 2 + 4 + 2);
    }

    #[test]
    fn puts_frame_with_their_value() {
        let entry = VersionEntry {
            revision: 5,
            kind: VERSION_KIND_PUT,
            key: b"k",
            value: b"hello",
        };
        let mut out = [0u8; 128];
        let n = build_frame(1, 0, &entry, &mut out).unwrap();
        assert_eq!(out[18], WATCH_EVENT_PUT);
        assert_eq!(&out[n - 5..n], b"hello");
    }

    /// A frame that does not fit is refused rather than truncated. A
    /// truncated frame would decode as a different event downstream.
    #[test]
    fn undersized_frame_buffer_refuses() {
        let entry = VersionEntry {
            revision: 5,
            kind: VERSION_KIND_PUT,
            key: b"k",
            value: b"hello",
        };
        let mut out = [0u8; 8];
        assert!(build_frame(1, 0, &entry, &mut out).is_none());
    }
}
