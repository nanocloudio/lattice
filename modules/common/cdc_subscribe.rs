//! cdc_subscribe — the CDC feed as held exchanges: a subscriber's GET on a
//! feed target is answered `200` flagged `HOLD|MORE`, then one BODY per
//! change event, never past the subscriber's credit.
//!
//! Dual-target facade: `#[path]`-mounted by `modules/app/cdc_front` (which
//! owns channels, params and the syscall table) and by the host suite
//! (`tests/harness/tests/contract_cdc_subscribe.rs`, which drives it against
//! a simulated MVCC provider). Every side effect goes through
//! [`SubscribeIo`] or the records [`Subscriptions::next_record`] hands back;
//! the core never touches a channel or a clock.
//!
//! # The subscription
//!
//! | Request | |
//! |---|---|
//! | method | `GET` |
//! | target | `/cdc/<prefix hex>` — the keyrange, as `cdc_pump`'s `prefix` param spells it: 1..=256 bytes, not starting `0x00` (the system prefix), not all `0xFF` |
//! | `after: <revision>` | optional: resume after this commit position (decimal). Without it the feed starts at the store's head: changes committed after the subscription opened |
//! | `feed-id: <n>`, `table-id: <n>` | optional: the envelope's identity fields (defaults: the module's `feed_id`, `table_id` params, whose defaults are `cdc_pump`'s) |
//! | body | none |
//!
//! The answer is a response HEAD `200` flagged `HOLD|MORE`, carrying the
//! header `revision: <n>` — the position the feed starts after — and no
//! body. Then each change in the keyrange is one BODY record flagged `MORE`
//! whose payload is the `cdc_wire::CdcEvent` envelope `cdc_pump` publishes
//! for the same change, byte for byte: kind put/delete, flags 0, the feed and
//! table ids, key, commit timestamp, range id 1, range generation 1, the
//! change's revision, the value. A BODY is written only when the
//! subscriber's response credit covers the whole envelope: credit is the
//! subscriber's backpressure, and an event is never split.
//!
//! Events arrive in revision order (changes of one revision in key order),
//! so the `revision` field of the last event a subscriber holds is the
//! position to resume after. A subscriber stopped part-way through one
//! revision's events resumes `after: <revision - 1>` and receives that
//! revision again whole.
//!
//! | Status | When |
//! |---|---|
//! | 200 `HOLD\|MORE` | the subscription is open |
//! | 400 | another method, a request body, a target that is not `/cdc/<hex>` of a legal prefix, a malformed `after`, `feed-id` or `table-id` |
//! | 410 | the `after` position is no longer retained: the store compacted past it |
//! | 413 | a target over 517 bytes, headers over 256 |
//! | 502 | the store kept failing the feed read, or the keyrange holds a large value (stored as chunks), which the envelope cannot carry |
//! | 503 | four subscriptions already held, or draining |
//! | 504 | the store did not answer the opening read within 15 s |
//!
//! A held subscription ends by the subscriber's ABORT, or by the provider's
//! ABORT: `STALLED` when the store compacted past the position the
//! subscriber had reached (re-subscribing `after` it answers 410), `FAILED`
//! for the 502 conditions above, `DRAINING` when the module drains.
//!
//! # Reading the feed
//!
//! The core is an ordinary KV client on a router anchor pair, as
//! `cdc_pump` is: `KV_OP_SCAN_VERSIONS` reads a window `(from, to]` of the
//! keyrange, and a `KV_OP_GET` of the prefix key reads the head revision a
//! subscription without `after` starts from. One KV request is in flight at
//! a time, shared by the subscriptions round-robin.
//!
//! The provider walks a window in KEY order, so a window that does not fit
//! one page is narrowed — its upper bound halved toward `from`, page 0
//! re-read — until it does, and its page is delivered sorted by revision. A
//! window of one revision too large for a page is walked page by page: every
//! change in it shares the revision, so key order is revision order. A
//! window closes when its last page is delivered; the next opens at its
//! upper bound, at once after a window that held changes, after
//! [`POLL_MS`] after an empty one.

#![allow(
    dead_code,
    reason = "shared via #[path] into the cdc_front module and host tests; each consumer uses a subset of the surface"
)]
#![allow(
    clippy::duplicate_mod,
    reason = "dual-target facade: consumers #[path]-remount the shared common code"
)]

#[path = "types.rs"]
pub mod types;

#[path = "wire.rs"]
pub mod wire;

#[path = "cdc_wire.rs"]
pub mod cdc_wire;

// The single mount of the SDK contract for this tree; consumers reach it as
// `cdc_subscribe::exchange`.
#[path = "../../target/fluxor/fluxor-abi/sdk/contracts/exchange.rs"]
pub mod exchange;

use cdc_wire::{CdcEvent, CDC_KIND_DELETE, CDC_KIND_PUT};
use exchange::{
    abort, flag, header, parse_request, seal_body, status, write_abort, write_response_head,
    ExchangeId, Record, ResponseHead, HDR, METHOD_GET,
};
use types::{
    KV_OP_GET, KV_OP_SCAN_VERSIONS, KV_RESULT_COMPACTED, KV_RESULT_MANIFEST, KV_RESULT_NOT_FOUND,
    KV_RESULT_OK, KV_RESULT_VERSIONS, PROTO_ETCD, VERSION_KIND_DELETE,
};
use wire::MSG_KV_REQUEST;

/// Subscriptions held at once.
pub const SUBS: usize = 4;
/// Longest keyrange prefix (`cdc_pump`'s).
pub const PREFIX_MAX: usize = 256;
/// Longest target: `/cdc/` and the prefix in hex.
pub const TARGET_MAX: usize = 5 + 2 * PREFIX_MAX;
/// Largest header block read.
pub const HEADERS_MAX: usize = 256;
/// Changes asked for per feed page (`cdc_pump`'s default).
pub const PAGE_LIMIT: u16 = 16;
/// Entries one staged page holds: the page limit.
pub const PAGE_ENTRIES: usize = 16;
/// One staged page: a whole KV answer body.
pub const PAGE_MAX: usize = wire::KV_RESPONSE_BODY_MAX;
/// Records owed on `response_out` besides the subscriptions' own: a
/// terminal per subscription and the refusal one read can add.
pub const OWED_MAX: usize = SUBS + 2;
/// The outgoing KV request: envelope, head, and the largest body (two
/// prefix-long span bounds and the window).
pub const KV_FRAME_MAX: usize = wire::ENVELOPE_HDR + KV_REQ_HEAD + 2 * (2 + PREFIX_MAX) + 26;
/// Consecutive failed feed reads before a subscription ends `FAILED`.
pub const WEDGE_BUDGET: u32 = 16;

/// A caught-up subscription's poll cadence (`cdc_pump`'s).
pub const POLL_MS: u64 = 100;
/// A KV request unanswered this long is issued again.
pub const KV_RETRY_MS: u64 = 3000;
/// A subscription not open this long after its request is answered 504.
pub const OPEN_TIMEOUT_MS: u64 = 15_000;

/// The `MSG_KV_REQUEST` head: corr, protocol, tenant, conn, consistency,
/// op, body length.
const KV_REQ_HEAD: usize = 18;
/// Correlation ids carry this bit: the router's inflight table is keyed by
/// the whole id and shared by every requester, and `cdc_pump` owns bit 61.
const CORR_BASE: u64 = 0x1000_0000_0000_0000;
/// The answer to a subscription whose `after` position the store no longer
/// retains: HTTP's Gone, as a Kubernetes watch answers a compacted resource
/// version. An answer about the position, not a status the provider raises
/// (it is not marked `RAISED`).
pub const GONE: u16 = 410;
/// A versions entry's fixed part: revision, commit_ts, kind, key length,
/// value length.
const ENTRY_FIXED: usize = 8 + 8 + 1 + 2 + 4;

/// The KV request side effect. A `false` is channel backpressure: the core
/// issues the request again on a later step.
pub trait SubscribeIo {
    /// Ship one `MSG_KV_REQUEST` frame (envelope included) toward the
    /// router pair. `&mut` because the channel-write ABI takes a mutable
    /// pointer; implementations must not mutate it.
    fn send_kv(&mut self, frame: &mut [u8]) -> bool;
}

// Subscription phases.
const PH_FREE: u8 = 0;
/// Reading the head revision (no `after`).
const PH_HEAD: u8 = 1;
/// Reading the first window from `after`: not yet known to be retained.
const PH_OPEN: u8 = 2;
/// Open: the HEAD is owed or written.
const PH_LIVE: u8 = 3;

/// One held subscription.
pub struct Sub {
    phase: u8,
    id: ExchangeId,
    /// Bumped on every reuse, so a KV answer for a freed subscription is
    /// never applied to its successor.
    generation: u32,
    head_owed: bool,
    head_sent: bool,
    credit: u32,
    feed_id: u64,
    table_id: u32,
    prefix: [u8; PREFIX_MAX],
    prefix_len: u16,
    /// The window `(from, to]`; `to == 0` until its first page pins it.
    from: u64,
    to: u64,
    cursor: u64,
    /// The window's upper bound was narrowed: the store holds more past it.
    narrowed: bool,
    /// The staged page is the window's last.
    closes: bool,
    /// The window delivered a change.
    had_events: bool,
    next_read_ms: u64,
    opened_ms: u64,
    wedge: u32,
    /// The staged page: raw versions entries, and their offsets in
    /// revision order.
    page: [u8; PAGE_MAX],
    order: [u16; PAGE_ENTRIES],
    count: u8,
    next: u8,
}

impl Sub {
    const fn new() -> Sub {
        Sub {
            phase: PH_FREE,
            id: ExchangeId::NONE,
            generation: 0,
            head_owed: false,
            head_sent: false,
            credit: 0,
            feed_id: 0,
            table_id: 0,
            prefix: [0; PREFIX_MAX],
            prefix_len: 0,
            from: 0,
            to: 0,
            cursor: 0,
            narrowed: false,
            closes: false,
            had_events: false,
            next_read_ms: 0,
            opened_ms: 0,
            wedge: 0,
            page: [0; PAGE_MAX],
            order: [0; PAGE_ENTRIES],
            count: 0,
            next: 0,
        }
    }

    fn live(&self) -> bool {
        self.phase != PH_FREE
    }

    fn staged(&self) -> bool {
        self.next < self.count
    }
}

/// One record owed on `response_out` outside a subscription's stream.
#[derive(Clone, Copy)]
enum Owed {
    /// A whole answer with no body: a refusal, or a subscription that never
    /// opened.
    Answer { id: ExchangeId, status: u16 },
    /// The provider ends a held subscription.
    Abort { id: ExchangeId, reason: u8 },
}

/// Every subscription the module holds, and what it owes.
pub struct Subscriptions {
    // ── params ──
    /// The envelope's `feed_id` when a request names none.
    pub feed_id: u64,
    /// The envelope's `table_id` when a request names none.
    pub table_id: u32,
    /// The protocol byte of the router pair the module is wired to.
    pub proto: u8,

    subs: [Sub; SUBS],
    owed: [Owed; OWED_MAX],
    owed_len: usize,
    draining: bool,
    /// Where the next record and the next KV read look first.
    rr_out: usize,
    rr_kv: usize,

    // The one KV request in flight.
    kv_busy: bool,
    kv_corr: u64,
    kv_seq: u64,
    kv_sub: usize,
    kv_generation: u32,
    kv_sent_ms: u64,
    frame: [u8; KV_FRAME_MAX],

    // ── counters ──
    pub m_opened: u64,
    pub m_events: u64,
    pub m_refused: u64,
    pub m_ended: u64,
}

impl Subscriptions {
    pub const fn new() -> Subscriptions {
        Subscriptions {
            feed_id: 1,
            table_id: 0,
            proto: PROTO_ETCD,
            subs: [const { Sub::new() }; SUBS],
            owed: [Owed::Answer {
                id: ExchangeId::NONE,
                status: 0,
            }; OWED_MAX],
            owed_len: 0,
            draining: false,
            rr_out: 0,
            rr_kv: 0,
            kv_busy: false,
            kv_corr: 0,
            kv_seq: 0,
            kv_sub: 0,
            kv_generation: 0,
            kv_sent_ms: 0,
            frame: [0; KV_FRAME_MAX],
            m_opened: 0,
            m_events: 0,
            m_refused: 0,
            m_ended: 0,
        }
    }

    /// Return to the state of [`Subscriptions::new`] with the params kept,
    /// in place — for a module whose state arrives as raw memory.
    pub fn reset(&mut self) {
        for s in self.subs.iter_mut() {
            s.phase = PH_FREE;
            s.generation = 0;
            s.count = 0;
            s.next = 0;
        }
        self.owed_len = 0;
        self.draining = false;
        self.rr_out = 0;
        self.rr_kv = 0;
        self.kv_busy = false;
        self.kv_corr = 0;
        self.kv_seq = 0;
        self.m_opened = 0;
        self.m_events = 0;
        self.m_refused = 0;
        self.m_ended = 0;
    }

    /// Subscriptions held.
    pub fn held(&self) -> usize {
        self.subs.iter().filter(|s| s.live()).count()
    }

    /// Whether the module may read one request record: the owed queue has
    /// room for a terminal per subscription and the refusal one read can
    /// add. Reading stops otherwise — the requester's backpressure.
    pub fn can_take(&self) -> bool {
        self.owed_len + self.held() < OWED_MAX
    }

    /// Nothing held, owed or in flight.
    pub fn idle(&self) -> bool {
        self.held() == 0 && self.owed_len == 0
    }

    // ── Requests ─────────────────────────────────────────────────────

    /// Take one request record.
    pub fn accept(&mut self, record: &[u8], now_ms: u64) {
        match parse_request(record) {
            Some(Record::Head(h)) => {
                let verdict = if self.draining {
                    Err(status::BUSY)
                } else {
                    self.judge(&h)
                };
                let req = match verdict {
                    Ok(r) => r,
                    Err(code) => {
                        self.refuse(h.id, code);
                        return;
                    }
                };
                let Some(i) = self.subs.iter().position(|s| !s.live()) else {
                    self.refuse(h.id, status::BUSY);
                    return;
                };
                let s = &mut self.subs[i];
                s.generation = s.generation.wrapping_add(1);
                s.phase = if req.after.is_some() {
                    PH_OPEN
                } else {
                    PH_HEAD
                };
                s.id = h.id;
                s.head_owed = false;
                s.head_sent = false;
                s.credit = h.resp_credit;
                s.feed_id = req.feed_id.unwrap_or(self.feed_id);
                s.table_id = req.table_id.unwrap_or(self.table_id);
                s.prefix[..req.prefix_len].copy_from_slice(&req.prefix[..req.prefix_len]);
                s.prefix_len = req.prefix_len as u16;
                s.from = req.after.unwrap_or(0);
                s.to = 0;
                s.cursor = 0;
                s.narrowed = false;
                s.closes = false;
                s.had_events = false;
                s.next_read_ms = now_ms;
                s.opened_ms = now_ms;
                s.wedge = 0;
                s.count = 0;
                s.next = 0;
            }
            Some(Record::Credit { id, bytes }) => {
                if let Some(s) = self.subs.iter_mut().find(|s| s.live() && s.id == id) {
                    s.credit = s.credit.saturating_add(bytes);
                }
            }
            Some(Record::Abort { id, .. }) => {
                if let Some(i) = self.find(&id) {
                    self.free(i);
                    self.m_ended = self.m_ended.wrapping_add(1);
                }
                self.retain(|o| match *o {
                    Owed::Answer { id: x, .. } | Owed::Abort { id: x, .. } => x != id,
                });
            }
            _ => {}
        }
    }

    /// Judge a subscription request: what it asks for, or the status that
    /// refuses it.
    fn judge(&self, h: &exchange::RequestHead<'_>) -> Result<Wanted, u16> {
        if h.target.len() > TARGET_MAX || h.headers.len() > HEADERS_MAX {
            return Err(status::TOO_LARGE);
        }
        if h.method != METHOD_GET || h.flags & flag::MORE != 0 || !h.body.is_empty() {
            return Err(status::BAD_REQUEST);
        }
        let mut w = Wanted {
            prefix: [0; PREFIX_MAX],
            prefix_len: 0,
            after: None,
            feed_id: None,
            table_id: None,
        };
        let hex = h.target.strip_prefix(b"/cdc/").ok_or(status::BAD_REQUEST)?;
        w.prefix_len = decode_hex(hex, &mut w.prefix).ok_or(status::BAD_REQUEST)?;
        let prefix = &w.prefix[..w.prefix_len];
        if prefix.first() == Some(&0x00) || prefix.iter().all(|b| *b == 0xFF) {
            return Err(status::BAD_REQUEST);
        }
        if let Some(v) = header(h.headers, b"after") {
            w.after = Some(parse_decimal(v).ok_or(status::BAD_REQUEST)?);
        }
        if let Some(v) = header(h.headers, b"feed-id") {
            w.feed_id = Some(parse_decimal(v).ok_or(status::BAD_REQUEST)?);
        }
        if let Some(v) = header(h.headers, b"table-id") {
            let t = parse_decimal(v).ok_or(status::BAD_REQUEST)?;
            w.table_id = Some(u32::try_from(t).map_err(|_| status::BAD_REQUEST)?);
        }
        Ok(w)
    }

    fn find(&self, id: &ExchangeId) -> Option<usize> {
        self.subs.iter().position(|s| s.live() && s.id == *id)
    }

    fn free(&mut self, i: usize) {
        let s = &mut self.subs[i];
        s.phase = PH_FREE;
        s.count = 0;
        s.next = 0;
        s.head_owed = false;
    }

    fn refuse(&mut self, id: ExchangeId, status: u16) {
        self.m_refused = self.m_refused.wrapping_add(1);
        self.push(Owed::Answer { id, status });
    }

    /// End subscription `i` from the provider side: a status when nothing
    /// has been answered yet, else an ABORT with `reason`.
    fn end(&mut self, i: usize, status: u16, reason: u8) {
        let (id, answered) = (self.subs[i].id, self.subs[i].head_sent);
        self.free(i);
        self.m_ended = self.m_ended.wrapping_add(1);
        if answered {
            self.push(Owed::Abort { id, reason });
        } else {
            self.push(Owed::Answer { id, status });
        }
    }

    /// Stop: every subscription held ends `DRAINING` (or 503 if not yet
    /// open), and every new request is answered 503.
    pub fn drain(&mut self) {
        self.draining = true;
        for i in 0..SUBS {
            if self.subs[i].live() {
                self.end(i, status::BUSY, abort::DRAINING);
            }
        }
    }

    // ── The feed ─────────────────────────────────────────────────────

    /// Issue the next KV read, retry one overdue, and answer 504 for a
    /// subscription that did not open in time.
    pub fn step(&mut self, io: &mut impl SubscribeIo, now_ms: u64) {
        for i in 0..SUBS {
            let s = &self.subs[i];
            if s.live()
                && !s.head_owed
                && !s.head_sent
                && now_ms.saturating_sub(s.opened_ms) > OPEN_TIMEOUT_MS
            {
                self.end(i, status::TIMEOUT, abort::FAILED);
            }
        }
        if self.kv_busy {
            let gone = !self.subs[self.kv_sub].live()
                || self.subs[self.kv_sub].generation != self.kv_generation;
            if gone || now_ms.saturating_sub(self.kv_sent_ms) > KV_RETRY_MS {
                // Abandoned, or overdue: its answer, if it ever comes, no
                // longer matches.
                self.kv_busy = false;
            } else {
                return;
            }
        }
        for k in 0..SUBS {
            let i = (self.rr_kv + k) % SUBS;
            let s = &self.subs[i];
            let wants = match s.phase {
                PH_HEAD | PH_OPEN => now_ms >= s.next_read_ms,
                PH_LIVE => !s.staged() && now_ms >= s.next_read_ms,
                _ => false,
            };
            if !wants {
                continue;
            }
            let Some(len) = self.build_read(i) else {
                self.end(i, status::BAD_GATEWAY, abort::FAILED);
                continue;
            };
            if io.send_kv(&mut self.frame[..len]) {
                self.kv_busy = true;
                self.kv_sub = i;
                self.kv_generation = self.subs[i].generation;
                self.kv_sent_ms = now_ms;
                self.rr_kv = (i + 1) % SUBS;
            }
            return;
        }
    }

    /// Build subscription `i`'s next read into `frame`: its length.
    fn build_read(&mut self, i: usize) -> Option<usize> {
        self.kv_seq = (self.kv_seq.wrapping_add(1)) & 0x0FFF_FFFF_FFFF_FFFF;
        self.kv_corr = CORR_BASE | self.kv_seq.max(1);
        let s = &self.subs[i];
        let prefix = &s.prefix[..s.prefix_len as usize];
        let at = wire::ENVELOPE_HDR + KV_REQ_HEAD;
        let f = &mut self.frame;
        let (op, body_len) = if s.phase == PH_HEAD {
            // GET body: [klen][key]. Any answer carries the engine revision.
            put(f, at, &(prefix.len() as u16).to_le_bytes())?;
            put(f, at + 2, prefix)?;
            (KV_OP_GET, 2 + prefix.len())
        } else {
            // SCAN_VERSIONS body: [slen][start][elen][end][from][to][cursor][limit].
            let mut end = [0u8; PREFIX_MAX];
            let elen = successor(prefix, &mut end)?;
            let mut p = at;
            put(f, p, &(prefix.len() as u16).to_le_bytes())?;
            p += 2;
            put(f, p, prefix)?;
            p += prefix.len();
            put(f, p, &(elen as u16).to_le_bytes())?;
            p += 2;
            put(f, p, &end[..elen])?;
            p += elen;
            put(f, p, &s.from.to_le_bytes())?;
            p += 8;
            put(f, p, &s.to.to_le_bytes())?;
            p += 8;
            put(f, p, &s.cursor.to_le_bytes())?;
            p += 8;
            put(f, p, &PAGE_LIMIT.to_le_bytes())?;
            p += 2;
            (KV_OP_SCAN_VERSIONS, p - at)
        };
        let payload = KV_REQ_HEAD + body_len;
        let e = wire::ENVELOPE_HDR;
        f[0] = MSG_KV_REQUEST;
        f[1..3].copy_from_slice(&(payload as u16).to_le_bytes());
        f[e..e + 8].copy_from_slice(&self.kv_corr.to_le_bytes());
        f[e + 8] = self.proto;
        f[e + 9..e + 13].copy_from_slice(&0u32.to_le_bytes());
        f[e + 13] = 0;
        f[e + 14] = 0;
        f[e + 15] = op;
        f[e + 16..e + 18].copy_from_slice(&(body_len as u16).to_le_bytes());
        Some(e + payload)
    }

    /// The router's answer to a KV request: `corr` as it came back.
    pub fn on_kv_response(
        &mut self,
        now_ms: u64,
        corr: u64,
        result: u8,
        revision: u64,
        body: &[u8],
    ) {
        if !self.kv_busy || corr != self.kv_corr {
            return;
        }
        self.kv_busy = false;
        let i = self.kv_sub;
        if !self.subs[i].live() || self.subs[i].generation != self.kv_generation {
            return;
        }
        if self.subs[i].phase == PH_HEAD {
            if result == KV_RESULT_OK
                || result == KV_RESULT_NOT_FOUND
                || result == KV_RESULT_MANIFEST
            {
                let s = &mut self.subs[i];
                s.from = revision;
                s.phase = PH_LIVE;
                s.head_owed = true;
                s.wedge = 0;
                s.next_read_ms = now_ms;
                self.m_opened = self.m_opened.wrapping_add(1);
            } else {
                self.failed_read(i, now_ms);
            }
            return;
        }
        match result {
            KV_RESULT_VERSIONS => self.on_page(i, now_ms, revision, body),
            // The window is gone: retention passed the position.
            KV_RESULT_COMPACTED => self.end(i, GONE, abort::STALLED),
            // A large value's manifest: the envelope carries values whole.
            KV_RESULT_MANIFEST => self.end(i, status::BAD_GATEWAY, abort::FAILED),
            _ => self.failed_read(i, now_ms),
        }
    }

    fn failed_read(&mut self, i: usize, now_ms: u64) {
        let s = &mut self.subs[i];
        s.wedge = s.wedge.saturating_add(1);
        s.next_read_ms = now_ms + POLL_MS;
        if s.wedge >= WEDGE_BUDGET {
            self.end(i, status::BAD_GATEWAY, abort::FAILED);
        }
    }

    /// A feed page for subscription `i`.
    fn on_page(&mut self, i: usize, now_ms: u64, revision: u64, body: &[u8]) {
        let Some((next_cursor, count)) = page_head(body) else {
            self.failed_read(i, now_ms);
            return;
        };
        let opening = self.subs[i].phase == PH_OPEN;
        let s = &mut self.subs[i];
        s.wedge = 0;
        if opening {
            // The position is retained: the subscription is open.
            s.phase = PH_LIVE;
            s.head_owed = true;
            self.m_opened = self.m_opened.wrapping_add(1);
        }
        let s = &mut self.subs[i];
        if s.to == 0 {
            // `to = 0` read "up to now"; the answer names the bound it
            // pinned, and the window's later pages must use the same.
            s.to = revision.max(s.from);
        }
        if next_cursor != 0 && s.cursor == 0 && s.to - s.from > 1 {
            // More than one page, walked in key order: narrow the window
            // until it fits one page, so it can be delivered in revision
            // order.
            s.to = s.from + (s.to - s.from) / 2;
            s.narrowed = true;
            s.next_read_ms = now_ms;
            return;
        }
        if stage(s, &body[10..], count).is_none() {
            self.failed_read(i, now_ms);
            return;
        }
        let s = &mut self.subs[i];
        if count > 0 {
            s.had_events = true;
        }
        if next_cursor == 0 {
            s.closes = true;
            s.cursor = 0;
        } else {
            s.closes = false;
            s.cursor = next_cursor;
        }
        if !s.staged() {
            advance(s, now_ms);
        }
    }

    // ── Writing ──────────────────────────────────────────────────────

    /// Encode the next record to write into `out` (`RECORD_MAX` bytes): an
    /// owed answer or ABORT, else a subscription's HEAD, else its next
    /// event the subscriber's credit covers. `None` when nothing can be
    /// written now. The caller places it (through an `ExchangeOutbox`).
    pub fn next_record(&mut self, now_ms: u64, out: &mut [u8]) -> Option<usize> {
        if self.owed_len > 0 {
            let n = match self.owed[0] {
                Owed::Answer { id, status } => write_response_head(
                    &ResponseHead {
                        id,
                        // Every status but `GONE` is one the provider
                        // raises itself.
                        flags: if status == GONE { 0 } else { flag::RAISED },
                        status,
                        content_type: &[],
                        headers: &[],
                        body: &[],
                    },
                    out,
                )?,
                Owed::Abort { id, reason } => write_abort(&id, reason, out)?,
            };
            self.owed.copy_within(1..self.owed_len, 0);
            self.owed_len -= 1;
            return Some(n);
        }
        for k in 0..SUBS {
            let i = (self.rr_out + k) % SUBS;
            if let Some(n) = self.next_of(i, now_ms, out) {
                self.rr_out = (i + 1) % SUBS;
                return Some(n);
            }
        }
        None
    }

    fn next_of(&mut self, i: usize, now_ms: u64, out: &mut [u8]) -> Option<usize> {
        let s = &mut self.subs[i];
        if s.phase != PH_LIVE {
            return None;
        }
        if s.head_owed {
            let mut hdr = [0u8; 32];
            let mut n = put_text(&mut hdr, 0, b"revision: ")?;
            n = put_decimal(&mut hdr, n, s.from)?;
            n = put_text(&mut hdr, n, b"\r\n")?;
            let len = write_response_head(
                &ResponseHead {
                    id: s.id,
                    flags: flag::HOLD | flag::MORE,
                    status: status::OK,
                    content_type: &[],
                    headers: &hdr[..n],
                    body: &[],
                },
                out,
            )?;
            s.head_owed = false;
            s.head_sent = true;
            return Some(len);
        }
        if !s.head_sent || !s.staged() {
            return None;
        }
        let at = s.order[s.next as usize] as usize;
        let e = entry(&s.page, at)?;
        let ev = CdcEvent {
            kind: if e.kind == VERSION_KIND_DELETE {
                CDC_KIND_DELETE
            } else {
                CDC_KIND_PUT
            },
            flags: 0,
            feed_id: s.feed_id,
            table_id: s.table_id,
            key: e.key,
            commit_ts: e.commit_ts,
            range_id: 1,
            range_generation: 1,
            revision: e.revision,
            value: e.value,
        };
        if ev.wire_len() as u64 > u64::from(s.credit) {
            return None;
        }
        let n = ev.encode(out.get_mut(HDR..)?)?;
        let len = seal_body(&s.id, flag::MORE, n, out)?;
        s.credit -= n as u32;
        s.next += 1;
        self.m_events = self.m_events.wrapping_add(1);
        let s = &mut self.subs[i];
        if !s.staged() {
            advance(s, now_ms);
        }
        Some(len)
    }

    // ── Internals ────────────────────────────────────────────────────

    fn push(&mut self, owed: Owed) {
        // `can_take` keeps a slot for every terminal and the read's refusal.
        if self.owed_len < OWED_MAX {
            self.owed[self.owed_len] = owed;
            self.owed_len += 1;
        }
    }

    fn retain(&mut self, keep: impl Fn(&Owed) -> bool) {
        let mut kept = 0;
        for i in 0..self.owed_len {
            let o = self.owed[i];
            if keep(&o) {
                self.owed[kept] = o;
                kept += 1;
            }
        }
        self.owed_len = kept;
    }
}

impl Default for Subscriptions {
    fn default() -> Self {
        Self::new()
    }
}

/// What a subscription request asks for.
struct Wanted {
    prefix: [u8; PREFIX_MAX],
    prefix_len: usize,
    after: Option<u64>,
    feed_id: Option<u64>,
    table_id: Option<u32>,
}

/// The staged page's last event is out: move the window on.
fn advance(s: &mut Sub, now_ms: u64) {
    if s.closes {
        // The window is delivered: the next opens at its upper bound, at
        // once when the store holds more past it.
        let busy = s.narrowed || s.had_events;
        s.from = s.to;
        s.to = 0;
        s.cursor = 0;
        s.narrowed = false;
        s.closes = false;
        s.had_events = false;
        s.next_read_ms = if busy { now_ms } else { now_ms + POLL_MS };
    } else {
        // The window's next page.
        s.next_read_ms = now_ms;
    }
}

/// `[next_cursor:u64][count:u16]` of a versions answer.
fn page_head(body: &[u8]) -> Option<(u64, u16)> {
    let next = u64::from_le_bytes(body.get(0..8)?.try_into().ok()?);
    let count = u16::from_le_bytes([*body.get(8)?, *body.get(9)?]);
    Some((next, count))
}

/// One versions entry, borrowed from a staged page.
struct Entry<'a> {
    revision: u64,
    commit_ts: u64,
    kind: u8,
    key: &'a [u8],
    value: &'a [u8],
}

/// The entry at `at`, and where the next begins.
fn entry_at(page: &[u8], at: usize) -> Option<(Entry<'_>, usize)> {
    let fixed = page.get(at..at + 19)?;
    let revision = u64::from_le_bytes(fixed[0..8].try_into().ok()?);
    let commit_ts = u64::from_le_bytes(fixed[8..16].try_into().ok()?);
    let kind = fixed[16];
    let klen = u16::from_le_bytes([fixed[17], fixed[18]]) as usize;
    let mut p = at + 19;
    let key = page.get(p..p + klen)?;
    p += klen;
    let vlen = u32::from_le_bytes(page.get(p..p + 4)?.try_into().ok()?) as usize;
    p += 4;
    let value = page.get(p..p + vlen)?;
    p += vlen;
    Some((
        Entry {
            revision,
            commit_ts,
            kind,
            key,
            value,
        },
        p,
    ))
}

fn entry(page: &[u8], at: usize) -> Option<Entry<'_>> {
    entry_at(page, at).map(|(e, _)| e)
}

/// Stage `count` entries of a versions page into `s`, ordered by revision
/// (stable: one revision's changes keep their key order).
fn stage(s: &mut Sub, entries: &[u8], count: u16) -> Option<()> {
    let count = count as usize;
    if count > PAGE_ENTRIES || entries.len() > PAGE_MAX {
        return None;
    }
    s.page[..entries.len()].copy_from_slice(entries);
    let page = &s.page[..entries.len()];
    let mut revs = [0u64; PAGE_ENTRIES];
    let mut p = 0usize;
    for (slot, rev) in s.order.iter_mut().zip(revs.iter_mut()).take(count) {
        let (e, next) = entry_at(page, p)?;
        if e.key.len() + e.value.len() + ENTRY_FIXED > entries.len() {
            return None;
        }
        *slot = p as u16;
        *rev = e.revision;
        p = next;
    }
    // Insertion sort: stable, and a page is at most `PAGE_ENTRIES` long.
    for k in 1..count {
        let mut j = k;
        while j > 0 && revs[j - 1] > revs[j] {
            revs.swap(j - 1, j);
            s.order.swap(j - 1, j);
            j -= 1;
        }
    }
    s.count = count as u8;
    s.next = 0;
    Some(())
}

/// The exclusive upper bound of every key under `prefix`: its last byte
/// that is not `0xFF` incremented, the `0xFF` run after it dropped.
fn successor(prefix: &[u8], out: &mut [u8; PREFIX_MAX]) -> Option<usize> {
    let mut n = prefix.len();
    while n > 0 {
        if prefix[n - 1] != 0xFF {
            out[..n].copy_from_slice(&prefix[..n]);
            out[n - 1] += 1;
            return Some(n);
        }
        n -= 1;
    }
    None
}

fn put(out: &mut [u8], at: usize, bytes: &[u8]) -> Option<()> {
    out.get_mut(at..at + bytes.len())?.copy_from_slice(bytes);
    Some(())
}

fn put_text(out: &mut [u8], at: usize, bytes: &[u8]) -> Option<usize> {
    put(out, at, bytes)?;
    Some(at + bytes.len())
}

fn put_decimal(out: &mut [u8], at: usize, v: u64) -> Option<usize> {
    let mut digits = [0u8; 20];
    let mut n = 0usize;
    let mut x = v;
    loop {
        digits[n] = b'0' + (x % 10) as u8;
        x /= 10;
        n += 1;
        if x == 0 {
            break;
        }
    }
    let mut p = at;
    while n > 0 {
        n -= 1;
        *out.get_mut(p)? = digits[n];
        p += 1;
    }
    Some(p)
}

/// A decimal `u64`, digits only.
fn parse_decimal(v: &[u8]) -> Option<u64> {
    if v.is_empty() || v.len() > 20 {
        return None;
    }
    let mut x: u64 = 0;
    for &c in v {
        if !c.is_ascii_digit() {
            return None;
        }
        x = x.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
    }
    Some(x)
}

/// Lower- or upper-case hex into `out`: the byte count. `None` for an odd
/// length, a non-hex digit, or nothing.
fn decode_hex(hex: &[u8], out: &mut [u8; PREFIX_MAX]) -> Option<usize> {
    if hex.is_empty() || !hex.len().is_multiple_of(2) || hex.len() / 2 > PREFIX_MAX {
        return None;
    }
    for (k, pair) in hex.chunks(2).enumerate() {
        out[k] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(hex.len() / 2)
}

fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}
