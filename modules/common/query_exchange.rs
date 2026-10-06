//! The provider half of an exchange, for a client that performs one request
//! at a time against one backend connection.
//!
//! A database or cache client (`pg_client`, `redis_client`, `mongo_client`)
//! and the reference sink (`loopback_sink`) take requests as exchange
//! records (`target/fluxor/fluxor-abi/sdk/contracts/exchange.rs`) and answer
//! each exactly once. What differs between them is the backend; what does
//! not differ is everything here:
//!
//! - collecting each request whole (`Collector`), granting the body credit it
//!   owes and answering the refusals it raises;
//! - judging the request: the one method the module serves, and only the
//!   flags that method may carry, else 400;
//! - the order requests are served in — the order they completed;
//! - streaming the answer under the requester's response credit: a HEAD
//!   carrying the status and as much of the body as the credit allows, then
//!   BODY records as further CREDIT arrives;
//! - the queue of small records owed on `response_out` (credits, refusals,
//!   LINK), which never drops one: the module reads a request only while
//!   the queue has room for everything that read and the requests already
//!   held could owe;
//! - LINK: `DOWN` when the backend connection is lost after it was up, `UP`
//!   when it is back.
//!
//! LINK reports a change, so neither record is written for the first
//! connection. After a `DOWN`, every request held — collected, queued, or
//! being performed — is dropped unanswered (the requester re-issues each
//! exchange it holds open once it reads the `UP`), and request records are
//! read and dropped until the `UP` leaves the queue, since reading the
//! originals as well would perform them twice.
//!
//! An answer that is ready leaves before the queue's next record, so a
//! `DOWN` never overtakes an answer produced before the link dropped; a
//! `DOWN` while an answer is still streaming withdraws it, since the
//! exchange is then one the requester re-issues.
//!
//! Dual-target: `#[path]`-mounted by each module and by the host suite
//! (`tests/harness/tests/contract_query_exchange.rs`). It does no I/O: the
//! module reads `request_in`, writes `response_out` (through an
//! `ExchangeOutbox`) and talks to its backend; this decides what each record
//! means and what is owed.

#![allow(
    dead_code,
    reason = "shared provider core; each module uses the subset its backend needs"
)]

// The single mount of the SDK contract for this file. A module reaches the
// contract through this mount (`query_exchange::exchange`) so the types it
// hands back here are the same types.
#[path = "../../target/fluxor/fluxor-abi/sdk/contracts/exchange.rs"]
pub mod exchange;

use exchange::{
    flag, link, parse_request, write_body, write_credit, write_link, write_response_head,
    Collector, ExchangeId, Record, Request, ResponseHead, BODY_MAX, HDR, RECORD_MAX,
    RESP_HEAD_FIXED,
};

/// Largest header block a request may carry. No module here reads a
/// request header, so the bound is room for what a requester attaches,
/// not a field anyone uses.
pub const HEADERS_MAX: usize = 256;

/// Records owed per request record read, at most: one credit or one
/// refusal.
const OWED_PER_READ: usize = 1;
/// LINK records the queue can hold at its tail: a `DOWN` and the `UP` after
/// it. A later `DOWN` cancels a queued `UP` rather than following it.
const LINK_RESERVE: usize = 2;
/// The most body bytes a response HEAD can carry with no content type and
/// no headers.
const HEAD_BODY_MAX: usize = RECORD_MAX - HDR - RESP_HEAD_FIXED;

/// One small record owed on `response_out`.
#[derive(Clone, Copy)]
enum Owed {
    /// A whole answer with an empty body: a refusal.
    Answer {
        id: ExchangeId,
        status: u16,
    },
    Credit {
        id: ExchangeId,
        bytes: u32,
    },
    Link(u8),
}

impl Owed {
    const NONE: Owed = Owed::Link(0);

    fn exchange(&self) -> Option<ExchangeId> {
        match *self {
            Owed::Answer { id, .. } | Owed::Credit { id, .. } => Some(id),
            Owed::Link(_) => None,
        }
    }
}

/// What the requester last learned of the backend link.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LinkSeen {
    /// Never connected: nothing was ever reported.
    Unknown,
    Up,
    Down,
}

/// The request being performed against the backend.
#[derive(Clone, Copy)]
struct Current {
    live: bool,
    /// The requester aborted it: the backend's verdict is still awaited,
    /// but nothing is written.
    aborted: bool,
    id: ExchangeId,
    /// Response-body bytes the requester has granted.
    credit: u32,
}

impl Current {
    const NONE: Current = Current {
        live: false,
        aborted: false,
        id: ExchangeId::NONE,
        credit: 0,
    };
}

/// The answer being written: its body lives in the module's own buffer.
#[derive(Clone, Copy)]
struct Answer {
    live: bool,
    id: ExchangeId,
    status: u16,
    len: u32,
    sent: u32,
    credit: u32,
    head_sent: bool,
}

impl Answer {
    const NONE: Answer = Answer {
        live: false,
        id: ExchangeId::NONE,
        status: 0,
        len: 0,
        sent: 0,
        credit: 0,
        head_sent: false,
    };
}

/// A provider's exchange state: `SLOTS` requests collected at once, each
/// with a target up to `TARGET` and a body up to `BODY` bytes; `QUEUE`
/// small records owed on `response_out`.
pub struct QueryDesk<const SLOTS: usize, const TARGET: usize, const BODY: usize, const QUEUE: usize>
{
    requests: Collector<SLOTS, TARGET, HEADERS_MAX, BODY>,
    /// Completion order per slot; 0 = not a complete request.
    order: [u32; SLOTS],
    next_order: u32,
    /// The one method this provider serves, and the request flags it takes.
    method: u8,
    flags_allowed: u8,
    queue: [Owed; QUEUE],
    head: usize,
    len: usize,
    seen: LinkSeen,
    discarding: bool,
    draining: bool,
    current: Current,
    answer: Answer,
}

impl<const SLOTS: usize, const TARGET: usize, const BODY: usize, const QUEUE: usize>
    QueryDesk<SLOTS, TARGET, BODY, QUEUE>
{
    /// The queue holds a refusal for every request the slots can hold, the
    /// record one read can add, and the LINK pair — so no owed record is
    /// ever refused room.
    const QUEUE_FITS: () = assert!(QUEUE >= SLOTS + OWED_PER_READ + LINK_RESERVE);

    /// A desk serving `method`, whose requests may carry `flags_allowed`
    /// (`MORE` is always allowed: a body may follow any HEAD).
    pub const fn new(method: u8, flags_allowed: u8) -> Self {
        let () = Self::QUEUE_FITS;
        QueryDesk {
            requests: Collector::new(),
            order: [0; SLOTS],
            next_order: 1,
            method,
            flags_allowed: flags_allowed | flag::MORE,
            queue: [Owed::NONE; QUEUE],
            head: 0,
            len: 0,
            seen: LinkSeen::Unknown,
            discarding: false,
            draining: false,
            current: Current::NONE,
            answer: Answer::NONE,
        }
    }

    /// Return to the state of [`QueryDesk::new`], in place — for a module
    /// whose state arrives as raw memory.
    pub fn reset(&mut self, method: u8, flags_allowed: u8) {
        let () = Self::QUEUE_FITS;
        for at in 0..SLOTS {
            self.requests.release(at);
        }
        let _ = self.requests.take_grant();
        let _ = self.requests.take_refusal();
        self.order = [0; SLOTS];
        self.next_order = 1;
        self.method = method;
        self.flags_allowed = flags_allowed | flag::MORE;
        self.head = 0;
        self.len = 0;
        self.seen = LinkSeen::Unknown;
        self.discarding = false;
        self.draining = false;
        self.current = Current::NONE;
        self.answer = Answer::NONE;
    }

    // ── Reading requests ─────────────────────────────────────────────

    /// Requests whose body is still arriving.
    fn collecting(&self) -> bool {
        self.requests.live() > self.complete()
    }

    fn complete(&self) -> usize {
        self.order.iter().filter(|o| **o != 0).count()
    }

    /// Whether the module may read one request record: the queue has room
    /// for everything that read and the requests already held could owe,
    /// and a slot is free for a new request or one is still collecting its
    /// body. Reading stops otherwise, which is the backpressure the
    /// requester sees. While draining, reading goes on only to finish the
    /// bodies already arriving, so each of those requests is answered.
    pub fn can_take(&self) -> bool {
        let room = self.len + self.requests.live() + OWED_PER_READ + LINK_RESERVE <= QUEUE;
        let slot = if self.draining {
            self.collecting()
        } else {
            self.requests.live() < SLOTS || self.collecting()
        };
        !self.discarding && room && slot
    }

    /// Whether request records are being read and dropped: a `DOWN` is owed
    /// or written, and its `UP` has not left the queue.
    pub fn discarding(&self) -> bool {
        self.discarding
    }

    /// Take one request record. Everything it settles — a body still to
    /// come, a refusal, an abort, a credit — is settled here, with any record
    /// it owes queued. A request it completes waits for
    /// [`QueryDesk::next_request`].
    pub fn accept(&mut self, record: &[u8]) {
        if self.discarding {
            return;
        }
        if self.draining {
            if let Some(Record::Head(h)) = parse_request(record) {
                self.push(Owed::Answer {
                    id: h.id,
                    status: exchange::status::BUSY,
                });
                return;
            }
        }
        match parse_request(record) {
            Some(Record::Abort { id, .. }) => self.abort(id),
            Some(Record::Credit { id, bytes }) => {
                if self.answer.live && self.answer.id == id {
                    self.answer.credit = self.answer.credit.saturating_add(bytes);
                } else if self.current.live && self.current.id == id {
                    self.current.credit = self.current.credit.saturating_add(bytes);
                }
            }
            _ => {}
        }
        let taken = self.requests.accept(record);
        if let Some((id, bytes)) = self.requests.take_grant() {
            self.push(Owed::Credit { id, bytes });
        }
        if let Some((id, why)) = self.requests.take_refusal() {
            self.push(Owed::Answer {
                id,
                status: why.status(),
            });
        }
        let Some(at) = taken.ok().flatten() else {
            return;
        };
        let Some((id, method, flags)) =
            self.requests.request(at).map(|r| (r.id, r.method, r.flags))
        else {
            self.requests.release(at);
            return;
        };
        let status = if self.draining {
            exchange::status::BUSY
        } else if method != self.method || flags & !self.flags_allowed != 0 {
            exchange::status::BAD_REQUEST
        } else {
            exchange::status::OK
        };
        if status != exchange::status::OK {
            self.requests.release(at);
            self.push(Owed::Answer { id, status });
            return;
        }
        self.order[at] = self.next_order;
        self.next_order = self.next_order.wrapping_add(1).max(1);
    }

    /// The oldest complete request, when nothing is being performed or
    /// answered: read it with [`QueryDesk::request`], then
    /// [`QueryDesk::start`] or [`QueryDesk::refuse`] it.
    pub fn next_request(&self) -> Option<usize> {
        if self.busy() || self.draining {
            return None;
        }
        (0..SLOTS)
            .filter(|at| self.order[*at] != 0)
            .min_by_key(|at| self.order[*at].wrapping_sub(self.next_order))
    }

    /// The complete request in slot `at`.
    pub fn request(&self, at: usize) -> Option<Request<'_>> {
        if self.order.get(at).copied().unwrap_or(0) == 0 {
            return None;
        }
        self.requests.request(at)
    }

    /// The request in slot `at` goes to the backend: it is answered by
    /// [`QueryDesk::finish`]. The slot is freed — the module has copied what
    /// it performs.
    pub fn start(&mut self, at: usize) {
        let Some(r) = self.request(at) else {
            return;
        };
        self.current = Current {
            live: true,
            aborted: false,
            id: r.id,
            credit: r.resp_credit,
        };
        self.order[at] = 0;
        self.requests.release(at);
    }

    /// Answer the request in slot `at` with a refusal of the module's own
    /// (400 for a request it cannot perform, 413 for one past what its
    /// backend takes) and free the slot.
    pub fn refuse(&mut self, at: usize, status: u16) {
        if let Some(id) = self.request(at).map(|r| r.id) {
            self.push(Owed::Answer { id, status });
        }
        if at < SLOTS {
            self.order[at] = 0;
        }
        self.requests.release(at);
    }

    /// A request is being performed or its answer written.
    pub fn busy(&self) -> bool {
        self.current.live || self.answer.live
    }

    /// A request is being performed against the backend.
    pub fn performing(&self) -> bool {
        self.current.live
    }

    /// The request being performed is answered: `status`, and the first
    /// `body_len` bytes of the module's answer buffer as the body. That
    /// buffer must hold them until the answer has left
    /// ([`QueryDesk::busy`] turns false). Nothing is written for a request
    /// the requester aborted.
    pub fn finish(&mut self, status: u16, body_len: usize) {
        if !self.current.live {
            return;
        }
        let c = self.current;
        self.current = Current::NONE;
        if c.aborted {
            return;
        }
        self.answer = Answer {
            live: true,
            id: c.id,
            status,
            len: body_len as u32,
            sent: 0,
            credit: c.credit,
            head_sent: false,
        };
    }

    // ── The backend link ─────────────────────────────────────────────

    /// The backend connection is lost. When the link had been up, every
    /// request held is now unknowable: all are dropped unanswered, an
    /// answer still streaming is withdrawn, `DOWN` is owed, and request
    /// records are dropped until the `UP`. Returns false — and changes
    /// nothing — when the link never came up: the caller then answers the
    /// request it is performing itself (502), since nothing was reported.
    pub fn link_down(&mut self) -> bool {
        if self.seen != LinkSeen::Up {
            return false;
        }
        self.seen = LinkSeen::Down;
        for at in 0..SLOTS {
            self.requests.release(at);
        }
        let _ = self.requests.take_grant();
        self.order = [0; SLOTS];
        self.current = Current::NONE;
        self.answer = Answer::NONE;
        self.retain(|o| !matches!(o, Owed::Credit { .. }));
        self.discarding = true;
        // A `DOWN` after an `UP` still queued: the requester has not been
        // told the link came back, so withdrawing the `UP` tells it the
        // same thing in one record.
        if self.len > 0 && matches!(self.at(self.len - 1), Owed::Link(link::UP)) {
            self.len -= 1;
        } else {
            self.push(Owed::Link(link::DOWN));
        }
        true
    }

    /// The backend connection is (re)established and accepting. Owes `UP`
    /// after a `DOWN`; the first connection owes nothing.
    pub fn link_up(&mut self) {
        match self.seen {
            LinkSeen::Up => {}
            LinkSeen::Unknown => self.seen = LinkSeen::Up,
            LinkSeen::Down => {
                self.seen = LinkSeen::Up;
                self.push(Owed::Link(link::UP));
            }
        }
    }

    /// Whether the link has been reported down and not yet up again.
    pub fn link_is_down(&self) -> bool {
        self.seen == LinkSeen::Down
    }

    /// Whether the link has ever come up.
    pub fn link_was_up(&self) -> bool {
        self.seen != LinkSeen::Unknown
    }

    // ── Draining ─────────────────────────────────────────────────────

    /// Stop taking requests: every request held and not yet performed is
    /// answered 503 (it may be repeated elsewhere), as far as the queue has
    /// room — call once per step until [`QueryDesk::idle`]. A request whose
    /// body is still arriving is answered 503 once it completes, and a new
    /// request is answered 503 as it is read. The request being performed
    /// finishes normally.
    pub fn drain(&mut self) {
        self.draining = true;
        for at in 0..SLOTS {
            if self.len >= QUEUE - LINK_RESERVE {
                return;
            }
            if let Some(id) = self.request(at).map(|r| r.id) {
                self.push(Owed::Answer {
                    id,
                    status: exchange::status::BUSY,
                });
                self.order[at] = 0;
                self.requests.release(at);
            }
        }
    }

    /// Nothing is held, performed, answered or owed.
    pub fn idle(&self) -> bool {
        !self.busy() && self.len == 0 && self.requests.live() == 0
    }

    // ── Writing ──────────────────────────────────────────────────────

    /// Records owed on `response_out`, the streaming answer aside.
    pub fn owed(&self) -> usize {
        self.len
    }

    /// Encode the next record to write into `out`. `body` is the module's
    /// answer buffer, consulted while an answer is streaming. The answer
    /// goes first while it can progress (its HEAD at once, its body as
    /// credit allows); otherwise the oldest owed record. The caller places
    /// it (through an `ExchangeOutbox`, which holds it if the port has no
    /// room). `None` when nothing can be written now.
    pub fn next_record(&mut self, body: &[u8], out: &mut [u8]) -> Option<usize> {
        if let Some(n) = self.next_answer_record(body, out) {
            return Some(n);
        }
        if self.len == 0 {
            return None;
        }
        let owed = self.at(0);
        let n = match owed {
            Owed::Answer { id, status } => write_response_head(
                &ResponseHead {
                    id,
                    flags: 0,
                    status,
                    content_type: &[],
                    headers: &[],
                    body: &[],
                },
                out,
            )?,
            Owed::Credit { id, bytes } => write_credit(&id, bytes, out)?,
            Owed::Link(state) => write_link(state, out)?,
        };
        self.head = (self.head + 1) % QUEUE;
        self.len -= 1;
        if matches!(owed, Owed::Link(link::UP)) {
            self.discarding = false;
        }
        Some(n)
    }

    fn next_answer_record(&mut self, body: &[u8], out: &mut [u8]) -> Option<usize> {
        if !self.answer.live {
            return None;
        }
        let a = self.answer;
        let len = a.len as usize;
        let sent = a.sent as usize;
        let Some(src) = body.get(sent..len) else {
            // The module named more body than its buffer holds: the answer
            // cannot be written as stated, so it fails rather than goes out
            // short.
            self.answer = Answer::NONE;
            self.push(Owed::Answer {
                id: a.id,
                status: exchange::status::FAILED,
            });
            return None;
        };
        if !a.head_sent {
            let inline = src.len().min(a.credit as usize).min(HEAD_BODY_MAX);
            let flags = if inline < src.len() { flag::MORE } else { 0 };
            let n = write_response_head(
                &ResponseHead {
                    id: a.id,
                    flags,
                    status: a.status,
                    content_type: &[],
                    headers: &[],
                    body: &src[..inline],
                },
                out,
            )?;
            self.advance(inline);
            return Some(n);
        }
        let chunk = src.len().min(a.credit as usize).min(BODY_MAX);
        if chunk == 0 {
            return None;
        }
        let flags = if chunk < src.len() { flag::MORE } else { 0 };
        let n = write_body(&a.id, flags, &src[..chunk], out)?;
        self.advance(chunk);
        Some(n)
    }

    fn advance(&mut self, n: usize) {
        self.answer.head_sent = true;
        self.answer.sent += n as u32;
        self.answer.credit -= n as u32;
        if self.answer.sent >= self.answer.len {
            self.answer = Answer::NONE;
        }
    }

    // ── Internals ────────────────────────────────────────────────────

    /// The requester ended exchange `id`: nothing more is written for it.
    fn abort(&mut self, id: ExchangeId) {
        if self.current.live && self.current.id == id {
            self.current.aborted = true;
        }
        if self.answer.live && self.answer.id == id {
            self.answer = Answer::NONE;
        }
        for at in 0..SLOTS {
            if self.requests.request(at).is_some_and(|r| r.id == id) {
                self.order[at] = 0;
            }
        }
        self.retain(|o| o.exchange() != Some(id));
    }

    fn at(&self, i: usize) -> Owed {
        self.queue[(self.head + i) % QUEUE]
    }

    fn push(&mut self, owed: Owed) {
        // `can_take` keeps a slot free for every record a read, a held
        // request or a link change can owe, so this never finds the queue
        // full.
        if self.len < QUEUE {
            self.queue[(self.head + self.len) % QUEUE] = owed;
            self.len += 1;
        }
    }

    /// Keep the owed records `keep` accepts, in order.
    fn retain(&mut self, keep: impl Fn(&Owed) -> bool) {
        let mut kept = 0;
        for i in 0..self.len {
            let o = self.at(i);
            if keep(&o) {
                self.queue[(self.head + kept) % QUEUE] = o;
                kept += 1;
            }
        }
        self.len = kept;
    }
}
