//! route_catalogue_front — the route catalogue as an exchange provider:
//! a publisher POSTs a route generation (its policies among its records),
//! a director GETs a revision-pinned snapshot page by page, an operator
//! POSTs a garbage-collection pass; a controller POSTs a declaration (one
//! record per id, only advancing) and appends to an operation's status
//! stream, and an operator reclaims a closed stream.
//!
//! Dual-target facade: `#[path]`-mounted by
//! `modules/app/route_catalogue_front` (which owns channels, params and
//! the syscall table) and by the host suite
//! (`tests/harness/tests/contract_route_catalogue.rs`). Every side effect
//! goes through [`FrontIo`] or the records [`Front::next_record`] hands
//! back; the machines themselves are `route_catalogue.rs`'s.
//!
//! # Requests
//!
//! Targets name the service as `/rc/<tenant hex32>/<service hex32>/…`.
//!
//! Every request is a `CALL` under the `exchange_http` binding, its verb
//! the method named below; request facts and answer facts are meta fields.
//!
//! | Request | Meta | Body | Answer |
//! |---|---|---|---|
//! | `POST …/publish` | `generation` (non-zero), `predecessor` (0: none), `endpoints` (each route's endpoint count — the count its manifest will carry — comma-separated in route order, one per route `0..`, at most [`ROUTES_MAX`]; empty: no route, the withdrawn form) | one item per record, the HEAD's inline body and each BODY: `[kind u8][route u8][id 16][WHR1 envelope]`, kind 1 route `route`'s route record (id ignored), 2 an endpoint of route `route`, 3 a policy of the generation (route and id ignored: a policy is keyed by its content address); the record without MORE ends the publication (a withdrawn generation's request has none) | `200` with `generation`, `revision` (the activation's) |
//! | `POST …/declare` | `id` (hex32, the declaration's id) | the HEAD's inline body: one placed WHR1 envelope | `200` with `generation` (the record's), `revision`, `unchanged` (0/1) |
//! | `POST …/status` | `operation` (hex32), `last` (0/1: the stream's last status) | the HEAD's inline body: one placed WHR1 envelope | `200` with `seq`, `revision` (the head's) |
//! | `POST …/reclaim` | `operation` (hex32: a closed stream) | none | `200` with `deleted` |
//! | `GET …/changes` | `after` (the revision the requester holds: its snapshot's, or the last page's `through`) | none | `200` with `revision` (the store's), `through` (every revision in `(after, through]` is on the page, whole), `more` (0/1: `through` is below `revision`), `changes` (how many); the body is the page: `[revision u64][index u16][count u16][kind u8][klen u16][key][vlen u32][value]` per change (kind 0 a write, 1 a delete with no value), in revision then key order, each change numbered `index` of its revision's `count`, at most [`PAGE_MAX`] bytes and the requester's response credit |
//! | `GET …/snapshot` | `at` (0 or absent: the store's revision, which the answer pins), `continue` (the last page's token, hex) | none | `200` with `revision`, `more` (0/1), `continue` (hex, when more), `count` and `digest` (hex, on the last page); the body is the page: `[klen u16 LE][key][vlen u32 LE][value]` per record, at most [`PAGE_MAX`] bytes and the requester's response credit |
//! | `POST …/gc` | `predecessor`, `keep`, `retention-ms`, `now-ms` (0: the module's clock) | none | `200` with `reclaimed`, `deleted` |
//!
//! | Status | When |
//! |---|---|
//! | 400 | another target, op or verb, a malformed meta field or item, an envelope refused structurally, a record of the wrong kind, a continuation not of this snapshot's revision or service, a revision past the store |
//! | 409 | the generation is already begun, the active pointer is not at the predecessor named, or a racing publisher won the CAS; a declaration at or behind the generation held, or racing; a status for a closed stream, one whose number another status holds, or racing; a reclaim of an open stream |
//! | 410 | a snapshot revision the store has compacted; changes after a revision it has compacted |
//! | 413 | one revision's changes over one page; a record over 4,096 bytes or over what one routed put carries (`route_catalogue::KV_BODY_MAX`), a target or meta over their bounds, a response credit too small for one record |
//! | 422 | a referential gap: a route's endpoints or the policies read back are not those staged, a declared route without its route record (or a route record not declared), or a withdrawn generation with records |
//! | 502 | the store refused an operation, or answered what cannot be read |
//! | 503 | a request is already held (one at a time), or draining |
//!
//! Every exchange is answered exactly once. One request is held at a
//! time; a HEAD that arrives while one is held is answered `503` from a
//! slot of its own, so the held request is never disturbed. A
//! publication's items are taken one at a time, each written before the
//! next is read: the front grants request-body credit for one item
//! ([`ITEM_MAX`] bytes) each time it is ready for the next, so a slow
//! store holds the publication at the requester rather than in memory. An
//! ABORT of the held request leaves its generation staged for the next
//! garbage-collection pass; nothing is owed.

#![allow(
    dead_code,
    reason = "shared via #[path] into the front module and the host suite; each uses a subset of the surface"
)]

pub use fluxor_abi::contracts::exchange;

use super::route_catalogue::{
    each_snapshot_record, Changes, ChangesError, ChangesPage, DeclareError, DeclarePhase, Declarer,
    EnvelopeError, Gc, GcPhase, KvIo, PublishError, PublishPhase, Publisher, Reclaim, ReclaimPhase,
    Service, Snapshot, SnapshotError, SnapshotPage, StatusError, StatusLog, StatusPhase,
    DIGEST_LEN, ID_LEN, KEY_MAX, KIND_ENDPOINT, KIND_ROUTE, RECORD_MAX, ROUTES_MAX, TOKEN_MAX,
};
use exchange::{
    flag, kind, meta_field_len, meta_get, op, parse_request, record_id, record_kind, write_credit,
    write_response_head, ExchangeId, MetaWriter, Record, RequestHead, HDR, META_CONTENT_TYPE,
    RECORD_MAX as EXCHANGE_RECORD_MAX, RESP_HEAD_FIXED,
};
use fluxor_abi::contracts::exchange_http::{self as http, method, Http};

/// How the front reaches the store: one request at a time.
pub trait FrontIo {
    fn send_kv(&mut self, op: u8, body: &[u8]) -> bool;
}

/// The longest target: `/rc/` and two hex ids and the longest verb.
pub const TARGET_MAX: usize = 4 + 32 + 1 + 32 + 1 + 9;
/// Meta a request may carry.
pub const META_MAX: usize = 512;
/// One publication item: the kind, the route, the id and the largest
/// envelope. The request-body credit granted per item.
pub const ITEM_HEAD: usize = 1 + 1 + ID_LEN;
pub const ITEM_MAX: usize = ITEM_HEAD + RECORD_MAX;
const CONTENT_TYPE: &[u8] = b"application/octet-stream";
/// The response meta's room: the content type, then a snapshot page's
/// `revision`, `more` and a hex continuation, or `count` and a hex digest
/// (asserted to fit, so no field is ever dropped).
const RESP_META: usize = 512;
const _: () = assert!(
    RESP_META
        >= meta_field_len(META_CONTENT_TYPE.len(), CONTENT_TYPE.len())
            + meta_field_len(8, 20)
            + meta_field_len(4, 1)
            + meta_field_len(8, 2 * TOKEN_MAX)
);
/// A snapshot page as the front hands it back: what one response record
/// holds beside its head and the largest meta, so a full page always fits
/// its record.
pub const PAGE_MAX: usize = EXCHANGE_RECORD_MAX - HDR - RESP_HEAD_FIXED - RESP_META;
// A page holds the largest record; the page buffer also holds a HEAD's
// inline publication item while the staging marker is written.
const _: () = assert!(PAGE_MAX >= 2 + KEY_MAX + 4 + RECORD_MAX);
const _: () = assert!(PAGE_MAX >= ITEM_MAX);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    None,
    Publish,
    Snapshot,
    Changes,
    Gc,
    Declare,
    Status,
    Reclaim,
}

/// The front: one held request and the machine serving it.
pub struct Front {
    pub publisher: Publisher,
    pub snapshot: Snapshot,
    pub changes: Changes,
    pub gc: Gc,
    pub declarer: Declarer,
    pub status_log: StatusLog,
    pub reclaim: Reclaim,
    /// The answer's machine: which of them the `200` owed reports.
    answered: Kind,
    kind: Kind,
    id: ExchangeId,
    service: Service,
    /// Publication: the routes declared and each one's endpoint count,
    /// whether the request's last item was read, and whether the seal was
    /// asked.
    routes: usize,
    declared: [u32; ROUTES_MAX],
    request_ended: bool,
    sealed: bool,
    /// Publication: the HEAD's inline item, held in `page` until the
    /// staging marker stands (`held` bytes, 0: none), and whether it ends
    /// the request.
    held: usize,
    held_ends: bool,
    /// Publication: request-body bytes granted and not yet received.
    credit: u32,
    /// Snapshot: the requester's response credit, the page gathered, and
    /// its outcome.
    resp_credit: u32,
    page: [u8; PAGE_MAX],
    page_len: usize,
    page_outcome: Option<SnapshotPage>,
    changes_outcome: Option<ChangesPage>,
    token: [u8; TOKEN_MAX],
    /// The answer owed to the held request: a status, or none yet.
    owed: Option<u16>,
    /// A `503` owed to a request refused while another was held.
    busy: Option<ExchangeId>,
    draining: bool,
    /// Which request the store's answers belong to: advanced when a request
    /// is taken and when one is aborted. A store request sent under one
    /// epoch is answered to the machine only while the epoch stands; an
    /// answer to an abandoned request is not this request's (see
    /// [`Front::epoch`]).
    epoch: u32,
    /// Counted (telemetry): publications activated, refused; snapshot
    /// pages served; gc passes.
    pub m_activated: u64,
    pub m_refused: u64,
    pub m_pages: u64,
    pub m_gc: u64,
}

impl Default for Front {
    fn default() -> Self {
        Self::new()
    }
}

/// `KvIo` over `FrontIo`.
struct Io<'a, F: FrontIo>(&'a mut F);

impl<F: FrontIo> KvIo for Io<'_, F> {
    fn request(&mut self, op: u8, body: &[u8]) -> bool {
        self.0.send_kv(op, body)
    }
}

/// Stage one publication item: `[kind u8][route u8][id 16][envelope]`.
fn stage_item(p: &mut Publisher, data: &[u8]) -> Result<(), PublishError> {
    if data.len() <= ITEM_HEAD {
        return Err(PublishError::Kind);
    }
    let (route, record) = (data[1], &data[ITEM_HEAD..]);
    match data[0] {
        1 => p.stage_route(route, record),
        2 => {
            let mut id = [0u8; ID_LEN];
            id.copy_from_slice(&data[2..ITEM_HEAD]);
            p.stage_endpoint(route, &id, record)
        }
        3 => p.stage_policy(record).map(|_| ()),
        _ => Err(PublishError::Kind),
    }
}

/// The `endpoints` field: each route's count, comma-separated in route
/// order (empty: no route). `None` for a malformed list or one past
/// [`ROUTES_MAX`] routes.
fn field_counts(meta: &[u8], out: &mut [u32; ROUTES_MAX]) -> Option<usize> {
    let v = meta_get(meta, b"endpoints")?;
    if v.is_empty() {
        return Some(0);
    }
    let mut n = 0;
    for part in v.split(|&c| c == b',') {
        if n == ROUTES_MAX || part.is_empty() || part.len() > 10 {
            return None;
        }
        let mut x: u64 = 0;
        for &c in part {
            if !c.is_ascii_digit() {
                return None;
            }
            x = x * 10 + u64::from(c - b'0');
        }
        out[n] = u32::try_from(x).ok()?;
        n += 1;
    }
    Some(n)
}

impl Front {
    pub const fn new() -> Self {
        Front {
            publisher: Publisher::new(),
            snapshot: Snapshot::new(),
            changes: Changes::new(),
            gc: Gc::new(),
            declarer: Declarer::new(),
            status_log: StatusLog::new(),
            reclaim: Reclaim::new(),
            answered: Kind::None,
            kind: Kind::None,
            id: ExchangeId::NONE,
            service: Service {
                tenant: [0; ID_LEN],
                service: [0; ID_LEN],
            },
            routes: 0,
            declared: [0; ROUTES_MAX],
            request_ended: false,
            sealed: false,
            held: 0,
            held_ends: false,
            credit: 0,
            resp_credit: 0,
            page: [0; PAGE_MAX],
            page_len: 0,
            page_outcome: None,
            changes_outcome: None,
            token: [0; TOKEN_MAX],
            owed: None,
            busy: None,
            draining: false,
            epoch: 0,
            m_activated: 0,
            m_refused: 0,
            m_pages: 0,
            m_gc: 0,
        }
    }

    pub fn reset(&mut self) {
        *self = Front::new();
    }

    /// Whether a request record may be read now: nothing owed, and no
    /// request held or a publication ready for its next item.
    pub fn can_take(&self) -> bool {
        if self.owed.is_some() || self.busy.is_some() {
            return false;
        }
        match self.kind {
            Kind::None => !self.draining,
            Kind::Publish => self.wants_item(),
            _ => false,
        }
    }

    /// A publication ready for its next item.
    fn wants_item(&self) -> bool {
        self.kind == Kind::Publish
            && !self.request_ended
            && self.held == 0
            && self.owed.is_none()
            && self.publisher.ready()
    }

    /// Nothing held, nothing owed.
    pub fn idle(&self) -> bool {
        self.kind == Kind::None && self.owed.is_none() && self.busy.is_none()
    }

    pub fn drain(&mut self) {
        self.draining = true;
    }

    /// The request the store's answers are for now: a store request sent
    /// under an earlier epoch was sent for a request that has since ended
    /// (aborted, or another taken), and its answer must be dropped, never
    /// handed to the machine serving this one.
    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    /// One request record from the channel.
    pub fn accept(&mut self, record: &[u8], now_ms: u64) {
        let rec = match parse_request::<Http>(record) {
            Ok(rec) => rec,
            Err(_) => {
                // A HEAD that breaks the binding's rules still names its
                // exchange, and is owed an answer.
                if let (Some(kind::HEAD), Some(id)) = (record_kind(record), record_id(record)) {
                    self.refuse_head(id, http::status::BAD_REQUEST);
                }
                return;
            }
        };
        match rec {
            Record::Head(h) => self.head(&h, now_ms),
            Record::Body { id, flags, data } => {
                if self.kind == Kind::Publish && id == self.id && self.owed.is_none() {
                    let n = u32::try_from(data.len()).unwrap_or(u32::MAX);
                    self.credit = self.credit.saturating_sub(n);
                    self.item(data, flags & flag::MORE == 0);
                }
            }
            Record::Abort { id, .. } => {
                if id == self.id && self.kind != Kind::None {
                    // The requester gave up: the generation stays staged
                    // (gc reclaims it); nothing is owed, and a store answer
                    // still to come is not the next request's.
                    self.epoch = self.epoch.wrapping_add(1);
                    self.kind = Kind::None;
                    self.owed = None;
                    self.held = 0;
                    self.publisher = Publisher::new();
                    self.gc = Gc::new();
                    self.declarer = Declarer::new();
                    self.status_log = StatusLog::new();
                    self.reclaim = Reclaim::new();
                }
            }
            _ => {}
        }
    }

    fn refuse(&mut self, id: ExchangeId, st: u16) {
        self.id = id;
        self.kind = Kind::None;
        self.held = 0;
        self.owed = Some(st);
        self.m_refused = self.m_refused.wrapping_add(1);
    }

    /// Whether a new request must be refused from its own slot: one is
    /// held, an answer is owed, or the front drains.
    fn occupied(&self) -> bool {
        self.kind != Kind::None || self.owed.is_some() || self.busy.is_some() || self.draining
    }

    /// Refuse a HEAD that could not be read: from the busy slot while a
    /// request is held, else as the answer owed.
    fn refuse_head(&mut self, id: ExchangeId, st: u16) {
        if self.occupied() {
            if self.busy.is_none() {
                self.busy = Some(id);
                self.m_refused = self.m_refused.wrapping_add(1);
            }
            return;
        }
        self.refuse(id, st);
    }

    fn head(&mut self, h: &RequestHead<'_>, now_ms: u64) {
        if self.occupied() {
            // Refused from its own slot: the held request and the answer
            // owed to it stay as they are.
            if self.busy.is_none() {
                self.busy = Some(h.id);
                self.m_refused = self.m_refused.wrapping_add(1);
            }
            return;
        }
        if h.target.len() > TARGET_MAX || h.meta.len() > META_MAX {
            return self.refuse(h.id, status::PAYLOAD_TOO_LARGE);
        }
        let Some((service, verb)) = parse_target(h.target) else {
            return self.refuse(h.id, status::BAD_REQUEST);
        };
        self.service = service;
        self.id = h.id;
        self.epoch = self.epoch.wrapping_add(1);
        if h.op != op::CALL {
            return self.refuse(h.id, status::BAD_REQUEST);
        }
        match (h.verb, verb) {
            (method::POST, b"publish") => {
                let mut declared = [0u32; ROUTES_MAX];
                let (Some(generation), Some(predecessor), Some(routes)) = (
                    field_u64(h.meta, b"generation"),
                    field_u64(h.meta, b"predecessor"),
                    field_counts(h.meta, &mut declared),
                ) else {
                    return self.refuse(h.id, status::BAD_REQUEST);
                };
                if generation == 0 {
                    return self.refuse(h.id, status::BAD_REQUEST);
                }
                if h.body.len() > ITEM_MAX {
                    return self.refuse(h.id, status::PAYLOAD_TOO_LARGE);
                }
                let mut publisher = [0u8; 16];
                publisher[..h.peer.len().min(16)].copy_from_slice(&h.peer[..h.peer.len().min(16)]);
                if !self
                    .publisher
                    .begin(service, generation, predecessor, now_ms, publisher)
                {
                    return self.refuse(h.id, status::SERVICE_UNAVAILABLE);
                }
                self.kind = Kind::Publish;
                self.routes = routes;
                self.declared = declared;
                self.request_ended = false;
                self.sealed = false;
                self.credit = 0;
                // An inline first item is staged once the marker stands; an
                // item-less request without MORE ends here.
                let ends = h.flags & flag::MORE == 0;
                if !h.body.is_empty() {
                    self.page[..h.body.len()].copy_from_slice(h.body);
                    self.held = h.body.len();
                    self.held_ends = ends;
                } else if ends {
                    self.request_ended = true;
                }
            }
            (method::GET, b"snapshot") => {
                let at = match meta_get(h.meta, b"at") {
                    Some(_) => match field_u64(h.meta, b"at") {
                        Some(at) => at,
                        None => return self.refuse(h.id, status::BAD_REQUEST),
                    },
                    None => 0,
                };
                let mut token = [0u8; TOKEN_MAX];
                let tn = match meta_get(h.meta, b"continue") {
                    Some(hex) => match unhex(hex, &mut token) {
                        Some(n) => n,
                        None => return self.refuse(h.id, status::BAD_REQUEST),
                    },
                    None => 0,
                };
                match self.snapshot.begin(service, at, &token[..tn]) {
                    Ok(()) => {
                        self.kind = Kind::Snapshot;
                        self.resp_credit = h.resp_credit;
                        self.page_len = 0;
                        self.page_outcome = None;
                    }
                    Err(_) => self.refuse(h.id, status::BAD_REQUEST),
                }
            }
            (method::GET, b"changes") => {
                let Some(after) = field_u64(h.meta, b"after") else {
                    return self.refuse(h.id, status::BAD_REQUEST);
                };
                match self.changes.begin(service, after) {
                    Ok(()) => {
                        self.kind = Kind::Changes;
                        self.resp_credit = h.resp_credit;
                        self.page_len = 0;
                        self.changes_outcome = None;
                    }
                    Err(_) => self.refuse(h.id, status::BAD_REQUEST),
                }
            }
            (method::POST, b"gc") => {
                let (Some(predecessor), Some(keep), Some(retention), Some(now)) = (
                    field_u64(h.meta, b"predecessor"),
                    field_u64(h.meta, b"keep"),
                    field_u64(h.meta, b"retention-ms"),
                    field_u64(h.meta, b"now-ms"),
                ) else {
                    return self.refuse(h.id, status::BAD_REQUEST);
                };
                let now = if now == 0 { now_ms } else { now };
                if !self.gc.begin(service, predecessor, keep, retention, now) {
                    return self.refuse(h.id, status::SERVICE_UNAVAILABLE);
                }
                self.kind = Kind::Gc;
            }
            (method::POST, b"declare") => {
                let Some(id) = field_id(h.meta, b"id") else {
                    return self.refuse(h.id, status::BAD_REQUEST);
                };
                match self.declarer.begin(service, id, h.body) {
                    Ok(true) => self.kind = Kind::Declare,
                    Ok(false) => self.refuse(h.id, status::SERVICE_UNAVAILABLE),
                    Err(e) => self.refuse(h.id, declare_status(e)),
                }
            }
            (method::POST, b"status") => {
                let (Some(operation), Some(last)) =
                    (field_id(h.meta, b"operation"), field_u64(h.meta, b"last"))
                else {
                    return self.refuse(h.id, status::BAD_REQUEST);
                };
                if last > 1 {
                    return self.refuse(h.id, status::BAD_REQUEST);
                }
                match self.status_log.begin(service, operation, h.body, last == 1) {
                    Ok(true) => self.kind = Kind::Status,
                    Ok(false) => self.refuse(h.id, status::SERVICE_UNAVAILABLE),
                    Err(e) => self.refuse(h.id, status_status(e)),
                }
            }
            (method::POST, b"reclaim") => {
                let Some(operation) = field_id(h.meta, b"operation") else {
                    return self.refuse(h.id, status::BAD_REQUEST);
                };
                if !self.reclaim.begin(service, operation) {
                    return self.refuse(h.id, status::SERVICE_UNAVAILABLE);
                }
                self.kind = Kind::Reclaim;
            }
            _ => self.refuse(h.id, status::BAD_REQUEST),
        }
    }

    /// A publication item from a BODY record.
    fn item(&mut self, data: &[u8], ends: bool) {
        if data.is_empty() {
            // An empty final BODY ends the request.
            if ends {
                self.request_ended = true;
            }
            return;
        }
        match stage_item(&mut self.publisher, data) {
            Ok(()) => {
                if ends {
                    self.request_ended = true;
                }
            }
            Err(e) => {
                let id = self.id;
                self.refuse(id, publish_status(e));
            }
        }
    }

    /// Drive the held machine.
    pub fn step(&mut self, io: &mut impl FrontIo, _now_ms: u64) {
        let mut kv = Io(io);
        match self.kind {
            Kind::Publish => {
                if self.held > 0 && self.publisher.ready() {
                    let n = self.held;
                    self.held = 0;
                    match stage_item(&mut self.publisher, &self.page[..n]) {
                        Ok(()) => self.request_ended = self.held_ends,
                        Err(e) => {
                            let id = self.id;
                            return self.refuse(id, publish_status(e));
                        }
                    }
                }
                if self.request_ended && !self.sealed && self.publisher.ready() {
                    let n = self.routes;
                    match self.publisher.seal(&self.declared[..n]) {
                        Ok(()) => self.sealed = true,
                        Err(e) => {
                            let id = self.id;
                            return self.refuse(id, publish_status(e));
                        }
                    }
                }
                self.publisher.step(&mut kv);
                if self.publisher.done() {
                    let st = match self.publisher.error {
                        None => {
                            self.m_activated = self.m_activated.wrapping_add(1);
                            status::OK
                        }
                        Some(e) => {
                            self.m_refused = self.m_refused.wrapping_add(1);
                            publish_status(e)
                        }
                    };
                    self.owed = Some(st);
                    self.kind = Kind::None;
                }
            }
            Kind::Snapshot => self.snapshot.step(&mut kv),
            Kind::Changes => self.changes.step(&mut kv),
            Kind::Declare => {
                self.declarer.step(&mut kv);
                if self.declarer.done() {
                    let st = match self.declarer.error {
                        None => status::OK,
                        Some(e) => declare_status(e),
                    };
                    self.settle(Kind::Declare, st);
                }
            }
            Kind::Status => {
                self.status_log.step(&mut kv);
                if self.status_log.done() {
                    let st = match self.status_log.error {
                        None => status::OK,
                        Some(e) => status_status(e),
                    };
                    self.settle(Kind::Status, st);
                }
            }
            Kind::Reclaim => {
                self.reclaim.step(&mut kv);
                if self.reclaim.done() {
                    let st = match self.reclaim.phase {
                        ReclaimPhase::Done => status::OK,
                        _ if self.reclaim.error == 0xFE => status::CONFLICT,
                        _ => status::BAD_GATEWAY,
                    };
                    self.settle(Kind::Reclaim, st);
                }
            }
            Kind::Gc => {
                self.gc.step(&mut kv);
                if self.gc.done() {
                    self.m_gc = self.m_gc.wrapping_add(1);
                    self.owed = Some(if self.gc.phase == GcPhase::Done {
                        status::OK
                    } else {
                        self.m_refused = self.m_refused.wrapping_add(1);
                        status::BAD_GATEWAY
                    });
                    self.kind = Kind::None;
                }
            }
            Kind::None => {}
        }
    }

    /// A machine done: the answer it owes, counted.
    fn settle(&mut self, kind: Kind, st: u16) {
        if st != status::OK {
            self.m_refused = self.m_refused.wrapping_add(1);
        }
        self.answered = kind;
        self.owed = Some(st);
        self.kind = Kind::None;
    }

    /// The store's answer to the request in flight.
    pub fn on_kv_response(&mut self, result: u8, revision: u64, body: &[u8]) {
        match self.kind {
            Kind::Publish => self.publisher.on_response(result, revision, body),
            Kind::Gc => self.gc.on_response(result, body),
            Kind::Declare => self.declarer.on_response(result, revision, body),
            Kind::Status => self.status_log.on_response(result, revision, body),
            Kind::Reclaim => self.reclaim.on_response(result, body),
            Kind::Changes => {
                let cap = PAGE_MAX.min(self.resp_credit as usize);
                match self
                    .changes
                    .on_response(result, revision, body, &mut self.page[..cap])
                {
                    // The window was narrowed: asked again on the next step.
                    Ok(None) => {}
                    Ok(Some(p)) => {
                        self.page_len = p.written;
                        self.changes_outcome = Some(p);
                        self.m_pages = self.m_pages.wrapping_add(1);
                        self.owed = Some(status::OK);
                        self.kind = Kind::None;
                    }
                    Err(e) => {
                        let id = self.id;
                        self.refuse(
                            id,
                            match e {
                                ChangesError::Compacted => status::GONE,
                                ChangesError::Future => status::BAD_REQUEST,
                                ChangesError::TooLarge => status::PAYLOAD_TOO_LARGE,
                                ChangesError::Store(_) | ChangesError::Answer => {
                                    status::BAD_GATEWAY
                                }
                            },
                        );
                    }
                }
            }
            Kind::Snapshot => {
                // The page holds what the requester's response credit allows.
                let cap = PAGE_MAX.min(self.resp_credit as usize);
                let r = self.snapshot.on_response(
                    result,
                    revision,
                    body,
                    &mut self.page[..cap],
                    &mut self.token,
                );
                match r {
                    Ok((n, outcome)) => {
                        self.page_len = n;
                        self.page_outcome = Some(outcome);
                        self.m_pages = self.m_pages.wrapping_add(1);
                        self.owed = Some(status::OK);
                    }
                    Err(e) => {
                        let id = self.id;
                        self.refuse(
                            id,
                            match e {
                                SnapshotError::Compacted => status::GONE,
                                SnapshotError::Future | SnapshotError::Token => status::BAD_REQUEST,
                                SnapshotError::PageTooSmall => status::PAYLOAD_TOO_LARGE,
                                SnapshotError::Store(_) | SnapshotError::Answer => {
                                    status::BAD_GATEWAY
                                }
                            },
                        );
                    }
                }
                self.kind = Kind::None;
            }
            Kind::None => {}
        }
    }

    /// The record owed, if any, into `out`: its length. A `503` to a
    /// request refused while another was held first, then the held
    /// request's answer, else a publication's request-body credit for its
    /// next item. A record that does not fit `out` stays owed.
    pub fn next_record(&mut self, out: &mut [u8]) -> Option<usize> {
        if let Some(id) = self.busy {
            let mut meta = [0u8; RESP_META];
            let mut w = MetaWriter::new(&mut meta);
            w.put(META_CONTENT_TYPE, CONTENT_TYPE);
            let ml = w.finish()?;
            let mut head = http::head(id, status::SERVICE_UNAVAILABLE, &meta[..ml], &[])?;
            head.flags = flag::RAISED;
            let n = write_response_head::<Http>(&head, out)?;
            self.busy = None;
            return Some(n);
        }
        let Some(st) = self.owed else {
            let want = ITEM_MAX as u32;
            if self.wants_item() && self.credit < want {
                let n = write_credit(&self.id, want - self.credit, out)?;
                self.credit = want;
                return Some(n);
            }
            return None;
        };
        let mut meta = [0u8; RESP_META];
        let mut w = MetaWriter::new(&mut meta);
        w.put(META_CONTENT_TYPE, CONTENT_TYPE);
        let body: &[u8] = if st == status::OK {
            if let Some(c) = self.changes_outcome {
                put_u64(&mut w, b"revision", c.revision);
                put_u64(&mut w, b"through", c.through);
                put_u64(&mut w, b"more", u64::from(c.through < c.revision));
                put_u64(&mut w, b"changes", u64::from(c.count));
                &self.page[..self.page_len]
            } else if let Some(outcome) = self.page_outcome {
                put_u64(&mut w, b"revision", self.snapshot.at_rev);
                match outcome {
                    SnapshotPage::More { token_len } => {
                        put_u64(&mut w, b"more", 1);
                        put_hex(&mut w, b"continue", &self.token[..token_len]);
                    }
                    SnapshotPage::Last { count, digest } => {
                        put_u64(&mut w, b"more", 0);
                        put_u64(&mut w, b"count", u64::from(count));
                        put_hex(&mut w, b"digest", &digest);
                    }
                }
                &self.page[..self.page_len]
            } else if self.answered == Kind::Declare
                && self.declarer.phase == DeclarePhase::Declared
            {
                put_u64(&mut w, b"generation", self.declarer.generation);
                put_u64(&mut w, b"revision", self.declarer.revision);
                put_u64(&mut w, b"unchanged", u64::from(self.declarer.unchanged));
                &[]
            } else if self.answered == Kind::Status
                && self.status_log.phase == StatusPhase::Appended
            {
                put_u64(&mut w, b"seq", self.status_log.seq);
                put_u64(&mut w, b"revision", self.status_log.revision);
                &[]
            } else if self.answered == Kind::Reclaim {
                put_u64(&mut w, b"deleted", u64::from(self.reclaim.deleted));
                &[]
            } else if self.publisher.phase == PublishPhase::Activated {
                put_u64(&mut w, b"generation", self.publisher.generation);
                put_u64(&mut w, b"revision", self.publisher.revision);
                &[]
            } else {
                put_u64(&mut w, b"reclaimed", u64::from(self.gc.reclaimed));
                put_u64(&mut w, b"deleted", u64::from(self.gc.deleted));
                &[]
            }
        } else {
            &[]
        };
        let ml = w.finish()?;
        let mut head = http::head(self.id, st, &meta[..ml], body)?;
        head.flags = if st == status::OK { 0 } else { flag::RAISED };
        let n = write_response_head::<Http>(&head, out)?;
        self.owed = None;
        // The machines are reset for the next request.
        self.publisher = Publisher::new();
        self.gc = Gc::new();
        self.declarer = Declarer::new();
        self.status_log = StatusLog::new();
        self.reclaim = Reclaim::new();
        self.answered = Kind::None;
        self.page_len = 0;
        self.page_outcome = None;
        self.changes_outcome = None;
        Some(n)
    }
}

/// The status a publication error answers.
fn publish_status(e: PublishError) -> u16 {
    match e {
        PublishError::Envelope(super::route_catalogue::EnvelopeError::TooLarge(_)) => {
            status::PAYLOAD_TOO_LARGE
        }
        PublishError::Envelope(_) | PublishError::Kind | PublishError::Sequence => {
            status::BAD_REQUEST
        }
        PublishError::Endpoints { .. } | PublishError::Policies { .. } | PublishError::NoRoute => {
            status::UNPROCESSABLE
        }
        PublishError::Exists | PublishError::Predecessor { .. } | PublishError::Raced => {
            status::CONFLICT
        }
        PublishError::Store(_) | PublishError::Answer => status::BAD_GATEWAY,
    }
}

/// The status a declaration error answers.
fn declare_status(e: DeclareError) -> u16 {
    match e {
        DeclareError::Envelope(EnvelopeError::TooLarge(_)) => status::PAYLOAD_TOO_LARGE,
        DeclareError::Envelope(_) => status::BAD_REQUEST,
        DeclareError::Stale { .. } | DeclareError::Conflict | DeclareError::Raced => {
            status::CONFLICT
        }
        DeclareError::Store(_) | DeclareError::Answer => status::BAD_GATEWAY,
    }
}

/// The status a status-append error answers.
fn status_status(e: StatusError) -> u16 {
    match e {
        StatusError::Envelope(EnvelopeError::TooLarge(_)) => status::PAYLOAD_TOO_LARGE,
        StatusError::Envelope(_) => status::BAD_REQUEST,
        StatusError::Closed | StatusError::Conflict | StatusError::Raced => status::CONFLICT,
        StatusError::Head | StatusError::Store(_) | StatusError::Answer => status::BAD_GATEWAY,
    }
}

/// A 32-digit hex identity field.
fn field_id(meta: &[u8], name: &[u8]) -> Option<[u8; ID_LEN]> {
    let v = meta_get(meta, name)?;
    let mut id = [0u8; ID_LEN];
    if v.len() != 2 * ID_LEN || unhex(v, &mut id)? != ID_LEN {
        return None;
    }
    Some(id)
}

/// Statuses the HTTP binding does not name.
pub mod extra_status {
    pub const GONE: u16 = 410;
    pub const UNPROCESSABLE: u16 = 422;
}

mod status {
    pub use super::extra_status::*;
    pub use fluxor_abi::contracts::exchange_http::status::*;
}

/// `/rc/<tenant hex32>/<service hex32>/<verb>`.
#[allow(
    clippy::manual_contains,
    reason = "slice::contains on bytes lowers to core's memchr, which the PIC build does not link"
)]
fn parse_target(t: &[u8]) -> Option<(Service, &[u8])> {
    let rest = t.strip_prefix(b"/rc/")?;
    let mut s = Service {
        tenant: [0; ID_LEN],
        service: [0; ID_LEN],
    };
    if unhex(rest.get(..32)?, &mut s.tenant)? != ID_LEN || *rest.get(32)? != b'/' {
        return None;
    }
    let rest = rest.get(33..)?;
    if unhex(rest.get(..32)?, &mut s.service)? != ID_LEN || *rest.get(32)? != b'/' {
        return None;
    }
    let verb = rest.get(33..)?;
    if verb.is_empty() || verb.iter().any(|&c| c == b'/') {
        return None;
    }
    Some((s, verb))
}

/// A decimal field.
fn field_u64(meta: &[u8], name: &[u8]) -> Option<u64> {
    let v = meta_get(meta, name)?;
    if v.is_empty() || v.len() > 20 {
        return None;
    }
    let mut n: u64 = 0;
    for &c in v {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
    }
    Some(n)
}

/// Hex into `out`: the bytes decoded, or `None` for odd or non-hex input
/// or one that does not fit.
pub fn unhex(hex: &[u8], out: &mut [u8]) -> Option<usize> {
    if hex.len() & 1 != 0 || hex.len() / 2 > out.len() {
        return None;
    }
    fn nib(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }
    for (i, pair) in hex.chunks(2).enumerate() {
        out[i] = (nib(pair[0])? << 4) | nib(pair[1])?;
    }
    Some(hex.len() / 2)
}

/// A decimal meta field.
fn put_u64(w: &mut MetaWriter<'_>, name: &[u8], value: u64) {
    let mut digits = [0u8; 20];
    let mut n = 0;
    let mut v = value;
    loop {
        digits[n] = b'0' + (v % 10) as u8;
        n += 1;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    digits[..n].reverse();
    w.put(name, &digits[..n]);
}

/// A lower-case hex meta field.
fn put_hex(w: &mut MetaWriter<'_>, name: &[u8], bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    w.begin(name);
    for &b in bytes {
        w.push(&[HEX[usize::from(b >> 4)], HEX[usize::from(b & 15)]]);
    }
    w.end();
}

/// Re-export for consumers that hold a page.
pub fn each_page_record(page: &[u8], f: impl FnMut(&[u8], &[u8])) -> bool {
    each_snapshot_record(page, f)
}

const _: () = assert!(KIND_ROUTE == 1 && KIND_ENDPOINT == 2);
const _: () = assert!(DIGEST_LEN == 32);
