//! JSON document wire — the channel contract between a JSON document
//! client and lattice's JSON dialect (`doc_json_dialect`), which runs the
//! operation on the document executor and renders the answer.
//!
//! The client is a record adapter that owns its own format (Chronicle's
//! `store_effect` / `store_source`): it states a canonical document
//! operation with its documents as JSON text, and names how the answer
//! is read — through which view, as which table. Everything that makes a
//! collection behave as Kubernetes objects do (stamping, guards, field
//! ownership, keyed merge, rendering) is the dialect's `schema` param,
//! never this wire and never the client.
//!
//! Mounted beside `doc_exec.rs` (reached as `super::doc_exec`): the head
//! and field framing are `MSG_DOC_REQUEST`'s and `MSG_DOC_RESPONSE`'s,
//! written with its `FieldWriter` and read with its `Fields`. The
//! operation codes, flags, outcomes and response flags are its too.
//!
//! ## `MSG_JSON_DOC_REQUEST` (0xF2)
//!
//! ```text
//! [corr:u64][dialect:u8][tenant:u32][conn:u8][op:u8][flags:u8]
//! {[field:u8][len:u32][body]}…
//! ```
//!
//! | Field | Tag | Body |
//! |---|---|---|
//! | `J_COLLECTION` | 1 | The collection's name |
//! | `J_ID` | 3 | JSON: the id — a string, or an array for a compound id (a create's is set on the object; one naming another is refused) |
//! | `J_DOC` | 4 | JSON: the object (create, replace, apply) |
//! | `J_PATCH` | 8 | JSON: a merge patch, a JSON Patch, or a keyed-merge patch |
//! | `J_MANAGER` | 9 | The writer's name (field ownership) |
//! | `J_IF_REVISION` | 10 | `u64`: write only at this revision |
//! | `J_LIMIT` | 11 | `u32`: at most this many objects (list) |
//! | `J_CONTINUE` | 12 | A list's continuation, as an answer gave it |
//! | `J_AT_REVISION` | 13 | `u64`: a watch's changes after this revision |
//! | `J_SCOPE_NAME` | 20 | The subresource a write is recorded under |
//! | `J_ID_PREFIX` | 21 | JSON array: the leading id components (a namespace; a create's are set on the object, as `J_ID`'s) |
//! | `J_VIEW` | 32 | The view the object is read and written through |
//! | `J_TABLE` | 33 | Answer as a table of this kind |
//! | `J_LIST_HEAD` | 34 | A list's opening text (its envelope up to the items) |
//! | `J_KIND` | 35 | The collection's kind, as the client names it (a watch's bookmark) |
//! | `J_API_VERSION` | 36 | Its API version, likewise |
//!
//! Operations: `OP_INSERT` (create), `OP_REPLACE`, `OP_PATCH_MERGE`,
//! `OP_PATCH_JSON`, `OP_PATCH_KEYED`, `OP_APPLY`, `OP_DELETE`, `OP_GET`,
//! `OP_LIST`, `OP_WATCH`. Flags: `FLAG_DRY_RUN`, `FLAG_FORCE`,
//! [`J_FLAG_BOOKMARK`] (a watch with no change answers a bookmark),
//! [`J_FLAG_FOLLOW`] (the watch stays open: every later change is answered
//! on its `corr`, each answer with `RFLAG_MORE`, and bookmarks when asked
//! for) and [`J_FLAG_CANCEL`] (end the followed watch this `corr` and
//! `conn` name).
//!
//! ## `MSG_JSON_DOC_RESPONSE` (0xF3)
//!
//! ```text
//! [corr:u64][conn:u8][outcome:u8][flags:u8][revision:u64]
//! {[field:u8][len:u32][body]}…
//! ```
//!
//! | Field | Tag | Body |
//! |---|---|---|
//! | `JR_TEXT` | 1 | JSON text: the object, a list page (the request's head, the items, the schema's `list.tail`) or a table, or a watch's event lines |
//! | `JR_CONTINUE` | 3 | A list's continuation, as text (with `RFLAG_MORE`) |
//! | `JR_ERROR` | 4 | Why the operation was refused, as the schema words it |
//!
//! A list pages by `J_CONTINUE` and a watch resumes by `J_AT_REVISION`; a
//! followed watch is the one thing the dialect holds between requests. A
//! request or answer larger than one frame crosses as `wire::MSG_PART`
//! parts.

use super::doc_exec::{
    Fields, DOC_REQUEST_HEAD, FIELD_HEAD, FLAG_DRY_RUN, FLAG_FORCE, OP_APPLY, OP_DELETE, OP_GET,
    OP_INSERT, OP_LIST, OP_PATCH_JSON, OP_PATCH_KEYED, OP_PATCH_MERGE, OP_REPLACE, OP_WATCH,
};

pub const J_COLLECTION: u8 = 1;
pub const J_ID: u8 = 3;
pub const J_DOC: u8 = 4;
pub const J_PATCH: u8 = 8;
pub const J_MANAGER: u8 = 9;
pub const J_IF_REVISION: u8 = 10;
pub const J_LIMIT: u8 = 11;
pub const J_CONTINUE: u8 = 12;
pub const J_AT_REVISION: u8 = 13;
pub const J_SCOPE_NAME: u8 = 20;
pub const J_ID_PREFIX: u8 = 21;
pub const J_VIEW: u8 = 32;
pub const J_TABLE: u8 = 33;
pub const J_LIST_HEAD: u8 = 34;
pub const J_KIND: u8 = 35;
pub const J_API_VERSION: u8 = 36;

pub const JR_TEXT: u8 = 1;
pub const JR_CONTINUE: u8 = 3;
pub const JR_ERROR: u8 = 4;

/// A watch that finds no change answers the schema's bookmark.
pub const J_FLAG_BOOKMARK: u8 = 0x40;
/// A watch that stays open (see the module docs).
pub const J_FLAG_FOLLOW: u8 = 0x20;
/// End the followed watch this request's `corr` and `conn` name.
pub const J_FLAG_CANCEL: u8 = 0x80;
const J_FLAGS_KNOWN: u8 =
    FLAG_DRY_RUN | FLAG_FORCE | J_FLAG_BOOKMARK | J_FLAG_FOLLOW | J_FLAG_CANCEL;

/// The head of both messages is the canonical wire's.
pub const JSON_DOC_REQUEST_HEAD: usize = DOC_REQUEST_HEAD;

/// Is `op` one this wire carries?
pub fn op_is_known(op: u8) -> bool {
    matches!(
        op,
        OP_INSERT
            | OP_REPLACE
            | OP_PATCH_MERGE
            | OP_PATCH_JSON
            | OP_PATCH_KEYED
            | OP_APPLY
            | OP_DELETE
            | OP_GET
            | OP_LIST
            | OP_WATCH
    )
}

/// The bit a field's tag holds in a seen-set (tags are at most 63).
fn tag_bit(tag: u8) -> Option<u64> {
    match tag {
        J_COLLECTION | J_ID | J_DOC | J_PATCH | J_MANAGER | J_IF_REVISION | J_LIMIT
        | J_CONTINUE | J_AT_REVISION | J_SCOPE_NAME | J_ID_PREFIX | J_VIEW | J_TABLE
        | J_LIST_HEAD | J_KIND | J_API_VERSION => Some(1u64 << tag),
        _ => None,
    }
}

/// Is `body` a well-shaped `tag` field? Fixed-width fields have their
/// width; the JSON fields are checked as JSON by the dialect, which
/// parses them.
fn field_is_valid(tag: u8, body: &[u8]) -> bool {
    match tag {
        J_IF_REVISION | J_AT_REVISION => body.len() == 8,
        J_LIMIT => body.len() == 4,
        J_COLLECTION | J_ID | J_DOC | J_PATCH | J_ID_PREFIX => !body.is_empty(),
        _ => true,
    }
}

fn rd_u32(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

fn rd_u64(b: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
}

/// A decoded, framing-checked `MSG_JSON_DOC_REQUEST`.
#[derive(Clone, Copy, Debug)]
pub struct JsonRequest<'a> {
    pub corr_id: u64,
    pub dialect: u8,
    pub tenant_id: u32,
    pub conn_id: u8,
    pub op: u8,
    pub flags: u8,
    fields: &'a [u8],
}

impl<'a> JsonRequest<'a> {
    /// Decode a request of at most `max` bytes. `None` for a head or
    /// field this wire does not carry, a repeated field, or a field of
    /// the wrong width.
    pub fn decode_up_to(p: &'a [u8], max: usize) -> Option<Self> {
        if p.len() < JSON_DOC_REQUEST_HEAD || p.len() > max {
            return None;
        }
        let op = p[14];
        let flags = p[15];
        if !op_is_known(op) || flags & !J_FLAGS_KNOWN != 0 {
            return None;
        }
        if op != OP_WATCH && flags & (J_FLAG_FOLLOW | J_FLAG_CANCEL) != 0 {
            return None;
        }
        let fields = p.get(JSON_DOC_REQUEST_HEAD..)?;
        let mut seen = 0u64;
        let mut end = 0usize;
        for (tag, body) in Fields::of(fields) {
            let bit = tag_bit(tag)?;
            if seen & bit != 0 || !field_is_valid(tag, body) {
                return None;
            }
            seen |= bit;
            end += FIELD_HEAD + body.len();
        }
        if end != fields.len() {
            return None;
        }
        Some(JsonRequest {
            corr_id: rd_u64(p, 0)?,
            dialect: p[8],
            tenant_id: rd_u32(p, 9)?,
            conn_id: p[13],
            op,
            flags,
            fields,
        })
    }

    /// A field's body.
    pub fn field(&self, tag: u8) -> Option<&'a [u8]> {
        Fields::of(self.fields)
            .find(|(t, _)| *t == tag)
            .map(|(_, b)| b)
    }

    pub fn if_revision(&self) -> Option<u64> {
        rd_u64(self.field(J_IF_REVISION)?, 0)
    }

    pub fn at_revision(&self) -> Option<u64> {
        rd_u64(self.field(J_AT_REVISION)?, 0)
    }

    pub fn limit(&self) -> Option<u32> {
        rd_u32(self.field(J_LIMIT)?, 0)
    }
}

/// A decoded `MSG_JSON_DOC_RESPONSE`.
#[derive(Clone, Copy, Debug)]
pub struct JsonResponse<'a> {
    pub corr_id: u64,
    pub conn_id: u8,
    pub outcome: u8,
    pub flags: u8,
    pub revision: u64,
    fields: &'a [u8],
}

impl<'a> JsonResponse<'a> {
    /// Decode an answer of at most `max` bytes; `None` when its framing
    /// is broken or it holds a field this wire does not carry.
    pub fn decode_up_to(p: &'a [u8], max: usize) -> Option<Self> {
        if p.len() < super::doc_exec::DOC_RESPONSE_HEAD || p.len() > max {
            return None;
        }
        let fields = p.get(super::doc_exec::DOC_RESPONSE_HEAD..)?;
        let mut seen = 0u8;
        let mut end = 0usize;
        for (tag, body) in Fields::of(fields) {
            let bit = match tag {
                JR_TEXT => 1,
                JR_CONTINUE => 2,
                JR_ERROR => 4,
                _ => return None,
            };
            if seen & bit != 0 {
                return None;
            }
            seen |= bit;
            end += FIELD_HEAD + body.len();
        }
        if end != fields.len() {
            return None;
        }
        Some(JsonResponse {
            corr_id: rd_u64(p, 0)?,
            conn_id: p[8],
            outcome: p[9],
            flags: p[10],
            revision: rd_u64(p, 11)?,
            fields,
        })
    }

    pub fn field(&self, tag: u8) -> Option<&'a [u8]> {
        Fields::of(self.fields)
            .find(|(t, _)| *t == tag)
            .map(|(_, b)| b)
    }

    /// The rendered answer.
    pub fn text(&self) -> Option<&'a [u8]> {
        self.field(JR_TEXT)
    }
}
