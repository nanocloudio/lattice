//! doc_json_dialect — JSON documents over the document executor.
//!
//! A record adapter that owns its own format (Chronicle's store
//! connectors) states document operations on `req_in` as
//! `MSG_JSON_DOC_REQUEST` (`modules/common/doc_json.rs`): canonical
//! operations with their documents as JSON text, and the view an object
//! is read and written through. This module translates each into the
//! canonical `MSG_DOC_REQUEST` for `document_executor`, and renders the
//! answer back as JSON text the way the graph's schema says —
//! ownership entries placed in the object, the view's template, an apply's
//! conflicts as the schema words them.
//!
//! The collection is the schema's: every write carries the descriptor the
//! `schema` / `patch_keys` / `id_paths` params translate to
//! (`doc_schema.rs`), and the executor creates the collection with it on
//! its first write. Nothing here names Kubernetes; the Kubernetes
//! statement is params in the graph.
//!
//! One operation at a time: `req_in` is not read while one is in flight,
//! so a producer is held back by its channel. A request or answer larger
//! than one frame crosses either edge as `wire::MSG_PART` parts (the
//! `big` twin, `doc_json_dialect_big`, holds documents to the executor's
//! big bound).

#![no_std]
#![allow(
    unused_imports,
    dead_code,
    unreachable_patterns,
    reason = "PIC build path-mounts the fluxor SDK wholesale; each module consumes only a subset of the ABI surface"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    clippy::too_many_arguments,
    clippy::duplicate_mod,
    reason = "fluxor module ABI: raw-pointer entry points are the contract, ABI fns carry a fixed arity, and the PIC build #[path]-remounts shared SDK/common code"
)]
#![allow(
    unexpected_cfgs,
    reason = "`big` is the doc_json_dialect_big twin's variant feature, declared in its manifest"
)]
use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");

#[path = "../../common/types.rs"]
mod types;

#[path = "../../common/wire.rs"]
mod wire;

#[path = "../../common/doc_exec.rs"]
mod doc_exec;
use doc_exec::{doc_catalog, doc_fields, doc_ops, doc_tree};

#[path = "../../common/doc_json.rs"]
mod doc_json;

#[path = "../../common/json_tree.rs"]
mod json_tree;

#[path = "../../common/doc_schema.rs"]
mod doc_schema;

#[path = "../../common/doc_render.rs"]
mod doc_render;

#[path = "../../common/doc_buf.rs"]
mod doc_buf;
use doc_buf::DocBuf;

#[path = "../../common/telemetry.rs"]
mod telemetry;

use doc_catalog::DescriptorWriter;
use doc_exec::*;
use doc_json::*;
use doc_render::{conflict_json, owners_dialect, owners_json, Out};
use doc_tree::{Value, Writer};
use wire::{MSG_DOC_REQUEST, MSG_DOC_RESPONSE, MSG_JSON_DOC_REQUEST, MSG_JSON_DOC_RESPONSE};

// ── Capacities ───────────────────────────────────────────────────────

/// A JSON request or answer: one channel frame; in the big build the
/// executor's document bound and room for the rest of the message.
#[cfg(not(feature = "big"))]
const JSON_MAX: usize = wire::FRAME_PAYLOAD_MAX;
/// The big build's document policy: the executor big twin's
/// (`document_executor_big`, 1.5 MiB).
#[cfg(feature = "big")]
const MAX_DOCUMENT: usize = 1572864;
#[cfg(feature = "big")]
const JSON_MAX: usize = MAX_DOCUMENT + 65536;
/// A canonical tree of a JSON text in the request, or of the object
/// answered: at most the executor's own message bound.
const TREE_BUF: usize = JSON_MAX;
/// The canonical request or answer, with its envelope.
const EXEC_BUF: usize = wire::ENVELOPE_HDR + JSON_MAX + 4096;
/// The client's answer, with its envelope and head.
const RESP_BUF: usize = wire::ENVELOPE_HDR + JSON_MAX + 256;
/// A document's ownership entries as JSON, and as a tree.
#[cfg(not(feature = "big"))]
const OWN_TEXT: usize = 8192;
/// The big build bounds them as it bounds the document: an entry's
/// `fieldsV1` grows with the document's fields (a ConfigMap of 22,000
/// keys spells ~300 KB of them), not with a fixed share of its bytes.
#[cfg(feature = "big")]
const OWN_TEXT: usize = JSON_MAX;
/// Room an entry's field set is spelled in, and an apply's conflicts.
#[cfg(not(feature = "big"))]
const SCRATCH: usize = 16384;
#[cfg(feature = "big")]
const SCRATCH: usize = JSON_MAX;
/// The graph's params.
const DOC_SCHEMA_MAX: usize = 4096;
const PATCH_KEYS_MAX: usize = 1024;
const ID_PATHS_MAX: usize = 1024;
const RV_PATH_MAX: usize = 64;
const TABLE_MAX: usize = 4096;
/// A list's continuation, as the executor states it.
const CONTINUE_MAX: usize = 1024;
/// Id paths a collection's id is read from (a compound id).
const MAX_ID_PATHS: usize = 4;
/// How long the executor has to answer before the client is told so.
const AWAIT_MS: u64 = 30000;
/// A descriptor as this module builds it.
const DESC_BUF: usize = doc_catalog::MAX_DESCRIPTOR_LEN;

/// Watches followed at once, each the executor's too.
const MAX_SESSIONS: usize = 16;
/// A followed watch's request, kept to render its later answers.
const SESSION_REQ_MAX: usize = 1024;
/// How often a followed watch that asked for bookmarks is sent one.
const BOOKMARK_MS: u64 = 60000;

/// A followed watch: the client's `corr` and `conn` it answers, the
/// executor's `corr` its changes arrive on, its request, the last
/// revision answered and the last bookmark.
#[derive(Clone, Copy)]
struct Session {
    live: bool,
    client_corr: u64,
    client_conn: u8,
    exec_corr: u64,
    last_rev: u64,
    bookmark_ms: u64,
    req_len: u16,
    req: [u8; SESSION_REQ_MAX],
}

const PHASE_IDLE: u8 = 0;
const PHASE_TO_EXEC: u8 = 1;
const PHASE_AWAIT: u8 = 2;
const PHASE_TO_CLIENT: u8 = 3;

/// A generated name's random characters and their alphabet (no vowels,
/// no look-alikes: Kubernetes' choice, and a reasonable default).
const NAME_SUFFIX: u8 = 5;

define_params! {
    DialectState;

    1, schema, str_chunked, 0
        => |s, d, len| {
            let at = s.schema_len;
            if at + len > DOC_SCHEMA_MAX {
                s.params_bad = 1;
            } else {
                s.schema[at..at + len].copy_from_slice(unsafe { core::slice::from_raw_parts(d, len) });
                s.schema_len = at + len;
            }
        };
    2, patch_keys, str_chunked, 0
        => |s, d, len| {
            let at = s.patch_keys_len;
            if at + len > PATCH_KEYS_MAX {
                s.params_bad = 1;
            } else {
                s.patch_keys[at..at + len].copy_from_slice(unsafe { core::slice::from_raw_parts(d, len) });
                s.patch_keys_len = at + len;
            }
        };
    3, id_paths, str_chunked, 0
        => |s, d, len| {
            let at = s.id_paths_len;
            if at + len > ID_PATHS_MAX {
                s.params_bad = 1;
            } else {
                s.id_paths[at..at + len].copy_from_slice(unsafe { core::slice::from_raw_parts(d, len) });
                s.id_paths_len = at + len;
            }
        };
    4, rv_path, str, 0
        => |s, d, len| {
            if len > RV_PATH_MAX {
                s.params_bad = 1;
            } else {
                s.rv_path[..len].copy_from_slice(unsafe { core::slice::from_raw_parts(d, len) });
                s.rv_path_len = len;
            }
        };
    5, table, str_chunked, 0
        => |s, d, len| {
            let at = s.table_len;
            if at + len > TABLE_MAX {
                s.params_bad = 1;
            } else {
                s.table[at..at + len].copy_from_slice(unsafe { core::slice::from_raw_parts(d, len) });
                s.table_len = at + len;
            }
        };
}

#[repr(C)]
struct DialectState {
    syscalls: *const SyscallTable,
    req_in: i32,
    exec_in: i32,
    resp_out: i32,
    exec_out: i32,
    metrics_out: i32,
    params_bad: u8,
    /// The schema as the collection states it: `rv=<rv_path>;` first when
    /// the graph names one, then the `schema` param.
    schema: [u8; DOC_SCHEMA_MAX + RV_PATH_MAX + 8],
    schema_len: usize,
    patch_keys: [u8; PATCH_KEYS_MAX],
    patch_keys_len: usize,
    id_paths: [u8; ID_PATHS_MAX],
    id_paths_len: usize,
    rv_path: [u8; RV_PATH_MAX],
    rv_path_len: usize,
    /// Table columns per kind (`doc_render.rs`'s grammar).
    table: [u8; TABLE_MAX],
    table_len: usize,

    phase: u8,
    since_ms: u64,
    /// The client's request (whole), and its parts as they arrive.
    jreq: DocBuf<JSON_MAX>,
    jreq_len: usize,
    jreq_parts: wire::Reassembly,
    /// The request's `corr`, `conn` and view, kept for the answer.
    client_corr: u64,
    client_conn: u8,
    client_op: u8,
    /// The canonical request, with its envelope, and its parts going out.
    ereq: DocBuf<EXEC_BUF>,
    ereq_len: usize,
    exec_corr: u64,
    exec_stream: u32,
    parts_out: wire::PartWriter,
    /// JSON text parsed to a tree, one field at a time.
    tree: DocBuf<TREE_BUF>,
    /// The executor's answer (whole), and its parts as they arrive.
    eans: DocBuf<EXEC_BUF>,
    eans_len: usize,
    eans_parts: wire::Reassembly,
    /// The answered object with its ownership entries placed, and its
    /// text as the client reads it.
    doc: DocBuf<TREE_BUF>,
    text: DocBuf<JSON_MAX>,
    own_text: DocBuf<OWN_TEXT>,
    own_tree: DocBuf<OWN_TEXT>,
    scratch: DocBuf<SCRATCH>,
    /// The client's answer, with its envelope, and its parts going out.
    jresp: DocBuf<RESP_BUF>,
    jresp_len: usize,
    resp_stream: u32,
    desc: [u8; DESC_BUF],
    env: [u8; wire::ENVELOPE_HDR + wire::FRAME_PAYLOAD_MAX],
    step_ctr: u32,
    sessions: [Session; MAX_SESSIONS],
    /// The executor corrs this module has used, and the session a cancel
    /// in flight ends (index + 1; 0: none).
    corr_seq: u64,
    cancel_of: u8,
    /// The phase to return to once an answer has gone (a followed watch's
    /// answer can go out while a request awaits its own).
    resume_phase: u8,
    m_requests: u64,
    m_answers: u64,
    m_refused: u64,
    m_timeouts: u64,
}

/// The big build's heap: every `DocBuf` in the state, and the allocator's
/// rounding.
#[cfg(feature = "big")]
const ARENA_BYTES: usize =
    2 * JSON_MAX + 2 * EXEC_BUF + 2 * TREE_BUF + 2 * OWN_TEXT + SCRATCH + RESP_BUF + 64 * 1024;
#[cfg(feature = "big")]
declare_module_arena_bytes!(ARENA_BYTES);

// ── Channels ─────────────────────────────────────────────────────────

/// One envelope from `chan` into `buf`: its type and payload length.
unsafe fn read_envelope(sys: &SyscallTable, chan: i32, buf: &mut [u8]) -> Option<(u8, usize)> {
    if chan < 0 {
        return None;
    }
    let poll = (sys.channel_poll)(chan, POLL_IN);
    if poll <= 0 || (poll as u32) & POLL_IN == 0 {
        return None;
    }
    let mut hdr = [0u8; wire::ENVELOPE_HDR];
    if (sys.channel_read)(chan, hdr.as_mut_ptr(), wire::ENVELOPE_HDR) < wire::ENVELOPE_HDR as i32 {
        return None;
    }
    let n = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
    if n > buf.len() {
        let mut sink = [0u8; 256];
        let mut left = n;
        while left > 0 {
            let k = left.min(sink.len());
            if (sys.channel_read)(chan, sink.as_mut_ptr(), k) <= 0 {
                break;
            }
            left -= k;
        }
        return None;
    }
    if n > 0 && ((sys.channel_read)(chan, buf.as_mut_ptr(), n) as usize) < n {
        return None;
    }
    Some((hdr[0], n))
}

/// Send `msg` (at `buf[ENVELOPE_HDR..ENVELOPE_HDR + len]`) on `chan`: one
/// envelope when it fits a frame, else the next of its parts. `true` when
/// all of it has gone.
unsafe fn send_msg(
    sys: &SyscallTable,
    chan: i32,
    msg_type: u8,
    buf: &mut [u8],
    len: usize,
    parts: &mut wire::PartWriter,
    frame: &mut [u8],
) -> bool {
    if chan < 0 {
        return false;
    }
    if len <= wire::FRAME_PAYLOAD_MAX {
        if !wire::write_envelope_hdr(buf, msg_type, len) {
            return true;
        }
        let total = wire::ENVELOPE_HDR + len;
        return (sys.channel_write)(chan, buf.as_mut_ptr(), total) == total as i32;
    }
    let at = wire::ENVELOPE_HDR;
    while !parts.done() {
        let Some(msg) = buf.get(at..at + len) else {
            return true;
        };
        let Some(n) = parts.frame(msg, frame) else {
            return true;
        };
        if (sys.channel_write)(chan, frame.as_mut_ptr(), n) != n as i32 {
            return false;
        }
        parts.sent();
    }
    true
}

// ── Translating a request ────────────────────────────────────────────

/// Why a request is answered here, without reaching the executor.
struct Refusal {
    outcome: u8,
    why: &'static [u8],
}

#[inline(never)]
fn refused(outcome: u8, why: &'static [u8]) -> Refusal {
    Refusal { outcome, why }
}

#[inline(never)]
fn why_json() -> &'static [u8] {
    b"a JSON field of the request does not parse"
}

#[inline(never)]
fn why_large() -> &'static [u8] {
    b"the request is larger than this dialect takes"
}

#[inline(never)]
fn why_schema() -> &'static [u8] {
    b"the dialect's schema does not translate into a collection"
}

#[inline(never)]
fn why_malformed() -> &'static [u8] {
    b"the request is not one this wire carries"
}

#[inline(never)]
fn why_view() -> &'static [u8] {
    b"the view is not one the schema states"
}

#[inline(never)]
fn why_no_watch() -> &'static [u8] {
    b"no watch is followed on this corr"
}

#[inline(never)]
fn why_busy() -> &'static [u8] {
    b"as many watches are followed as this dialect holds"
}

#[inline(never)]
fn why_timeout() -> &'static [u8] {
    b"the executor did not answer in time"
}

fn is_write(op: u8) -> bool {
    matches!(
        op,
        OP_INSERT | OP_REPLACE | OP_PATCH_MERGE | OP_PATCH_JSON | OP_PATCH_KEYED | OP_APPLY
    )
}

/// `prefix` then `name` into `out`: a directive's name (`view.<v>`).
fn directive_name<'o>(prefix: &[u8], name: &[u8], out: &'o mut [u8; 72]) -> Option<&'o [u8]> {
    let n = prefix.len() + name.len();
    out.get_mut(..prefix.len())?.copy_from_slice(prefix);
    out.get_mut(prefix.len()..n)?.copy_from_slice(name);
    out.get(..n)
}

/// Parse the JSON of `field` into `tree` and add it to `w` as `tag`.
/// Parse the JSON of `field` into `tree` and add it to `w` as `tag`: the
/// tree's length.
fn json_field(
    tree: &mut [u8],
    w: &mut FieldWriter<'_>,
    tag: u8,
    json: &[u8],
) -> Result<usize, Refusal> {
    let n = json_tree::parse(json, tree).map_err(|e| match e.reason {
        json_tree::JsonReason::Overflow => refused(OUT_TOO_LARGE, why_large()),
        _ => refused(OUT_MALFORMED, why_json()),
    })?;
    w.field(tag, &[tree.get(..n).unwrap_or(&[])])
        .ok_or_else(|| refused(OUT_TOO_LARGE, why_large()))?;
    Ok(n)
}

/// Does the schema make the revision a body states the precondition of
/// `op`? `rv.if=<ops>` names them — `replace`, `merge`, `keyed`, `apply`
/// — as Kubernetes does for an update, a merge or strategic patch, and a
/// server-side apply. A JSON Patch states no object, so never one.
fn rv_conditional(schema: &[u8], op: u8) -> bool {
    let Some(list) = doc_schema::directive(schema, b"rv.if") else {
        return false;
    };
    // Each name compared in place: a `match` yielding the literals would
    // lower to a table of absolute pointers, which a module cannot hold.
    list.split(|&c| c == b',').map(trim).any(|o| {
        (op == OP_REPLACE && o == b"replace")
            || (op == OP_PATCH_MERGE && o == b"merge")
            || (op == OP_PATCH_KEYED && o == b"keyed")
            || (op == OP_APPLY && o == b"apply")
    })
}

/// The revision a request body (a tree) states at the schema's `rv` path:
/// a decimal string or an integer, above 0. An empty or zero one states
/// none (the write is unconditional), as upstream reads it.
fn body_revision(schema: &[u8], body: &[u8]) -> Option<u64> {
    let rvp = doc_schema::directive(schema, b"rv")?;
    let mut pb = [0u8; RV_PATH_MAX * 2];
    let pl = doc_schema::dotted_path(rvp, &mut pb).ok()?;
    let path = doc_tree::Path::parse(pb.get(..pl)?).ok()?;
    let v = doc_tree::lookup(Value::parse(body).ok()?, path)?;
    let rev = if let Some(s) = v.as_str() {
        if s.is_empty() || !s.iter().all(u8::is_ascii_digit) {
            return None;
        }
        s.iter().try_fold(0u64, |a, &c| {
            a.checked_mul(10)?.checked_add(u64::from(c - b'0'))
        })?
    } else {
        u64::try_from(v.as_i64()?).ok()?
    };
    (rev > 0).then_some(rev)
}

/// A view's write scope (`write.<v>=<path>,…`) as a path list.
fn write_scope(list: &[u8], out: &mut [u8]) -> Option<usize> {
    *out.get_mut(0)? = MODE_INCLUDE;
    let mut n = 0u8;
    let mut at = 2usize;
    for ent in list.split(|&c| c == b',') {
        let ent = trim(ent);
        if ent.is_empty() {
            continue;
        }
        let room = out.get_mut(at + 2..)?;
        let pl = doc_schema::dotted_path(ent, room).ok()?;
        out.get_mut(at..at + 2)?
            .copy_from_slice(&(pl as u16).to_le_bytes());
        at += 2 + pl;
        n = n.checked_add(1)?;
    }
    *out.get_mut(1)? = n;
    Some(at)
}

fn trim(mut b: &[u8]) -> &[u8] {
    while let Some((&c, rest)) = b.split_first() {
        if c != b' ' {
            break;
        }
        b = rest;
    }
    while let Some((&c, rest)) = b.split_last() {
        if c != b' ' {
            break;
        }
        b = rest;
    }
    b
}

/// The keyed-patch directive names the schema spells (`smp.*`), as the
/// canonical field: `{[len:u8][name]}` × 4.
fn directives(schema: &[u8], out: &mut [u8; 256]) -> Option<usize> {
    let mut at = 0usize;
    for name in [
        doc_schema::directive(schema, b"smp.patch"),
        doc_schema::directive(schema, b"smp.retain"),
        doc_schema::directive(schema, b"smp.delete"),
        doc_schema::directive(schema, b"smp.prefix"),
    ] {
        let v = name.unwrap_or(&[]);
        *out.get_mut(at)? = u8::try_from(v.len()).ok()?;
        out.get_mut(at + 1..at + 1 + v.len())?.copy_from_slice(v);
        at += 1 + v.len();
    }
    Some(at)
}

/// The collection's descriptor from the schema, into `st.desc`.
/// A collection's id paths from the `id_paths` param,
/// `<paths>[;<collection>,…=<paths>]…`: the paths of the entry that names
/// the collection, else the first entry's (a cluster-scoped kind's id is
/// its name alone, a namespaced kind's its namespace and name).
fn id_paths_for<'p>(param: &'p [u8], collection: &[u8]) -> &'p [u8] {
    let mut default: &[u8] = &[];
    for (i, ent) in param.split(|&c| c == b';').enumerate() {
        match ent.iter().position(|&c| c == b'=') {
            Some(eq) => {
                let names = ent.get(..eq).unwrap_or(&[]);
                if names.split(|&c| c == b',').any(|n| trim(n) == collection) {
                    return ent.get(eq + 1..).unwrap_or(&[]);
                }
            }
            None if i == 0 => default = ent,
            None => {}
        }
    }
    default
}

fn describe(st: &mut DialectState, name: &[u8]) -> Result<usize, Refusal> {
    let bad = || refused(OUT_INVALID, why_schema());
    let ids_text = id_paths_for(st.id_paths.get(..st.id_paths_len).unwrap_or(&[]), name);
    let mut ids: [&[u8]; MAX_ID_PATHS] = [&[]; MAX_ID_PATHS];
    let mut n = 0usize;
    for p in ids_text.split(|&c| c == b',') {
        let p = trim(p);
        if p.is_empty() {
            continue;
        }
        *ids.get_mut(n).ok_or_else(bad)? = p;
        n += 1;
    }
    let defaults = doc_schema::Defaults {
        suffix: NAME_SUFFIX,
        alphabet: b"bcdfghjklmnpqrstvwxz2456789",
        id_paths: ids.get(..n).unwrap_or(&[]),
        collection: name,
    };
    let mut w = DescriptorWriter::new(&mut st.desc, 0, name).map_err(|_| bad())?;
    doc_schema::describe(
        st.schema.get(..st.schema_len).unwrap_or(&[]),
        st.patch_keys.get(..st.patch_keys_len).unwrap_or(&[]),
        &defaults,
        &mut w,
    )
    .map_err(|_| bad())?;
    w.finish().map_err(|_| bad())
}

/// The canonical request for the client's, into `st.ereq`: its length.
fn translate(st: &mut DialectState) -> Result<usize, Refusal> {
    let jl = st.jreq_len;
    let jb: &[u8] = &st.jreq;
    // The request is read from `jreq` while `ereq`, `tree` and `desc`
    // are written: a view of it that does not borrow the state.
    // SAFETY: `jreq` is not written until this request is answered.
    let jb: &[u8] = unsafe { core::slice::from_raw_parts(jb.as_ptr(), jl.min(jb.len())) };
    let Some(r) = JsonRequest::decode_up_to(jb, JSON_MAX) else {
        return Err(refused(OUT_MALFORMED, why_malformed()));
    };
    st.client_corr = r.corr_id;
    st.client_conn = r.conn_id;
    st.client_op = r.op;
    if r.flags & J_FLAG_CANCEL != 0 {
        return translate_cancel(st, &r);
    }
    let follow = r.flags & J_FLAG_FOLLOW != 0;
    if follow {
        if jl > SESSION_REQ_MAX {
            return Err(refused(OUT_TOO_LARGE, why_large()));
        }
        if !st.sessions.iter().any(|x| !x.live) {
            return Err(refused(OUT_BUSY, why_busy()));
        }
    }
    let schema_len = st.schema_len;
    let name = r.field(J_COLLECTION).unwrap_or(&[]);
    let dn = if is_write(r.op) {
        describe(st, name)?
    } else {
        0
    };
    let mut flags = r.flags & (FLAG_DRY_RUN | FLAG_FORCE);
    if is_write(r.op) {
        flags |= FLAG_AUTO_CREATE;
    }
    if follow {
        flags |= FLAG_FOLLOW;
    }
    st.corr_seq = st.corr_seq.wrapping_add(1);
    st.exec_corr = st.corr_seq;
    let corr = st.exec_corr;
    let large = || refused(OUT_TOO_LARGE, why_large());
    let DialectState {
        ereq,
        tree,
        desc,
        schema,
        ..
    } = st;
    let schema = schema.get(..schema_len).unwrap_or(&[]);
    let out = ereq.get_mut(wire::ENVELOPE_HDR..).unwrap_or(&mut []);
    let mut w = FieldWriter::request(out, corr, r.dialect, r.tenant_id, 0, r.op, flags)
        .ok_or_else(large)?;
    w.field(FIELD_COLLECTION, &[name]).ok_or_else(large)?;
    if dn > 0 {
        w.field(FIELD_DESCRIPTOR, &[desc.get(..dn).unwrap_or(&[])])
            .ok_or_else(large)?;
    }
    // The revision the body states, where the schema makes it the write's
    // precondition (`rv.if`) and the request states none of its own.
    let mut stated: Option<u64> = None;
    let conditional = r.field(J_IF_REVISION).is_none() && rv_conditional(schema, r.op);
    for (jt, ct) in [
        (J_ID, FIELD_ID),
        (J_DOC, FIELD_DOC),
        (J_PATCH, FIELD_PATCH),
        (J_ID_PREFIX, FIELD_ID_PREFIX),
    ] {
        if let Some(j) = r.field(jt) {
            let n = json_field(tree, &mut w, ct, j)?;
            if conditional && stated.is_none() && matches!(jt, J_DOC | J_PATCH) {
                stated = body_revision(schema, tree.get(..n).unwrap_or(&[]));
            }
        }
    }
    if let Some(rev) = stated {
        w.field(FIELD_IF_REVISION, &[&rev.to_le_bytes()])
            .ok_or_else(large)?;
    }
    for (jt, ct) in [
        (J_MANAGER, FIELD_MANAGER),
        (J_IF_REVISION, FIELD_IF_REVISION),
        (J_LIMIT, FIELD_LIMIT),
        (J_AT_REVISION, FIELD_AT_REVISION),
    ] {
        if let Some(b) = r.field(jt) {
            w.field(ct, &[b]).ok_or_else(large)?;
        }
    }
    if let Some(t) = r.field(J_CONTINUE) {
        // The client's text back into the executor's continuation.
        let mut cb = [0u8; CONTINUE_MAX];
        let n = unhex(t, &mut cb).ok_or_else(|| refused(OUT_MALFORMED, why_malformed()))?;
        w.field(FIELD_CONTINUE, &[cb.get(..n).unwrap_or(&[])])
            .ok_or_else(large)?;
    }
    if matches!(r.op, OP_LIST | OP_WATCH) {
        // What a page may hold as trees: the answer's JSON is larger (text
        // escapes, the head and tail), so half of what it can carry.
        w.field(FIELD_MAX_BYTES, &[&((JSON_MAX / 2) as u32).to_le_bytes()])
            .ok_or_else(large)?;
    }
    let view = r.field(J_VIEW).filter(|v| !v.is_empty());
    // A write through a view is recorded under it, and changes only what
    // the view writes; an apply is recorded under no scope.
    let scope = r
        .field(J_SCOPE_NAME)
        .or(if r.op == OP_APPLY { None } else { view });
    if is_write(r.op) {
        if let Some(s) = scope {
            w.field(FIELD_SCOPE_NAME, &[s]).ok_or_else(large)?;
        }
        if let Some(v) = view {
            let mut nb = [0u8; 72];
            let key = directive_name(b"write.", v, &mut nb)
                .ok_or_else(|| refused(OUT_INVALID, why_view()))?;
            if let Some(list) = doc_schema::directive(schema, key) {
                let mut pl = [0u8; 1024];
                let n =
                    write_scope(list, &mut pl).ok_or_else(|| refused(OUT_INVALID, why_schema()))?;
                w.field(FIELD_WRITE_SCOPE, &[pl.get(..n).unwrap_or(&[])])
                    .ok_or_else(large)?;
            }
        }
        if matches!(r.op, OP_PATCH_KEYED | OP_APPLY) {
            let mut db = [0u8; 256];
            let n =
                directives(schema, &mut db).ok_or_else(|| refused(OUT_INVALID, why_schema()))?;
            w.field(FIELD_DIRECTIVES, &[db.get(..n).unwrap_or(&[])])
                .ok_or_else(large)?;
        }
    }
    Ok(w.finish())
}

/// A cancel: the executor's followed watch the session names, ended on
/// its own corr.
fn translate_cancel(st: &mut DialectState, r: &JsonRequest<'_>) -> Result<usize, Refusal> {
    let Some(i) = st
        .sessions
        .iter()
        .position(|x| x.live && x.client_corr == r.corr_id && x.client_conn == r.conn_id)
    else {
        return Err(refused(OUT_NOT_FOUND, why_no_watch()));
    };
    st.exec_corr = st.sessions[i].exec_corr;
    st.cancel_of = (i + 1) as u8;
    let large = || refused(OUT_TOO_LARGE, why_large());
    let out = st.ereq.get_mut(wire::ENVELOPE_HDR..).unwrap_or(&mut []);
    let mut w = FieldWriter::request(
        out,
        st.exec_corr,
        r.dialect,
        r.tenant_id,
        0,
        OP_WATCH,
        FLAG_FOLLOW | FLAG_CANCEL,
    )
    .ok_or_else(large)?;
    w.field(FIELD_COLLECTION, &[r.field(J_COLLECTION).unwrap_or(&[])])
        .ok_or_else(large)?;
    Ok(w.finish())
}

// ── Rendering an answer ──────────────────────────────────────────────

/// `b` as lowercase hex into `out`.
fn hex_into(b: &[u8], out: &mut Out<'_>) -> Result<(), doc_render::RenderError> {
    for &c in b {
        let d = [hex_digit(c >> 4), hex_digit(c & 0xF)];
        out.put(&d)?;
    }
    Ok(())
}

fn hex_digit(v: u8) -> u8 {
    if v < 10 {
        b'0' + v
    } else {
        b'a' + v - 10
    }
}

/// Lowercase or uppercase hex `t` into `out`: its length.
fn unhex(t: &[u8], out: &mut [u8]) -> Option<usize> {
    if !t.len().is_multiple_of(2) || t.len() / 2 > out.len() {
        return None;
    }
    let val = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    for (i, pair) in t.chunks(2).enumerate() {
        let (h, l) = (*pair.first()?, *pair.get(1)?);
        *out.get_mut(i)? = (val(h)? << 4) | val(l)?;
    }
    Some(t.len() / 2)
}

/// The buffers an answer is rendered with.
struct Bufs<'b> {
    tree: &'b mut [u8],
    own_text: &'b mut [u8],
    own_tree: &'b mut [u8],
    scratch: &'b mut [u8],
}

/// How the client asked the answer to read.
#[derive(Clone, Copy)]
struct Reading<'r> {
    op: u8,
    view: Option<&'r [u8]>,
    /// The table kind (`J_TABLE`).
    kind: Option<&'r [u8]>,
    head: &'r [u8],
    /// A watch with no change answers a bookmark of this kind and version.
    bookmark: bool,
    res_kind: &'r [u8],
    api: &'r [u8],
    /// The collection's id paths (a deleted document's object is its id).
    id_paths: &'r [u8],
}

/// The client's answer to the executor's, into `st.jresp`: its length.
/// `req` is the client's request the answer is for (the one in flight, or
/// a followed watch's), `corr`, `conn` and `op` its own.
fn render(st: &mut DialectState, req: &[u8], corr: u64, conn: u8, op: u8) -> usize {
    let el = st.eans_len;
    let eb: &[u8] = &st.eans;
    // SAFETY: `eans` is not written until the next request's answer.
    let eb: &[u8] = unsafe { core::slice::from_raw_parts(eb.as_ptr(), el.min(eb.len())) };
    let Some(a) = DocResponse::decode_up_to(eb, EXEC_BUF) else {
        return answer_refusal(st, refused(OUT_CORRUPT, why_malformed()));
    };
    let req = JsonRequest::decode_up_to(req, JSON_MAX);
    let field = |t| {
        req.and_then(|r| r.field(t))
            .filter(|v: &&[u8]| !v.is_empty())
    };
    let reading = Reading {
        op,
        view: field(J_VIEW),
        kind: field(J_TABLE),
        head: field(J_LIST_HEAD).unwrap_or(&[]),
        bookmark: req.is_some_and(|r| r.flags & J_FLAG_BOOKMARK != 0),
        res_kind: field(J_KIND).unwrap_or(&[]),
        api: field(J_API_VERSION).unwrap_or(&[]),
        id_paths: &[],
    };
    let now_s = (unsafe { dev_unix_millis(&*st.syscalls) } / 1000) as i64;
    let (schema_len, table_len, id_paths_len) = (st.schema_len, st.table_len, st.id_paths_len);
    let DialectState {
        jresp,
        doc,
        text,
        own_text,
        own_tree,
        scratch,
        schema,
        table,
        id_paths,
        ..
    } = st;
    let schema = schema.get(..schema_len).unwrap_or(&[]);
    let table = table.get(..table_len).unwrap_or(&[]);
    let reading = Reading {
        id_paths: id_paths_for(
            id_paths.get(..id_paths_len).unwrap_or(&[]),
            req.and_then(|r| r.field(J_COLLECTION)).unwrap_or(&[]),
        ),
        ..reading
    };
    let mut bufs = Bufs {
        tree: doc,
        own_text,
        own_tree,
        scratch,
    };
    let out = jresp.get_mut(wire::ENVELOPE_HDR..).unwrap_or(&mut []);
    let Some(mut w) = FieldWriter::response(out, corr, conn, a.outcome, a.flags, a.revision) else {
        return 0;
    };
    let mut o = Out::new(text);
    let rendered = match a.outcome {
        OUT_OK if reading.op == OP_WATCH => Some(watch(schema, &reading, &a, &mut bufs, &mut o)),
        OUT_COMPACTED if reading.op == OP_WATCH => {
            doc_schema::directive(schema, b"expired").map(|m| o.put(m).map_err(|_| ()))
        }
        OUT_OK if reading.op == OP_LIST || reading.kind.is_some() => {
            Some(page(schema, table, &reading, &a, now_s, &mut bufs, &mut o))
        }
        OUT_OK => a
            .docs()
            .next()
            .map(|(_, d)| object(schema, reading.view, d, a.owners_of(0), &mut bufs, &mut o)),
        OUT_OWNERSHIP => Some(
            conflict_json(
                schema,
                a.conflicts().map(|(m, _, v, f)| (m, v, f)),
                a.conflicts().count(),
                bufs.scratch,
                &mut o,
            )
            .map_err(|_| ()),
        ),
        OUT_NOT_FOUND => {
            doc_schema::directive(schema, b"missing").map(|m| o.put(m).map_err(|_| ()))
        }
        _ => None,
    };
    let n = o.len();
    match rendered {
        Some(Ok(())) if n <= w.room() => {
            let _ = w.field(JR_TEXT, &[text.get(..n).unwrap_or(&[])]);
        }
        Some(_) => w.set_outcome(OUT_TOO_LARGE, a.flags),
        None => {
            if let Some((_, _, msg)) = a.error() {
                let _ = w.field(JR_ERROR, &[msg]);
            }
        }
    }
    if let Some(c) = a.continuation() {
        let mut cb = [0u8; 2 * CONTINUE_MAX];
        let mut co = Out::new(&mut cb);
        if hex_into(c, &mut co).is_ok() {
            let cl = co.len();
            let _ = w.field(JR_CONTINUE, &[cb.get(..cl).unwrap_or(&[])]);
        }
    }
    w.finish()
}

/// The object as the client reads it, into `o`: its ownership entries
/// placed at the schema's `owners` path, then through the view when one
/// is named.
fn object(
    schema: &[u8],
    view: Option<&[u8]>,
    d: Value<'_>,
    owners: Option<doc_fields::Owners<'_>>,
    b: &mut Bufs<'_>,
    o: &mut Out<'_>,
) -> Result<(), ()> {
    let obj = match (owners, owners_dialect(schema)) {
        (Some(ow), Some(dl)) => {
            let mut ob = doc_render::OwnersBufs {
                scratch: b.scratch,
                text: b.own_text,
                tree: b.own_tree,
            };
            let mut w = Writer::new(b.tree);
            doc_render::with_owners(d, ow, &dl, &mut ob, &mut w).map_err(|_| ())?;
            let n = w.finish().map_err(|_| ())?;
            Value::parse(b.tree.get(..n).unwrap_or(&[])).map_err(|_| ())?
        }
        _ => d,
    };
    let tpl = view.and_then(|v| {
        let mut nb = [0u8; 72];
        let key = directive_name(b"view.", v, &mut nb)?;
        doc_schema::directive(schema, key)?.strip_prefix(b"tpl:")
    });
    match tpl {
        Some(t) => doc_render::view(t, obj, o).map_err(|_| ()),
        None => o.json(obj).map_err(|_| ()),
    }
}

/// A list page (or an object read as a table), into `o`: the request's
/// head — or, as a table, the schema's `table.head` with the kind's
/// columns — the items (objects as the client reads them, or table rows),
/// then the schema's `list.tail` with the revision read at and, when the
/// page was cut, where the next continues.
fn page(
    schema: &[u8],
    table: &[u8],
    r: &Reading<'_>,
    a: &DocResponse<'_>,
    now_s: i64,
    b: &mut Bufs<'_>,
    o: &mut Out<'_>,
) -> Result<(), ()> {
    let cols = r.kind.map(|k| doc_render::table_columns(table, k));
    match cols {
        Some(c) => doc_render::table_head(schema, c, o).map_err(|_| ())?,
        None => o.put(r.head).map_err(|_| ())?,
    }
    let mut first = true;
    let mut fields = a.fields().peekable();
    while let Some((t, body)) = fields.next() {
        if t != RESP_DOC {
            continue;
        }
        let Some(d) = body.get(8..).and_then(|v| Value::parse(v).ok()) else {
            return Err(());
        };
        let owners = match fields.peek() {
            Some((RESP_OWNERS, ob)) => doc_fields::Owners::parse(ob).ok(),
            _ => None,
        };
        if !first {
            o.put(b",").map_err(|_| ())?;
        }
        first = false;
        match cols {
            Some(c) => doc_render::table_row(schema, c, d, now_s, o).map_err(|_| ())?,
            None => object(schema, r.view, d, owners, b, o)?,
        }
    }
    let mut rv = [0u8; 20];
    let rl = decimal_into(a.revision, &mut rv);
    let mut cb = [0u8; 2 * CONTINUE_MAX];
    let mut co = Out::new(&mut cb);
    if let Some(c) = a.continuation() {
        hex_into(c, &mut co).map_err(|_| ())?;
    }
    let cl = co.len();
    let tail = doc_schema::directive(schema, b"list.tail").unwrap_or(&[]);
    let vars: [(&[u8], &[u8]); 2] = [
        (b"rv", rv.get(..rl).unwrap_or(&[])),
        (b"continue", cb.get(..cl).unwrap_or(&[])),
    ];
    doc_render::fill(tail, &vars, o).map_err(|_| ())
}

/// A watch's changes as the schema frames them, into `o`: one line per
/// change (`event`, its type the schema's `event.types` name for a
/// creation, a change or a deletion), joined by newlines; with no change
/// and a bookmark asked for, the schema's `bookmark` at the revision the
/// watch resumes from.
fn watch(
    schema: &[u8],
    r: &Reading<'_>,
    a: &DocResponse<'_>,
    b: &mut Bufs<'_>,
    o: &mut Out<'_>,
) -> Result<(), ()> {
    let types = doc_schema::directive(schema, b"event.types").ok_or(())?;
    let mut names = types.split(|&c| c == b',').map(trim);
    let created = names.next().ok_or(())?;
    let modified = names.next().ok_or(())?;
    let deleted = names.next().ok_or(())?;
    let tpl = doc_schema::directive(schema, b"event").ok_or(())?;
    let (pre, post) = doc_render::split(tpl, b"{{object}}").ok_or(())?;
    let mut n = 0usize;
    let mut fields = a.fields().peekable();
    while let Some((t, body)) = fields.next() {
        if t != RESP_EVENT {
            continue;
        }
        let rev = body
            .get(..8)
            .and_then(|v| v.try_into().ok())
            .map(u64::from_le_bytes)
            .ok_or(())?;
        let kind = *body.get(8).ok_or(())?;
        let v = body.get(9..).and_then(|v| Value::parse(v).ok()).ok_or(())?;
        let owners = match fields.peek() {
            Some((RESP_OWNERS, ob)) => doc_fields::Owners::parse(ob).ok(),
            _ => None,
        };
        let name = match kind {
            EVENT_CREATE => created,
            EVENT_PUT => modified,
            _ => deleted,
        };
        if n > 0 {
            o.put(b"\n").map_err(|_| ())?;
        }
        n += 1;
        let vars: [(&[u8], &[u8]); 1] = [(b"type", name)];
        doc_render::fill(pre, &vars, o).map_err(|_| ())?;
        if kind == EVENT_DELETE {
            gone(schema, r.id_paths, v, rev, b, o)?;
        } else {
            object(schema, r.view, v, owners, b, o)?;
        }
        doc_render::fill(post, &vars, o).map_err(|_| ())?;
    }
    if n == 0 && r.bookmark {
        bookmark_line(schema, r, a.revision, o)?;
    }
    Ok(())
}

/// A bookmark as a watch's line, into `o`: the schema's `bookmark` of the
/// kind and version the client states, at `rev`, framed by `event` with the
/// schema's bookmark name.
fn bookmark_line(schema: &[u8], r: &Reading<'_>, rev: u64, o: &mut Out<'_>) -> Result<(), ()> {
    let types = doc_schema::directive(schema, b"event.types").ok_or(())?;
    let marked = types.split(|&c| c == b',').map(trim).nth(3).ok_or(())?;
    let tpl = doc_schema::directive(schema, b"event").ok_or(())?;
    let (pre, post) = doc_render::split(tpl, b"{{object}}").ok_or(())?;
    let mut rv = [0u8; 20];
    let rl = decimal_into(rev, &mut rv);
    let bm = doc_schema::directive(schema, b"bookmark").ok_or(())?;
    let vars: [(&[u8], &[u8]); 1] = [(b"type", marked)];
    let bvars: [(&[u8], &[u8]); 3] = [
        (b"kind", r.res_kind),
        (b"av", r.api),
        (b"rv", rv.get(..rl).unwrap_or(&[])),
    ];
    doc_render::fill(pre, &vars, o).map_err(|_| ())?;
    doc_render::fill(bm, &bvars, o).map_err(|_| ())?;
    doc_render::fill(post, &vars, o).map_err(|_| ())
}

/// A deleted document as a change reports it, into `o`: its id at the
/// collection's id paths and the deleting revision at the schema's `rv`
/// path — what a reader needs to drop it (the document itself is gone).
fn gone(
    schema: &[u8],
    id_paths: &[u8],
    id: Value<'_>,
    rev: u64,
    b: &mut Bufs<'_>,
    o: &mut Out<'_>,
) -> Result<(), ()> {
    // Built by edits, alternating between two buffers.
    let mut w = Writer::new(b.own_tree);
    w.begin_object().map_err(|_| ())?;
    w.end().map_err(|_| ())?;
    let mut len = w.finish().map_err(|_| ())?;
    let mut in_own = true;
    let mut set =
        |path: &[u8], v: Value<'_>, len: &mut usize, in_own: &mut bool| -> Result<(), ()> {
            let mut pb = [0u8; 512];
            let pl = doc_schema::dotted_path(path, &mut pb).map_err(|_| ())?;
            let p = doc_tree::Path::parse(pb.get(..pl).unwrap_or(&[])).map_err(|_| ())?;
            let (src, dst): (&[u8], &mut [u8]) = if *in_own {
                (&*b.own_tree, &mut *b.tree)
            } else {
                (&*b.tree, &mut *b.own_tree)
            };
            let cur = Value::parse(src.get(..*len).unwrap_or(&[])).map_err(|_| ())?;
            let mut w = Writer::new(dst);
            doc_ops::edit(cur, p, doc_ops::Edit::Set(v), &mut w).map_err(|_| ())?;
            *len = w.finish().map_err(|_| ())?;
            *in_own = !*in_own;
            Ok(())
        };
    let mut paths = id_paths
        .split(|&c| c == b',')
        .map(trim)
        .filter(|p| !p.is_empty());
    if id.ty() == doc_tree::T_ARRAY {
        for (p, v) in paths.by_ref().zip(id.elements()) {
            set(p, v, &mut len, &mut in_own)?;
        }
    } else if let Some(p) = paths.next_back() {
        set(p, id, &mut len, &mut in_own)?;
    }
    if let Some(rvp) = doc_schema::directive(schema, b"rv") {
        let mut digits = [0u8; 20];
        let dl = decimal_into(rev, &mut digits);
        let mut vb = [0u8; 32];
        let mut vw = Writer::new(&mut vb);
        vw.string(digits.get(..dl).unwrap_or(&[])).map_err(|_| ())?;
        let vn = vw.finish().map_err(|_| ())?;
        let v = Value::parse(vb.get(..vn).unwrap_or(&[])).map_err(|_| ())?;
        set(rvp, v, &mut len, &mut in_own)?;
    }
    let src: &[u8] = if in_own { &*b.own_tree } else { &*b.tree };
    let obj = Value::parse(src.get(..len).unwrap_or(&[])).map_err(|_| ())?;
    o.json(obj).map_err(|_| ())
}

/// `v` in decimal into `out`: its length.
fn decimal_into(mut v: u64, out: &mut [u8; 20]) -> usize {
    let mut d = [0u8; 20];
    let mut n = 0usize;
    loop {
        d[19 - n] = b'0' + (v % 10) as u8;
        n += 1;
        v /= 10;
        if v == 0 || n == 20 {
            break;
        }
    }
    let mut k = 0;
    while k < n {
        out[k] = d[20 - n + k];
        k += 1;
    }
    n
}

/// The client's answer to a request refused here, into `st.jresp`.
fn answer_refusal(st: &mut DialectState, r: Refusal) -> usize {
    st.m_refused = st.m_refused.wrapping_add(1);
    let (corr, conn) = (st.client_corr, st.client_conn);
    let out = st.jresp.get_mut(wire::ENVELOPE_HDR..).unwrap_or(&mut []);
    let Some(mut w) = FieldWriter::response(out, corr, conn, r.outcome, 0, 0) else {
        return 0;
    };
    let _ = w.field(JR_ERROR, &[r.why]);
    w.finish()
}

// ── The operation in flight ──────────────────────────────────────────

/// A whole request is in `jreq`: translate it and send it on, or answer
/// it here.
fn begin(st: &mut DialectState) {
    st.m_requests = st.m_requests.wrapping_add(1);
    // The head's corr and conn, for an answer to a request that does not
    // decode.
    st.client_corr = st
        .jreq
        .get(..8)
        .and_then(|b| b.try_into().ok())
        .map_or(0, u64::from_le_bytes);
    st.client_conn = st.jreq.get(13).copied().unwrap_or(0);
    if st.params_bad != 0 {
        let n = answer_refusal(st, refused(OUT_INVALID, why_schema()));
        to_client(st, n);
        return;
    }
    match translate(st) {
        Ok(n) => {
            st.ereq_len = n;
            st.exec_stream = st.exec_stream.wrapping_add(1);
            st.parts_out = wire::PartWriter::new(st.exec_stream, MSG_DOC_REQUEST, n);
            st.phase = PHASE_TO_EXEC;
            st.since_ms = now_ms(st);
        }
        Err(r) => {
            let n = answer_refusal(st, r);
            to_client(st, n);
        }
    }
}

/// Send the client its answer of `len` bytes (at `jresp[ENVELOPE_HDR..]`).
fn to_client(st: &mut DialectState, len: usize) {
    st.jresp_len = len;
    st.resp_stream = st.resp_stream.wrapping_add(1);
    st.parts_out = wire::PartWriter::new(st.resp_stream, MSG_JSON_DOC_RESPONSE, len);
    st.phase = PHASE_TO_CLIENT;
    st.resume_phase = PHASE_IDLE;
    st.m_answers = st.m_answers.wrapping_add(1);
}

fn now_ms(st: &DialectState) -> u64 {
    if st.syscalls.is_null() {
        return 0;
    }
    unsafe { dev_millis(&*st.syscalls) }
}

/// The executor's answer is whole in `eans`: to the request in flight, or
/// a followed watch's next changes.
fn answered(st: &mut DialectState) {
    let corr = st
        .eans
        .get(..8)
        .and_then(|b| b.try_into().ok())
        .map_or(0, u64::from_le_bytes);
    let flags = st.eans.get(10).copied().unwrap_or(0);
    let revision = st
        .eans
        .get(11..19)
        .and_then(|b| b.try_into().ok())
        .map_or(0, u64::from_le_bytes);
    if st.phase == PHASE_AWAIT && corr == st.exec_corr {
        if let Some(i) = st.cancel_of.checked_sub(1) {
            if let Some(x) = st.sessions.get_mut(i as usize) {
                x.live = false;
            }
            st.cancel_of = 0;
        }
        let jl = st.jreq_len;
        let jb: &[u8] = &st.jreq;
        // SAFETY: `jreq` is not written until this request is answered.
        let jb: &[u8] = unsafe { core::slice::from_raw_parts(jb.as_ptr(), jl.min(jb.len())) };
        let (cc, cn, op) = (st.client_corr, st.client_conn, st.client_op);
        let follow = JsonRequest::decode_up_to(jb, JSON_MAX)
            .is_some_and(|r| r.flags & J_FLAG_FOLLOW != 0 && r.flags & J_FLAG_CANCEL == 0);
        if follow && flags & RFLAG_MORE != 0 {
            follow_register(st, corr, revision);
        }
        let n = render(st, jb, cc, cn, op);
        to_client(st, n);
        return;
    }
    let Some(i) = st
        .sessions
        .iter()
        .position(|x| x.live && x.exec_corr == corr)
    else {
        // An answer to a request this module gave up on.
        return;
    };
    let x = &mut st.sessions[i];
    x.last_rev = revision;
    if flags & RFLAG_MORE == 0 {
        // The watch ended (compacted, an error): this answer says so.
        x.live = false;
    }
    let (cc, cn, rl) = (x.client_corr, x.client_conn, x.req_len as usize);
    let rb: &[u8] = &st.sessions[i].req;
    // SAFETY: a session's request is written only when it is registered.
    let rb: &[u8] = unsafe { core::slice::from_raw_parts(rb.as_ptr(), rl.min(rb.len())) };
    let resume = st.phase;
    let n = render(st, rb, cc, cn, OP_WATCH);
    to_client(st, n);
    st.resume_phase = resume;
}

/// Keep the request in flight as a followed watch answered on `exec_corr`.
fn follow_register(st: &mut DialectState, exec_corr: u64, revision: u64) {
    let Some(i) = st.sessions.iter().position(|x| !x.live) else {
        return;
    };
    let now = now_ms(st);
    let jl = st.jreq_len.min(SESSION_REQ_MAX);
    let (cc, cn) = (st.client_corr, st.client_conn);
    let DialectState { sessions, jreq, .. } = st;
    let x = &mut sessions[i];
    let mut k = 0;
    while k < jl {
        x.req[k] = jreq.get(k).copied().unwrap_or(0);
        k += 1;
    }
    x.req_len = jl as u16;
    x.client_corr = cc;
    x.client_conn = cn;
    x.exec_corr = exec_corr;
    x.last_rev = revision;
    x.bookmark_ms = now;
    x.live = true;
}

/// A bookmark for a followed watch that asked for them, when one is due.
fn bookmark_due(st: &mut DialectState) {
    let now = now_ms(st);
    for i in 0..MAX_SESSIONS {
        let x = &st.sessions[i];
        if !x.live || now.saturating_sub(x.bookmark_ms) < BOOKMARK_MS {
            continue;
        }
        let rl = x.req_len as usize;
        let rb: &[u8] = &x.req;
        // SAFETY: a session's request is written only when it is registered.
        let rb: &[u8] = unsafe { core::slice::from_raw_parts(rb.as_ptr(), rl.min(rb.len())) };
        let Some(r) = JsonRequest::decode_up_to(rb, JSON_MAX) else {
            continue;
        };
        st.sessions[i].bookmark_ms = now;
        if r.flags & J_FLAG_BOOKMARK == 0 {
            continue;
        }
        let x = st.sessions[i];
        let field = |t| r.field(t).unwrap_or(&[]);
        let reading = Reading {
            op: OP_WATCH,
            view: None,
            kind: None,
            head: &[],
            bookmark: true,
            res_kind: field(J_KIND),
            api: field(J_API_VERSION),
            id_paths: &[],
        };
        let schema_len = st.schema_len;
        let DialectState {
            jresp,
            text,
            schema,
            ..
        } = st;
        let schema = schema.get(..schema_len).unwrap_or(&[]);
        let mut o = Out::new(text);
        if bookmark_line(schema, &reading, x.last_rev, &mut o).is_err() {
            continue;
        }
        let tl = o.len();
        let out = jresp.get_mut(wire::ENVELOPE_HDR..).unwrap_or(&mut []);
        let Some(mut w) = FieldWriter::response(
            out,
            x.client_corr,
            x.client_conn,
            OUT_OK,
            RFLAG_MORE,
            x.last_rev,
        ) else {
            continue;
        };
        let _ = w.field(JR_TEXT, &[text.get(..tl).unwrap_or(&[])]);
        let n = w.finish();
        to_client(st, n);
        return;
    }
}

/// One envelope from the executor.
fn on_exec(st: &mut DialectState, msg: u8, n: usize) {
    let DialectState {
        env,
        eans,
        eans_parts,
        eans_len,
        ..
    } = st;
    let at = wire::ENVELOPE_HDR;
    let payload = env.get(at..at + n).unwrap_or(&[]);
    let whole = match msg {
        MSG_DOC_RESPONSE => {
            eans_parts.reset();
            match eans.get_mut(..n) {
                Some(d) => {
                    d.copy_from_slice(payload);
                    *eans_len = n;
                    true
                }
                None => false,
            }
        }
        wire::MSG_PART => match eans_parts.take(payload, eans) {
            Ok(Some((_, total))) => {
                *eans_len = total;
                true
            }
            _ => false,
        },
        _ => false,
    };
    if whole {
        answered(st);
    }
}

/// What one envelope from the client makes of the request.
enum Arrival {
    Pending,
    Whole,
    TooLarge,
}

/// One envelope from the client (read only while idle).
fn on_client(st: &mut DialectState, msg: u8, n: usize) {
    let arrival = {
        let DialectState {
            env,
            jreq,
            jreq_parts,
            jreq_len,
            ..
        } = st;
        let at = wire::ENVELOPE_HDR;
        let payload = env.get(at..at + n).unwrap_or(&[]);
        match msg {
            MSG_JSON_DOC_REQUEST => {
                jreq_parts.reset();
                match jreq.get_mut(..n) {
                    Some(d) => {
                        d.copy_from_slice(payload);
                        *jreq_len = n;
                        Arrival::Whole
                    }
                    None => Arrival::Pending,
                }
            }
            wire::MSG_PART => match jreq_parts.take(payload, jreq) {
                Ok(Some((MSG_JSON_DOC_REQUEST, total))) => {
                    *jreq_len = total;
                    Arrival::Whole
                }
                Err(wire::PartError::TooLarge) => {
                    // The first part carries the head: keep it to answer.
                    let head = payload.get(wire::PART_HEAD..).unwrap_or(&[]);
                    let k = head.len().min(DOC_REQUEST_HEAD);
                    if let (Some(d), Some(h)) = (jreq.get_mut(..k), head.get(..k)) {
                        d.copy_from_slice(h);
                    }
                    Arrival::TooLarge
                }
                _ => Arrival::Pending,
            },
            _ => Arrival::Pending,
        }
    };
    match arrival {
        Arrival::Pending => {}
        Arrival::Whole => begin(st),
        Arrival::TooLarge => {
            st.client_corr = st
                .jreq
                .get(..8)
                .and_then(|b| b.try_into().ok())
                .map_or(0, u64::from_le_bytes);
            st.client_conn = st.jreq.get(13).copied().unwrap_or(0);
            let n = answer_refusal(st, refused(OUT_TOO_LARGE, why_large()));
            to_client(st, n);
        }
    }
}

// ── Module ABI ───────────────────────────────────────────────────────

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<DialectState>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    ctrl_chan: i32,
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    let _ = ctrl_chan;
    if state.is_null() || syscalls.is_null() || state_size < core::mem::size_of::<DialectState>() {
        return -1;
    }
    let st = unsafe { &mut *state.cast::<DialectState>() };
    // The state arrives zeroed; set what is not zero.
    st.syscalls = syscalls.cast::<SyscallTable>();
    // inputs: req_in[0], exec_in[1]; outputs: resp_out[0], exec_out[1], metrics[2]
    st.req_in = in_chan;
    st.resp_out = out_chan;
    unsafe {
        let sys = &*st.syscalls;
        st.exec_in = dev_channel_port(sys, 0, 1);
        st.exec_out = dev_channel_port(sys, 1, 1);
        st.metrics_out = dev_channel_port(sys, 1, 2);
        #[cfg(feature = "big")]
        {
            if !(st.jreq.alloc(sys)
                && st.ereq.alloc(sys)
                && st.tree.alloc(sys)
                && st.eans.alloc(sys)
                && st.doc.alloc(sys)
                && st.text.alloc(sys)
                && st.own_text.alloc(sys)
                && st.own_tree.alloc(sys)
                && st.scratch.alloc(sys)
                && st.jresp.alloc(sys))
            {
                return -1;
            }
        }
    }
    if !params.is_null() && params_len >= 4 {
        unsafe { parse_tlv(st, params, params_len) };
    }
    if st.exec_in < 0 || st.exec_out < 0 {
        return -1;
    }
    // The schema the collection states: the graph's revision path first
    // (`directive` answers the first statement of a name).
    let rl = st.rv_path_len;
    if rl > 0 {
        let hl = 3 + rl + 1;
        let sl = st.schema_len;
        if sl + hl > st.schema.len() {
            st.params_bad = 1;
            return 0;
        }
        // Back to front, so nothing is overwritten before it is read.
        let mut k = sl;
        while k > 0 {
            k -= 1;
            st.schema[k + hl] = st.schema[k];
        }
        st.schema[..3].copy_from_slice(b"rv=");
        let mut k = 0;
        while k < rl {
            st.schema[3 + k] = st.rv_path[k];
            k += 1;
        }
        st.schema[3 + rl] = b';';
        st.schema_len = sl + hl;
    }
    0
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    if state.is_null() {
        return -1;
    }
    let st = unsafe { &mut *state.cast::<DialectState>() };
    let sys_ptr = st.syscalls;
    if sys_ptr.is_null() {
        return -1;
    }
    let sys = unsafe { &*sys_ptr };
    // What is leaving goes first.
    if st.phase == PHASE_TO_EXEC {
        let DialectState {
            exec_out,
            ereq,
            ereq_len,
            parts_out,
            env,
            ..
        } = st;
        if unsafe {
            send_msg(
                sys,
                *exec_out,
                MSG_DOC_REQUEST,
                ereq,
                *ereq_len,
                parts_out,
                env,
            )
        } {
            st.phase = PHASE_AWAIT;
        }
    }
    if st.phase == PHASE_TO_CLIENT {
        let DialectState {
            resp_out,
            jresp,
            jresp_len,
            parts_out,
            env,
            ..
        } = st;
        if unsafe {
            send_msg(
                sys,
                *resp_out,
                MSG_JSON_DOC_RESPONSE,
                jresp,
                *jresp_len,
                parts_out,
                env,
            )
        } {
            st.phase = st.resume_phase;
        }
    }
    // The executor's answers: read only while nothing is leaving (an
    // answer is rendered into the buffer the one leaving is sent from).
    for _ in 0..8 {
        if !matches!(st.phase, PHASE_IDLE | PHASE_AWAIT) {
            break;
        }
        let got = {
            let DialectState { exec_in, env, .. } = st;
            unsafe {
                read_envelope(
                    sys,
                    *exec_in,
                    env.get_mut(wire::ENVELOPE_HDR..).unwrap_or(&mut []),
                )
            }
        };
        let Some((msg, n)) = got else {
            break;
        };
        on_exec(st, msg, n);
    }
    if st.phase == PHASE_AWAIT && now_ms(st).saturating_sub(st.since_ms) > AWAIT_MS {
        st.m_timeouts = st.m_timeouts.wrapping_add(1);
        let n = answer_refusal(st, refused(OUT_TIMEOUT, why_timeout()));
        to_client(st, n);
    }
    if st.phase == PHASE_IDLE {
        bookmark_due(st);
    }
    // A new request only when idle: the producer waits in its channel.
    for _ in 0..8 {
        if st.phase != PHASE_IDLE {
            break;
        }
        let got = {
            let DialectState { req_in, env, .. } = st;
            unsafe {
                read_envelope(
                    sys,
                    *req_in,
                    env.get_mut(wire::ENVELOPE_HDR..).unwrap_or(&mut []),
                )
            }
        };
        let Some((msg, n)) = got else {
            break;
        };
        on_client(st, msg, n);
    }
    st.step_ctr = st.step_ctr.wrapping_add(1);
    if st.step_ctr.is_multiple_of(5000) {
        unsafe {
            telemetry::emit_counters(
                sys,
                st.metrics_out,
                &[st.m_requests, st.m_answers, st.m_refused, st.m_timeouts],
            );
        }
    }
    0
}
