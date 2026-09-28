//! A collection's schema as directive text: what a graph states about its
//! documents' lifecycle, translated into the collection descriptor the
//! executor applies.
//!
//! A JSON dialect is configured the way nanocloud's store connectors are:
//! one `schema` param of `name=value;…` directives and one `patch_keys`
//! param. The Kubernetes statement of a resource is data in the graph,
//! and nothing here names Kubernetes. This file owns the grammar and the
//! translation of the writer directives into `doc_catalog` entries; the
//! directives that shape an answer (views, templates, tables) are read by
//! the dialect that renders them, through [`directive`].
//!
//! Mounted beside `doc_catalog.rs`, `doc_tree.rs` and `json_tree.rs`
//! (reached as `super::…`).
//!
//! ## Grammar
//!
//! Directives are separated by `;` and their values cannot hold one. A
//! path is dotted; a segment holding dots (or `[`) is bracketed
//! (`labels[example.com/name]`). A list is `,`-separated. A generator is
//! `uuid`, `now`, `name`, or `#<json>` (a literal value, which cannot hold
//! `,` or `;`).
//!
//! | Directive | Descriptor |
//! |---|---|
//! | `identity=<path>,…` | `IDENTITY` per path |
//! | `create=<path>:<gen>,…` | `STAMP` per entry |
//! | `generate=<path>[\|<n>\|<alphabet>]` | `GENERATE`: the string at path plus `n` characters of `alphabet` (the dialect's default when not stated) |
//! | `keep=<path>,…` | `KEEP` per path |
//! | `named=<collection>\|<path>:<gen>,…` | For that collection only: `STAMP` and `KEEP` of the path (Kubernetes' namespace name label) |
//! | `guard=<path>` | `GUARD` |
//! | `deleting=<path>:<gen>,…` | `DELETING` per entry, the first being the mark |
//! | `rv=<path>` | `REVISION_PATH`, the revision as a decimal string |
//! | `owners=<path>` | `OWNERSHIP` on (the entries' rendering is the dialect's), with `owners.api=<path>` as its version path |
//! | `unowned=<path>,…` | `UNOWNED` per path |
//! | `codec=path` | `CODEC` [`CODEC_PATH_JSON`]: documents are JSON text at `/<collection>/<id>` (`tree`, the default, states the engine's own layout) |
//!
//! Generators: `uuid` → an RFC 4122 string, `now` → an RFC 3339 string,
//! `#<json>` → the literal, and `name` → the object's name, which the
//! dialect's key mapping defines (see [`Defaults::name`]).
//!
//! `patch_keys` states how a keyed merge merges lists: `list=member` (every
//! list named `list`), `list@parent=member` (a `list` inside an element of
//! a `parent` list; the more specific entry wins), `+list` (a set of
//! scalars). Each becomes a `LIST_KEY` entry.

use super::doc_catalog::{
    DescriptorError, DescriptorWriter, CODEC_PATH_JSON, CODEC_TREE, FORM_TEXT, FORM_VALUE, GEN_ID,
    GEN_LITERAL, GEN_NOW, GEN_UUID, OWNERSHIP_ON, TAG_CODEC, TAG_DELETING, TAG_GUARD, TAG_IDENTITY,
    TAG_ID_PATH, TAG_KEEP, TAG_OWNERSHIP, TAG_OWNERS_TEXT, TAG_REVISION_PATH, TAG_STAMP,
    TAG_UNOWNED,
};

/// Longest ownership dialect a stored collection carries (its directives'
/// text).
const OWNERS_TEXT_MAX: usize = 1024;
use super::doc_tree::{self, MAX_PATH_SEGMENTS};
use super::json_tree;

/// Why a schema was refused: the whole schema is refused, never half
/// applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaError {
    /// A path that is empty, unbalanced, or too deep.
    BadPath,
    /// A generator that is not one of the grammar's, or a literal that is
    /// not JSON.
    BadGenerator,
    /// An entry that does not have its directive's shape.
    BadEntry,
    /// A `name` generator where the dialect maps no name.
    NoName,
    /// The descriptor refused an entry, or ran out of room.
    Descriptor(DescriptorError),
}

impl From<DescriptorError> for SchemaError {
    fn from(e: DescriptorError) -> Self {
        SchemaError::Descriptor(e)
    }
}

/// What the dialect supplies where the schema states nothing.
pub struct Defaults<'a> {
    /// Random characters a generated name gets, and their alphabet.
    pub suffix: u8,
    pub alphabet: &'a [u8],
    /// The dialect's key mapping: the dotted paths the document id is
    /// read from, in order (several: a compound id). Declared as the
    /// collection's id paths. A `name` stamp on one of them is the id
    /// itself (nothing to stamp); elsewhere it stamps a single-path id,
    /// and is refused for a compound one or where none is mapped.
    pub id_paths: &'a [&'a [u8]],
    /// The collection the descriptor is for (`named` entries name theirs).
    pub collection: &'a [u8],
}

fn is_space(c: u8) -> bool {
    c == b' ' || c == b'\t' || c == b'\n' || c == b'\r'
}

fn trim(mut b: &[u8]) -> &[u8] {
    while let Some((&c, rest)) = b.split_first() {
        if !is_space(c) {
            break;
        }
        b = rest;
    }
    while let Some((&c, rest)) = b.split_last() {
        if !is_space(c) {
            break;
        }
        b = rest;
    }
    b
}

/// The value of directive `name` in `schema`: the first one stated.
pub fn directive<'a>(schema: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    schema.split(|&b| b == b';').find_map(|ent| {
        let ent = trim(ent);
        let eq = ent.iter().position(|&b| b == b'=')?;
        let (k, v) = ent.split_at(eq);
        (trim(k) == name).then(|| trim(v.get(1..).unwrap_or(&[])))
    })
}

/// The non-empty entries of a `,`-list.
fn entries(list: &[u8]) -> impl Iterator<Item = &[u8]> {
    list.split(|&b| b == b',')
        .map(trim)
        .filter(|e| !e.is_empty())
}

/// A dotted path (`a.b[c.d].e`) as a tree path into `out`: its length.
pub fn dotted_path(text: &[u8], out: &mut [u8]) -> Result<usize, SchemaError> {
    let mut at = 1usize;
    let mut n = 0usize;
    let mut i = 0usize;
    let put = |out: &mut [u8], at: &mut usize, b: &[u8]| -> Result<(), SchemaError> {
        let d = out
            .get_mut(*at..*at + b.len())
            .ok_or(SchemaError::Descriptor(DescriptorError::Overflow))?;
        for (x, y) in d.iter_mut().zip(b) {
            *x = *y;
        }
        *at += b.len();
        Ok(())
    };
    while i < text.len() {
        let rest = text.get(i..).unwrap_or(&[]);
        let (seg, next) = if rest.first() == Some(&b'[') {
            let e = rest
                .iter()
                .position(|&b| b == b']')
                .ok_or(SchemaError::BadPath)?;
            (rest.get(1..e).unwrap_or(&[]), i + e + 1)
        } else {
            // A `]` outside brackets is a bracket never opened.
            let e = rest
                .iter()
                .position(|&b| b == b'.' || b == b'[' || b == b']')
                .unwrap_or(rest.len());
            if rest.get(e) == Some(&b']') {
                return Err(SchemaError::BadPath);
            }
            (rest.get(..e).unwrap_or(&[]), i + e)
        };
        if seg.is_empty() || n == MAX_PATH_SEGMENTS {
            return Err(SchemaError::BadPath);
        }
        let len = u16::try_from(seg.len()).map_err(|_| SchemaError::BadPath)?;
        put(out, &mut at, &len.to_le_bytes())?;
        put(out, &mut at, seg)?;
        n += 1;
        i = next;
        // A dot separates; a bracketed segment may follow directly.
        match text.get(i) {
            Some(b'.') if i + 1 < text.len() => i += 1,
            Some(b'[') | None => {}
            _ => return Err(SchemaError::BadPath),
        }
    }
    if n == 0 {
        return Err(SchemaError::BadPath);
    }
    *out.first_mut()
        .ok_or(SchemaError::Descriptor(DescriptorError::Overflow))? = n as u8;
    doc_tree::Path::parse(out.get(..at).unwrap_or(&[])).map_err(|_| SchemaError::BadPath)?;
    Ok(at)
}

/// Largest encoded path or literal one directive entry makes.
const ENTRY_BUF: usize = 512;

/// A `<path>:<gen>` entry as a `STAMP` or `DELETING`.
/// A stamp entry's path and generator: the generator follows the last `:`
/// outside a literal (a literal's own colons are inside the JSON after
/// `#`).
fn split_gen(ent: &[u8]) -> Result<(&[u8], &[u8]), SchemaError> {
    let c = match ent.iter().position(|&b| b == b'#') {
        Some(h) => h.checked_sub(1).filter(|&c| ent.get(c) == Some(&b':')),
        None => ent.iter().rposition(|&b| b == b':'),
    }
    .ok_or(SchemaError::BadEntry)?;
    Ok((ent.get(..c).unwrap_or(&[]), ent.get(c + 1..).unwrap_or(&[])))
}

fn stamp_entry(
    tag: u8,
    ent: &[u8],
    d: &Defaults<'_>,
    w: &mut DescriptorWriter<'_>,
) -> Result<(), SchemaError> {
    let (path, gen) = split_gen(ent)?;
    let mut p = [0u8; ENTRY_BUF];
    let pl = dotted_path(trim(path), &mut p)?;
    let path = p.get(..pl).unwrap_or(&[]);
    match trim(gen) {
        b"uuid" => w.stamp(tag, path, GEN_UUID, FORM_TEXT, &[])?,
        b"now" => w.stamp(tag, path, GEN_NOW, FORM_TEXT, &[])?,
        b"name" => {
            let mut q = [0u8; ENTRY_BUF];
            let mut is_id = false;
            for id in d.id_paths {
                let ql = dotted_path(id, &mut q)?;
                is_id |= q.get(..ql) == Some(path);
            }
            if is_id {
                // The id is read from here: stamping it is the id itself.
            } else if d.id_paths.len() == 1 {
                w.stamp(tag, path, GEN_ID, FORM_VALUE, &[])?
            } else {
                return Err(SchemaError::NoName);
            }
        }
        g => {
            let lit = g.strip_prefix(b"#").ok_or(SchemaError::BadGenerator)?;
            let mut v = [0u8; ENTRY_BUF];
            let n = json_tree::parse(lit, &mut v).map_err(|_| SchemaError::BadGenerator)?;
            w.stamp(
                tag,
                path,
                GEN_LITERAL,
                FORM_VALUE,
                v.get(..n).unwrap_or(&[]),
            )?;
        }
    }
    Ok(())
}

/// Every path of a `,`-list as an entry with body `path`.
fn path_entries(tag: u8, list: &[u8], w: &mut DescriptorWriter<'_>) -> Result<(), SchemaError> {
    for ent in entries(list) {
        let mut p = [0u8; ENTRY_BUF];
        let pl = dotted_path(ent, &mut p)?;
        w.path(tag, p.get(..pl).unwrap_or(&[]))?;
    }
    Ok(())
}

/// Translate the writer directives of `schema` and the list keys of
/// `patch_keys` into entries of `w`. Directives this file does not
/// translate (the dialect's rendering ones) are left to the dialect.
pub fn describe(
    schema: &[u8],
    patch_keys: &[u8],
    d: &Defaults<'_>,
    w: &mut DescriptorWriter<'_>,
) -> Result<(), SchemaError> {
    for id in d.id_paths {
        let mut p = [0u8; ENTRY_BUF];
        let pl = dotted_path(id, &mut p)?;
        w.path(TAG_ID_PATH, p.get(..pl).unwrap_or(&[]))?;
    }
    if let Some(list) = directive(schema, b"identity") {
        path_entries(TAG_IDENTITY, list, w)?;
    }
    if let Some(list) = directive(schema, b"keep") {
        path_entries(TAG_KEEP, list, w)?;
    }
    if let Some(list) = directive(schema, b"create") {
        for ent in entries(list) {
            stamp_entry(TAG_STAMP, ent, d, w)?;
        }
    }
    if let Some(list) = directive(schema, b"named") {
        for ent in entries(list) {
            let bar = ent
                .iter()
                .position(|&b| b == b'|')
                .ok_or(SchemaError::BadEntry)?;
            if trim(ent.get(..bar).unwrap_or(&[])) != d.collection {
                continue;
            }
            let rest = ent.get(bar + 1..).unwrap_or(&[]);
            stamp_entry(TAG_STAMP, rest, d, w)?;
            let (path, _) = split_gen(rest)?;
            path_entries(TAG_KEEP, trim(path), w)?;
        }
    }
    if let Some(list) = directive(schema, b"deleting") {
        for ent in entries(list) {
            stamp_entry(TAG_DELETING, ent, d, w)?;
        }
    }
    if let Some(g) = directive(schema, b"guard") {
        let mut p = [0u8; ENTRY_BUF];
        let pl = dotted_path(g, &mut p)?;
        w.path(TAG_GUARD, p.get(..pl).unwrap_or(&[]))?;
    }
    if let Some(g) = directive(schema, b"generate") {
        let mut parts = g.split(|&b| b == b'|');
        let path = trim(parts.next().unwrap_or(&[]));
        let (suffix, alphabet) = match (parts.next(), parts.next(), parts.next()) {
            (None, None, None) => (d.suffix, d.alphabet),
            (Some(n), Some(a), None) => (parse_u8(trim(n))?, trim(a)),
            _ => return Err(SchemaError::BadEntry),
        };
        if suffix == 0 || alphabet.is_empty() {
            return Err(SchemaError::BadEntry);
        }
        let mut p = [0u8; ENTRY_BUF];
        let pl = dotted_path(path, &mut p)?;
        w.generate(p.get(..pl).unwrap_or(&[]), suffix, alphabet)?;
    }
    if let Some(rv) = directive(schema, b"rv") {
        let mut p = [0u8; ENTRY_BUF];
        let pl = dotted_path(rv, &mut p)?;
        w.entry(
            TAG_REVISION_PATH,
            &[&[FORM_TEXT], p.get(..pl).unwrap_or(&[])],
        )?;
    }
    if directive(schema, b"owners").is_some_and(|o| !o.is_empty()) {
        let mut p = [0u8; ENTRY_BUF];
        let pl = match directive(schema, b"owners.api").filter(|a| !a.is_empty()) {
            Some(api) => dotted_path(api, &mut p)?,
            None => 0,
        };
        w.entry(
            TAG_OWNERSHIP,
            &[&[OWNERSHIP_ON], p.get(..pl).unwrap_or(&[])],
        )?;
    }
    if let Some(list) = directive(schema, b"unowned") {
        path_entries(TAG_UNOWNED, list, w)?;
    }
    if let Some(c) = directive(schema, b"codec") {
        let codec = match trim(c) {
            b"path" => CODEC_PATH_JSON,
            b"tree" => CODEC_TREE,
            _ => return Err(SchemaError::BadEntry),
        };
        w.entry(TAG_CODEC, &[&[codec]])?;
        // The object carries its ownership: the engine keeps it there as
        // the dialect spells it.
        if codec == CODEC_PATH_JSON && directive(schema, b"owners").is_some_and(|o| !o.is_empty()) {
            let mut text = [0u8; OWNERS_TEXT_MAX];
            let mut at = 0usize;
            for name in [
                &b"owners"[..],
                b"owners.keys",
                b"owners.ops",
                b"owners.api",
                b"owners.entry",
                b"owners.marks",
            ] {
                if let Some(v) = directive(schema, name) {
                    for part in [name, b"=", v, b";"] {
                        let d = text
                            .get_mut(at..at + part.len())
                            .ok_or(SchemaError::BadEntry)?;
                        for (x, y) in d.iter_mut().zip(part) {
                            *x = *y;
                        }
                        at += part.len();
                    }
                }
            }
            w.entry(TAG_OWNERS_TEXT, &[text.get(..at).unwrap_or(&[])])?;
        }
    }
    for ent in entries(patch_keys) {
        list_key_entry(ent, w)?;
    }
    Ok(())
}

fn parse_u8(t: &[u8]) -> Result<u8, SchemaError> {
    let mut v: u8 = 0;
    if t.is_empty() {
        return Err(SchemaError::BadEntry);
    }
    for &c in t {
        if !c.is_ascii_digit() {
            return Err(SchemaError::BadEntry);
        }
        v = v
            .checked_mul(10)
            .and_then(|v| v.checked_add(c - b'0'))
            .ok_or(SchemaError::BadEntry)?;
    }
    Ok(v)
}

/// One `patch_keys` entry as a `LIST_KEY`.
fn list_key_entry(ent: &[u8], w: &mut DescriptorWriter<'_>) -> Result<(), SchemaError> {
    let (lhs, member) = match ent.strip_prefix(b"+") {
        Some(set) => (set, None),
        None => {
            let eq = ent
                .iter()
                .position(|&b| b == b'=')
                .ok_or(SchemaError::BadEntry)?;
            (
                ent.get(..eq).unwrap_or(&[]),
                Some(trim(ent.get(eq + 1..).unwrap_or(&[]))),
            )
        }
    };
    let lhs = trim(lhs);
    // `list@parent`: the list inside any element of `parent`.
    let (list, parent) = match lhs.iter().position(|&b| b == b'@') {
        Some(at) => (
            lhs.get(..at).unwrap_or(&[]),
            Some(lhs.get(at + 1..).unwrap_or(&[])),
        ),
        None => (lhs, None),
    };
    let mut p = [0u8; ENTRY_BUF];
    let mut at = 1usize;
    let mut n = 0u8;
    let mut seg = |s: &[u8]| -> Result<(), SchemaError> {
        if s.is_empty() || s.len() > u8::MAX as usize {
            return Err(SchemaError::BadEntry);
        }
        let d = p
            .get_mut(at..at + 2 + s.len())
            .ok_or(SchemaError::BadEntry)?;
        let len = (s.len() as u16).to_le_bytes();
        for (x, y) in d.iter_mut().zip(len.iter().chain(s)) {
            *x = *y;
        }
        at += 2 + s.len();
        n += 1;
        Ok(())
    };
    if let Some(parent) = parent {
        seg(trim(parent))?;
        seg(super::doc_catalog::EVERY_ELEMENT)?;
    }
    seg(trim(list))?;
    p[0] = n;
    let mut keys = [0u8; 2 + u8::MAX as usize];
    let kl = match member {
        None => 1,
        Some(m) => {
            if m.is_empty() || m.len() > u8::MAX as usize {
                return Err(SchemaError::BadEntry);
            }
            keys[0] = 1;
            keys[1] = m.len() as u8;
            for (x, y) in keys.iter_mut().skip(2).zip(m) {
                *x = *y;
            }
            2 + m.len()
        }
    };
    w.list_key(p.get(..at).unwrap_or(&[]), keys.get(..kl).unwrap_or(&[]))?;
    Ok(())
}
