//! Document collections: the descriptor record and the catalog keys.
//!
//! Collection metadata declares identifier, shard key, validation,
//! indexes, retention, and limits.
//!
//! A collection's descriptor is what makes the document executor
//! generic: every behaviour a dialect needs beyond storing trees — which
//! path holds the identity, what is stamped when a document is created,
//! what survives a replace, what holds a delete, how list elements are
//! identified for a keyed merge, whether writers own fields — is
//! DECLARED here as data, and the executor applies whatever is declared.
//! A Mongo collection declares `_id` and nothing else; a Kubernetes
//! resource declares its metadata stamps, finalizer guard, strategic
//! merge keys and field ownership. Neither dialect's names appear in the
//! executor.
//!
//! Contracts only — no ports, no I/O. Mounted by `doc_exec.rs`, which
//! mounts `doc_tree.rs` beside it; this file reaches the tree as
//! `super::doc_tree`.
//!
//! ## Record, version 1
//!
//! ```text
//! [version:u16 = 1][id:u32][name_len:u8][name][count:u8]
//! {[tag:u8][len:u16][body]}…
//! ```
//!
//! All integers little-endian. Entries appear in any order; a tag may
//! repeat where the table says so. An unknown tag, version or generator
//! is refused — a descriptor that half-applies is a collection that
//! half-behaves.
//!
//! | Tag | Body | Repeats |
//! |---|---|---|
//! | `ID_PATH` 1 | path — where the id lives; several make a compound id, the values at each in order (Kubernetes: namespace, name) | yes (default `_id`) |
//! | `SHARD_PATH` 2 | path | no (default: one shard) |
//! | `IDENTITY` 3 | path — no patch or replace may change it | yes |
//! | `STAMP` 4 | `[plen:u16][path][generator]` — set on create | yes |
//! | `KEEP` 5 | path — carried from the stored document across a replace | yes |
//! | `GENERATE` 6 | `[plen:u16][path][suffix:u8][alphabet]` — a create naming no id takes the string at path plus `suffix` random characters from `alphabet` as its id | no |
//! | `GUARD` 7 | path — a non-empty array there turns a delete into a held one | no |
//! | `DELETING` 8 | `[plen:u16][path][generator]` — what a held delete stamps; the first is the mark | yes |
//! | `LIST_KEY` 9 | `[plen:u16][path][n:u8]{[klen:u8][member]}…` — how a keyed patch merges the arrays at path: by the members that identify an element, or (`n` = 0) as a set of scalars. The path matches an array's location as a SUFFIX (`containers` is every `containers` array; `containers/*/ports` the `ports` in any of its elements; `*` is an element) and the longest match wins; an array no entry matches is replaced whole | yes |
//! | `OWNERSHIP` 10 | `[flags:u8][version path]`, bit 0: writers own the fields they set; the optional path names the value each writer's entry records as the document's version (Kubernetes' `apiVersion`) | no |
//! | `REVISION_PATH` 11 | `[form:u8][path]` — where an answered document carries its revision: `FORM_VALUE` an Integer, `FORM_TEXT` its decimal string | no |
//! | `MAX_BYTES` 12 | `[u32]` — largest document | no |
//! | `UNOWNED` 14 | path — a field no writer owns (bookkeeping: identity, revision, the ownership's own rendering) | yes |
//! | `INDEX` 13 | `[id:u32][flags:u8][state:u8][nlen:u8][name][n:u8]{[plen:u16][path]}…` — a secondary index over the values at its paths; `flags` bit 0 unique; `state` `INDEX_BUILDING` (writers keep it, readers do not use it) or `INDEX_READY` | yes |
//!
//! Generators: `[kind:u8][form:u8][args]`.
//!
//! | Kind | Forms |
//! |---|---|
//! | `GEN_UUID` 1 | `FORM_VALUE` 16-byte Identifier; `FORM_TEXT` RFC 4122 string |
//! | `GEN_NOW` 2 | `FORM_VALUE` Timestamp; `FORM_TEXT` RFC 3339 string (UTC, seconds) |
//! | `GEN_LITERAL` 3 | `FORM_VALUE`, args: one value |
//! | `GEN_ID` 4 | `FORM_VALUE`: the document's id |
//!
//! ## Catalog keys
//!
//! Under `models::KS_DOCUMENT_CATALOG`, in the layout the Mongo anchor
//! first laid down (its name record held the bare id; it now holds the
//! descriptor, which carries the id):
//!
//! ```text
//! [db:u32 BE][0x00]          the collection-id allocator (INCR)
//! [db:u32 BE][0x01][name]    the collection's descriptor record
//! ```

use super::doc_tree::{has_nul, utf8_is_valid, Path, TreeError, Value};

pub const DESCRIPTOR_V1: u16 = 1;
/// Longest collection name.
pub const MAX_NAME_LEN: usize = 64;
/// Largest descriptor record.
pub const MAX_DESCRIPTOR_LEN: usize = 2048;

pub const TAG_ID_PATH: u8 = 1;
pub const TAG_SHARD_PATH: u8 = 2;
pub const TAG_IDENTITY: u8 = 3;
pub const TAG_STAMP: u8 = 4;
pub const TAG_KEEP: u8 = 5;
pub const TAG_GENERATE: u8 = 6;
pub const TAG_GUARD: u8 = 7;
pub const TAG_DELETING: u8 = 8;
pub const TAG_LIST_KEY: u8 = 9;
pub const TAG_OWNERSHIP: u8 = 10;
pub const TAG_REVISION_PATH: u8 = 11;
pub const TAG_MAX_BYTES: u8 = 12;
pub const TAG_INDEX: u8 = 13;
pub const TAG_UNOWNED: u8 = 14;
/// How the collection's documents are stored (`[codec:u8]`); absent:
/// [`CODEC_TREE`].
pub const TAG_CODEC: u8 = 15;
/// The ownership dialect a [`CODEC_PATH_JSON`] collection stores its
/// ownership in (the schema's `owners…` directives, as text): the entries
/// live inside the stored object, so the engine reads and writes them.
pub const TAG_OWNERS_TEXT: u8 = 16;
const TAG_LAST: u8 = TAG_OWNERS_TEXT;

/// Documents as `doc_tree` records in `KS_DOCUMENT_DATA`, keyed by
/// collection id and id key (the engine's own layout).
pub const CODEC_TREE: u8 = 0;
/// Documents as JSON text in the flat protocol keyspace, at
/// `/<collection name>/<id>` (a compound id's components joined by `/`):
/// the keys and values a store's other clients read and write, so the
/// engine and they see one object (`doc_ops::path_key`).
pub const CODEC_PATH_JSON: u8 = 1;

pub const INDEX_UNIQUE: u8 = 1;
pub const INDEX_BUILDING: u8 = 1;
pub const INDEX_READY: u8 = 2;
/// Most paths one index spans.
pub const MAX_INDEX_PATHS: usize = 8;

pub const GEN_UUID: u8 = 1;
pub const GEN_NOW: u8 = 2;
pub const GEN_LITERAL: u8 = 3;
pub const GEN_ID: u8 = 4;

pub const FORM_VALUE: u8 = 1;
pub const FORM_TEXT: u8 = 2;

pub const OWNERSHIP_ON: u8 = 1;

/// The segment that stands for every element in a `LIST_KEY` path.
pub const EVERY_ELEMENT: &[u8] = b"*";

/// The default identity path.
pub const DEFAULT_ID_PATH: &[u8] = &[1, 3, 0, b'_', b'i', b'd'];

pub const CAT_KIND_SEQ: u8 = 0x00;
pub const CAT_KIND_NAME: u8 = 0x01;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DescriptorError {
    Truncated,
    UnknownVersion,
    UnknownTag,
    /// A tag stated twice that may be stated once.
    Repeated,
    BadName,
    BadPath,
    BadGenerator,
    BadBody,
    Overflow,
}

impl From<TreeError> for DescriptorError {
    fn from(_: TreeError) -> Self {
        DescriptorError::BadPath
    }
}

fn rd_u16(b: &[u8], at: usize) -> Option<u16> {
    let s = b.get(at..at.checked_add(2)?)?;
    Some(u16::from_le_bytes([s[0], s[1]]))
}

fn rd_u32(b: &[u8], at: usize) -> Option<u32> {
    let s = b.get(at..at.checked_add(4)?)?;
    Some(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

/// A value generator, as a descriptor states one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Generator<'a> {
    pub kind: u8,
    pub form: u8,
    /// `GEN_LITERAL`: the value's bytes. Empty otherwise.
    pub literal: &'a [u8],
}

impl<'a> Generator<'a> {
    pub fn parse(b: &'a [u8]) -> Result<Self, DescriptorError> {
        let kind = *b.first().ok_or(DescriptorError::BadGenerator)?;
        let form = *b.get(1).ok_or(DescriptorError::BadGenerator)?;
        let args = b.get(2..).unwrap_or(&[]);
        let ok = match kind {
            GEN_UUID | GEN_NOW => matches!(form, FORM_VALUE | FORM_TEXT) && args.is_empty(),
            GEN_ID => form == FORM_VALUE && args.is_empty(),
            GEN_LITERAL => form == FORM_VALUE && Value::parse(args).is_ok(),
            _ => false,
        };
        if !ok {
            return Err(DescriptorError::BadGenerator);
        }
        Ok(Generator {
            kind,
            form,
            literal: args,
        })
    }
}

/// `[plen:u16][path][rest]` → `(path, rest)`.
fn split_path(b: &[u8]) -> Result<(Path<'_>, &[u8]), DescriptorError> {
    let n = rd_u16(b, 0).ok_or(DescriptorError::BadBody)? as usize;
    let p = b.get(2..2 + n).ok_or(DescriptorError::BadBody)?;
    Ok((Path::parse(p)?, b.get(2 + n..).unwrap_or(&[])))
}

/// A stamp or deleting entry.
#[derive(Clone, Copy, Debug)]
pub struct Stamp<'a> {
    pub path: Path<'a>,
    pub generator: Generator<'a>,
}

/// A generated-id declaration.
#[derive(Clone, Copy, Debug)]
pub struct Generate<'a> {
    pub path: Path<'a>,
    pub suffix: u8,
    pub alphabet: &'a [u8],
}

/// A list-key declaration.
#[derive(Clone, Copy, Debug)]
pub struct ListKey<'a> {
    pub path: Path<'a>,
    /// `[n:u8]{[klen:u8][member]}…`
    keys: &'a [u8],
}

impl<'a> ListKey<'a> {
    /// A set of scalars (no identifying members).
    pub fn is_set(&self) -> bool {
        self.key_count() == 0
    }

    pub fn key_count(&self) -> usize {
        self.keys.first().copied().unwrap_or(0) as usize
    }

    /// The identifying members, in declared order.
    pub fn keys(&self) -> impl Iterator<Item = &'a [u8]> + 'a {
        let b = self.keys;
        let mut at = 1usize;
        let mut left = self.key_count();
        core::iter::from_fn(move || {
            if left == 0 {
                return None;
            }
            let n = *b.get(at)? as usize;
            let k = b.get(at + 1..at + 1 + n)?;
            at += 1 + n;
            left -= 1;
            Some(k)
        })
    }
}

fn parse_list_keys(b: &[u8]) -> Result<(), DescriptorError> {
    // n = 0: a set of scalars.
    let n = *b.first().ok_or(DescriptorError::BadBody)? as usize;
    let mut at = 1;
    for _ in 0..n {
        let k = *b.get(at).ok_or(DescriptorError::BadBody)? as usize;
        let key = b.get(at + 1..at + 1 + k).ok_or(DescriptorError::BadBody)?;
        if key.is_empty() || !utf8_is_valid(key) {
            return Err(DescriptorError::BadBody);
        }
        at += 1 + k;
    }
    if at != b.len() {
        return Err(DescriptorError::BadBody);
    }
    Ok(())
}

/// A secondary index a collection declares.
#[derive(Clone, Copy, Debug)]
pub struct Index<'a> {
    pub id: u32,
    pub flags: u8,
    pub state: u8,
    pub name: &'a [u8],
    /// `[n:u8]{[plen:u16][path]}…`
    paths: &'a [u8],
}

impl<'a> Index<'a> {
    pub fn parse(b: &'a [u8]) -> Option<Self> {
        let id = rd_u32(b, 0)?;
        let flags = *b.get(4)?;
        let state = *b.get(5)?;
        let nl = *b.get(6)? as usize;
        let name = b.get(7..7 + nl)?;
        let paths = b.get(7 + nl..)?;
        let n = *paths.first()? as usize;
        if flags & !INDEX_UNIQUE != 0
            || !matches!(state, INDEX_BUILDING | INDEX_READY)
            || name.is_empty()
            || !utf8_is_valid(name)
            || n == 0
            || n > MAX_INDEX_PATHS
        {
            return None;
        }
        let mut rest = paths.get(1..)?;
        for _ in 0..n {
            let (p, r) = split_path(rest).ok()?;
            if p.is_empty() {
                return None;
            }
            rest = r;
        }
        rest.is_empty().then_some(Index {
            id,
            flags,
            state,
            name,
            paths,
        })
    }

    pub fn unique(&self) -> bool {
        self.flags & INDEX_UNIQUE != 0
    }

    pub fn ready(&self) -> bool {
        self.state == INDEX_READY
    }

    /// The indexed paths, in order.
    pub fn paths(&self) -> impl Iterator<Item = Path<'a>> + 'a {
        let mut rest = self.paths.get(1..).unwrap_or(&[]);
        let mut left = self.paths.first().copied().unwrap_or(0) as usize;
        core::iter::from_fn(move || {
            if left == 0 {
                return None;
            }
            let (p, r) = split_path(rest).ok()?;
            rest = r;
            left -= 1;
            Some(p)
        })
    }
}

/// An index entry's body: `[id:u32][flags][state][nlen][name][n]{paths}`.
pub fn encode_index(
    out: &mut [u8],
    id: u32,
    flags: u8,
    state: u8,
    name: &[u8],
    paths: &[&[u8]],
) -> Option<usize> {
    let mut at = 0usize;
    let mut put = |b: &[u8], at: &mut usize| -> Option<()> {
        out.get_mut(*at..*at + b.len())?.copy_from_slice(b);
        *at += b.len();
        Some(())
    };
    put(&id.to_le_bytes(), &mut at)?;
    put(&[flags, state, u8::try_from(name.len()).ok()?], &mut at)?;
    put(name, &mut at)?;
    put(&[u8::try_from(paths.len()).ok()?], &mut at)?;
    for p in paths {
        put(&(p.len() as u16).to_le_bytes(), &mut at)?;
        put(p, &mut at)?;
    }
    Some(at)
}

/// A validated descriptor record.
#[derive(Clone, Copy, Debug)]
pub struct Descriptor<'a> {
    b: &'a [u8],
    id: u32,
    name: &'a [u8],
    /// Where the entries start.
    entries: usize,
    count: u8,
}

impl<'a> Descriptor<'a> {
    /// Validate a record whole: every entry, every path, every generator.
    pub fn parse(b: &'a [u8]) -> Result<Self, DescriptorError> {
        if b.len() > MAX_DESCRIPTOR_LEN {
            return Err(DescriptorError::Overflow);
        }
        let version = rd_u16(b, 0).ok_or(DescriptorError::Truncated)?;
        if version != DESCRIPTOR_V1 {
            return Err(DescriptorError::UnknownVersion);
        }
        let id = rd_u32(b, 2).ok_or(DescriptorError::Truncated)?;
        let nl = *b.get(6).ok_or(DescriptorError::Truncated)? as usize;
        let name = b.get(7..7 + nl).ok_or(DescriptorError::Truncated)?;
        if !name_is_valid(name) {
            return Err(DescriptorError::BadName);
        }
        let count = *b.get(7 + nl).ok_or(DescriptorError::Truncated)?;
        let d = Descriptor {
            b,
            id,
            name,
            entries: 8 + nl,
            count,
        };
        let mut seen = 0u32;
        let mut n = 0usize;
        let mut at = d.entries;
        while at < b.len() {
            let tag = b[at];
            let len = rd_u16(b, at + 1).ok_or(DescriptorError::Truncated)? as usize;
            let body = b
                .get(at + 3..at + 3 + len)
                .ok_or(DescriptorError::Truncated)?;
            if tag == 0 || tag > TAG_LAST {
                return Err(DescriptorError::UnknownTag);
            }
            let once = !matches!(
                tag,
                TAG_ID_PATH
                    | TAG_IDENTITY
                    | TAG_STAMP
                    | TAG_KEEP
                    | TAG_DELETING
                    | TAG_LIST_KEY
                    | TAG_INDEX
                    | TAG_UNOWNED
            );
            if once && seen & (1 << tag) != 0 {
                return Err(DescriptorError::Repeated);
            }
            seen |= 1 << tag;
            check_entry(tag, body)?;
            at += 3 + len;
            n += 1;
        }
        if n != count as usize {
            return Err(DescriptorError::Truncated);
        }
        // A collection at the store's own keys: its key is its id alone
        // (no shard), it keeps no index (its other writers could not keep
        // one), and ownership it tracks lives in the object, spelled as
        // the dialect it carries says.
        if d.codec() == CODEC_PATH_JSON
            && (seen & (1 << TAG_SHARD_PATH) != 0
                || seen & (1 << TAG_INDEX) != 0
                || (seen & (1 << TAG_OWNERSHIP) != 0 && seen & (1 << TAG_OWNERS_TEXT) == 0))
        {
            return Err(DescriptorError::BadBody);
        }
        Ok(d)
    }

    pub fn bytes(&self) -> &'a [u8] {
        self.b
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn name(&self) -> &'a [u8] {
        self.name
    }

    /// `(tag, body)` of every entry, in record order.
    pub fn entries(&self) -> impl Iterator<Item = (u8, &'a [u8])> + 'a {
        let b = self.b;
        let mut at = self.entries;
        let mut left = self.count as usize;
        core::iter::from_fn(move || {
            if left == 0 {
                return None;
            }
            let tag = *b.get(at)?;
            let len = rd_u16(b, at + 1)? as usize;
            let body = b.get(at + 3..at + 3 + len)?;
            at += 3 + len;
            left -= 1;
            Some((tag, body))
        })
    }

    fn first(&self, tag: u8) -> Option<&'a [u8]> {
        self.entries().find(|(t, _)| *t == tag).map(|(_, b)| b)
    }

    fn paths(&self, tag: u8) -> impl Iterator<Item = Path<'a>> + 'a {
        self.entries()
            .filter(move |(t, _)| *t == tag)
            .filter_map(|(_, b)| Path::parse(b).ok())
    }

    fn stamps(&self, tag: u8) -> impl Iterator<Item = Stamp<'a>> + 'a {
        self.entries()
            .filter(move |(t, _)| *t == tag)
            .filter_map(|(_, b)| {
                let (path, rest) = split_path(b).ok()?;
                Some(Stamp {
                    path,
                    generator: Generator::parse(rest).ok()?,
                })
            })
    }

    /// Where the identity lives.
    /// Where the id lives — the first path of a compound id.
    pub fn id_path(&self) -> Path<'a> {
        self.first(TAG_ID_PATH)
            .and_then(|b| Path::parse(b).ok())
            .unwrap_or(Path::trusted(DEFAULT_ID_PATH))
    }

    /// Every id path, in order: one for a plain id, several for a
    /// compound one.
    pub fn id_paths(&self) -> impl Iterator<Item = Path<'a>> + 'a {
        let declared = self.entries().any(|(t, _)| t == TAG_ID_PATH);
        let default = (!declared).then_some(Path::trusted(DEFAULT_ID_PATH));
        self.paths(TAG_ID_PATH).chain(default)
    }

    /// Is the id compound (several id paths)?
    pub fn compound_id(&self) -> bool {
        self.paths(TAG_ID_PATH).nth(1).is_some()
    }

    /// Where the shard key lives, when the collection has one.
    pub fn shard_path(&self) -> Option<Path<'a>> {
        self.first(TAG_SHARD_PATH).and_then(|b| Path::parse(b).ok())
    }

    pub fn identity_paths(&self) -> impl Iterator<Item = Path<'a>> + 'a {
        self.paths(TAG_IDENTITY)
    }

    pub fn keep_paths(&self) -> impl Iterator<Item = Path<'a>> + 'a {
        self.paths(TAG_KEEP)
    }

    pub fn create_stamps(&self) -> impl Iterator<Item = Stamp<'a>> + 'a {
        self.stamps(TAG_STAMP)
    }

    pub fn deleting_stamps(&self) -> impl Iterator<Item = Stamp<'a>> + 'a {
        self.stamps(TAG_DELETING)
    }

    pub fn generate(&self) -> Option<Generate<'a>> {
        let b = self.first(TAG_GENERATE)?;
        let (path, rest) = split_path(b).ok()?;
        Some(Generate {
            path,
            suffix: *rest.first()?,
            alphabet: rest.get(1..)?,
        })
    }

    pub fn guard_path(&self) -> Option<Path<'a>> {
        self.first(TAG_GUARD).and_then(|b| Path::parse(b).ok())
    }

    pub fn list_keys(&self) -> impl Iterator<Item = ListKey<'a>> + 'a {
        self.entries()
            .filter(|(t, _)| *t == TAG_LIST_KEY)
            .filter_map(|(_, b)| {
                let (path, keys) = split_path(b).ok()?;
                Some(ListKey { path, keys })
            })
    }

    /// Where a writer's entry reads the document's version, if declared.
    pub fn version_path(&self) -> Option<Path<'a>> {
        let b = self.first(TAG_OWNERSHIP)?;
        let p = b.get(1..).filter(|p| !p.is_empty())?;
        Path::parse(p).ok()
    }

    /// Fields no writer owns.
    pub fn unowned_paths(&self) -> impl Iterator<Item = Path<'a>> + 'a {
        self.paths(TAG_UNOWNED)
    }

    pub fn ownership(&self) -> bool {
        self.first(TAG_OWNERSHIP)
            .and_then(|b| b.first())
            .is_some_and(|f| f & OWNERSHIP_ON != 0)
    }

    /// `(form, path)`: where answers carry their revision, and as what.
    pub fn revision_path(&self) -> Option<(u8, Path<'a>)> {
        let b = self.first(TAG_REVISION_PATH)?;
        Some((*b.first()?, Path::parse(b.get(1..)?).ok()?))
    }

    /// The collection's secondary indexes.
    pub fn indexes(&self) -> impl Iterator<Item = Index<'a>> + 'a {
        self.entries()
            .filter(|(t, _)| *t == TAG_INDEX)
            .filter_map(|(_, b)| Index::parse(b))
    }

    pub fn max_bytes(&self) -> Option<u32> {
        self.first(TAG_MAX_BYTES).and_then(|b| rd_u32(b, 0))
    }

    /// The ownership dialect's directives, as text (a
    /// [`CODEC_PATH_JSON`] collection that tracks ownership).
    pub fn owners_text(&self) -> Option<&'a [u8]> {
        self.first(TAG_OWNERS_TEXT)
    }

    /// How the collection's documents are stored.
    pub fn codec(&self) -> u8 {
        self.first(TAG_CODEC)
            .and_then(|b| b.first().copied())
            .unwrap_or(CODEC_TREE)
    }
}

fn check_entry(tag: u8, body: &[u8]) -> Result<(), DescriptorError> {
    match tag {
        TAG_REVISION_PATH => {
            let form = *body.first().ok_or(DescriptorError::BadBody)?;
            if !matches!(form, FORM_VALUE | FORM_TEXT) {
                return Err(DescriptorError::BadBody);
            }
            if Path::parse(&body[1..])?.is_empty() {
                return Err(DescriptorError::BadPath);
            }
        }
        TAG_ID_PATH | TAG_SHARD_PATH | TAG_IDENTITY | TAG_KEEP | TAG_GUARD | TAG_UNOWNED => {
            let p = Path::parse(body)?;
            if matches!(tag, TAG_ID_PATH | TAG_SHARD_PATH) && p.is_empty() {
                return Err(DescriptorError::BadPath);
            }
        }
        TAG_STAMP | TAG_DELETING => {
            let (p, rest) = split_path(body)?;
            if p.is_empty() {
                return Err(DescriptorError::BadPath);
            }
            Generator::parse(rest)?;
        }
        TAG_GENERATE => {
            let (p, rest) = split_path(body)?;
            let suffix = *rest.first().ok_or(DescriptorError::BadBody)?;
            let alphabet = rest.get(1..).unwrap_or(&[]);
            if p.is_empty()
                || suffix == 0
                || alphabet.is_empty()
                || alphabet.len() > 64
                || !alphabet.iter().all(|c| c.is_ascii_graphic())
            {
                return Err(DescriptorError::BadBody);
            }
        }
        TAG_LIST_KEY => {
            let (p, rest) = split_path(body)?;
            if p.is_empty() {
                return Err(DescriptorError::BadPath);
            }
            parse_list_keys(rest)?;
        }
        TAG_OWNERSHIP => {
            let flags = *body.first().ok_or(DescriptorError::BadBody)?;
            if flags & !OWNERSHIP_ON != 0 {
                return Err(DescriptorError::BadBody);
            }
            let version = body.get(1..).unwrap_or(&[]);
            if !version.is_empty() && Path::parse(version)?.is_empty() {
                return Err(DescriptorError::BadPath);
            }
        }
        TAG_MAX_BYTES => {
            if body.len() != 4 {
                return Err(DescriptorError::BadBody);
            }
        }
        TAG_CODEC => {
            if !matches!(body, [CODEC_TREE] | [CODEC_PATH_JSON]) {
                return Err(DescriptorError::BadBody);
            }
        }
        TAG_OWNERS_TEXT => {
            if body.is_empty() || !utf8_is_valid(body) {
                return Err(DescriptorError::BadBody);
            }
        }
        TAG_INDEX => {
            Index::parse(body).ok_or(DescriptorError::BadBody)?;
        }
        _ => return Err(DescriptorError::UnknownTag),
    }
    Ok(())
}

/// A collection name: 1..=64 bytes of UTF-8, no NUL.
pub fn name_is_valid(name: &[u8]) -> bool {
    !name.is_empty() && name.len() <= MAX_NAME_LEN && !has_nul(name) && utf8_is_valid(name)
}

/// Builds a descriptor record.
pub struct DescriptorWriter<'a> {
    out: &'a mut [u8],
    pos: usize,
    count_at: usize,
    count: u8,
}

impl<'a> DescriptorWriter<'a> {
    pub fn new(out: &'a mut [u8], id: u32, name: &[u8]) -> Result<Self, DescriptorError> {
        if !name_is_valid(name) {
            return Err(DescriptorError::BadName);
        }
        let mut w = DescriptorWriter {
            out,
            pos: 0,
            count_at: 0,
            count: 0,
        };
        w.put(&DESCRIPTOR_V1.to_le_bytes())?;
        w.put(&id.to_le_bytes())?;
        w.put(&[name.len() as u8])?;
        w.put(name)?;
        w.count_at = w.pos;
        w.put(&[0])?;
        Ok(w)
    }

    fn put(&mut self, b: &[u8]) -> Result<(), DescriptorError> {
        let end = self.pos + b.len();
        let d = self
            .out
            .get_mut(self.pos..end)
            .ok_or(DescriptorError::Overflow)?;
        for (x, y) in d.iter_mut().zip(b) {
            *x = *y;
        }
        self.pos = end;
        Ok(())
    }

    /// Append one entry from its parts, concatenated as its body.
    pub fn entry(&mut self, tag: u8, parts: &[&[u8]]) -> Result<(), DescriptorError> {
        let len: usize = parts.iter().map(|p| p.len()).sum();
        let len16 = u16::try_from(len).map_err(|_| DescriptorError::Overflow)?;
        if self.count == u8::MAX {
            return Err(DescriptorError::Overflow);
        }
        self.put(&[tag])?;
        self.put(&len16.to_le_bytes())?;
        for p in parts {
            self.put(p)?;
        }
        self.count += 1;
        Ok(())
    }

    /// An entry whose body is one path.
    pub fn path(&mut self, tag: u8, path: &[u8]) -> Result<(), DescriptorError> {
        self.entry(tag, &[path])
    }

    /// A `STAMP` or `DELETING` entry.
    pub fn stamp(
        &mut self,
        tag: u8,
        path: &[u8],
        kind: u8,
        form: u8,
        literal: &[u8],
    ) -> Result<(), DescriptorError> {
        let pl = (path.len() as u16).to_le_bytes();
        self.entry(tag, &[&pl, path, &[kind, form], literal])
    }

    pub fn generate(
        &mut self,
        path: &[u8],
        suffix: u8,
        alphabet: &[u8],
    ) -> Result<(), DescriptorError> {
        let pl = (path.len() as u16).to_le_bytes();
        self.entry(TAG_GENERATE, &[&pl, path, &[suffix], alphabet])
    }

    /// A `LIST_KEY` entry; `keys` is `[n:u8]{[klen:u8][member]}…`.
    pub fn list_key(&mut self, path: &[u8], keys: &[u8]) -> Result<(), DescriptorError> {
        let pl = (path.len() as u16).to_le_bytes();
        self.entry(TAG_LIST_KEY, &[&pl, path, keys])
    }

    /// Close the record; it is validated before it is answered.
    pub fn finish(self) -> Result<usize, DescriptorError> {
        let n = self.pos;
        *self
            .out
            .get_mut(self.count_at)
            .ok_or(DescriptorError::Overflow)? = self.count;
        Descriptor::parse(self.out.get(..n).ok_or(DescriptorError::Overflow)?)?;
        Ok(n)
    }
}

/// `rec` with one more entry `(tag, body)` appended, into `out`.
pub fn with_entry(
    rec: &[u8],
    tag: u8,
    body: &[u8],
    out: &mut [u8],
) -> Result<usize, DescriptorError> {
    let nl = *rec.get(6).ok_or(DescriptorError::Truncated)? as usize;
    let count = *rec.get(7 + nl).ok_or(DescriptorError::Truncated)?;
    let n = rec.len() + 3 + body.len();
    if n > MAX_DESCRIPTOR_LEN || count == u8::MAX {
        return Err(DescriptorError::Overflow);
    }
    let len = u16::try_from(body.len()).map_err(|_| DescriptorError::Overflow)?;
    let o = out.get_mut(..n).ok_or(DescriptorError::Overflow)?;
    o[..rec.len()].copy_from_slice(rec);
    o[7 + nl] = count + 1;
    o[rec.len()] = tag;
    o[rec.len() + 1..rec.len() + 3].copy_from_slice(&len.to_le_bytes());
    o[rec.len() + 3..].copy_from_slice(body);
    Descriptor::parse(o)?;
    Ok(n)
}

/// `rec` without its index `id`, into `out`.
pub fn without_index(rec: &[u8], id: u32, out: &mut [u8]) -> Result<usize, DescriptorError> {
    let nl = *rec.get(6).ok_or(DescriptorError::Truncated)? as usize;
    let head = 8 + nl;
    let o = out.get_mut(..rec.len()).ok_or(DescriptorError::Overflow)?;
    o[..head].copy_from_slice(rec.get(..head).ok_or(DescriptorError::Truncated)?);
    let (mut at, mut w, mut count) = (head, head, 0u8);
    while at + 3 <= rec.len() {
        let tag = rec[at];
        let len = rd_u16(rec, at + 1).ok_or(DescriptorError::Truncated)? as usize;
        let end = at + 3 + len;
        let entry = rec.get(at..end).ok_or(DescriptorError::Truncated)?;
        let drop = tag == TAG_INDEX && rd_u32(entry, 3) == Some(id);
        if !drop {
            o[w..w + entry.len()].copy_from_slice(entry);
            w += entry.len();
            count += 1;
        }
        at = end;
    }
    o[7 + nl] = count;
    Descriptor::parse(&o[..w])?;
    Ok(w)
}

/// Set index `id`'s state in place.
pub fn set_index_state(rec: &mut [u8], id: u32, state: u8) -> Result<(), DescriptorError> {
    let nl = *rec.get(6).ok_or(DescriptorError::Truncated)? as usize;
    let mut at = 8 + nl;
    while at + 3 <= rec.len() {
        let tag = rec[at];
        let len = rd_u16(rec, at + 1).ok_or(DescriptorError::Truncated)? as usize;
        if tag == TAG_INDEX && rd_u32(rec, at + 3) == Some(id) {
            *rec.get_mut(at + 3 + 5).ok_or(DescriptorError::Truncated)? = state;
            return Ok(());
        }
        at += 3 + len;
    }
    Err(DescriptorError::BadBody)
}

/// Number the record's indexes `first`, `first + 1`, … in record order
/// (a create allocates the collection's id and its indexes' together).
pub fn set_index_ids(rec: &mut [u8], first: u32) -> Result<(), DescriptorError> {
    let nl = *rec.get(6).ok_or(DescriptorError::Truncated)? as usize;
    let mut at = 8 + nl;
    let mut next = first;
    while at + 3 <= rec.len() {
        let tag = rec[at];
        let len = rd_u16(rec, at + 1).ok_or(DescriptorError::Truncated)? as usize;
        if tag == TAG_INDEX {
            let d = rec
                .get_mut(at + 3..at + 7)
                .ok_or(DescriptorError::Truncated)?;
            d.copy_from_slice(&next.to_le_bytes());
            next = next.checked_add(1).ok_or(DescriptorError::Overflow)?;
        }
        at += 3 + len;
    }
    Ok(())
}

/// Rewrite a descriptor's id (a create learns it from the allocator
/// after the dialect built the record).
pub fn set_descriptor_id(rec: &mut [u8], id: u32) -> Result<(), DescriptorError> {
    let d = rec.get_mut(2..6).ok_or(DescriptorError::Truncated)?;
    for (x, y) in d.iter_mut().zip(id.to_le_bytes()) {
        *x = y;
    }
    Ok(())
}

// ── Catalog keys ──────────────────────────────────────────────────────

/// `[db:u32 BE][0x00]`: the collection-id allocator.
pub fn catalog_seq_key(out: &mut [u8], db: u32) -> Option<usize> {
    let d = out.get_mut(..5)?;
    for (x, y) in d.iter_mut().zip(db.to_be_bytes()) {
        *x = y;
    }
    d[4] = CAT_KIND_SEQ;
    Some(5)
}

/// `[db:u32 BE][0x01][name]`: a collection's descriptor.
pub fn catalog_name_key(out: &mut [u8], db: u32, name: &[u8]) -> Option<usize> {
    if !name_is_valid(name) {
        return None;
    }
    let n = catalog_names_prefix(out, db)?;
    let d = out.get_mut(n..n + name.len())?;
    for (x, y) in d.iter_mut().zip(name) {
        *x = *y;
    }
    Some(n + name.len())
}

/// `[db:u32 BE][0x01]`: every descriptor of one database.
pub fn catalog_names_prefix(out: &mut [u8], db: u32) -> Option<usize> {
    let d = out.get_mut(..5)?;
    for (x, y) in d.iter_mut().zip(db.to_be_bytes()) {
        *x = y;
    }
    d[4] = CAT_KIND_NAME;
    Some(5)
}
