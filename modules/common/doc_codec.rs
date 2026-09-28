//! A collection's records as the collection stores them
//! (`doc_catalog::TAG_CODEC`).
//!
//! The engine works on a document as a tree and its ownership as a record
//! (`doc_fields::Owners`). A [`CODEC_TREE`] collection stores exactly that
//! (`doc_tree`'s `FORMAT_V1` / `FORMAT_OWNED` records, under its own
//! keyspace). A [`CODEC_PATH_JSON`] collection stores what a store's other
//! clients read and write at the same key: the document as JSON text, its
//! ownership entries inside it where the schema keeps them (Kubernetes'
//! `metadata.managedFields`), spelled as the dialect the descriptor carries
//! says. So a controller's GET and the engine's read are one value, and a
//! write by either is the other's next read.
//!
//! Ownership entries a store's other writer wrote in a spelling the dialect
//! cannot read are not the engine's: the document reads with none, and the
//! engine's next write records its own (`doc_render::split_owners`).
//!
//! Mounted beside `doc_exec`'s modules, `doc_render` and `json_tree`
//! (reached as `super::…`).

use super::doc_catalog::{Descriptor, CODEC_PATH_JSON, CODEC_TREE};
use super::doc_fields::Owners;
use super::doc_ops::Keyed;
use super::doc_render::{
    owners_dialect, split_owners, with_owners, Out, OwnersBufs, OwnersDialect,
};
use super::doc_tree::{Value, Writer};
use super::json_tree;
use super::Directives;

/// The buffers the JSON codec works in: a tree the size of a document, the
/// ownership entries' own (see [`OwnersBufs`]), and a field path.
pub struct CodecBufs<'b> {
    pub tree: &'b mut [u8],
    pub own: OwnersBufs<'b>,
    pub path: &'b mut [u8],
}

/// Does the collection store JSON text at the store's own keys?
pub fn is_json(desc: &Descriptor<'_>) -> bool {
    desc.codec() == CODEC_PATH_JSON
}

/// Does the collection store the engine's own records?
pub fn is_tree(desc: &Descriptor<'_>) -> bool {
    desc.codec() == CODEC_TREE
}

/// The dialect a JSON collection keeps its ownership in.
pub fn dialect<'d>(desc: &Descriptor<'d>) -> Option<OwnersDialect<'d>> {
    owners_dialect(desc.owners_text()?)
}

/// A document (and, when the collection tracks it, its ownership) as the
/// JSON text a [`CODEC_PATH_JSON`] collection stores, into `out`: its
/// length. A record with no entries writes none (the object as its other
/// writers leave it). `None` when it does not fit, or cannot be written as
/// JSON.
pub fn encode_json(
    desc: &Descriptor<'_>,
    doc: Value<'_>,
    owners: Option<Owners<'_>>,
    b: &mut CodecBufs<'_>,
    out: &mut [u8],
) -> Option<usize> {
    let obj = match (owners, dialect(desc)) {
        (Some(o), Some(d)) if desc.ownership() && o.entries().next().is_some() => {
            let mut w = Writer::new(b.tree);
            with_owners(doc, o, &d, &mut b.own, &mut w).ok()?;
            let n = w.finish().ok()?;
            Value::parse(b.tree.get(..n)?).ok()?
        }
        _ => doc,
    };
    let mut o = Out::new(out);
    o.json(obj).ok()?;
    Some(o.len())
}

/// A stored [`CODEC_PATH_JSON`] value read back: the document's tree into
/// `doc_out` and its ownership record into `owners_out` (none — length 0 —
/// when the collection keeps none): `(doc length, owners length)`. `None`
/// for a value that is not a JSON object, or does not fit.
pub fn decode_json(
    desc: &Descriptor<'_>,
    text: &[u8],
    b: &mut CodecBufs<'_>,
    doc_out: &mut [u8],
    owners_out: &mut [u8],
) -> Option<(usize, usize)> {
    let n = json_tree::parse(text, b.tree).ok()?;
    let obj = Value::parse(b.tree.get(..n)?).ok()?;
    if obj.ty() != super::doc_tree::T_OBJECT {
        return None;
    }
    match dialect(desc) {
        Some(d) if desc.ownership() => {
            let keyed = Keyed {
                desc: *desc,
                dir: Directives::NONE,
            };
            let mut w = Writer::new(doc_out);
            let on = split_owners(obj, &d, &keyed, b.path, &mut w, owners_out).ok()?;
            Some((w.finish().ok()?, on))
        }
        _ => {
            let bytes = obj.bytes();
            let dst = doc_out.get_mut(..bytes.len())?;
            for (x, y) in dst.iter_mut().zip(bytes) {
                *x = *y;
            }
            Some((bytes.len(), 0))
        }
    }
}
