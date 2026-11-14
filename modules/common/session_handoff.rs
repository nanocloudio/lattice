//! session_handoff — the single mount of Fluxor's `session_handoff`
//! SDK core for this tree (chunked opaque export/import with CRC32,
//! and the delivery-cursor pair with its admission check).
//!
//! Re-exported from `fluxor_sdk`. Consumers reach it as
//! `session_handoff::{HandoffExport, HandoffImport, SessionCursors,
//! cursors_admit, …}` through whichever core mounts this file.

pub use fluxor_sdk::cores::session_handoff::*;
