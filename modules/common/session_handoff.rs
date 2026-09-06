//! session_handoff — the single mount of Fluxor's `session_handoff`
//! SDK core for this tree (chunked opaque export/import with CRC32,
//! and the delivery-cursor pair with its admission check).
//!
//! The SDK core is written as bare items, so it is `include!`d rather
//! than `#[path]`-mounted. Consumers reach it as
//! `session_handoff::{HandoffExport, HandoffImport, SessionCursors,
//! cursors_admit, …}` through whichever core mounts this file.

#![allow(
    dead_code,
    reason = "platform core included wholesale; each consumer uses a subset"
)]

include!("../../target/fluxor/fluxor-abi/sdk/cores/session_handoff.rs");
