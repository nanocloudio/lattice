//! Build script for Lattice.
//!
//! Currently a no-op placeholder. The etcd v3 API types are implemented
//! directly in Rust (see src/adapters/etcd/) rather than generated from
//! protobuf definitions.
//!
//! This approach was chosen because:
//! - It avoids proto file dependencies and build-time codegen complexity
//! - It allows for Lattice-specific type customizations
//! - The etcd API surface used by Lattice is a stable subset
//!
//! If proto-based codegen is needed in the future, tonic-build can be
//! configured here to compile proto files from a `proto/` directory.

fn main() {
    // Rerun if build.rs changes
    println!("cargo:rerun-if-changed=build.rs");
}
