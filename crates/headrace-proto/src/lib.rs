//! Checked-in gRPC stubs for Headrace's admin services.
//!
//! The generated code under `src/gen/` is produced by `headrace-proto-gen`
//! (`cargo run -p headrace-proto-gen`) and committed, so the normal build never needs
//! `protoc`. Edit `proto/*.proto` and regenerate; do not hand-edit the generated files.

/// The `headrace.v1` package: the `State` inspection service (ADR-0014).
#[allow(clippy::all, clippy::pedantic, clippy::nursery, missing_docs)]
#[rustfmt::skip]
pub mod v1 {
    include!("gen/headrace.v1.rs");
}

/// Serialized `FileDescriptorSet` for the `headrace.v1` protos, for gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("gen/state_descriptor.bin");
