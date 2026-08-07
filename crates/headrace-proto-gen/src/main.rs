//! Regenerates the checked-in gRPC stubs in the `headrace-proto` crate.
//!
//! Run from anywhere in the workspace: `cargo run -p headrace-proto-gen`. Requires
//! `protoc` on PATH - but only here, at dev time. The normal build compiles the
//! checked-in output, so `cargo build` and the release image never need `protoc`.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // This crate lives beside the stubs crate under `crates/`; write into it.
    let crates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate is under crates/")
        .to_path_buf();
    let proto_crate = crates.join("headrace-proto");
    let proto_dir = proto_crate.join("proto");
    let out_dir = proto_crate.join("src/gen");
    std::fs::create_dir_all(&out_dir)?;

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("state_descriptor.bin"))
        .out_dir(&out_dir)
        .compile_protos(&[proto_dir.join("state.proto")], &[proto_dir])?;

    println!("generated stubs in {}", out_dir.display());
    Ok(())
}
