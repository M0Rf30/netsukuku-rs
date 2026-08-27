//! Generates Rust types from `proto/ntk.proto` and `proto/domain.proto` at
//! build time, compiled together so `domain.proto`'s messages are visible to
//! `ntk.proto` on import (and vice versa, if ever needed).
//!
//! Uses `protox` (a pure-Rust protobuf parser) instead of `prost-build`'s
//! default `protoc`-shelling-out path, so the workspace never needs a system
//! `protoc` binary — a deliberate choice for cross-compiling to
//! embedded/router-class targets (research/notes/06-rust-stack.md, "RPC wire
//! format" verdict).
//!
//! Publishes its own `proto/` directory as build-script metadata (`cargo:proto_include=`),
//! which cargo's `links = "ntk_proto"` (see Cargo.toml) propagates to any directly-dependent
//! crate's build script as the `DEP_NTK_PROTO_PROTO_INCLUDE` env var. Sibling module crates
//! (`ntk-qspn`, `ntk-peerservices`, `ntk-andna`, `ntk-coordinator`) read that var instead of
//! constructing `../ntk-proto/proto` by hand: a hardcoded sibling path only works inside this
//! workspace's checkout, and breaks the moment `cargo package` extracts a crate standalone into
//! `target/package/<crate>-<version>/` with no `ntk-proto/` next to it. `CARGO_MANIFEST_DIR`
//! here always resolves correctly (in-workspace or from an extracted registry tarball) because
//! `proto/` lives inside this crate's own directory.

fn main() {
    let proto_files = ["proto/ntk.proto", "proto/domain.proto"];
    for proto_file in proto_files {
        println!("cargo:rerun-if-changed={proto_file}");
    }

    let proto_include = concat!(env!("CARGO_MANIFEST_DIR"), "/proto");
    println!("cargo:proto_include={proto_include}");

    let file_descriptor_set = protox::Compiler::new(["proto"])
        .expect("proto/ include path is valid")
        .include_source_info(true)
        .open_files(proto_files)
        .expect("proto/*.proto parse")
        .file_descriptor_set();

    prost_build::Config::new()
        .compile_fds(file_descriptor_set)
        .expect("generate Rust types from proto/*.proto");
}
