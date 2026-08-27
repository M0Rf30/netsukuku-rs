//! Generates Rust types from `proto/identities.proto` at build time.
//!
//! Mirrors `ntk-proto/build.rs` exactly: `protox` (pure-Rust protobuf
//! parser) instead of shelling out to a system `protoc`, per
//! `research/notes/06-rust-stack.md`'s "RPC wire format" verdict.

fn main() {
    let proto_file = "proto/identities.proto";
    println!("cargo:rerun-if-changed={proto_file}");

    let file_descriptor_set = protox::Compiler::new(["proto"])
        .expect("proto/ include path is valid")
        .include_source_info(true)
        .open_file(proto_file)
        .expect("proto/identities.proto parses")
        .file_descriptor_set();

    prost_build::Config::new()
        .compile_fds(file_descriptor_set)
        .expect("generate Rust types from proto/identities.proto");
}
