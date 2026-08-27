//! Generates Rust types from `proto/coordinator.proto` at build time (pure-Rust `protox`, no
//! system `protoc`), exactly as sibling module crates do.
//!
//! `proto/coordinator.proto` imports the sibling `ntk-proto` crate's own `proto/ntk.proto`
//! purely for `ntk.rpc.v1.TypedValue`, the opaque payload type this module's own
//! request/response envelopes carry. The include path to `ntk-proto`'s `proto/` directory comes
//! from the `DEP_NTK_PROTO_PROTO_INCLUDE` env var, which cargo sets from `ntk-proto`'s
//! `links = "ntk_proto"` build-script metadata (`cargo:proto_include=...` in
//! `ntk-proto/build.rs`), not from a hardcoded `../ntk-proto/proto` relative path. A relative
//! sibling path only resolves inside this workspace's checkout; `cargo package` extracts each
//! crate standalone with no sibling directory present, which broke `cargo publish --dry-run`
//! before this fix. Cargo only forwards `DEP_*` vars to build scripts of crates that directly
//! depend on the `links` crate, which this crate does.
//! `.extern_path` tells `prost-build` to reference `ntk_proto::v1`'s already-generated types for
//! the `ntk.rpc.v1` package instead of generating a second, incompatible copy.

fn main() {
    let proto_file = "proto/coordinator.proto";
    let ntk_proto_include = std::env::var("DEP_NTK_PROTO_PROTO_INCLUDE")
        .expect("ntk-proto sets DEP_NTK_PROTO_PROTO_INCLUDE via its `links` build metadata");
    println!("cargo:rerun-if-changed={proto_file}");
    println!("cargo:rerun-if-changed={ntk_proto_include}/ntk.proto");

    let file_descriptor_set = protox::Compiler::new(["proto", &ntk_proto_include])
        .expect("proto include dirs are valid")
        .include_source_info(true)
        // `ntk.rpc.v1.TypedValue` fields are singular (non-repeated) message fields, so
        // prost-build's Copy/Eq-derivability check (`Context::can_message_derive_copy`)
        // recursively looks up their message descriptor even though `extern_path` skips
        // codegen for it — that descriptor must still be present in the compiled set.
        .include_imports(true)
        .open_file(proto_file)
        .expect("proto/coordinator.proto parses")
        .file_descriptor_set();

    prost_build::Config::new()
        .extern_path(".ntk.rpc.v1", "::ntk_proto::v1")
        .compile_fds(file_descriptor_set)
        .expect("generate Rust types from proto/coordinator.proto");
}
