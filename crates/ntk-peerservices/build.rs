//! Generates Rust types from `proto/peerservices.proto` at build time, exactly as
//! `ntk-proto/build.rs` does (pure-Rust `protox`, no system `protoc`).
//!
//! `proto/peerservices.proto` imports `domain.proto` and `ntk.proto` (for `ntk.rpc.v1.Auth`,
//! `PeerMessageForwarder`'s origin-auth field) from the sibling `ntk-proto` crate. The include
//! path to `ntk-proto`'s `proto/` directory comes from the `DEP_NTK_PROTO_PROTO_INCLUDE` env
//! var, which cargo sets from `ntk-proto`'s `links = "ntk_proto"` build-script metadata
//! (`cargo:proto_include=...` in `ntk-proto/build.rs`), not from a hardcoded
//! `../ntk-proto/proto` relative path. A relative sibling path only resolves inside this
//! workspace's checkout; `cargo package` extracts each crate standalone with no sibling
//! directory present, which broke `cargo publish --dry-run` before this fix. Cargo only forwards
//! `DEP_*` vars to build scripts of crates that directly depend on the `links` crate, which this
//! crate does.
//! `.extern_path` tells `prost-build` to reference `ntk_proto::domain::v1`'s/`ntk_proto::v1`'s
//! already-generated types for the `ntk.domain.v1`/`ntk.rpc.v1` packages instead of generating a
//! second, incompatible copy.

fn main() {
    let proto_file = "proto/peerservices.proto";
    let domain_include = std::env::var("DEP_NTK_PROTO_PROTO_INCLUDE")
        .expect("ntk-proto sets DEP_NTK_PROTO_PROTO_INCLUDE via its `links` build metadata");
    println!("cargo:rerun-if-changed={proto_file}");
    println!("cargo:rerun-if-changed={domain_include}/domain.proto");
    println!("cargo:rerun-if-changed={domain_include}/ntk.proto");

    let file_descriptor_set = protox::Compiler::new(["proto", &domain_include])
        .expect("proto include dirs are valid")
        .include_source_info(true)
        // `ntk.rpc.v1.Auth` is a singular (non-repeated) message field on
        // `PeerMessageForwarder`, so prost-build's Copy/Eq-derivability check recursively looks
        // up its message descriptor even though `extern_path` skips codegen for it — that
        // descriptor must still be present in the compiled set (`ntk-coordinator/build.rs`'s own
        // doc explains this in more depth for its own `ntk.rpc.v1.TypedValue` fields).
        .include_imports(true)
        .open_file(proto_file)
        .expect("proto/peerservices.proto parses")
        .file_descriptor_set();

    prost_build::Config::new()
        .extern_path(".ntk.domain.v1", "::ntk_proto::domain::v1")
        .extern_path(".ntk.rpc.v1", "::ntk_proto::v1")
        .compile_fds(file_descriptor_set)
        .expect("generate Rust types from proto/peerservices.proto");
}
