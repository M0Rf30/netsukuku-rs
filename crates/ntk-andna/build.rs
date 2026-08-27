//! Generates Rust types from `proto/andna.proto` at build time, exactly as
//! `ntk-peerservices/build.rs` does (pure-Rust `protox`, no system `protoc`).
//!
//! `proto/andna.proto` imports `domain.proto` from the sibling `ntk-proto` crate. The include
//! path to `ntk-proto`'s `proto/` directory comes from the `DEP_NTK_PROTO_PROTO_INCLUDE` env
//! var, which cargo sets from `ntk-proto`'s `links = "ntk_proto"` build-script metadata
//! (`cargo:proto_include=...` in `ntk-proto/build.rs`), not from a hardcoded
//! `../ntk-proto/proto` relative path. A relative sibling path only resolves inside this
//! workspace's checkout; `cargo package` extracts each crate standalone with no sibling
//! directory present, which broke `cargo publish --dry-run` before this fix. Cargo only forwards
//! `DEP_*` vars to build scripts of crates that directly depend on the `links` crate, which this
//! crate does.
//! `.extern_path` tells `prost-build` to reference `ntk_proto::domain::v1`'s
//! already-generated types for the `ntk.domain.v1` package instead of generating a second,
//! incompatible copy.
//!
//! Every field of the extern-mapped `ntk.domain.v1.Naddr` type in `proto/andna.proto` is
//! `repeated` (holding exactly one element in practice) rather than singular — see
//! `AndnaAddress`'s doc comment in that file. A singular extern-mapped message field trips a
//! `prost-build` 0.14 panic in its Copy/Eq auto-derive analysis (`can_message_derive_copy`/
//! `can_message_derive_eq` unconditionally `.unwrap()` the extern type's own descriptor out of a
//! message graph that never contains it, since the type is only referenced via `.extern_path`,
//! never compiled here); `repeated` fields short-circuit that analysis before it ever needs the
//! extern descriptor, exactly like `ntk-peerservices/proto/peerservices.proto`'s own `repeated
//! ntk.domain.v1.HCoord` field already does.

fn main() {
    let proto_file = "proto/andna.proto";
    let domain_include = std::env::var("DEP_NTK_PROTO_PROTO_INCLUDE")
        .expect("ntk-proto sets DEP_NTK_PROTO_PROTO_INCLUDE via its `links` build metadata");
    println!("cargo:rerun-if-changed={proto_file}");
    println!("cargo:rerun-if-changed={domain_include}/domain.proto");

    let file_descriptor_set = protox::Compiler::new(["proto", &domain_include])
        .expect("proto include dirs are valid")
        .include_source_info(true)
        .open_file(proto_file)
        .expect("proto/andna.proto parses")
        .file_descriptor_set();

    prost_build::Config::new()
        .extern_path(".ntk.domain.v1", "::ntk_proto::domain::v1")
        .compile_fds(file_descriptor_set)
        .expect("generate Rust types from proto/andna.proto");
}
