//! Generates Rust types from `proto/qspn.proto` at build time.
//!
//! `qspn.proto` imports `ntk-proto`'s `domain.proto` for
//! `Naddr`/`HCoord`/`Fingerprint`/`Cost` rather than redeclaring them. Those
//! types are already compiled and exposed at `ntk_proto::domain::v1`.
//! `extern_path` maps every reference to `ntk.domain.v1` onto that path;
//! `domain.proto`'s own `FileDescriptorProto` is still passed to
//! `compile_fds` (prost-build needs it in the message graph to answer
//! `Copy`/`Eq`-derivability questions about `qspn.proto`'s own
//! message-typed fields), but `prost-build` itself skips emitting content
//! for any message whose fully-qualified name resolves via `extern_path`
//! (`append_message`'s "Skip external types" check), so no duplicate
//! `ntk.domain.v1` module is produced here.
//!
//! The include path to `ntk-proto`'s `proto/` directory comes from the
//! `DEP_NTK_PROTO_PROTO_INCLUDE` env var, which cargo sets from `ntk-proto`'s
//! `links = "ntk_proto"` build-script metadata (`cargo:proto_include=...` in
//! `ntk-proto/build.rs`), not from a hardcoded `../ntk-proto/proto` relative
//! path. A relative sibling path only resolves inside this workspace's
//! checkout; `cargo package` extracts each crate standalone with no sibling
//! directory present, which broke `cargo publish --dry-run` before this
//! fix. Cargo only forwards `DEP_*` vars to build scripts of crates that
//! directly depend on the `links` crate, which this crate does.

fn main() {
    let proto_file = "proto/qspn.proto";
    println!("cargo:rerun-if-changed={proto_file}");

    let domain_proto_dir = std::env::var("DEP_NTK_PROTO_PROTO_INCLUDE")
        .expect("ntk-proto sets DEP_NTK_PROTO_PROTO_INCLUDE via its `links` build metadata");
    println!("cargo:rerun-if-changed={domain_proto_dir}/domain.proto");

    let file_descriptor_set = protox::Compiler::new(["proto", domain_proto_dir.as_str()])
        .expect("proto include paths are valid")
        .include_source_info(true)
        .include_imports(true)
        .open_file(proto_file)
        .expect("proto/qspn.proto parses")
        .file_descriptor_set();

    prost_build::Config::new()
        .extern_path(".ntk.domain.v1", "::ntk_proto::domain::v1")
        .compile_fds(file_descriptor_set)
        .expect("generate Rust types from proto/qspn.proto");
}
