//! Generates Rust types from `proto/hooking.proto` at build time. Unlike
//! sibling protocol crates, `hooking.proto` does not import `ntk-proto`'s
//! `domain.proto`: none of Hooking's own wire payloads (`TupleGNode` and
//! friends, `research/impl/vala/hooking/serializables.vala`) are a
//! validated `Naddr`/`HCoord`/`Fingerprint`/`Cost` — `TupleGNode` is a
//! *relative* position/eldership tuple private to the migration-path search
//! algorithm — so there is nothing here to map through `extern_path`.

fn main() {
    let proto_file = "proto/hooking.proto";
    println!("cargo:rerun-if-changed={proto_file}");

    let file_descriptor_set = protox::Compiler::new(["proto"])
        .expect("proto include path is valid")
        .include_source_info(true)
        .include_imports(true)
        .open_file(proto_file)
        .expect("proto/hooking.proto parses")
        .file_descriptor_set();

    prost_build::Config::new()
        .compile_fds(file_descriptor_set)
        .expect("generate Rust types from proto/hooking.proto");
}
