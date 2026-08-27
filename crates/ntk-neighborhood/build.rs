//! Generates Rust types from `proto/neighborhood.proto` at build time, using
//! `protox` exactly as `ntk-proto/build.rs` does (no system `protoc`).

fn main() {
    let proto_file = "proto/neighborhood.proto";
    println!("cargo:rerun-if-changed={proto_file}");

    let file_descriptor_set = protox::Compiler::new(["proto"])
        .expect("proto/ include path is valid")
        .include_source_info(true)
        .open_file(proto_file)
        .expect("proto/neighborhood.proto parses")
        .file_descriptor_set();

    prost_build::Config::new()
        .compile_fds(file_descriptor_set)
        .expect("generate Rust types from proto/neighborhood.proto");
}
