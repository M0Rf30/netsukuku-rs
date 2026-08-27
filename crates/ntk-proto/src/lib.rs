//! Wire schema for the netsukuku-rs inter-node RPC protocol.
//!
//! This crate is generated-code-plus-glue: [`v1`] holds the `prost`-generated
//! types compiled from `proto/ntk.proto` at build time (see `build.rs`);
//! [`domain`] holds the shared domain vocabulary compiled from
//! `proto/domain.proto` plus the `ntk-common` <-> wire conversions every
//! phase-2 protocol module codes against. The rest of this crate adds the
//! handful of things codegen cannot produce — envelope construction/
//! inspection helpers and a [`v1::ProtocolVersion`] compatibility check.
//!
//! See `proto/ntk.proto` for the full method surface and the design
//! rationale for each message (in particular [`v1::TypedValue`], the
//! replacement for zcd's `{typename, value}` polymorphic envelope), and
//! [`domain`] for the `TypedValue` encode/decode helpers module payload
//! types use to travel inside it.

/// Generated protobuf types for protocol version 1 (`proto/ntk.proto`,
/// package `ntk.rpc.v1`). Doc comments on individual messages/fields are
/// copied from the `.proto` source by `prost-build`.
#[allow(clippy::doc_markdown, clippy::large_enum_variant)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/ntk.rpc.v1.rs"));
}

pub mod auth;
pub mod domain;

mod envelope;

pub use envelope::VersionMismatch;
