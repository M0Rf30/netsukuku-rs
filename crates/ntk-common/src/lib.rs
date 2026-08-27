//! Shared base types for `netsukuku-rs`, the Rust analogue of Vala's
//! `ntkd-common` (`research/impl/vala/ntkd-common/`).
//!
//! This crate carries no protocol logic — no RPC, no transport, no netlink, no
//! QSPN algorithm — only the vocabulary every other crate builds on:
//!
//! - [`Topology`] — a network's shape (levels and per-level g-node size).
//! - [`Naddr`] — a hierarchical address bound to a [`Topology`].
//! - [`HCoord`] — a bare (level, position) coordinate.
//! - [`Fingerprint`] — g-node identity and eldership for QSPN split/merge
//!   detection, generic over an opaque identity type.
//! - [`Cost`] — the ETP path cost metric.
//! - [`Error`] — the crate-wide error type.
//!
//! Depends on no sibling `ntk-*` crate.

mod cost;
mod error;
mod fingerprint;
mod hcoord;
mod naddr;
mod topology;

pub use cost::Cost;
pub use error::Error;
pub use fingerprint::{Fingerprint, FingerprintParts};
pub use hcoord::HCoord;
pub use naddr::Naddr;
pub use topology::Topology;
