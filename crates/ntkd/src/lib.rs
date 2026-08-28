//! `ntkd`: the Netsukuku routing daemon. Composition root wiring the eleven `ntk-*` library
//! crates into a running node; see `research/notes/02-vala-services-daemon.md` §5 for the
//! upstream daemon this replaces.
//!
//! A library target (with `src/main.rs` as a thin `fn main` shim over it)
//! exists so integration tests can `use ntkd::...` directly instead of re-including source
//! files with `#[path]` — the previous approach compiled `kernel`/`node` as an entirely separate
//! crate per test binary, which made every test-only accessor (e.g.
//! `kernel::routes::RouteInstaller::kernel_ref`) look dead to `cargo clippy`'s per-crate
//! dead-code analysis when only the bin target was checked.

pub mod kernel;

pub mod node;
