//! The half of `ntkd` that talks to the machine: parsing the on-disk config file, probing the
//! running kernel's routing capabilities, and translating QSPN's routing decisions into netlink
//! state. Its sibling, `crate::node`, is the protocol composition half (spawning and wiring the
//! `ntk-*` actor crates together); `kernel` knows nothing about RPC, arcs, or peer discovery — it
//! only knows NIP↔IPv4 addressing, `ntk_common::Topology`, and `ntk_netlink`.

pub mod addressing;
pub mod config;
pub mod preflight;
pub mod routes;
