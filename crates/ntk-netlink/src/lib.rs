//! Native kernel-state manipulation for Netsukuku's L3 routing daemon.
//!
//! Upstream's `ntkd` drives the kernel exclusively by shelling out to
//! `ip`(8)/`iptables`(8)/`sysctl`(8) (`ntkd/identity_ip_commands.vala`,
//! `research/notes/02-vala-services-daemon.md` §5) and recovers from a
//! crash by regex-scraping those same tools' text output
//! (`ntkd/cleaning/cleaning.vala`). This crate replaces both with real
//! netlink requests over [`rtnetlink`]/`netlink-packet-route`.
//!
//! # Scope (phase 1)
//! - IPv4 address management ([`AddressTable`]) — `ip address add|del|show`.
//! - Routing-table entries ([`RouteTable`]), including multipath/ECMP —
//!   `ip route add|change|del|show table <t>`.
//! - Policy-routing rules ([`RuleTable`]) — `ip rule add|del|show`.
//! - Link and neighbour-cache introspection ([`TopologyQuery`]) — `ip link
//!   show`, `ip neighbour show`.
//! - Numbered routing-table/rule-priority allocation ([`TableAllocator`]).
//! - Kernel-feature preflight ([`detect_capabilities`]).
//! - Crash-recovery ([`cleanup`]), scoped to exactly what the above traits
//!   can create (see that module's documentation for the precise ownership
//!   rule per object kind).
//!
//! **Explicitly out of scope**: TUN devices (Netsukuku is an L3 routing
//! protocol, not a TUN overlay — `research/README.md`) and `iptables`/NAT
//! rule manipulation (no evaluated native crate, and the anonymizing-address
//! feature that needs it is deferred — `research/notes/06-rust-stack.md`
//! open question 5). Neither appears anywhere in this crate's API.
//!
//! # Two implementations, one trait seam
//! [`RealNetlink`] is the production backend. [`FakeNetlink`] is an
//! in-memory, non-privileged recording implementation for upper-layer unit
//! and `turmoil` tests — see `research/notes/06-rust-stack.md` "Trait
//! boundary is load-bearing for simulation coverage". Both implement
//! [`AddressTable`], [`RouteTable`], [`RuleTable`] and [`TopologyQuery`]
//! (jointly, [`Netlink`]) identically from a caller's point of view.

mod capability;
mod cleanup;
mod error;
mod fake;
mod real;
mod table;
mod traits;
mod types;

pub use capability::{KernelCapabilities, UnsupportedKernel, detect as detect_capabilities};
pub use cleanup::{CleanupReport, cleanup};
pub use error::NetlinkError;
pub use fake::FakeNetlink;
pub use real::RealNetlink;
pub use table::{
    DEFAULT_MAIN_RULE_PRIORITY, DEFAULT_MAIN_TABLE_ID, DEFAULT_PEER_TABLE_RANGE, RT_TABLE_DEFAULT,
    RT_TABLE_LOCAL, RT_TABLE_MAIN, RT_TABLE_UNSPEC, TableAllocator, TableAllocatorError,
    is_kernel_reserved_table,
};
pub use traits::{AddressTable, Netlink, RouteTable, RuleTable, TopologyQuery, resolve_interface};
pub use types::{
    AddressEntry, Interface, Ipv4Net, LinkInfo, NETSUKUKU_ADDRESS_SPACE, NeighbourInfo,
    NeighbourState, Nexthop, Operation, RouteKey, RouteSpec, RouteTarget, RuleSelector, RuleSpec,
};
