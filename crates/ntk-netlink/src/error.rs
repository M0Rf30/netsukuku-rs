//! Error types for kernel-state manipulation.

use crate::types::Interface;

/// Failure manipulating or querying kernel routing state.
#[derive(Debug, thiserror::Error)]
pub enum NetlinkError {
    /// Opening the netlink socket itself failed (see [`crate::RealNetlink::new`]).
    #[error("failed to open netlink connection: {0}")]
    Connect(#[source] std::io::Error),

    /// The kernel rejected a request (`rtnetlink`'s own error, e.g. `EEXIST`,
    /// `ESRCH`, `EPERM` for a missing `CAP_NET_ADMIN`).
    #[error(transparent)]
    Netlink(#[from] rtnetlink::Error),

    /// `interface` does not exist in the kernel's link table.
    #[error("interface {0:?} not found")]
    InterfaceNotFound(Interface),

    /// An IPv4 prefix length outside `0..=32`.
    #[error("invalid IPv4 prefix length {0} (must be 0..=32)")]
    InvalidPrefixLength(u8),

    /// The caller tried to mutate a kernel-reserved table (`unspec`/`default`/
    /// `main`/`local`) through [`crate::RouteTable`] or [`crate::RuleTable`].
    /// Netsukuku must never touch these (design decision, `research/README.md`
    /// "Netsukuku is an L3 routing protocol, not a TUN overlay").
    #[error(
        "table {0} is a kernel-reserved table (unspec/default/main/local) and cannot be used by Netsukuku"
    )]
    ReservedTable(u32),

    /// No kernel object matched the given key (used by [`crate::FakeNetlink`]
    /// to mirror the kernel's `ENOENT`/`ESRCH` on deleting something that
    /// isn't there).
    #[error("no matching kernel object: {0}")]
    NotFound(String),

    /// The requested kernel object already exists (used by
    /// [`crate::FakeNetlink`] to mirror the kernel's `EEXIST` on adding
    /// something that is already there).
    #[error("kernel object already exists: {0}")]
    AlreadyExists(String),
}
