//! The trait seam: every kernel-state operation upstream's
//! `identity_ip_commands.vala`/`cleaning.vala` performs by shelling out to
//! `ip`(8), reimplemented as async traits over real netlink. Split into four
//! small traits — one per `ip` sub-command family the inventory in
//! `research/notes/02-vala-services-daemon.md` §5 shows being used — rather
//! than one monolithic trait, so a consumer that only needs, say, address
//! management doesn't have to depend on route/rule mocking too.
//!
//! [`RealNetlink`](crate::RealNetlink) and [`FakeNetlink`](crate::FakeNetlink)
//! implement all four; upper-layer crates (and their turmoil-based
//! simulations) should be generic over these traits, never over a concrete
//! implementation, per `research/notes/06-rust-stack.md` "Trait boundary is
//! load-bearing for simulation coverage".

use crate::error::NetlinkError;
use crate::types::{
    AddressEntry, Interface, LinkInfo, NeighbourInfo, RouteKey, RouteSpec, RuleSpec,
};

/// IPv4 address management on a network interface (`ip address add|del|show`).
///
/// `async fn` in a public trait normally warns (`async_fn_in_trait`) because
/// it cannot express a `Send` bound on the returned future. This crate's
/// concurrency model (`research/notes/06-rust-stack.md` "single-owner actor
/// task + mpsc/oneshot") never sends these futures across a spawn boundary
/// generically — each identity's actor task owns and awaits them directly —
/// so the lint does not apply; suppressed deliberately, not accidentally.
#[allow(async_fn_in_trait)]
pub trait AddressTable {
    /// `ip address add <network> dev <interface>`. Adding a duplicate
    /// address is an error, matching the kernel's own `EEXIST` — this trait
    /// never silently no-ops a duplicate add.
    async fn add_address(
        &self,
        interface: &Interface,
        network: crate::types::Ipv4Net,
    ) -> Result<(), NetlinkError>;

    /// `ip address del <network> dev <interface>`.
    async fn remove_address(
        &self,
        interface: &Interface,
        network: crate::types::Ipv4Net,
    ) -> Result<(), NetlinkError>;

    /// `ip address show [dev <interface>]`. `interface = None` lists every
    /// interface, matching plain `ip address show`.
    async fn list_addresses(
        &self,
        interface: Option<&Interface>,
    ) -> Result<Vec<AddressEntry>, NetlinkError>;
}

/// Routing-table entry management (`ip route add|change|del|show table <t>`).
#[allow(async_fn_in_trait)]
pub trait RouteTable {
    /// `ip route add ... table <t>`. Fails if the route already exists
    /// (`NLM_F_EXCL`) — use [`RouteTable::change_route`] to replace.
    async fn add_route(&self, route: &RouteSpec) -> Result<(), NetlinkError>;

    /// `ip route change ... table <t>` (`NLM_F_REPLACE`) — installs the
    /// route whether or not one already exists for the destination.
    async fn change_route(&self, route: &RouteSpec) -> Result<(), NetlinkError>;

    /// `ip route del <destination> table <table>`.
    async fn remove_route(&self, route: RouteKey) -> Result<(), NetlinkError>;

    /// `ip route list [table <t>]`. `table = None` lists every table.
    async fn list_routes(&self, table: Option<u32>) -> Result<Vec<RouteSpec>, NetlinkError>;
}

/// Policy-routing rule management (`ip rule add|del|show`), requiring
/// `CONFIG_IP_MULTIPLE_TABLES` — see [`crate::capability`].
#[allow(async_fn_in_trait)]
pub trait RuleTable {
    /// `ip rule add ...`. Fails if an identical rule already exists.
    async fn add_rule(&self, rule: &RuleSpec) -> Result<(), NetlinkError>;

    /// `ip rule del ...`, matched on `(selector, table, priority)`.
    async fn remove_rule(&self, rule: &RuleSpec) -> Result<(), NetlinkError>;

    /// `ip rule show`.
    async fn list_rules(&self) -> Result<Vec<RuleSpec>, NetlinkError>;
}

/// Read-only link and neighbour-cache introspection (`ip link show`,
/// `ip neighbour show`) — used to resolve [`Interface::Name`] to an
/// `ifindex` and, for future consumers, to read the ARP/NDP cache peer
/// discovery populates.
#[allow(async_fn_in_trait)]
pub trait TopologyQuery {
    /// `ip link show`.
    async fn list_links(&self) -> Result<Vec<LinkInfo>, NetlinkError>;

    /// `ip neighbour show [dev <interface>]`.
    async fn list_neighbours(
        &self,
        interface: Option<&Interface>,
    ) -> Result<Vec<NeighbourInfo>, NetlinkError>;
}

/// The full kernel-state surface this crate manipulates. Blanket-implemented
/// for anything implementing all four traits — a convenience bound for
/// functions (like [`crate::cleanup::cleanup`]) that need the whole surface,
/// without forcing every function to spell out all four bounds.
pub trait Netlink: AddressTable + RouteTable + RuleTable + TopologyQuery {}
impl<T: AddressTable + RouteTable + RuleTable + TopologyQuery> Netlink for T {}

/// Resolves an [`Interface`] to its current [`LinkInfo`] via
/// [`TopologyQuery::list_links`]. Shared by [`RealNetlink`](crate::RealNetlink)
/// (which must turn a name into an `ifindex` before building any netlink
/// message) and [`FakeNetlink`](crate::FakeNetlink) (which resolves against
/// its seeded link table so its address/route model stays keyed on a single
/// canonical `ifindex` regardless of which form the caller used).
pub async fn resolve_interface<T: TopologyQuery + ?Sized>(
    kernel: &T,
    interface: &Interface,
) -> Result<LinkInfo, NetlinkError> {
    let links = kernel.list_links().await?;
    let found = match interface {
        Interface::Index(index) => links.into_iter().find(|link| link.index == *index),
        Interface::Name(name) => links.into_iter().find(|link| &link.name == name),
    };
    found.ok_or_else(|| NetlinkError::InterfaceNotFound(interface.clone()))
}
