//! [`RealIpRouteManager`]: the real-transport [`ntk_neighborhood::IpRouteManager`], over the
//! concrete [`ntk_netlink::RealNetlink`] (not generic — `IpRouteManager`'s methods return
//! `BoxFuture`, which needs a provably-`Send` future; `ntk_netlink`'s `async fn`-in-trait
//! methods only prove that for concrete implementors, the same reason `ntk-neighborhood`'s own
//! sealed `InterfaceState` trait exists). Tests use `ntk-neighborhood`'s own
//! [`ntk_neighborhood::FakeIpRouteManager`] directly instead of this type.
//!
//! `add_address`/`remove_address` are real, via [`ntk_netlink::AddressTable`].
//!
//! # `add_neighbor`/`remove_neighbor`: a per-neighbor `/32` on-link route
//! This is the fix for the relay reply-path defect: a node monitoring 2+ NICs installs one
//! `169.254.0.0/16` connected route per NIC (RFC 3927), so the kernel FIB holds several routes
//! to the *identical* prefix at metric 0 and resolves any destination-based lookup in that
//! prefix — including a `TcpServer`-accepted connection's own reply traffic, which never goes
//! through `ntk_rpc::TcpRpcClient::connect_via`'s `SO_BINDTODEVICE` dial-side fix — to whichever
//! NIC's route was installed first, regardless of which NIC the peer is actually reachable on.
//! [`ntk_netlink::RouteTarget::OnLink`] (added alongside this fix) is longest-prefix-match's
//! own escape hatch: a `/32` host route to the *specific* neighbor, via the *specific* device
//! the neighborhood handshake already learned it on, always outranks the ambiguous `/16`.
//!
//! ## Table choice: [`NEIGHBOR_ROUTE_TABLE`], a table of its own — deliberately not
//! [`ntk_netlink::DEFAULT_MAIN_TABLE_ID`]
//! The obvious first choice is the main identity's own table (251): `crate::kernel::routes::
//! RouteInstaller::install_identity` already installs an unconditional
//! [`ntk_netlink::RuleSelector::Any`] rule there, at [`ntk_netlink::DEFAULT_MAIN_RULE_PRIORITY`]
//! — well ahead of the kernel's own implicit main-table rule (priority 32766) — so anything in
//! that table is already consulted before the kernel's ambiguous `/16`s, for free. **Tried
//! first, rejected on evidence**: `tests/mesh.rs`'s own fixture (`tests/netns/mod.rs`'s
//! `NodeReport`) reads back exactly this table and several of its scenarios assert the *exact*
//! set of routes found there (e.g. `chain_of_four_converges_to_exact_multi_hop_routes`'s "must
//! have exactly the 3 converged destinations installed, nothing stale") — confirmed by directly
//! running that suite with the main-table version of this fix: a real, permanent on-link
//! neighbor route is exactly the kind of "extra" entry those assertions correctly reject, since
//! table 251 is that fixture's *only* signal for "what did `RouteInstaller` converge to". Table
//! 251 must stay [`crate::kernel::routes::RouteInstaller`]'s exclusively.
//!
//! So this uses [`NEIGHBOR_ROUTE_TABLE`] — a second, dedicated fixed table, outside both
//! [`ntk_netlink::DEFAULT_MAIN_TABLE_ID`] and [`ntk_netlink::DEFAULT_PEER_TABLE_RANGE`] (so it
//! can never collide with a future real per-peer `fwmark` table allocation from that pool) —
//! with its own unconditional `Any` rule at [`NEIGHBOR_RULE_PRIORITY`], installed lazily and
//! idempotently by [`ensure_neighbor_rule`] the first time [`RealIpRouteManager::add_neighbor`]
//! runs (there is no per-node startup hook this type could use instead — [`RealIpRouteManager`]
//! is constructed as a plain struct literal by several call sites this crate does not own, so
//! it cannot gain a new field to remember "already installed" and must re-check idempotently;
//! see [`ensure_neighbor_rule`]'s own doc). Priority `10_001` is chosen only to sit clear of
//! [`ntk_netlink::TableAllocator`]'s peer-priority pool (`9_949..10_000` under the default
//! range) and of `251`'s own `10_000` — evaluation order between the two `Any` rules does not
//! matter, since each table only ever holds routes for a disjoint destination range (`10.0.0.0/8`
//! vs `169.254.0.0/16`), so a lookup that misses one falls through with no effect either way.
//!
//! Because [`NEIGHBOR_ROUTE_TABLE`] is deliberately outside [`ntk_netlink::TableAllocator`]'s
//! own ownership range, [`ntk_netlink::cleanup`]'s existing route/rule sweep does **not** see
//! it — verified directly, not assumed: [`cleanup_neighbor_routes`] is this table's explicit
//! crash-recovery counterpart, called by [`crate::node::supervisor::run`] alongside the main
//! `ntk_netlink::cleanup` call.
//!
//! ## Why this is race-free
//! `add_neighbor`/`remove_neighbor` only ever run from inside `ntk_neighborhood::Manager`'s
//! command loop, reacting to a `here_i_am`/`request_arc`/arc-removal *message*, and that loop
//! processes commands strictly one at a time — so two concurrent `add_neighbor` calls racing
//! [`ensure_neighbor_rule`]'s own check-then-add against each other cannot happen for a single
//! node's one `RealIpRouteManager`.
//!
//! `add_address`'s widened-to-`/16` rationale is unaffected by any of the above — see
//! [`linklocal_net`]'s own doc.

use futures::future::BoxFuture;
use ntk_neighborhood::{IpRouteManager, NeighborhoodError};
use ntk_netlink::{
    AddressTable, Interface, Ipv4Net, NetlinkError, RealNetlink, RouteKey, RouteSpec, RouteTable,
    RouteTarget, RuleSelector, RuleSpec, RuleTable,
};

/// RFC 3927 §2.1's link-local block, `169.254.0.0/16` — the prefix length every neighborhood
/// link-local address is installed at (see [`linklocal_net`]'s doc for why).
const LINKLOCAL_PREFIX_LEN: u8 = 16;

/// The dedicated table [`RealIpRouteManager::add_neighbor`]/[`RealIpRouteManager::remove_neighbor`]
/// install into — see this module's doc comment's "Table choice" section for why this exact,
/// separate table rather than the main identity's own. Outside [`ntk_netlink::RT_TABLE_MAIN`]/
/// `RT_TABLE_DEFAULT`/`RT_TABLE_LOCAL`/`RT_TABLE_UNSPEC` (kernel-reserved, `guard_table` would
/// reject it), [`ntk_netlink::DEFAULT_MAIN_TABLE_ID`] (251, `RouteInstaller`'s own), and
/// [`ntk_netlink::DEFAULT_PEER_TABLE_RANGE`] (`200..=250`, reserved for a future real per-peer
/// allocation via [`ntk_netlink::TableAllocator::acquire`]).
pub const NEIGHBOR_ROUTE_TABLE: u32 = 252;

/// [`NEIGHBOR_ROUTE_TABLE`]'s own catch-all rule priority — see this module's doc comment for
/// why this exact value.
const NEIGHBOR_RULE_PRIORITY: u32 = 10_001;

#[derive(Debug)]
pub struct RealIpRouteManager {
    pub kernel: RealNetlink,
}

fn parse_addr(addr: &str) -> Result<std::net::Ipv4Addr, NeighborhoodError> {
    addr.parse()
        .map_err(|_| NeighborhoodError::MalformedWire(format!("invalid IPv4 address {addr:?}")))
}

/// The [`Ipv4Net`] this manager installs/removes for a neighborhood link-local address.
///
/// Deliberately **not** [`Ipv4Net::host`] (a `/32`): `addr` here is always drawn from RFC
/// 3927's `169.254.0.0/16` block (`crate::node::lifecycle::linklocal_allocator`), and a `/32`
/// linklocal address is broken on a real kernel — confirmed by a minimal two-namespace veth
/// reproduction outside this codebase: with each side's address installed as `/32`,
/// `sendto(255.255.255.255)` reports success but the kernel never actually delivers the frame
/// (a host route has no on-link broadcast domain to broadcast onto), so neighborhood
/// discovery's UDP broadcast handshake never arrives. Installing the full `/16` — the exact
/// prefix RFC 3927 itself specifies for this block — gives the interface a real on-link
/// broadcast domain and fixes delivery. Widening from `/32` to `/16` does not by itself make
/// two nodes' addresses collide: each is still a distinct address *within* the shared prefix
/// (see `crate::node::lifecycle::linklocal_allocator`'s doc for how that distinctness is
/// arranged) — the two properties (on-link prefix width, address distinctness) are independent
/// and both were broken, which is why fixing only one left discovery red.
///
/// This is the only prefix length this file ever chooses on its own. Every other [`Ipv4Net`]
/// this daemon installs (`crate::kernel::routes`) is a real Netsukuku identity or g-node
/// address/range, whose prefix comes from the topology/NIP model, not from here — those are
/// genuinely host routes (`/32` per identity) or genuine sub-ranges, not an on-link broadcast
/// prefix, so they correctly keep using host/topology-derived prefixes instead of this one.
fn linklocal_net(addr: std::net::Ipv4Addr) -> Ipv4Net {
    Ipv4Net::new(addr, LINKLOCAL_PREFIX_LEN)
        .expect("16 is always a valid IPv4 prefix length (0..=32)")
}

/// The `/32` on-link [`RouteSpec`] [`RealIpRouteManager::add_neighbor`] installs for
/// `neighbor` via `my_dev` — split out from the trait method so it can be pinned by a plain,
/// unprivileged unit test (below) without a real or fake netlink backend.
fn neighbor_route(my_dev: &str, neighbor: std::net::Ipv4Addr) -> RouteSpec {
    RouteSpec {
        destination: Ipv4Net::host(neighbor),
        table: NEIGHBOR_ROUTE_TABLE,
        target: RouteTarget::OnLink {
            dev: Interface::name(my_dev),
        },
    }
}

/// The [`RouteKey`] [`RealIpRouteManager::remove_neighbor`] removes for `neighbor` — always
/// exactly the key of the [`RouteSpec`] [`neighbor_route`] built for that same address.
fn neighbor_route_key(neighbor: std::net::Ipv4Addr) -> RouteKey {
    RouteKey {
        destination: Ipv4Net::host(neighbor),
        table: NEIGHBOR_ROUTE_TABLE,
    }
}

/// Idempotently makes sure [`NEIGHBOR_ROUTE_TABLE`]'s catch-all rule exists, installing it on
/// the first call and no-op'ing on every later one. Cannot instead be installed once, eagerly,
/// at node startup (mirroring how `RouteInstaller::install_identity` installs table 251's own
/// rule): [`RealIpRouteManager`] is built as a plain `pub` struct literal by several call sites
/// outside this crate's control, so it cannot gain a "not yet installed" field to make that
/// eager version race-free without breaking every one of them — see this module's doc comment.
/// Checking via [`ntk_netlink::RuleTable::list_rules`] rather than blindly calling `add_rule`
/// and swallowing its error: a real kernel failure surfaces as the generic, unmatchable
/// [`NetlinkError::Netlink`] variant, not the specific [`NetlinkError::AlreadyExists`]
/// [`ntk_netlink::FakeNetlink`] uses for the same condition — there is no reliable way to tell
/// "already there" apart from a genuine failure by matching on that variant alone.
async fn ensure_neighbor_rule(kernel: &RealNetlink) -> Result<(), NetlinkError> {
    let rule = RuleSpec {
        table: NEIGHBOR_ROUTE_TABLE,
        priority: NEIGHBOR_RULE_PRIORITY,
        selector: RuleSelector::Any,
    };
    if kernel.list_rules().await?.contains(&rule) {
        return Ok(());
    }
    kernel.add_rule(&rule).await
}

/// [`NEIGHBOR_ROUTE_TABLE`]'s explicit crash-recovery counterpart to [`ntk_netlink::cleanup`] —
/// see this module's doc comment for why this table needs one of its own. Called by
/// [`crate::node::supervisor::run`] alongside the main `ntk_netlink::cleanup` sweep.
///
/// # Errors
/// [`NetlinkError`] if a kernel query or mutation fails.
pub async fn cleanup_neighbor_routes(kernel: &RealNetlink) -> Result<(), NetlinkError> {
    for route in kernel.list_routes(Some(NEIGHBOR_ROUTE_TABLE)).await? {
        kernel
            .remove_route(RouteKey {
                destination: route.destination,
                table: route.table,
            })
            .await?;
    }
    for rule in kernel.list_rules().await? {
        if rule.table == NEIGHBOR_ROUTE_TABLE {
            kernel.remove_rule(&rule).await?;
        }
    }
    Ok(())
}

impl IpRouteManager for RealIpRouteManager {
    fn add_address<'a>(
        &'a self,
        my_dev: &'a str,
        my_addr: &'a str,
    ) -> BoxFuture<'a, Result<(), NeighborhoodError>> {
        Box::pin(async move {
            let addr = parse_addr(my_addr)?;
            self.kernel
                .add_address(&Interface::name(my_dev), linklocal_net(addr))
                .await
                .map_err(NeighborhoodError::Netlink)
        })
    }

    fn remove_address<'a>(
        &'a self,
        my_dev: &'a str,
        my_addr: &'a str,
    ) -> BoxFuture<'a, Result<(), NeighborhoodError>> {
        Box::pin(async move {
            let addr = parse_addr(my_addr)?;
            self.kernel
                .remove_address(&Interface::name(my_dev), linklocal_net(addr))
                .await
                .map_err(NeighborhoodError::Netlink)
        })
    }
    /// Installs a `/32` on-link host route to `neighbor_addr` via `my_dev`, in
    /// [`NEIGHBOR_ROUTE_TABLE`] — see this module's doc comment for the full rationale.
    /// [`RouteTable::change_route`] rather than `add_route`: idempotent by construction, so a
    /// stale route left over from a not-yet-cleaned-up prior arc to the same address is
    /// replaced rather than rejected as a duplicate.
    fn add_neighbor<'a>(
        &'a self,
        my_dev: &'a str,
        _my_addr: &'a str,
        neighbor_addr: &'a str,
    ) -> BoxFuture<'a, Result<(), NeighborhoodError>> {
        Box::pin(async move {
            let neighbor = parse_addr(neighbor_addr)?;
            ensure_neighbor_rule(&self.kernel)
                .await
                .map_err(NeighborhoodError::Netlink)?;
            self.kernel
                .change_route(&neighbor_route(my_dev, neighbor))
                .await
                .map_err(NeighborhoodError::Netlink)
        })
    }

    /// Removes the `/32` on-link host route [`RealIpRouteManager::add_neighbor`] installed for
    /// `neighbor_addr`.
    fn remove_neighbor<'a>(
        &'a self,
        _my_dev: &'a str,
        _my_addr: &'a str,
        neighbor_addr: &'a str,
    ) -> BoxFuture<'a, Result<(), NeighborhoodError>> {
        Box::pin(async move {
            let neighbor = parse_addr(neighbor_addr)?;
            self.kernel
                .remove_route(neighbor_route_key(neighbor))
                .await
                .map_err(NeighborhoodError::Netlink)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{NEIGHBOR_ROUTE_TABLE, linklocal_net, neighbor_route, neighbor_route_key};
    use ntk_netlink::{Interface, RouteTarget};

    #[test]
    fn linklocal_network_is_a_slash_16_not_a_host_route() {
        // Bug 1, pinned: `add_address`/`remove_address` used to install/remove
        // `Ipv4Net::host(addr)` (a `/32`), which a real kernel silently fails to broadcast
        // onto — see `linklocal_net`'s doc comment.
        let net = linklocal_net("169.254.12.34".parse().unwrap());
        assert_eq!(net.prefix_len(), 16);
        assert_eq!(
            net.address(),
            "169.254.12.34".parse::<std::net::Ipv4Addr>().unwrap()
        );
    }

    #[test]
    fn neighbor_route_is_a_slash_32_on_link_route_via_the_right_device() {
        // Bug 2, pinned: `add_neighbor` used to be a documented no-op, leaving multiple
        // ambiguous `/16` connected routes (one per monitored NIC) as the only routes to a
        // neighbor's linklocal address — see this module's doc comment for the full defect.
        let neighbor = "169.254.7.9".parse().unwrap();
        let spec = neighbor_route("ntkd-mnr-rb", neighbor);
        assert_eq!(spec.destination.address(), neighbor);
        assert_eq!(spec.destination.prefix_len(), 32);
        assert_eq!(spec.table, NEIGHBOR_ROUTE_TABLE);
        assert_eq!(
            spec.target,
            RouteTarget::OnLink {
                dev: Interface::name("ntkd-mnr-rb")
            }
        );
    }

    #[test]
    fn removing_a_neighbor_targets_exactly_the_route_that_was_added() {
        let neighbor = "169.254.7.9".parse().unwrap();
        let added = neighbor_route("ntkd-mnr-rb", neighbor);
        let removed = neighbor_route_key(neighbor);
        assert_eq!(removed.destination, added.destination);
        assert_eq!(removed.table, added.table);
    }
}
