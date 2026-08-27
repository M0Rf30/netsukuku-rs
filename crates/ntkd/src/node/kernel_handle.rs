//! [`SendNetlink`] + [`KernelHandle`]: a `Send`-provable bridge onto [`ntk_netlink::Netlink`],
//! and a cheap-clone `Arc<K>` wrapper implementing it by delegation — mirroring
//! `ntk_neighborhood::interface_state::InterfaceState`'s own rationale exactly.
//!
//! `ntk-netlink`'s traits are `async fn`s-in-a-trait with `#[allow(async_fn_in_trait)]`
//! suppressing the "future may not be `Send`" lint, on the documented assumption that generic
//! callers never cross a `tokio::spawn` boundary with one. This daemon's steady-state loop
//! (`crate::node::lifecycle`) *does* spawn a task that calls into a generic `K: Netlink`
//! (through [`crate::kernel::routes::RouteInstaller`]), so it needs a provably-`Send` future —
//! [`SendNetlink`] states that boundary explicitly via `BoxFuture` (which bakes in `Send`),
//! implemented concretely for exactly `ntk-netlink`'s two real implementors (both of which are
//! genuinely `Send`, per `ntk_neighborhood::interface_state`'s own module doc). [`KernelHandle`]
//! then re-implements `Netlink`'s four traits over any `SendNetlink`, so
//! `RouteInstaller<KernelHandle<K>>` gets a provably-`Send` future for free.

use std::sync::Arc;

use futures::future::BoxFuture;
use ntk_netlink::{
    AddressEntry, AddressTable, FakeNetlink, Interface, Ipv4Net, LinkInfo, NeighbourInfo,
    NetlinkError, RealNetlink, RouteKey, RouteSpec, RouteTable, RuleSpec, RuleTable, TopologyQuery,
};

/// `Send`-provable version of [`ntk_netlink::Netlink`]'s four traits, combined — see the module
/// doc. Implemented concretely for [`RealNetlink`] and [`FakeNetlink`] only.
pub trait SendNetlink: Send + Sync + std::fmt::Debug {
    fn add_address<'a>(
        &'a self,
        interface: &'a Interface,
        network: Ipv4Net,
    ) -> BoxFuture<'a, Result<(), NetlinkError>>;
    fn remove_address<'a>(
        &'a self,
        interface: &'a Interface,
        network: Ipv4Net,
    ) -> BoxFuture<'a, Result<(), NetlinkError>>;
    fn list_addresses<'a>(
        &'a self,
        interface: Option<&'a Interface>,
    ) -> BoxFuture<'a, Result<Vec<AddressEntry>, NetlinkError>>;
    fn add_route<'a>(&'a self, route: &'a RouteSpec) -> BoxFuture<'a, Result<(), NetlinkError>>;
    fn change_route<'a>(&'a self, route: &'a RouteSpec) -> BoxFuture<'a, Result<(), NetlinkError>>;
    fn remove_route(&self, route: RouteKey) -> BoxFuture<'_, Result<(), NetlinkError>>;
    fn list_routes(
        &self,
        table: Option<u32>,
    ) -> BoxFuture<'_, Result<Vec<RouteSpec>, NetlinkError>>;
    fn add_rule<'a>(&'a self, rule: &'a RuleSpec) -> BoxFuture<'a, Result<(), NetlinkError>>;
    fn remove_rule<'a>(&'a self, rule: &'a RuleSpec) -> BoxFuture<'a, Result<(), NetlinkError>>;
    fn list_rules(&self) -> BoxFuture<'_, Result<Vec<RuleSpec>, NetlinkError>>;
    fn list_links(&self) -> BoxFuture<'_, Result<Vec<LinkInfo>, NetlinkError>>;
    fn list_neighbours<'a>(
        &'a self,
        interface: Option<&'a Interface>,
    ) -> BoxFuture<'a, Result<Vec<NeighbourInfo>, NetlinkError>>;
}

macro_rules! impl_send_netlink {
    ($ty:ty) => {
        impl SendNetlink for $ty {
            fn add_address<'a>(
                &'a self,
                interface: &'a Interface,
                network: Ipv4Net,
            ) -> BoxFuture<'a, Result<(), NetlinkError>> {
                Box::pin(AddressTable::add_address(self, interface, network))
            }
            fn remove_address<'a>(
                &'a self,
                interface: &'a Interface,
                network: Ipv4Net,
            ) -> BoxFuture<'a, Result<(), NetlinkError>> {
                Box::pin(AddressTable::remove_address(self, interface, network))
            }
            fn list_addresses<'a>(
                &'a self,
                interface: Option<&'a Interface>,
            ) -> BoxFuture<'a, Result<Vec<AddressEntry>, NetlinkError>> {
                Box::pin(AddressTable::list_addresses(self, interface))
            }
            fn add_route<'a>(
                &'a self,
                route: &'a RouteSpec,
            ) -> BoxFuture<'a, Result<(), NetlinkError>> {
                Box::pin(RouteTable::add_route(self, route))
            }
            fn change_route<'a>(
                &'a self,
                route: &'a RouteSpec,
            ) -> BoxFuture<'a, Result<(), NetlinkError>> {
                Box::pin(RouteTable::change_route(self, route))
            }
            fn remove_route(&self, route: RouteKey) -> BoxFuture<'_, Result<(), NetlinkError>> {
                Box::pin(RouteTable::remove_route(self, route))
            }
            fn list_routes(
                &self,
                table: Option<u32>,
            ) -> BoxFuture<'_, Result<Vec<RouteSpec>, NetlinkError>> {
                Box::pin(RouteTable::list_routes(self, table))
            }
            fn add_rule<'a>(
                &'a self,
                rule: &'a RuleSpec,
            ) -> BoxFuture<'a, Result<(), NetlinkError>> {
                Box::pin(RuleTable::add_rule(self, rule))
            }
            fn remove_rule<'a>(
                &'a self,
                rule: &'a RuleSpec,
            ) -> BoxFuture<'a, Result<(), NetlinkError>> {
                Box::pin(RuleTable::remove_rule(self, rule))
            }
            fn list_rules(&self) -> BoxFuture<'_, Result<Vec<RuleSpec>, NetlinkError>> {
                Box::pin(RuleTable::list_rules(self))
            }
            fn list_links(&self) -> BoxFuture<'_, Result<Vec<LinkInfo>, NetlinkError>> {
                Box::pin(TopologyQuery::list_links(self))
            }
            fn list_neighbours<'a>(
                &'a self,
                interface: Option<&'a Interface>,
            ) -> BoxFuture<'a, Result<Vec<NeighbourInfo>, NetlinkError>> {
                Box::pin(TopologyQuery::list_neighbours(self, interface))
            }
        }
    };
}

impl_send_netlink!(RealNetlink);
impl_send_netlink!(FakeNetlink);

/// Cheap-clone `Arc<K>` wrapper re-implementing `ntk_netlink::Netlink`'s four traits over any
/// [`SendNetlink`], with a provably-`Send` future (see the module doc).
#[derive(Debug)]
pub struct KernelHandle<K>(pub Arc<K>);

impl<K> Clone for KernelHandle<K> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K: SendNetlink> AddressTable for KernelHandle<K> {
    async fn add_address(
        &self,
        interface: &Interface,
        network: Ipv4Net,
    ) -> Result<(), NetlinkError> {
        self.0.add_address(interface, network).await
    }
    async fn remove_address(
        &self,
        interface: &Interface,
        network: Ipv4Net,
    ) -> Result<(), NetlinkError> {
        self.0.remove_address(interface, network).await
    }
    async fn list_addresses(
        &self,
        interface: Option<&Interface>,
    ) -> Result<Vec<AddressEntry>, NetlinkError> {
        self.0.list_addresses(interface).await
    }
}

impl<K: SendNetlink> RouteTable for KernelHandle<K> {
    async fn add_route(&self, route: &RouteSpec) -> Result<(), NetlinkError> {
        self.0.add_route(route).await
    }
    async fn change_route(&self, route: &RouteSpec) -> Result<(), NetlinkError> {
        self.0.change_route(route).await
    }
    async fn remove_route(&self, route: RouteKey) -> Result<(), NetlinkError> {
        self.0.remove_route(route).await
    }
    async fn list_routes(&self, table: Option<u32>) -> Result<Vec<RouteSpec>, NetlinkError> {
        self.0.list_routes(table).await
    }
}

impl<K: SendNetlink> RuleTable for KernelHandle<K> {
    async fn add_rule(&self, rule: &RuleSpec) -> Result<(), NetlinkError> {
        self.0.add_rule(rule).await
    }
    async fn remove_rule(&self, rule: &RuleSpec) -> Result<(), NetlinkError> {
        self.0.remove_rule(rule).await
    }
    async fn list_rules(&self) -> Result<Vec<RuleSpec>, NetlinkError> {
        self.0.list_rules().await
    }
}

impl<K: SendNetlink> TopologyQuery for KernelHandle<K> {
    async fn list_links(&self) -> Result<Vec<LinkInfo>, NetlinkError> {
        self.0.list_links().await
    }
    async fn list_neighbours(
        &self,
        interface: Option<&Interface>,
    ) -> Result<Vec<NeighbourInfo>, NetlinkError> {
        self.0.list_neighbours(interface).await
    }
}
