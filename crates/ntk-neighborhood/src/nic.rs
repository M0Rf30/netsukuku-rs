//! Local-NIC seams: [`LocalNic`] (what a caller hands to
//! [`crate::Handle::start_monitor`]), [`RttProbe`] (upstream's
//! `INeighborhoodNetworkInterface::measure_rtt`) and [`IpRouteManager`]
//! (upstream's `INeighborhoodIPRouteManager`, `research/impl/vala/neighborhood/api.vala:79-102`).
//!
//! # Why these stay caller-injected rather than netlink-backed
//! `api.vala`'s own doc comment states the architecture directly:
//! "neighborhood never touches the OS network stack directly, only through
//! this interface" (`api.vala:74-78`). This crate keeps that split:
//! *which* local interfaces exist and are up is resolved through
//! `ntk-netlink`'s [`ntk_netlink::TopologyQuery`] (see
//! `crate::Manager::start_monitor`/`sync_interfaces` — the "never a second
//! interface-enumeration path" constraint), but *acting* on one (assigning
//! its linklocal address, adding/removing a neighbor route) and knowing its
//! MAC remain behind [`IpRouteManager`]/[`LocalNic`] exactly as upstream
//! injects `INeighborhoodIPRouteManager`/`INeighborhoodNetworkInterface`.
//! `ntk-netlink`'s `LinkInfo` (`crate::TopologyQuery::list_links`) carries
//! no MAC address and its `RouteTarget` has no on-link/no-gateway variant
//! matching "route straight to this host, no gateway" — modeling
//! `add_neighbor`/`remove_neighbor` as a caller-injected seam avoids both
//! gaps rather than guessing at an encoding ntk-netlink was not designed to
//! express.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use crate::error::NeighborhoodError;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A local NIC a caller wants monitored (upstream's
/// `INeighborhoodNetworkInterface.dev`/`.mac`,
/// `research/impl/vala/neighborhood/api.vala:29-34`, minus `measure_rtt` —
/// that is [`RttProbe`], injected separately since it is a per-manager, not
/// per-NIC, capability in this crate).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalNic {
    /// Interface name (`ntk_netlink::Interface::Name` form).
    pub dev: String,
    /// Hardware (MAC) address.
    pub mac: String,
}

/// Best-effort RTT measurement between local and peer addresses
/// (`INeighborhoodNetworkInterface::measure_rtt`, `api.vala:33`). A `None`
/// result mirrors upstream's `rtt == -1`/`NeighborhoodGetRttError` case:
/// non-fatal, the arc is maintained and no cost update happens this tick
/// (`neighborhood.vala:253-259`).
pub trait RttProbe: Send + Sync + std::fmt::Debug {
    /// Measures the current RTT from `my_addr` on `my_dev` to `peer_addr`.
    fn measure_rtt<'a>(
        &'a self,
        my_dev: &'a str,
        my_addr: &'a str,
        peer_addr: &'a str,
    ) -> BoxFuture<'a, Option<u64>>;
}

/// OS network-stack actions this module performs only through this
/// injected seam (`INeighborhoodIPRouteManager`, `api.vala:79-102`) — see
/// the module doc comment for why this is not netlink-backed inside this
/// crate.
pub trait IpRouteManager: Send + Sync + std::fmt::Debug {
    /// `add_address(my_addr, my_dev)` — assigns the NIC's fixed linklocal
    /// address.
    fn add_address<'a>(
        &'a self,
        my_dev: &'a str,
        my_addr: &'a str,
    ) -> BoxFuture<'a, Result<(), NeighborhoodError>>;

    /// `remove_address(my_addr, my_dev)`.
    fn remove_address<'a>(
        &'a self,
        my_dev: &'a str,
        my_addr: &'a str,
    ) -> BoxFuture<'a, Result<(), NeighborhoodError>>;

    /// `add_neighbor(my_addr, my_dev, neighbor_addr)` — makes the peer's
    /// fixed linklocal address reachable over this NIC.
    fn add_neighbor<'a>(
        &'a self,
        my_dev: &'a str,
        my_addr: &'a str,
        neighbor_addr: &'a str,
    ) -> BoxFuture<'a, Result<(), NeighborhoodError>>;

    /// `remove_neighbor(my_addr, my_dev, neighbor_addr)`.
    fn remove_neighbor<'a>(
        &'a self,
        my_dev: &'a str,
        my_addr: &'a str,
        neighbor_addr: &'a str,
    ) -> BoxFuture<'a, Result<(), NeighborhoodError>>;
}

/// One recorded [`IpRouteManager`] call, in invocation order — mirrors
/// `ntk_netlink::Operation`'s role for [`ntk_netlink::FakeNetlink`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IpRouteOperation {
    AddAddress {
        dev: String,
        addr: String,
    },
    RemoveAddress {
        dev: String,
        addr: String,
    },
    AddNeighbor {
        dev: String,
        my_addr: String,
        neighbor_addr: String,
    },
    RemoveNeighbor {
        dev: String,
        my_addr: String,
        neighbor_addr: String,
    },
}

/// A non-privileged, in-memory [`IpRouteManager`] that always succeeds and
/// records every call — for tests and simulation, mirroring
/// `ntk_netlink::FakeNetlink`'s shape.
#[derive(Debug, Default)]
pub struct FakeIpRouteManager {
    operations: Mutex<Vec<IpRouteOperation>>,
}

impl FakeIpRouteManager {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every recorded operation, in invocation order.
    #[must_use]
    pub fn operations(&self) -> Vec<IpRouteOperation> {
        self.operations
            .lock()
            .expect("operations mutex poisoned")
            .clone()
    }

    fn record(&self, op: IpRouteOperation) {
        self.operations
            .lock()
            .expect("operations mutex poisoned")
            .push(op);
    }
}

impl IpRouteManager for FakeIpRouteManager {
    fn add_address<'a>(
        &'a self,
        my_dev: &'a str,
        my_addr: &'a str,
    ) -> BoxFuture<'a, Result<(), NeighborhoodError>> {
        self.record(IpRouteOperation::AddAddress {
            dev: my_dev.to_owned(),
            addr: my_addr.to_owned(),
        });
        Box::pin(async { Ok(()) })
    }

    fn remove_address<'a>(
        &'a self,
        my_dev: &'a str,
        my_addr: &'a str,
    ) -> BoxFuture<'a, Result<(), NeighborhoodError>> {
        self.record(IpRouteOperation::RemoveAddress {
            dev: my_dev.to_owned(),
            addr: my_addr.to_owned(),
        });
        Box::pin(async { Ok(()) })
    }

    fn add_neighbor<'a>(
        &'a self,
        my_dev: &'a str,
        my_addr: &'a str,
        neighbor_addr: &'a str,
    ) -> BoxFuture<'a, Result<(), NeighborhoodError>> {
        self.record(IpRouteOperation::AddNeighbor {
            dev: my_dev.to_owned(),
            my_addr: my_addr.to_owned(),
            neighbor_addr: neighbor_addr.to_owned(),
        });
        Box::pin(async { Ok(()) })
    }

    fn remove_neighbor<'a>(
        &'a self,
        my_dev: &'a str,
        my_addr: &'a str,
        neighbor_addr: &'a str,
    ) -> BoxFuture<'a, Result<(), NeighborhoodError>> {
        self.record(IpRouteOperation::RemoveNeighbor {
            dev: my_dev.to_owned(),
            my_addr: my_addr.to_owned(),
            neighbor_addr: neighbor_addr.to_owned(),
        });
        Box::pin(async { Ok(()) })
    }
}

/// An [`RttProbe`] that always reports the same fixed value — for tests.
#[derive(Debug, Clone, Copy)]
pub struct FixedRttProbe(pub Option<u64>);

impl RttProbe for FixedRttProbe {
    fn measure_rtt<'a>(
        &'a self,
        _my_dev: &'a str,
        _my_addr: &'a str,
        _peer_addr: &'a str,
    ) -> BoxFuture<'a, Option<u64>> {
        Box::pin(async move { self.0 })
    }
}
