//! Kernel-capability preflight: Netsukuku's L3 routing model needs
//! `CONFIG_IP_MULTIPLE_TABLES` (per-peer policy routing, `research/README.md`
//! "Netsukuku is an L3 routing protocol, not a TUN overlay — ... needs
//! `CONFIG_IP_MULTIPLE_TABLES`, `IP_ROUTE_MULTIPATH`") and
//! `CONFIG_IP_ROUTE_MULTIPATH` (the `Multipath` [`crate::RouteTarget`]).
//! Both are detected by real, functional probes against the trait seam
//! (not by reading `/proc` — `/proc/net/fib_rules`'s presence turned out to
//! be unreliable across containerised kernels during development, while a
//! live probe works identically everywhere the trait itself works), so the
//! exact same detection logic is exercised by [`crate::FakeNetlink`] in unit
//! tests and by [`crate::RealNetlink`] against an actual kernel.

use std::fmt;
use std::net::Ipv4Addr;

use crate::traits::{RouteTable, RuleTable, TopologyQuery};
use crate::types::{Interface, Ipv4Net, Nexthop, RouteKey, RouteSpec, RouteTarget};

/// A routing table id vanishingly unlikely to collide with anything real,
/// used only to install-then-immediately-remove a throwaway multipath route
/// as a capability probe. Not in the kernel-reserved set and outside every
/// range [`crate::TableAllocator`] would ever hand out.
const CAPABILITY_PROBE_TABLE: u32 = 0xFFFF_FFF0;

/// Whether the running kernel has the routing features Netsukuku requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelCapabilities {
    /// `CONFIG_IP_MULTIPLE_TABLES` — policy routing / numbered routing
    /// tables beyond `main`/`local`/`default`.
    pub multiple_routing_tables: bool,
    /// `CONFIG_IP_ROUTE_MULTIPATH` — ECMP routes with several nexthops.
    pub multipath_routes: bool,
}

impl KernelCapabilities {
    /// Returns `Ok(())` if every required feature is present, otherwise a
    /// [`UnsupportedKernel`] naming exactly which ones are missing.
    pub fn ensure_supported(&self) -> Result<(), UnsupportedKernel> {
        if self.multiple_routing_tables && self.multipath_routes {
            Ok(())
        } else {
            Err(UnsupportedKernel {
                missing_multiple_routing_tables: !self.multiple_routing_tables,
                missing_multipath_routes: !self.multipath_routes,
            })
        }
    }
}

/// The kernel is missing one or more routing features Netsukuku requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsupportedKernel {
    /// `CONFIG_IP_MULTIPLE_TABLES` is missing.
    pub missing_multiple_routing_tables: bool,
    /// `CONFIG_IP_ROUTE_MULTIPATH` is missing.
    pub missing_multipath_routes: bool,
}

impl fmt::Display for UnsupportedKernel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "kernel is missing required routing features:")?;
        if self.missing_multiple_routing_tables {
            write!(
                f,
                " CONFIG_IP_MULTIPLE_TABLES (policy routing / multiple routing tables)"
            )?;
        }
        if self.missing_multipath_routes {
            write!(f, " CONFIG_IP_ROUTE_MULTIPATH (multipath/ECMP routes)")?;
        }
        Ok(())
    }
}

impl std::error::Error for UnsupportedKernel {}

/// Probes `kernel` for [`KernelCapabilities`]. Never fails: every probe
/// failure (missing feature, permission error, disconnected socket) is
/// reported as that capability being absent rather than propagated, since
/// this function's whole purpose is to turn "can this even work" into a
/// plain status report a caller inspects with [`KernelCapabilities::ensure_supported`].
pub async fn detect<T>(kernel: &T) -> KernelCapabilities
where
    T: RuleTable + RouteTable + TopologyQuery,
{
    KernelCapabilities {
        multiple_routing_tables: kernel.list_rules().await.is_ok(),
        multipath_routes: probe_multipath(kernel).await,
    }
}

/// Installs a throwaway ECMP route entirely within `127.0.0.0/8` via `lo`
/// (both nexthops are loopback addresses, so no real connectivity is
/// required) in [`CAPABILITY_PROBE_TABLE`], then removes it. Requires
/// `CAP_NET_ADMIN` against a real kernel, like every other mutating call in
/// this crate — [`crate::FakeNetlink`] always succeeds, exercising this
/// function's control flow without privilege.
async fn probe_multipath<T: RouteTable + TopologyQuery>(kernel: &T) -> bool {
    let Ok(links) = kernel.list_links().await else {
        return false;
    };
    let Some(loopback) = links.iter().find(|link| link.name == "lo") else {
        return false;
    };
    let destination =
        Ipv4Net::new(Ipv4Addr::new(127, 255, 255, 0), 24).expect("valid literal prefix length");
    let probe = RouteSpec {
        destination,
        table: CAPABILITY_PROBE_TABLE,
        target: RouteTarget::Multipath(vec![
            Nexthop {
                via: Ipv4Addr::new(127, 0, 0, 2),
                dev: Interface::Index(loopback.index),
                weight: 1,
            },
            Nexthop {
                via: Ipv4Addr::new(127, 0, 0, 3),
                dev: Interface::Index(loopback.index),
                weight: 1,
            },
        ]),
    };
    let installed = kernel.add_route(&probe).await.is_ok();
    if installed {
        let _ = kernel
            .remove_route(RouteKey {
                destination,
                table: CAPABILITY_PROBE_TABLE,
            })
            .await;
    }
    installed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeNetlink;
    use crate::types::LinkInfo;

    #[tokio::test]
    async fn reports_full_support_when_lo_is_present() {
        let fake = FakeNetlink::with_links(vec![LinkInfo {
            index: 1,
            name: "lo".into(),
            is_up: true,
        }]);
        let caps = detect(&fake).await;
        assert_eq!(
            caps,
            KernelCapabilities {
                multiple_routing_tables: true,
                multipath_routes: true
            }
        );
        assert!(caps.ensure_supported().is_ok());
        // The probe route must not leak into the model it was checked against.
        assert!(
            fake.list_routes(Some(CAPABILITY_PROBE_TABLE))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn reports_missing_multipath_without_loopback() {
        let fake = FakeNetlink::new();
        let caps = detect(&fake).await;
        assert!(!caps.multipath_routes);
        let err = caps.ensure_supported().unwrap_err();
        assert!(err.missing_multipath_routes);
        assert!(err.to_string().contains("CONFIG_IP_ROUTE_MULTIPATH"));
    }
}
