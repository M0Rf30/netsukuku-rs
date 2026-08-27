//! Installing one identity's kernel routing state to match QSPN's routing decisions.
//!
//! Replaces `identity_ip_commands.vala`'s `ip route change ... table ntk` calls
//! (`research/impl/vala/ntkd/identity_ip_commands.vala:533-560`) with real, diffed netlink
//! mutations: [`RouteInstaller::apply`] issues only the operations a changed
//! [`ntk_qspn::RouteSnapshot`] actually requires, never a blind reinstall of everything.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use ntk_common::{Cost, HCoord, Naddr};
use ntk_netlink::{
    Interface, Ipv4Net, Netlink, Nexthop, RouteKey, RouteSpec, RouteTarget, RuleSelector, RuleSpec,
};
use ntk_qspn::{ArcId, RoutePath, RouteSnapshot};

use crate::kernel::addressing::{self, AddressingError};

/// The interface an identity's own routable address is installed on.
///
/// Upstream installs the identity's global `/32` on `lo` *in addition to* every physical `dev`
/// (`identity_ip_commands.vala:37-49`) — the per-device copies exist only to support anonymizing
/// NAT's `POSTROUTING` rewrite, which this daemon deliberately omits (see the batch contract's
/// NAT scope note). Without NAT, the address only needs to exist somewhere the kernel considers
/// local for routing purposes, so `lo` alone is kept.
const IDENTITY_ADDRESS_INTERFACE: &str = "lo";

/// Counts of route mutations one [`RouteInstaller::apply`] call issued.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AppliedDelta {
    /// Destinations that newly appeared (`add_route`).
    pub added: usize,
    /// Destinations whose target changed (`change_route`).
    pub changed: usize,
    /// Destinations that disappeared (`remove_route`).
    pub removed: usize,
}

/// Owns one identity's kernel routing state and reconciles it against QSPN's published
/// [`RouteSnapshot`]s.
#[derive(Debug)]
pub struct RouteInstaller<K> {
    kernel: K,
    my_naddr: Naddr,
    table: u32,
    rule_priority: u32,
    /// Gateway endpoint for each currently-known local arc, keyed by [`ArcId`] — the arc→(via,
    /// dev) mapping a path's leading arc resolves through to become a real `RouteTarget`.
    arc_endpoints: BTreeMap<ArcId, (Ipv4Addr, Interface)>,
    /// The last route actually installed per destination, so [`RouteInstaller::apply`] can diff
    /// against it instead of blindly reinstalling everything.
    applied: BTreeMap<HCoord, RouteSpec>,
    /// This identity's own address, once [`RouteInstaller::install_identity`] has added it.
    identity_address: Option<Ipv4Net>,
}

impl<K> RouteInstaller<K> {
    /// Test-only accessor to the underlying kernel handle, so integration tests can assert on
    /// [`ntk_netlink::FakeNetlink`]'s recorded operation log directly.
    #[cfg(any(test, feature = "test-util"))]
    pub fn kernel_ref(&self) -> &K {
        &self.kernel
    }
}

impl<K: Netlink> RouteInstaller<K> {
    /// Builds an installer for one identity, over the already-allocated `table`/`rule_priority`
    /// pair (see [`ntk_netlink::TableAllocator::acquire`]).
    pub fn new(kernel: K, my_naddr: Naddr, table: u32, rule_priority: u32) -> Self {
        Self {
            kernel,
            my_naddr,
            table,
            rule_priority,
            arc_endpoints: BTreeMap::new(),
            applied: BTreeMap::new(),
            identity_address: None,
        }
    }

    /// Transitions this installer to `real_naddr` once a migrating identity's negotiated
    /// position is confirmed reachable (`ntkd::node::lifecycle::migrate`'s bootstrap-complete
    /// wait) — the counterpart to constructing this installer with a virtual placeholder
    /// [`Naddr`] up front so [`Self::install_identity`]/[`Self::apply`] stay real no-ops for the
    /// whole migration window. Never installs anything itself: the caller still needs its own
    /// follow-up [`Self::install_identity`]/[`Self::apply`] calls once this returns, exactly as
    /// if a fresh, real-`Naddr`-constructed installer had been used from the start.
    pub fn realize(&mut self, real_naddr: Naddr) {
        self.my_naddr = real_naddr;
    }

    /// Records the gateway endpoint of `arc`: the neighbour address and outgoing interface a
    /// path whose leading arc is `arc` should route through.
    pub fn set_arc_endpoint(&mut self, arc: ArcId, via: Ipv4Addr, dev: Interface) {
        self.arc_endpoints.insert(arc, (via, dev));
    }

    /// Forgets `arc`'s endpoint. Any not-yet-reapplied path leading with `arc` becomes
    /// unreachable the next time [`RouteInstaller::apply`] runs.
    pub fn clear_arc_endpoint(&mut self, arc: ArcId) {
        self.arc_endpoints.remove(&arc);
    }

    /// Installs this identity's own address (on [`IDENTITY_ADDRESS_INTERFACE`]) and its
    /// catch-all [`RuleSelector::Any`] rule at `rule_priority`.
    ///
    /// A no-op, deliberately, while [`Naddr::is_virtual`] — a migrating identity holds a
    /// virtual position for exactly the window between "the Coordinator resolved a slot" and
    /// "qspn has verified it is actually reachable", and a virtual position's own out-of-range
    /// (`pos >= gsize(level)`) encoding is not a valid bit pattern for
    /// [`addressing::host_address`]'s packing — installing it would not fail loudly, it would
    /// silently claim a wrong `/32` (the out-of-range value corrupting adjacent levels' bits in
    /// the packed accumulator) or a right-looking one that stops being right the moment a real
    /// position is finally assigned. Suppressing here rather than erroring keeps a caller that
    /// constructs a [`RouteInstaller`] before a position resolves working exactly as if it had
    /// waited to construct one at all — [`Self::identity_address`] simply stays `None` until a
    /// caller re-invokes this once [`Naddr::is_virtual`] is false (a fresh, real [`Naddr`]).
    ///
    /// # Errors
    /// [`RouteError::Addressing`] if this identity's [`Naddr`] does not fit the `10.0.0.0/8`
    /// address space; [`RouteError::Netlink`] if either kernel mutation fails.
    pub async fn install_identity(&mut self) -> Result<(), RouteError> {
        if self.my_naddr.is_virtual() {
            return Ok(());
        }
        let address = addressing::host_address(&self.my_naddr)?;
        self.kernel
            .add_address(&Interface::name(IDENTITY_ADDRESS_INTERFACE), address)
            .await?;
        self.kernel
            .add_rule(&RuleSpec {
                table: self.table,
                priority: self.rule_priority,
                selector: RuleSelector::Any,
            })
            .await?;
        self.identity_address = Some(address);
        Ok(())
    }

    /// Diffs `snapshot` against the last applied one and issues only the resulting deltas:
    /// `add_route` for a newly-reachable destination, `change_route` for one whose target
    /// changed, `remove_route` for one that dropped out of the snapshot, and nothing for an
    /// unchanged destination.
    ///
    /// A no-op, deliberately, while [`Naddr::is_virtual`] — see [`Self::install_identity`]'s
    /// doc for why: [`addressing::gnode_destination`] reads every level above a destination's
    /// own from this identity's `my_naddr`, so a virtual position anywhere in it can corrupt an
    /// otherwise-real destination's computed CIDR, not merely the identity's own address. Never
    /// silently drops a *previously* applied real route, either — with nothing new to diff
    /// against, the last-known-good `self.applied` set is left exactly as it was rather than
    /// torn down, so a caller must still call [`Self::teardown`] explicitly to remove it.
    ///
    /// A destination whose [`HCoord`] [`addressing::gnode_destination`] cannot encode (an
    /// out-of-range position that reached this far — `ntk_qspn::check_incoming_message` rejects
    /// it before it is ever admitted into routing state, but this is still the last line of
    /// defense) is logged via `tracing::warn!` and dropped from the diff, never propagated as an
    /// error: one such entry must not abort every other, unrelated destination's route
    /// maintenance for this identity, or a single malformed snapshot becomes a denial of
    /// service. Because the dropped entry is simply absent from the diffed set, any route it
    /// previously had installed is withdrawn by the same "destination missing from the new
    /// snapshot" path below that already handles a destination going unreachable — a stale route
    /// this update can no longer vouch for (the destination may have moved) is left installed
    /// nowhere rather than left pointing at a CIDR nothing currently corroborates.
    ///
    /// # Errors
    /// [`RouteError::Netlink`] if a kernel mutation fails.
    pub async fn apply(&mut self, snapshot: &RouteSnapshot) -> Result<AppliedDelta, RouteError> {
        if self.my_naddr.is_virtual() {
            return Ok(AppliedDelta::default());
        }
        let src = self.identity_address.map(|net| net.address());
        let mut next = BTreeMap::new();
        for entry in snapshot.levels.iter().flatten() {
            let destination = match addressing::gnode_destination(&self.my_naddr, entry.destination)
            {
                Ok(destination) => destination,
                Err(error) => {
                    tracing::warn!(
                        level = entry.destination.level,
                        pos = entry.destination.pos,
                        %error,
                        "route snapshot entry dropped: destination cannot be encoded"
                    );
                    continue;
                }
            };
            let target = route_target(&entry.paths, &self.arc_endpoints, src);
            next.insert(
                entry.destination,
                RouteSpec {
                    destination,
                    table: self.table,
                    target,
                },
            );
        }

        let mut delta = AppliedDelta::default();
        for (destination, spec) in &next {
            match self.applied.get(destination) {
                None => {
                    self.kernel.add_route(spec).await?;
                    delta.added += 1;
                }
                Some(previous) if previous != spec => {
                    self.kernel.change_route(spec).await?;
                    delta.changed += 1;
                }
                Some(_) => {}
            }
        }
        for (destination, previous) in &self.applied {
            if !next.contains_key(destination) {
                self.kernel
                    .remove_route(RouteKey {
                        destination: previous.destination,
                        table: previous.table,
                    })
                    .await?;
                delta.removed += 1;
            }
        }

        self.applied = next;
        Ok(delta)
    }

    /// Removes everything this installer added: every currently-applied route, then the
    /// catch-all rule, then the identity address — the reverse of the order
    /// [`RouteInstaller::install_identity`]/[`RouteInstaller::apply`] added them in.
    ///
    /// # Errors
    /// [`RouteError::Netlink`] if a kernel mutation fails.
    pub async fn teardown(&mut self) -> Result<(), RouteError> {
        for (_, spec) in std::mem::take(&mut self.applied) {
            self.kernel
                .remove_route(RouteKey {
                    destination: spec.destination,
                    table: spec.table,
                })
                .await?;
        }
        if let Some(address) = self.identity_address.take() {
            self.kernel
                .remove_rule(&RuleSpec {
                    table: self.table,
                    priority: self.rule_priority,
                    selector: RuleSelector::Any,
                })
                .await?;
            self.kernel
                .remove_address(&Interface::name(IDENTITY_ADDRESS_INTERFACE), address)
                .await?;
        }
        Ok(())
    }
}

/// A usable path: its leading arc resolved to a real gateway/interface.
struct Usable {
    via: Ipv4Addr,
    dev: Interface,
    cost: Cost,
}

/// Resolves every path's leading arc against `endpoints`, dropping any path whose arc has no
/// recorded endpoint or whose cost is [`Cost::Dead`] (unreachable regardless of endpoint
/// knowledge). `paths` is ascending cost per [`ntk_qspn::RouteSnapshot`]'s contract, so the
/// filtered order stays best-first.
fn resolve_usable(
    paths: &[RoutePath],
    endpoints: &BTreeMap<ArcId, (Ipv4Addr, Interface)>,
) -> Vec<Usable> {
    paths
        .iter()
        .filter(|path| path.cost != Cost::Dead)
        .filter_map(|path| {
            let (via, dev) = endpoints.get(&path.arc)?;
            Some(Usable {
                via: *via,
                dev: dev.clone(),
                cost: path.cost,
            })
        })
        .collect()
}

/// Builds the [`RouteTarget`] for one destination's paths: [`RouteTarget::Unreachable`] if no
/// path's leading arc is known, [`RouteTarget::Gateway`] for exactly one usable path (after
/// [`dedupe_by_gateway`] collapses any sharing a kernel-visible gateway), or
/// [`RouteTarget::Multipath`] for several — see [`nexthop_weight`] for the weight derivation.
fn route_target(
    paths: &[RoutePath],
    endpoints: &BTreeMap<ArcId, (Ipv4Addr, Interface)>,
    src: Option<Ipv4Addr>,
) -> RouteTarget {
    let usable = dedupe_by_gateway(resolve_usable(paths, endpoints));
    match usable.as_slice() {
        [] => RouteTarget::Unreachable,
        [only] => RouteTarget::Gateway {
            via: only.via,
            dev: only.dev.clone(),
            src,
        },
        several => {
            let best = several
                .iter()
                .map(|u| effective_cost(u.cost))
                .min()
                .expect("non-empty slice");
            RouteTarget::Multipath(
                several
                    .iter()
                    .map(|u| Nexthop {
                        via: u.via,
                        dev: u.dev.clone(),
                        weight: nexthop_weight(u.cost, best),
                    })
                    .collect(),
            )
        }
    }
}

/// Collapses [`Usable`] paths that resolve to the same kernel-visible gateway `(via, dev)` into
/// one entry each.
///
/// A kernel FIB nexthop is identified by `(via, dev)` alone — qspn, however, may legitimately
/// admit several [`RoutePath`]s to the same destination that share a *leading* arc and diverge
/// only afterwards (their full paths differ, but this node's next hop is identical). Installing
/// one nexthop per such path would register the same `(via, dev)` pair twice in one
/// [`RouteTarget::Multipath`], which every netlink backend this crate targets either rejects or
/// silently collapses itself — either way, not the deliberate weight split this crate computes.
///
/// `usable` arrives best-first (ascending cost, [`resolve_usable`]'s doc), so keeping the first
/// occurrence of each `(via, dev)` keeps its cheapest path — the most favorable weight that
/// gateway can offer. Merging by *summing* the colliding paths' weights was considered and
/// rejected: once traffic is handed to a gateway it leaves this node's control at that first
/// hop, so two qspn paths sharing one only ever describe one physical link's worth of egress
/// capacity, not two — summing would double-count it and unfairly starve a genuinely distinct
/// gateway's share of the split.
fn dedupe_by_gateway(usable: Vec<Usable>) -> Vec<Usable> {
    let mut seen = std::collections::HashSet::new();
    usable
        .into_iter()
        .filter(|u| seen.insert((u.via, u.dev.clone())))
        .collect()
}

/// A path's cost as a strictly positive magnitude for weight-ratio purposes:
/// [`Cost::Null`] (a trivial zero-cost path) is treated as `1` so it still receives the maximum
/// share rather than triggering a division by zero, and larger [`Cost::Finite`] magnitudes are
/// used as-is (also floored at `1`).
fn effective_cost(cost: Cost) -> u64 {
    match cost {
        Cost::Null => 1,
        Cost::Finite(magnitude) => magnitude.max(1),
        Cost::Dead => unreachable!("Dead-cost paths are filtered out by resolve_usable"),
    }
}

/// Derives a [`Nexthop::weight`] from `cost` relative to the cheapest usable path's cost `best`.
///
/// [`Nexthop::weight`] is the raw kernel field, where the *real* ECMP share the kernel gives a
/// nexthop is `weight + 1` (0..=255 stored, 1..=256 real). This function gives the cheapest path
/// the maximum real weight (256) and scales every other path's real weight down by
/// `best / effective_cost(cost)`, floored at a real weight of 1 (stored `0`) so no reachable path
/// is ever starved to zero traffic — upstream never installed multipath routes at all
/// (`research/notes/03-specs-and-rfcs.md` RFC 0013), so this ratio is this crate's own design
/// choice: proportional-to-inverse-cost is the standard weighted-ECMP heuristic (lower latency/
/// metric gets more traffic), applied here as the simplest ratio that is monotonic, bounded, and
/// exactly reproduces equal weights for equal costs.
fn nexthop_weight(cost: Cost, best: u64) -> u8 {
    let effective = effective_cost(cost);
    let real_weight = (256u128 * u128::from(best) / u128::from(effective)).clamp(1, 256);
    (real_weight - 1) as u8
}

/// Everything that can go wrong installing or diffing one identity's kernel routing state.
#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    /// A kernel-state mutation failed.
    #[error(transparent)]
    Netlink(#[from] ntk_netlink::NetlinkError),
    /// A [`Naddr`]/[`HCoord`] could not be translated into the `10.0.0.0/8` address space.
    #[error(transparent)]
    Addressing(#[from] AddressingError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntk_qspn::RouteEntry;

    #[test]
    fn nexthop_weight_gives_cheapest_path_max_weight() {
        let best = effective_cost(Cost::Finite(10));
        assert_eq!(nexthop_weight(Cost::Finite(10), best), 255);
        // 256 * 10 / 20 = 128 real weight -> stored 127.
        assert_eq!(nexthop_weight(Cost::Finite(20), best), 127);
        // 256 * 10 / 1000 rounds down to 2 real weight -> stored 1, never starved to 0.
        assert_eq!(nexthop_weight(Cost::Finite(1000), best), 1);
    }

    #[test]
    fn nexthop_weight_treats_null_cost_as_best() {
        let best = effective_cost(Cost::Null);
        assert_eq!(nexthop_weight(Cost::Null, best), 255);
    }

    fn path(arc: u32, cost: u64) -> RoutePath {
        RoutePath {
            arc: ArcId::from(arc),
            hops: Vec::new(),
            cost: Cost::Finite(cost),
            nodes_inside: 0,
        }
    }

    /// Two paths sharing a leading arc (they diverge only downstream of it) must collapse into
    /// exactly one kernel nexthop — a FIB nexthop is `(via, dev)`, not "which qspn path chose
    /// it" — while a path over a genuinely distinct arc keeps its own nexthop.
    #[test]
    fn route_target_dedupes_nexthops_sharing_a_gateway() {
        let via1: Ipv4Addr = "169.254.1.1".parse().unwrap();
        let via2: Ipv4Addr = "169.254.2.2".parse().unwrap();
        let dev1 = Interface::name("eth0");
        let dev2 = Interface::name("eth1");
        let mut endpoints = BTreeMap::new();
        endpoints.insert(ArcId::from(1), (via1, dev1.clone()));
        endpoints.insert(ArcId::from(2), (via2, dev2.clone()));

        // Ascending cost, as `RouteSnapshot` guarantees: arc 1 twice (a cheaper and a costlier
        // path sharing that same first hop), then arc 2 once.
        let paths = vec![path(1, 10), path(1, 15), path(2, 20)];

        let target = route_target(&paths, &endpoints, None);
        let RouteTarget::Multipath(nexthops) = target else {
            panic!("expected a multipath target, got {target:?}");
        };
        assert_eq!(
            nexthops.len(),
            2,
            "the two arc-1 paths must collapse into a single nexthop: {nexthops:?}"
        );
        let best = effective_cost(Cost::Finite(10));
        assert_eq!(
            nexthops,
            vec![
                Nexthop {
                    via: via1,
                    dev: dev1,
                    weight: nexthop_weight(Cost::Finite(10), best),
                },
                Nexthop {
                    via: via2,
                    dev: dev2,
                    weight: nexthop_weight(Cost::Finite(20), best),
                },
            ]
        );
    }

    /// Once dedupe collapses every path down to one gateway, the target must be a plain
    /// [`RouteTarget::Gateway`], never a one-element `Multipath`.
    #[test]
    fn route_target_collapses_single_gateway_to_plain_gateway() {
        let via: Ipv4Addr = "169.254.9.9".parse().unwrap();
        let dev = Interface::name("eth0");
        let mut endpoints = BTreeMap::new();
        endpoints.insert(ArcId::from(1), (via, dev.clone()));

        let paths = vec![path(1, 10), path(1, 25)];
        let target = route_target(&paths, &endpoints, None);
        assert_eq!(
            target,
            RouteTarget::Gateway {
                via,
                dev,
                src: None,
            }
        );
    }

    /// A migrating identity's [`RouteInstaller`] must never touch the kernel while
    /// [`Naddr::is_virtual`] — see [`RouteInstaller::install_identity`]/[`RouteInstaller::apply`]'s
    /// own docs for why a virtual position's out-of-range encoding would otherwise corrupt the
    /// packed `10.0.0.0/8` bit pattern rather than merely fail.
    #[tokio::test]
    async fn virtual_naddr_suppresses_both_identity_and_route_installation() {
        let topology = ntk_common::Topology::new([4, 2]).unwrap();
        // Level 0's position is out of range (gsize(0) == 4) — virtual by construction.
        let virtual_naddr = Naddr::new_allowing_virtual(topology.clone(), [4, 0]).unwrap();
        assert!(virtual_naddr.is_virtual());

        let kernel = ntk_netlink::FakeNetlink::with_links(vec![ntk_netlink::LinkInfo {
            index: 1,
            name: "lo".into(),
            is_up: true,
        }]);
        let mut installer = RouteInstaller::new(kernel, virtual_naddr.clone(), 200, 9_990);

        installer
            .install_identity()
            .await
            .expect("a virtual naddr must not error, just skip");
        assert!(
            installer.kernel_ref().operations().is_empty(),
            "no address/rule must ever be installed for a virtual position"
        );

        let snapshot = RouteSnapshot {
            levels: vec![
                vec![RouteEntry {
                    destination: HCoord::new(0, 1),
                    paths: vec![RoutePath {
                        arc: ArcId::from(1),
                        hops: Vec::new(),
                        cost: Cost::Finite(10),
                        nodes_inside: 1,
                    }],
                }],
                Vec::new(),
            ],
        };
        let delta = installer
            .apply(&snapshot)
            .await
            .expect("a virtual naddr must not error, just skip");
        assert_eq!(
            delta,
            AppliedDelta::default(),
            "no route diff must ever be computed against a virtual position"
        );
        assert!(
            installer.kernel_ref().operations().is_empty(),
            "apply must not touch the kernel while the position is virtual, even with real \
             destinations in the snapshot"
        );

        // Once a real (non-virtual) naddr is known, a fresh installer behaves normally.
        let real_naddr = Naddr::new(topology, [1, 0]).unwrap();
        let mut real_installer = RouteInstaller::new(
            ntk_netlink::FakeNetlink::with_links(vec![ntk_netlink::LinkInfo {
                index: 1,
                name: "lo".into(),
                is_up: true,
            }]),
            real_naddr,
            200,
            9_990,
        );
        real_installer
            .install_identity()
            .await
            .expect("a real naddr installs normally");
        assert!(!real_installer.kernel_ref().operations().is_empty());
    }

    /// [`RouteInstaller::realize`] must let the *same* installer transition from virtual
    /// (no-op) to real (actually installs) in place, keeping arc endpoints recorded while still
    /// virtual — [`ntkd::node::lifecycle::migrate`]'s own "confirm bootstrap, then realize"
    /// sequence depends on this rather than discarding and rebuilding the installer.
    #[tokio::test]
    async fn realize_transitions_a_virtual_installer_to_real_in_place() {
        let topology = ntk_common::Topology::new([4, 2]).unwrap();
        let virtual_naddr = Naddr::new_allowing_virtual(topology.clone(), [4, 0]).unwrap();
        let kernel = ntk_netlink::FakeNetlink::with_links(vec![ntk_netlink::LinkInfo {
            index: 1,
            name: "lo".into(),
            is_up: true,
        }]);
        let mut installer = RouteInstaller::new(kernel, virtual_naddr, 200, 9_990);
        let via: Ipv4Addr = "169.254.1.1".parse().unwrap();
        installer.set_arc_endpoint(ArcId::from(1), via, Interface::name("eth0"));

        installer
            .install_identity()
            .await
            .expect("still virtual: must not error");
        assert!(
            installer.kernel_ref().operations().is_empty(),
            "still virtual: no kernel writes yet"
        );

        let real_naddr = Naddr::new(topology, [1, 0]).unwrap();
        installer.realize(real_naddr);
        installer
            .install_identity()
            .await
            .expect("now real: installs for the first time");
        assert!(
            !installer.kernel_ref().operations().is_empty(),
            "realize must let install_identity actually write kernel state"
        );

        let snapshot = RouteSnapshot {
            levels: vec![
                vec![RouteEntry {
                    destination: HCoord::new(0, 1),
                    paths: vec![RoutePath {
                        arc: ArcId::from(1),
                        hops: Vec::new(),
                        cost: Cost::Finite(10),
                        nodes_inside: 1,
                    }],
                }],
                Vec::new(),
            ],
        };
        let delta = installer.apply(&snapshot).await.unwrap();
        assert_eq!(
            delta.added, 1,
            "the arc endpoint recorded while virtual must resolve now that the position is real"
        );
    }

    /// A snapshot with one out-of-range destination alongside several valid ones must not
    /// abort — every valid destination's diff still applies, and the malformed one is dropped
    /// (never surfaced as an `Err`, per [`RouteInstaller::apply`]'s doc). This is the DoS-vs-
    /// integrity tradeoff this test exists to pin: before this behavior, a single bad entry
    /// aborted the whole snapshot and *zero* routes were installed or updated for it.
    #[tokio::test]
    async fn apply_skips_unencodable_entry_and_withdraws_its_previous_route() {
        let topology = ntk_common::Topology::new([4, 2, 2, 2]).unwrap();
        let naddr = Naddr::new(topology, [1, 0, 1, 0]).unwrap();
        let kernel = ntk_netlink::FakeNetlink::with_links(vec![ntk_netlink::LinkInfo {
            index: 1,
            name: "lo".into(),
            is_up: true,
        }]);
        let mut installer = RouteInstaller::new(kernel, naddr, 200, 9_990);

        let stable = HCoord::new(1, 1); // gsize(1) == 2: valid.
        let stale = HCoord::new(2, 0); // gsize(2) == 2: valid, but dropped in round 2.

        let baseline = RouteSnapshot {
            levels: vec![
                Vec::new(),
                vec![RouteEntry {
                    destination: stable,
                    paths: vec![path(1, 10)],
                }],
                vec![RouteEntry {
                    destination: stale,
                    paths: vec![path(2, 10)],
                }],
                Vec::new(),
            ],
        };
        let baseline_delta = installer.apply(&baseline).await.unwrap();
        assert_eq!(baseline_delta.added, 2, "both valid destinations install");
        installer.kernel_ref().clear_operations();

        // Round 2: `stable` is unchanged, `stale`'s slot is replaced by an out-of-range
        // position (pos 7 needs 3 bits, level 2's field only has 1 -- gsize(2) == 2), and a
        // brand-new valid destination `fresh` is added.
        let fresh = HCoord::new(0, 3); // gsize(0) == 4: valid.
        let corrupt = HCoord::new(2, 7); // gsize(2) == 2: out of range.
        let next_snapshot = RouteSnapshot {
            levels: vec![
                vec![RouteEntry {
                    destination: fresh,
                    paths: vec![path(3, 10)],
                }],
                vec![RouteEntry {
                    destination: stable,
                    paths: vec![path(1, 10)],
                }],
                vec![RouteEntry {
                    destination: corrupt,
                    paths: vec![path(4, 10)],
                }],
                Vec::new(),
            ],
        };
        let delta = installer
            .apply(&next_snapshot)
            .await
            .expect("one unencodable entry must not abort the whole snapshot");
        assert_eq!(delta.added, 1, "fresh is added");
        assert_eq!(delta.changed, 0, "stable is unchanged, no op issued for it");
        assert_eq!(
            delta.removed, 1,
            "stale's previously-installed route is withdrawn: a corrupt update can no longer \
             vouch for a CIDR it can no longer even name, so the safer state is unrouted, not a \
             route nothing currently corroborates"
        );

        let ops = installer.kernel_ref().operations();
        assert_eq!(
            ops.len(),
            2,
            "exactly the good destinations' ops, never zero and never the bad one: {ops:?}"
        );
        assert!(
            matches!(&ops[0], ntk_netlink::Operation::AddRoute(spec) if spec.table == 200),
            "fresh must still be added: {ops:?}"
        );
        assert!(
            matches!(&ops[1], ntk_netlink::Operation::RemoveRoute(key) if key.table == 200),
            "stale's stale route must be withdrawn: {ops:?}"
        );
    }
}
