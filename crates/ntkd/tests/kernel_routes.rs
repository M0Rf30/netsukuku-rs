//! Integration tests for [`ntkd::kernel::routes::RouteInstaller`] against
//! [`ntk_netlink::FakeNetlink`]'s recorded operation log — see the batch contract's
//! "route-installation semantics" for the exact behavior asserted here.

use ntkd::kernel;
use ntkd::kernel::routes::RouteInstaller;

use ntk_common::{Cost, HCoord, Naddr, Topology};
use ntk_netlink::{
    FakeNetlink, Interface, Ipv4Net, LinkInfo, Nexthop, Operation, RouteTarget, RuleSelector,
    RuleSpec,
};
use ntk_qspn::{ArcId, Hop, RouteEntry, RoutePath, RouteSnapshot};
use std::net::Ipv4Addr;

const TABLE: u32 = 200;
const PRIORITY: u32 = 9_990;

fn topology() -> Topology {
    Topology::new([4, 2, 2, 2]).unwrap()
}

fn naddr(pos: [u32; 4]) -> Naddr {
    Naddr::new(topology(), pos).unwrap()
}

fn fake() -> FakeNetlink {
    FakeNetlink::with_links(vec![
        LinkInfo {
            index: 1,
            name: "lo".into(),
            is_up: true,
        },
        LinkInfo {
            index: 2,
            name: "eth0".into(),
            is_up: true,
        },
        LinkInfo {
            index: 3,
            name: "eth1".into(),
            is_up: true,
        },
    ])
}

fn path(arc: u32, cost: Cost) -> RoutePath {
    RoutePath {
        arc: ArcId::from(arc),
        hops: vec![Hop {
            arc: ArcId::from(arc),
            coord: HCoord::new(0, 0),
        }],
        cost,
        nodes_inside: 1,
    }
}

fn snapshot_with(entries: Vec<RouteEntry>) -> RouteSnapshot {
    RouteSnapshot {
        levels: vec![entries],
    }
}

fn my_ip() -> Ipv4Addr {
    kernel::addressing::host_address(&naddr([1, 0, 1, 0]))
        .unwrap()
        .address()
}

#[tokio::test]
async fn reapplying_identical_snapshot_issues_zero_operations() {
    let kernel = fake();
    let mut installer = RouteInstaller::new(kernel, naddr([1, 0, 1, 0]), TABLE, PRIORITY);
    installer.set_arc_endpoint(
        ArcId::from(1),
        Ipv4Addr::new(192, 0, 2, 1),
        Interface::name("eth0"),
    );

    let dest = HCoord::new(1, 1);
    let snapshot = snapshot_with(vec![RouteEntry {
        destination: dest,
        paths: vec![path(1, Cost::Finite(10))],
    }]);

    let first = installer.apply(&snapshot).await.unwrap();
    assert_eq!(first.added, 1);

    installer.kernel_ref().clear_operations();
    let second = installer.apply(&snapshot).await.unwrap();
    assert_eq!(second, Default::default());
    assert!(installer.kernel_ref().operations().is_empty());
}

#[tokio::test]
async fn gateway_change_issues_exactly_one_change_route() {
    let kernel = fake();
    let mut installer = RouteInstaller::new(kernel, naddr([1, 0, 1, 0]), TABLE, PRIORITY);
    installer.set_arc_endpoint(
        ArcId::from(1),
        Ipv4Addr::new(192, 0, 2, 1),
        Interface::name("eth0"),
    );

    let dest = HCoord::new(1, 1);
    let snapshot = snapshot_with(vec![RouteEntry {
        destination: dest,
        paths: vec![path(1, Cost::Finite(10))],
    }]);
    installer.apply(&snapshot).await.unwrap();
    installer.kernel_ref().clear_operations();

    // Same arc, new gateway address -> the same destination's target changes.
    installer.set_arc_endpoint(
        ArcId::from(1),
        Ipv4Addr::new(192, 0, 2, 2),
        Interface::name("eth0"),
    );
    let delta = installer.apply(&snapshot).await.unwrap();
    assert_eq!(delta.changed, 1);
    assert_eq!(delta.added, 0);
    assert_eq!(delta.removed, 0);

    let ops = installer.kernel_ref().operations();
    assert_eq!(ops.len(), 1);
    assert!(matches!(ops[0], Operation::ChangeRoute(_)));
}

#[tokio::test]
async fn destination_appearing_and_vanishing_issues_add_then_remove() {
    let kernel = fake();
    let mut installer = RouteInstaller::new(kernel, naddr([1, 0, 1, 0]), TABLE, PRIORITY);
    installer.set_arc_endpoint(
        ArcId::from(1),
        Ipv4Addr::new(192, 0, 2, 1),
        Interface::name("eth0"),
    );

    let dest = HCoord::new(1, 1);
    let snapshot_with_dest = snapshot_with(vec![RouteEntry {
        destination: dest,
        paths: vec![path(1, Cost::Finite(10))],
    }]);
    let delta = installer.apply(&snapshot_with_dest).await.unwrap();
    assert_eq!(delta.added, 1);
    assert!(matches!(
        installer.kernel_ref().operations().as_slice(),
        [Operation::AddRoute(_)]
    ));

    installer.kernel_ref().clear_operations();
    let empty_snapshot = snapshot_with(vec![]);
    let delta = installer.apply(&empty_snapshot).await.unwrap();
    assert_eq!(delta.removed, 1);
    let ops = installer.kernel_ref().operations();
    assert_eq!(ops.len(), 1);
    let expected_dest = kernel::addressing::gnode_destination(&naddr([1, 0, 1, 0]), dest)
        .unwrap()
        .address();
    assert!(
        matches!(&ops[0], Operation::RemoveRoute(key) if key.destination.address() == expected_dest)
    );
}

#[tokio::test]
async fn multipath_destination_gets_best_first_weighted_nexthops() {
    let kernel = fake();
    let mut installer = RouteInstaller::new(kernel, naddr([1, 0, 1, 0]), TABLE, PRIORITY);
    installer.set_arc_endpoint(
        ArcId::from(1),
        Ipv4Addr::new(192, 0, 2, 1),
        Interface::name("eth0"),
    );
    installer.set_arc_endpoint(
        ArcId::from(2),
        Ipv4Addr::new(192, 0, 2, 2),
        Interface::name("eth1"),
    );

    let dest = HCoord::new(1, 1);
    // Ascending cost, as RouteSnapshot's contract requires.
    let snapshot = snapshot_with(vec![RouteEntry {
        destination: dest,
        paths: vec![path(1, Cost::Finite(10)), path(2, Cost::Finite(20))],
    }]);
    installer.apply(&snapshot).await.unwrap();

    let ops = installer.kernel_ref().operations();
    assert_eq!(ops.len(), 1);
    let Operation::AddRoute(spec) = &ops[0] else {
        panic!("expected AddRoute, got {:?}", ops[0]);
    };
    assert_eq!(
        spec.target,
        RouteTarget::Multipath(vec![
            Nexthop {
                via: Ipv4Addr::new(192, 0, 2, 1),
                dev: Interface::name("eth0"),
                weight: 255,
            },
            Nexthop {
                via: Ipv4Addr::new(192, 0, 2, 2),
                dev: Interface::name("eth1"),
                weight: 127,
            },
        ])
    );
}

#[tokio::test]
async fn unknown_arc_endpoint_is_unreachable_until_recorded() {
    let kernel = fake();
    let mut installer = RouteInstaller::new(kernel, naddr([1, 0, 1, 0]), TABLE, PRIORITY);

    let dest = HCoord::new(1, 1);
    let snapshot = snapshot_with(vec![RouteEntry {
        destination: dest,
        paths: vec![path(1, Cost::Finite(10))],
    }]);
    installer.apply(&snapshot).await.unwrap();
    let ops = installer.kernel_ref().operations();
    let Operation::AddRoute(spec) = &ops[0] else {
        panic!("expected AddRoute");
    };
    assert_eq!(spec.target, RouteTarget::Unreachable);

    installer.kernel_ref().clear_operations();
    installer.set_arc_endpoint(
        ArcId::from(1),
        Ipv4Addr::new(192, 0, 2, 1),
        Interface::name("eth0"),
    );
    let delta = installer.apply(&snapshot).await.unwrap();
    assert_eq!(delta.changed, 1);
    let ops = installer.kernel_ref().operations();
    let Operation::ChangeRoute(spec) = &ops[0] else {
        panic!("expected ChangeRoute");
    };
    assert!(matches!(spec.target, RouteTarget::Gateway { .. }));
}

#[tokio::test]
async fn clearing_arc_endpoint_makes_its_destinations_unreachable_again() {
    let kernel = fake();
    let mut installer = RouteInstaller::new(kernel, naddr([1, 0, 1, 0]), TABLE, PRIORITY);

    let dest = HCoord::new(1, 1);
    let snapshot = snapshot_with(vec![RouteEntry {
        destination: dest,
        paths: vec![path(1, Cost::Finite(10))],
    }]);
    installer.set_arc_endpoint(
        ArcId::from(1),
        Ipv4Addr::new(192, 0, 2, 1),
        Interface::name("eth0"),
    );
    installer.apply(&snapshot).await.unwrap();
    let Operation::AddRoute(spec) = &installer.kernel_ref().operations()[0] else {
        panic!("expected AddRoute");
    };
    assert!(matches!(spec.target, RouteTarget::Gateway { .. }));

    installer.kernel_ref().clear_operations();
    installer.clear_arc_endpoint(ArcId::from(1));
    let delta = installer.apply(&snapshot).await.unwrap();
    assert_eq!(delta.changed, 1);
    let Operation::ChangeRoute(spec) = &installer.kernel_ref().operations()[0] else {
        panic!("expected ChangeRoute");
    };
    assert_eq!(spec.target, RouteTarget::Unreachable);
}

#[tokio::test]
async fn install_identity_then_teardown_is_the_exact_inverse() {
    let kernel = fake();
    let mut installer = RouteInstaller::new(kernel, naddr([1, 0, 1, 0]), TABLE, PRIORITY);

    installer.install_identity().await.unwrap();
    installer.teardown().await.unwrap();

    let address = Ipv4Net::host(my_ip());
    let expected = vec![
        Operation::AddAddress {
            interface: Interface::name("lo"),
            network: address,
        },
        Operation::AddRule(RuleSpec {
            table: TABLE,
            priority: PRIORITY,
            selector: RuleSelector::Any,
        }),
        Operation::RemoveRule(RuleSpec {
            table: TABLE,
            priority: PRIORITY,
            selector: RuleSelector::Any,
        }),
        Operation::RemoveAddress {
            interface: Interface::name("lo"),
            network: address,
        },
    ];
    assert_eq!(installer.kernel_ref().operations(), expected);
}

#[tokio::test]
async fn mid_batch_failure_recovers_on_the_next_apply_without_replaying_the_succeeded_destination()
{
    let kernel = fake();
    let mut installer = RouteInstaller::new(kernel, naddr([1, 0, 1, 0]), TABLE, PRIORITY);
    installer.set_arc_endpoint(
        ArcId::from(1),
        Ipv4Addr::new(192, 0, 2, 1),
        Interface::name("eth0"),
    );

    // dest_a sorts before dest_b in the installer's internal BTreeMap, so a single apply()
    // call processes dest_a's add_route first and dest_b's second.
    let dest_a = HCoord::new(0, 2);
    let dest_b = HCoord::new(1, 1);
    let snapshot = snapshot_with(vec![
        RouteEntry {
            destination: dest_a,
            paths: vec![path(1, Cost::Finite(10))],
        },
        RouteEntry {
            destination: dest_b,
            paths: vec![path(1, Cost::Finite(10))],
        },
    ]);

    let dest_b_net = kernel::addressing::gnode_destination(&naddr([1, 0, 1, 0]), dest_b).unwrap();
    installer.kernel_ref().arm_route_failure(
        dest_b_net,
        ntk_netlink::NetlinkError::AlreadyExists("simulated mid-batch failure".into()),
    );

    // First apply(): dest_a's add_route lands, dest_b's fails and aborts the batch.
    installer.apply(&snapshot).await.unwrap_err();
    let ops = installer.kernel_ref().operations();
    let dest_a_net = kernel::addressing::gnode_destination(&naddr([1, 0, 1, 0]), dest_a).unwrap();
    assert!(
        matches!(ops.as_slice(), [Operation::AddRoute(spec)] if spec.destination == dest_a_net),
        "expected exactly dest_a's AddRoute to have landed, got {ops:?}"
    );

    // Second apply() of the SAME snapshot: dest_a must not be replayed (no AlreadyExists), and
    // dest_b must now succeed, converging the installer's state with the kernel's.
    installer.kernel_ref().clear_operations();
    let delta = installer.apply(&snapshot).await.unwrap();
    assert_eq!(delta.added, 1);
    assert_eq!(delta.changed, 0);
    assert_eq!(delta.removed, 0);
    let ops = installer.kernel_ref().operations();
    assert!(
        matches!(ops.as_slice(), [Operation::AddRoute(spec)] if spec.destination == dest_b_net),
        "expected exactly dest_b's AddRoute on the retry, got {ops:?}"
    );

    // A third apply() of the same snapshot is now a true no-op: both destinations converged.
    installer.kernel_ref().clear_operations();
    let delta = installer.apply(&snapshot).await.unwrap();
    assert_eq!(delta, Default::default());
    assert!(installer.kernel_ref().operations().is_empty());
}

#[tokio::test]
async fn destination_that_fails_to_apply_then_vanishes_leaks_no_kernel_route() {
    let kernel = fake();
    let mut installer = RouteInstaller::new(kernel, naddr([1, 0, 1, 0]), TABLE, PRIORITY);
    installer.set_arc_endpoint(
        ArcId::from(1),
        Ipv4Addr::new(192, 0, 2, 1),
        Interface::name("eth0"),
    );

    let dest = HCoord::new(1, 1);
    let snapshot_with_dest = snapshot_with(vec![RouteEntry {
        destination: dest,
        paths: vec![path(1, Cost::Finite(10))],
    }]);
    let dest_net = kernel::addressing::gnode_destination(&naddr([1, 0, 1, 0]), dest).unwrap();
    installer.kernel_ref().arm_route_failure(
        dest_net,
        ntk_netlink::NetlinkError::AlreadyExists("simulated failure".into()),
    );

    // The only destination in the batch fails to apply: nothing landed in the kernel, and
    // nothing should be recorded as applied.
    installer.apply(&snapshot_with_dest).await.unwrap_err();
    assert!(installer.kernel_ref().operations().is_empty());

    // The destination then vanishes from the next snapshot entirely. Since it was never
    // recorded as applied (its add_route never actually landed in the kernel), there is
    // nothing to remove: this must issue zero operations, never a `remove_route` against a
    // route the kernel was never given.
    installer.kernel_ref().clear_operations();
    let empty_snapshot = snapshot_with(vec![]);
    let delta = installer.apply(&empty_snapshot).await.unwrap();
    assert_eq!(delta, Default::default());
    assert!(installer.kernel_ref().operations().is_empty());
}
