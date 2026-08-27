# ntk-neighborhood

Link/arc discovery and liveness monitoring for netsukuku-rs — the Rust port
of Vala's `neighborhood/` module.

## Where this sits

`ntk-neighborhood` depends on `ntk-common`, `ntk-proto`, `ntk-rpc`, and
`ntk-netlink` (for [`ntk_netlink::TopologyQuery`], used to enumerate which
local NICs participate). `ntk-qspn` and every other module crate treat it as
the source of truth for "which arcs exist and at what cost" — `ntkd` reads
its arc snapshots and feeds them into `ntk-qspn` as arc costs change. This
crate has no sibling protocol dependency of its own: it only talks to the
wire and the kernel routing-table seam.

Its scope stops at discovery and liveness: it runs the UDP-broadcast 3-way
handshake (`here_i_am` / `request_arc` / `can_you_export`), tracks each
[`Arc`]'s lifecycle, keeps link cost fresh via periodic TCP `nop()` probing,
and republishes cost under a hysteresis gate. It knows nothing about QSPN
maps, multi-identity addressing, or hooking.

## What it provides

- [`NodeId`] — a positive, nonzero 31-bit **discovery** id chosen at random
  per identity, used only to disambiguate neighbors on a MAC-collision-prone
  medium. It is explicitly *not* a cryptographic identity, and it is not the
  identity `ntk-qspn`/`ntk-identities` reason about — do not read more into
  it than "which of my neighbors is this".
- [`Arc`]/[`ArcState`] — one discovered/negotiated/established link.
- [`cost::ema_step`]/[`cost::exceeds_hysteresis`] — the pure link-cost math:
  an asymmetric EMA (rises slowly, falls quickly) smooths raw RTT samples in
  **microseconds**, and a value is only published — only then does an
  `arc_changed` event fire — once it clears a 2x hysteresis band around the
  last published cost. A link that briefly wobbles never spams downstream
  route recomputation.
- [`RttProbe`] — the injectable RTT-measurement seam; [`IcmpRttProbe`] is the
  real ICMPv4-echo implementation, [`nic::FixedRttProbe`] a test double. A
  probe that fails or times out degrades to a fallback cost rather than
  blocking or tearing down arc formation — a dead-reckoning arc is better
  than none while liveness is re-established.
- [`Manager`]/[`Handle`]/[`Event`] — the actor, its cheap-clone handle, and
  its broadcast event stream; [`NeighborhoodRpcHandler`] — the inbound
  handler for the 5 `MethodCall` arms this module owns.
- [`NeighborhoodStubFactory`], [`FakeIpRouteManager`] — the outbound-call
  seam and its non-privileged fake.

## Usage

Like the other module crates, `Manager` only does something useful wired to
a real kernel/RPC seam, which is `ntkd`'s job. The pure cost math, though, is
directly usable:

```rust
use ntk_neighborhood::cost::{ema_step, exceeds_hysteresis};

let mut smoothed = 1_000u64; // microseconds
let published = 1_000u64;
smoothed = ema_step(smoothed, 2_500); // one worse sample
if exceeds_hysteresis(published, smoothed) {
    // republish `smoothed` as the arc's new cost.
}
```

## License

GPL-3.0-or-later. Part of the [netsukuku-rs](https://github.com/M0Rf30/netsukuku-rs)
workspace.
