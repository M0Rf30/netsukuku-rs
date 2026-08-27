# Vala core routing: qspn, neighborhood, identities, hooking, coordinator

Open this when the question is how the Vala rewrite's routing core actually fits together —
module boundaries, the QSPN map algorithm, arc/identity/hooking lifecycles, Coordinator election —
before drawing the equivalent crate boundary in Rust. Every claim below is checked in at a `path:line`.

Source: lukisi/netsukuku Vala rewrite (2017-2020), vendored at `research/impl/vala/`. All
citations are `path:line` relative to repo root; module version tags are all `0.1`/`0.2`
(`configure.ac` `AC_INIT`), no upstream commit hashes are embedded in the tree (git history
not vendored, only a snapshot).

## 1. Module dependency graph

Each module is an autotools convenience library (`noinst_LTLIBRARIES`) built independently
and exposing a generated `.vapi`. Cross-module coupling is **interface-only**: a module
defines abstract `I*` interfaces in its own `api.vala` for whatever it needs from the
outside world, and the daemon (out of scope here, see `ntkd/`) wires concrete adapters at
composition time. There is no `pkg-config`/`--pkg` dependency from qspn, neighborhood,
identities or hooking onto each other.

Direct library deps (`PKG_CHECK_MODULES` + `--pkg` in `Makefile.am`), all modules:
`gee-0.8`, `json-glib-1.0`, `tasklet-system`, `pth-tasklet` (test only), `ntkd-common`,
`ntkdrpc`.

- `qspn/configure.ac:20-32`, `qspn/Makefile.am:34-47` — no other Netsukuku module.
- `neighborhood/configure.ac:20-32`, `neighborhood/Makefile.am:29-42` — none.
- `identities/configure.ac:20-32`, `identities/Makefile.am:26-39` — none.
- `hooking/configure.ac:20-32`, `hooking/Makefile.am:35-48` — none (talks to qspn/
  neighborhood/identities only via `hooking/api.vala` interfaces `IHookingMapPaths`,
  `ICoordinator`, `IIdentityArc`).
- `coordinator/configure.ac:20-34`, `coordinator/Makefile.am:31-46` — **directly depends
  on `peers` (PeerServices)**, `--pkg peers` (`coordinator/Makefile.am:39,45`). Coordinator
  is implemented as a `PeerService` on top of PeerServices' fixed-keys DHT (§7).

```
ntkd-common ← ntkdrpc ← { qspn, neighborhood, identities, hooking, coordinator }
                                                              coordinator ← peers (PeerServices)
```

`ntkdrpc` itself has no Netsukuku-module deps beyond `zcd` (transport, not in scope) and
`ntkd-common` (`ntkdrpc/ntkdrpc.deps:1`).

## 2. ntkd-common + ntkdrpc: shared types and RPC surface

**ntkd-common** (`ntkd-common/ntkd_common.vala`): two GObjects shared by every module.
- `HCoord{lvl:int, pos:int}` — hierarchical coordinate of a g-node (`:21-35`).
- `NodeID{id:int}` — a random per-identity node id (`:37-49`).

**ntkdrpc**: the wire contract. `ntkdrpc/interfaces.vala:19-79` declares 8 error domains
(each single-variant `GENERIC`): `QspnNotAcceptedError`, `QspnBootstrapInProgressError`,
`PeersUnknownMessageError`, `PeersInvalidRequest`, `HookingNotPrincipalError`,
`NotBootstrappedError`, `NoMigrationPathFoundError`, `MigrationPathExecuteFailureError`.
It also declares ~20 marker interfaces (`IQspnEtpMessage`, `IQspnAddress`,
`IIdentityID`, `ICoordTupleGNode`, `ICoordObject`, `INetworkData`, `IEntryData`, …,
`ntkdrpc/interfaces.vala:117-223`) that each module's concrete serializable classes
implement — this is how one RPC dispatcher (`AddressManager`) can carry heterogeneous
per-module payloads.

The canonical RPC surface is spelled out as an IDL comment reproduced verbatim in both
`ntkdrpc/interfaces.vala:19-79` and `ntkdrpc/build_sources/interfaces.rpcidl:1-56`
(the `.rpcidl` is the actual code-gen input for `addr_skeleton.vala`/`addr_stub.vala`,
130KB/101KB generated dispatch code — not hand-read, structural only). One root object
`AddressManager addr` exposes 5 sub-managers, each a facade onto one module:

| Interface (`addr.X`) | Method | Args → Return | Throws | Implementing module |
|---|---|---|---|---|
| `neighborhood_manager` | `here_i_am` | `(id,mac,nic_addr)` → void | – | Neighborhood |
| | `request_arc` | `(dest_id,dest_mac,dest_nic_addr, my_id,my_mac,my_nic_addr)` → void | – | Neighborhood |
| | `can_you_export` | `(bool i_can_export)` → bool | – | Neighborhood |
| | `remove_arc` | same shape as `request_arc` → void | – | Neighborhood |
| | `nop` | `()` → void | – | Neighborhood |
| `identity_manager` | `match_duplication` | `(migration_id,peer_id,old_id,new_id,old_id_new_mac,old_id_new_linklocal)` → `IDuplicationData?` | – | Identities |
| | `get_peer_main_id` | `()` → `IIdentityID` | – | Identities |
| | `notify_identity_arc_removed` | `(peer_id,my_id)` → void | – | Identities |
| `qspn_manager` | `get_full_etp` | `(requesting_address)` → `IQspnEtpMessage` | `QspnNotAcceptedError, QspnBootstrapInProgressError` | Qspn |
| | `send_etp` | `(etp,is_full)` → void | `QspnNotAcceptedError` | Qspn |
| | `got_prepare_destroy` | `()` → void | – | Qspn |
| | `got_destroy` | `()` → void | – | Qspn |
| `peers_manager` | 11 methods (`forward_peer_message`, `get_request`, `set_response`, `set_refuse_message`, `set_redo_from_start`, `set_next_destination`, `set_failure`, `set_non_participant`, `set_missing_optional_maps`, `set_participant`, `give_participant_maps`, `ask_participant_maps`) | — | `PeersUnknownMessageError, PeersInvalidRequest` (on `get_request`) | PeerServices — **out of scope** |
| `coordinator_manager` | `execute_prepare_migration`, `execute_finish_migration`, `execute_prepare_enter`, `execute_finish_enter`, `execute_we_have_splitted` | `(tuple:ICoordTupleGNode, fp_id:int64, propagation_id:int, lvl:int, data:ICoordObject)` → void | – | Coordinator |
| `hooking_manager` | `retrieve_network_data` | `(ask_coord:bool)` → `INetworkData` | `HookingNotPrincipalError, NotBootstrappedError` | Hooking |
| | `search_migration_path` | `(lvl:int)` → `IEntryData` | `NoMigrationPathFoundError, MigrationPathExecuteFailureError, NotBootstrappedError` | Hooking |
| | `route_search_request/_error/_response`, `route_explore_request/_response`, `route_delete_reserve_request`, `route_mig_request/_response` | routing envelopes, void | – | Hooking |

Every skeleton method takes an implicit trailing `CallerInfo? _rpc_caller=null` (e.g.
`qspn/qspn.vala:2541-2543`, `neighborhood/neighborhood.vala:363`) used to recover which
arc/nic the call physically arrived on. `CallerInfo` is a sealed hierarchy
(`ntkdrpc/caller_info.vala:23-134`): `StreamCallerInfo` (TCP unicast; carries
`source_id`,`src_nic`,`unicast_id`) vs `DatagramCallerInfo` (UDP broadcast; carries
`broadcast_id`, `send_ack`), each with a `Listener` (`StreamNetListener`,
`DatagramNetListener`, …) describing the local endpoint.

## 3. QSPN v2 (`qspn/`)

### Types (`qspn/api.vala:23-162`)
`IQspnNaddr` (levels/gsize/pos getters) ⊂ `IQspnMyNaddr` (+ `i_qspn_get_coord_by_address`).
`IQspnFingerprint` (equals, level, `construct(children_fps, is_null_eldership)`,
`elder_seed` — total order used to pick the "elder" fingerprint on a fork).
`IQspnCost` (compare_to, add_segment, `important_variation` — hysteresis predicate,
`is_dead`, `is_null`); two built-ins `NullCost` (identity, always "not dead") and
`DeadCost` (absorbing, `compare_to`→+1 vs anything but itself) at `:53-112`.
`IQspnArc` (cost, equals, `i_qspn_comes_from(CallerInfo)`). `IQspnHop{arc_id,hcoord}`.
`IQspnNodePath` (arc, hops, cost, nodes_inside). `IQspnStubFactory` gives broadcast/tcp
stubs per arc-set — this is qspn's sole coupling to Neighborhood/transport.

### State (`QspnManager`, `qspn/qspn.vala:66-119`)
Per-identity object: `my_naddr`, `my_arcs`, `arc_to_naddr` (arc→peer address, `null` until
first ETP), `id_arc_map` (random 31-bit `arc_id` ↔ arc, generated in `arc_add`/ctor,
`qspn.vala:290-300,727-731`), `my_fingerprints[0..levels]`, `my_nodes_inside[0..levels]`,
`destinations: ArrayList<HashMap<int,Destination>>` indexed `[level][pos]`. Static
per-process params from `QspnManager.init`: `max_paths`, `max_common_hops_ratio`,
`arc_timeout`, `threshold_calculator` (`:68-96`). `levels`/`gsizes` derived once from
`my_naddr` at first construction (`:179-181`).

Two constructors: `create_net` (root of a new 1-node network, immediately
`bootstrap_complete`, `:161-219`) and `enter_net` (hooking into an existing gnode; carries
over arc IDs from `previous_identity` for internal arcs, `:223-355`). This is how identity
migration (§5) hands off qspn state across `IdentityManager.add_identity`.

### ETP structure (`qspn/serializables.vala:25-217`, `qspn/etp_message.vala`)
`EtpMessage{node_address, fingerprints[], nodes_inside[], hops:List<HCoord>, p_list:List<EtpPath>}`.
`EtpPath{hops, arcs, cost, fingerprint, nodes_inside, ignore_outside:List<bool>}` — one path
to one destination, `ignore_outside[i]=true` means "path fact not valid outside level i"
(pruning rule so an inner path doesn't leak details to outer levels). Polymorphic fields
(`IQspnFingerprint`, `IQspnCost`, `IQspnNaddr`) are (de)serialized with a `{typename,value}`
JSON envelope (`serialize_object`/`deserialize_object`, `qspn/serializables.vala:219-263`)
so third-party implementations of these interfaces round-trip.

### ETP revision rules — `revise_etp` (`qspn/qspn.vala:1074-1232`)
Given an incoming/outgoing `EtpMessage m` from arc with local id `arc_id`, `v = HCoord(sender)`:
1. **Grouping rule**: drop any leading `m.hops`/`p.hops` entries whose level < `v.lvl` (they
   belong to inner topology I don't need), then prepend `v` (`:1090-1094,1121-1130`).
2. **Acyclic rule**: if any hop equals my own position at that level, the message/path has
   looped through my own g-node — drop it (throws `AcyclicError` for the top-level message,
   silently drops individual paths, `:1096-1153`).
3. **Intrinsic path to v**: synthesize a zero-cost `NullCost` path straight to the sender
   using `m.fingerprints[i-1]`/`m.nodes_inside[i-1]` (`:1154-1165`); if the peer's naddr at
   this level changed since last ETP (identity migrated on the other end), also synthesize a
   `DeadCost` path to the *old* position so it gets withdrawn (`:1166-1181`).
4. **Full-ETP implicit withdrawal**: on a *full* ETP, any path I currently hold through this
   arc that is absent from the incoming full list is implicitly dead (`DeadCost`,
   `:1182-1223`) — this is qspn's dead-path/garbage-collection mechanism, no explicit
   "withdraw" message type exists.

### Path admission — `update_map` (`qspn/qspn.vala:1334-1816`)
Per destination `d` (grouped from the revised path set `q_set`, processed in ascending
level then ascending hop-count order, `:1350-1398`):
- Merge existing paths (`md_set`) with new candidates (`qd_set`) into `od_set`, replacing a
  path when fingerprint differs, `cost.i_qspn_important_variation()` fires, or
  `nodes_inside` moved by >10% (`p1.nodes_inside*1.1 < p2.nodes_inside` etc.,
  `:1429-1433` — this ±10% band is the noise filter for `nodes_inside` churn).
- Drop candidates whose intermediate hop g-node is not yet a known destination
  (`:1466-1486`, "wait to learn the hop before trusting a path through it").
- **Disjoint-path selection** (mesh diversity, `:1522-1621`): sort `od_set` by cost; a path
  is *mandatory* if it introduces a new fingerprint not yet in `fd`, or removes the last
  path through some existing gateway g-node (`vnd`), or reaches a new sibling g-node
  (`z1d`); otherwise it's admitted only while `rd.size < max_paths` **and** its estimated
  hop-overlap with every already-admitted path stays ≤ `mch_ratio` (max-common-hops ratio).
  Overlap is computed hop-by-hop weighting each shared g-node by
  `floor(1.5·sqrt(nodes_inside))` for intermediate hops and `floor(0.75·sqrt(nodes_inside))-1`
  for the destination hop (`:1567-1610`) — larger destinations are "cheaper" to share paths
  through. `mch_ratio` itself is size/gateway-adaptive, see `get_mch_ratio` below.
- **Elder fingerprint rule**: at levels >0, of the surviving fingerprints for `d`, only
  paths carrying the fingerprint that `i_qspn_elder_seed`-wins are exposed via
  `path_added`/`path_changed` signals and via `get_paths_to` (`:1622-1717`,`2169-2177`);
  losing-fingerprint paths are tracked internally but not surfaced — this is the client-
  visible half of split handling.
- **Split first-detection**: when a destination now has >1 distinct fingerprint where it
  previously had ≤1 seen-by-me fingerprint, the g-node is queued into `b_set` for an
  immediate re-flood (`spawn_flood_first_detection_split`, `:1763-1774`), and independently
  (regardless of first-detection) each non-eldest fingerprint schedules a `signal_split`
  tasklet after `threshold_calculator.i_qspn_calculate_threshold(eldest_path, other_path)`
  ms — a pluggable debounce so transient forks don't trigger migration (`:1775-1811`).

`get_mch_ratio(size, numgw)` (`qspn/qspn.vala:1888-1909`): `l` by gateway count
`{1:0.45, 2:0.35, 3:0.27, 4:0.20, 5:0.15, 6:0.12, 7:0.10, else:0.08}`; `g` by destination
size `{<10:1.0, <25:0.9, <75:0.8, <250:0.6, <750:0.3, <3000:0.1, else:0.0001}`; returns
`(max_common_hops_ratio - max_common_hops_ratio*l)*g + max_common_hops_ratio*l` — i.e. more
gateways or a bigger destination g-node ⇒ tighter (smaller) overlap tolerance.

### Split / merge of *my own* g-node — `update_clusters` (`qspn/qspn.vala:1954-2074`)
Bottom-up fold, level 1 special-cased: my fingerprint at level `i` =
`my_fingerprints[i-1].i_qspn_construct(best-path-fingerprints of all level-(i-1)
destinations, is_null_eldership)`; `is_null_eldership=true` iff my own pos at level `i-1`
is virtual (`pos>=gsize`, i.e. I'm hooked in via a virtual/reserved slot, not fully
integrated yet, `:1962-1966,2010-2014`). `my_nodes_inside[i]` sums children counts. Changes
fire `changed_fp(i)`/`changed_nodes_inside(i)`.

### Flooding / propagation
- `arc_add`/`arc_is_changed`/`arc_remove` (`:696-1070`) each: fetch full ETP from the
  affected arc(s) (`retrieve_full_etp`/`gather_full_etp_set`, parallel via one tasklet per
  arc + join, `qspn/etp_retrieve.vala:102-130`), `revise_etp`, `update_map`, then
  conditionally forward a partial ETP (`prepare_fwd_etp`/`prepare_new_etp`) to all other
  arcs (`send_etp_multi`) only if `all_paths_set` non-empty or my own gnode fingerprints
  changed — this is qspn's flood-suppression: no change ⇒ no forward.
- `send_etp` skeleton (peer-initiated, `:2608-2751`): validates the caller is a known arc
  (polling `arc_timeout` ms in 10ms steps via `Timer`, `:2550-2565`), validates message
  shape (`check_incoming_message`), during bootstrap only exits bootstrap once an ETP with
  a path into the *host* gnode arrives (`:2671-2706`), then revises+applies+conditionally
  forwards to all arcs but the sender.
- **Reliable broadcast with unicast fallback**: every broadcast send passes an
  `IQspnMissingArcHandler`; if a neighbor's ack is missing the same ETP is resent via TCP
  unicast to just that arc (`MissingArcSendEtp`, `qspn/missing_arcs.vala:24-40`); the same
  pattern retries `got_prepare_destroy`/`got_destroy` (`:42-102`).
- `publish_connectivity` (`qspn/etp_publish.vala:110-144`): on `make_connectivity` (identity
  becomes a *connectivity* identity spanning `[from_level,to_level]`), sends a void ETP
  (empty path list, just `hops=[old position]`) to neighbors outside the old level so they
  withdraw the now-obsolete gateway — the mechanism by which a migrating gnode announces its
  old address is gone without waiting for full-ETP GC.

### Identity/connectivity lifecycle (`qspn/qspn.vala:2228-2505`)
`make_connectivity(from,to,update_naddr)` turns a *main* identity into a *connectivity*
identity spanning levels `[from,to]` (used mid-migration to keep routing alive across the
gnode being migrated) — rewrites `my_naddr` and internal arcs via caller-supplied delegate,
then delayed (`ms_wait(50)`) `publish_connectivity`. `exit_network(lvl)` drops all
destinations/arcs at/above `lvl`. `check_connectivity()` (`:2371-2448`) is a **structural
invariant check**: a connectivity identity may only be torn down if doing so would not
disconnect any g-node it currently bridges (BFS-style neighbor/path coverage check over
`destinations[i..j-1]`). `prepare_destroy`/`destroy` broadcast `got_prepare_destroy`/
`got_destroy` to internal/outer arcs respectively with the same missing-arc-retry pattern.

### Constants
| Value | Meaning | Site |
|---|---|---|
| `max_paths`, `max_common_hops_ratio`, `arc_timeout` | ctor params, per-deployment | `qspn.vala:70-72,93-95` |
| 1 ms | delay before emitting `qspn_bootstrap_complete` (let ctor return first) | `qspn.vala:215` |
| 10000 ms | fallback max bootstrap wait if no ETP settles it | `qspn.vala:558-560` |
| 1000 ms | pause after `exit_bootstrap_phase` before `presence_notified` | `qspn.vala:625` |
| 600000 ms (10 min) | periodic full-ETP re-publish while ≥1 arc | `qspn.vala:680` |
| 500 ms | delay before first-detection-of-split re-flood | `qspn.vala:1932` |
| 50 ms | delay before `publish_connectivity` after `make_connectivity` | `qspn.vala:2259` |
| 10000 ms | wait after `got_prepare_destroy` before self-removal | `qspn.vala:2761` |
| 10 ms | poll interval while resolving `CallerInfo`→arc | `qspn.vala:2564,2627,2785` |
| ±10% | `nodes_inside` change-noise tolerance | `qspn.vala:1433` |
| 1.5·√n / 0.75·√n−1 | intermediate/destination hop weight in overlap calc | `qspn.vala:1575,1594-1595` |
| mch_ratio table | see `get_mch_ratio` above | `qspn.vala:1888-1909` |

## 4. `neighborhood/`: link discovery

### Types (`neighborhood/api.vala:29-102`, `structs.vala:23-87`)
`INeighborhoodNetworkInterface{dev,mac,measure_rtt()}` — one real NIC. `INeighborhoodArc`
(peer mac/nic_addr/id, cost, owning nic). Concrete `NeighborhoodRealArc` adds `available`
(cost assigned) and `exported` (mutually accepted, visible to qspn) flags. Caller supplies
`INeighborhoodStubFactory` (broadcast-for-radar / unicast stubs) and
`INeighborhoodIPRouteManager` (linklocal add/remove address, add/remove neighbor route) —
neighborhood never touches the OS network stack directly, only through this interface.

### Arc lifecycle — 3-way handshake over UDP broadcast, confirm over TCP unicast
1. **Radar**: `MonitorRunTasklet` (`neighborhood.vala:166-190`) broadcasts `here_i_am(my_id,
   my_mac, my_addr)` on each monitored NIC every **60000 ms**.
2. **`here_i_am`** skeleton (`:363-433`): receiver dedups by 4 collision rules (no two
   neighbors share a MAC; a MAC's linklocal is fixed; one NIC pairing per remote MAC; one
   dev per remote node-id, `:397-410`), creates a `NeighborhoodRealArc`, registers it in 6
   parallel indices (`arcs_by_itsmac/itsll/itsnodeid`, and per-`my_dev` variants,
   `:381-419`), calls `ip_mgr.add_neighbor`, then broadcasts back `request_arc`.
3. **`request_arc`** skeleton (`:435-536`): same dedup, creates/reuses the arc, then does a
   **TCP** `can_you_export(my_capacity)` round-trip to the peer; if both sides have export
   capacity (`exported_arcs.size < max_arcs`) both flag the arc `exported=true` and start
   `start_arc_monitor`.
4. **`can_you_export`** skeleton (`:538-558`) mirrors the capacity check and export flag
   locally for the reverse direction.
5. **Cost/keepalive** — `ArcMonitorRunTasklet` (`:210-293`), once per exported arc: calls
   `nic.measure_rtt` (best-effort — a failed rtt is only a warning) then a **TCP `nop()`**
   liveness check; **a failed `nop` immediately removes the arc** (`remove_my_arc(arc,
   false)`, `:246-251`) — `nop` failure, not RTT failure, is the actual dead-arc detector.
   Cost is smoothed, not raw RTT: `delta = rtt-last; delta/=10 if >0 else delta/=3` then
   only signals `arc_changed` if the smoothed cost moves outside `[0.5x, 2x]` of the last
   signaled value (`:274-286`) — asymmetric smoothing (converge up faster than down) with a
   2× hysteresis band before qspn is told. Interval: random **28000–30000 ms**.
6. **Teardown**: `remove_my_arc` (`:306-350`) unregisters from all 6 indices, calls
   `ip_mgr.remove_neighbor`, and if the arc is still reachable, broadcasts `remove_arc` so
   the peer tears its mirror down too (`is_still_usable` flag suppresses this when we're
   the one racing a dead link). `stop_monitor(dev)` tears down every arc on that dev plus
   the dev's own linklocal address.

`max_arcs` is a per-node hard cap on **exported** (qspn-visible) arcs, enforced
independently by each side (`:514,549`); non-exported arcs still occupy the discovery
indices but are invisible above neighborhood.

## 5. `identities/`: multi-identity per node

### Why identities exist
A physical node hosts ≥1 `Identity` (`NodeID` + private `Linux` network namespace +
per-real-`dev` pseudo-device). The **main identity** lives in the default namespace on real
NICs. Extra identities exist purely to support **live gnode migration during hooking**: to
move gnode `G` to a new position without breaking `G`'s internal connectivity, the node
that was `G`'s single point of contact with the outside forks into two identities — the
*old* identity keeps `G`'s external arcs alive as a **connectivity identity** (bridges
`[from_level,to_level]`, no full network position) while the *new* identity takes over
`G`'s internal presence and proceeds through hooking at the new external position
(`identities.vala:441-577`, cross-referenced from `hooking/arc_handler.vala:337-357` which
calls `HookingManager.finish_enter`/qspn's connectivity ctor). Each `IdmgmtArc → IdentityArc`
association is **per (my identity, physical arc)**, so a single real NIC/arc pair can carry
several logical qspn arcs, one per local identity sharing that link (`identity_arcs:
HashMap<"nodeid-arcid", ArrayList<IdentityArc>>`, `:129,182-215`).

### Pseudo-address / pseudo-device
`IIdmgmtNetnsManager` (`:26-36`) is the OS adapter: `create_namespace`,
`create_pseudodev(real_dev,ns,pseudo_dev,out pseudo_mac)`, `add_address`,
`add_gateway`/`remove_gateway` (route from an identity's pseudodev linklocal to a specific
peer linklocal — this is how one physical link serves N identities: N pseudo-devices, N
linklocals, N host routes), `flush_table`, `delete_pseudodev`, `delete_namespace`.
`get_pseudodev(id,dev)` (`:362-367`) is how upper layers (qspn arcs) find the
netns-local device name to bind sockets to.

### Migration state machine (`add_identity`, `identities.vala:441-577`)
1. `prepare_add_identity(migration_id, old_id)` (caller: hooking) stashes a pending
   `MigrationData{migration_id, old_id, ready=false}` and schedules a **600000 ms** cleanup
   in case the caller never follows through (`:399-438`).
2. `add_identity(migration_id, old_id)`: allocates a new namespace `ntkv<seq>`, **swaps**
   the old identity into the new temp namespace while the *new* `Identity` inherits the old
   identity's original namespace/nodeid slot in `namespaces` (`:462-464`) — the new identity
   is the one that "keeps the name", old one is renamed away. For every handled real `dev`,
   creates a pseudodev+new linklocal for the *old* (now-renamed) identity
   (`MigrationDeviceData`, `:466-489`), marks `migration_data.ready=true`.
3. For every existing arc, duplicates each `IdentityArc` (old keeps a copy `w0`, new gets
   `w1`) and calls **`match_duplication`** on the peer (min **10000 ms** total, min **3000
   ms** per call, `Timer`, `:493-548`) so the peer learns both the new peer-id and the new
   MAC/linklocal to reach the *old* identity now that it moved to a pseudodev. Route added:
   `add_gateway(old identity's new linklocal → peer's old linklocal)` (`:560`). Arc-level
   failure (timeout/error) marks it broken and schedules async removal, doesn't abort the
   whole migration.
4. Peer side, `match_duplication` skeleton (`:827-876`): if it already has a pending
   migration matching `(migration_id, old_id==caller's old id)` it **busy-waits**
   (`while(!ready) ms_wait(50)`) then answers with its own duplication data (**symmetric**
   migration, both sides mid-flight) — otherwise (peer not migrating) it answers `null` and
   asynchronously runs `neighbour_migrated` (`:877-907`) to add the new identity-arc + patch
   the old one's peer linklocal, purely reactive to the initiator.

### Removal (`remove_identity`, `:685-730`)
For a non-main identity: notify every identity-arc peer (`notify_identity_arc_removed`,
500 ms timeout each, best-effort — errors just `break` out of that arc's peer loop,
`:697-716`), `netns_manager.flush_table` + delete every pseudodev + delete the namespace.
`remove_arc` on a physical arc likewise tears down every identity's `IdentityArc` over it
first (`:318-339`).

## 6. `hooking/`: bootstrap / hook state machine

### Roles
`IHookingMapPaths` (`hooking/api.vala:23-50`) is the read-only view onto my current
position/topology/map, supplied by the daemon (backed by qspn's destinations + naddr).
`ICoordinator` (`:59-81`) is the proxy to the (per-level) Coordinator: `evaluate_enter`,
`begin_enter`/`completed_enter`/`abort_enter`, `reserve`/`delete_reserve`, hooking-memory
get/set, propagation triggers.

### Per-arc state machine — `ArcHandler.add_arc_tasklet` (`hooking/arc_handler.vala:62-359`)
One tasklet per identity-arc, driven purely by that arc's peer:
1. If I am not "real" (all positions non-virtual) — i.e. I am a connectivity identity —
   terminate immediately (`:95-99`); connectivity identities never hook.
2. `retrieve_network_data(false)` on the peer; `NotBootstrappedError` ⇒ wait 1000ms and
   retry (peer still hooking itself); `HookingNotPrincipalError` ⇒ peer is connectivity,
   terminate (`:100-117`).
3. Same `network_id` ⇒ `same_network` signal, done (`:124-129`); different but
   **incompatible topology** (`gsizes` mismatch) ⇒ silently terminate (`:130-149`) — two
   Netsukuku networks with different level/gsize parameters can never merge.
4. **Merge-direction heuristic** (`:150-178`): fires `another_network`; compares
   `neighbor_n_nodes` vs my `n_nodes`. Proceed unconditionally only if the peer's network is
   **>10×** mine; if roughly equal (or the ratio is in between) ask the Coordinator for an
   authoritative `n_nodes` and re-decide, tie-broken on `network_id > my network_id`
   (`:206-207`) — larger network absorbs smaller; equal size ⇒ arbitrary but deterministic
   tiebreak so both sides agree. If not proceeding: wait **600000 ms**, retry (`:209-214`).
5. **Network-wide evaluation**: `proxy_coord.evaluate_enter(EvaluateEnterData)` — may need
   retry on `AskAgainError` (wait `get_global_timeout(n)/4`) or abandon-and-restart on
   `IgnoreNetworkError` (wait `get_global_timeout(n)*20`) (`:224-247`).
6. **Begin/search loop** (`:250-334`): `proxy_coord.begin_enter(ask_lvl)` (may retry the
   whole outer loop after `get_global_timeout(n)*20` on `AlreadyEnteringError`); then
   `st.search_migration_path(ask_lvl)` on the peer (peer runs the BFS, §below);
   `NoMigrationPathFoundError` ⇒ `abort_enter` then **retry at `ask_lvl-1`**, or give up
   (wait `*20`, restart) if `ask_lvl` was already 0; `MigrationPathExecuteFailureError` ⇒
   immediate retry same level.
7. On success: `proxy_coord.completed_enter(ask_lvl)`, then **propagate** (not proxy —
   local flood inside my own resulting gnode) `prepare_enter(enter_id)` then
   `finish_enter(enter_id, entry_data, go_connectivity_position)` where
   `go_connectivity_position` is a random virtual pos `≥ gsizes[ask_lvl]` reserved for the
   connectivity identity that will keep the *old* subtree alive (`:349-357`).

`get_global_timeout(size)` (`hooking.vala:46-57`): `{<5:1000, <15:2000, <25:3000, <100:5000,
else:10000}` ms — explicitly flagged in-source as placeholder/debug-only tuning
(`// TODO … For real cases I don't know`).

### Finding a migration path — `find_shortest_mig` BFS (`hooking.vala:326-464`)
Run by the *host* network's principal peer that answered `search_migration_path`. BFS over
`TupleGNode` (relative position tuple from some level up to root) starting at my own gnode
at `first_host_lvl = lvl+1`. Each visited node issues `message_routing.send_search_request`
(routed RPC into the target gnode) which calls back into **that node's** `execute_search`
(`hooking.vala:156-228`): tries `coord.reserve(min_host_lvl,...)` climbing levels until a
non-virtual (`pos < gsize`) reservation succeeds or `max_host_lvl` is exhausted, returning
adjacent gnodes to explore further. BFS keeps the best (shortest) solution per level once
found (`ok_host_lvl = lvl+epsilon` short-circuits early, `hooking.vala:522-524`) and prunes
already-visited/ancestor gnodes (`S`, `:426-460`) plus deeper solutions once a shallower one
is 30%/5-hops worse (`prev_sol_distance` bound, `:347-350`). Every rejected `Solution`
along the way schedules a `delete_reserve` cleanup on its dest gnode (`:531-541`).
`execute_shortest_mig` (`:466-490`) then walks the resulting `Solution` chain **farthest
hop first**: sends `PREPARE_MIGRATION` to every hop, then `FINISH_MIGRATION` starting from
the farthest, propagating each hop's real new pos/eldership inward.

## 7. `coordinator/`: reserved-position allocator + per-level election

### What it coordinates
For every level `l` from 1..levels, "the Coordinator of gnode at level l" is not a fixed
node — it is whichever node the PeerServices fixed-keys DHT elects as **servant** for key
`CoordinatorKey(l)`, via `perfect_tuple(k) = [0,0,...,0]` (`l` zeros)
(`coordinator/peer_service.vala:158-166`) — i.e. the DHT hashes to the position-0 (eldest)
node inside that gnode; `CoordService` runs as a `PeerService` (`p_id=1`,
`coordinator/peer_service.vala:25-55`) registered only on the **main identity**
(`bootstrap_completed(..., is_main_id)`, `coord.vala:136-146`) so connectivity identities
never serve as coordinator. On identity migration, `prev_service`'s fixed-keys DB state is
handed to the new `CoordService` at construction (`coord.vala:142-146`,
`peer_service.vala:39-72`, via `peers_manager.fixed_keys_db_on_startup(fkdd, p_id,
prev_fkdd)`) — this is the coordinator hand-off protocol.

### State — `CoordGnodeMemory` per level (`coordinator/serializables.vala:183-190`)
`reserve_list: List<Booking{reserve_request_id,new_pos,new_eldership,timeout}>`,
`max_virtual_pos`, `max_eldership` (both monotonically increasing counters, seeded from
`gsizes[lvl-1]`/`0`, `coordinator/peer_service.vala:79-88`), `n_nodes` (nullable,
cached network size + `n_nodes_timeout`), `hooking_memory` (opaque per-level scratch object
for Hooking, via `get/set_hooking_memory`).

### Reserve protocol (`coordinator/fk_database.vala:502-573`)
Idempotent by `reserve_request_id`: expired bookings purged first
(`timeout.is_expired()`); if this request_id was already served, refresh its timeout and
return the same `(pos,eldership)` (replay-safe against retries). Otherwise: try
`mgr.map.get_free_pos(lvl-1)` for a real slot not already booked; if none free, allocate a
**virtual** position `++max_virtual_pos` (beyond `gsize`, i.e. reserved-but-not-yet-real —
this is what `execute_search` in Hooking detects via `pos>=gsizes[lvl-1]` to keep climbing
levels, `hooking.vala:191-199`). Eldership is always `++max_eldership` (globally
increasing per gnode, never reused). Booking TTL: **60000 ms**
(`CoordService.msec_new_reservation`, `peer_service.vala:28`); `n_nodes` cache TTL:
**20000 ms** (`msec_n_nodes`, `:29`); replica fanout `q_replica_new_reservation=15`
(`:30`). `delete_reserve` just removes the booking by `reserve_request_id`
(`fk_database.vala:574-588`).

### Proxy + propagation to Hooking (`coord.vala:153-420`)
`evaluate_enter`/`begin_enter`/`completed_enter`/`abort_enter` are pure DHT round-trips
(`CoordClient.call(k, Request, timeout)`, `peer_service.vala:216-314`) to whichever node
is elected servant for that level — this is the **per-level election** in action, Hooking
never talks to a fixed node. `prepare_migration`/`finish_migration`/`prepare_enter`/
`finish_enter`/`we_have_splitted` are **not** DHT calls: they're local propagation events
broadcast to `stub_factory.get_stub_for_each_neighbor()` (prepare*) or
`get_stub_for_all_neighbors()` (finish*/splitted, presumably a flood/reliable-broadcast
group) and deduped by `(fp_id, propagation_id)` — `check_propagation` (`:424-440`) rejects
replays (`propagation_id` seen before) and calls from a node no longer matching my current
`(tuple,fp_id)` for that level (stale propagation after I've already moved on), with a
**200000 ms** propagation-id retention window (`propagation_cleanup`, `:238-259`).

## Open questions / risks for the Rust port

- **Interface-only decoupling is the whole architecture.** A Rust port should mirror this
  with traits (`IQspnArc`, `IHookingMapPaths`, `ICoordinator`, …) rather than concrete
  cross-crate deps; conflating modules would violate the tested composition boundary
  (system_peer test suites in each module construct fakes for every dependency interface).
- **Tasklet-per-call concurrency model** (`pth-tasklet`/green threads) pervades qspn,
  hooking and identities (e.g. `gather_full_etp_set` spawns one tasklet per arc and joins,
  `qspn/etp_retrieve.vala:102-129`; `ArcHandler` runs one tasklet per arc indefinitely).
  Porting to async Rust needs an explicit decision: `tokio` tasks + `JoinSet`, or an actor
  per arc — the join/cancel semantics (`ITaskletHandle.kill()`,
  `neighborhood.vala:152,204`) must be preserved for correctness (killing a monitor tasklet
  on `stop_monitor`/`remove_arc`).
- **Threshold/backoff constants are explicitly flagged provisional** in the source itself
  (`get_global_timeout`, `hooking.vala:46-57`: "*these are really just for scripted
  debugging*"; `bootstrap_phase`'s `max_wait=10000` has a `// TODO` for a size-adaptive
  formula, `qspn.vala:558-559`). Do not treat these numeric constants as normative; they're
  a reasonable de-risking starting point, not a protocol invariant.
- **`get_mch_ratio`/overlap-weighting formula** (`qspn.vala:1567-1610,1888-1909`) is dense,
  empirically-tuned, and load-bearing for path diversity vs. state size trade-off — this is
  the single highest-risk piece of qspn to get subtly wrong in a reimplementation; needs a
  dedicated unit-test transcription from the Vala `testsuites/` (see note 02) before trusting
  a Rust rewrite.
- **No git history vendored** for `research/impl/vala/*` — commit-hash citations were not
  obtainable from these working trees; only the snapshot's own line numbers are citable.
  Note 04 (bibliography) should record whatever upstream commit metadata is recoverable
  from GitHub directly if precise provenance is needed later.
- **PeerServices (DHT) is a hard dependency of Coordinator**, not optional plumbing:
  Coordinator's election, replication, and reserve persistence are *entirely* implemented
  as one `PeerService` over PeerServices' fixed-keys database
  (`coordinator/fk_database.vala`, `peer_service.vala`). Since PeerServices/ANDNA are an
  explicit non-goal of this note, a Rust Coordinator cannot be implemented or even
  correctly scoped without first specifying PeerServices' fixed-keys-DB contract
  (`IFixedKeysDatabaseDescriptor`, replica quorum `q_replica_new_reservation=15`) — flag
  this as a cross-note dependency for whichever note covers PeerServices.
- **Identity migration correctness hinges on `match_duplication`'s busy-wait** (`identities.vala:854`,
  `while(!ready) ms_wait(50)`) racing `prepare_add_identity`'s 600000 ms cleanup — a
  Rust port should replace this poll with a condvar/oneshot and re-derive the timeout
  relationship explicitly rather than copying the polling constant.
