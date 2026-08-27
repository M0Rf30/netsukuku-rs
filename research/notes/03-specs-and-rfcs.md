# 03 — Specs and RFCs: documentation inventory

Open this before citing any spec: it says which of the three documentation strata — 2005-09 prose,
the 2007-09 split PDFs, the 2017-2020 Vala design docs — is authoritative for a given claim, and
where the C and Vala doc trees disagree.

Scope: `research/impl/vala/documentation` (HEAD `9466354`, 2020-07-13) and
`research/impl/c/netsukuku` (HEAD `886a24a`, 2025-06-12, but `doc/` untouched
since the single import commit `f1761ca`, 2013-09-06). All artifacts judged
worth preserving are copied verbatim into `research/specs/` as
`vala-doc--<name>` / `c-doc--<name>` (106 files total: 85 from the vala tree,
21 from the c tree — only the c-side files that are byte-identical to their
vala-side counterpart were skipped, noted below).

Three documentation strata exist, oldest to newest:

1. **Alpt-era prose spec** (2005-2009, AlpT + contributors) — the original
   monolithic doc, later split into standalone PDFs, plus the `NTK_RFC` wiki
   series. Present in both trees, with the c-repo's frozen 2013 snapshot vs.
   the vala-repo's more complete/later 2020 pull of the same historical wiki.
2. **Split PDF spec** (2007-2009, AlpT) — `topology.pdf`, `qspn.pdf` (QSPN
   v2), `andna.pdf`, `inetntk.pdf`. Only present in the vala tree
   (`old_doc/main_doc/`); the c tree's `doc/main_doc/` never received them.
3. **Vala-era modular design docs** (`ita/`, 2017-2020, Luca Dionisi/lukisi)
   — per-module functional analyses (Neighborhood, Identities, Qspn,
   PeerServices, Coordinator, Hooking) that specify the reimplementation
   found in `research/impl/vala/{neighborhood,identities,qspn,peerservices,
   coordinator,hooking}`. This is the newest and most authoritative spec.

## Document table

### General / top-level docs

| doc (in `research/specs/`) | origin path | date | status | summary |
|---|---|---|---|---|
| `vala-doc--olddoc-main_doc-netsukuku-index` | `old_doc/main_doc/netsukuku` | undated index | HISTORICAL | 152-line plain-text table of contents pointing at the 4 split PDFs + RFCs/FAQ/HOWTOs; not itself a spec. |
| `vala-doc--olddoc-main_doc-netsukuku-overview.pdf` | `old_doc/main_doc/netsukuku.pdf` | 2008-12-05 | HISTORICAL (general-audience) | Polished English essay "Close the world, Open the next"; plain-English overview referencing the 3 technical PDFs below. |
| `vala-doc--olddoc-main_doc-topology.pdf` | `old_doc/main_doc/topology.pdf` | 2009-04-18 | CURRENT (legacy addressing baseline) | Hierarchical gnode/bnode topology, fixed 256-fanout, membership IDs, communicating-vessels hooking, coordinator node concept, compact-gnode heuristics. |
| `vala-doc--olddoc-main_doc-qspn.pdf` | `old_doc/main_doc/qspn.pdf` | 2009-09-08 | CURRENT (routing baseline) | QSPN v1→v2: Tracer Packet flood, Interesting-Information rule, Extended Tracer Packet (ETP) for dynamic updates, disjoint-route metric, cryptographic QSPN. |
| `vala-doc--olddoc-main_doc-andna.pdf` | `old_doc/main_doc/andna.pdf` | 2008-07-03 | CURRENT (naming baseline) | ANDNA hash-gnode design, counter-gnode anti-spam, hostname hibernation, SNSD (§4). |
| `vala-doc--olddoc-main_doc-inetntk.pdf` | `old_doc/main_doc/inetntk.pdf` | 2007-03-19 | SUPERSEDED (IP-restriction part) / CURRENT (IGS, Viphilama) | Internet compatibility: restricted-mode IP class, Net Split, IGS, Viphilama summary. |
| `c-doc--main_doc-netsukuku-Npv7_HT-draft` | `c/doc/main_doc/netsukuku` | ~2005-2007 (frozen 2013 import) | HISTORICAL | 1616-line original monolithic draft ("Npv7_HT: the seventh son of Ipv7"); fixed `MAXGROUPNODE=256`; describes the pre-split "Radar" link-measurement + "Hook & Unhook" mechanism later replaced by Neighborhood/Hooking modules. Fully superseded by items above. |
| `c-doc--main_doc-netsukuku.ita` | `c/doc/main_doc/netsukuku.ita` | same era | HISTORICAL | Italian translation of the same monolithic draft. |
| `vala-doc--andrea_master_thesis.pdf` | `old_doc/andrea_master_thesis.pdf` | pre-2020 | HISTORICAL | Alpt's master's thesis; academic writeup of the original design. |
| `vala-doc--README.md` | `documentation/README.md` | 2020 | CURRENT (pointer) | One-line pointer: "documentation for developers, available only in Italian (`ita/Home.md`)". |
| `c-doc--README.md`, `c-doc--doc-README`, `c-doc--ChangeLog` | `c/README.md`, `c/doc/README`, `c/ChangeLog` | 2013-2025 | HISTORICAL/META | Revived-repo project readme and 8.7 KB ChangeLog (build-system history, not protocol history — see "State of the C revived project" below). |

### Manuals, misc, howto, articles, FAQ (man-page-style operational docs, Alpt-era)

All under `old_doc/{manuals,misc,howto,articles,faq}` (vala) and `doc/{manuals,misc,howto,articles,faq}` (c). Where content is byte-identical across trees, only the vala copy was kept; where it differs, both are kept.

| topic | vala copy | c copy | identical? | note |
|---|---|---|---|---|
| `netsukuku.conf`(5) | `vala-doc--manuals-netsukuku.conf` | `c-doc--manuals-netsukuku.conf` | no (13-line diff) | vala's has an extra `load_module`/`-m` module-loading option not in c's — later revision. |
| `ntkd`(8) | `vala-doc--manuals-ntkd` | `c-doc--manuals-ntkd` | no (8-line diff) | same `-m/--module` addition. |
| `andna`(8) | `vala-doc--manuals-andna` | `c-doc--manuals-andna` | no (title line only) | c: "ANDNA - Abnormal Netsukuku Domain Name Anarchy" (joke backronym); vala: "A Netsukuku Domain Name Architecture" (serious name) — rebranding evidence. |
| `ntk-resolv`(8), `ntk-wifi`(8) | `vala-doc--manuals-*` | — | yes | c copy skipped. |
| `Ntk_features_list` | `vala-doc--misc-Ntk_features_list` | `c-doc--misc-Ntk_features_list` | no (51-line diff) | vala's is the more complete feature list. |
| `Ntk_scalability` | `vala-doc--misc-Ntk_scalability` | `c-doc--misc-Ntk_scalability` | no (8-line diff) | QSPN v1-only worst-case packet/flood counts: `MAXGROUPNODE=256`, ipv4 `levels=4`; explicitly superseded by `qspn.pdf` (QSPN v2). |
| `Ntk_Internet_tunnels`, `mailinglist`, `Ntk_Grow_Netsukuku` | `vala-doc--misc-*` | — | yes | c copies skipped. |
| `rfc1035.txt` | `vala-doc--misc-rfc1035.txt` | — (not in c tree) | n/a | verbatim copy of IETF RFC 1035 (DNS), kept only as a reference the ANDNS protocol doc cites. |
| `Ntk_New_Global_Net`, `Ntk_Developing_World` (articles) | `vala-doc--articles-*` | — | yes | c copies skipped; both trees also carry fr/it translations under `lang/`, not copied (non-English duplicates). |
| `Ntk_civic_net` (article) | `vala-doc--articles-Ntk_civic_net` | `c-doc--articles-Ntk_civic_net` | no (125-line diff) | outreach essays, not technical specs; low priority for the port. |
| `igs_howto` | `vala-doc--howto-igs_howto` | `c-doc--howto-igs_howto` | no (3-line diff) | vala's copy carries an explicit `:WARNING: this howto refers to the old C implementation:WARNING:` banner absent from c's copy — the vala docs themselves flag this HOWTO stale. |
| `kernel_modules_howto`, `pyntk_howto` | `vala-doc--howto-*` | not present in c tree | n/a | `pyntk_howto` documents a defunct Python/Stackless rewrite attempt (SVN trunk, dead link) — HISTORICAL dead branch, useful only as "prior art that didn't survive". |
| `FAQ` | `vala-doc--faq-FAQ` | `c-doc--faq-FAQ` | no (562-line diff — near-total rewrite) | c's FAQ uses the older "radar sends echo packets every 10s" / "Abnormal…Anarchy" phrasing; vala's is a later, edited wiki snapshot. Keep vala's as normative FAQ text. |
| `FAQ.fr`, `FAQ.ru` (c only) | — | not copied | n/a | non-English translations, existence noted only. |

### NTK_RFC series (see dedicated section below for per-RFC detail)

15 numbered/companion files under `old_doc/main_doc/ntk_rfc/` (vala) — all copied as `vala-doc--rfc-<name>`. The c tree's `doc/main_doc/ntk_rfc/` has only 10 of these (no `Ntk_carciofo`, `Ntk_caustic_routing`, `Ntk_local_ANDNA`, `Ntk_net_split`, `Ntk_viphilama_static`); of the 10 shared names, 2 are byte-identical (`Ntk_MX_request`, `Ntk_andna_counter_pubk`, skipped) and 8 differ — from a 4-line typo fix up to a 669-line near-total rewrite (`Ntk_viphilama`) — because the vala repo's 2020 snapshot captured continued wiki edits after the c repo's 2013 import froze. All 8 differing c copies are kept as `c-doc--rfc-<name>`.

### Vala-era modular design spec (`ita/`, CURRENT — normative for the port)

Written in Italian; one row per module document set (all files individually copied to `research/specs/vala-doc--ita-<Module>-<File>.md`).

| module dir | files | status | summary |
|---|---|---|---|
| `ita/Home.md` | 1 | CURRENT (index) | Table of contents for the module docs; explicitly marks `Proof/` as *"Vecchi documenti (deprecati) del proof-of-concept"* — old, deprecated. |
| `ita/note-ntkd.md` | 1 | CURRENT (integration notes) | 1165-line running log of `system-ntkd` integration decisions (module wiring, `IdentityData`/`IdentityArc`/`NodeArc` glue classes); implementation notes more than a spec — cross-reference for notes 01/02. |
| `ita/Sistema/Requisiti.md` | 1 | CURRENT | OS-level requirements a host must satisfy to run the ntkd suite (per-OS config, e.g. network namespaces). |
| `ita/DemoneNTKD/` | 8 (`AnalisiFunzionale`, `DettagliTecnici`, `EsplorazioneRete`, `IndirizziIP`, `RPC`, `RPCLatoServer`, `RisoluzioneNomi`, `RotteKernel`) | CURRENT | Top-level daemon spec: roles of Neighborhood/Identities/Qspn/PeerServices/Coordinator/Hooking, kernel route management, name resolution (ANDNA) integration. |
| `ita/ModuloNeighborhood/` | 3 (`AnalisiFunzionale`, `DettagliTecnici`, `POC`) | CURRENT | Direct-neighbor discovery/link-cost module (fills the role of the legacy "Radar"). |
| `ita/ModuloIdentities/` | 2 | CURRENT | Multiple "identities" (network stacks) per system node, one principal + N connectivity identities; underlies g-node migration and the newer address model. |
| `ita/ModuloQspn/` | 5 (`AnalisiFunzionale`, `DettagliTecnici`, `EsplorazioneRete`, `PercorsiDisgiunti`, `RoutingIndirizziInterni`) | CURRENT | QSPN v2 reimplementation: generalized `gsize(i)` per level (not fixed 256), `Naddr`/`IQspnNaddr` address model, virtual positions for in-migration g-nodes, disjoint-path selection. |
| `ita/ModuloPeers/` | 8 (`AnalisiFunzionale`, `DettagliTecnici`, `AlgoritmiInstradamento`, `AlgoritmiComplementari`, `DatabaseFixedKeys`, `DatabaseTTL`, `MetodiHelper`, `Strutture`) | CURRENT | PeerServices: generic (H)DHT-over-hierarchy — replaces ANDNA's ad-hoc hash-gnode placement with a reusable peer-to-peer service framework; ANDNA becomes one client of it (see `research/impl/vala/andna`). |
| `ita/ModuloCoordinator/` | 2 | CURRENT | Per-g-node "shared memory" peer service (`CoordGnodeMemory`) used by Hooking for serialized enter/reserve/migrate decisions — replaces the old informal "coordinator node" sketch in `topology.pdf` §7.1.2. |
| `ita/ModuloHooking/` | 2 | CURRENT | Full replacement for the legacy monolithic "Hook & Unhook": network-merge negotiation, migration-path search/execution, g-node split handling. |
| `ita/Librerie/` | 5 (`TaskletSystem`, `ZCD`, `ZcdDettagliTecnici`, `ZcdRpcidl`, `Common`) | CURRENT | Cross-module infra: cooperative-tasklet abstraction, ZCD RPC message framework. |
| `ita/Assemblaggio/` | 5 (`CicloDiVita`, `Interdipendenze`, `README`, `Scenario01`, `CasoBanale`) | CURRENT | Integration/assembly notes wiring all modules into `system-ntkd`, plus 2 testsuite scenarios (not copied — subdirectories `Scenario01ImplementazioneTestsuite/`, `CasoBanaleImplementazioneTestsuite/`). |
| `ita/Proof/` (58 files, incl. `img/`) | not copied | SUPERSEDED | Explicitly marked deprecated proof-of-concept design docs by `Home.md` itself; superseded by the per-module docs above. Not copied to `research/specs/` — cite `research/impl/vala/documentation/ita/Proof/` directly if ever needed. |

## NTK_RFC series — per-RFC detail

Numbering gap: **RFC 0006 was never published** (no reference to it anywhere in either corpus). `Ntk_andna_and_dns` (ANDNS wire protocol) and `Ntk_viphilama_static` (ASCII-art companion draft to RFC 0010) are unnumbered companion docs.

| RFC | title | changes vs. base spec | adopted by vala impl? |
|---|---|---|---|
| 0001 | Gnode contiguity | Fixes IP-collision-on-merge: smaller/younger gnode re-hooks; adds computability-power challenge to stop a gnode faking its node count to force a rehook. | Superseded — vala's Hooking module (`evaluate_enter`/network-size comparison via Coordinator) is a full redesign, not this challenge-response scheme (`vala-doc--ita-ModuloHooking-AnalisiFunzionale.md`). |
| 0002 | Bandwidth measurement | Adds bandwidth (not just rtt) to Tracer Packets; libpcap-based link monitoring; 3 routing tables (bw/latency/avg). | [INFERENCE] Not found as a distinct concept in `ModuloQspn` docs — REM/cost is opaque to QSPN v2 (delegated to the arc's `IQspnCost`), so bandwidth-awareness is a possible cost-function plugin, not a QSPN mechanism. |
| 0003 | Internet Gateway Search (IGS) | Multi-gateway sharing, anti-loop shield, load sharing, traffic shaping for restricted-mode nodes. | Not covered by `ita/` (no ModuloIGS); legacy `igs_howto` itself flags itself as referring to "the old C implementation". Open item for the port. |
| 0004 | Mail Exchange (MX) request | Redirect-based MX resolution for ANDNA. | **Deprecated by RFC 0009** (SNSD) per the RFC text itself. |
| 0005 | Life probability | Prefer older (higher-uptime) links/routes. | **Deprecated** (marked in the doc itself, no successor RFC named). [INFERENCE] conceptually related to QSPN v2's REM/eldership-based tie-breaking in `ModuloQspn`/`ModuloHooking` fingerprints, but not the same mechanism. |
| 0007 | ANDNA counter system based on public key | Fix: derive `counter_gnode` IP from hash of register-node's IP, not its pubkey, to stop multi-keypair hostname-limit bypass. | [INFERENCE] Not addressed in `ModuloPeers`/DemoneNTKD docs read; PeerServices' generic HDHT does not special-case a "counter node" — ANDNA-specific anti-abuse logic would live in `research/impl/vala/andna`, outside this note's scope. |
| 0008 | Private IP classes in restricted mode | Allow `172.16.0.0/12` as alternate restricted-mode class besides `10.0.0.0/8`. | **Deprecated by RFC 0012** (Net Split) per the RFC text itself. |
| 0009 | Scattered Name Service Disgregation (SNSD) | ANDNA's SRV-record equivalent: service/priority/weight records, up to 16 per service / 256 total. | Referenced throughout `ModuloPeers`/ANDNA design; SNSD is the current naming extension mechanism. [INFERENCE — not verified against `research/impl/vala/andna` source, out of scope here]. |
| 0010 | Viphilama (Virtual-to-Physical-Layer Mapper) | Overlay Netsukuku over the Internet via geo-coordinate-driven tunnels, auto-replaced by physical links as they appear. | No `ModuloViphilama` in `ita/`; not implemented in the vala rewrite as far as this doc set shows. Open item. |
| 0011 | Carciofo ("the vegetable sister of Tor") | Tor-like anonymity layer: masqueraded hop chains, hidden servers via ANDNA+SNSD. | No corresponding vala module found. Open item / non-goal candidate. |
| 0012 | Net Split | Use the full IPv4 space while staying Internet-compatible: dual NTK/INET routing tables + `.ntk`/`.int` suffixes, `NETSPLIT_MARK` netfilter redirect via `127.0.0.0/8`. | [INFERENCE] Not covered by the `ita/` docs read; likely still a `system-ntkd`/deployment-layer concern, not a QSPN/Hooking/PeerServices concept. |
| 0013 | Caustic Routing | Recursive multipath generalization (Caustic Routing Tree) for load balancing. Explicitly marked "not complete" in the RFC itself. | Not found in `ita/`; `ModuloQspn/PercorsiDisgiunti.md` covers *disjoint routes* (single-path diversity, §5.1 of `qspn.pdf`) but not recursive multipath — different mechanism. |
| 0014 | P2P over Ntk | Generic DHT framework over the Netsukuku address space (`h(k)`, closest-IP hash-node, replication to nearest 3 backups). | **Superseded** by `ModuloPeers` (PeerServices/HDHT), which is the vala-era generalization of exactly this idea, adapted to the hierarchical `gsize(i)` address space instead of a flat IP-distance metric. |
| 0015 | Local ANDNA | Split hostname registration per address-level for faster local updates on IP change. | [INFERENCE] Not found addressed in the `ita/` docs read; PeerServices' HDHT walks the hierarchy per-request already (`ModuloPeers/AnalisiFunzionale.md:96-114`, `H_t` computed level-by-level), which is structurally similar but not confirmed as the same optimization. |
| — | `Ntk_andna_and_dns` (ANDNS protocol) | Defines the wire protocol (headers, query types, realms NTK/INET, rcodes) used by DNS↔ANDNA wrapper. | Not superseded; still the only documented wire format for `.ntk`/`.int` name resolution. References RFC 0009 throughout. |
| — | `Ntk_viphilama_static` | Alternate, simpler `vplamad` daemon sketch for RFC 0010 (unix-socket bridge to `ntkd`, no hooking, pure tunnel routing). | Superseded by nothing; RFC 0010 itself is unimplemented in `ita/`, see above. |

## Topology / addressing parameters

| parameter | legacy (Alpt, `topology.pdf`/`qspn.pdf`/c monolithic doc) | vala-era (`ita/ModuloQspn`, `ita/ModuloHooking`) |
|---|---|---|
| levels | fixed by address width: ipv4 → `n=4` (5 levels incl. level 0), ipv6 → `n=16` (17 levels). `topology.pdf:189-191`. | `levels` is a network-wide parameter read from the address (`i_qspn_get_levels()`), not hardwired — `research/impl/vala/qspn/api.vala:25`. |
| g-node size (fanout) | fixed `MAXGROUPNODE = 256` (2^8) at every level. `c-doc--main_doc-netsukuku-Npv7_HT-draft:392-393`; `topology.pdf:115,128`. | per-level, arbitrary `gsize(i)` (`gsizes[]` array), not required to be 256 or even uniform across levels — `vala-doc--ita-ModuloQspn-AnalisiFunzionale.md:136-142,355-356`. Generalizes the address space beyond octet-aligned IPv4/IPv6 grouping. |
| address format | position-only tuple `g₃.g₂.g₁.g₀` (IP-shaped, one byte/level in ipv4). Membership ID scheme in `topology.pdf:148-166`. | `Naddr` class: `pos: int[]` (per-level position) + `sizes: int[]` (per-level `gsize`), plus a separate `Fingerprint` (per-level "eldership"/uptime-like counters) for identity, decoupled from IP octets — `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:7-13`. No literal "NIP" acronym exists in either corpus; it is this report's shorthand for the `Naddr`/`pos`+`sizes` tuple, to contrast with the legacy `inet_prefix`-based address (`research/impl/c/netsukuku/src/inet.h:73-79`) which *is* the raw IPv4/IPv6 bytes. |
| virtual positions | not modeled; g-node splits/merges handled by rehook (§7.2 of `topology.pdf`). | explicit *virtual* `pos ≥ gsize(i)` values for a g-node mid-migration, decoupling topological address from allocation state — `vala-doc--ita-ModuloQspn-AnalisiFunzionale.md:403-449`. |
| IPv4 vs IPv6 mapping | direct: the Netsukuku address *is* the IP address, split into 256-groups; ipv4 4 levels / ipv6 16 levels, 144 KB vs 1996 KB of map memory. `c-doc--main_doc-netsukuku-Npv7_HT-draft:689-693`. | [INFERENCE] not addressed directly in the module docs read (`ModuloQspn`/`ModuloIdentities`); the `gsizes[]` generalization implies the IP-octet mapping becomes one *instance* of a configurable topology rather than the definition of it, but no explicit ipv4/ipv6 worked example was found in the files read for this note. |
| max-nodes math | `2^32` (ipv4) / `2^128` (ipv6) theoretical; `total_routes ≈ MAXGROUPNODE*(levels+1)` ⇒ 1024 routes (ipv4), 4352 (ipv6) — `vala-doc--misc-Ntk_scalability` (QSPN v1) and `c-doc--main_doc-netsukuku-Npv7_HT-draft:704-712`. Real deployments assume `10.0.0.0/8` ⇒ up to `2^22` (4M) nodes / 36 map entries — `vala-doc--ita-DemoneNTKD-AnalisiFunzionale.md:82-84`. | Route/map bound is `sum(gsize(i))` over levels rather than a fixed `256*(levels+1)`; no closed-form re-derivation found in the docs read — depends on the deployment's chosen `gsizes[]`. |

## Divergence list: vala-era spec vs. legacy C spec

- **QSPN v1 → v2**: v1 gives only download-best routes (asymmetric-route workaround needed, RFC-adjacent note in `Ntk_bandwidth_measurement`); v2 (`qspn.pdf`, 2009) adds the Extended Tracer Packet (ETP) for incremental updates and is "flattened" across levels via the *group rule*. The `ita/ModuloQspn` docs implement v2 semantics with a generalized `gsize(i)`, not v1's fixed 256-fanout math.
- **Hooking/Coordinator vs. old radar+hook**: legacy Npv7_HT conflates neighbor discovery ("Radar" — rtt/loss ping every ~10s per the c-repo FAQ) and network-merge logic ("Hook & Unhook", §5.2 of the monolithic draft) into the daemon core. Vala splits this into 3 independent modules: **Neighborhood** (link discovery/cost only), **Hooking** (network-merge negotiation, migration-path search — `vala-doc--ita-ModuloHooking-AnalisiFunzionale.md`), and **Coordinator** (per-g-node serialization/shared-memory service used *by* Hooking, formalizing the informal "coordinator node" sketched in `topology.pdf` §7.1.2 — `vala-doc--ita-ModuloCoordinator-AnalisiFunzionale.md`). The legacy "communicating vessels" uniform-distribution heuristic (`topology.pdf` §7.1) has no direct equivalent found in the `ita/ModuloHooking` docs read; Hooking instead reasons about relative network sizes (with a 20x-difference threshold) to decide merge direction.
- **PeerServices vs. old ANDNA-specific hash-node placement**: legacy ANDNA hard-codes its own hash-gnode/rounded-hash-gnode placement (`andna.pdf` §3.1-3.2) and its own counter-gnode anti-abuse scheme (RFC 0007). Vala factors this into a generic **PeerServices** module implementing a Hierarchical DHT (`H_t: S → dom(α_t)` computed level-by-level, `vala-doc--ita-ModuloPeers-AnalisiFunzionale.md:34-114`) that any service (ANDNA, Coordinator, ...) registers against with a `p_id`; RFC 0014 (P2P over Ntk, 2008) was the original proposal for this generalization and PeerServices is its realized, hierarchy-aware successor.
- **Address format**: legacy address *is* the raw IP (fixed 256-way split per level, `topology.pdf`). Vala's `Naddr` decouples "position in hierarchy" (`pos[]`) from "hierarchy shape" (`sizes[]`/`gsizes[]`), and separates identity/age (`Fingerprint`) from position — enabling non-256 group sizes and virtual (mid-migration) positions. See table above for citations.
- **ANDNA naming/branding**: backronym changed from the joke "Abnormal Netsukuku Domain Name Anarchy" (c-repo `manuals/andna`, `doc/main_doc/netsukuku` §7) to the serious "A Netsukuku Domain Name Architecture" (vala-repo `manuals/andna`, `andna.pdf`) — cosmetic but shows editorial maturation between snapshots.

## State of the C "revived" project

`git log` on `research/impl/c/netsukuku` (199 commits total, first `4c9747e` 2013-09-06, last `886a24a` 2025-06-12):

- `doc/` has **exactly one commit ever** (`f1761ca`, the 2013 import) — confirmed via `git log --oneline -- doc/`. None of the 12 years of subsequent activity touched documentation.
- The most recent commits (all 2025-01 to 2025-06) are purely build/CI maintenance: `5c94b31` "migrate SConstruct from Py2 to Py3", `da7d7f2` "Added -fcommon gcc option", `78e5b78`/`3645ecb`/`886a24a` add GitHub Actions workflow + `SECURITY.md` + issue-label config. No protocol or routing-algorithm changes appear anywhere in the visible recent history.
- Older activity (2013-2015) is scattered compile-error fixes (`inet.c`/`off64_t`, `insert_rule` checks) and a `netsplit` feature branch — i.e. RFC 0012 (Net Split) implementation work — plus spell-check and OpenWRT packaging.
- Conclusion: the "revived" C project keeps the legacy Npv7_HT (QSPN v1-era, radar/hook, flat 256-fanout) codebase *compiling on modern toolchains*; it does not track the split-PDF spec (topology.pdf/qspn.pdf v2/andna.pdf, 2007-2009) and has no relationship at all to the vala-era modular respec (2017-2020). It is a legacy/compatibility project, not a live spec source.

## Recommendation: normative document set for netsukuku-rs

1. **Primary/normative**: `ita/` module docs (`ModuloQspn`, `ModuloHooking`, `ModuloCoordinator`, `ModuloPeers`, `ModuloNeighborhood`, `ModuloIdentities`, `DemoneNTKD`, `Librerie`) — these define the address model (`Naddr`/`gsizes`), QSPN v2 semantics, and the Hooking/Coordinator/PeerServices architecture the Rust port should target. Cross-check against the corresponding Vala source (see notes 01/02) since the docs occasionally say **TODO** (e.g. `ModuloHooking/AnalisiFunzionale.md:2426`, RPC module in `DemoneNTKD/AnalisiFunzionale.md:134`).
2. **Secondary/background**: `topology.pdf`, `qspn.pdf`, `andna.pdf`, `inetntk.pdf` for the conceptual vocabulary (gnode, bnode, REM, fingerprint, hash-gnode) the `ita/` docs assume as read — `ita/DemoneNTKD/AnalisiFunzionale.md` explicitly builds on this vocabulary.
3. **RFC series**: treat individually per the adoption table above. Only RFC 0009 (SNSD) and RFC 0014 (P2P over Ntk → PeerServices) are confirmed live in the vala design; 0004/0005/0008 are explicitly dead; 0001/0002/0003/0007/0010/0011/0012/0013/0015 are either superseded by a different vala mechanism or **not found addressed** in the module docs read here (flagged `[INFERENCE]`/"open item" above) — treat as features to explicitly scope in/out of netsukuku-rs rather than assume-ported.
4. **Do not use** the c-repo's `doc/` tree (`c-doc--*`) as a spec source — it is a frozen, never-updated 2013 mirror of the *pre-2007-split* corpus, superseded on every count established above. Its only use is as evidence for the divergence list (naming/wording drift) and for the `netsplit_howto`/`igs_howto` HOWTOs the vala docs don't replace with anything (see open questions).

## Open questions / risks for the Rust port

- No literal "NIP" term exists in either corpus (see addressing table); confirm with the other note authors that "NIP" in the shared vocabulary is understood as shorthand for the `Naddr`(`pos[]`+`sizes[]`) tuple, not a distinct named format.
- RFC 0006 is missing entirely — worth a one-line web check of the original lab.dyne.org wiki (now defunct) before assuming it never existed, but not a blocker: nothing in either tree references it.
- Several legacy features have **no found vala-era replacement** in the docs read: IGS (RFC 0003, Internet gateway sharing), Net Split (RFC 0012), Viphilama (RFC 0010), Carciofo (RFC 0011), Local ANDNA (RFC 0015), the bandwidth-measurement cost model (RFC 0002), and the ANDNA-specific counter-gnode anti-abuse fix (RFC 0007). These may live in `research/impl/vala/andna`/`system-ntkd` source not covered by this documentation-only note, or may simply be unported ideas — needs a source-level check (out of scope here) before netsukuku-rs decides whether to implement them.
- `ita/ModuloHooking/AnalisiFunzionale.md` and `ita/DemoneNTKD/AnalisiFunzionale.md` both contain literal `**TODO**` sections (RPC module role, split-resolution algorithm tail) — the vala spec itself is not 100% complete; the Rust port will need to consult the corresponding Vala source (notes 01/02) to fill these gaps rather than the docs alone.
- IPv4-vs-IPv6 address-mapping under the generalized `gsizes[]` model (i.e., how/whether an IPv6-shaped deployment still gets a clean octet-per-level mapping) was not found spelled out in the module docs read; needs either a deeper `ita/ModuloQspn` read or a source-level check of `research/impl/vala/qspn` before the Rust port fixes its own address encoding.
- `Ntk_features_list`, `Ntk_scalability`, and the FAQ differ substantially between the two trees (51/8/562 diff-lines) with no timestamp on the vala side proving which is "later" beyond the repo pull dates — treat vala's copies as the presumed-later wiki state but do not treat either as authoritative for exact numeric claims (e.g. map-size KB figures) without cross-checking actual Vala source constants.
