//! Hooking: the bootstrap/join protocol.
//!
//! Ports `research/impl/vala/hooking/` (NORMATIVE upstream source)
//! faithfully, with dependency inversion in place of the upstream
//! composition (`research/notes/01-vala-core-routing.md` §6 "Open
//! questions": "Interface-only decoupling is the whole architecture").
//! Upstream's `HookingManager` is wired at daemon-composition time to
//! concrete QSPN/Identities/Coordinator adapters via three interfaces
//! (`IHookingMapPaths`, `ICoordinator`, `IIdentityArc`); this crate declares
//! those same capabilities as its own traits ([`view::QspnView`],
//! [`coordinator::CoordinatorClient`], [`stub::HookingStubFactory`]) so it
//! never depends on `ntk-qspn`/`ntk-identities`/`ntk-neighborhood`/
//! `ntk-coordinator` — the `ntkd` composition root (phase 4) implements
//! them by delegating to those real crates.
//!
//! # Layout
//! - [`domain`] — `TupleGNode` and every other hooking payload type
//!   (`serializables.vala`), plus the tuple helper functions
//!   (`structs.vala:76-165`).
//! - [`merge`] — the pure size-based merge-direction heuristic
//!   (`arc_handler.vala:150-214`).
//! - [`search`] — `execute_search`/`execute_explore`/`execute_delete_reserve`/
//!   `execute_mig` and the `find_shortest_mig` BFS (`hooking.vala:156-490`),
//!   decoupled from the wire via [`search::SearchRouter`].
//! - [`routing`] — [`routing::MessageRouting`], the real `SearchRouter` plus
//!   the inbound `route_*` handlers (`message_routing.vala`).
//! - [`arc`] — `ArcId` and the per-arc handler task
//!   (`arc_handler.vala:62-359`).
//! - [`manager`] — the actor ([`manager::spawn`], [`manager::HookingHandle`]).
//! - [`view`]/[`coordinator`]/[`stub`] — the three dependency-inverted
//!   traits this crate declares, plus [`fake`]'s in-memory implementations.
//! - [`rpc`] — inbound [`ntk_rpc::RpcHandler`] for the 10 `hooking_*`
//!   methods.
//! - [`wire`] — `proto/hooking.proto` <-> domain type conversions.
//! - [`config`] — every injectable timer/backoff.
//! - [`events`]/[`snapshot`] — the `broadcast` event stream and `watch`
//!   snapshot.

mod arc;
mod config;
mod coordinator;
mod domain;
mod error;
mod events;
mod fake;
mod idgen;
mod manager;
mod merge;
mod routing;
mod rpc;
mod search;
mod snapshot;
mod stub;
mod view;
mod wire;

pub use arc::ArcId;
pub use config::{HookingConfig, default_global_timeout};
pub use coordinator::{CoordinatorClient, CoordinatorError, MergeArbitrationRequest, Reservation};
pub use domain::{
    DeleteReservationRequest, EntryData, EvaluateEnterRequest, ExploreGNodeRequest,
    ExploreGNodeResponse, FinishEnterData, FinishMigrationData, MigOp, NetworkData,
    PairTupleGNodeInt, PathHop, RequestPacket, ResponsePacket, SearchMigrationPathErrorPkt,
    SearchMigrationPathRequest, SearchMigrationPathResponse, TupleGNode,
};
pub use error::HookingError;
pub use events::HookingEvent;
pub use fake::{FakeCoordinatorClient, FakeHookingStubFactory, FakeQspnView, ScriptedHookingStub};
pub use manager::{HookingHandle, HookingOrigin, spawn};
pub use merge::{MergeDecision, merge_direction, merge_tiebreak};
pub use routing::MessageRouting;
pub use rpc::HookingRpcHandler;
pub use search::{
    MigrationSolution, RoutingError, SearchRouter, SearchStepResult, execute_delete_reserve,
    execute_explore, execute_mig, execute_search, execute_shortest_mig, find_shortest_mig,
};
pub use snapshot::{ArcPhase, ChosenAddress, HookingSnapshot};
pub use stub::{HookingStub, HookingStubFactory};
pub use view::{AdjacentGNode, QspnView};
pub use wire::{
    DELETE_RESERVE_REQUEST_TAG, ENTRY_DATA_TAG, EXPLORE_REQUEST_TAG, EXPLORE_RESPONSE_TAG,
    MIG_REQUEST_TAG, MIG_RESPONSE_TAG, NETWORK_DATA_TAG, SEARCH_ERROR_TAG, SEARCH_REQUEST_TAG,
    SEARCH_RESPONSE_TAG, WireError, decode_delete_reserve_request, decode_entry_data,
    decode_explore_request, decode_explore_response, decode_mig_request, decode_mig_response,
    decode_network_data, decode_search_error, decode_search_request, decode_search_response,
    encode_delete_reserve_request, encode_entry_data, encode_explore_request,
    encode_explore_response, encode_mig_request, encode_mig_response, encode_network_data,
    encode_search_error, encode_search_request, encode_search_response,
};
