//! `TypedValue` codecs for the payloads that cross the boundary between
//! [`ntk_hooking::CoordinatorClient`] (the asker, implemented in [`crate::node::adapters`])
//! and `ntk_coordinator`'s `EvaluateEnterHandler`/`BeginEnterHandler`/`CompletedEnterHandler`/
//! `AbortEnterHandler`/`PropagationHandler` (the answerer, also implemented there).
//!
//! Neither `ntk-hooking` nor `ntk-coordinator` defines a wire encoding for these payloads —
//! by design, both cross that boundary as an opaque [`ntk_proto::v1::TypedValue`] the two
//! crates never interpret (`ntk_coordinator::traits`'s module doc: "declared as this crate's
//! own traits rather than a dependency on ntk-qspn/ntk-hooking"). Since every node in this
//! deployment runs the same `ntkd` binary, `ntkd` is free to pick its own encoding for its own
//! protocol data; this module does so with a plain `serde`+`toml` round trip (both already
//! workspace dependencies) rather than adding a new format crate — every payload here is a
//! small struct of primitives, so a text encoding costs nothing observable. Unlike
//! [`ntk_proto::domain::typed_value`]/`from_typed_value` (generic over `prost::Message`, which
//! none of these plain structs implement), this module builds/inspects [`TypedValue`] directly.

use std::collections::HashMap;

use ntk_hooking::{EntryData, EvaluateEnterRequest, FinishEnterData, FinishMigrationData};
use ntk_proto::v1::TypedValue;
use serde::{Deserialize, Serialize};

const TAG_EVALUATE_ENTER_REQUEST: &str = "ntkd.EvaluateEnterRequest";
const TAG_EVALUATE_ENTER_ANSWER: &str = "ntkd.EvaluateEnterAnswer";
const TAG_MIGRATION_ID: &str = "ntkd.MigrationId";
const TAG_ENTER_ID: &str = "ntkd.EnterId";
const TAG_FINISH_MIGRATION_DATA: &str = "ntkd.FinishMigrationData";
const TAG_FINISH_ENTER_DATA: &str = "ntkd.FinishEnterData";
const TAG_HOOKING_MEMORY: &str = "ntkd.HookingMemory";
const TAG_UNIT: &str = "ntkd.Unit";

#[derive(Serialize, Deserialize)]
struct Empty {}

/// A `TypedValue` carrying nothing but a tag — used for the infallible
/// `completed_enter`/`abort_enter` request bodies, whose only real content is `top`/`lvl`
/// itself (already carried by `CoordinatorExecuteArgs`/the DHT target).
pub fn encode_unit() -> TypedValue {
    wire(TAG_UNIT, &Empty {})
}

#[derive(Serialize, Deserialize)]
struct WireEvaluateEnterRequest {
    network_id: i64,
    neighbor_pos: Vec<u32>,
    neighbor_min_lvl: usize,
    min_lvl: usize,
    evaluate_enter_id: i32,
}

pub fn encode_evaluate_enter_request(req: &EvaluateEnterRequest) -> TypedValue {
    wire(
        TAG_EVALUATE_ENTER_REQUEST,
        &WireEvaluateEnterRequest {
            network_id: req.network_id,
            neighbor_pos: req.neighbor_pos.clone(),
            neighbor_min_lvl: req.neighbor_min_lvl,
            min_lvl: req.min_lvl,
            evaluate_enter_id: req.evaluate_enter_id,
        },
    )
}

/// # Errors
/// [`CodecError`] on a malformed/mistagged payload.
pub fn decode_evaluate_enter_request(tv: &TypedValue) -> Result<EvaluateEnterRequest, CodecError> {
    let w: WireEvaluateEnterRequest = unwire(tv, TAG_EVALUATE_ENTER_REQUEST)?;
    Ok(EvaluateEnterRequest {
        network_id: w.network_id,
        neighbor_pos: w.neighbor_pos,
        neighbor_min_lvl: w.neighbor_min_lvl,
        min_lvl: w.min_lvl,
        evaluate_enter_id: w.evaluate_enter_id,
    })
}

/// The servant's answer to `evaluate_enter` — this daemon's own arbitration outcome (see
/// [`crate::node::adapters`]'s module doc comment for the algorithm), encoded so the asker can
/// tell "proceed at this level" from "come back later" from "abandon, incompatible network"
/// without a third [`ntk_hooking::CoordinatorError`] wire representation to invent.
/// Internally tagged (`#[serde(tag = "kind")]`), not serde's default externally-tagged
/// representation: `toml` requires every document root to be a table, and an externally-tagged
/// unit variant (`AskAgain`/`IgnoreNetwork`) serializes to a bare string, which `toml::to_string`
/// cannot encode at all — internally-tagged folds the discriminant into the same table
/// (`{ kind = "AskAgain" }`), which is always table-shaped regardless of variant.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(tag = "kind")]
pub enum EvaluateEnterAnswer {
    Accepted { chosen_lvl: usize },
    AskAgain,
    IgnoreNetwork,
}

pub fn encode_evaluate_enter_answer(answer: EvaluateEnterAnswer) -> TypedValue {
    wire(TAG_EVALUATE_ENTER_ANSWER, &answer)
}

/// # Errors
/// [`CodecError`] on a malformed/mistagged payload.
pub fn decode_evaluate_enter_answer(tv: &TypedValue) -> Result<EvaluateEnterAnswer, CodecError> {
    unwire(tv, TAG_EVALUATE_ENTER_ANSWER)
}

/// Wraps a bare scalar in a one-field table — `toml::to_string` cannot encode a document whose
/// root is not a table, so `migration_id`/`enter_id` (plain `i32`s) can never be wired directly.
#[derive(Serialize, Deserialize)]
struct Scalar<T> {
    value: T,
}

pub fn encode_migration_id(migration_id: i32) -> TypedValue {
    wire(
        TAG_MIGRATION_ID,
        &Scalar {
            value: migration_id,
        },
    )
}

/// # Errors
/// [`CodecError`] on a malformed/mistagged payload.
pub fn decode_migration_id(tv: &TypedValue) -> Result<i32, CodecError> {
    unwire::<Scalar<i32>>(tv, TAG_MIGRATION_ID).map(|s| s.value)
}

pub fn encode_enter_id(enter_id: i32) -> TypedValue {
    wire(TAG_ENTER_ID, &Scalar { value: enter_id })
}

/// # Errors
/// [`CodecError`] on a malformed/mistagged payload.
pub fn decode_enter_id(tv: &TypedValue) -> Result<i32, CodecError> {
    unwire::<Scalar<i32>>(tv, TAG_ENTER_ID).map(|s| s.value)
}

/// One level's currently-granted enter election (`CoordGnodeMemory.hooking_memory`'s
/// Hooking-owned election fields, mirroring upstream's own
/// `HookingMemory.evaluate_enter_status`/`evaluate_enter_elected`,
/// `research/impl/vala/hooking/serializables.vala:301-323`) —
/// [`crate::node::adapters::EnterArbiter`]'s own doc has the full election algorithm.
/// Wall-clock-stamped, not a `std::time::Instant`, so a *different* physical node reading the
/// replicated record can independently judge staleness against `EnterArbiter::ELECTED_TTL`.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct ElectionRecord {
    pub network_id: i64,
    pub evaluate_enter_id: i32,
    pub granted_at_millis: u64,
}

/// The Coordinator-held Hooking-module shared memory
/// (`CoordGnodeMemory.hooking_memory: Object?`,
/// `research/impl/vala/coordinator/serializables.vala:182-201`) — this daemon's single opaque
/// blob for *every* piece of Hooking-owned per-network state its elected Coordinator persists,
/// not merge decisions alone: [`ntk_hooking::CoordinatorClient::decide_merge`]'s
/// verdicts and [`crate::node::adapters::EnterArbiter`]'s own enter elections, each keyed
/// independently so one writer's read-modify-write round trip never clobbers the other's
/// portion (both still race each other on the write itself — see `decide_merge`'s own doc on
/// why this memory has no compare-and-swap primitive).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HookingMemory {
    /// `neighbor_network_id -> (decision, decided_at_millis)`.
    pub merge_decisions: HashMap<i64, (bool, u64)>,
    /// `chosen_lvl -> election`.
    pub elections: HashMap<usize, ElectionRecord>,
}

/// Wire shape: `toml` map keys must be strings and `i64`/`usize` keys are not, so both maps
/// travel as tuples rather than as `HashMap`s directly.
#[derive(Serialize, Deserialize, Default)]
struct WireHookingMemory {
    /// `(neighbor_network_id, decision, decided_at_millis)`.
    merge_decisions: Vec<(i64, bool, u64)>,
    /// `(chosen_lvl, network_id, evaluate_enter_id, granted_at_millis)`.
    elections: Vec<(u32, i64, i32, u64)>,
}

/// Wall-clock milliseconds since the Unix epoch — the timestamp [`encode_hooking_memory`]
/// attaches to each merge verdict/election so a reader (possibly a different process) can
/// bound how long it trusts one, across process boundaries where a monotonic
/// [`std::time::Instant`] would not be comparable.
#[must_use]
pub fn now_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub fn encode_hooking_memory(mem: &HookingMemory) -> TypedValue {
    wire(
        TAG_HOOKING_MEMORY,
        &WireHookingMemory {
            merge_decisions: mem
                .merge_decisions
                .iter()
                .map(|(&k, &(decision, decided_at))| (k, decision, decided_at))
                .collect(),
            elections: mem
                .elections
                .iter()
                .map(|(&level, &e)| {
                    (
                        u32::try_from(level).unwrap_or(u32::MAX),
                        e.network_id,
                        e.evaluate_enter_id,
                        e.granted_at_millis,
                    )
                })
                .collect(),
        },
    )
}

/// # Errors
/// [`CodecError`] on a malformed/mistagged payload.
pub fn decode_hooking_memory(tv: &TypedValue) -> Result<HookingMemory, CodecError> {
    unwire::<WireHookingMemory>(tv, TAG_HOOKING_MEMORY).map(|w| HookingMemory {
        merge_decisions: w
            .merge_decisions
            .into_iter()
            .map(|(k, decision, decided_at)| (k, (decision, decided_at)))
            .collect(),
        elections: w
            .elections
            .into_iter()
            .map(
                |(level, network_id, evaluate_enter_id, granted_at_millis)| {
                    (
                        level as usize,
                        ElectionRecord {
                            network_id,
                            evaluate_enter_id,
                            granted_at_millis,
                        },
                    )
                },
            )
            .collect(),
    })
}

#[derive(Serialize, Deserialize)]
struct WireEntryData {
    network_id: i64,
    pos: Vec<u32>,
    elderships: Vec<i32>,
}

impl From<&EntryData> for WireEntryData {
    fn from(d: &EntryData) -> Self {
        Self {
            network_id: d.network_id,
            pos: d.pos.clone(),
            elderships: d.elderships.clone(),
        }
    }
}

impl From<WireEntryData> for EntryData {
    fn from(w: WireEntryData) -> Self {
        Self {
            network_id: w.network_id,
            pos: w.pos,
            elderships: w.elderships,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct WireFinishMigrationData {
    migration_id: i32,
    migration_data: WireEntryData,
    go_connectivity_position: u32,
}

pub fn encode_finish_migration_data(data: &FinishMigrationData) -> TypedValue {
    wire(
        TAG_FINISH_MIGRATION_DATA,
        &WireFinishMigrationData {
            migration_id: data.migration_id,
            migration_data: (&data.migration_data).into(),
            go_connectivity_position: data.go_connectivity_position,
        },
    )
}

/// # Errors
/// [`CodecError`] on a malformed/mistagged payload.
pub fn decode_finish_migration_data(tv: &TypedValue) -> Result<FinishMigrationData, CodecError> {
    let w: WireFinishMigrationData = unwire(tv, TAG_FINISH_MIGRATION_DATA)?;
    Ok(FinishMigrationData {
        migration_id: w.migration_id,
        migration_data: w.migration_data.into(),
        go_connectivity_position: w.go_connectivity_position,
    })
}

#[derive(Serialize, Deserialize)]
struct WireFinishEnterData {
    enter_id: i32,
    entry_data: WireEntryData,
    go_connectivity_position: u32,
}

pub fn encode_finish_enter_data(data: &FinishEnterData) -> TypedValue {
    wire(
        TAG_FINISH_ENTER_DATA,
        &WireFinishEnterData {
            enter_id: data.enter_id,
            entry_data: (&data.entry_data).into(),
            go_connectivity_position: data.go_connectivity_position,
        },
    )
}

/// # Errors
/// [`CodecError`] on a malformed/mistagged payload.
pub fn decode_finish_enter_data(tv: &TypedValue) -> Result<FinishEnterData, CodecError> {
    let w: WireFinishEnterData = unwire(tv, TAG_FINISH_ENTER_DATA)?;
    Ok(FinishEnterData {
        enter_id: w.enter_id,
        entry_data: w.entry_data.into(),
        go_connectivity_position: w.go_connectivity_position,
    })
}
/// Everything that can go wrong decoding one of this module's `TypedValue` payloads.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("TypedValue type_tag mismatch: expected {expected:?}, got {actual:?}")]
    TagMismatch { expected: String, actual: String },
    #[error("malformed ntkd-internal payload: {0}")]
    Toml(String),
}

fn wire<T: Serialize>(tag: &str, value: &T) -> TypedValue {
    let text = toml::to_string(value).expect("ntkd-internal payload types always serialize");
    TypedValue::new(tag, text.into_bytes())
}

fn unwire<T: for<'de> Deserialize<'de>>(
    tv: &TypedValue,
    expected_tag: &str,
) -> Result<T, CodecError> {
    if tv.type_tag != expected_tag {
        return Err(CodecError::TagMismatch {
            expected: expected_tag.to_owned(),
            actual: tv.type_tag.clone(),
        });
    }
    let text = std::str::from_utf8(&tv.payload).map_err(|e| CodecError::Toml(e.to_string()))?;
    toml::from_str(text).map_err(|e| CodecError::Toml(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluate_enter_request_round_trips() {
        let req = EvaluateEnterRequest {
            network_id: 42,
            neighbor_pos: vec![1, 2, 3],
            neighbor_min_lvl: 1,
            min_lvl: 0,
            evaluate_enter_id: 7,
        };
        let decoded = decode_evaluate_enter_request(&encode_evaluate_enter_request(&req)).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn hooking_memory_round_trips_and_starts_empty() {
        assert_eq!(
            decode_hooking_memory(&encode_hooking_memory(&HookingMemory::default())).unwrap(),
            HookingMemory::default()
        );
        let mut mem = HookingMemory::default();
        mem.merge_decisions.insert(11i64, (true, 1_000u64));
        mem.merge_decisions.insert(-5i64, (false, 2_000u64));
        mem.elections.insert(
            1,
            ElectionRecord {
                network_id: 42,
                evaluate_enter_id: 7,
                granted_at_millis: 3_000,
            },
        );
        let decoded = decode_hooking_memory(&encode_hooking_memory(&mem)).unwrap();
        assert_eq!(decoded, mem);
    }

    /// A write that only touches `merge_decisions` (`decide_merge`'s own read-modify-write)
    /// must not clobber an `elections` entry already in the same blob, and vice versa — the
    /// whole reason this is one combined struct rather than two independently-encoded tags.
    #[test]
    fn merge_decisions_and_elections_round_trip_independently() {
        let mut mem = HookingMemory::default();
        mem.elections.insert(
            0,
            ElectionRecord {
                network_id: 1,
                evaluate_enter_id: 1,
                granted_at_millis: 500,
            },
        );
        let mut mem = decode_hooking_memory(&encode_hooking_memory(&mem)).unwrap();
        mem.merge_decisions.insert(9, (true, 100));
        let decoded = decode_hooking_memory(&encode_hooking_memory(&mem)).unwrap();
        assert_eq!(decoded.merge_decisions.get(&9), Some(&(true, 100)));
        assert_eq!(
            decoded.elections.get(&0),
            Some(&ElectionRecord {
                network_id: 1,
                evaluate_enter_id: 1,
                granted_at_millis: 500,
            })
        );
    }

    /// Every variant, including the unit ones — these are exactly what panicked before this
    /// module switched `EvaluateEnterAnswer` to an internally tagged representation (see its
    /// doc comment): serde's default externally-tagged encoding turns a unit variant into a
    /// bare string, which `toml::to_string` cannot place at a document root.
    #[test]
    fn evaluate_enter_answer_round_trips_every_variant() {
        for answer in [
            EvaluateEnterAnswer::Accepted { chosen_lvl: 3 },
            EvaluateEnterAnswer::AskAgain,
            EvaluateEnterAnswer::IgnoreNetwork,
        ] {
            let decoded =
                decode_evaluate_enter_answer(&encode_evaluate_enter_answer(answer)).unwrap();
            assert_eq!(decoded, answer);
        }
    }

    /// `migration_id`/`enter_id` are bare `i32`s — before [`Scalar`] wrapped them in a table,
    /// `wire` panicked on every single call (a bare integer can never be a `toml` document root).
    #[test]
    fn migration_id_and_enter_id_round_trip() {
        assert_eq!(decode_migration_id(&encode_migration_id(-5)).unwrap(), -5);
        assert_eq!(decode_enter_id(&encode_enter_id(9)).unwrap(), 9);
    }

    #[test]
    fn finish_migration_data_round_trips() {
        let data = FinishMigrationData {
            migration_id: 11,
            migration_data: EntryData {
                network_id: 5,
                pos: vec![0, 1],
                elderships: vec![-1, 2],
            },
            go_connectivity_position: 3,
        };
        let decoded = decode_finish_migration_data(&encode_finish_migration_data(&data)).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn finish_enter_data_round_trips() {
        let data = FinishEnterData {
            enter_id: 21,
            entry_data: EntryData {
                network_id: 6,
                pos: vec![2],
                elderships: vec![0],
            },
            go_connectivity_position: 4,
        };
        let decoded = decode_finish_enter_data(&encode_finish_enter_data(&data)).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn unit_round_trips_and_decoders_reject_a_mismatched_tag() {
        decode_evaluate_enter_answer(&encode_unit())
            .expect_err("wrong tag must be rejected, not silently misdecoded");
        let tv = encode_evaluate_enter_answer(EvaluateEnterAnswer::AskAgain);
        assert_eq!(tv.type_tag, TAG_EVALUATE_ENTER_ANSWER);
    }
}
