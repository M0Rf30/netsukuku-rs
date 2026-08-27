//! `contact_peer`: recursive request routing to whichever node/g-node is closest to a target
//! address, keeping upstream's three distinct failure modes — servant `refuse` (level-scoped
//! exclusion), servant `redo_from_start` (full restart), and plain call timeout — as separate,
//! non-overlapping code paths (`research/impl/vala/peerservices/message_routing.vala:267-956`).
//! `forward_msg` is the server-side hop-by-hop counterpart; `replicate` layers RFC 0014 §2.2 step
//! 5's redundancy rule on top of `contact_peer`.
//!
//! **Deviation, deliberate**: upstream's `WaitingAnswer` is one shared mutable object whose
//! fields (`exclude_gnode`, `non_participant_gnode`, `response`, `refuse_message`,
//! `redo_from_start`, `missing_optional_maps`) any `set_*` RPC can write, and the waiting
//! tasklet re-checks *all* of them in a fixed priority order on every wakeup
//! (`message_routing.vala:420-538`) — an artifact of modeling every signal as the same generic
//! "something changed, go look" doorbell. This port instead gives each signal its own
//! [`crate::actor::RouteEvent`] variant delivered over an unbounded channel in arrival order.
//! For the overwhelmingly common case (one signal per wakeup) this is behaviorally identical;
//! when signals could otherwise race, delivering each as its own message removes the ambiguity
//! a shared-field priority scan has to resolve.
//!
//! **Scope note**: like [`crate::tuple`], this module assumes an always-fully-hooked local
//! identity (no virtual positions, no guest/host migration boundary) — see
//! [`crate::actor::Manager::new`]'s doc comment.

use std::sync::Arc;
use std::time::Duration;

use ntk_common::HCoord;
use ntk_proto::v1::TypedValue;
use tokio::time::Instant;

use crate::actor::{Handle, RouteEvent, all_gnodes_up_to_lvl};
use crate::service::{ExecError, Refusal, ServiceId};
use crate::stub::{PeerMessageForwarder, PeersStub};
use crate::tuple::{
    GNodeRelation, TupleGNode, TupleNode, approximate, convert_tuple_gnode, make_tuple_gnode,
    make_tuple_node, rebase_tuple_gnode, rebase_tuple_node, tuple_gnode_containing,
    tuple_node_to_tuple_gnode, visible_by_someone_inside_my_gnode,
};

/// Terminal failure of a whole `contact_peer` call, once every routing avenue is exhausted
/// (`PeersNoParticipantsInNetworkError`/`PeersDatabaseError`, `research/impl/vala/peerservices/
/// message_routing.vala:267-276,312-322`). The three *retryable* signals that drive the routing
/// state machine towards this outcome — refuse, redo-from-start, timeout — are not variants
/// here: they are handled internally and never escape as an `Err` on their own; only running out
/// of routing options entirely does.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ContactPeerError {
    /// No g-node is known to participate in this service at all (RFC 0014 §2.2 note 1: "There
    /// are no participants").
    #[error("no participants in the network for this service")]
    NoParticipants,
    /// Every candidate was exhausted via accumulated servant refusals.
    #[error("database error: {0}")]
    Database(String),
    /// The search exceeded [`crate::Config::max_contact_peer_hops`] hops without reaching a
    /// terminal outcome.
    ///
    /// **Deviation, deliberate**: upstream (`research/impl/vala/peerservices/
    /// message_routing.vala:267-956`) has no hop counter — only [`crate::Config::routing_timeout`]
    /// bounds a call, indirectly and only per attempt. This is this crate's own defensive guard
    /// against a routing pathology (a cycle of refusals/failures/restarts) consuming the whole
    /// timeout budget one hop at a time; see [`crate::Config::max_contact_peer_hops`]'s own doc.
    #[error("routing exceeded the maximum of {max} hops")]
    TooManyHops { max: usize },
}

fn format_refusals(refusals: &[Refusal]) -> String {
    let mut out = String::new();
    for r in refusals {
        out.push_str(&r.message);
        out.push_str(" - ");
    }
    out
}

/// The peerservices actor already shut down (cancelled) mid-call. A live in-flight
/// `contact_peer`/`replicate` racing that shutdown treats it as an ordinary terminal routing
/// failure, not a panic — there is no longer anyone local to route through.
fn actor_shut_down() -> ContactPeerError {
    ContactPeerError::Database("peerservices actor is shutting down".to_owned())
}

impl Handle {
    /// Routes `request` to whichever node is closest to `x_macron` (RFC 0014 §2, the `h(k) =
    /// H(h'(k))` composition; `contact_peer`, `research/impl/vala/peerservices/
    /// message_routing.vala:267-572`). `x_macron = None` (or an empty tuple) means "route to
    /// myself". `exclude_my_gnode`, if given, excludes every g-node up to that level from
    /// consideration up front (used by callers that already know their own g-node shouldn't
    /// answer). `seed_exclude_tuple_list` seeds already-excluded g-nodes, letting
    /// [`Handle::replicate`] chain multiple calls that never revisit the same node.
    ///
    /// # Errors
    /// [`ContactPeerError::NoParticipants`] or [`ContactPeerError::Database`] once every routing
    /// avenue is exhausted.
    pub async fn contact_peer(
        &self,
        p_id: ServiceId,
        x_macron: Option<TupleNode>,
        request: TypedValue,
        timeout_exec: Duration,
        exclude_my_gnode: Option<usize>,
        seed_exclude_tuple_list: Vec<TupleGNode>,
    ) -> Result<(TypedValue, TupleNode), ContactPeerError> {
        let target_levels = x_macron.as_ref().map_or(0, TupleNode::top);
        let topology = self.topology().clone();
        let my_pos = self.my_pos().positions().to_vec();
        // Counts every candidate `approximate()` resolves across the whole call, including
        // across `'restart` — see `ContactPeerError::TooManyHops`'s own doc for why it must not
        // reset with a restart.
        let mut hops: usize = 0;

        'restart: loop {
            let mut refuse_messages: Vec<Refusal> = Vec::new();
            let Some(mut exclude_gnode_list) =
                self.non_participant_gnodes(p_id, target_levels).await
            else {
                return Err(actor_shut_down());
            };
            if let Some(lvl) = exclude_my_gnode {
                exclude_gnode_list.extend(all_gnodes_up_to_lvl(&topology, &my_pos, lvl));
            }
            let mut exclude_tuple_list = seed_exclude_tuple_list.clone();
            for gn in &exclude_tuple_list {
                if let (GNodeRelation::Visible, ret) = convert_tuple_gnode(&my_pos, gn) {
                    exclude_gnode_list.push(ret);
                }
            }
            let mut non_participant_tuple_list: Vec<TupleGNode> = Vec::new();

            'attempt: loop {
                hops += 1;
                if hops > self.config.max_contact_peer_hops {
                    tracing::debug!(
                        ?p_id,
                        hops,
                        max = self.config.max_contact_peer_hops,
                        "TRACE contact_peer: hop bound exceeded"
                    );
                    return Err(ContactPeerError::TooManyHops {
                        max: self.config.max_contact_peer_hops,
                    });
                }
                let mut respondant: Option<TupleNode> = None;

                let Some(x) = approximate(
                    &topology,
                    &my_pos,
                    x_macron.as_ref(),
                    &exclude_gnode_list,
                    |h| self.env.gnode_exists(h),
                ) else {
                    tracing::debug!(
                        ?p_id,
                        ?x_macron,
                        ?my_pos,
                        ?exclude_gnode_list,
                        "TRACE contact_peer: approximate found no candidate"
                    );
                    return Err(if refuse_messages.is_empty() {
                        ContactPeerError::NoParticipants
                    } else {
                        ContactPeerError::Database(format_refusals(&refuse_messages))
                    });
                };
                tracing::debug!(
                    ?p_id,
                    ?x_macron,
                    ?my_pos,
                    ?x,
                    self_loop = (x.level == 0 && x.pos == my_pos[0]),
                    ?exclude_gnode_list,
                    "TRACE contact_peer: approximate resolved elect target"
                );

                if x.level == 0 && x.pos == my_pos[0] {
                    let outcome = self.exec_local(p_id, request.clone(), &[]).await;
                    tracing::debug!(
                        ?p_id,
                        ?my_pos,
                        outcome = ?outcome,
                        "TRACE contact_peer: self-loop served locally"
                    );
                    match outcome {
                        None => {
                            // Not actually registered here; exclude myself and keep routing.
                            exclude_gnode_list.push(HCoord::new(0, my_pos[0]));
                            continue 'attempt;
                        }
                        Some(Ok(response)) => {
                            let respondant = if target_levels == 0 {
                                TupleNode::new(topology.clone(), Vec::new())
                                    .expect("empty tuple is always valid")
                            } else {
                                make_tuple_node(
                                    &topology,
                                    &my_pos,
                                    HCoord::new(0, my_pos[0]),
                                    target_levels,
                                )
                            };
                            return Ok((response, respondant));
                        }
                        Some(Err(ExecError::RedoFromStart)) => continue 'restart,
                        Some(Err(ExecError::Refuse(refusal))) => {
                            let lvl = refusal.level;
                            refuse_messages.push(refusal);
                            if refuse_messages.len() > self.config.max_refuse_messages {
                                refuse_messages.remove(0);
                            }
                            exclude_gnode_list
                                .extend(all_gnodes_up_to_lvl(&topology, &my_pos, lvl));
                            continue 'attempt;
                        }
                    }
                }

                let Some(msg_id) = self.next_msg_id().await else {
                    return Err(actor_shut_down());
                };

                let n = make_tuple_node(&topology, &my_pos, HCoord::new(0, my_pos[0]), x.level + 1);
                let auth = self.sign_origin(n.positions(), p_id, &request);
                let mf = PeerMessageForwarder {
                    inside_level: target_levels,
                    n,
                    x_macron: (x.level > 0).then(|| {
                        TupleNode::new(
                            topology.clone(),
                            x_macron
                                .as_ref()
                                .expect("x.level > 0 implies x_macron is Some")
                                .positions()[..x.level]
                                .to_vec(),
                        )
                        .expect("prefix of a valid tuple is valid")
                    }),
                    lvl: x.level,
                    pos: x.pos,
                    p_id,
                    msg_id,
                    exclude_tuple_list: exclude_tuple_list
                        .iter()
                        .filter_map(|t| {
                            let (rel, ret) = convert_tuple_gnode(&my_pos, t);
                            (rel == GNodeRelation::Hidden && ret == x).then(|| {
                                TupleGNode::new(
                                    topology.clone(),
                                    x.level,
                                    t.positions()[..x.level - t.level()].to_vec(),
                                )
                                .expect("prefix of a valid tuple is valid")
                            })
                        })
                        .collect(),
                    non_participant_tuple_list: non_participant_tuple_list
                        .iter()
                        .filter(|t| visible_by_someone_inside_my_gnode(&my_pos, t, x.level + 1))
                        .cloned()
                        .collect(),
                    auth,
                };

                let min_target = make_tuple_gnode(&topology, &my_pos, x, x.level + 1);
                let Some(mut rx) = self
                    .register_waiting(msg_id, min_target.clone(), Some(request.clone()))
                    .await
                else {
                    return Err(actor_shut_down());
                };

                let sent = self.try_forward(&mf, x).await;
                if !sent {
                    self.unregister_waiting(mf.msg_id).await;
                    tokio::time::sleep(self.config.gateway_retry_backoff).await;
                    continue 'attempt;
                }

                let mut timeout = self
                    .config
                    .routing_timeout(self.env.nodes_in_my_group(x.level + 1));
                let mut min_target = min_target;

                loop {
                    let event = match tokio::time::timeout(timeout, rx.recv()).await {
                        Ok(Some(event)) => event,
                        Ok(None) | Err(_) => {
                            let t = rebase_tuple_gnode(&my_pos, &min_target, target_levels);
                            tracing::debug!(
                                ?p_id,
                                ?my_pos,
                                ?t,
                                "TRACE contact_peer: attempt timed out"
                            );
                            if let (GNodeRelation::Visible, ret) = convert_tuple_gnode(&my_pos, &t)
                            {
                                exclude_gnode_list.push(ret);
                            }
                            exclude_tuple_list.push(t);
                            self.unregister_waiting(mf.msg_id).await;
                            continue 'attempt;
                        }
                    };
                    match event {
                        RouteEvent::NextDestination(t) => {
                            min_target = t;
                        }
                        RouteEvent::Failure(t) => {
                            let t = rebase_tuple_gnode(&my_pos, &t, target_levels);
                            tracing::debug!(
                                ?p_id,
                                ?my_pos,
                                ?t,
                                "TRACE contact_peer: remote Failure received"
                            );
                            if let (GNodeRelation::Visible, ret) = convert_tuple_gnode(&my_pos, &t)
                            {
                                exclude_gnode_list.push(ret);
                            }
                            exclude_tuple_list.push(t);
                            self.unregister_waiting(mf.msg_id).await;
                            continue 'attempt;
                        }
                        RouteEvent::NonParticipant(t) => {
                            let t = rebase_tuple_gnode(&my_pos, &t, target_levels);
                            if let (GNodeRelation::Visible, ret) = convert_tuple_gnode(&my_pos, &t)
                            {
                                exclude_gnode_list.push(ret);
                            }
                            exclude_tuple_list.push(t.clone());
                            non_participant_tuple_list.push(t);
                            self.unregister_waiting(mf.msg_id).await;
                            continue 'attempt;
                        }
                        RouteEvent::MissingOptionalMaps => {
                            self.unregister_waiting(mf.msg_id).await;
                            tokio::time::sleep(self.config.gateway_retry_backoff).await;
                            continue 'restart;
                        }
                        RouteEvent::RespondantNode(n) if respondant.is_none() => {
                            respondant = Some(rebase_tuple_node(&my_pos, &n, target_levels));
                            timeout = timeout_exec;
                        }
                        RouteEvent::Response(resp) => {
                            self.unregister_waiting(mf.msg_id).await;
                            let respondant = respondant
                                .expect("a Response only follows a registered respondant");
                            return Ok((resp, respondant));
                        }
                        RouteEvent::Refuse(refusal) if respondant.is_some() => {
                            let respondant_ref = respondant.as_ref().expect("checked by guard");
                            let level = refusal.level;
                            tracing::debug!(
                                ?p_id,
                                ?my_pos,
                                level,
                                message = %refusal.message,
                                ?respondant_ref,
                                "TRACE contact_peer: remote Refuse received"
                            );
                            refuse_messages.push(refusal);
                            if refuse_messages.len() > self.config.max_refuse_messages {
                                refuse_messages.remove(0);
                            }
                            match tuple_gnode_containing(
                                &tuple_node_to_tuple_gnode(respondant_ref),
                                level,
                            ) {
                                Ok(t) => {
                                    match convert_tuple_gnode(&my_pos, &t) {
                                        (GNodeRelation::Visible, ret) => {
                                            exclude_gnode_list.push(ret);
                                        }
                                        (GNodeRelation::Mine, _) => exclude_gnode_list.extend(
                                            all_gnodes_up_to_lvl(&topology, &my_pos, level),
                                        ),
                                        (GNodeRelation::Hidden, _) => {}
                                    }
                                    exclude_tuple_list.push(t);
                                }
                                // `level` names no real ancestor (at or beyond this tuple's own
                                // scope boundary): fall back to the same coarse exclusion already
                                // used for `GNodeRelation::Mine` above, rather than a panic.
                                Err(_) => exclude_gnode_list
                                    .extend(all_gnodes_up_to_lvl(&topology, &my_pos, level)),
                            }
                            self.unregister_waiting(mf.msg_id).await;
                            continue 'attempt;
                        }
                        RouteEvent::RedoFromStart if respondant.is_some() => {
                            self.unregister_waiting(mf.msg_id).await;
                            continue 'restart;
                        }
                        // A stray/out-of-order Refuse or RedoFromStart that arrived before any
                        // RespondantNode: upstream's own guard (`respondant != null && ...`,
                        // `message_routing.vala:492,528`) drops these identically.
                        // A stray duplicate `RespondantNode` (already have one) is likewise
                        // dropped rather than overwriting a live respondant.
                        RouteEvent::Refuse(_)
                        | RouteEvent::RedoFromStart
                        | RouteEvent::RespondantNode(_) => {}
                    }
                }
            }
        }
    }

    /// RFC 0014 §2.2 step 5's redundancy rule: after a hash node accepts a request, replicate it
    /// to `q` more nodes with the closest position to the target, so any of them can take over
    /// if the hash node dies (`begin_replica`/`next_replica`,
    /// `research/impl/vala/peerservices/databases.vala:246-292`). Each successive
    /// [`Handle::contact_peer`] call excludes every node already collected, so replicas are
    /// always distinct. Stops early (with fewer than `q` replicas) if routing is exhausted —
    /// mirrors upstream's `next_replica` returning `false` on
    /// `PeersNoParticipantsInNetworkError`/`PeersDatabaseError`.
    ///
    /// **Bounded wall clock, deliberate**: this loop is serial *by necessity* — each iteration
    /// depends on the previous one's `exclude_tuple_list` to keep replicas distinct, so it
    /// cannot be `join_all`'d without either losing that distinctness or risking every write
    /// landing on the same node. Serial `q` attempts at up to `timeout_exec` each is still
    /// unbounded in `q` (ANDNA's own `q = 31` could serialize to ~155s for one registration —
    /// the audit finding this fixes), so the whole call is additionally capped at
    /// `timeout_exec * `[`Config::replicate_deadline_multiplier`], independent of `q`. Once that
    /// deadline passes, this method returns whatever replicas it already has — the same
    /// "partial results are a valid outcome" contract `next_replica` already has, just also
    /// reachable by wall clock instead of only by routing exhaustion.
    pub async fn replicate(
        &self,
        p_id: ServiceId,
        target: TupleNode,
        request: TypedValue,
        timeout_exec: Duration,
        q: u32,
    ) -> Vec<(TypedValue, TupleNode)> {
        let deadline = timeout_exec.saturating_mul(self.config.replicate_deadline_multiplier);
        let start = Instant::now();
        let mut replicas = Vec::new();
        let mut exclude_tuple_list: Vec<TupleGNode> = Vec::new();
        while (replicas.len() as u32) < q {
            let Some(remaining) = deadline.checked_sub(start.elapsed()) else {
                tracing::debug!(
                    ?p_id,
                    collected = replicas.len(),
                    q,
                    "TRACE replicate: overall deadline elapsed, returning partial replicas"
                );
                break;
            };
            let outcome = tokio::time::timeout(
                remaining,
                self.contact_peer(
                    p_id,
                    Some(target.clone()),
                    request.clone(),
                    timeout_exec,
                    None,
                    exclude_tuple_list.clone(),
                ),
            )
            .await;
            match outcome {
                Ok(Ok((response, respondant))) => {
                    exclude_tuple_list.push(tuple_node_to_tuple_gnode(&respondant));
                    replicas.push((response, respondant));
                }
                Ok(Err(_)) => break,
                Err(_) => {
                    tracing::debug!(
                        ?p_id,
                        collected = replicas.len(),
                        q,
                        "TRACE replicate: overall deadline elapsed mid-attempt, returning partial replicas"
                    );
                    break;
                }
            }
        }
        replicas
    }

    /// Server-side counterpart of `contact_peer`: routes a forwarded message one hop deeper, or
    /// (once `am_i_servant_for` this message) fetches and executes the request
    /// (`forward_msg`, `research/impl/vala/peerservices/message_routing.vala:716-956`).
    pub(crate) async fn forward_msg(&self, mf: PeerMessageForwarder) {
        let topology = self.topology().clone();
        let my_pos = self.my_pos().positions().to_vec();

        if my_pos[mf.lvl] != mf.pos {
            self.relay(&mf, HCoord::new(mf.lvl, mf.pos)).await;
            return;
        }

        let Some(optional) = self.is_service_optional(mf.p_id).await else {
            return;
        };
        let below_level = self.snapshot_retrieved_below_level();
        if optional
            && mf
                .x_macron
                .as_ref()
                .is_some_and(|xm| below_level < xm.top())
        {
            if let Some(originator) = self.env.dial(&mf.n) {
                let _ = originator.set_missing_optional_maps(mf.msg_id).await;
            }
            return;
        }
        if optional {
            let Some(participates) = self.gnode_participates(mf.p_id, mf.lvl).await else {
                return;
            };
            if !participates {
                if let Some(originator) = self.env.dial(&mf.n) {
                    let gn = make_tuple_gnode(
                        &topology,
                        &my_pos,
                        HCoord::new(mf.lvl, mf.pos),
                        mf.n.top(),
                    );
                    let _ = originator.set_non_participant(mf.msg_id, gn).await;
                }
                return;
            }
        }

        let Some(mut exclude_gnode_list) =
            self.non_participant_gnodes(mf.p_id, mf.inside_level).await
        else {
            return;
        };
        for gn in &mf.exclude_tuple_list {
            match convert_tuple_gnode(&my_pos, gn) {
                (GNodeRelation::Mine, ret) => {
                    exclude_gnode_list.extend(all_gnodes_up_to_lvl(&topology, &my_pos, ret.level))
                }
                (GNodeRelation::Visible, ret) => exclude_gnode_list.push(ret),
                (GNodeRelation::Hidden, _) => {}
            }
        }

        loop {
            let Some(x) = approximate(
                &topology,
                &my_pos,
                mf.x_macron.as_ref(),
                &exclude_gnode_list,
                |h| self.env.gnode_exists(h),
            ) else {
                if let Some(originator) = self.env.dial(&mf.n) {
                    let gn = make_tuple_gnode(
                        &topology,
                        &my_pos,
                        HCoord::new(mf.lvl, mf.pos),
                        mf.n.top(),
                    );
                    let _ = originator.set_failure(mf.msg_id, gn).await;
                }
                return;
            };

            if x.level == 0 && x.pos == my_pos[0] {
                let Some(originator) = self.env.dial(&mf.n) else {
                    return;
                };
                let tuple_respondant =
                    make_tuple_node(&topology, &my_pos, HCoord::new(0, my_pos[0]), mf.n.top());
                let Ok(request) = originator
                    .get_request(mf.msg_id, tuple_respondant.clone())
                    .await
                else {
                    return;
                };
                if let Err(err) = self
                    .verify_origin(mf.auth.as_ref(), mf.n.positions(), mf.p_id, &request)
                    .await
                {
                    tracing::debug!(
                        ?my_pos,
                        ?mf.p_id,
                        %err,
                        "TRACE handle_incoming_message: rejecting a request that failed origin-auth"
                    );
                    let _ = originator
                        .set_refuse_message(
                            mf.msg_id,
                            Refusal {
                                level: 0,
                                message: format!("origin authentication failed: {err}"),
                            },
                            tuple_respondant,
                        )
                        .await;
                    return;
                }
                match self.exec_local(mf.p_id, request, mf.n.positions()).await {
                    None => {} // not actually registered here; silently give up rather than answer wrongly
                    Some(Ok(response)) => {
                        let _ = originator
                            .set_response(mf.msg_id, response, tuple_respondant)
                            .await;
                    }
                    Some(Err(ExecError::RedoFromStart)) => {
                        let _ = originator
                            .set_redo_from_start(mf.msg_id, tuple_respondant)
                            .await;
                    }
                    Some(Err(ExecError::Refuse(refusal))) => {
                        tracing::debug!(
                            ?my_pos,
                            level = refusal.level,
                            message = %refusal.message,
                            "TRACE handle_incoming_message: sending Refuse to originator"
                        );
                        let _ = originator
                            .set_refuse_message(mf.msg_id, refusal, tuple_respondant)
                            .await;
                    }
                }
                return;
            }

            let mut mf2 = mf.clone();
            mf2.lvl = x.level;
            mf2.pos = x.pos;
            mf2.x_macron = (x.level > 0).then(|| {
                TupleNode::new(
                    topology.clone(),
                    mf.x_macron
                        .as_ref()
                        .expect("x.level > 0 implies x_macron is Some")
                        .positions()[..x.level]
                        .to_vec(),
                )
                .expect("prefix of a valid tuple is valid")
            });
            mf2.exclude_tuple_list = mf
                .exclude_tuple_list
                .iter()
                .filter_map(|t| {
                    let (rel, ret) = convert_tuple_gnode(&my_pos, t);
                    (rel == GNodeRelation::Hidden && ret == x).then(|| {
                        TupleGNode::new(
                            topology.clone(),
                            x.level,
                            t.positions()[..x.level - t.level()].to_vec(),
                        )
                        .expect("prefix of a valid tuple is valid")
                    })
                })
                .collect();
            mf2.non_participant_tuple_list = mf
                .non_participant_tuple_list
                .iter()
                .filter(|t| visible_by_someone_inside_my_gnode(&my_pos, t, x.level + 1))
                .cloned()
                .collect();

            let delivered = self.try_forward(&mf2, x).await;
            if delivered {
                if let Some(originator) = self.env.dial(&mf.n) {
                    let gn = make_tuple_gnode(&topology, &my_pos, x, mf.n.top());
                    let _ = originator.set_next_destination(mf.msg_id, gn).await;
                }
                return;
            }
            tokio::time::sleep(self.config.gateway_retry_backoff).await;
        }
    }

    /// Repeatedly asks the environment for a gateway towards `target` and tries
    /// `forward_peer_message` against each candidate in turn, honoring
    /// [`Config::max_relay_attempts`] and sleeping [`Config::gateway_retry_backoff`] between
    /// attempts, and bailing out early once the owning `Manager` has already shut down
    /// ([`Handle::is_shut_down`]). Shared by [`Handle::contact_peer`]'s and
    /// [`Handle::forward_msg`]'s own per-hop dispatch loops and by [`Handle::relay`] — see
    /// `relay`'s own doc for the upstream citation and the reasoning behind the bound. Returns
    /// `true` once a candidate accepted the message.
    async fn try_forward(&self, mf: &PeerMessageForwarder, target: HCoord) -> bool {
        let mut failed: Option<Arc<dyn PeersStub>> = None;
        for _ in 0..self.config.max_relay_attempts {
            if self.is_shut_down() {
                return false;
            }
            let Some(gw) = self.env.gateway(target, failed.as_ref()) else {
                return false;
            };
            match gw.forward_peer_message(mf.clone()).await {
                Ok(()) => return true,
                Err(_) => failed = Some(gw),
            }
            tokio::time::sleep(self.config.gateway_retry_backoff).await;
        }
        false
    }

    /// Keeps forwarding `mf` towards `target` until a gateway accepts it, no candidate remains,
    /// the attempt is exhausted, or the owning `Manager` has already shut down (the "not yet at
    /// my level/position" branch of `forward_msg`, `message_routing.vala:934-955`).
    ///
    /// **Diagnosed live**: before [`Handle::try_forward`] existed, this loop retried
    /// `self.env.gateway`/`forward_peer_message` with no bound, no backoff, and no shutdown
    /// check. A real-kernel capture of
    /// `isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit` caught a node's dedicated OS
    /// thread mid-poll here, `strace` showing zero syscalls across 2-3s at ~100% CPU:
    /// `RoutingEnvAdapter::gateway` (`ntkd`) used to discard `failed` entirely and always
    /// re-resolve the identical candidate, so once that candidate's real connection broke,
    /// every iteration resolved synchronously and the task never yielded to its single-threaded
    /// runtime — no `CancellationToken` check, task-abort, or tokio coop budget can preempt a
    /// loop with no real `.await` suspension point in it at all.
    ///
    /// Upstream's own production `get_gateway` (`research/impl/vala/ntkd/peers_helpers.vala:
    /// 72-135`) converges instead: given a non-null `failed`, it physically removes the
    /// underlying neighborhood arc and re-queries fresh paths, so a single-path target's very
    /// next call returns nothing and upstream's own loop hits its unconditional "give up
    /// routing" exit (`message_routing.vala:941-943`) after exactly one failure — upstream
    /// carries no numeric attempt cap or backoff at this layer because exclusion alone is its
    /// whole convergence argument. This port's `RoutingEnvAdapter::gateway` now honours `failed`
    /// the same way, without the destructive arc teardown (see that method's own doc), so the
    /// same convergence holds for a correct [`RoutingEnv`] — [`Handle::try_forward`]'s bound and
    /// backoff are this crate's own defensive backstop on top, so a dead gateway can never wedge
    /// the calling task's runtime regardless of the injected environment's own behavior.
    async fn relay(&self, mf: &PeerMessageForwarder, target: HCoord) {
        self.try_forward(mf, target).await;
    }
}

#[cfg(test)]
mod relay_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use ntk_common::{Naddr, Topology};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::actor::Manager;
    use crate::config::Config;
    use crate::participation::ParticipantSet;
    use crate::stub::{GetRequestError, RoutingEnv, StubCallError};

    /// A gateway stub whose `forward_peer_message` fails on every call with no real `.await`
    /// suspension point at all (the returned future contains no internal `.await`, so it
    /// resolves on its very first poll) — reproducing the live-kernel capture `relay`'s own doc
    /// describes: a thread mid-poll inside `forward_peer_message`, zero syscalls, ~100% CPU.
    /// Self-bounded to 5s of wall time so a still-spinning (pre-fix) `relay` cannot hang this
    /// *test process* forever, only fail this test's own bound — mirrors
    /// `ntkd::node::supervisor::drain_tasks_tests::returns_promptly_when_a_task_never_yields`'s
    /// own `stop_at` guard.
    struct AlwaysFailStub {
        calls: Arc<AtomicUsize>,
        give_up_at: Instant,
    }

    impl PeersStub for AlwaysFailStub {
        fn forward_peer_message(
            &self,
            _msg: PeerMessageForwarder,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let past_deadline = Instant::now() > self.give_up_at;
            Box::pin(async move {
                if past_deadline {
                    Ok(())
                } else {
                    Err(StubCallError("gateway unreachable".to_owned()))
                }
            })
        }
        fn get_request(
            &self,
            _msg_id: i32,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<TypedValue, GetRequestError>> {
            unreachable!("not exercised by this test")
        }
        fn set_response(
            &self,
            _msg_id: i32,
            _response: TypedValue,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_refuse_message(
            &self,
            _msg_id: i32,
            _refusal: Refusal,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_redo_from_start(
            &self,
            _msg_id: i32,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_next_destination(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_failure(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_non_participant(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_missing_optional_maps(
            &self,
            _msg_id: i32,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_participant(
            &self,
            _p_id: ServiceId,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn give_participant_maps(
            &self,
            _maps: ParticipantSet,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn ask_participant_maps(
            &self,
        ) -> futures::future::BoxFuture<'_, Result<ParticipantSet, StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    /// Always hands out the same never-succeeding gateway regardless of `failed` — the exact
    /// pre-fix shape of `ntkd`'s own `RoutingEnvAdapter::gateway` (`relay`'s own doc), which
    /// discarded `failed` entirely. `relay`'s own bound must hold even against an environment
    /// that cannot (or will not) exclude a known-dead candidate.
    struct DeadGatewayEnv {
        stub: Arc<AlwaysFailStub>,
    }

    impl RoutingEnv for DeadGatewayEnv {
        fn gnode_exists(&self, _hc: HCoord) -> bool {
            true
        }
        fn gateway(
            &self,
            _hc: HCoord,
            _failed: Option<&Arc<dyn PeersStub>>,
        ) -> Option<Arc<dyn PeersStub>> {
            Some(self.stub.clone())
        }
        fn dial(&self, _n: &TupleNode) -> Option<Arc<dyn PeersStub>> {
            None
        }
        fn nodes_in_my_group(&self, _level: usize) -> usize {
            1
        }
        fn neighbors(&self) -> Vec<Arc<dyn PeersStub>> {
            Vec::new()
        }
    }

    fn minimal_mf(topology: &Topology) -> PeerMessageForwarder {
        PeerMessageForwarder {
            inside_level: topology.levels(),
            n: TupleNode::new(topology.clone(), vec![0, 0]).unwrap(),
            x_macron: None,
            lvl: 0,
            pos: 1,
            p_id: ServiceId::new(1),
            msg_id: 0,
            exclude_tuple_list: Vec::new(),
            non_participant_tuple_list: Vec::new(),
            auth: None,
        }
    }

    /// Pins the fix: `relay` against a gateway that always fails must return control to its
    /// runtime and terminate within a small bound, not spin forever — asserting completion
    /// under a wall-clock bound, not merely "it eventually stopped" (a test that would also pass
    /// against the pre-fix spinning version proves nothing). Multi-thread runtime (mirrors
    /// `drain_tasks_tests`' own reasoning): a still-spinning `relay` has no real `.await`
    /// suspension point, so it occupies its worker thread completely; only a second worker
    /// thread lets this test's own `timeout` fire at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn relay_against_a_persistently_dead_gateway_returns_promptly() {
        let calls = Arc::new(AtomicUsize::new(0));
        let stub = Arc::new(AlwaysFailStub {
            calls: calls.clone(),
            give_up_at: Instant::now() + Duration::from_secs(5),
        });
        let env: Arc<dyn RoutingEnv> = Arc::new(DeadGatewayEnv { stub });
        let topology = Topology::new([2, 2]).unwrap();
        let my_pos = Naddr::new(topology.clone(), vec![0, 0]).unwrap();
        let (manager, handle) = Manager::new(
            topology.clone(),
            my_pos,
            env,
            Config::default(),
            topology.levels(),
        );
        let cancel = CancellationToken::new();
        let manager_task = tokio::spawn(manager.run(cancel.child_token()));

        let mf = minimal_mf(&topology);
        let target = HCoord::new(0, 1);

        let start = Instant::now();
        let relay_task = tokio::spawn(async move {
            handle.relay(&mf, target).await;
        });
        let outcome = tokio::time::timeout(Duration::from_secs(2), relay_task).await;

        assert!(
            outcome.is_ok(),
            "relay must return control within its own bound instead of spinning the runtime \
             forever, still running after {:?}",
            start.elapsed()
        );
        outcome
            .unwrap()
            .expect("the relay task itself must not panic");
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "relay took {:?}, not bounded",
            start.elapsed()
        );
        let seen = calls.load(Ordering::SeqCst);
        assert_eq!(
            seen,
            Config::default().max_relay_attempts,
            "a persistently dead gateway must be tried exactly the configured bound of times"
        );

        cancel.cancel();
        manager_task.await.unwrap();
    }
}

#[cfg(test)]
mod bound_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ntk_common::{Naddr, Topology};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::actor::Manager;
    use crate::config::Config;
    use crate::participation::ParticipantSet;
    use crate::service::PeerService;
    use crate::stub::{GetRequestError, RoutingEnv, StubCallError};

    /// A gateway stub that always accepts the forward (`Ok(())`) but never calls back into the
    /// actor — every attempt therefore times out, forcing `contact_peer` to keep hopping instead
    /// of resolving quickly, so a low `Config::max_contact_peer_hops` is the only thing that can
    /// end the search.
    struct AcceptStub {
        calls: Arc<AtomicUsize>,
    }

    impl PeersStub for AcceptStub {
        fn forward_peer_message(
            &self,
            _msg: PeerMessageForwarder,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(()) })
        }
        fn get_request(
            &self,
            _msg_id: i32,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<TypedValue, GetRequestError>> {
            unreachable!("hop-bound test never reaches a real servant")
        }
        fn set_response(
            &self,
            _msg_id: i32,
            _response: TypedValue,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_refuse_message(
            &self,
            _msg_id: i32,
            _refusal: Refusal,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_redo_from_start(
            &self,
            _msg_id: i32,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_next_destination(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_failure(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_non_participant(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_missing_optional_maps(
            &self,
            _msg_id: i32,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_participant(
            &self,
            _p_id: ServiceId,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn give_participant_maps(
            &self,
            _maps: ParticipantSet,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn ask_participant_maps(
            &self,
        ) -> futures::future::BoxFuture<'_, Result<ParticipantSet, StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    struct AcceptingGatewayEnv {
        stub: Arc<AcceptStub>,
    }

    impl RoutingEnv for AcceptingGatewayEnv {
        fn gnode_exists(&self, _hc: HCoord) -> bool {
            true
        }
        fn gateway(
            &self,
            _hc: HCoord,
            _failed: Option<&Arc<dyn PeersStub>>,
        ) -> Option<Arc<dyn PeersStub>> {
            Some(self.stub.clone())
        }
        fn dial(&self, _n: &TupleNode) -> Option<Arc<dyn PeersStub>> {
            None
        }
        fn nodes_in_my_group(&self, _level: usize) -> usize {
            5
        }
        fn neighbors(&self) -> Vec<Arc<dyn PeersStub>> {
            Vec::new()
        }
    }

    struct MandatoryEcho {
        id: ServiceId,
    }

    impl PeerService for MandatoryEcho {
        fn service_id(&self) -> ServiceId {
            self.id
        }
        fn is_optional(&self) -> bool {
            false
        }
        fn exec<'a>(
            &'a self,
            request: TypedValue,
            _client_tuple: &'a [u32],
        ) -> futures::future::BoxFuture<'a, Result<TypedValue, ExecError>> {
            Box::pin(async move { Ok(request) })
        }
    }

    /// Pins the fix: a routing search that never converges (every hop times out against a
    /// gateway that accepts forwarding but whose servant never answers) must give up after
    /// [`Config::max_contact_peer_hops`], not run until [`Config::routing_timeout`] alone —
    /// proven by a hop bound (3) far below the number of distinct candidates the topology offers
    /// (49), so only the hop bound, not routing exhaustion, can explain termination.
    #[tokio::test(start_paused = true)]
    async fn contact_peer_gives_up_after_the_configured_hop_bound_not_routing_exhaustion() {
        let calls = Arc::new(AtomicUsize::new(0));
        let stub = Arc::new(AcceptStub {
            calls: calls.clone(),
        });
        let env: Arc<dyn RoutingEnv> = Arc::new(AcceptingGatewayEnv { stub });
        let topology = Topology::new([50]).unwrap();
        let my_pos = Naddr::new(topology.clone(), vec![0]).unwrap();
        let config = Config {
            max_contact_peer_hops: 3,
            ..Config::default()
        };
        let (manager, handle) =
            Manager::new(topology.clone(), my_pos, env, config, topology.levels());
        let cancel = CancellationToken::new();
        let manager_task = tokio::spawn(manager.run(cancel.child_token()));

        let p_id = ServiceId::new(1);
        handle.register(Arc::new(MandatoryEcho { id: p_id })).await;

        let target = TupleNode::new(topology.clone(), vec![25]).unwrap();
        let request = TypedValue::new("test.echo", b"hi".to_vec());
        let result = handle
            .contact_peer(
                p_id,
                Some(target),
                request,
                Duration::from_millis(50),
                None,
                Vec::new(),
            )
            .await;

        assert_eq!(
            result,
            Err(ContactPeerError::TooManyHops { max: 3 }),
            "must fail with the hop-bound error, not routing exhaustion or a wrong outcome"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "must attempt exactly the configured number of hops before giving up"
        );

        cancel.cancel();
        manager_task.await.unwrap();
    }

    /// A fake servant that always succeeds, but only after `per_hop_delay` — simulating a slow
    /// (not broken) network so `replicate`'s loop keeps making genuine progress instead of
    /// failing fast, letting the overall deadline (not routing exhaustion, not a hop bound) be
    /// the only thing that can stop it short of `q`. Plays both wire roles inline: it is hop 1's
    /// gateway (`forward_peer_message`) *and*, since this test runs single-process, it drives
    /// the reply handshake (`get_request`/`set_response`) directly against the same [`Handle`]
    /// that is waiting on it — the same two calls a real remote servant's `forward_msg` would
    /// make via a stub dialed back to the originator.
    struct SlowServantStub {
        handle: std::sync::OnceLock<Handle>,
        topology: Topology,
        calls: Arc<AtomicUsize>,
        per_hop_delay: Duration,
    }

    impl PeersStub for SlowServantStub {
        fn forward_peer_message(
            &self,
            msg: PeerMessageForwarder,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let handle = self
                .handle
                .get()
                .expect("handle set before first call")
                .clone();
            let respondant = TupleNode::new(self.topology.clone(), vec![msg.pos])
                .expect("candidate position is always in range");
            let delay = self.per_hop_delay;
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                let _ = handle.get_request(msg.msg_id, respondant.clone()).await;
                let response = TypedValue::new("test.echo", b"ok".to_vec());
                handle.set_response(msg.msg_id, response, respondant).await;
                Ok(())
            })
        }
        fn get_request(
            &self,
            _msg_id: i32,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<TypedValue, GetRequestError>> {
            unreachable!("not exercised by this test")
        }
        fn set_response(
            &self,
            _msg_id: i32,
            _response: TypedValue,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_refuse_message(
            &self,
            _msg_id: i32,
            _refusal: Refusal,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_redo_from_start(
            &self,
            _msg_id: i32,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_next_destination(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_failure(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_non_participant(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_missing_optional_maps(
            &self,
            _msg_id: i32,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_participant(
            &self,
            _p_id: ServiceId,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn give_participant_maps(
            &self,
            _maps: ParticipantSet,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn ask_participant_maps(
            &self,
        ) -> futures::future::BoxFuture<'_, Result<ParticipantSet, StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    struct SlowGatewayEnv {
        stub: Arc<SlowServantStub>,
    }

    impl RoutingEnv for SlowGatewayEnv {
        fn gnode_exists(&self, _hc: HCoord) -> bool {
            true
        }
        fn gateway(
            &self,
            _hc: HCoord,
            _failed: Option<&Arc<dyn PeersStub>>,
        ) -> Option<Arc<dyn PeersStub>> {
            Some(self.stub.clone())
        }
        fn dial(&self, _n: &TupleNode) -> Option<Arc<dyn PeersStub>> {
            None
        }
        fn nodes_in_my_group(&self, _level: usize) -> usize {
            5
        }
        fn neighbors(&self) -> Vec<Arc<dyn PeersStub>> {
            Vec::new()
        }
    }

    /// Pins the fix: `replicate` against a slow-but-healthy network (every attempt eventually
    /// succeeds, never fails or exhausts routing) must still stop once its overall wall-clock
    /// deadline elapses, returning a *partial* replica set well short of `q` — proven with `q`
    /// far larger than the deadline could ever admit attempts for, so only the deadline (not
    /// `q` itself, not routing exhaustion) can explain stopping early.
    #[tokio::test(start_paused = true)]
    async fn replicate_returns_partial_results_once_its_overall_deadline_elapses() {
        let calls = Arc::new(AtomicUsize::new(0));
        let topology = Topology::new([50]).unwrap();
        let per_hop_delay = Duration::from_millis(30);
        let stub = Arc::new(SlowServantStub {
            handle: std::sync::OnceLock::new(),
            topology: topology.clone(),
            calls: calls.clone(),
            per_hop_delay,
        });
        let env: Arc<dyn RoutingEnv> = Arc::new(SlowGatewayEnv { stub: stub.clone() });
        let my_pos = Naddr::new(topology.clone(), vec![0]).unwrap();
        let timeout_exec = Duration::from_millis(150);
        let deadline_multiplier = 4;
        let config = Config {
            replicate_deadline_multiplier: deadline_multiplier,
            ..Config::default()
        };
        let (manager, handle) =
            Manager::new(topology.clone(), my_pos, env, config, topology.levels());
        stub.handle
            .set(handle.clone())
            .expect("set once, before use");
        let cancel = CancellationToken::new();
        let manager_task = tokio::spawn(manager.run(cancel.child_token()));

        let p_id = ServiceId::new(1);
        handle.register(Arc::new(MandatoryEcho { id: p_id })).await;

        let target = TupleNode::new(topology.clone(), vec![25]).unwrap();
        let request = TypedValue::new("test.echo", b"hi".to_vec());
        // q = 45 is reachable inside a topology of 49 distinct candidates, but the deadline
        // (timeout_exec * multiplier = 600ms, at ~30ms/replica) admits only ~20 before cutoff.
        let q = 45;
        let start = Instant::now();
        let replicas = handle
            .replicate(p_id, target, request, timeout_exec, q)
            .await;
        let elapsed = start.elapsed();

        assert!(
            !replicas.is_empty(),
            "the network is healthy, not broken — at least one replica must succeed"
        );
        assert!(
            (replicas.len() as u32) < q,
            "replicate collected {} of the requested {q}; it must stop short of q once its \
             deadline elapses, not run to completion",
            replicas.len()
        );
        let deadline = timeout_exec * deadline_multiplier;
        assert!(
            elapsed <= deadline + timeout_exec,
            "replicate ran for {elapsed:?}, past its {deadline:?} deadline plus one attempt's \
             worth of overshoot slack"
        );

        cancel.cancel();
        manager_task.await.unwrap();
    }
}

#[cfg(test)]
mod origin_auth_tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ed25519_dalek::SigningKey;
    use ntk_common::{Naddr, Topology};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::actor::Manager;
    use crate::config::Config;
    use crate::participation::ParticipantSet;
    use crate::service::PeerService;
    use crate::stub::{GetRequestError, RoutingEnv, StubCallError};

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn tv(payload: &[u8]) -> TypedValue {
        TypedValue::new("origin-auth-test", payload.to_vec())
    }

    /// What the servant sent back to whichever node `mf.n` claims — a `set_response` means
    /// `exec_local` actually ran; a `set_refuse_message` means origin-auth rejected the request
    /// before `exec_local` ever saw it.
    #[derive(Default)]
    struct Outcome {
        responses: Mutex<Vec<TypedValue>>,
        refusals: Mutex<Vec<Refusal>>,
    }

    /// A minimal "originator" stub: always answers `get_request` with a fixed `request` and
    /// records every `set_response`/`set_refuse_message` it receives. Every other method is
    /// unreachable — `forward_msg`'s self-loop branch (the only branch every test here
    /// exercises) never calls them.
    struct FakeOriginator {
        request: TypedValue,
        outcome: Arc<Outcome>,
    }

    impl PeersStub for FakeOriginator {
        fn forward_peer_message(
            &self,
            _msg: PeerMessageForwarder,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised: this stub only plays the originator role")
        }
        fn get_request(
            &self,
            _msg_id: i32,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<TypedValue, GetRequestError>> {
            let request = self.request.clone();
            Box::pin(async move { Ok(request) })
        }
        fn set_response(
            &self,
            _msg_id: i32,
            response: TypedValue,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            self.outcome
                .responses
                .lock()
                .expect("mutex poisoned")
                .push(response);
            Box::pin(async { Ok(()) })
        }
        fn set_refuse_message(
            &self,
            _msg_id: i32,
            refusal: Refusal,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            self.outcome
                .refusals
                .lock()
                .expect("mutex poisoned")
                .push(refusal);
            Box::pin(async { Ok(()) })
        }
        fn set_redo_from_start(
            &self,
            _msg_id: i32,
            _respondant: TupleNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_next_destination(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_failure(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_non_participant(
            &self,
            _msg_id: i32,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_missing_optional_maps(
            &self,
            _msg_id: i32,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn set_participant(
            &self,
            _p_id: ServiceId,
            _tuple: TupleGNode,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn give_participant_maps(
            &self,
            _maps: ParticipantSet,
        ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn ask_participant_maps(
            &self,
        ) -> futures::future::BoxFuture<'_, Result<ParticipantSet, StubCallError>> {
            unreachable!("not exercised by this test")
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    /// Dials straight to the one `FakeOriginator` regardless of `n`, and never expects to
    /// relay — every test here directly targets the servant's own `forward_msg` self-loop.
    struct DialsFakeOriginator {
        stub: Arc<FakeOriginator>,
    }

    impl RoutingEnv for DialsFakeOriginator {
        fn gnode_exists(&self, _hc: HCoord) -> bool {
            true
        }
        fn gateway(
            &self,
            _hc: HCoord,
            _failed: Option<&Arc<dyn PeersStub>>,
        ) -> Option<Arc<dyn PeersStub>> {
            unreachable!("not exercised: this test never relays past the servant")
        }
        fn dial(&self, _n: &TupleNode) -> Option<Arc<dyn PeersStub>> {
            Some(self.stub.clone())
        }
        fn nodes_in_my_group(&self, _level: usize) -> usize {
            1
        }
        fn neighbors(&self) -> Vec<Arc<dyn PeersStub>> {
            Vec::new()
        }
    }

    /// Never dialed or gatewayed through — used only for a throwaway `Handle` built purely to
    /// reach [`Handle::sign_origin`], which touches no actor state and no `RoutingEnv` at all.
    struct UnusedEnv;

    impl RoutingEnv for UnusedEnv {
        fn gnode_exists(&self, _hc: HCoord) -> bool {
            true
        }
        fn gateway(
            &self,
            _hc: HCoord,
            _failed: Option<&Arc<dyn PeersStub>>,
        ) -> Option<Arc<dyn PeersStub>> {
            unreachable!("never exercised: this handle only ever signs")
        }
        fn dial(&self, _n: &TupleNode) -> Option<Arc<dyn PeersStub>> {
            unreachable!("never exercised: this handle only ever signs")
        }
        fn nodes_in_my_group(&self, _level: usize) -> usize {
            1
        }
        fn neighbors(&self) -> Vec<Arc<dyn PeersStub>> {
            Vec::new()
        }
    }

    /// Always echoes `request` back, and counts how many times it actually ran — the direct
    /// proxy for "did origin-auth let `exec_local` execute this request".
    struct EchoService {
        id: ServiceId,
        calls: Arc<AtomicUsize>,
    }

    impl PeerService for EchoService {
        fn service_id(&self) -> ServiceId {
            self.id
        }
        fn is_optional(&self) -> bool {
            false
        }
        fn exec<'a>(
            &'a self,
            request: TypedValue,
            _client_tuple: &'a [u32],
        ) -> futures::future::BoxFuture<'a, Result<TypedValue, ExecError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(request) })
        }
    }

    /// Boots a lone servant `Handle` at `[0, 0]` with `require_auth` as given, registers
    /// `service`, and returns it alongside the `Manager`'s cancellation token.
    async fn servant(
        topology: &Topology,
        require_auth: bool,
        service: Arc<dyn PeerService>,
        env: Arc<dyn RoutingEnv>,
    ) -> (Handle, CancellationToken) {
        let my_pos = Naddr::new(topology.clone(), vec![0, 0]).unwrap();
        let config = Config {
            require_auth,
            ..Config::default()
        };
        let (manager, handle) =
            Manager::new(topology.clone(), my_pos, env, config, topology.levels());
        let cancel = CancellationToken::new();
        tokio::spawn(manager.run(cancel.child_token()));
        handle.register_cmd(service).await;
        (handle, cancel)
    }

    /// A throwaway `Handle`, never run as an actor, that only ever calls
    /// [`Handle::sign_origin`] — the exact API surface the true originator's own node would
    /// call before ever sending anything over the wire.
    fn signer(topology: &Topology, seed: u8) -> Handle {
        let my_pos = Naddr::new(topology.clone(), vec![1, 1]).unwrap();
        let (_manager, handle) = Manager::new(
            topology.clone(),
            my_pos,
            Arc::new(UnusedEnv),
            Config::default(),
            topology.levels(),
        );
        handle.with_signing_key(key(seed))
    }

    fn mf_for(
        topology: &Topology,
        client_tuple: &[u32],
        p_id: ServiceId,
        auth: Option<ntk_proto::v1::Auth>,
    ) -> PeerMessageForwarder {
        PeerMessageForwarder {
            inside_level: topology.levels(),
            n: TupleNode::new(topology.clone(), client_tuple.to_vec()).unwrap(),
            x_macron: None,
            lvl: 0,
            pos: 0,
            p_id,
            msg_id: 0,
            exclude_tuple_list: Vec::new(),
            non_participant_tuple_list: Vec::new(),
            auth,
        }
    }

    #[tokio::test]
    async fn a_valid_origin_signature_is_accepted() {
        let topology = Topology::new([2, 2]).unwrap();
        let p_id = ServiceId::new(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let service = Arc::new(EchoService {
            id: p_id,
            calls: calls.clone(),
        });
        let outcome = Arc::new(Outcome::default());
        let request = tv(b"payload");
        let stub = Arc::new(FakeOriginator {
            request: request.clone(),
            outcome: outcome.clone(),
        });
        let env: Arc<dyn RoutingEnv> = Arc::new(DialsFakeOriginator { stub });
        let (servant_handle, cancel) = servant(&topology, true, service, env).await;

        let client_tuple = [1u32, 1u32];
        let auth = signer(&topology, 1).sign_origin(&client_tuple, p_id, &request);
        let mf = mf_for(&topology, &client_tuple, p_id, auth);

        servant_handle.forward_msg(mf).await;

        assert_eq!(
            outcome.responses.lock().expect("mutex poisoned").as_slice(),
            [request],
            "a valid, untampered origin signature must let exec_local run and answer"
        );
        assert!(outcome.refusals.lock().expect("mutex poisoned").is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        cancel.cancel();
    }

    /// The actual audit finding: `PeerMessageForwarder::n` (`client_tuple`) travels through
    /// relays unauthenticated, so a malicious relay could rewrite it in transit. Simulated here
    /// by delivering to the servant a forwarder whose `n` differs from what the true originator
    /// actually signed — indistinguishable, from the servant's point of view, from a relay
    /// having rewritten it en route. Origin-auth must catch this: the signature was computed
    /// over the true `client_tuple`, so verifying it against the tampered one fails.
    #[tokio::test]
    async fn a_relay_that_rewrites_client_tuple_is_rejected() {
        let topology = Topology::new([2, 2]).unwrap();
        let p_id = ServiceId::new(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let service = Arc::new(EchoService {
            id: p_id,
            calls: calls.clone(),
        });
        let outcome = Arc::new(Outcome::default());
        let request = tv(b"payload");
        let stub = Arc::new(FakeOriginator {
            request: request.clone(),
            outcome: outcome.clone(),
        });
        let env: Arc<dyn RoutingEnv> = Arc::new(DialsFakeOriginator { stub });
        let (servant_handle, cancel) = servant(&topology, true, service, env).await;

        let true_client_tuple = [1u32, 1u32];
        let auth = signer(&topology, 1).sign_origin(&true_client_tuple, p_id, &request);
        // The relay's rewrite: the forwarder that actually reaches the servant claims a
        // *different* origin than the one the signature covers.
        let tampered_client_tuple = [0u32, 1u32];
        let mf = mf_for(&topology, &tampered_client_tuple, p_id, auth);

        servant_handle.forward_msg(mf).await;

        assert!(
            outcome.responses.lock().expect("mutex poisoned").is_empty(),
            "exec_local must never run against a rewritten client_tuple"
        );
        assert_eq!(outcome.refusals.lock().expect("mutex poisoned").len(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        cancel.cancel();
    }

    /// A signature is bound to one exact `(client_tuple, p_id, request)` triple — it must not
    /// verify once the request payload is swapped out from under it (a relay can't see the
    /// payload at all in the real protocol, but a colluding/compromised answer to
    /// `get_request` could still try to substitute one).
    #[tokio::test]
    async fn a_signature_transplanted_onto_a_different_request_is_rejected() {
        let topology = Topology::new([2, 2]).unwrap();
        let p_id = ServiceId::new(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let service = Arc::new(EchoService {
            id: p_id,
            calls: calls.clone(),
        });
        let outcome = Arc::new(Outcome::default());
        let signed_request = tv(b"the request the originator actually signed");
        let substituted_request = tv(b"a different request smuggled in at get_request time");
        let stub = Arc::new(FakeOriginator {
            request: substituted_request,
            outcome: outcome.clone(),
        });
        let env: Arc<dyn RoutingEnv> = Arc::new(DialsFakeOriginator { stub });
        let (servant_handle, cancel) = servant(&topology, true, service, env).await;

        let client_tuple = [1u32, 1u32];
        let auth = signer(&topology, 1).sign_origin(&client_tuple, p_id, &signed_request);
        let mf = mf_for(&topology, &client_tuple, p_id, auth);

        servant_handle.forward_msg(mf).await;

        assert!(outcome.responses.lock().expect("mutex poisoned").is_empty());
        assert_eq!(outcome.refusals.lock().expect("mutex poisoned").len(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        cancel.cancel();
    }

    /// A signature is equally bound to the target service — transplanting a signature that was
    /// produced for `p_id` onto a request actually routed for a *different* `p_id` must fail,
    /// even though a service is genuinely registered at that other id too (proving rejection
    /// comes from verification, not merely "no such service").
    #[tokio::test]
    async fn a_signature_transplanted_onto_a_different_service_is_rejected() {
        let topology = Topology::new([2, 2]).unwrap();
        let signed_for = ServiceId::new(1);
        let actually_targeted = ServiceId::new(2);
        let calls = Arc::new(AtomicUsize::new(0));
        let request = tv(b"payload");
        let outcome = Arc::new(Outcome::default());
        let stub = Arc::new(FakeOriginator {
            request: request.clone(),
            outcome: outcome.clone(),
        });
        let env: Arc<dyn RoutingEnv> = Arc::new(DialsFakeOriginator { stub });
        let my_pos = Naddr::new(topology.clone(), vec![0, 0]).unwrap();
        let config = Config {
            require_auth: true,
            ..Config::default()
        };
        let (manager, servant_handle) =
            Manager::new(topology.clone(), my_pos, env, config, topology.levels());
        let cancel = CancellationToken::new();
        tokio::spawn(manager.run(cancel.child_token()));
        servant_handle
            .register_cmd(Arc::new(EchoService {
                id: signed_for,
                calls: calls.clone(),
            }))
            .await;
        servant_handle
            .register_cmd(Arc::new(EchoService {
                id: actually_targeted,
                calls: calls.clone(),
            }))
            .await;

        let client_tuple = [1u32, 1u32];
        let auth = signer(&topology, 1).sign_origin(&client_tuple, signed_for, &request);
        let mf = mf_for(&topology, &client_tuple, actually_targeted, auth);

        servant_handle.forward_msg(mf).await;

        assert!(outcome.responses.lock().expect("mutex poisoned").is_empty());
        assert_eq!(outcome.refusals.lock().expect("mutex poisoned").len(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        cancel.cancel();
    }

    /// [`ntk_proto::auth::SequenceGuard`]'s replay policy, exercised through the servant's own
    /// `verify_origin`: the same, otherwise perfectly valid, `Auth` must be accepted the first
    /// time and rejected every time after.
    #[tokio::test]
    async fn a_replayed_sequence_is_rejected() {
        let topology = Topology::new([2, 2]).unwrap();
        let p_id = ServiceId::new(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let service = Arc::new(EchoService {
            id: p_id,
            calls: calls.clone(),
        });
        let outcome = Arc::new(Outcome::default());
        let request = tv(b"payload");
        let stub = Arc::new(FakeOriginator {
            request: request.clone(),
            outcome: outcome.clone(),
        });
        let env: Arc<dyn RoutingEnv> = Arc::new(DialsFakeOriginator { stub });
        let (servant_handle, cancel) = servant(&topology, true, service, env).await;

        let client_tuple = [1u32, 1u32];
        let origin = signer(&topology, 1);
        let auth = origin.sign_origin(&client_tuple, p_id, &request);

        servant_handle
            .forward_msg(mf_for(&topology, &client_tuple, p_id, auth.clone()))
            .await;
        servant_handle
            .forward_msg(mf_for(&topology, &client_tuple, p_id, auth))
            .await;

        assert_eq!(
            outcome.responses.lock().expect("mutex poisoned").len(),
            1,
            "the first use of a fresh sequence must succeed"
        );
        assert_eq!(
            outcome.refusals.lock().expect("mutex poisoned").len(),
            1,
            "replaying the exact same sequence a second time must be rejected"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        cancel.cancel();
    }

    /// Pins the off switch: with `require_auth` at its default (`false`), a request carrying no
    /// `Auth` at all — the only shape `PeerMessageForwarder` could ever take before this
    /// feature existed — is accepted exactly as before, indistinguishable from a node that has
    /// never heard of origin-auth.
    #[tokio::test]
    async fn with_require_auth_off_an_unsigned_request_behaves_exactly_as_before() {
        let topology = Topology::new([2, 2]).unwrap();
        let p_id = ServiceId::new(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let service = Arc::new(EchoService {
            id: p_id,
            calls: calls.clone(),
        });
        let outcome = Arc::new(Outcome::default());
        let request = tv(b"payload");
        let stub = Arc::new(FakeOriginator {
            request: request.clone(),
            outcome: outcome.clone(),
        });
        let env: Arc<dyn RoutingEnv> = Arc::new(DialsFakeOriginator { stub });
        let (servant_handle, cancel) = servant(&topology, false, service, env).await;

        let mf = mf_for(&topology, &[1, 1], p_id, None);
        servant_handle.forward_msg(mf).await;

        assert_eq!(
            outcome.responses.lock().expect("mutex poisoned").as_slice(),
            [request]
        );
        assert!(outcome.refusals.lock().expect("mutex poisoned").is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        cancel.cancel();
    }

    /// The other half of the off switch: `require_auth = false` must not merely tolerate a
    /// *missing* `Auth`, it must never even look at one that *is* present — an old, unsigned
    /// deployment migrating field-by-field must never start silently rejecting traffic from a
    /// node that (for unrelated reasons) sent a garbage/invalid `Auth` block.
    #[tokio::test]
    async fn with_require_auth_off_an_invalid_auth_block_is_never_checked() {
        let topology = Topology::new([2, 2]).unwrap();
        let p_id = ServiceId::new(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let service = Arc::new(EchoService {
            id: p_id,
            calls: calls.clone(),
        });
        let outcome = Arc::new(Outcome::default());
        let request = tv(b"payload");
        let stub = Arc::new(FakeOriginator {
            request: request.clone(),
            outcome: outcome.clone(),
        });
        let env: Arc<dyn RoutingEnv> = Arc::new(DialsFakeOriginator { stub });
        let (servant_handle, cancel) = servant(&topology, false, service, env).await;

        let garbage_auth = ntk_proto::v1::Auth {
            signer_key: vec![0u8; 3],
            sequence: 0,
            signature: vec![0u8; 3],
        };
        let mf = mf_for(&topology, &[1, 1], p_id, Some(garbage_auth));
        servant_handle.forward_msg(mf).await;

        assert_eq!(
            outcome.responses.lock().expect("mutex poisoned").as_slice(),
            [request]
        );
        assert!(outcome.refusals.lock().expect("mutex poisoned").is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        cancel.cancel();
    }

    /// Pins the originator-side off switch: without [`Handle::with_signing_key`], `contact_peer`
    /// never populates `PeerMessageForwarder::auth` — the wire shape stays byte-for-byte what
    /// it was before this feature existed.
    #[test]
    fn sign_origin_returns_none_without_a_configured_key() {
        let topology = Topology::new([2, 2]).unwrap();
        let my_pos = Naddr::new(topology.clone(), vec![0, 0]).unwrap();
        let (_manager, handle) = Manager::new(
            topology.clone(),
            my_pos,
            Arc::new(UnusedEnv),
            Config::default(),
            topology.levels(),
        );
        let auth = handle.sign_origin(&[0, 0], ServiceId::new(1), &tv(b"payload"));
        assert!(auth.is_none());
    }
}
