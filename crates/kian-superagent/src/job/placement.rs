//! Placement — choosing a worker for one step attempt, once, as a pure
//! function of the declaration and the worker catalog
//! (`design/JOB-MODEL.md` §9).
//!
//! Three rules are load-bearing:
//!
//! - a *capability is eligibility, not authority*: a worker that declares
//!   `fs.home.read` is not thereby granted it — its own launch
//!   configuration decides that (WORKER-MODEL §9, J15). Placement only
//!   refuses candidates that do not declare what the step needs;
//! - the choice is *committed before the child exists* and is never
//!   recomputed for the attempt (J3): a re-driven drain reuses the
//!   dispatch record instead of re-selecting;
//! - an empty admissible set is a *refusal*, never a wait (§9.4): the step
//!   fails `no_candidate` naming which exclusion emptied the set, so "no
//!   available worker" is answered in one tick, not in one timeout.

use crate::directory::{MemberState, MemberView};
use crate::job::plan::{JobLimits, StepPlan};

/// A candidate, ranked §9.3's way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// The worker id.
    pub id: String,
    /// Its last declared state.
    pub state: MemberState,
    /// Its index in the step's (or job's) `prefer` list; `usize::MAX` when
    /// unpreferred.
    pub declared_rank: usize,
}

impl Candidate {
    /// §9.3's sort key: reachable first, then preference, then id — a
    /// total order, so two conductors place a step identically.
    #[must_use]
    pub fn sort_key(&self) -> (u8, usize, &str) {
        (state_rank(self.state), self.declared_rank, self.id.as_str())
    }
}

/// Why a worker ranks where it does (§9.3).
#[must_use]
pub fn state_rank(state: MemberState) -> u8 {
    match state {
        MemberState::Online => 0,
        MemberState::Draining => 1,
        MemberState::Offline => 2,
    }
}

/// The placement of one attempt, as it is committed to the run's stream
/// (the `job` key of a `thread/started` record, §8.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The chosen worker.
    pub worker: String,
    /// Every admissible worker and its rank, in sort order — the audit
    /// trail that makes a surprising placement explainable.
    pub candidates: Vec<Candidate>,
}

impl Placement {
    /// The rationale string recorded beside the candidate list.
    #[must_use]
    pub fn rationale(&self) -> String {
        let names: Vec<&str> = self.candidates.iter().map(|c| c.id.as_str()).collect();
        format!("chose {} among [{}]", self.worker, names.join(", "))
    }
}

/// Why no worker was chosen (§9.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementRefusal {
    /// `pin` names a worker the catalog does not know.
    PinUnknown {
        /// The pinned id.
        pin: String,
    },
    /// `pin` names a worker that cannot run the step.
    PinIneligible {
        /// The pinned id.
        pin: String,
        /// The capabilities it does not declare.
        missing: Vec<String>,
    },
    /// No worker declares what the step needs.
    NoCapable {
        /// The required capabilities.
        requires: Vec<String>,
    },
    /// Every capable worker is offline and the step does not allow that.
    AllOffline {
        /// The capable but unreachable workers.
        workers: Vec<String>,
    },
    /// Every capable worker already failed this step in this run.
    AllTried {
        /// The workers already exhausted.
        workers: Vec<String>,
    },
    /// Every admissible worker is bound to a live child; the conductor
    /// waits (`defer_budget_ms`) instead of failing (§9.4).
    AllBusy {
        /// The busy workers.
        workers: Vec<String>,
    },
}

impl PlacementRefusal {
    /// The reason code an operator surface shows (§15.3) and the verdict
    /// detail names (§9.4).
    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::PinUnknown { .. } => "pin_unknown",
            Self::PinIneligible { .. } => "pin_ineligible",
            Self::NoCapable { .. } => "no_capable_worker",
            Self::AllOffline { .. } => "all_offline",
            Self::AllTried { .. } => "all_tried",
            Self::AllBusy { .. } => "all_busy",
        }
    }

    /// Whether the refusal is worth waiting for. Only `AllBusy` is
    /// transient; everything else is final for this attempt (§9.4).
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::AllBusy { .. })
    }
}

/// The facts placement is allowed to read (§9.5).
#[derive(Debug, Clone, Copy, Default)]
pub struct PlacementFacts<'a> {
    /// The worker catalog: declared capabilities and last state.
    pub members: &'a [MemberView],
    /// Workers currently bound to a live child — the read-only binding
    /// view, bounded-stale by construction.
    pub busy: &'a [String],
    /// Workers whose earlier attempt of *this* step did not succeed
    /// (§9.2 repeat-exclusion).
    pub tried: &'a [String],
}

/// Choose a worker for one attempt of `step` (§9.1-§9.3).
///
/// `Ok(Placement)` commits the choice; `Err(PlacementRefusal)` records why
/// nothing could be chosen. Neither result creates anything: the caller
/// writes the dispatch record first and admits the child second (§8.3).
pub fn select(
    step: &StepPlan,
    limits: &JobLimits,
    facts: &PlacementFacts<'_>,
) -> Result<Placement, PlacementRefusal> {
    let prefer = preferred(step, limits);

    let eligible: Vec<&MemberView> = match &step.pin {
        Some(pin) => {
            let Some(member) = facts.members.iter().find(|member| &member.id == pin) else {
                return Err(PlacementRefusal::PinUnknown { pin: pin.clone() });
            };
            let missing = missing_capabilities(step, member);
            if !missing.is_empty() {
                return Err(PlacementRefusal::PinIneligible {
                    pin: pin.clone(),
                    missing,
                });
            }
            vec![member]
        }
        None => {
            let capable: Vec<&MemberView> = facts
                .members
                .iter()
                .filter(|member| missing_capabilities(step, member).is_empty())
                .collect();
            if capable.is_empty() {
                return Err(PlacementRefusal::NoCapable {
                    requires: step.requires.clone(),
                });
            }
            capable
        }
    };

    let reachable: Vec<&MemberView> = if step.allow_offline {
        eligible
    } else {
        let reachable: Vec<&MemberView> = eligible
            .iter()
            .copied()
            .filter(|member| !matches!(member.state, MemberState::Offline))
            .collect();
        if reachable.is_empty() {
            return Err(PlacementRefusal::AllOffline {
                workers: eligible.iter().map(|member| member.id.clone()).collect(),
            });
        }
        reachable
    };

    let untried: Vec<&MemberView> = reachable
        .iter()
        .copied()
        .filter(|member| !facts.tried.iter().any(|tried| tried == &member.id))
        .collect();
    if untried.is_empty() {
        return Err(PlacementRefusal::AllTried {
            workers: reachable.iter().map(|member| member.id.clone()).collect(),
        });
    }

    let free: Vec<&MemberView> = untried
        .iter()
        .copied()
        .filter(|member| !facts.busy.iter().any(|busy| busy == &member.id))
        .collect();
    if free.is_empty() {
        return Err(PlacementRefusal::AllBusy {
            workers: untried.iter().map(|member| member.id.clone()).collect(),
        });
    }

    let mut candidates: Vec<Candidate> = free
        .iter()
        .map(|member| Candidate {
            id: member.id.clone(),
            state: member.state,
            declared_rank: prefer
                .iter()
                .position(|worker| worker == &member.id)
                .unwrap_or(usize::MAX),
        })
        .collect();
    candidates.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));

    let Some(chosen) = candidates.first() else {
        return Err(PlacementRefusal::NoCapable {
            requires: step.requires.clone(),
        });
    };

    Ok(Placement {
        worker: chosen.id.clone(),
        candidates,
    })
}

fn preferred<'a>(step: &'a StepPlan, limits: &'a JobLimits) -> &'a [String] {
    if step.prefer.is_empty() {
        &limits.prefer
    } else {
        &step.prefer
    }
}

fn missing_capabilities(step: &StepPlan, member: &MemberView) -> Vec<String> {
    step.requires
        .iter()
        .filter(|required| {
            !member
                .capabilities
                .iter()
                .any(|declared| declared == *required)
        })
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// Tests (L1): capability filtering, ranking, refusals
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::plan::JobPlan;
    use serde_json::json;

    fn member(id: &str, state: MemberState, capabilities: &[&str]) -> MemberView {
        MemberView {
            id: id.to_string(),
            description: None,
            version: Some(3),
            capabilities: capabilities.iter().map(|c| (*c).to_string()).collect(),
            declaration_stale: false,
            state,
            last_seen_ms: 1,
        }
    }

    fn step(value: serde_json::Value) -> StepPlan {
        match serde_json::from_value(value) {
            Ok(step) => step,
            Err(error) => panic!("valid step: {error}"),
        }
    }

    fn plain(requires: &[&str]) -> StepPlan {
        step(json!({"id": "fetch", "action": "agent", "prompt": "a",
                    "requires": requires,
                    "verify": [{"check": "final_message_present"}]}))
    }

    fn limits(prefer: &[&str]) -> JobLimits {
        JobLimits {
            prefer: prefer.iter().map(|w| (*w).to_string()).collect(),
            ..Default::default()
        }
    }

    fn catalog() -> Vec<MemberView> {
        vec![
            member("vultr", MemberState::Online, &["net.unrestricted"]),
            member(
                "mac",
                MemberState::Online,
                &["net.unrestricted", "fs.home.read"],
            ),
            member("nas", MemberState::Offline, &["net.unrestricted"]),
        ]
    }

    #[test]
    fn a_step_without_requirements_can_run_anywhere() {
        let members = catalog();
        let facts = PlacementFacts {
            members: &members,
            ..PlacementFacts::default()
        };
        let placement = select(&plain(&[]), &limits(&[]), &facts).expect("placed");
        assert_eq!(placement.worker, "mac", "id order breaks the tie");
        assert_eq!(
            placement.candidates.len(),
            2,
            "offline is excluded by default"
        );
    }

    #[test]
    fn requirements_filter_the_catalog() {
        let members = catalog();
        let facts = PlacementFacts {
            members: &members,
            ..PlacementFacts::default()
        };
        let placement = select(&plain(&["fs.home.read"]), &limits(&[]), &facts).expect("placed");
        assert_eq!(placement.worker, "mac");

        let impossible = plain(&["imessage.send"]);
        assert_eq!(
            select(&impossible, &limits(&[]), &facts),
            Err(PlacementRefusal::NoCapable {
                requires: vec!["imessage.send".into()]
            })
        );
    }

    #[test]
    fn a_capability_is_only_an_eligibility_filter() {
        let members = vec![member("liar", MemberState::Online, &["imessage.send"])];
        let facts = PlacementFacts {
            members: &members,
            ..PlacementFacts::default()
        };
        let send = plain(&["imessage.send"]);
        let placement =
            select(&send, &limits(&[]), &facts).expect("placement trusts the declaration");
        assert_eq!(placement.worker, "liar");
    }

    #[test]
    fn pin_narrows_to_one_worker_and_is_checked() {
        let members = catalog();
        let facts = PlacementFacts {
            members: &members,
            ..PlacementFacts::default()
        };

        let pinned = step(
            json!({"id": "fetch", "action": "agent", "prompt": "a", "pin": "vultr",
                                 "verify": [{"check": "final_message_present"}]}),
        );
        assert_eq!(
            select(&pinned, &limits(&["mac"]), &facts)
                .expect("placed")
                .worker,
            "vultr"
        );

        let unknown = step(
            json!({"id": "fetch", "action": "agent", "prompt": "a", "pin": "gone",
                                  "verify": [{"check": "final_message_present"}]}),
        );
        assert_eq!(
            select(&unknown, &limits(&[]), &facts),
            Err(PlacementRefusal::PinUnknown { pin: "gone".into() })
        );

        let ineligible = step(
            json!({"id": "fetch", "action": "agent", "prompt": "a", "pin": "vultr",
                                     "requires": ["fs.home.read"],
                                     "verify": [{"check": "final_message_present"}]}),
        );
        assert_eq!(
            select(&ineligible, &limits(&[]), &facts),
            Err(PlacementRefusal::PinIneligible {
                pin: "vultr".into(),
                missing: vec!["fs.home.read".into()],
            })
        );
    }

    #[test]
    fn preference_reorders_without_excluding() {
        let members = catalog();
        let facts = PlacementFacts {
            members: &members,
            ..PlacementFacts::default()
        };
        let placement =
            select(&plain(&["net.unrestricted"]), &limits(&["vultr"]), &facts).expect("placed");
        assert_eq!(placement.worker, "vultr");
        assert_eq!(placement.candidates[1].id, "mac");
        assert_eq!(placement.candidates[1].declared_rank, usize::MAX);

        let step_prefer = step(json!({"id": "fetch", "action": "agent", "prompt": "a",
                                      "prefer": ["mac"],
                                      "verify": [{"check": "final_message_present"}]}));
        let placement = select(&step_prefer, &limits(&["vultr"]), &facts).expect("placed");
        assert_eq!(placement.worker, "mac", "a step's own preference wins");
    }

    #[test]
    fn state_rank_orders_reachable_workers_first() {
        let members = vec![
            member("draining", MemberState::Draining, &["net.unrestricted"]),
            member("online", MemberState::Online, &["net.unrestricted"]),
        ];
        let facts = PlacementFacts {
            members: &members,
            ..PlacementFacts::default()
        };
        let placement =
            select(&plain(&["net.unrestricted"]), &limits(&[]), &facts).expect("placed");
        assert_eq!(placement.worker, "online");
        assert_eq!(placement.candidates[1].id, "draining");
    }

    #[test]
    fn an_offline_worker_needs_an_explicit_allowance_and_ranks_last() {
        let members = catalog();
        let facts = PlacementFacts {
            members: &members,
            ..PlacementFacts::default()
        };
        let only_offline = step(json!({"id": "fetch", "action": "agent", "prompt": "a",
                                       "requires": ["net.unrestricted"], "pin": "nas",
                                       "verify": [{"check": "final_message_present"}]}));
        assert_eq!(
            select(&only_offline, &limits(&[]), &facts),
            Err(PlacementRefusal::AllOffline {
                workers: vec!["nas".into()]
            })
        );

        let permissive = step(json!({"id": "fetch", "action": "agent", "prompt": "a",
                                     "pin": "nas", "allow_offline": true,
                                     "verify": [{"check": "final_message_present"}]}));
        assert_eq!(
            select(&permissive, &limits(&[]), &facts)
                .expect("placed")
                .worker,
            "nas"
        );
    }

    #[test]
    fn a_worker_that_already_failed_this_step_is_not_retried() {
        let members = catalog();
        let tried = vec!["vultr".to_string()];
        let facts = PlacementFacts {
            members: &members,
            tried: &tried,
            ..PlacementFacts::default()
        };
        let placement =
            select(&plain(&["net.unrestricted"]), &limits(&["vultr"]), &facts).expect("placed");
        assert_eq!(placement.worker, "mac", "repeat-exclusion beats preference");

        let all_tried = vec!["vultr".to_string(), "mac".to_string(), "nas".to_string()];
        let facts = PlacementFacts {
            members: &members,
            tried: &all_tried,
            ..PlacementFacts::default()
        };
        assert_eq!(
            select(&plain(&["net.unrestricted"]), &limits(&[]), &facts),
            Err(PlacementRefusal::AllTried {
                workers: vec!["vultr".into(), "mac".into()]
            })
        );
    }

    #[test]
    fn a_busy_catalog_defers_instead_of_failing() {
        let members = catalog();
        let busy = vec!["vultr".to_string(), "mac".to_string()];
        let facts = PlacementFacts {
            members: &members,
            busy: &busy,
            ..PlacementFacts::default()
        };
        let refusal =
            select(&plain(&["net.unrestricted"]), &limits(&[]), &facts).expect_err("busy");
        assert_eq!(
            refusal,
            PlacementRefusal::AllBusy {
                workers: vec!["vultr".into(), "mac".into()]
            }
        );
        assert!(refusal.is_transient());
        assert_eq!(refusal.reason(), "all_busy");
    }

    #[test]
    fn the_rationale_names_the_choice_and_the_candidates() {
        let members = catalog();
        let facts = PlacementFacts {
            members: &members,
            ..PlacementFacts::default()
        };
        let placement =
            select(&plain(&["net.unrestricted"]), &limits(&["mac"]), &facts).expect("placed");
        assert_eq!(placement.rationale(), "chose mac among [mac, vultr]");
    }

    #[test]
    fn placement_only_reads_the_plan_it_is_given() {
        let members = catalog();
        let facts = PlacementFacts {
            members: &members,
            ..PlacementFacts::default()
        };
        let plan = JobPlan {
            id: "digest".into(),
            limits: limits(&["nas"]),
            steps: vec![plain(&["net.unrestricted"])],
        };
        let placement = select(&plan.steps[0], &plan.limits, &facts).expect("placed");
        assert_eq!(
            placement.worker, "mac",
            "an offline preference never wins, and ranks nothing"
        );
    }
}
