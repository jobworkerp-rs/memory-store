//! Pure ThreadGroup domain values and deterministic selection rules.

use super::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupStatus {
    Active,
    Redirected,
    Split,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MembershipRole {
    Root,
    Member,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MembershipState {
    Active,
    Deleted,
    Redirected,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MembershipProvenance {
    Reconciler,
    Operator,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Membership {
    pub thread_canonical_key: String,
    pub role: MembershipRole,
    pub state: MembershipState,
    pub provenance: MembershipProvenance,
    pub deleted_at: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Group {
    pub canonical_key: String,
    pub status: GroupStatus,
    pub memberships: Vec<Membership>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SplitOutcome {
    pub source: Group,
    pub successors: Vec<Group>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupError {
    SourceNotActive,
    TooFewPartitions,
    EmptyPartition,
    DuplicateMember(String),
    MembershipMismatch,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RelationType {
    Delegated,
    Fork,
    Continuation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceConfidence {
    Exact,
    Strong,
    Heuristic,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CandidateEvidence {
    Source { confidence: SourceConfidence },
    OperatorConfirmation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub parent_key: String,
    pub relation_type: RelationType,
    pub evidence: CandidateEvidence,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CandidateSelection {
    Selected(Candidate),
    Conflict(Vec<Candidate>),
    NoSelection,
}

pub fn select_parent_candidate(candidates: &[Candidate]) -> CandidateSelection {
    let mut best_by_parent = BTreeMap::<(&str, &RelationType), &Candidate>::new();
    for candidate in candidates {
        let Some(rank) = candidate_rank(candidate) else {
            continue;
        };
        let key = (candidate.parent_key.as_str(), &candidate.relation_type);
        match best_by_parent.get(&key) {
            Some(existing) if candidate_rank(existing).is_some_and(|current| current >= rank) => {}
            _ => {
                best_by_parent.insert(key, candidate);
            }
        }
    }

    let highest = best_by_parent
        .values()
        .filter_map(|candidate| candidate_rank(candidate))
        .max();
    let Some(highest) = highest else {
        return CandidateSelection::NoSelection;
    };

    let winners = best_by_parent
        .into_values()
        .filter(|candidate| candidate_rank(candidate) == Some(highest))
        .cloned()
        .collect::<Vec<_>>();
    if winners.len() == 1 {
        CandidateSelection::Selected(winners.into_iter().next().expect("one winner"))
    } else {
        CandidateSelection::Conflict(winners)
    }
}

pub fn would_create_cycle(
    active_edges: &[(String, String)],
    parent_key: &str,
    child_key: &str,
) -> bool {
    if parent_key == child_key {
        return true;
    }

    let mut children_by_parent = BTreeMap::<&str, Vec<&str>>::new();
    for (parent, child) in active_edges {
        children_by_parent
            .entry(parent.as_str())
            .or_default()
            .push(child.as_str());
    }

    let mut pending = vec![child_key];
    let mut visited = HashSet::new();
    while let Some(current) = pending.pop() {
        if !visited.insert(current) {
            continue;
        }
        if current == parent_key {
            return true;
        }
        if let Some(children) = children_by_parent.get(current) {
            pending.extend(children);
        }
    }
    false
}

pub fn split_group(group: &Group, partitions: &[Vec<&str>]) -> Result<SplitOutcome, GroupError> {
    if group.status != GroupStatus::Active {
        return Err(GroupError::SourceNotActive);
    }
    if partitions.len() < 2 {
        return Err(GroupError::TooFewPartitions);
    }

    let members_by_key = group
        .memberships
        .iter()
        .map(|member| (member.thread_canonical_key.as_str(), member))
        .collect::<BTreeMap<_, _>>();
    let mut assigned = HashSet::new();
    let mut successors = Vec::with_capacity(partitions.len());
    for partition in partitions {
        if partition.is_empty() {
            return Err(GroupError::EmptyPartition);
        }
        let mut memberships = Vec::with_capacity(partition.len());
        for key in partition {
            if !assigned.insert(*key) {
                return Err(GroupError::DuplicateMember((*key).into()));
            }
            let Some(member) = members_by_key.get(key) else {
                return Err(GroupError::MembershipMismatch);
            };
            memberships.push(Membership {
                thread_canonical_key: member.thread_canonical_key.clone(),
                role: member.role.clone(),
                state: member.state.clone(),
                provenance: MembershipProvenance::Operator,
                deleted_at: member.deleted_at,
            });
        }
        let keys = memberships
            .iter()
            .map(|member| member.thread_canonical_key.as_str())
            .collect::<Vec<_>>();
        successors.push(Group {
            canonical_key: split_group_canonical_key(&group.canonical_key, &keys),
            status: GroupStatus::Active,
            memberships,
        });
    }
    if assigned.len() != members_by_key.len() {
        return Err(GroupError::MembershipMismatch);
    }

    let mut source = group.clone();
    source.status = GroupStatus::Split;
    for member in &mut source.memberships {
        member.state = MembershipState::Redirected;
    }
    Ok(SplitOutcome { source, successors })
}

fn candidate_rank(candidate: &Candidate) -> Option<u8> {
    match candidate.evidence {
        CandidateEvidence::Source {
            confidence: SourceConfidence::Exact,
        } => Some(3),
        CandidateEvidence::OperatorConfirmation => Some(2),
        CandidateEvidence::Source {
            confidence: SourceConfidence::Strong,
        } => Some(1),
        CandidateEvidence::Source {
            confidence: SourceConfidence::Heuristic | SourceConfidence::Unsupported,
        } => None,
    }
}
