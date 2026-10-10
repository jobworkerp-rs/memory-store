//! `embedding plan`: what changing to a target space requires
//! (spec §3.4 "embedding plan").

use super::counts::{CountKind, Counts};
use super::inspect::Observation;
use super::output::{Count, Decision, ErrorCode, PlanLine, SpaceValue, State};

pub fn derive(obs: &Observation, inspected: State, target_space: &str) -> PlanLine {
    let decision = if obs.nothing_to_embed && !obs.has_markers {
        Decision::AdoptOnStart
    } else if obs.space == SpaceValue::Id(target_space.to_string()) {
        match inspected {
            State::Consistent | State::Failed => Decision::NoChange,
            State::Incomplete | State::Unverified => Decision::ReconcileRequired,
            _ => Decision::ReembedRequired,
        }
    } else {
        Decision::ReembedRequired
    };
    let reason = if obs.storage_mismatch {
        Some(ErrorCode::StorageMismatch)
    } else if obs.has_markers {
        Some(ErrorCode::EmbeddingSwitchInProgress)
    } else if decision != Decision::ReembedRequired {
        Some(ErrorCode::NoReembedNeeded)
    } else {
        None
    };
    let counts = obs.counts.clone().unwrap_or_default();
    let required = |k: CountKind| counts.kind(k).required;
    let failed = |f: fn(&Counts) -> u64| match (&obs.counts, &obs.space) {
        (Some(c), SpaceValue::Id(_)) => Count::Known(f(c)),
        _ => Count::Unknown,
    };
    PlanLine {
        decision,
        executable: reason.is_none(),
        reason,
        current_space: obs.space.clone(),
        target_space: target_space.to_string(),
        memory_text: required(CountKind::MemoryText),
        memory_media: required(CountKind::MemoryMedia),
        thread: required(CountKind::Thread),
        reflection_intent: required(CountKind::ReflectionIntent),
        failed_transient: failed(|c| c.total().failed_transient),
        failed_permanent: failed(|c| c.total().failed_permanent),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(space: SpaceValue) -> Observation {
        let mut c = Counts::default();
        c.by_kind[CountKind::MemoryText as usize].required = 3;
        c.by_kind[CountKind::MemoryMedia as usize].required = 1;
        c.by_kind[CountKind::Thread as usize].failed_permanent = 2;
        Observation {
            space,
            counts: Some(c),
            ..Default::default()
        }
    }

    #[test]
    fn decision_table() {
        let same = SpaceValue::Id("t".into());
        let cases = [
            (
                obs(same.clone()),
                State::Consistent,
                Decision::NoChange,
                Some(ErrorCode::NoReembedNeeded),
            ),
            (
                obs(same.clone()),
                State::Failed,
                Decision::NoChange,
                Some(ErrorCode::NoReembedNeeded),
            ),
            (
                obs(same.clone()),
                State::Incomplete,
                Decision::ReconcileRequired,
                Some(ErrorCode::NoReembedNeeded),
            ),
            (
                obs(same.clone()),
                State::Unverified,
                Decision::ReconcileRequired,
                Some(ErrorCode::NoReembedNeeded),
            ),
            (
                obs(SpaceValue::Id("o".into())),
                State::Consistent,
                Decision::ReembedRequired,
                None,
            ),
            (
                obs(SpaceValue::Unknown),
                State::Unknown,
                Decision::ReembedRequired,
                None,
            ),
            (
                obs(SpaceValue::None),
                State::Incomplete,
                Decision::ReembedRequired,
                None,
            ),
            (
                Observation {
                    nothing_to_embed: true,
                    ..obs(SpaceValue::Id("o".into()))
                },
                State::Consistent,
                Decision::AdoptOnStart,
                Some(ErrorCode::NoReembedNeeded),
            ),
        ];
        for (o, st, decision, reason) in cases {
            let line = derive(&o, st, "t");
            assert_eq!(
                (line.decision, line.reason),
                (decision, reason),
                "{o:?} {st:?}"
            );
            assert_eq!(line.executable, reason.is_none());
        }
    }

    #[test]
    fn counts_and_blockers() {
        let line = derive(&obs(SpaceValue::Id("o".into())), State::Consistent, "t");
        assert_eq!(
            (line.memory_text, line.memory_media, line.thread),
            (3, 1, 0)
        );
        assert_eq!(line.failed_permanent, Count::Known(2));
        let unknown = derive(&obs(SpaceValue::Unknown), State::Unknown, "t");
        assert_eq!(unknown.failed_permanent, Count::Unknown);

        let mut o = obs(SpaceValue::Id("o".into()));
        o.has_markers = true;
        assert_eq!(
            derive(&o, State::RebuildInconsistent, "t").reason,
            Some(ErrorCode::EmbeddingSwitchInProgress)
        );
        o.storage_mismatch = true;
        assert_eq!(
            derive(&o, State::Unknown, "t").reason,
            Some(ErrorCode::StorageMismatch)
        );
    }
}
