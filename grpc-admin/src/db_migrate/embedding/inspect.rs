//! `embedding inspect`: derive the state and next action from what was
//! observed (spec §3.4, §3.10 "inspect の next_action").

use super::attempt::{AttemptRecord, Method};
use super::counts::Counts;
use super::output::{Count, InspectLine, NextAction, SpaceValue, State, UnavailableReason};
use super::stage::{effective_stage, inspect_next_action};
use infra::infra::embedding_space::record::MigrationMarker;

/// Everything `inspect` reports on, as observed.
#[derive(Debug, Clone, Default)]
pub struct Observation {
    /// The RDB or a LanceDB store could not be opened.
    pub unavailable: Option<UnavailableReason>,
    /// The RDB, vector stores, and state directory do not belong together.
    pub storage_mismatch: bool,
    /// Some vector table carries a migration marker.
    pub has_markers: bool,
    /// Markers of every configured table, in configuration order.
    pub markers: Vec<Option<MigrationMarker>>,
    /// The latest attempt record.
    pub attempt: Option<AttemptRecord>,
    /// The attempt record exists but this tool cannot read it.
    pub attempt_unreadable: bool,
    /// Another command holds the operation lock.
    pub operation_locked: bool,
    /// Recorded space of the tables (or what startup would record).
    pub space: SpaceValue,
    /// Counts of the classification; `None` when it could not be done.
    pub counts: Option<Counts>,
    /// Every store is a local directory (backups can be taken).
    pub backup_supported: Option<bool>,
    /// Every vector table is empty, nothing is recorded, and the RDB has
    /// no embedding target.
    pub nothing_to_embed: bool,
}

pub fn derive(obs: &Observation) -> InspectLine {
    let mut line = InspectLine {
        backup_supported: obs.backup_supported,
        ..Default::default()
    };
    if let Some(reason) = obs.unavailable {
        line.state = State::Unavailable;
        line.unavailable_reason = Some(reason);
        line.next_action = match reason {
            UnavailableReason::ResourceUnavailable => NextAction::CheckEnvironment,
            UnavailableReason::ResourceCorrupt => NextAction::ManualRecovery,
        };
        return line;
    }
    if obs.operation_locked {
        line.state = State::Unknown;
        line.next_action = NextAction::Wait;
        return line;
    }
    if obs.storage_mismatch {
        line.state = State::Unknown;
        line.next_action = NextAction::CheckEnvironment;
        return line;
    }
    line.space = obs.space.clone();
    if obs.attempt_unreadable {
        line.state = State::Unknown;
        line.next_action = NextAction::ManualRecovery;
        return line;
    }
    let unfinished = obs
        .attempt
        .as_ref()
        .and_then(|a| effective_stage(a, &obs.markers).map(|s| (a, s)));
    let foreign_markers = obs.markers.iter().flatten().any(|m| {
        obs.attempt
            .as_ref()
            .is_none_or(|a| a.attempt_id != m.attempt_id)
    });
    if foreign_markers {
        // Markers no attempt record accounts for identify no attempt.
        line.state = State::RebuildInconsistent;
        line.next_action = match &obs.attempt {
            Some(a) if a.method == Method::NoBackup => NextAction::Abandon,
            Some(a) if a.method == Method::Backup && a.backup_complete => {
                line.attempt = Some(a.info("replace"));
                line.backup = a.backup_path.clone();
                NextAction::Restore
            }
            _ => NextAction::ManualRecovery,
        };
        return line;
    }
    if let Some((a, stage)) = unfinished {
        line.state = State::Migrating;
        line.attempt = Some(a.info(stage.as_str()));
        if a.method == Method::Backup && a.backup_complete {
            line.backup = a.backup_path.clone();
        }
        line.next_action = inspect_next_action(a.method, stage);
        return line;
    }
    if obs.space == SpaceValue::Unknown {
        line.state = State::Unknown;
        line.next_action = NextAction::Switch;
        return line;
    }
    let Some(counts) = &obs.counts else {
        line.state = State::Unknown;
        line.next_action = NextAction::Switch;
        return line;
    };
    let t = counts.total();
    line.required = Count::Known(t.required);
    line.complete = Count::Known(t.complete);
    line.missing = Count::Known(t.missing);
    line.stale = Count::Known(t.stale);
    line.unverified = Count::Known(t.unverified);
    line.failed_transient = Count::Known(t.failed_transient);
    line.failed_permanent = Count::Known(t.failed_permanent);
    line.orphan = Count::Known(counts.orphan);
    (line.state, line.next_action) = if t.missing + t.stale + counts.orphan > 0 {
        (State::Incomplete, NextAction::Reconcile)
    } else if t.unverified > 0 {
        (State::Unverified, NextAction::Reconcile)
    } else if t.failed() > 0 {
        (State::Failed, NextAction::ResolveFailed)
    } else {
        (State::Consistent, NextAction::None)
    };
    line
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_migrate::embedding::counts::CountKind;

    fn counts(f: impl FnOnce(&mut Counts)) -> Counts {
        let mut c = Counts::default();
        f(&mut c);
        c
    }

    fn obs(c: Counts) -> Observation {
        Observation {
            space: SpaceValue::Id("s".into()),
            counts: Some(c),
            backup_supported: Some(true),
            ..Default::default()
        }
    }

    fn state(o: &Observation) -> (State, NextAction) {
        let l = derive(o);
        (l.state, l.next_action)
    }

    #[test]
    fn state_priority_table() {
        let text = CountKind::MemoryText as usize;
        assert_eq!(
            state(&obs(counts(|c| c.by_kind[text].required = 1))),
            (State::Consistent, NextAction::None),
            "required but no other state → consistent counts nothing as missing"
        );
        let base = |f: fn(&mut Counts)| state(&obs(counts(f)));
        assert_eq!(
            base(|c| {
                c.by_kind[0].missing = 1;
                c.by_kind[0].unverified = 1;
                c.by_kind[0].failed_permanent = 1;
            }),
            (State::Incomplete, NextAction::Reconcile)
        );
        assert_eq!(
            base(|c| c.orphan = 1),
            (State::Incomplete, NextAction::Reconcile)
        );
        assert_eq!(
            base(|c| {
                c.by_kind[0].unverified = 1;
                c.by_kind[0].failed_transient = 1;
            }),
            (State::Unverified, NextAction::Reconcile)
        );
        assert_eq!(
            base(|c| c.by_kind[2].failed_transient = 1),
            (State::Failed, NextAction::ResolveFailed)
        );
    }

    #[test]
    fn blocking_conditions_come_first() {
        let o = Observation {
            unavailable: Some(UnavailableReason::ResourceCorrupt),
            storage_mismatch: true,
            has_markers: true,
            ..obs(Counts::default())
        };
        let l = derive(&o);
        assert_eq!(
            (l.state, l.next_action),
            (State::Unavailable, NextAction::ManualRecovery)
        );
        assert_eq!(l.required, Count::Unknown);

        let o = Observation {
            storage_mismatch: true,
            has_markers: true,
            ..obs(Counts::default())
        };
        assert_eq!(state(&o), (State::Unknown, NextAction::CheckEnvironment));
        let o = Observation {
            has_markers: true,
            markers: vec![Some(MigrationMarker {
                state: infra::infra::embedding_space::record::MarkerState::Pending,
                attempt_id: "x".into(),
            })],
            ..obs(Counts::default())
        };
        assert_eq!(
            state(&o),
            (State::RebuildInconsistent, NextAction::ManualRecovery)
        );
        let o = Observation {
            space: SpaceValue::Unknown,
            ..obs(Counts::default())
        };
        assert_eq!(state(&o), (State::Unknown, NextAction::Switch));
    }

    #[test]
    fn unfinished_attempts_report_their_stage_and_next_action() {
        use crate::db_migrate::embedding::attempt::{FORMAT_VERSION, Stage, Status};
        use infra::infra::embedding_space::record::MarkerState;
        let attempt = AttemptRecord {
            format_version: FORMAT_VERSION,
            attempt_id: "a1".into(),
            method: Method::Backup,
            source_space_id: Some("s".into()),
            target_space_id: "t".into(),
            target_space: infra::infra::embedding_space::SpaceComponents {
                model_id: "m".into(),
                tokenizer_model_id: String::new(),
                revision: "r".into(),
                dimension: 4,
                distance: "cosine".into(),
            },
            backup_path: Some("/backups/a1".into()),
            backup_complete: true,
            stage: Stage::RebuildPending,
            status: Status::Running,
            cancel_operation: None,
            discarded: false,
            accepted_failed: None,
            backup_keep: None,
            started_at: 0,
        };
        let pending = Some(MigrationMarker {
            state: MarkerState::Pending,
            attempt_id: "a1".into(),
        });
        let o = Observation {
            attempt: Some(attempt.clone()),
            markers: vec![pending.clone(), pending],
            has_markers: true,
            ..obs(Counts::default())
        };
        let l = derive(&o);
        assert_eq!(
            (l.state, l.next_action),
            (State::Migrating, NextAction::StartRebuild)
        );
        assert_eq!(l.attempt.unwrap().stage, "rebuild_pending");
        assert_eq!(l.backup.as_deref(), Some("/backups/a1"));

        // A finished attempt with no markers is not migrating.
        let done = Observation {
            attempt: Some(AttemptRecord {
                status: Status::Completed,
                ..attempt
            }),
            ..obs(Counts::default())
        };
        assert_eq!(derive(&done).state, State::Consistent);

        let locked = Observation {
            operation_locked: true,
            ..obs(Counts::default())
        };
        assert_eq!(state(&locked), (State::Unknown, NextAction::Wait));
    }

    #[test]
    fn unavailable_resource_points_at_the_environment() {
        let o = Observation {
            unavailable: Some(UnavailableReason::ResourceUnavailable),
            ..Default::default()
        };
        assert_eq!(
            state(&o),
            (State::Unavailable, NextAction::CheckEnvironment)
        );
    }
}
