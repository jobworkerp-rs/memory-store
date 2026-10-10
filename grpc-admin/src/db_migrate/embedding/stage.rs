//! Effective stage of an unfinished attempt and the per-stage responses
//! of every command (spec §3.3 "実効段階", §3.10).
//!
//! The effective stage is derived from the attempt record and the table
//! markers together, so an interruption between updating the two still
//! yields one stage. The response tables are data so that tests and
//! commands read the same table.

use super::attempt::{AttemptRecord, CancelOperation, Method, Stage, Status};
use super::output::{ErrorCode, Resolution};
use infra::infra::embedding_space::record::{MarkerState, MigrationMarker};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectiveStage {
    Backup,
    Replace,
    RebuildPending,
    Commit,
    Restoring,
    Discarding,
}

impl EffectiveStage {
    pub const ALL: [EffectiveStage; 6] = [
        Self::Backup,
        Self::Replace,
        Self::RebuildPending,
        Self::Commit,
        Self::Restoring,
        Self::Discarding,
    ];

    /// `attempt_stage=` value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Backup => "backup",
            Self::Replace => "replace",
            Self::RebuildPending => "rebuild_pending",
            Self::Commit => "commit",
            Self::Restoring => "restoring",
            Self::Discarding => "discarding",
        }
    }
}

/// The effective stage, or `None` for a finished attempt (terminal
/// status and no marker left).
pub fn effective_stage(
    record: &AttemptRecord,
    markers: &[Option<MigrationMarker>],
) -> Option<EffectiveStage> {
    let mine: Vec<Option<MarkerState>> = markers
        .iter()
        .map(|m| {
            m.as_ref()
                .filter(|m| m.attempt_id == record.attempt_id)
                .map(|m| m.state)
        })
        .collect();
    let any = |s: MarkerState| mine.contains(&Some(s));
    let marked = mine.iter().any(Option::is_some);
    let running = record.status == Status::Running;
    if !running && !marked {
        return None;
    }
    if record.cancel_operation == Some(CancelOperation::Restore)
        || any(MarkerState::Restoring)
        || record.status == Status::Restored
    {
        return Some(EffectiveStage::Restoring);
    }
    if record.cancel_operation == Some(CancelOperation::Discard)
        || any(MarkerState::Discarding)
        || (record.status == Status::Abandoned && record.discarded)
    {
        return Some(EffectiveStage::Discarding);
    }
    if any(MarkerState::Committing)
        || record.stage == Stage::Commit
        || record.status == Status::Completed
    {
        return Some(EffectiveStage::Commit);
    }
    if !mine.is_empty() && mine.iter().all(|m| *m == Some(MarkerState::Pending)) {
        return Some(EffectiveStage::RebuildPending);
    }
    if any(MarkerState::Pending) || record.stage == Stage::Replace {
        return Some(EffectiveStage::Replace);
    }
    Some(EffectiveStage::Backup)
}

/// Commands that consult the stage tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    SwitchNew,
    SwitchResume,
    Abandon,
    Finalize,
    Restore,
    /// `local apply` / `release` work and `local restore` restores that
    /// would be refused while a migration is unfinished.
    ApplyOrRestore,
}

impl Operation {
    pub const ALL: [Operation; 6] = [
        Self::SwitchNew,
        Self::SwitchResume,
        Self::Abandon,
        Self::Finalize,
        Self::Restore,
        Self::ApplyOrRestore,
    ];
}

/// Success responses decided by the tables (resends of finished work).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlreadyDone {
    Switched,
    Completed,
    Restored,
    Abandoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refusal {
    /// `None` for the apply/restore column, whose code is
    /// `embedding_migration_in_progress`.
    pub error_code: Option<ErrorCode>,
    pub resolution: Resolution,
    pub with_attempt: bool,
    pub with_backup: bool,
    pub cancel_operation: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Response {
    /// Go on to the ordinary checks and the operation itself.
    Continue,
    Succeed(AlreadyDone),
    Refuse(Refusal),
}

fn refuse(
    error_code: Option<ErrorCode>,
    resolution: Resolution,
    attempt: bool,
    backup: bool,
) -> Response {
    Response::Refuse(Refusal {
        error_code,
        resolution,
        with_attempt: attempt,
        with_backup: backup,
        cancel_operation: None,
    })
}

fn cancel(resolution: Resolution, backup: bool, operation: &'static str) -> Response {
    Response::Refuse(Refusal {
        error_code: Some(ErrorCode::CancelInProgress),
        resolution,
        with_attempt: true,
        with_backup: backup,
        cancel_operation: Some(operation),
    })
}

/// Response for a finished attempt (stage 2a). Commands that take no
/// attempt ID treat a finished attempt as "no unfinished migration".
pub fn finished_response(status: Status, op: Operation) -> Response {
    use ErrorCode::{AttemptFinished, RebuildNotPending};
    use Operation::*;
    use Resolution::PlanRequired;
    let finished = refuse(Some(AttemptFinished), PlanRequired, false, false);
    match (status, op) {
        (_, SwitchNew | ApplyOrRestore) | (Status::Running, _) => Response::Continue,
        (Status::Completed, Finalize) => Response::Succeed(AlreadyDone::Completed),
        (Status::Restored, Restore) => Response::Succeed(AlreadyDone::Restored),
        (Status::Abandoned, Abandon) => Response::Succeed(AlreadyDone::Abandoned),
        (Status::Restored | Status::Abandoned, Finalize) => {
            refuse(Some(RebuildNotPending), PlanRequired, false, false)
        }
        _ => finished,
    }
}

/// Response for an unfinished attempt at `stage` (stage 3).
pub fn stage_response(method: Method, stage: EffectiveStage, op: Operation) -> Response {
    use EffectiveStage as S;
    use ErrorCode::*;
    use Operation::*;
    use Resolution as R;
    let b = method == Method::Backup;
    let in_progress = Some(EmbeddingSwitchInProgress);
    match (stage, op) {
        (S::Backup, SwitchNew) => refuse(in_progress, R::ResumeOrAbandon, true, false),
        (S::Backup, SwitchResume | Abandon) => Response::Continue,
        (S::Backup, Finalize) => refuse(Some(RebuildNotPending), R::ResumeOrAbandon, true, false),
        (S::Backup, Restore) => refuse(Some(RestoreNotAvailable), R::AbandonRequired, true, false),
        (S::Backup, ApplyOrRestore) => refuse(None, R::AbandonRequired, true, false),

        (S::Replace, SwitchNew) if b => refuse(in_progress, R::ResumeOrRestore, true, true),
        (S::Replace, SwitchNew) => refuse(in_progress, R::ResumeOrAbandon, true, false),
        (S::Replace, SwitchResume) => Response::Continue,
        (S::Replace, Abandon) if b => {
            refuse(Some(AbandonNotAllowed), R::ResumeOrRestore, true, true)
        }
        (S::Replace, Abandon) => Response::Continue,
        (S::Replace, Finalize) if b => {
            refuse(Some(RebuildNotPending), R::ResumeOrRestore, true, true)
        }
        (S::Replace, Finalize) => refuse(Some(RebuildNotPending), R::ResumeOrAbandon, true, false),
        (S::Replace, Restore) if b => Response::Continue,
        (S::Replace, Restore) => refuse(Some(RestoreNotAvailable), R::ResumeOrAbandon, true, false),
        (S::Replace, ApplyOrRestore) if b => refuse(None, R::RestoreRequired, true, true),
        (S::Replace, ApplyOrRestore) => refuse(None, R::AbandonRequired, true, false),

        (S::RebuildPending, SwitchNew) if b => {
            refuse(in_progress, R::ContinueOrRestore, true, true)
        }
        (S::RebuildPending, SwitchNew) => refuse(in_progress, R::ContinueOrAbandon, true, false),
        (S::RebuildPending, SwitchResume) => Response::Succeed(AlreadyDone::Switched),
        (S::RebuildPending, Abandon) if b => {
            refuse(Some(AbandonNotAllowed), R::ContinueOrRestore, true, true)
        }
        (S::RebuildPending, Abandon) => Response::Continue,
        (S::RebuildPending, Finalize) => Response::Continue,
        (S::RebuildPending, Restore) if b => Response::Continue,
        (S::RebuildPending, Restore) => {
            refuse(Some(RestoreNotAvailable), R::ContinueOrAbandon, true, false)
        }
        (S::RebuildPending, ApplyOrRestore) if b => refuse(None, R::ContinueOrRestore, true, true),
        (S::RebuildPending, ApplyOrRestore) => refuse(None, R::ContinueOrAbandon, true, false),

        (S::Commit, SwitchNew) => refuse(in_progress, R::FinalizeRequired, true, false),
        (S::Commit, SwitchResume) => {
            refuse(Some(FinalizeInProgress), R::FinalizeRequired, true, false)
        }
        (S::Commit, Abandon) => refuse(Some(AbandonNotAllowed), R::FinalizeRequired, true, false),
        (S::Commit, Finalize) => Response::Continue,
        (S::Commit, Restore) if b => Response::Continue,
        (S::Commit, Restore) => refuse(Some(RestoreNotAvailable), R::FinalizeRequired, true, false),
        (S::Commit, ApplyOrRestore) => refuse(None, R::FinalizeRequired, true, false),

        (S::Restoring, Restore) => Response::Continue,
        (S::Restoring, ApplyOrRestore) if b => refuse(None, R::RestoreRequired, true, true),
        (S::Restoring, ApplyOrRestore) => refuse(None, R::ManualRecovery, true, false),
        (S::Restoring, _) if b => cancel(R::RestoreRequired, true, "restore"),
        // A no-backup attempt never restores; this state cannot be
        // continued by any command.
        (S::Restoring, _) => cancel(R::ManualRecovery, false, "restore"),

        (S::Discarding, Abandon) => Response::Continue,
        (S::Discarding, ApplyOrRestore) => refuse(None, R::AbandonRequired, true, false),
        (S::Discarding, _) => cancel(R::AbandonRequired, false, "abandon"),
    }
}

/// `inspect`'s next action for an unfinished attempt.
pub fn inspect_next_action(method: Method, stage: EffectiveStage) -> super::output::NextAction {
    use super::output::NextAction as N;
    match stage {
        EffectiveStage::Restoring => N::Restore,
        EffectiveStage::Discarding => N::Abandon,
        EffectiveStage::Backup => N::ResumeOrAbandon,
        EffectiveStage::Replace if method == Method::Backup => N::ResumeOrRestore,
        EffectiveStage::Replace => N::ResumeOrAbandon,
        EffectiveStage::RebuildPending => N::StartRebuild,
        EffectiveStage::Commit => N::Finalize,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_migrate::embedding::attempt::{AttemptRecord, FORMAT_VERSION};
    use infra::infra::embedding_space::SpaceComponents;

    fn record(status: Status, stage: Stage) -> AttemptRecord {
        AttemptRecord {
            format_version: FORMAT_VERSION,
            attempt_id: "a".into(),
            method: Method::Backup,
            source_space_id: None,
            target_space_id: "t".into(),
            target_space: SpaceComponents {
                model_id: "m".into(),
                tokenizer_model_id: String::new(),
                revision: "r".into(),
                dimension: 4,
                distance: "cosine".into(),
            },
            backup_path: None,
            backup_complete: false,
            stage,
            status,
            cancel_operation: None,
            discarded: false,
            accepted_failed: None,
            backup_keep: None,
            started_at: 0,
        }
    }

    fn m(state: MarkerState) -> Option<MigrationMarker> {
        Some(MigrationMarker {
            state,
            attempt_id: "a".into(),
        })
    }

    #[test]
    fn effective_stage_table() {
        use EffectiveStage as E;
        use MarkerState::*;
        let run = |stage| record(Status::Running, stage);
        type Case = (
            &'static str,
            AttemptRecord,
            Vec<Option<MigrationMarker>>,
            Option<E>,
        );
        let cases: Vec<Case> = vec![
            (
                "finished",
                record(Status::Completed, Stage::Commit),
                vec![None, None],
                None,
            ),
            (
                "backup, no markers yet",
                run(Stage::Backup),
                vec![None, None],
                Some(E::Backup),
            ),
            (
                "backup, switching",
                run(Stage::Backup),
                vec![m(Switching), m(Switching)],
                Some(E::Backup),
            ),
            (
                "replace recorded",
                run(Stage::Replace),
                vec![m(Switching), m(Switching)],
                Some(E::Replace),
            ),
            (
                "partly replaced",
                run(Stage::Backup),
                vec![m(Pending), m(Switching)],
                Some(E::Replace),
            ),
            (
                "all pending",
                run(Stage::Replace),
                vec![m(Pending), m(Pending)],
                Some(E::RebuildPending),
            ),
            (
                "committing marker",
                run(Stage::RebuildPending),
                vec![m(Committing), m(Pending)],
                Some(E::Commit),
            ),
            (
                "completed with markers",
                record(Status::Completed, Stage::Commit),
                vec![m(Committing), None],
                Some(E::Commit),
            ),
            (
                "restoring marker",
                run(Stage::RebuildPending),
                vec![m(Restoring), m(Pending)],
                Some(E::Restoring),
            ),
            (
                "restored with markers",
                record(Status::Restored, Stage::RebuildPending),
                vec![m(Restoring), None],
                Some(E::Restoring),
            ),
            (
                "discarding marker",
                run(Stage::RebuildPending),
                vec![m(Discarding), m(Pending)],
                Some(E::Discarding),
            ),
        ];
        for (name, rec, markers, expected) in cases {
            assert_eq!(effective_stage(&rec, &markers), expected, "{name}");
        }
        let mut cancelled = run(Stage::RebuildPending);
        cancelled.cancel_operation = Some(CancelOperation::Restore);
        assert_eq!(
            effective_stage(&cancelled, &[m(Pending)]),
            Some(E::Restoring)
        );
        let mut discarded = record(Status::Abandoned, Stage::RebuildPending);
        discarded.discarded = true;
        assert_eq!(
            effective_stage(&discarded, &[m(Discarding)]),
            Some(E::Discarding)
        );
        // Markers of another attempt do not count as this attempt's.
        let foreign = Some(MigrationMarker {
            state: Pending,
            attempt_id: "other".into(),
        });
        assert_eq!(
            effective_stage(&record(Status::Completed, Stage::Commit), &[foreign]),
            None
        );
    }

    #[test]
    fn every_stage_and_operation_has_a_response() {
        for method in [Method::Backup, Method::NoBackup] {
            for stage in EffectiveStage::ALL {
                for op in Operation::ALL {
                    let r = stage_response(method, stage, op);
                    if let Response::Refuse(refusal) = r {
                        assert!(
                            refusal.with_attempt
                                || refusal.resolution == Resolution::ManualRecovery
                        );
                        if refusal.with_backup {
                            assert_eq!(method, Method::Backup, "{stage:?} {op:?}");
                        }
                        assert_eq!(
                            refusal.error_code.is_none(),
                            op == Operation::ApplyOrRestore
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn spot_checks_against_the_spec_tables() {
        use EffectiveStage as S;
        assert_eq!(
            stage_response(Method::Backup, S::RebuildPending, Operation::SwitchResume),
            Response::Succeed(AlreadyDone::Switched)
        );
        assert_eq!(
            stage_response(Method::NoBackup, S::RebuildPending, Operation::Abandon),
            Response::Continue,
            "a no-backup rebuild can be discarded"
        );
        let Response::Refuse(r) =
            stage_response(Method::Backup, S::Commit, Operation::SwitchResume)
        else {
            panic!()
        };
        assert_eq!(
            (r.error_code, r.resolution),
            (
                Some(ErrorCode::FinalizeInProgress),
                Resolution::FinalizeRequired
            )
        );
        let Response::Refuse(r) = stage_response(Method::Backup, S::Restoring, Operation::Finalize)
        else {
            panic!()
        };
        assert_eq!(r.cancel_operation, Some("restore"));
        assert!(r.with_backup);
    }

    #[test]
    fn finished_attempts() {
        use Operation::*;
        assert_eq!(
            finished_response(Status::Completed, Finalize),
            Response::Succeed(AlreadyDone::Completed)
        );
        assert_eq!(
            finished_response(Status::Restored, Restore),
            Response::Succeed(AlreadyDone::Restored)
        );
        assert_eq!(
            finished_response(Status::Abandoned, Abandon),
            Response::Succeed(AlreadyDone::Abandoned)
        );
        assert_eq!(
            finished_response(Status::Completed, SwitchNew),
            Response::Continue
        );
        let Response::Refuse(r) = finished_response(Status::Abandoned, Finalize) else {
            panic!()
        };
        assert_eq!(r.error_code, Some(ErrorCode::RebuildNotPending));
        let Response::Refuse(r) = finished_response(Status::Completed, Restore) else {
            panic!()
        };
        assert_eq!(
            (r.error_code, r.resolution),
            (Some(ErrorCode::AttemptFinished), Resolution::PlanRequired)
        );
    }

    /// Every cell of the stage tables of spec §3.10, for both methods.
    #[test]
    fn stage_tables_match_the_spec_cell_by_cell() {
        use EffectiveStage as S;
        use ErrorCode::*;
        use Resolution as R;
        #[derive(Debug, Clone, Copy, PartialEq)]
        enum Cell {
            Go,
            Switched,
            /// error_code (None for the apply/restore column), resolution,
            /// whether `backup` is required.
            No(Option<ErrorCode>, Resolution, bool),
        }
        use Cell::{Go, No, Switched};
        let esip = Some(EmbeddingSwitchInProgress);
        let rnp = Some(RebuildNotPending);
        let rna = Some(RestoreNotAvailable);
        let ana = Some(AbandonNotAllowed);
        let fip = Some(FinalizeInProgress);
        let cip = Some(CancelInProgress);
        // Columns: switch (new), switch --resume, abandon, finalize,
        // restore, apply / restore.
        let backup: [(S, [Cell; 6]); 5] = [
            (
                S::Backup,
                [
                    No(esip, R::ResumeOrAbandon, false),
                    Go,
                    Go,
                    No(rnp, R::ResumeOrAbandon, false),
                    No(rna, R::AbandonRequired, false),
                    No(None, R::AbandonRequired, false),
                ],
            ),
            (
                S::Replace,
                [
                    No(esip, R::ResumeOrRestore, true),
                    Go,
                    No(ana, R::ResumeOrRestore, true),
                    No(rnp, R::ResumeOrRestore, true),
                    Go,
                    No(None, R::RestoreRequired, true),
                ],
            ),
            (
                S::RebuildPending,
                [
                    No(esip, R::ContinueOrRestore, true),
                    Switched,
                    No(ana, R::ContinueOrRestore, true),
                    Go,
                    Go,
                    No(None, R::ContinueOrRestore, true),
                ],
            ),
            (
                S::Commit,
                [
                    No(esip, R::FinalizeRequired, false),
                    No(fip, R::FinalizeRequired, false),
                    No(ana, R::FinalizeRequired, false),
                    Go,
                    Go,
                    No(None, R::FinalizeRequired, false),
                ],
            ),
            (
                S::Restoring,
                [
                    No(cip, R::RestoreRequired, true),
                    No(cip, R::RestoreRequired, true),
                    No(cip, R::RestoreRequired, true),
                    No(cip, R::RestoreRequired, true),
                    Go,
                    No(None, R::RestoreRequired, true),
                ],
            ),
        ];
        let no_backup: [(S, [Cell; 6]); 5] = [
            (
                S::Backup,
                [
                    No(esip, R::ResumeOrAbandon, false),
                    Go,
                    Go,
                    No(rnp, R::ResumeOrAbandon, false),
                    No(rna, R::AbandonRequired, false),
                    No(None, R::AbandonRequired, false),
                ],
            ),
            (
                S::Replace,
                [
                    No(esip, R::ResumeOrAbandon, false),
                    Go,
                    Go,
                    No(rnp, R::ResumeOrAbandon, false),
                    No(rna, R::ResumeOrAbandon, false),
                    No(None, R::AbandonRequired, false),
                ],
            ),
            (
                S::RebuildPending,
                [
                    No(esip, R::ContinueOrAbandon, false),
                    Switched,
                    Go,
                    Go,
                    No(rna, R::ContinueOrAbandon, false),
                    No(None, R::ContinueOrAbandon, false),
                ],
            ),
            (
                S::Commit,
                [
                    No(esip, R::FinalizeRequired, false),
                    No(fip, R::FinalizeRequired, false),
                    No(ana, R::FinalizeRequired, false),
                    Go,
                    No(rna, R::FinalizeRequired, false),
                    No(None, R::FinalizeRequired, false),
                ],
            ),
            (
                S::Discarding,
                [
                    No(cip, R::AbandonRequired, false),
                    No(cip, R::AbandonRequired, false),
                    Go,
                    No(cip, R::AbandonRequired, false),
                    No(cip, R::AbandonRequired, false),
                    No(None, R::AbandonRequired, false),
                ],
            ),
        ];
        let ops = [
            Operation::SwitchNew,
            Operation::SwitchResume,
            Operation::Abandon,
            Operation::Finalize,
            Operation::Restore,
            Operation::ApplyOrRestore,
        ];
        for (method, table) in [(Method::Backup, backup), (Method::NoBackup, no_backup)] {
            for (stage, row) in table {
                for (op, want) in ops.iter().zip(row) {
                    let got = match stage_response(method, stage, *op) {
                        Response::Continue => Go,
                        Response::Succeed(AlreadyDone::Switched) => Switched,
                        Response::Succeed(other) => panic!("{other:?}"),
                        Response::Refuse(r) => {
                            // Every refusal of an unfinished attempt names it,
                            // and a cancel names the interrupted operation.
                            assert!(r.with_attempt, "{method:?} {stage:?} {op:?}");
                            assert_eq!(
                                r.cancel_operation.is_some(),
                                r.error_code == cip,
                                "{method:?} {stage:?} {op:?}"
                            );
                            No(r.error_code, r.resolution, r.with_backup)
                        }
                    };
                    assert_eq!(got, want, "{method:?} {stage:?} {op:?}");
                }
            }
        }
    }
}
