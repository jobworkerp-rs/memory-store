//! Startup decision over the set of vector tables (rules 0–8).
//!
//! Pure: callers observe each table and the RDB, then apply the returned
//! record writes. Keeping the decision free of I/O lets every rule and
//! rule-ordering case be table-tested.

use super::record::{MarkerState, MigrationMarker, SpaceRecord, TableRecord};
use super::{SpaceComponents, SpaceId};
use crate::infra::startup_error::StartupError;
use std::collections::BTreeSet;

/// Embedding-model labels found in a table's rows (rule 8 input).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowModels {
    pub names: BTreeSet<String>,
    /// At least one row has no model label.
    pub has_unlabeled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableObservation {
    pub table: &'static str,
    pub record: TableRecord,
    pub is_empty: bool,
    /// Observed only for an unrecorded, non-empty table.
    pub row_models: Option<RowModels>,
}

/// A record write the caller must apply before serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordWrite {
    pub table: &'static str,
    pub space: SpaceRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupDecision {
    /// Start, after applying `writes` (possibly none).
    Start {
        writes: Vec<RecordWrite>,
    },
    /// Start in the rebuild-pending state of `attempt_id` (rule 4).
    StartRebuilding {
        attempt_id: String,
    },
    Fail(StartupError),
}

/// Whether [`decide`] needs to know if the RDB holds any embedding
/// target. Only rule 3 consults it, so callers can skip the RDB scan
/// otherwise.
pub fn needs_rdb_target_check(tables: &[TableObservation]) -> bool {
    !tables.is_empty()
        && tables
            .iter()
            .all(|t| t.is_empty && t.record.marker.is_none())
}

/// The chunking check of rule 4: the first start of a pending rebuild
/// pins the current chunking settings, and later starts must match them.
/// Returns the tables that still need the pin (a partially applied pin is
/// completed rather than treated as a change).
pub fn pin_rebuild_chunking(
    tables: &[TableObservation],
    attempt_id: &str,
    current: &str,
) -> Result<Vec<&'static str>, StartupError> {
    if tables
        .iter()
        .filter_map(|t| t.record.rebuild_chunking.as_deref())
        .any(|pinned| pinned != current)
    {
        return Err(StartupError::EmbeddingRebuildConfigChanged {
            attempt_id: attempt_id.to_string(),
        });
    }
    Ok(tables
        .iter()
        .filter(|t| t.record.rebuild_chunking.is_none())
        .map(|t| t.table)
        .collect())
}

pub fn decide(
    tables: &[TableObservation],
    current: &SpaceComponents,
    rdb_has_targets: bool,
) -> StartupDecision {
    use StartupDecision::{Fail, Start, StartRebuilding};
    if tables.is_empty() {
        return Start { writes: vec![] };
    }
    let current_id = current.space_id();
    let markers: Vec<(&str, &MigrationMarker)> = tables
        .iter()
        .filter_map(|t| t.record.marker.as_ref().map(|m| (t.table, m)))
        .collect();
    let first_with = |states: &[MarkerState]| {
        markers
            .iter()
            .find(|(_, m)| states.contains(&m.state))
            .map(|(_, m)| *m)
    };

    // Rule 0
    if let Some(m) = first_with(&[MarkerState::Restoring, MarkerState::Discarding]) {
        return Fail(StartupError::EmbeddingCancelIncomplete {
            attempt_id: m.attempt_id.clone(),
            operation: if m.state == MarkerState::Restoring {
                "restore"
            } else {
                "abandon"
            }
            .to_string(),
        });
    }
    // Rule 1
    if let Some(m) = first_with(&[MarkerState::Committing]) {
        return Fail(StartupError::EmbeddingFinalizeIncomplete {
            attempt_id: m.attempt_id.clone(),
        });
    }
    // Rule 2
    if let Some(m) = first_with(&[MarkerState::Switching]) {
        return Fail(StartupError::EmbeddingSwitchIncomplete {
            attempt_id: m.attempt_id.clone(),
        });
    }
    // Only `pending` markers remain.
    if !markers.is_empty() {
        let attempts: BTreeSet<&str> = markers.iter().map(|(_, m)| m.attempt_id.as_str()).collect();
        // Rule 2a
        if markers.len() != tables.len() || attempts.len() != 1 {
            return Fail(StartupError::EmbeddingRebuildInconsistent {
                markers: describe_markers(tables),
            });
        }
        // Rule 4
        let attempt_id = markers[0].1.attempt_id.clone();
        for t in tables {
            if let Some(err) = mismatch(t, current, &current_id) {
                return Fail(err);
            }
        }
        return StartRebuilding { attempt_id };
    }

    // Rule 3
    if tables.iter().all(|t| t.is_empty) && !rdb_has_targets {
        let writes = tables
            .iter()
            .filter(|t| t.record.space.as_ref().map(|s| &s.space_id) != Some(&current_id))
            .map(|t| RecordWrite {
                table: t.table,
                space: SpaceRecord::new(current, false),
            })
            .collect();
        return Start { writes };
    }

    // Rule 5
    let recorded: BTreeSet<&SpaceId> = tables
        .iter()
        .filter_map(|t| t.record.space.as_ref().map(|s| &s.space_id))
        .collect();
    if recorded.len() > 1 {
        let culprit = tables
            .iter()
            .find(|t| {
                t.record
                    .space
                    .as_ref()
                    .is_some_and(|s| s.space_id != current_id)
            })
            .expect("two distinct recorded spaces cannot both equal the current one");
        return Fail(mismatch(culprit, current, &current_id).expect("recorded space differs"));
    }

    let mut writes = Vec::new();
    for t in tables {
        if t.record.space.is_some() {
            // Rule 6
            if let Some(err) = mismatch(t, current, &current_id) {
                return Fail(err);
            }
        } else if t.is_empty {
            // Rule 7
            writes.push(RecordWrite {
                table: t.table,
                space: SpaceRecord::new(current, false),
            });
        } else {
            // Rule 8: the dimension was already checked by the schema
            // fingerprint when the table was opened.
            let models = t.row_models.clone().unwrap_or_default();
            if models.has_unlabeled || models.names.len() != 1 {
                return Fail(StartupError::EmbeddingSpaceUnknown {
                    table: t.table.to_string(),
                    row_models: serde_json::to_string(&models.names)
                        .expect("string set serializes"),
                });
            }
            writes.push(RecordWrite {
                table: t.table,
                space: SpaceRecord::new(current, true),
            });
        }
    }
    Start { writes }
}

fn mismatch(
    t: &TableObservation,
    current: &SpaceComponents,
    current_id: &SpaceId,
) -> Option<StartupError> {
    let recorded = t.record.space.as_ref()?;
    if &recorded.space_id == current_id {
        return None;
    }
    Some(StartupError::EmbeddingSpaceMismatch {
        table: t.table.to_string(),
        current_space_id: current_id.to_string(),
        current_space: current.to_json(),
        recorded_space_id: recorded.space_id.to_string(),
        recorded_space: recorded
            .components
            .as_ref()
            .map(SpaceComponents::to_json)
            .unwrap_or_default(),
    })
}

fn describe_markers(tables: &[TableObservation]) -> String {
    let map: serde_json::Map<String, serde_json::Value> = tables
        .iter()
        .map(|t| {
            let value = match &t.record.marker {
                Some(m) => {
                    serde_json::json!({"marker": m.state.as_str(), "attempt_id": m.attempt_id})
                }
                None => serde_json::json!({"marker": null, "attempt_id": null}),
            };
            (t.table.to_string(), value)
        })
        .collect();
    serde_json::Value::Object(map).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn space(model: &str) -> SpaceComponents {
        SpaceComponents {
            model_id: model.into(),
            tokenizer_model_id: String::new(),
            revision: "unversioned".into(),
            dimension: 4,
            distance: "cosine".into(),
        }
    }

    fn table(name: &'static str) -> TableObservation {
        TableObservation {
            table: name,
            record: TableRecord::default(),
            is_empty: true,
            row_models: None,
        }
    }

    fn recorded(mut t: TableObservation, model: &str, legacy: bool) -> TableObservation {
        t.record.space = Some(SpaceRecord::new(&space(model), legacy));
        t
    }

    fn with_data(mut t: TableObservation) -> TableObservation {
        t.is_empty = false;
        t
    }

    fn with_marker(mut t: TableObservation, state: MarkerState, attempt: &str) -> TableObservation {
        t.record.marker = Some(MigrationMarker {
            state,
            attempt_id: attempt.into(),
        });
        t
    }

    fn with_models(mut t: TableObservation, names: &[&str], unlabeled: bool) -> TableObservation {
        t.row_models = Some(RowModels {
            names: names.iter().map(|s| s.to_string()).collect(),
            has_unlabeled: unlabeled,
        });
        t
    }

    fn code(d: &StartupDecision) -> &'static str {
        match d {
            StartupDecision::Start { .. } => "start",
            StartupDecision::StartRebuilding { .. } => "start_rebuilding",
            StartupDecision::Fail(e) => match e {
                StartupError::EmbeddingCancelIncomplete { .. } => "embedding_cancel_incomplete",
                StartupError::EmbeddingFinalizeIncomplete { .. } => "embedding_finalize_incomplete",
                StartupError::EmbeddingSwitchIncomplete { .. } => "embedding_switch_incomplete",
                StartupError::EmbeddingRebuildInconsistent { .. } => {
                    "embedding_rebuild_inconsistent"
                }
                StartupError::EmbeddingSpaceMismatch { .. } => "embedding_space_mismatch",
                StartupError::EmbeddingSpaceUnknown { .. } => "embedding_space_unknown",
                other => panic!("unexpected {other:?}"),
            },
        }
    }

    fn writes(d: &StartupDecision) -> Vec<(&'static str, bool, SpaceId)> {
        match d {
            StartupDecision::Start { writes } => writes
                .iter()
                .map(|w| (w.table, w.space.legacy_accept, w.space.space_id.clone()))
                .collect(),
            other => panic!("expected start, got {other:?}"),
        }
    }

    #[test]
    fn rule_table() {
        let cur = space("cur");
        let p = MarkerState::Pending;
        let cases: Vec<(&str, Vec<TableObservation>, bool, &str)> = vec![
            (
                "0 restoring wins over everything",
                vec![
                    with_marker(table("a"), MarkerState::Restoring, "x"),
                    with_marker(table("b"), MarkerState::Committing, "x"),
                ],
                false,
                "embedding_cancel_incomplete",
            ),
            (
                "0 discarding",
                vec![
                    with_marker(table("a"), MarkerState::Discarding, "x"),
                    table("b"),
                ],
                false,
                "embedding_cancel_incomplete",
            ),
            (
                "1 committing beats switching",
                vec![
                    with_marker(table("a"), MarkerState::Switching, "x"),
                    with_marker(table("b"), MarkerState::Committing, "x"),
                ],
                false,
                "embedding_finalize_incomplete",
            ),
            (
                "2 switching with some tables already pending",
                vec![
                    with_marker(table("a"), MarkerState::Switching, "x"),
                    with_marker(table("b"), p, "x"),
                ],
                false,
                "embedding_switch_incomplete",
            ),
            (
                "2a only some tables pending",
                vec![
                    with_marker(recorded(table("a"), "cur", false), p, "x"),
                    table("b"),
                ],
                false,
                "embedding_rebuild_inconsistent",
            ),
            (
                "2a attempts differ",
                vec![
                    with_marker(recorded(table("a"), "cur", false), p, "x"),
                    with_marker(recorded(table("b"), "cur", false), p, "y"),
                ],
                false,
                "embedding_rebuild_inconsistent",
            ),
            (
                "4 all pending on the current space",
                vec![
                    with_marker(recorded(table("a"), "cur", false), p, "x"),
                    with_marker(recorded(table("b"), "cur", false), p, "x"),
                ],
                false,
                "start_rebuilding",
            ),
            (
                "4 all pending on another space",
                vec![
                    with_marker(recorded(table("a"), "old", false), p, "x"),
                    with_marker(recorded(table("b"), "old", false), p, "x"),
                ],
                false,
                "embedding_space_mismatch",
            ),
            (
                "3 empty tables and no RDB targets adopt the current space",
                vec![recorded(table("a"), "old", true), table("b")],
                false,
                "start",
            ),
            (
                "5 recorded spaces differ between tables",
                vec![
                    with_data(recorded(table("a"), "cur", false)),
                    recorded(table("b"), "old", false),
                ],
                false,
                "embedding_space_mismatch",
            ),
            (
                "6 recorded equals current",
                vec![with_data(recorded(table("a"), "cur", false))],
                false,
                "start",
            ),
            (
                "6 recorded differs",
                vec![with_data(recorded(table("a"), "old", false))],
                false,
                "embedding_space_mismatch",
            ),
            (
                "6 applies to empty tables when the RDB has targets",
                vec![recorded(table("a"), "old", false)],
                true,
                "embedding_space_mismatch",
            ),
            ("7 unrecorded empty table", vec![table("a")], true, "start"),
            (
                "8 single labeled model",
                vec![with_models(with_data(table("a")), &["any-model"], false)],
                true,
                "start",
            ),
            (
                "8 mixed models",
                vec![with_models(with_data(table("a")), &["m1", "m2"], false)],
                true,
                "embedding_space_unknown",
            ),
            (
                "8 unlabeled rows",
                vec![with_models(with_data(table("a")), &["m1"], true)],
                true,
                "embedding_space_unknown",
            ),
        ];
        for (name, tables, rdb, expected) in cases {
            let d = decide(&tables, &cur, rdb);
            assert_eq!(code(&d), expected, "case: {name}: {d:?}");
        }
    }

    #[test]
    fn rule_3_replaces_records_without_legacy_acceptance() {
        let cur = space("cur");
        let d = decide(
            &[
                recorded(table("a"), "old", true),
                recorded(table("b"), "cur", false),
            ],
            &cur,
            false,
        );
        assert_eq!(writes(&d), vec![("a", false, cur.space_id())]);
    }

    #[test]
    fn rule_7_records_current_space_without_legacy_acceptance() {
        let cur = space("cur");
        let d = decide(&[table("a")], &cur, true);
        assert_eq!(writes(&d), vec![("a", false, cur.space_id())]);
    }

    #[test]
    fn rule_8_does_not_compare_row_model_with_config() {
        let cur = space("cur");
        let d = decide(
            &[with_models(
                with_data(table("a")),
                &["unrelated-name"],
                false,
            )],
            &cur,
            true,
        );
        assert_eq!(writes(&d), vec![("a", true, cur.space_id())]);
    }

    #[test]
    fn mixed_recorded_and_unrecorded_tables() {
        // A newly enabled table next to a recorded one is recorded by
        // rule 7 while the other keeps its record (rule 6).
        let cur = space("cur");
        let d = decide(
            &[with_data(recorded(table("a"), "cur", true)), table("b")],
            &cur,
            true,
        );
        assert_eq!(writes(&d), vec![("b", false, cur.space_id())]);
    }

    #[test]
    fn mismatch_reports_both_spaces() {
        let cur = space("cur");
        let d = decide(&[with_data(recorded(table("a"), "old", false))], &cur, true);
        let StartupDecision::Fail(StartupError::EmbeddingSpaceMismatch {
            table,
            current_space_id,
            recorded_space_id,
            current_space,
            recorded_space,
        }) = d
        else {
            panic!("expected mismatch");
        };
        assert_eq!(table, "a");
        assert_eq!(current_space_id, cur.space_id().to_string());
        assert_eq!(recorded_space_id, space("old").space_id().to_string());
        assert!(current_space.contains("\"cur\""));
        assert!(recorded_space.contains("\"old\""));
    }

    #[test]
    fn marker_attempt_and_operation_are_reported() {
        let cur = space("cur");
        match decide(
            &[with_marker(table("a"), MarkerState::Discarding, "att-9")],
            &cur,
            false,
        ) {
            StartupDecision::Fail(StartupError::EmbeddingCancelIncomplete {
                attempt_id,
                operation,
            }) => {
                assert_eq!(attempt_id, "att-9");
                assert_eq!(operation, "abandon");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn rdb_check_is_needed_only_when_every_table_is_empty_and_unmarked() {
        assert!(needs_rdb_target_check(&[table("a"), table("b")]));
        assert!(!needs_rdb_target_check(&[
            table("a"),
            with_data(table("b"))
        ]));
        assert!(!needs_rdb_target_check(&[with_marker(
            table("a"),
            MarkerState::Pending,
            "x"
        )]));
        assert!(!needs_rdb_target_check(&[]));
    }

    #[test]
    fn rebuild_chunking_is_pinned_once_and_then_enforced() {
        let obs = |label: &'static str, pinned: Option<&str>| TableObservation {
            table: label,
            record: TableRecord {
                rebuild_chunking: pinned.map(str::to_string),
                ..TableRecord::default()
            },
            is_empty: true,
            row_models: None,
        };
        assert_eq!(
            pin_rebuild_chunking(&[obs("memory", None), obs("thread", None)], "a", "c1"),
            Ok(vec!["memory", "thread"])
        );
        assert_eq!(
            pin_rebuild_chunking(&[obs("memory", Some("c1")), obs("thread", None)], "a", "c1"),
            Ok(vec!["thread"]),
            "an interrupted pin is completed"
        );
        assert_eq!(
            pin_rebuild_chunking(&[obs("memory", Some("c1"))], "a", "c1"),
            Ok(vec![])
        );
        assert_eq!(
            pin_rebuild_chunking(&[obs("memory", Some("c1")), obs("thread", None)], "a", "c2"),
            Err(StartupError::EmbeddingRebuildConfigChanged {
                attempt_id: "a".into()
            })
        );
    }
}
