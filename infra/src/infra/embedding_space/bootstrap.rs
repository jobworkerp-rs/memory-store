//! Observe the vector tables, run the startup decision, and apply the
//! resulting space records.

use super::SpaceComponents;
use super::record::{TableRecord, read_table_record, write_rebuild_chunking, write_space_record};
use super::startup::{
    RowModels, StartupDecision, TableObservation, decide, needs_rdb_target_check,
    pin_rebuild_chunking,
};
use crate::infra::startup_error::StartupError;
use arrow_array::{Array, StringArray};
use futures::StreamExt as _;
use lancedb::Table;
use lancedb::query::{ExecutableQuery as _, QueryBase as _};
use std::future::Future;

/// Column holding the runner-reported model label in every vector table.
const MODEL_COLUMN: &str = "embedding_model";

/// One vector table taking part in the startup decision. `label` names
/// the table in errors and RPC output (`memory`, `thread`,
/// `reflection_intent`).
#[derive(Clone)]
pub struct VectorTable {
    pub label: &'static str,
    pub table: Table,
}

/// The state memories serves with after a successful decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceState {
    pub current: SpaceComponents,
    /// Set while a rebuild of this attempt is pending (rule 4).
    pub rebuilding_attempt: Option<String>,
    /// Records after applying the decision's writes, per table label.
    pub tables: Vec<(&'static str, TableRecord)>,
}

impl SpaceState {
    /// Token-less writes are accepted only when every table's record
    /// carries legacy acceptance and no rebuild is pending.
    pub fn accepts_legacy_writes(&self) -> bool {
        self.rebuilding_attempt.is_none()
            && !self.tables.is_empty()
            && self
                .tables
                .iter()
                .all(|(_, r)| r.space.as_ref().is_some_and(|s| s.legacy_accept))
    }
}

/// Errors are `StartupError` wrapped in `anyhow::Error` when the decision
/// stops startup, so the caller routes them through `fatal_anyhow`.
/// `chunking` is the current chunking fingerprint, pinned by a pending
/// rebuild.
pub async fn run<F, Fut>(
    tables: &[VectorTable],
    current: &SpaceComponents,
    chunking: &str,
    rdb_has_targets: F,
) -> anyhow::Result<SpaceState>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<bool>>,
{
    let observations = observe(tables).await?;
    let rdb = if needs_rdb_target_check(&observations) {
        rdb_has_targets().await?
    } else {
        false
    };
    match decide(&observations, current, rdb) {
        StartupDecision::Fail(e) => Err(anyhow::Error::new(e)),
        StartupDecision::StartRebuilding { attempt_id } => {
            let unpinned = pin_rebuild_chunking(&observations, &attempt_id, chunking)
                .map_err(anyhow::Error::new)?;
            let mut state = SpaceState {
                current: current.clone(),
                rebuilding_attempt: Some(attempt_id),
                tables: records(&observations),
            };
            for label in unpinned {
                let t = tables
                    .iter()
                    .find(|t| t.label == label)
                    .expect("only observed tables are pinned");
                write_rebuild_chunking(&t.table, Some(chunking)).await?;
                if let Some((_, r)) = state.tables.iter_mut().find(|(l, _)| *l == label) {
                    r.rebuild_chunking = Some(chunking.to_string());
                }
            }
            Ok(state)
        }
        StartupDecision::Start { writes } => {
            let mut state = SpaceState {
                current: current.clone(),
                rebuilding_attempt: None,
                tables: records(&observations),
            };
            for w in writes {
                let t = tables
                    .iter()
                    .find(|t| t.label == w.table)
                    .expect("decision only names observed tables");
                write_space_record(&t.table, &w.space).await?;
                tracing::info!(
                    table = t.label,
                    space_id = %w.space.space_id,
                    legacy_accept = w.space.legacy_accept,
                    "recorded embedding space"
                );
                if let Some((_, r)) = state.tables.iter_mut().find(|(l, _)| *l == t.label) {
                    r.space = Some(w.space);
                }
            }
            Ok(state)
        }
    }
}

fn records(observations: &[TableObservation]) -> Vec<(&'static str, TableRecord)> {
    observations
        .iter()
        .map(|o| (o.table, o.record.clone()))
        .collect()
}

pub async fn observe(tables: &[VectorTable]) -> anyhow::Result<Vec<TableObservation>> {
    let mut out = Vec::with_capacity(tables.len());
    for t in tables {
        let record = read_table_record(&t.table).await?;
        let is_empty = t.table.count_rows(None).await? == 0;
        let row_models = if record.space.is_none() && !is_empty {
            Some(scan_row_models(&t.table).await?)
        } else {
            None
        };
        out.push(TableObservation {
            table: t.label,
            record,
            is_empty,
            row_models,
        });
    }
    Ok(out)
}

/// Distinct model labels across all rows. Runs once per pre-existing
/// table (until its space is recorded), so a full column scan is fine.
pub async fn scan_row_models(table: &Table) -> anyhow::Result<RowModels> {
    let mut models = RowModels::default();
    let mut stream = table
        .query()
        .select(lancedb::query::Select::columns(&[MODEL_COLUMN]))
        .execute()
        .await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let Some(col) = batch
            .column_by_name(MODEL_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        else {
            anyhow::bail!("vector table has no {MODEL_COLUMN} string column");
        };
        for i in 0..col.len() {
            if col.is_null(i) || col.value(i).is_empty() {
                models.has_unlabeled = true;
            } else if !models.names.contains(col.value(i)) {
                models.names.insert(col.value(i).to_string());
            }
        }
    }
    Ok(models)
}

/// Convenience for callers that want the structured error, if any.
pub fn as_startup_error(e: &anyhow::Error) -> Option<&StartupError> {
    e.downcast_ref::<StartupError>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::vector_table::open_or_create;
    use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(MODEL_COLUMN, DataType::Utf8, true),
        ]))
    }

    fn space(model: &str) -> SpaceComponents {
        SpaceComponents {
            model_id: model.into(),
            tokenizer_model_id: String::new(),
            revision: "unversioned".into(),
            dimension: 4,
            distance: "cosine".into(),
        }
    }

    async fn table(dir: &tempfile::TempDir, rows: &[Option<&str>]) -> VectorTable {
        let uri = dir.path().to_string_lossy().to_string();
        let t = open_or_create(&uri, "t", &schema()).await.unwrap().table;
        if !rows.is_empty() {
            let batch = RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..rows.len() as i64)),
                    Arc::new(StringArray::from(rows.to_vec())),
                ],
            )
            .unwrap();
            let reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
                RecordBatchIterator::new(vec![Ok::<_, arrow_schema::ArrowError>(batch)], schema()),
            );
            t.add(reader).execute().await.unwrap();
        }
        VectorTable {
            label: "memory",
            table: t,
        }
    }

    fn code(e: &anyhow::Error) -> String {
        serde_json::to_value(as_startup_error(e).expect("structured error")).unwrap()["code"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn records_then_accepts_the_same_space() {
        let dir = tempfile::tempdir().unwrap();
        let t = table(&dir, &[]).await;
        let state = run(std::slice::from_ref(&t), &space("a"), "c", || async {
            Ok(true)
        })
        .await
        .unwrap();
        let rec = state.tables[0].1.space.clone().unwrap();
        assert_eq!(rec.space_id, space("a").space_id());
        assert!(!rec.legacy_accept);
        assert!(!state.accepts_legacy_writes());
        // Second start reads the persisted record (rule 6).
        let again = run(&[t], &space("a"), "c", || async { Ok(true) })
            .await
            .unwrap();
        assert_eq!(again.tables[0].1.space, Some(rec));
    }

    #[tokio::test]
    async fn changed_space_stops_unless_everything_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let t = table(&dir, &[]).await;
        run(std::slice::from_ref(&t), &space("a"), "c", || async {
            Ok(true)
        })
        .await
        .unwrap();
        let err = run(std::slice::from_ref(&t), &space("b"), "c", || async {
            Ok(true)
        })
        .await
        .unwrap_err();
        assert_eq!(code(&err), "embedding_space_mismatch");
        // Rule 3: nothing to lose, so the new space is adopted.
        let state = run(&[t], &space("b"), "c", || async { Ok(false) })
            .await
            .unwrap();
        assert_eq!(
            state.tables[0].1.space.as_ref().unwrap().space_id,
            space("b").space_id()
        );
    }

    #[tokio::test]
    async fn pre_existing_rows_with_one_model_get_legacy_acceptance() {
        let dir = tempfile::tempdir().unwrap();
        let t = table(&dir, &[Some("runner-model"), Some("runner-model")]).await;
        let rdb_called = std::sync::atomic::AtomicBool::new(false);
        let state = run(&[t], &space("a"), "c", || async {
            rdb_called.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(true)
        })
        .await
        .unwrap();
        assert!(state.accepts_legacy_writes());
        assert!(
            !rdb_called.load(std::sync::atomic::Ordering::SeqCst),
            "the RDB is only consulted when every table is empty"
        );
    }

    #[tokio::test]
    async fn pre_existing_rows_without_labels_are_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let t = table(&dir, &[Some("m"), None]).await;
        let err = run(std::slice::from_ref(&t), &space("a"), "c", || async {
            Ok(true)
        })
        .await
        .unwrap_err();
        assert_eq!(code(&err), "embedding_space_unknown");
        assert_eq!(
            scan_row_models(&t.table).await.unwrap(),
            RowModels {
                names: ["m".to_string()].into(),
                has_unlabeled: true
            }
        );
    }

    #[tokio::test]
    async fn markers_stop_startup() {
        use crate::infra::embedding_space::record::{MarkerState, MigrationMarker, write_marker};
        let dir = tempfile::tempdir().unwrap();
        let t = table(&dir, &[]).await;
        write_marker(
            &t.table,
            Some(&MigrationMarker {
                state: MarkerState::Switching,
                attempt_id: "att".into(),
            }),
        )
        .await
        .unwrap();
        let err = run(&[t], &space("a"), "c", || async { Ok(false) })
            .await
            .unwrap_err();
        assert_eq!(code(&err), "embedding_switch_incomplete");
    }

    #[tokio::test]
    async fn pending_rebuild_pins_chunking_and_stops_when_it_changes() {
        use crate::infra::embedding_space::record::{
            MarkerState, MigrationMarker, SpaceRecord, write_marker,
        };
        let dir = tempfile::tempdir().unwrap();
        let t = table(&dir, &[]).await;
        write_space_record(&t.table, &SpaceRecord::new(&space("a"), false))
            .await
            .unwrap();
        write_marker(
            &t.table,
            Some(&MigrationMarker {
                state: MarkerState::Pending,
                attempt_id: "att".into(),
            }),
        )
        .await
        .unwrap();
        let state = run(std::slice::from_ref(&t), &space("a"), "c1", || async {
            Ok(true)
        })
        .await
        .unwrap();
        assert_eq!(state.rebuilding_attempt.as_deref(), Some("att"));
        assert_eq!(state.tables[0].1.rebuild_chunking.as_deref(), Some("c1"));
        assert_eq!(
            read_table_record(&t.table)
                .await
                .unwrap()
                .rebuild_chunking
                .as_deref(),
            Some("c1")
        );
        // Restart with the same settings.
        run(std::slice::from_ref(&t), &space("a"), "c1", || async {
            Ok(true)
        })
        .await
        .unwrap();
        let err = run(&[t], &space("a"), "c2", || async { Ok(true) })
            .await
            .unwrap_err();
        assert_eq!(code(&err), "embedding_rebuild_config_changed");
    }
}
