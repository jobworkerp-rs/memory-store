//! `reflection_fact` child table. Stores the post-resolution facts
//! emitted by the LLM (kind = `OUTCOME_EVIDENCE` / `LESSON_SOURCE` /
//! ...) along with the JSON-encoded `links_json` array.
//!
//! `links_json` is kept as raw text at infra level so sqlite (TEXT)
//! and postgres (JSONB) can share the same row struct. The app layer
//! parses / serialises with `serde_json`.

use super::rows::ReflectionFactRow;
use crate::error::LlmMemoryError;
use crate::sql::{IN_LIST_CHUNK_SIZE, build_in_placeholders, p, p_jsonb};
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

#[cfg(feature = "postgres")]
const INSERT_SQL: &str = concat!(
    "INSERT INTO reflection_fact \
     (memory_id, fact_memory_id, fact_kind, turn_index, weight, note, links_json) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ",",
    p!(5),
    ",",
    p!(6),
    ",",
    p_jsonb!(7),
    ") ON CONFLICT (memory_id, fact_memory_id, fact_kind) DO NOTHING;"
);

#[cfg(not(feature = "postgres"))]
const INSERT_SQL: &str = concat!(
    "INSERT OR IGNORE INTO reflection_fact \
     (memory_id, fact_memory_id, fact_kind, turn_index, weight, note, links_json) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ",",
    p!(5),
    ",",
    p!(6),
    ",",
    p_jsonb!(7),
    ");"
);

// Postgres needs `links_json::text AS links_json` to coerce JSONB
// into the FromRow `String` mapping; sqlite stores JSON as TEXT and
// passes it through. The placeholder is unified via `p!`.
#[cfg(feature = "postgres")]
const LIST_BY_MEMORY_SQL: &str = concat!(
    "SELECT memory_id, fact_memory_id, fact_kind, turn_index, weight, note, \
     links_json::text AS links_json \
     FROM reflection_fact WHERE memory_id = ",
    p!(1),
    ";"
);

#[cfg(not(feature = "postgres"))]
const LIST_BY_MEMORY_SQL: &str = concat!(
    "SELECT memory_id, fact_memory_id, fact_kind, turn_index, weight, note, links_json \
     FROM reflection_fact WHERE memory_id = ",
    p!(1),
    ";"
);

const DELETE_BY_MEMORY_SQL: &str =
    concat!("DELETE FROM reflection_fact WHERE memory_id = ", p!(1), ";");

#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait ReflectionFactRepository: UseRdbPool + Send + Sync {
    /// `turn_index` is the LLM-original global turn position of the
    /// anchor; pass 0 when the workflow has no turn information.
    /// `links_json` is the serialised form of `[{field, index}, ...]`;
    /// pass `None` when the fact has no links.
    async fn insert_fact_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
        fact_memory_id: i64,
        fact_kind: i32,
        turn_index: i32,
        weight: Option<f64>,
        note: Option<&str>,
        links_json: Option<&str>,
    ) -> Result<()> {
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(memory_id)
            .bind(fact_memory_id)
            .bind(fact_kind)
            .bind(turn_index)
            .bind(weight)
            .bind(note)
            .bind(links_json)
            .execute(tx)
            .await
            .map(|_| ())
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    async fn list_by_memory_id(&self, memory_id: i64) -> Result<Vec<ReflectionFactRow>> {
        sqlx::query_as::<Rdb, ReflectionFactRow>(LIST_BY_MEMORY_SQL)
            .bind(memory_id)
            .fetch_all(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// Bulk variant. Postgres needs `links_json::text` so JSONB coerces
    /// to the FromRow `String` mapping — same cfg split as the
    /// per-row `LIST_BY_MEMORY_SQL` above.
    async fn list_by_memory_ids(&self, memory_ids: &[i64]) -> Result<Vec<ReflectionFactRow>> {
        if memory_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(memory_ids.len());
        for chunk in memory_ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = build_in_placeholders(chunk.len(), 1);
            #[cfg(feature = "postgres")]
            let sql = format!(
                "SELECT memory_id, fact_memory_id, fact_kind, turn_index, weight, note, \
                 links_json::text AS links_json \
                 FROM reflection_fact WHERE memory_id IN ({placeholders});"
            );
            #[cfg(not(feature = "postgres"))]
            let sql = format!(
                "SELECT memory_id, fact_memory_id, fact_kind, turn_index, weight, note, links_json \
                 FROM reflection_fact WHERE memory_id IN ({placeholders});"
            );
            let mut q = sqlx::query_as::<Rdb, ReflectionFactRow>(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                q = q.bind(id);
            }
            let rows = q
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?;
            out.extend(rows);
        }
        Ok(out)
    }

    async fn delete_by_memory_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
    ) -> Result<()> {
        sqlx::query::<Rdb>(DELETE_BY_MEMORY_SQL)
            .bind(memory_id)
            .execute(tx)
            .await
            .map(|_| ())
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }
}

pub struct ReflectionFactRepositoryImpl {
    pool: &'static RdbPool,
}

impl ReflectionFactRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ReflectionFactRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ReflectionFactRepository for ReflectionFactRepositoryImpl {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::reflection::rdb::ThreadReflectionIndexRepositoryImpl;
    use crate::infra::reflection::test_support::{cleanup_child_table, ensure_sidecar, setup_pool};
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn cleanup(pool: &RdbPool, memory_ids: &[i64]) {
        cleanup_child_table(pool, "reflection_fact", memory_ids).await;
    }

    async fn seed_with_anchors(
        pool: &'static RdbPool,
        memory_id: i64,
        anchor_ids: &[i64],
        links_json: Option<&str>,
    ) -> Result<()> {
        let index_repo = ThreadReflectionIndexRepositoryImpl::new(pool);
        let repo = ReflectionFactRepositoryImpl::new(pool);
        // Sidecars for the reflection itself and every fact anchor:
        // anchors are FK'd via `reflection_fact.fact_memory_id ->
        // thread_reflection_index.memory_id` (the schema mirrors this).
        ensure_sidecar(pool, &index_repo, memory_id).await?;
        for anchor_id in anchor_ids {
            ensure_sidecar(pool, &index_repo, *anchor_id).await?;
        }
        let mut tx = pool.begin().await?;
        for anchor_id in anchor_ids {
            repo.insert_fact_tx(
                &mut *tx, memory_id, *anchor_id, 1, 0, None, None, links_json,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    #[test]
    fn run_list_by_memory_ids_returns_rows_for_present_ids() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let ids = [9_004_001_i64, 9_004_002, 9_004_003];
            // Anchor sidecars used as fact_memory_id targets.
            let anchors = [9_004_101_i64, 9_004_102];
            let all_ids: Vec<i64> = ids.iter().chain(anchors.iter()).copied().collect();
            cleanup(pool, &all_ids).await;
            seed_with_anchors(pool, ids[0], &anchors, None).await?;
            seed_with_anchors(pool, ids[1], &anchors[..1], None).await?;
            seed_with_anchors(pool, ids[2], &[], None).await?;

            let repo = ReflectionFactRepositoryImpl::new(pool);
            let rows = repo.list_by_memory_ids(&ids).await?;
            assert_eq!(rows.len(), 3);
            let mut by_id: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
            for r in rows {
                *by_id.entry(r.memory_id).or_default() += 1;
            }
            assert_eq!(by_id.get(&ids[0]), Some(&2));
            assert_eq!(by_id.get(&ids[1]), Some(&1));
            assert!(!by_id.contains_key(&ids[2]));

            cleanup(pool, &all_ids).await;
            Ok(())
        })
    }

    #[test]
    fn run_list_by_memory_ids_empty_input_returns_empty() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let repo = ReflectionFactRepositoryImpl::new(pool);
            let rows = repo.list_by_memory_ids(&[]).await?;
            assert!(rows.is_empty());
            Ok(())
        })
    }

    /// links_json round-trip: the bulk variant has its own Postgres
    /// `::text` cast and we should make sure the JSON payload survives.
    #[test]
    fn run_list_by_memory_ids_preserves_links_json_payload() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let memory_id = 9_004_010_i64;
            let anchor = 9_004_111_i64;
            cleanup(pool, &[memory_id, anchor]).await;
            let payload = r#"[{"field":"lessons","index":0}]"#;
            seed_with_anchors(pool, memory_id, &[anchor], Some(payload)).await?;

            let repo = ReflectionFactRepositoryImpl::new(pool);
            let rows = repo.list_by_memory_ids(&[memory_id]).await?;
            assert_eq!(rows.len(), 1);
            let got = rows[0].links_json.as_deref().unwrap_or("");
            // Postgres JSONB normalises whitespace, so compare on the
            // parsed value rather than byte-for-byte.
            let got_v: serde_json::Value = serde_json::from_str(got)?;
            let expected_v: serde_json::Value = serde_json::from_str(payload)?;
            assert_eq!(got_v, expected_v);

            cleanup(pool, &[memory_id, anchor]).await;
            Ok(())
        })
    }
}
