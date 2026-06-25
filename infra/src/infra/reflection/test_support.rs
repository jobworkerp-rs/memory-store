//! Shared test fixtures for the reflection child-table repos. Sidecar
//! fixture + pool setup + per-table cleanup are identical across the
//! 6 child repos; child repos pass their own `&'static str` table name
//! to `cleanup_child_table` and delegate the sidecar delete here.

use super::rdb::{ThreadReflectionIndexRepository, ThreadReflectionIndexRepositoryImpl};
use super::rows::ThreadReflectionIndexRow;
use crate::sql::p;
use anyhow::Result;
use infra_utils::infra::rdb::{Rdb, RdbPool};
use infra_utils::infra::test::setup_test_rdb_from;

pub(crate) async fn setup_pool() -> &'static RdbPool {
    if cfg!(feature = "postgres") {
        setup_test_rdb_from("sql/postgres").await
    } else {
        setup_test_rdb_from("sql/sqlite").await
    }
}

pub(crate) fn fixture_sidecar(memory_id: i64) -> ThreadReflectionIndexRow {
    ThreadReflectionIndexRow {
        memory_id,
        thread_id: memory_id,
        origin_thread_id: memory_id,
        origin_user_id: 1,
        origin_channel: None,
        outcome: 1,
        score: 0.5,
        score_self: 0.5,
        score_heuristic: 0.5,
        task_category: 1,
        reflection_aspect: 1,
        dataset_quality: 1,
        summary_embedding_status: 1,
        summary_embedding_error: None,
        intent_embedding_status: 1,
        intent_embedding_error: None,
        prompt_version: "v1".to_string(),
        target_model_version: None,
        experiment_id: None,
        experiment_variant: None,
        previous_reflection_id: None,
        pinned: false,
        is_recurrence: false,
        mitigation_fingerprint: None,
        created_at: 1_700_000_000_000,
        updated_at: 1_700_000_000_000,
    }
}

/// Deletes only `thread_reflection_index` rows for the given memory ids.
pub(crate) async fn cleanup_sidecar(pool: &RdbPool, memory_ids: &[i64]) {
    for id in memory_ids {
        let _ = sqlx::query::<Rdb>(concat!(
            "DELETE FROM thread_reflection_index WHERE memory_id = ",
            p!(1)
        ))
        .bind(id)
        .execute(pool)
        .await;
    }
}

/// Deletes rows from the given child table and from the sidecar.
/// `table` must be a hard-coded `&'static str` (the reflection child
/// tables are an enumerable set known at compile time) — there is no
/// SQL-injection guard because every caller passes a literal.
pub(crate) async fn cleanup_child_table(pool: &RdbPool, table: &str, memory_ids: &[i64]) {
    let sql = format!("DELETE FROM {table} WHERE memory_id = {}", p!(1));
    for id in memory_ids {
        let _ = sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql.clone()))
            .bind(id)
            .execute(pool)
            .await;
    }
    cleanup_sidecar(pool, memory_ids).await;
}

/// Idempotent sidecar insert: silently skip when the sidecar already
/// exists. `insert_index_tx` is a raw `INSERT`, so duplicates blow up
/// UNIQUE without this guard.
pub(crate) async fn ensure_sidecar(
    pool: &RdbPool,
    index_repo: &ThreadReflectionIndexRepositoryImpl,
    memory_id: i64,
) -> Result<()> {
    if index_repo.find_by_memory_id(memory_id).await?.is_some() {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    index_repo
        .insert_index_tx(&mut *tx, &fixture_sidecar(memory_id))
        .await?;
    tx.commit().await?;
    Ok(())
}
