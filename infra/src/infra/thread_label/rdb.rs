use crate::sql::{IN_LIST_CHUNK_SIZE, build_in_placeholders, dyn_placeholder, p};
use anyhow::{Context, Result};
use async_trait::async_trait;
use infra_utils::infra::rdb::Rdb;
use infra_utils::infra::rdb::RdbPool;
use infra_utils::infra::rdb::UseRdbPool;
use sqlx::Executor;

use super::rows::{LabelWithCountRow, ThreadLabelRow};

// Idempotent insert: silently skip duplicates
#[cfg(feature = "postgres")]
const INSERT_OR_IGNORE_SQL: &str = concat!(
    "INSERT INTO thread_label (thread_id, label, created_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ") ON CONFLICT (thread_id, label) DO NOTHING"
);

#[cfg(not(feature = "postgres"))]
const INSERT_OR_IGNORE_SQL: &str = concat!(
    "INSERT OR IGNORE INTO thread_label (thread_id, label, created_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ")"
);

const FIND_LABELS_BY_THREAD_SQL: &str = concat!(
    "SELECT thread_id, label, created_at FROM thread_label WHERE thread_id = ",
    p!(1),
    " ORDER BY label"
);

const DELETE_BY_THREAD_SQL: &str = concat!("DELETE FROM thread_label WHERE thread_id = ", p!(1));

use crate::sql::escape_like;

/// (P9) Build a thread time-range WHERE-fragment and the matching bind values.
///
/// Convention matches `FindThreadListByLabels` (P8): `*_after` is strict
/// (`>`) and `*_before` is inclusive (`<=`). The fragment is empty when no
/// bound is set; otherwise it always starts with ` AND ` so callers can
/// concatenate it directly after an existing WHERE predicate without
/// worrying about WHERE/AND switching.
///
/// `qualifier` is the SQL alias for the `thread` table in the caller's
/// query (e.g. `t` or `t_inner`) so the helper works for both top-level
/// WHERE clauses and the `find_co_occurring_labels` subquery.
///
/// `param_offset` is the first free 1-indexed placeholder number; the
/// returned `next_offset` is the *next* free number after this filter
/// (callers assign instead of incrementing, which avoids drift bugs).
struct ThreadTimeFilter {
    sql: String,
    binds: Vec<i64>,
    next_offset: usize,
}

fn build_thread_time_filter(
    qualifier: &str,
    created_after: Option<i64>,
    created_before: Option<i64>,
    updated_after: Option<i64>,
    updated_before: Option<i64>,
    param_offset: usize,
) -> ThreadTimeFilter {
    let mut sql = String::new();
    let mut binds: Vec<i64> = Vec::new();
    let mut next = param_offset;
    let pairs: [(&str, Option<i64>); 4] = [
        ("created_at >", created_after),
        ("created_at <=", created_before),
        ("updated_at >", updated_after),
        ("updated_at <=", updated_before),
    ];
    for (op, v) in pairs {
        if let Some(value) = v {
            sql.push_str(&format!(" AND {qualifier}.{op} {}", dyn_placeholder(next)));
            binds.push(value);
            next += 1;
        }
    }
    ThreadTimeFilter {
        sql,
        binds,
        next_offset: next,
    }
}

#[async_trait]
pub trait ThreadLabelRepository: UseRdbPool + Sync + Send {
    /// Add labels to a thread (idempotent — duplicates silently skipped).
    async fn add_labels(&self, thread_id: i64, labels: &[String], created_at: i64) -> Result<()> {
        let pool = self.db_pool();
        for label in labels {
            sqlx::query(INSERT_OR_IGNORE_SQL)
                .bind(thread_id)
                .bind(label)
                .bind(created_at)
                .execute(pool)
                .await
                .context("add_labels: insert")?;
        }
        Ok(())
    }

    /// Transaction-aware variant of `add_labels`.
    /// Caller must pass `&mut *tx` for each call since Executor is consumed.
    async fn add_labels_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
        label: &str,
        created_at: i64,
    ) -> Result<()> {
        sqlx::query(INSERT_OR_IGNORE_SQL)
            .bind(thread_id)
            .bind(label)
            .bind(created_at)
            .execute(tx)
            .await
            .context("add_labels_tx: insert")?;
        Ok(())
    }

    /// Remove labels from a thread (idempotent — missing labels silently ignored).
    async fn remove_labels(&self, thread_id: i64, labels: &[String]) -> Result<u64> {
        self.remove_labels_tx(self.db_pool(), thread_id, labels)
            .await
    }

    /// Transaction-aware variant of `remove_labels`.
    async fn remove_labels_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
        labels: &[String],
    ) -> Result<u64> {
        if labels.is_empty() {
            return Ok(0);
        }
        let placeholders = build_in_placeholders(labels.len(), 2);
        let sql = format!(
            "DELETE FROM thread_label WHERE thread_id = {} AND label IN ({})",
            p!(1),
            placeholders
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(thread_id);
        for label in labels {
            query = query.bind(label);
        }
        let result = query
            .execute(tx)
            .await
            .context("remove_labels_tx: delete")?;
        Ok(result.rows_affected())
    }

    /// Get all labels for a thread, ordered alphabetically.
    async fn find_labels_by_thread_tx<'c, E: Executor<'c, Database = Rdb> + 'c>(
        &self,
        tx: E,
        thread_id: i64,
    ) -> Result<Vec<String>> {
        let rows: Vec<ThreadLabelRow> = sqlx::query_as(FIND_LABELS_BY_THREAD_SQL)
            .bind(thread_id)
            .fetch_all(tx)
            .await
            .context("find_labels_by_thread_tx")?;
        Ok(rows.into_iter().map(|r| r.label).collect())
    }

    /// Get labels for a thread (pool-based convenience).
    async fn find_labels_by_thread(&self, thread_id: i64) -> Result<Vec<String>> {
        self.find_labels_by_thread_tx(self.db_pool(), thread_id)
            .await
    }

    /// Batch-get labels for multiple threads. Returns (thread_id, labels) pairs.
    async fn find_labels_by_thread_ids(&self, thread_ids: &[i64]) -> Result<Vec<ThreadLabelRow>> {
        if thread_ids.is_empty() {
            return Ok(vec![]);
        }
        let mut all_rows = Vec::with_capacity(thread_ids.len());
        // Chunk to avoid SQLite parameter limits
        for chunk in thread_ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT thread_id, label, created_at FROM thread_label WHERE thread_id IN ({}) ORDER BY thread_id, label",
                placeholders
            );
            let mut query = sqlx::query_as::<_, ThreadLabelRow>(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                query = query.bind(id);
            }
            let rows = query
                .fetch_all(self.db_pool())
                .await
                .context("find_labels_by_thread_ids")?;
            all_rows.extend(rows);
        }
        Ok(all_rows)
    }

    /// Find thread IDs that have the specified labels (ANY or ALL mode).
    async fn find_thread_ids_by_labels(
        &self,
        labels: &[String],
        match_all: bool,
        user_id: Option<i64>,
        limit: Option<i32>,
        offset: Option<i64>,
    ) -> Result<Vec<i64>> {
        if labels.is_empty() {
            return Ok(vec![]);
        }
        // Deduplicate to prevent HAVING COUNT(DISTINCT) mismatch with labels.len()
        let labels: Vec<String> = labels
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();

        let label_placeholders = build_in_placeholders(labels.len(), 1);
        let mut param_offset = 1 + labels.len();

        // JOIN thread to exclude orphan labels from deleted threads (no FK)
        let user_filter = if user_id.is_some() {
            param_offset += 1;
            #[cfg(feature = "postgres")]
            let f = format!(" AND t.user_id = ${}", param_offset - 1);
            #[cfg(not(feature = "postgres"))]
            let f = " AND t.user_id = ?".to_string();
            f
        } else {
            String::new()
        };

        // `tl.thread_id DESC` is a stable tiebreaker so pages stay
        // deterministic when several threads share the same MAX(updated_at).
        // Without it, the app-level fast path (which trusts this SQL's
        // order verbatim) and `find_thread_list_by_user_id`'s
        // `updated_at DESC, id DESC` would disagree on tied rows and
        // shuffle pagination boundaries.
        let sql = if match_all {
            let label_count = labels.len();
            format!(
                "SELECT tl.thread_id FROM thread_label tl \
                 JOIN thread t ON tl.thread_id = t.id \
                 WHERE tl.label IN ({label_placeholders}){user_filter} \
                 GROUP BY tl.thread_id \
                 HAVING COUNT(DISTINCT tl.label) = {label_count} \
                 ORDER BY MAX(t.updated_at) DESC, tl.thread_id DESC \
                 LIMIT {} OFFSET {}",
                build_in_placeholders(1, param_offset),
                build_in_placeholders(1, param_offset + 1),
            )
        } else {
            format!(
                "SELECT tl.thread_id FROM thread_label tl \
                 JOIN thread t ON tl.thread_id = t.id \
                 WHERE tl.label IN ({label_placeholders}){user_filter} \
                 GROUP BY tl.thread_id \
                 ORDER BY MAX(t.updated_at) DESC, tl.thread_id DESC \
                 LIMIT {} OFFSET {}",
                build_in_placeholders(1, param_offset),
                build_in_placeholders(1, param_offset + 1),
            )
        };

        let mut query = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql));
        for label in labels {
            query = query.bind(label);
        }
        if let Some(uid) = user_id {
            query = query.bind(uid);
        }
        query = query.bind(limit.unwrap_or(100) as i64);
        query = query.bind(offset.unwrap_or(0));

        query
            .fetch_all(self.db_pool())
            .await
            .context("find_thread_ids_by_labels")
    }

    /// Get distinct labels with usage count.
    ///
    /// (P9) `created_*` / `updated_*` (epoch ms) optionally filter the
    /// underlying threads before label aggregation. Lower bounds are
    /// strict (`>`) and upper bounds inclusive (`<=`), matching the P8
    /// `FindThreadListByLabels` convention.
    #[allow(clippy::too_many_arguments)]
    async fn find_distinct_labels(
        &self,
        user_id: Option<i64>,
        limit: Option<i32>,
        offset: Option<i64>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        updated_after: Option<i64>,
        updated_before: Option<i64>,
    ) -> Result<Vec<LabelWithCountRow>> {
        let mut next_param: usize = 1;
        let user_filter = if user_id.is_some() {
            let s = format!(" AND t.user_id = {}", dyn_placeholder(next_param));
            next_param += 1;
            s
        } else {
            String::new()
        };
        let time = build_thread_time_filter(
            "t",
            created_after,
            created_before,
            updated_after,
            updated_before,
            next_param,
        );
        next_param = time.next_offset;
        // Both `user_filter` and `time.sql` start with " AND " (or are
        // empty); collapse the leading " AND " into "WHERE " so the
        // grammar is right whether or not any optional predicate fired.
        let combined = format!("{user_filter}{}", time.sql);
        let where_clause = combined
            .strip_prefix(" AND ")
            .map(|rest| format!("WHERE {rest} "))
            .unwrap_or_default();

        let limit_ph = dyn_placeholder(next_param);
        let offset_ph = dyn_placeholder(next_param + 1);

        let sql = format!(
            "SELECT tl.label, COUNT(DISTINCT tl.thread_id) AS thread_count \
             FROM thread_label tl \
             JOIN thread t ON tl.thread_id = t.id \
             {where_clause}\
             GROUP BY tl.label \
             ORDER BY tl.label \
             LIMIT {limit_ph} OFFSET {offset_ph}"
        );

        let mut query = sqlx::query_as::<_, LabelWithCountRow>(sqlx::AssertSqlSafe(sql));
        if let Some(uid) = user_id {
            query = query.bind(uid);
        }
        for v in &time.binds {
            query = query.bind(v);
        }
        query = query.bind(limit.unwrap_or(100) as i64);
        query = query.bind(offset.unwrap_or(0));

        query
            .fetch_all(self.db_pool())
            .await
            .context("find_distinct_labels")
    }

    /// Search labels by substring (case-insensitive LIKE) for suggestion/autocomplete.
    ///
    /// (P9) Same time-range parameters and semantics as `find_distinct_labels`.
    #[allow(clippy::too_many_arguments)]
    async fn search_labels(
        &self,
        query_str: &str,
        user_id: Option<i64>,
        limit: Option<i32>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        updated_after: Option<i64>,
        updated_before: Option<i64>,
    ) -> Result<Vec<LabelWithCountRow>> {
        let escaped = escape_like(query_str);
        let pattern = format!("%{escaped}%");

        // $1 is the LIKE pattern; subsequent placeholders claim $2.. in
        // bind order (user_id → time bounds → limit).
        let mut next_param: usize = 2;
        let user_filter = if user_id.is_some() {
            let s = format!(" AND t.user_id = {}", dyn_placeholder(next_param));
            next_param += 1;
            s
        } else {
            String::new()
        };
        let time = build_thread_time_filter(
            "t",
            created_after,
            created_before,
            updated_after,
            updated_before,
            next_param,
        );
        next_param = time.next_offset;
        let limit_ph = dyn_placeholder(next_param);

        let sql = format!(
            "SELECT tl.label, COUNT(DISTINCT tl.thread_id) AS thread_count \
             FROM thread_label tl \
             JOIN thread t ON tl.thread_id = t.id \
             WHERE LOWER(tl.label) LIKE LOWER({}) ESCAPE '\\'{user_filter}{} \
             GROUP BY tl.label \
             ORDER BY thread_count DESC, tl.label \
             LIMIT {limit_ph}",
            p!(1),
            time.sql
        );

        let mut query = sqlx::query_as::<_, LabelWithCountRow>(sqlx::AssertSqlSafe(sql));
        query = query.bind(&pattern);
        if let Some(uid) = user_id {
            query = query.bind(uid);
        }
        for v in &time.binds {
            query = query.bind(v);
        }
        query = query.bind(limit.unwrap_or(20) as i64);

        query
            .fetch_all(self.db_pool())
            .await
            .context("search_labels")
    }

    /// Find co-occurring labels: labels on threads that share all specified labels.
    ///
    /// (P9) Optional time-range filters apply to the **inner** subquery
    /// (i.e. the population of threads that match all `labels`). The
    /// `thread_count` returned therefore reflects the filtered population,
    /// matching the spec contract that aggregation runs on the filtered set.
    #[allow(clippy::too_many_arguments)]
    async fn find_co_occurring_labels(
        &self,
        labels: &[String],
        user_id: Option<i64>,
        limit: Option<i32>,
        offset: Option<i64>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        updated_after: Option<i64>,
        updated_before: Option<i64>,
    ) -> Result<Vec<LabelWithCountRow>> {
        if labels.is_empty() {
            return Ok(vec![]);
        }
        // Deduplicate to prevent HAVING COUNT(DISTINCT) mismatch with label_count
        let labels: Vec<String> = labels
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();

        let label_count = labels.len();
        let inner_placeholders = build_in_placeholders(label_count, 1);
        let mut next_param = 1 + label_count;

        // Time filters apply to the inner subquery (the candidate
        // population), so co-occurrence aggregation runs over the
        // filtered set per spec.
        let user_filter = if user_id.is_some() {
            let s = format!(" AND t_inner.user_id = {}", dyn_placeholder(next_param));
            next_param += 1;
            s
        } else {
            String::new()
        };
        let time = build_thread_time_filter(
            "t_inner",
            created_after,
            created_before,
            updated_after,
            updated_before,
            next_param,
        );
        next_param = time.next_offset;

        // NOT IN exclusion reuses `labels` but lives outside the subquery
        // so it claims a fresh placeholder range.
        let exclude_placeholders = build_in_placeholders(label_count, next_param);
        next_param += label_count;
        let limit_ph = dyn_placeholder(next_param);
        let offset_ph = dyn_placeholder(next_param + 1);

        let sql = format!(
            "SELECT tl2.label, COUNT(DISTINCT tl2.thread_id) AS thread_count \
             FROM thread_label tl2 \
             JOIN thread t_outer ON tl2.thread_id = t_outer.id \
             WHERE tl2.thread_id IN ( \
                 SELECT tl_inner.thread_id FROM thread_label tl_inner \
                 JOIN thread t_inner ON tl_inner.thread_id = t_inner.id \
                 WHERE tl_inner.label IN ({inner_placeholders}){user_filter}{} \
                 GROUP BY tl_inner.thread_id \
                 HAVING COUNT(DISTINCT tl_inner.label) = {label_count} \
             ) \
             AND tl2.label NOT IN ({exclude_placeholders}) \
             GROUP BY tl2.label \
             ORDER BY thread_count DESC, tl2.label \
             LIMIT {limit_ph} OFFSET {offset_ph}",
            time.sql
        );

        let mut query = sqlx::query_as::<_, LabelWithCountRow>(sqlx::AssertSqlSafe(sql));
        for label in &labels {
            query = query.bind(label);
        }
        if let Some(uid) = user_id {
            query = query.bind(uid);
        }
        for v in &time.binds {
            query = query.bind(v);
        }
        for label in &labels {
            query = query.bind(label);
        }
        query = query.bind(limit.unwrap_or(20) as i64);
        query = query.bind(offset.unwrap_or(0));

        query
            .fetch_all(self.db_pool())
            .await
            .context("find_co_occurring_labels")
    }

    /// Delete all labels for a thread (used in cascade delete).
    async fn delete_by_thread_tx<'c, E: Executor<'c, Database = Rdb> + 'c>(
        &self,
        tx: E,
        thread_id: i64,
    ) -> Result<u64> {
        let result = sqlx::query(DELETE_BY_THREAD_SQL)
            .bind(thread_id)
            .execute(tx)
            .await
            .context("delete_by_thread_tx")?;
        Ok(result.rows_affected())
    }
}

pub struct ThreadLabelRepositoryImpl {
    pool: &'static RdbPool,
}

impl ThreadLabelRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ThreadLabelRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ThreadLabelRepository for ThreadLabelRepositoryImpl {}

/// Capability trait so app-layer types can declare access to the
/// `ThreadLabelRepositoryImpl` (mirrors `UseThreadRepository` etc.).
/// Added in P8 because `MemoryAppImpl` now needs to plumb the labels
/// route of the thread_filter resolver.
pub trait UseThreadLabelRepository {
    fn thread_label_repository(&self) -> &ThreadLabelRepositoryImpl;
}

#[cfg(test)]
#[cfg(not(feature = "postgres"))]
mod test {
    use super::*;
    use crate::infra::thread::rdb::{ThreadRepository, ThreadRepositoryImpl};
    use anyhow::Context;
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::test::{TEST_RUNTIME, setup_test_rdb_from};
    use protobuf::llm_memory::data::{ThreadData, UserId};

    fn create_test_thread_data(user_id: i64) -> ThreadData {
        ThreadData {
            default_system_memory_id: None,
            user_id: Some(UserId { value: user_id }),
            description: Some("label test thread".to_string()),
            channel: None,
            embedding: None,
            embedding_dim: None,
            created_at: 0,
            updated_at: 0,
            labels: vec![],
            metadata: None,
        }
    }

    async fn _test_label_crud(pool: &'static RdbPool) -> Result<()> {
        let id_gen = crate::test_helper::shared_id_generator();
        let thread_repo = ThreadRepositoryImpl::new(id_gen, pool);
        let label_repo = ThreadLabelRepositoryImpl::new(pool);
        let db = label_repo.db_pool();

        // Create a thread
        let data = create_test_thread_data(1);
        let mut tx = db.begin().await.context("begin")?;
        let thread_id = thread_repo.create(&mut *tx, &data).await?;
        tx.commit().await.context("commit thread")?;

        // Add labels
        let now = command_utils::util::datetime::now_millis();
        let labels = vec!["rust".to_string(), "async".to_string(), "tokio".to_string()];
        label_repo.add_labels(thread_id.value, &labels, now).await?;

        // Find labels
        let found = label_repo.find_labels_by_thread(thread_id.value).await?;
        assert_eq!(found, vec!["async", "rust", "tokio"]); // alphabetical

        // Idempotent add
        label_repo
            .add_labels(thread_id.value, &["rust".to_string()], now)
            .await?;
        let found = label_repo.find_labels_by_thread(thread_id.value).await?;
        assert_eq!(found.len(), 3); // still 3

        // Remove one label
        let removed = label_repo
            .remove_labels(thread_id.value, &["async".to_string()])
            .await?;
        assert_eq!(removed, 1);
        let found = label_repo.find_labels_by_thread(thread_id.value).await?;
        assert_eq!(found, vec!["rust", "tokio"]);

        // Remove non-existent label (idempotent)
        let removed = label_repo
            .remove_labels(thread_id.value, &["nonexistent".to_string()])
            .await?;
        assert_eq!(removed, 0);

        // Cascade delete
        let mut tx = db.begin().await.context("begin")?;
        let deleted = label_repo
            .delete_by_thread_tx(&mut *tx, thread_id.value)
            .await?;
        tx.commit().await.context("commit cascade")?;
        assert_eq!(deleted, 2);
        let found = label_repo.find_labels_by_thread(thread_id.value).await?;
        assert!(found.is_empty());

        // Cleanup
        thread_repo.delete(&thread_id).await?;
        Ok(())
    }

    async fn _test_find_by_labels_and_distinct(pool: &'static RdbPool) -> Result<()> {
        let id_gen = crate::test_helper::shared_id_generator();
        let thread_repo = ThreadRepositoryImpl::new(id_gen, pool);
        let label_repo = ThreadLabelRepositoryImpl::new(pool);
        let db = label_repo.db_pool();
        let now = command_utils::util::datetime::now_millis();

        // Create threads
        let data1 = create_test_thread_data(10);
        let data2 = create_test_thread_data(10);
        let mut tx = db.begin().await?;
        let t1 = thread_repo.create(&mut *tx, &data1).await?;
        tx.commit().await?;
        let mut tx = db.begin().await?;
        let t2 = thread_repo.create(&mut *tx, &data2).await?;
        tx.commit().await?;

        // Assign labels
        label_repo
            .add_labels(
                t1.value,
                &["project-a".to_string(), "rust".to_string()],
                now,
            )
            .await?;

        label_repo
            .add_labels(
                t2.value,
                &["project-a".to_string(), "python".to_string()],
                now,
            )
            .await?;

        // Find by labels (ANY)
        let ids = label_repo
            .find_thread_ids_by_labels(&["rust".to_string()], false, None, None, None)
            .await?;
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], t1.value);

        // Find by labels (ANY) with shared label
        let ids = label_repo
            .find_thread_ids_by_labels(&["project-a".to_string()], false, None, None, None)
            .await?;
        assert_eq!(ids.len(), 2);

        // Find by labels (ALL)
        let ids = label_repo
            .find_thread_ids_by_labels(
                &["project-a".to_string(), "rust".to_string()],
                true,
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], t1.value);

        // Distinct labels
        let distinct = label_repo
            .find_distinct_labels(None, None, None, None, None, None, None)
            .await?;
        assert!(distinct.len() >= 3);

        // Search labels
        let results = label_repo
            .search_labels("proj", None, None, None, None, None, None)
            .await?;
        assert!(results.iter().any(|r| r.label == "project-a"));

        // Co-occurring labels for "project-a"
        let co = label_repo
            .find_co_occurring_labels(
                &["project-a".to_string()],
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;
        assert!(co.iter().any(|r| r.label == "rust"));
        assert!(co.iter().any(|r| r.label == "python"));
        // "project-a" itself should be excluded
        assert!(!co.iter().any(|r| r.label == "project-a"));

        // Verify sort order: results are ordered by updated_at DESC.
        // Update t1 to have a newer updated_at so it comes first.
        let mut tx = db.begin().await?;
        thread_repo
            .update_updated_at_tx(&mut *tx, &t1, now + 1000)
            .await?;
        tx.commit().await?;

        let ids = label_repo
            .find_thread_ids_by_labels(&["project-a".to_string()], false, None, None, None)
            .await?;
        assert_eq!(ids.len(), 2);
        assert_eq!(
            ids[0], t1.value,
            "most recently updated thread should come first"
        );

        // Now update t2 to be even newer
        let mut tx = db.begin().await?;
        thread_repo
            .update_updated_at_tx(&mut *tx, &t2, now + 2000)
            .await?;
        tx.commit().await?;

        let ids = label_repo
            .find_thread_ids_by_labels(&["project-a".to_string()], false, None, None, None)
            .await?;
        assert_eq!(
            ids[0], t2.value,
            "t2 now has newer updated_at and should come first"
        );

        // Also verify match_all respects updated_at DESC order
        let ids = label_repo
            .find_thread_ids_by_labels(
                &["project-a".to_string(), "rust".to_string()],
                true,
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], t1.value);

        // Cleanup
        thread_repo.delete(&t1).await?;
        thread_repo.delete(&t2).await?;
        // Labels are orphaned now (no FK); queries JOIN thread to exclude them
        Ok(())
    }

    async fn _test_batch_labels(pool: &'static RdbPool) -> Result<()> {
        let id_gen = crate::test_helper::shared_id_generator();
        let thread_repo = ThreadRepositoryImpl::new(id_gen, pool);
        let label_repo = ThreadLabelRepositoryImpl::new(pool);
        let db = label_repo.db_pool();
        let now = command_utils::util::datetime::now_millis();

        let data = create_test_thread_data(20);
        let mut tx = db.begin().await?;
        let t1 = thread_repo.create(&mut *tx, &data).await?;
        tx.commit().await?;
        let mut tx = db.begin().await?;
        let t2 = thread_repo.create(&mut *tx, &data).await?;
        tx.commit().await?;

        label_repo
            .add_labels(t1.value, &["a".to_string(), "b".to_string()], now)
            .await?;
        label_repo
            .add_labels(t2.value, &["b".to_string(), "c".to_string()], now)
            .await?;

        // Batch get
        let rows = label_repo
            .find_labels_by_thread_ids(&[t1.value, t2.value])
            .await?;
        let t1_labels: Vec<_> = rows
            .iter()
            .filter(|r| r.thread_id == t1.value)
            .map(|r| r.label.as_str())
            .collect();
        let t2_labels: Vec<_> = rows
            .iter()
            .filter(|r| r.thread_id == t2.value)
            .map(|r| r.label.as_str())
            .collect();
        assert_eq!(t1_labels, vec!["a", "b"]);
        assert_eq!(t2_labels, vec!["b", "c"]);

        // Cleanup
        thread_repo.delete(&t1).await?;
        thread_repo.delete(&t2).await?;
        Ok(())
    }

    async fn _test_sort_stability_and_paging(pool: &'static RdbPool) -> Result<()> {
        let id_gen = crate::test_helper::shared_id_generator();
        let thread_repo = ThreadRepositoryImpl::new(id_gen, pool);
        let label_repo = ThreadLabelRepositoryImpl::new(pool);
        let db = label_repo.db_pool();
        let now = command_utils::util::datetime::now_millis();

        // Create 3 threads with distinct updated_at values
        let data = create_test_thread_data(30);
        let mut tx = db.begin().await?;
        let t1 = thread_repo.create(&mut *tx, &data).await?;
        tx.commit().await?;
        let mut tx = db.begin().await?;
        let t2 = thread_repo.create(&mut *tx, &data).await?;
        tx.commit().await?;
        let mut tx = db.begin().await?;
        let t3 = thread_repo.create(&mut *tx, &data).await?;
        tx.commit().await?;

        // Set distinct updated_at: t2 newest, t3 middle, t1 oldest
        for (tid, ts) in [(&t1, now + 1000), (&t3, now + 2000), (&t2, now + 3000)] {
            let mut tx = db.begin().await?;
            thread_repo.update_updated_at_tx(&mut *tx, tid, ts).await?;
            tx.commit().await?;
        }

        // Assign a shared label
        for tid in [&t1, &t2, &t3] {
            label_repo
                .add_labels(tid.value, &["shared".to_string()], now)
                .await?;
        }

        // Verify updated_at DESC ordering
        let ids = label_repo
            .find_thread_ids_by_labels(&["shared".to_string()], false, None, None, None)
            .await?;
        assert_eq!(ids, vec![t2.value, t3.value, t1.value]);

        // Paging: limit=2, offset=0 should return the first 2
        let page1 = label_repo
            .find_thread_ids_by_labels(&["shared".to_string()], false, None, Some(2), Some(0))
            .await?;
        assert_eq!(page1, vec![t2.value, t3.value]);

        // Paging: limit=2, offset=2 should return the last 1
        let page2 = label_repo
            .find_thread_ids_by_labels(&["shared".to_string()], false, None, Some(2), Some(2))
            .await?;
        assert_eq!(page2, vec![t1.value]);

        // Pages must not overlap
        assert!(
            !page1.contains(&page2[0]),
            "pages must not contain duplicate entries"
        );

        // Paging with match_all mode
        for tid in [&t1, &t2, &t3] {
            label_repo
                .add_labels(tid.value, &["extra".to_string()], now)
                .await?;
        }

        let all_page1 = label_repo
            .find_thread_ids_by_labels(
                &["shared".to_string(), "extra".to_string()],
                true,
                None,
                Some(2),
                Some(0),
            )
            .await?;
        let all_page2 = label_repo
            .find_thread_ids_by_labels(
                &["shared".to_string(), "extra".to_string()],
                true,
                None,
                Some(2),
                Some(2),
            )
            .await?;
        assert_eq!(all_page1, vec![t2.value, t3.value]);
        assert_eq!(all_page2, vec![t1.value]);

        // Cleanup
        thread_repo.delete(&t1).await?;
        thread_repo.delete(&t2).await?;
        thread_repo.delete(&t3).await?;
        Ok(())
    }

    /// (P9) Helper to create a thread with a fixed `created_at` and
    /// optionally bump its `updated_at` afterwards. Bypasses the
    /// `fill_timestamps` server-side default by supplying a non-zero
    /// `created_at` directly.
    async fn create_thread_with_timestamps(
        thread_repo: &ThreadRepositoryImpl,
        pool: &'static RdbPool,
        user_id: i64,
        created_at: i64,
        updated_at: i64,
    ) -> Result<protobuf::llm_memory::data::ThreadId> {
        let mut data = create_test_thread_data(user_id);
        data.created_at = created_at;
        // updated_at gets overwritten to `now` on insert when 0; pass the
        // intended value so the initial row is correct, then call
        // `update_updated_at_tx` afterwards if the test needs to diverge
        // from `created_at`.
        data.updated_at = if updated_at == 0 {
            created_at
        } else {
            updated_at
        };
        let mut tx = pool.begin().await.context("begin")?;
        let id = thread_repo.create(&mut *tx, &data).await?;
        tx.commit().await.context("commit thread")?;
        Ok(id)
    }

    async fn _test_find_distinct_labels_with_time_filter(pool: &'static RdbPool) -> Result<()> {
        let id_gen = crate::test_helper::shared_id_generator();
        let thread_repo = ThreadRepositoryImpl::new(id_gen, pool);
        let label_repo = ThreadLabelRepositoryImpl::new(pool);
        let now = command_utils::util::datetime::now_millis();

        // Three distinct created_at buckets: old, mid, new.
        let t_old =
            create_thread_with_timestamps(&thread_repo, pool, 100, now - 30_000, now - 30_000)
                .await?;
        let t_mid =
            create_thread_with_timestamps(&thread_repo, pool, 100, now - 20_000, now - 20_000)
                .await?;
        let t_new =
            create_thread_with_timestamps(&thread_repo, pool, 100, now - 10_000, now - 10_000)
                .await?;

        label_repo
            .add_labels(
                t_old.value,
                &["old_only".to_string(), "shared".to_string()],
                now,
            )
            .await?;
        label_repo
            .add_labels(
                t_mid.value,
                &["mid_only".to_string(), "shared".to_string()],
                now,
            )
            .await?;
        label_repo
            .add_labels(
                t_new.value,
                &["new_only".to_string(), "shared".to_string()],
                now,
            )
            .await?;

        // No filter — all 4 distinct labels are present.
        let all = label_repo
            .find_distinct_labels(Some(100), None, None, None, None, None, None)
            .await?;
        let names: Vec<&str> = all.iter().map(|r| r.label.as_str()).collect();
        assert!(names.contains(&"old_only"));
        assert!(names.contains(&"mid_only"));
        assert!(names.contains(&"new_only"));
        assert!(names.contains(&"shared"));

        // created_after = now - 25_000 (strict `>`): excludes t_old.
        let mid_and_newer = label_repo
            .find_distinct_labels(Some(100), None, None, Some(now - 25_000), None, None, None)
            .await?;
        let names: Vec<&str> = mid_and_newer.iter().map(|r| r.label.as_str()).collect();
        assert!(
            !names.contains(&"old_only"),
            "old_only must be filtered out"
        );
        assert!(names.contains(&"mid_only"));
        assert!(names.contains(&"new_only"));
        let shared_count = mid_and_newer
            .iter()
            .find(|r| r.label == "shared")
            .unwrap()
            .thread_count;
        assert_eq!(shared_count, 2, "shared count reflects filtered population");

        // created_before = now - 20_000 (inclusive `<=`): includes t_old and t_mid.
        let mid_and_older = label_repo
            .find_distinct_labels(Some(100), None, None, None, Some(now - 20_000), None, None)
            .await?;
        let names: Vec<&str> = mid_and_older.iter().map(|r| r.label.as_str()).collect();
        assert!(names.contains(&"old_only"));
        assert!(names.contains(&"mid_only"));
        assert!(
            !names.contains(&"new_only"),
            "new_only must be filtered out"
        );

        // AND of created_after + created_before isolates t_mid only.
        let only_mid = label_repo
            .find_distinct_labels(
                Some(100),
                None,
                None,
                Some(now - 25_000),
                Some(now - 20_000),
                None,
                None,
            )
            .await?;
        let names: Vec<&str> = only_mid.iter().map(|r| r.label.as_str()).collect();
        assert!(names.contains(&"mid_only"));
        assert!(!names.contains(&"old_only"));
        assert!(!names.contains(&"new_only"));
        let shared_count = only_mid
            .iter()
            .find(|r| r.label == "shared")
            .unwrap()
            .thread_count;
        assert_eq!(shared_count, 1);

        // updated_after path: bump t_old's updated_at past now and verify
        // it survives an updated_after filter even though created_at is old.
        let mut tx = pool.begin().await?;
        thread_repo
            .update_updated_at_tx(&mut *tx, &t_old, now + 5_000)
            .await?;
        tx.commit().await?;
        let recently_updated = label_repo
            .find_distinct_labels(Some(100), None, None, None, None, Some(now), None)
            .await?;
        let names: Vec<&str> = recently_updated.iter().map(|r| r.label.as_str()).collect();
        assert!(
            names.contains(&"old_only"),
            "old_only thread now has fresh updated_at"
        );
        assert!(!names.contains(&"mid_only"));
        assert!(!names.contains(&"new_only"));

        Ok(())
    }

    async fn _test_search_labels_with_time_filter(pool: &'static RdbPool) -> Result<()> {
        let id_gen = crate::test_helper::shared_id_generator();
        let thread_repo = ThreadRepositoryImpl::new(id_gen, pool);
        let label_repo = ThreadLabelRepositoryImpl::new(pool);
        let now = command_utils::util::datetime::now_millis();

        let t_old =
            create_thread_with_timestamps(&thread_repo, pool, 200, now - 30_000, now - 30_000)
                .await?;
        let t_new =
            create_thread_with_timestamps(&thread_repo, pool, 200, now - 5_000, now - 5_000)
                .await?;

        label_repo
            .add_labels(
                t_old.value,
                &["agent_old".to_string(), "other_a".to_string()],
                now,
            )
            .await?;
        label_repo
            .add_labels(
                t_new.value,
                &["agent_new".to_string(), "other_b".to_string()],
                now,
            )
            .await?;

        // Without time filter, both `agent_*` labels show up.
        let all = label_repo
            .search_labels("agent", Some(200), None, None, None, None, None)
            .await?;
        let names: Vec<&str> = all.iter().map(|r| r.label.as_str()).collect();
        assert!(names.contains(&"agent_old"));
        assert!(names.contains(&"agent_new"));

        // created_after = now - 10_000 isolates t_new only.
        let recent = label_repo
            .search_labels(
                "agent",
                Some(200),
                None,
                Some(now - 10_000),
                None,
                None,
                None,
            )
            .await?;
        let names: Vec<&str> = recent.iter().map(|r| r.label.as_str()).collect();
        assert!(names.contains(&"agent_new"));
        assert!(!names.contains(&"agent_old"));

        Ok(())
    }

    async fn _test_find_co_occurring_labels_with_time_filter(pool: &'static RdbPool) -> Result<()> {
        let id_gen = crate::test_helper::shared_id_generator();
        let thread_repo = ThreadRepositoryImpl::new(id_gen, pool);
        let label_repo = ThreadLabelRepositoryImpl::new(pool);
        let now = command_utils::util::datetime::now_millis();

        // Two threads that both have ("workflow-chat" + "coding_agent");
        // their secondary co-occurring labels differ so we can tell them
        // apart in the result.
        let t_old =
            create_thread_with_timestamps(&thread_repo, pool, 300, now - 30_000, now - 30_000)
                .await?;
        let t_new =
            create_thread_with_timestamps(&thread_repo, pool, 300, now - 5_000, now - 5_000)
                .await?;

        label_repo
            .add_labels(
                t_old.value,
                &[
                    "workflow-chat".to_string(),
                    "coding_agent".to_string(),
                    "co_old".to_string(),
                ],
                now,
            )
            .await?;
        label_repo
            .add_labels(
                t_new.value,
                &[
                    "workflow-chat".to_string(),
                    "coding_agent".to_string(),
                    "co_new".to_string(),
                ],
                now,
            )
            .await?;

        // No time filter: both co_old and co_new appear, query labels excluded.
        let all = label_repo
            .find_co_occurring_labels(
                &["workflow-chat".to_string(), "coding_agent".to_string()],
                Some(300),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;
        let names: Vec<&str> = all.iter().map(|r| r.label.as_str()).collect();
        assert!(names.contains(&"co_old"));
        assert!(names.contains(&"co_new"));
        assert!(
            !names.contains(&"workflow-chat"),
            "query labels must be excluded"
        );
        assert!(!names.contains(&"coding_agent"));

        // updated_after isolates t_new — only co_new should remain.
        let recent = label_repo
            .find_co_occurring_labels(
                &["workflow-chat".to_string(), "coding_agent".to_string()],
                Some(300),
                None,
                None,
                None,
                None,
                Some(now - 10_000),
                None,
            )
            .await?;
        let names: Vec<&str> = recent.iter().map(|r| r.label.as_str()).collect();
        assert!(names.contains(&"co_new"));
        assert!(
            !names.contains(&"co_old"),
            "co_old must be excluded by updated_after"
        );
        let co_new_count = recent
            .iter()
            .find(|r| r.label == "co_new")
            .unwrap()
            .thread_count;
        assert_eq!(co_new_count, 1, "thread_count reflects filtered population");

        // created_before isolates t_old — only co_old should remain.
        let older = label_repo
            .find_co_occurring_labels(
                &["workflow-chat".to_string(), "coding_agent".to_string()],
                Some(300),
                None,
                None,
                None,
                Some(now - 10_000),
                None,
                None,
            )
            .await?;
        let names: Vec<&str> = older.iter().map(|r| r.label.as_str()).collect();
        assert!(names.contains(&"co_old"));
        assert!(!names.contains(&"co_new"));

        Ok(())
    }

    async fn setup_pool() -> &'static RdbPool {
        if cfg!(feature = "postgres") {
            let pool = setup_test_rdb_from("sql/postgres").await;
            sqlx::query("TRUNCATE TABLE thread, thread_label CASCADE;")
                .execute(pool)
                .await
                .unwrap();
            pool
        } else {
            let pool = setup_test_rdb_from("sql/sqlite").await;
            sqlx::query("DELETE FROM thread_label;")
                .execute(pool)
                .await
                .unwrap();
            sqlx::query("DELETE FROM thread;")
                .execute(pool)
                .await
                .unwrap();
            pool
        }
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn test_label_crud_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_label_crud(pool).await
        })
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn test_find_by_labels_and_distinct_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_by_labels_and_distinct(pool).await
        })
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn test_batch_labels_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_labels(pool).await
        })
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn test_sort_stability_and_paging_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_sort_stability_and_paging(pool).await
        })
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn test_find_distinct_labels_with_time_filter_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_distinct_labels_with_time_filter(pool).await
        })
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn test_search_labels_with_time_filter_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_search_labels_with_time_filter(pool).await
        })
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn test_find_co_occurring_labels_with_time_filter_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_co_occurring_labels_with_time_filter(pool).await
        })
    }
}
