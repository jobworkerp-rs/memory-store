//! Typed-owner backfill where the typed `user_id` columns are the owner of
//! record (used from `thread-groups-user-ids-v1@4`).
//!
//! The retired legacy `owner_scope` strings are read only to fill a typed owner
//! that is still missing; their value never blocks the migration.

use super::placeholder;
use anyhow::{Context, Result, bail};
use common::thread_group_key::parse_legacy_owner_scope;
use infra_utils::infra::rdb::RdbTransaction;
use std::collections::BTreeMap;

/// A typed owner column, the legacy column that may fill it, and the Thread
/// reference that may fill it when the legacy value is unusable.
struct TypedOwnerField {
    table: &'static str,
    user_column: &'static str,
    legacy_column: &'static str,
    thread_column: Option<&'static str>,
    /// Ownerless rows are valid only where the domain allows them (outbox).
    owner_required: bool,
    /// Whether the owner must equal the owner of the referenced Thread. Manual
    /// collections may legitimately hold another user's Thread.
    matches_thread_owner: bool,
    row_filter: Option<&'static str>,
}

impl TypedOwnerField {
    const fn new(
        table: &'static str,
        user_column: &'static str,
        legacy_column: &'static str,
        thread_column: Option<&'static str>,
    ) -> Self {
        Self {
            table,
            user_column,
            legacy_column,
            thread_column,
            owner_required: true,
            matches_thread_owner: thread_column.is_some(),
            row_filter: None,
        }
    }

    const fn owner_optional(mut self) -> Self {
        self.owner_required = false;
        self
    }

    const fn owner_independent_of_thread(mut self) -> Self {
        self.matches_thread_owner = false;
        self
    }

    const fn only_rows(mut self, filter: &'static str) -> Self {
        self.row_filter = Some(filter);
        self
    }

    fn name(&self) -> String {
        format!("{}.{}", self.table, self.user_column)
    }

    fn missing_owner_predicate(&self) -> String {
        match self.row_filter {
            Some(filter) => format!("{} IS NULL AND {filter}", self.user_column),
            None => format!("{} IS NULL", self.user_column),
        }
    }
}

// Observations reach a Thread only through a source identity keyed by the same
// typed owner, so the identity row check already covers them.
const TYPED_OWNER_FIELDS: &[TypedOwnerField] = &[
    TypedOwnerField::new(
        "thread_group_member",
        "user_id",
        "owner_scope",
        Some("thread_id"),
    ),
    TypedOwnerField::new(
        "thread_relation",
        "parent_user_id",
        "parent_owner_scope",
        Some("parent_thread_id"),
    ),
    TypedOwnerField::new(
        "thread_relation",
        "child_user_id",
        "child_owner_scope",
        Some("child_thread_id"),
    ),
    TypedOwnerField::new(
        "thread_observation",
        "subject_user_id",
        "subject_owner_scope",
        None,
    ),
    TypedOwnerField::new(
        "thread_observation",
        "candidate_parent_user_id",
        "candidate_parent_owner_scope",
        None,
    )
    .only_rows("candidate_parent_present = TRUE"),
    TypedOwnerField::new(
        "thread_group_candidate_association",
        "subject_user_id",
        "subject_owner_scope",
        Some("subject_thread_id"),
    ),
    TypedOwnerField::new(
        "source_thread_identity",
        "user_id",
        "owner_scope",
        Some("thread_id"),
    ),
    TypedOwnerField::new(
        "thread_canonical_key",
        "user_id",
        "owner_scope",
        Some("thread_id"),
    ),
    TypedOwnerField::new("thread_deletion_marker", "user_id", "owner_scope", None),
    TypedOwnerField::new("operator_decision", "user_id", "owner_scope", None),
    TypedOwnerField::new("manual_collection", "user_id", "owner_scope", None),
    TypedOwnerField::new(
        "manual_collection_member",
        "user_id",
        "owner_scope",
        Some("thread_id"),
    )
    .owner_independent_of_thread(),
    // An event without a legacy owner is ownerless, immutable history; its Thread
    // reference is not evidence of an owner, so only the legacy owner fills it.
    TypedOwnerField::new("thread_group_event_outbox", "user_id", "owner_scope", None)
        .owner_optional(),
];

const ABSENT_PARENT_WITH_OWNER_SQL: &str = "SELECT COUNT(*) FROM thread_observation \
    WHERE candidate_parent_present = FALSE AND candidate_parent_user_id IS NOT NULL";

/// Result of filling missing typed owners inside the caller's transaction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct TypedOwnerPreparation {
    pub(super) backfilled: BTreeMap<String, u64>,
    /// Rows whose owner no source can supply (or, for the legacy basis, rows
    /// still waiting for the backfill).
    pub(super) unresolved: BTreeMap<String, u64>,
}

/// Fill missing typed owners, then reject contradictions between typed owners
/// and the Threads they reference. Idempotent, so inspection runs it in a
/// transaction that is rolled back.
pub(super) async fn prepare_typed_owners_tx(
    tx: &mut RdbTransaction<'_>,
) -> Result<TypedOwnerPreparation> {
    let mut preparation = TypedOwnerPreparation::default();
    for field in TYPED_OWNER_FIELDS {
        let filled = backfill_from_legacy_scope(tx, field).await?
            + backfill_from_referenced_thread(tx, field).await?;
        if filled > 0 {
            preparation.backfilled.insert(field.name(), filled);
        }
        if field.owner_required {
            let missing = count(
                tx,
                &format!(
                    "SELECT COUNT(*) FROM {} WHERE {}",
                    field.table,
                    field.missing_owner_predicate()
                ),
            )
            .await?;
            if missing > 0 {
                preparation.unresolved.insert(field.name(), missing);
            }
        }
    }
    validate_referenced_thread_owners(tx).await?;
    if count(tx, ABSENT_PARENT_WITH_OWNER_SQL).await? > 0 {
        bail!("absent observation parent must not have candidate_parent_user_id");
    }
    super::thread_groups_user_ids_v1::validate_group_owners(tx).await?;
    super::thread_groups_user_ids_v1::validate_marker_key_assignments(tx).await?;
    Ok(preparation)
}

/// Owners per statement; keeps bind parameters well below backend limits.
const LEGACY_OWNERS_PER_UPDATE: usize = 500;

async fn backfill_from_legacy_scope(
    tx: &mut RdbTransaction<'_>,
    field: &TypedOwnerField,
) -> Result<u64> {
    let select = format!(
        "SELECT DISTINCT {legacy} FROM {table} WHERE {missing} AND {legacy} IS NOT NULL",
        legacy = field.legacy_column,
        table = field.table,
        missing = field.missing_owner_predicate(),
    );
    let scopes: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(select))
        .fetch_all(&mut **tx)
        .await
        .with_context(|| format!("reading legacy owners for {}", field.name()))?;
    // Unusable legacy values are skipped here and left to the Thread fallback.
    let owners = scopes
        .into_iter()
        .filter_map(|scope| {
            parse_legacy_owner_scope(&scope)
                .filter(|id| *id > 0)
                .map(|id| (scope, id))
        })
        .collect::<Vec<_>>();
    let mut filled = 0;
    // One pass over the table per chunk, instead of one per owner.
    for chunk in owners.chunks(LEGACY_OWNERS_PER_UPDATE) {
        let values = (0..chunk.len())
            .map(|index| {
                format!(
                    "({}, {})",
                    placeholder(2 * index + 1),
                    placeholder(2 * index + 2)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let update = format!(
            "WITH legacy_owner(legacy_scope, typed_owner) AS (VALUES {values}) \
             UPDATE {table} SET {user} = \
               (SELECT typed_owner FROM legacy_owner WHERE legacy_scope = {table}.{legacy}) \
             WHERE {missing} AND {legacy} IN (SELECT legacy_scope FROM legacy_owner)",
            table = field.table,
            user = field.user_column,
            legacy = field.legacy_column,
            missing = field.missing_owner_predicate(),
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(update));
        for (scope, user_id) in chunk {
            query = query.bind(scope).bind(*user_id);
        }
        filled += query
            .execute(&mut **tx)
            .await
            .with_context(|| format!("backfilling {} from legacy owners", field.name()))?
            .rows_affected();
    }
    Ok(filled)
}

async fn backfill_from_referenced_thread(
    tx: &mut RdbTransaction<'_>,
    field: &TypedOwnerField,
) -> Result<u64> {
    let Some(thread_column) = field.thread_column else {
        return Ok(0);
    };
    let update = format!(
        "UPDATE {table} SET {user} = (SELECT thread.user_id FROM thread WHERE thread.id = {table}.{thread_column}) \
         WHERE {missing} AND {thread_column} IN (SELECT id FROM thread)",
        table = field.table,
        user = field.user_column,
        missing = field.missing_owner_predicate(),
    );
    Ok(sqlx::query(sqlx::AssertSqlSafe(update))
        .execute(&mut **tx)
        .await
        .with_context(|| format!("backfilling {} from referenced Threads", field.name()))?
        .rows_affected())
}

async fn validate_referenced_thread_owners(tx: &mut RdbTransaction<'_>) -> Result<()> {
    for field in TYPED_OWNER_FIELDS
        .iter()
        .filter(|field| field.matches_thread_owner)
    {
        let thread_column = field
            .thread_column
            .expect("only Thread-referencing fields are compared with a Thread owner");
        let sql = format!(
            "SELECT COUNT(*) FROM {table} JOIN thread ON thread.id = {table}.{thread_column} \
             WHERE {table}.{user} <> thread.user_id",
            table = field.table,
            user = field.user_column,
        );
        let mismatched = count(tx, &sql).await?;
        if mismatched > 0 {
            bail!(
                "{mismatched} rows of {} disagree with the owner of the referenced Thread",
                field.name()
            );
        }
    }
    Ok(())
}

async fn count(tx: &mut RdbTransaction<'_>, sql: &str) -> Result<u64> {
    let value: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_owned()))
        .fetch_one(&mut **tx)
        .await
        .with_context(|| format!("running typed owner check {sql}"))?;
    Ok(value.max(0) as u64)
}

#[cfg(test)]
mod tests {
    use super::TYPED_OWNER_FIELDS;
    use crate::db_migrate::thread_groups_user_ids_v1::{BACKFILL_SQL, TYPED_ID_BACKFILL_COUNT};

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn legacy_scopes_of_several_owners_are_filled_in_one_pass() {
        use super::prepare_typed_owners_tx;
        use crate::db_migrate::thread_groups_user_ids_v3::tests::test_pool;
        use std::collections::BTreeMap;

        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            for (id, scope) in [(1, "user:1"), (2, "user:2"), (3, "user:2"), (4, "user:0")] {
                sqlx::query(
                    "INSERT INTO manual_collection (id, user_id, owner_scope, title, created_at, updated_at) \
                     VALUES (?, NULL, ?, 'fixture', 1, 1)",
                )
                .bind(id)
                .bind(scope)
                .execute(pool)
                .await
                .unwrap();
            }
            let mut tx = pool.begin().await.unwrap();
            let preparation = prepare_typed_owners_tx(&mut tx).await.unwrap();
            let owners: Vec<(i64, Option<i64>)> =
                sqlx::query_as("SELECT id, user_id FROM manual_collection ORDER BY id")
                    .fetch_all(&mut *tx)
                    .await
                    .unwrap();
            tx.rollback().await.unwrap();

            assert_eq!(
                owners,
                vec![(1, Some(1)), (2, Some(2)), (3, Some(2)), (4, None)]
            );
            let manual = "manual_collection.user_id".to_string();
            assert_eq!(preparation.backfilled, BTreeMap::from([(manual.clone(), 3)]));
            // Non-positive owners are not canonical and have no Thread fallback here.
            assert_eq!(preparation.unresolved, BTreeMap::from([(manual, 1)]));
        });
    }

    #[test]
    fn covers_the_same_typed_owner_columns_as_the_released_backfill() {
        // A typed owner column added to one generation must not be missed by the other.
        let released = BACKFILL_SQL
            .iter()
            .take(TYPED_ID_BACKFILL_COUNT)
            .map(|sql| {
                let mut words = sql.split_whitespace().skip(1);
                let table = words.next().unwrap();
                let column = words.nth(1).unwrap();
                format!("{table}.{column}")
            })
            .collect::<Vec<_>>();
        let current = TYPED_OWNER_FIELDS
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>();
        assert_eq!(current, released);
    }
}
