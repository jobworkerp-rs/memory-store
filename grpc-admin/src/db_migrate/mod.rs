//! Shared implementation for the release migration command.
//!
//! The public command lives in `memories-db-migrate`; keeping its task
//! registry here makes catalog validation and task behaviour testable without
//! spawning a process.

use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::RdbPool;

pub mod catalog;
pub mod embedding;
pub mod local;
pub mod state;
pub mod thread_groups_canonical_keys_v1;
pub mod thread_groups_user_ids_v1;
pub mod thread_groups_user_ids_v3;
pub mod thread_groups_user_ids_v4;
pub mod thread_message_times_v1;
mod typed_owner_backfill;
pub mod vocabulary;

/// Bind placeholder for the compiled RDB backend.
fn placeholder(index: usize) -> String {
    #[cfg(feature = "postgres")]
    {
        format!("${index}")
    }
    #[cfg(not(feature = "postgres"))]
    {
        let _ = index;
        "?".to_string()
    }
}

/// Storage outside the RDB that a task may change. Local backups copy it
/// together with the database so that both can be restored as one unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TaskResource {
    ThreadLanceDb,
}

/// Resources changed by a registered task implementation.
pub fn task_resources(implementation: &str) -> &'static [TaskResource] {
    match implementation {
        "thread_message_times_v1::ThreadMessageTimesV1Task" => &[TaskResource::ThreadLanceDb],
        _ => &[],
    }
}

/// Fixed-registry contract for a release-bound post-schema migration task.
///
/// Implementations are selected only by a catalog entry validated by the
/// coordinator; they are never synthesized from user-provided SQL or a
/// command string.
#[async_trait]
pub trait DataMigrationTask: Send + Sync {
    fn task_identity(&self) -> String;
    async fn inspect(&self) -> Result<serde_json::Value>;
    async fn dry_run(&self) -> Result<serde_json::Value>;
    async fn apply(&self, execution_id: &str, holder_id: &str) -> Result<serde_json::Value>;
    async fn verify(&self) -> Result<()>;
}

/// Whether a registered task is embedding neutral (spec: embedding space
/// management §3.8): it touches no vector table or embedding index, and
/// changes neither which entities are embedded nor their source versions.
/// Such work may run while an embedding migration is unfinished. A task
/// not listed here is treated as not neutral.
pub fn task_embedding_neutral(implementation: &str) -> bool {
    matches!(
        implementation,
        "thread_groups_canonical_keys_v1::ThreadGroupsCanonicalKeysV1Task"
            | "thread_groups_canonical_keys_v2::ThreadGroupsCanonicalKeysV2Task"
            | "thread_groups_user_ids_v1::ThreadGroupsUserIdsV1Task"
            | "thread_groups_user_ids_v2::ThreadGroupsUserIdsV2Task"
            | "thread_groups_user_ids_v3::ThreadGroupsUserIdsV3Task"
            | "thread_groups_user_ids_v4::ThreadGroupsUserIdsV4Task"
    )
}

/// Schema migration versions that are embedding neutral (same meaning as
/// [`task_embedding_neutral`]). Versions not listed are treated as not
/// neutral; older ones are always applied before an embedding migration
/// can exist, since it needs the storage identity of 20261009000001.
pub const EMBEDDING_NEUTRAL_SCHEMA_VERSIONS: &[&str] = &[
    "20260920000001",
    "20260926000001",
    "20260930000001",
    "20261009000001",
];

/// Whether all of `schema_versions` and `implementations` are embedding
/// neutral.
pub fn work_is_embedding_neutral<'a>(
    schema_versions: impl IntoIterator<Item = &'a str>,
    implementations: impl IntoIterator<Item = &'a str>,
) -> bool {
    schema_versions
        .into_iter()
        .all(|v| EMBEDDING_NEUTRAL_SCHEMA_VERSIONS.contains(&v))
        && implementations.into_iter().all(task_embedding_neutral)
}

/// Whether a catalog implementation identifier is compiled into this release.
/// The catalog remains declarative; this allowlist prevents it from becoming a
/// user-controlled code-loading mechanism.
pub fn has_registered_implementation(implementation: &str) -> bool {
    matches!(
        implementation,
        "thread_message_times_v1::ThreadMessageTimesV1Task"
            | "thread_groups_canonical_keys_v1::ThreadGroupsCanonicalKeysV1Task"
            | "thread_groups_canonical_keys_v2::ThreadGroupsCanonicalKeysV2Task"
            | "thread_groups_user_ids_v1::ThreadGroupsUserIdsV1Task"
            | "thread_groups_user_ids_v2::ThreadGroupsUserIdsV2Task"
            | "thread_groups_user_ids_v3::ThreadGroupsUserIdsV3Task"
            | "thread_groups_user_ids_v4::ThreadGroupsUserIdsV4Task"
    )
}

/// Construct the task selected by a validated catalog entry.
pub fn task_from_catalog(
    pool: RdbPool,
    entry: catalog::TaskCatalogEntry,
) -> Result<Box<dyn DataMigrationTask>> {
    catalog::validate_fixed_catalog_entry(&entry)?;
    match entry.implementation.as_str() {
        "thread_message_times_v1::ThreadMessageTimesV1Task" => Ok(Box::new(
            thread_message_times_v1::ThreadMessageTimesV1Task::new(pool, entry)?,
        )),
        "thread_groups_canonical_keys_v1::ThreadGroupsCanonicalKeysV1Task" => Ok(Box::new(
            thread_groups_canonical_keys_v1::ThreadGroupsCanonicalKeysV1Task::new(pool, entry)?,
        )),
        "thread_groups_canonical_keys_v2::ThreadGroupsCanonicalKeysV2Task" => Ok(Box::new(
            thread_groups_canonical_keys_v1::ThreadGroupsCanonicalKeysV2Task::new(pool, entry)?,
        )),
        "thread_groups_user_ids_v1::ThreadGroupsUserIdsV1Task" => Ok(Box::new(
            thread_groups_user_ids_v1::ThreadGroupsUserIdsV1Task::new(pool, entry)?,
        )),
        "thread_groups_user_ids_v2::ThreadGroupsUserIdsV2Task" => Ok(Box::new(
            thread_groups_user_ids_v1::ThreadGroupsUserIdsV2Task::new(pool, entry)?,
        )),
        "thread_groups_user_ids_v3::ThreadGroupsUserIdsV3Task" => Ok(Box::new(
            thread_groups_user_ids_v3::ThreadGroupsUserIdsV3Task::new(pool, entry)?,
        )),
        "thread_groups_user_ids_v4::ThreadGroupsUserIdsV4Task" => Ok(Box::new(
            thread_groups_user_ids_v4::ThreadGroupsUserIdsV4Task::new(pool, entry)?,
        )),
        _ => unreachable!("TaskCatalogEntry::validate rejects unregistered implementations"),
    }
}

#[cfg(test)]
mod embedding_neutrality_tests {
    use super::*;

    #[test]
    fn only_declared_work_is_neutral() {
        assert!(work_is_embedding_neutral([], []));
        assert!(work_is_embedding_neutral(
            ["20261009000001"],
            ["thread_groups_user_ids_v4::ThreadGroupsUserIdsV4Task"]
        ));
        // Touches thread vector rows.
        assert!(!work_is_embedding_neutral(
            [],
            ["thread_message_times_v1::ThreadMessageTimesV1Task"]
        ));
        // Undeclared schema versions and tasks are not neutral.
        assert!(!work_is_embedding_neutral(["20991231000001"], []));
        assert!(!work_is_embedding_neutral([], ["future::Task"]));
    }

    #[test]
    fn every_neutral_task_is_registered_and_changes_no_resource() {
        for implementation in [
            "thread_groups_canonical_keys_v1::ThreadGroupsCanonicalKeysV1Task",
            "thread_groups_canonical_keys_v2::ThreadGroupsCanonicalKeysV2Task",
            "thread_groups_user_ids_v1::ThreadGroupsUserIdsV1Task",
            "thread_groups_user_ids_v2::ThreadGroupsUserIdsV2Task",
            "thread_groups_user_ids_v3::ThreadGroupsUserIdsV3Task",
            "thread_groups_user_ids_v4::ThreadGroupsUserIdsV4Task",
        ] {
            assert!(task_embedding_neutral(implementation));
            assert!(has_registered_implementation(implementation));
            assert!(task_resources(implementation).is_empty());
        }
    }
}
