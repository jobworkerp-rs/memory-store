//! Shared implementation for the release migration command.
//!
//! The public command lives in `memories-db-migrate`; keeping its task
//! registry here makes catalog validation and task behaviour testable without
//! spawning a process.

use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::RdbPool;

pub mod catalog;
pub mod local;
pub mod state;
pub mod thread_groups_canonical_keys_v1;
pub mod thread_groups_user_ids_v1;
pub mod thread_groups_user_ids_v3;
pub mod thread_groups_user_ids_v4;
pub mod thread_message_times_v1;
mod typed_owner_backfill;

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
