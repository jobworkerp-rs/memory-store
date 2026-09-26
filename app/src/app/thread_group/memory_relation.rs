//! Group–Memory relationships and their deletion policy.

use super::prelude::*;
use infra::infra::thread_group::memory_relation::ThreadGroupMemoryRelationRepositoryImpl;
use infra::infra::thread_label::rdb::{ThreadLabelRepository, ThreadLabelRepositoryImpl};
use infra::infra::thread_memory::rdb::{ThreadMemoryRepository, ThreadMemoryRepositoryImpl};
use protobuf::llm_memory::data::MemoryData;

pub(crate) fn validate_delete_summary_identity(
    purpose: &str,
    data: &MemoryData,
    group_id: i64,
    memory_id: i64,
) -> anyhow::Result<()> {
    if purpose != "summary" {
        anyhow::bail!("unsupported delete relationship purpose for memory {memory_id}");
    }
    if data.external_id.as_deref()
        != Some(super::thread_group_summary_external_id(group_id).as_str())
        || data.memory_kind != MemoryKind::DerivedSummary as i32
    {
        anyhow::bail!("memory {memory_id} has no verified group summary identity");
    }
    Ok(())
}

pub(crate) fn validate_delete_owner(
    data: &MemoryData,
    thread_owner_user_id: i64,
    memory_id: i64,
) -> anyhow::Result<()> {
    if data.user_id.map(|user| user.value) != Some(thread_owner_user_id) {
        anyhow::bail!("memory {memory_id} is owned by another user");
    }
    Ok(())
}

pub(crate) fn validate_delete_storage_labels(
    labels: &[String],
    group_id: i64,
    memory_id: i64,
) -> anyhow::Result<()> {
    if !labels.iter().any(|label| label == "thread_group_summary")
        || !labels
            .iter()
            .any(|label| label == &format!("group_{group_id}"))
    {
        anyhow::bail!("memory {memory_id} has no dedicated group storage Thread");
    }
    Ok(())
}

pub(crate) fn validate_legacy_summary_kind(
    external_id: Option<&str>,
    memory_kind: i32,
) -> anyhow::Result<()> {
    if external_id.is_some_and(|id| id.starts_with(super::THREAD_GROUP_SUMMARY_EXTERNAL_ID_PREFIX))
        && memory_kind != MemoryKind::DerivedSummary as i32
    {
        anyhow::bail!("thread group summary external ID requires derived summary memory kind");
    }
    Ok(())
}

pub(crate) async fn lock_group_mutations_tx(
    tx: &mut RdbTransaction<'_>,
    pool: &'static RdbPool,
) -> anyhow::Result<()> {
    ThreadGroupLockRepositoryImpl::new(pool)
        .lock_group_membership_tx(&mut **tx)
        .await
}

pub(crate) async fn validate_delete_target_tx(
    tx: &mut RdbTransaction<'_>,
    pool: &'static RdbPool,
    group_id: i64,
    memory_id: i64,
    purpose: &str,
) -> anyhow::Result<()> {
    if purpose != "summary" {
        anyhow::bail!("unsupported delete relationship purpose for memory {memory_id}");
    }
    let ids = IdGeneratorWrapper::new();
    let memories = MemoryRepositoryImpl::new(ids.clone(), pool);
    let locked = memories.find_by_ids_for_update_tx(tx, &[memory_id]).await?;
    let data = locked
        .first()
        .and_then(|row| row.data.as_ref())
        .ok_or_else(|| anyhow::anyhow!("memory {memory_id} not found"))?;
    validate_delete_summary_identity(purpose, data, group_id, memory_id)?;
    let junction = ThreadMemoryRepositoryImpl::new(pool);
    let thread_ids = junction
        .find_all_threads_by_memory_tx(&mut **tx, memory_id)
        .await?;
    if thread_ids.len() != 1 {
        anyhow::bail!("memory {memory_id} does not have a dedicated storage Thread");
    }
    let threads = ThreadRepositoryImpl::new(ids, pool);
    if !threads
        .find_thread_ids_by_default_system_memory_for_update_tx(&mut **tx, memory_id)
        .await?
        .is_empty()
    {
        anyhow::bail!("memory {memory_id} is referenced as a Thread default");
    }
    if !memories
        .find_referring_memory_ids_tx(&mut **tx, memory_id)
        .await?
        .is_empty()
    {
        anyhow::bail!("memory {memory_id} is referenced as a parent by another Memory");
    }
    let thread_id = thread_ids[0];
    let thread = threads
        .find_row_for_update_tx(
            &mut **tx,
            &protobuf::llm_memory::data::ThreadId { value: thread_id },
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("storage Thread {thread_id} not found"))?;
    validate_delete_owner(data, thread.user_id, memory_id)?;
    let labels = ThreadLabelRepositoryImpl::new(pool)
        .find_labels_by_thread_tx(&mut **tx, thread_id)
        .await?;
    validate_delete_storage_labels(&labels, group_id, memory_id)?;
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupMemoryDeletePolicy {
    Retain,
    Delete,
}

impl GroupMemoryDeletePolicy {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Retain => "retain",
            Self::Delete => "delete",
        }
    }
}

pub struct ThreadGroupMemoryRelationService {
    pool: &'static RdbPool,
    groups: ThreadGroupRepositoryImpl,
    memories: MemoryRepositoryImpl,
    relations: ThreadGroupMemoryRelationRepositoryImpl,
}

impl ThreadGroupMemoryRelationService {
    pub fn new(pool: &'static RdbPool) -> Self {
        let ids = IdGeneratorWrapper::new();
        Self {
            pool,
            groups: ThreadGroupRepositoryImpl::new(ids.clone(), pool),
            memories: MemoryRepositoryImpl::new(ids, pool),
            relations: ThreadGroupMemoryRelationRepositoryImpl::new(pool),
        }
    }

    /// Link an already persisted Memory; callers creating a new Memory must
    /// use a transaction that also creates the Memory to avoid orphan writes.
    pub async fn link_existing(
        &self,
        group_id: i64,
        memory_id: i64,
        purpose: &str,
        policy: GroupMemoryDeletePolicy,
    ) -> anyhow::Result<()> {
        super::ensure_thread_group_writes()?;
        if purpose.is_empty() {
            anyhow::bail!("group memory relationship purpose must not be empty");
        }
        let mut tx = self.pool.begin().await?;
        ThreadGroupLockRepositoryImpl::new(self.pool)
            .lock_group_membership_tx(&mut *tx)
            .await?;
        if self
            .groups
            .find_by_id_tx(&mut *tx, group_id)
            .await?
            .is_none()
        {
            anyhow::bail!("thread group {group_id} not found");
        }
        let locked = self
            .memories
            .find_by_ids_for_update_tx(&mut tx, &[memory_id])
            .await?;
        let Some(data) = locked.first().and_then(|memory| memory.data.as_ref()) else {
            anyhow::bail!("memory {memory_id} not found");
        };
        if let Some(external_id) = data.external_id.as_deref()
            && external_id.starts_with(super::THREAD_GROUP_SUMMARY_EXTERNAL_ID_PREFIX)
            && (external_id != super::thread_group_summary_external_id(group_id)
                || data.memory_kind != MemoryKind::DerivedSummary as i32)
        {
            anyhow::bail!("legacy summary identity or kind does not match group {group_id}");
        }
        let existing = self
            .relations
            .list_by_memory_id_tx(&mut *tx, memory_id)
            .await?;
        if policy == GroupMemoryDeletePolicy::Delete {
            validate_delete_target_tx(&mut tx, self.pool, group_id, memory_id, purpose).await?;
        }
        if existing.iter().any(|link| link.group_id == group_id) {
            if existing.iter().any(|link| {
                link.group_id == group_id
                    && link.purpose == purpose
                    && link.on_group_delete == policy.as_str()
            }) {
                tx.commit().await?;
                return Ok(());
            }
            anyhow::bail!("memory {memory_id} relationship already exists with a different policy");
        }
        if existing.iter().any(|link| link.on_group_delete == "delete")
            || (policy == GroupMemoryDeletePolicy::Delete && !existing.is_empty())
        {
            anyhow::bail!("memory {memory_id} is already related to another group");
        }
        self.relations
            .insert_tx(
                &mut *tx,
                group_id,
                memory_id,
                purpose,
                policy.as_str(),
                command_utils::util::datetime::now_millis(),
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

/// Guard the legacy external ID during the transition to explicit links.
/// An unlinked summary must not appear after its owner group is purged.
pub(crate) async fn validate_legacy_summary_target_tx(
    tx: &mut RdbTransaction<'_>,
    pool: &'static RdbPool,
    external_id: Option<&str>,
    memory_kind: i32,
) -> anyhow::Result<()> {
    let Some(raw_id) = external_id
        .and_then(|value| value.strip_prefix(super::THREAD_GROUP_SUMMARY_EXTERNAL_ID_PREFIX))
    else {
        return Ok(());
    };
    let group_id = raw_id.parse::<i64>().ok().filter(|id| *id > 0);
    let Some(group_id) = group_id.filter(|id| id.to_string() == raw_id) else {
        anyhow::bail!("invalid legacy thread group summary external ID");
    };
    validate_legacy_summary_kind(external_id, memory_kind)?;
    super::ensure_thread_group_writes()?;
    ThreadGroupLockRepositoryImpl::new(pool)
        .lock_group_membership_tx(&mut **tx)
        .await?;
    if ThreadGroupRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
        .find_by_id_tx(&mut **tx, group_id)
        .await?
        .is_none()
    {
        anyhow::bail!("thread group {group_id} not found");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use protobuf::llm_memory::data::{MemoryData, UserId};

    #[test]
    fn delete_summary_identity_and_storage_evidence_require_group_ownership() {
        let group_id = 42;
        let id = 7;
        let data = MemoryData {
            user_id: Some(UserId { value: 3 }),
            memory_kind: MemoryKind::DerivedSummary as i32,
            external_id: Some(super::super::thread_group_summary_external_id(group_id)),
            ..Default::default()
        };
        let labels = ["thread_group_summary".into(), "group_42".into()];
        assert!(validate_delete_summary_identity("summary", &data, group_id, id).is_ok());
        assert!(validate_delete_owner(&data, 3, id).is_ok());
        assert!(validate_delete_storage_labels(&labels, group_id, id).is_ok());
        assert!(validate_delete_summary_identity("reference", &data, group_id, id).is_err());
        assert!(validate_delete_summary_identity("summary", &data, 41, id).is_err());
        assert!(validate_delete_owner(&data, 4, id).is_err());
        assert!(validate_delete_storage_labels(&labels[..1], group_id, id).is_err());
    }

    #[test]
    fn reserved_summary_external_id_requires_derived_kind() {
        let external_id = "thread-group-summary:42";
        assert!(validate_legacy_summary_kind(Some(external_id), MemoryKind::Raw as i32).is_err());
        assert!(
            validate_legacy_summary_kind(Some(external_id), MemoryKind::DerivedSummary as i32)
                .is_ok()
        );
        assert!(
            validate_legacy_summary_kind(Some("claude_code:raw"), MemoryKind::Raw as i32).is_ok()
        );
        assert!(validate_legacy_summary_kind(None, MemoryKind::Raw as i32).is_ok());
    }
}
