//! Explicit operator mutations and manual collection operations.

use super::domain::{Group, GroupStatus, Membership, MembershipProvenance, split_group};
use super::observation::ObservedEndpoint;
use super::prelude::*;
use super::{
    ensure_thread_group_writes, known_scope_value, membership_role_of, membership_role_str,
    membership_state_of, membership_state_str,
};

/// Outcome of an explicit group merge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeOutcome {
    pub target_group_id: i64,
    pub moved_members: usize,
    pub already_merged: bool,
}

/// App-layer operator commands: merge / split / attach and the
/// owner-scoped manual collection reference sets. Every method owns its
/// transaction; `actor_id` is recorded as an audit marker only.
pub struct ThreadGroupOperatorService {
    pool: &'static RdbPool,
    groups: ThreadGroupRepositoryImpl,
    members: ThreadGroupMemberRepositoryImpl,
    audit: ThreadGroupAuditRepositoryImpl,
    collections: ManualCollectionRepositoryImpl,
    locks: ThreadGroupLockRepositoryImpl,
}

/// Audit identity describes the partition sets, not the caller's JSON order.
/// The original order is still used for the first split so successor order
/// and all existing first-call behavior remain unchanged.
fn normalize_split_partitions(partitions: &[Vec<String>]) -> Vec<Vec<String>> {
    let mut normalized = partitions
        .iter()
        .map(|partition| {
            let mut keys = partition.clone();
            keys.sort_unstable();
            keys
        })
        .collect::<Vec<_>>();
    normalized.sort_unstable();
    normalized
}

impl ThreadGroupOperatorService {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self::with_id_generator(pool, IdGeneratorWrapper::new())
    }

    /// Injection point for tests that share one process-wide Snowflake
    /// bucket (`infra::test_helper::shared_id_generator`).
    pub fn with_id_generator(pool: &'static RdbPool, id_generator: IdGeneratorWrapper) -> Self {
        Self {
            pool,
            groups: ThreadGroupRepositoryImpl::new(id_generator.clone(), pool),
            members: ThreadGroupMemberRepositoryImpl::new(pool),
            audit: ThreadGroupAuditRepositoryImpl::new(id_generator.clone(), pool),
            collections: ManualCollectionRepositoryImpl::new(id_generator, pool),
            locks: ThreadGroupLockRepositoryImpl::new(pool),
        }
    }

    /// Merge `source_group_id` into `target_group_id`, preserving each
    /// member's active / deleted state and redirecting the source's
    /// current memberships. Idempotent per source group.
    pub async fn merge_groups(
        &self,
        source_group_id: i64,
        target_group_id: i64,
        actor_id: &str,
        reason: &str,
        now: i64,
    ) -> anyhow::Result<MergeOutcome> {
        ensure_thread_group_writes()?;
        if source_group_id == target_group_id {
            anyhow::bail!("cannot merge a group into itself");
        }
        if let Some(existing) = self
            .audit
            .find_merge_by_source_group_id(source_group_id)
            .await?
        {
            return Ok(MergeOutcome {
                target_group_id: existing.target_group_id.unwrap_or(target_group_id),
                moved_members: 0,
                already_merged: true,
            });
        }
        let mut tx = self.pool.begin().await?;
        self.locks.lock_group_membership_tx(&mut *tx).await?;
        let source = self.groups.find_by_id_tx(&mut *tx, source_group_id).await?;
        let target = self.groups.find_by_id_tx(&mut *tx, target_group_id).await?;
        let source =
            source.ok_or_else(|| anyhow::anyhow!("source group {source_group_id} not found"))?;
        let target =
            target.ok_or_else(|| anyhow::anyhow!("target group {target_group_id} not found"))?;
        if source.status != values::group_status::ACTIVE
            || target.status != values::group_status::ACTIVE
        {
            anyhow::bail!("merge requires two active groups");
        }

        let source_members = self
            .members
            .list_current_by_group_id_tx(&mut *tx, source_group_id)
            .await?;
        let target_keys: HashSet<String> = self
            .members
            .list_current_by_group_id_tx(&mut *tx, target_group_id)
            .await?
            .into_iter()
            .map(|member| member.thread_canonical_key)
            .collect();
        // Redirect the source's current rows first: the partial unique
        // index allows only one current membership per canonical key, so
        // the target insert would otherwise collide.
        for member in &source_members {
            self.members
                .redirect_current_tx(&mut *tx, &member.thread_canonical_key, source_group_id, now)
                .await?;
        }
        let mut moved_members = 0usize;
        for member in &source_members {
            if target_keys.contains(&member.thread_canonical_key) {
                continue;
            }
            self.members
                .insert_tx(
                    &mut *tx,
                    &NewThreadGroupMember {
                        group_id: target_group_id,
                        thread_id: member.thread_id,
                        thread_canonical_key: member.thread_canonical_key.clone(),
                        user_id: member.user_id,
                        source: member.source.clone(),
                        identity_scope: member.identity_scope.clone(),
                        native_id: member.native_id.clone(),
                        role: member.role.clone(),
                        state: member.state.clone(),
                        provenance: values::grouping_authority::OPERATOR.to_string(),
                        deleted_at: member.deleted_at,
                        created_at: now,
                        updated_at: now,
                    },
                )
                .await?;
            moved_members += 1;
        }
        self.groups
            .redirect_tx(&mut *tx, source_group_id, target_group_id, now)
            .await?;
        // Flatten existing redirects that pointed at the source.
        for group in self
            .groups
            .list_by_status(values::group_status::REDIRECTED, None, None)
            .await?
        {
            if group.redirect_to_group_id == Some(source_group_id) {
                self.groups
                    .repoint_redirect_tx(&mut *tx, group.id, target_group_id, now)
                    .await?;
            }
        }
        self.groups
            .set_grouping_authority_tx(
                &mut *tx,
                target_group_id,
                values::grouping_authority::OPERATOR,
                now,
            )
            .await?;
        for member in self
            .members
            .list_current_by_group_id_tx(&mut *tx, target_group_id)
            .await?
        {
            self.members
                .set_role_provenance_tx(
                    &mut *tx,
                    &member.thread_canonical_key,
                    &member.role,
                    values::grouping_authority::OPERATOR,
                    now,
                )
                .await?;
        }
        self.audit
            .append_merge_tx(
                &mut *tx,
                &NewGroupAuditMerge {
                    source_group_id,
                    target_group_id,
                    actor_id: actor_id.to_string(),
                    reason: reason.to_string(),
                    created_at: now,
                },
            )
            .await?;
        tx.commit().await?;
        Ok(MergeOutcome {
            target_group_id,
            moved_members,
            already_merged: false,
        })
    }

    /// Split an active group into two or more partitions of canonical
    /// keys. Retrying the same partition returns the existing
    /// successors; a different partition is rejected.
    pub async fn split_group(
        &self,
        source_group_id: i64,
        partitions: &[Vec<String>],
        actor_id: &str,
        reason: &str,
        now: i64,
    ) -> anyhow::Result<Vec<i64>> {
        ensure_thread_group_writes()?;
        let canonical_partition = serde_json::to_string(&normalize_split_partitions(partitions))?;
        let mut tx = self.pool.begin().await?;
        self.locks.lock_group_membership_tx(&mut *tx).await?;
        let source = self
            .groups
            .find_by_id_tx(&mut *tx, source_group_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source group {source_group_id} not found"))?;
        // A retry of a completed split must return the recorded
        // successors even though the source is now `split`, so consult
        // the audit before rejecting a non-active source.
        if let Some(existing) = self
            .audit
            .find_split_by_source_group_id_tx(&mut *tx, source_group_id)
            .await?
        {
            if existing.canonical_partition.as_deref() == Some(canonical_partition.as_str()) {
                let successors: Vec<i64> = existing
                    .successor_group_ids
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?
                    .unwrap_or_default();
                return Ok(successors);
            }
            anyhow::bail!("source group was already split with a different partition");
        }
        if source.status != values::group_status::ACTIVE {
            anyhow::bail!("split requires an active source group");
        }

        let rows = self
            .members
            .list_current_by_group_id_tx(&mut *tx, source_group_id)
            .await?;
        let group = Group {
            canonical_key: source.group_canonical_key.clone(),
            status: GroupStatus::Active,
            memberships: rows
                .iter()
                .map(|row| Membership {
                    thread_canonical_key: row.thread_canonical_key.clone(),
                    role: membership_role_of(&row.role),
                    state: membership_state_of(&row.state),
                    provenance: MembershipProvenance::Reconciler,
                    deleted_at: row.deleted_at,
                })
                .collect(),
        };
        let partition_refs: Vec<Vec<&str>> = partitions
            .iter()
            .map(|partition| partition.iter().map(String::as_str).collect())
            .collect();
        let outcome = split_group(&group, &partition_refs)
            .map_err(|error| anyhow::anyhow!("invalid split partition: {error:?}"))?;

        // Redirect the source's current rows before inserting the
        // successors: the partial unique index allows only one current
        // membership per canonical key.
        for row in &rows {
            self.members
                .redirect_current_tx(&mut *tx, &row.thread_canonical_key, source_group_id, now)
                .await?;
        }

        let mut successor_ids = Vec::with_capacity(outcome.successors.len());
        for successor in &outcome.successors {
            let group_id = self
                .groups
                .create_tx(
                    &mut *tx,
                    &NewThreadGroup {
                        user_id: source.user_id,
                        group_canonical_key: successor.canonical_key.clone(),
                        title: source.title.clone(),
                        status: values::group_status::ACTIVE.to_string(),
                        grouping_authority: values::grouping_authority::OPERATOR.to_string(),
                        redirect_to_group_id: None,
                        created_at: now,
                        updated_at: now,
                    },
                )
                .await?;
            successor_ids.push(group_id);
            for membership in &successor.memberships {
                let row = rows
                    .iter()
                    .find(|row| row.thread_canonical_key == membership.thread_canonical_key)
                    .ok_or_else(|| anyhow::anyhow!("partition member lost its source row"))?;
                self.members
                    .insert_tx(
                        &mut *tx,
                        &NewThreadGroupMember {
                            group_id,
                            thread_id: row.thread_id,
                            thread_canonical_key: membership.thread_canonical_key.clone(),
                            user_id: row.user_id,
                            source: row.source.clone(),
                            identity_scope: row.identity_scope.clone(),
                            native_id: row.native_id.clone(),
                            role: membership_role_str(&membership.role),
                            state: membership_state_str(&membership.state),
                            provenance: values::grouping_authority::OPERATOR.to_string(),
                            deleted_at: membership.deleted_at,
                            created_at: now,
                            updated_at: now,
                        },
                    )
                    .await?;
            }
        }
        self.groups
            .mark_split_tx(&mut *tx, source_group_id, now)
            .await?;
        self.audit
            .append_split_tx(
                &mut *tx,
                &NewGroupAuditSplit {
                    source_group_id,
                    actor_id: actor_id.to_string(),
                    reason: reason.to_string(),
                    canonical_partition,
                    successor_group_ids: serde_json::to_string(&successor_ids)?,
                    created_at: now,
                },
            )
            .await?;
        tx.commit().await?;
        Ok(successor_ids)
    }

    /// Attach one Thread to an existing active group as an
    /// operator-fixed membership. Idempotent.
    pub async fn attach_member(
        &self,
        group_id: i64,
        thread_id: i64,
        thread_canonical_key: &str,
        endpoint: Option<&ObservedEndpoint>,
        now: i64,
    ) -> anyhow::Result<()> {
        ensure_thread_group_writes()?;
        let mut tx = self.pool.begin().await?;
        self.locks
            .lock_thread_canonical_key_tx(&mut *tx, thread_canonical_key)
            .await?;
        self.locks.lock_group_membership_tx(&mut *tx).await?;
        let group = self
            .groups
            .find_by_id_tx(&mut *tx, group_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("group {group_id} not found"))?;
        if group.status != values::group_status::ACTIVE {
            anyhow::bail!("attach requires an active group");
        }
        if self
            .members
            .find_current_by_thread_canonical_key_tx(&mut *tx, thread_canonical_key)
            .await?
            .is_some()
        {
            return Ok(());
        }
        self.members
            .insert_tx(
                &mut *tx,
                &NewThreadGroupMember {
                    group_id,
                    thread_id: Some(thread_id),
                    thread_canonical_key: thread_canonical_key.to_string(),
                    user_id: endpoint
                        .map(|endpoint| endpoint.user_id)
                        .unwrap_or_default(),
                    source: endpoint.map(|endpoint| endpoint.source.clone()),
                    identity_scope: endpoint
                        .and_then(|endpoint| known_scope_value(&endpoint.identity_scope)),
                    native_id: endpoint.map(|endpoint| endpoint.native_id.clone()),
                    role: values::member_role::MEMBER.to_string(),
                    state: values::member_state::ACTIVE.to_string(),
                    provenance: values::grouping_authority::OPERATOR.to_string(),
                    deleted_at: None,
                    created_at: now,
                    updated_at: now,
                },
            )
            .await?;
        self.groups
            .set_grouping_authority_tx(
                &mut *tx,
                group_id,
                values::grouping_authority::OPERATOR,
                now,
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn create_manual_collection(
        &self,
        user_id: i64,
        title: &str,
        now: i64,
    ) -> anyhow::Result<i64> {
        ensure_thread_group_writes()?;
        let mut tx = self.pool.begin().await?;
        let id = self
            .collections
            .create_tx(
                &mut *tx,
                &NewManualCollection {
                    user_id,
                    title: title.to_string(),
                    created_at: now,
                    updated_at: now,
                },
            )
            .await?;
        tx.commit().await?;
        Ok(id)
    }

    pub async fn list_manual_collections(
        &self,
        user_id: i64,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> anyhow::Result<Vec<ManualCollectionRow>> {
        self.collections.list_by_owner(user_id, limit, offset).await
    }

    pub async fn rename_manual_collection(
        &self,
        collection_id: i64,
        owner_user_id: Option<i64>,
        title: &str,
        now: i64,
    ) -> anyhow::Result<bool> {
        ensure_thread_group_writes()?;
        let Some(collection) = self.collections.find_by_id(collection_id).await? else {
            return Ok(false);
        };
        if owner_user_id.is_some_and(|user_id| user_id != collection.user_id) {
            return Ok(false);
        }
        let mut tx = self.pool.begin().await?;
        let renamed = self
            .collections
            .rename_tx(&mut *tx, collection_id, title, now)
            .await?;
        tx.commit().await?;
        Ok(renamed)
    }

    /// Delete a collection and its member links only. Threads, Memory,
    /// and primary lineage are untouched.
    pub async fn delete_manual_collection(
        &self,
        collection_id: i64,
        owner_user_id: Option<i64>,
    ) -> anyhow::Result<bool> {
        ensure_thread_group_writes()?;
        let Some(collection) = self.collections.find_by_id(collection_id).await? else {
            return Ok(false);
        };
        if owner_user_id.is_some_and(|user_id| user_id != collection.user_id) {
            return Ok(false);
        }
        let mut tx = self.pool.begin().await?;
        self.collections
            .detach_all_members_tx(&mut *tx, collection_id)
            .await?;
        let deleted = self.collections.delete_tx(&mut *tx, collection_id).await?;
        tx.commit().await?;
        Ok(deleted)
    }

    pub async fn attach_manual_collection_member(
        &self,
        collection_id: i64,
        thread_id: i64,
        user_id: i64,
        now: i64,
    ) -> anyhow::Result<bool> {
        ensure_thread_group_writes()?;
        let Some(collection) = self.collections.find_by_id(collection_id).await? else {
            return Ok(false);
        };
        if collection.user_id != user_id {
            return Ok(false);
        }
        let mut tx = self.pool.begin().await?;
        let attached = self
            .collections
            .attach_member_tx(&mut *tx, collection_id, thread_id, user_id)
            .await?;
        if attached {
            self.collections
                .touch_tx(&mut *tx, collection_id, now)
                .await?;
        }
        tx.commit().await?;
        Ok(attached)
    }

    pub async fn detach_manual_collection_member(
        &self,
        collection_id: i64,
        thread_id: i64,
        owner_user_id: Option<i64>,
        now: i64,
    ) -> anyhow::Result<bool> {
        ensure_thread_group_writes()?;
        let Some(collection) = self.collections.find_by_id(collection_id).await? else {
            return Ok(false);
        };
        if owner_user_id.is_some_and(|user_id| user_id != collection.user_id) {
            return Ok(false);
        }
        let mut tx = self.pool.begin().await?;
        let detached = self
            .collections
            .detach_member_tx(&mut *tx, collection_id, thread_id)
            .await?;
        if detached {
            self.collections
                .touch_tx(&mut *tx, collection_id, now)
                .await?;
        }
        tx.commit().await?;
        Ok(detached)
    }

    pub async fn list_manual_collection_members(
        &self,
        collection_id: i64,
        owner_user_id: Option<i64>,
    ) -> anyhow::Result<Vec<ManualCollectionMemberRow>> {
        let Some(collection) = self.collections.find_by_id(collection_id).await? else {
            return Ok(Vec::new());
        };
        if owner_user_id.is_some_and(|user_id| user_id != collection.user_id) {
            return Ok(Vec::new());
        }
        self.collections.list_members(collection_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_partition_normalization_ignores_partition_and_member_order() {
        let original = vec![
            vec!["parent-a".to_string(), "child-a".to_string()],
            vec!["parent-b".to_string(), "child-b".to_string()],
        ];
        let reordered = vec![
            vec!["child-b".to_string(), "parent-b".to_string()],
            vec!["child-a".to_string(), "parent-a".to_string()],
        ];

        assert_eq!(
            normalize_split_partitions(&original),
            normalize_split_partitions(&reordered)
        );
        assert_ne!(
            normalize_split_partitions(&original),
            normalize_split_partitions(&[
                vec!["parent-a".to_string(), "child-b".to_string()],
                vec!["parent-b".to_string(), "child-a".to_string()],
            ])
        );
    }
}
