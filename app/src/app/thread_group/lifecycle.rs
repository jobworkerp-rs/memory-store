//! Inactive-history lifecycle and purge operations.

use super::ensure_thread_group_writes;
use super::prelude::*;

/// Preview of an inactive-history purge. `digest` pins the exact target
/// set so `delete` can reject a stale preview.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PurgePreview {
    pub group_id: i64,
    pub group_status: String,
    pub inactive_memberships: i64,
    pub active_memberships: i64,
    pub inactive_relations: i64,
    pub active_relations: i64,
    pub audit_rows: i64,
    /// Deletion markers whose identity is referenced only by this
    /// group's placeholders; these are purged.
    pub purgeable_deletion_markers: i64,
    /// Deletion markers still referenced by another group's history;
    /// these are retained and reported.
    pub retained_deletion_markers: i64,
    /// Groups whose redirect points at this group. A redirected target
    /// is repointed to its own redirect target; a split target with
    /// inbound redirects is rejected.
    pub dangling_redirects: i64,
    pub digest: String,
}

/// Result of an executed purge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PurgeOutcome {
    pub deleted_memberships: u64,
    pub deleted_relations: usize,
    pub deleted_audit_rows: usize,
    pub deleted_deletion_markers: usize,
    pub repointed_redirects: usize,
    pub group_deleted: bool,
}

/// Inactive-history purge. Conservative by design: deletion markers and
/// observations are retained (they may be shared with active or
/// non-purged history), and a group with active members is rejected
/// rather than partially emptied.
pub struct ThreadGroupPurgeService {
    pool: &'static RdbPool,
    groups: ThreadGroupRepositoryImpl,
    members: ThreadGroupMemberRepositoryImpl,
    relations: ThreadRelationRepositoryImpl,
    audit: ThreadGroupAuditRepositoryImpl,
    markers: ThreadDeletionMarkerRepositoryImpl,
}

impl ThreadGroupPurgeService {
    pub fn new(pool: &'static RdbPool) -> Self {
        let id_generator = IdGeneratorWrapper::new();
        Self {
            pool,
            groups: ThreadGroupRepositoryImpl::new(id_generator.clone(), pool),
            members: ThreadGroupMemberRepositoryImpl::new(pool),
            relations: ThreadRelationRepositoryImpl::new(id_generator.clone(), pool),
            audit: ThreadGroupAuditRepositoryImpl::new(id_generator, pool),
            markers: ThreadDeletionMarkerRepositoryImpl::new(pool),
        }
    }

    pub async fn preview(&self, group_id: i64) -> anyhow::Result<Option<PurgePreview>> {
        let Some(group) = self.groups.find_by_id(group_id).await? else {
            return Ok(None);
        };
        let members = self.members.list_all_by_group_id(group_id).await?;
        let inactive_memberships = members
            .iter()
            .filter(|m| m.state != values::member_state::ACTIVE)
            .count() as i64;
        let active_memberships = members
            .iter()
            .filter(|m| m.state == values::member_state::ACTIVE)
            .count() as i64;
        let keys: HashSet<&str> = members
            .iter()
            .map(|m| m.thread_canonical_key.as_str())
            .collect();
        let mut inactive_relations = 0i64;
        let mut active_relations = 0i64;
        let mut seen = HashSet::new();
        for key in &keys {
            for relation in self
                .relations
                .list_by_child_canonical_key(key, None)
                .await?
                .into_iter()
                .chain(
                    self.relations
                        .list_by_parent_canonical_key(key, None)
                        .await?,
                )
            {
                if !seen.insert(relation.id) {
                    continue;
                }
                // Cross-group / shared relations (one endpoint outside
                // this group) are retained, never purged (design 4.8).
                if !keys.contains(relation.parent_thread_canonical_key.as_str())
                    || !keys.contains(relation.child_thread_canonical_key.as_str())
                {
                    continue;
                }
                if relation.state == values::relation_state::ACTIVE {
                    active_relations += 1;
                } else {
                    inactive_relations += 1;
                }
            }
        }
        let audit_rows = self.audit.list_by_group_id(group_id).await?.len() as i64;
        // A marker is purgeable only when no membership outside this
        // group references its identity; otherwise it is retained
        // (design 4.8).
        let mut purgeable_deletion_markers = 0i64;
        let mut retained_deletion_markers = 0i64;
        let mut purgeable_identities: Vec<String> = Vec::new();
        for member in &members {
            if member.state != values::member_state::DELETED {
                continue;
            }
            let Some(source) = member.source.as_deref() else {
                continue;
            };
            let Some(native_id) = member.native_id.as_deref() else {
                continue;
            };
            let scope = member.identity_scope.as_deref().unwrap_or_default();
            let key = SourceIdentityKey {
                owner_scope: &member.owner_scope,
                source,
                identity_scope: scope,
                native_id,
            };
            if self.markers.find(&key).await?.is_none() {
                continue;
            }
            if self
                .members
                .exists_source_identity_outside_group(
                    &member.owner_scope,
                    source,
                    scope,
                    native_id,
                    group_id,
                )
                .await?
            {
                retained_deletion_markers += 1;
            } else {
                purgeable_deletion_markers += 1;
                purgeable_identities.push(format!(
                    "{}|{}|{}|{}",
                    member.owner_scope, source, scope, native_id
                ));
            }
        }
        purgeable_identities.sort();
        let dangling_redirects = self
            .groups
            .list_by_status(values::group_status::REDIRECTED, None, None)
            .await?
            .into_iter()
            .filter(|group| group.redirect_to_group_id == Some(group_id))
            .count() as i64;
        let digest = sha256_hex(
            serde_json::json!({
                "group_id": group_id,
                "status": group.status,
                "inactive_memberships": inactive_memberships,
                "active_memberships": active_memberships,
                "inactive_relations": inactive_relations,
                "active_relations": active_relations,
                "audit_rows": audit_rows,
                "purgeable_deletion_markers": purgeable_deletion_markers,
                "purgeable_identities": purgeable_identities,
                "dangling_redirects": dangling_redirects,
            })
            .to_string()
            .as_bytes(),
        );
        Ok(Some(PurgePreview {
            group_id,
            group_status: group.status,
            inactive_memberships,
            active_memberships,
            inactive_relations,
            active_relations,
            audit_rows,
            purgeable_deletion_markers,
            retained_deletion_markers,
            dangling_redirects,
            digest,
        }))
    }

    /// Execute a previewed purge. Rejects a stale digest or a group that
    /// still has active members.
    pub async fn delete(
        &self,
        group_id: i64,
        expected_digest: &str,
    ) -> anyhow::Result<PurgeOutcome> {
        ensure_thread_group_writes()?;
        let preview = self
            .preview(group_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("thread group {group_id} not found"))?;
        if preview.digest != expected_digest {
            anyhow::bail!("purge preview is stale; request a new preview");
        }
        if preview.active_memberships > 0 {
            anyhow::bail!("cannot purge a group with active members");
        }
        // A split group has no single redirect target, so inbound
        // redirects would dangle; require the operator to resolve them
        // first.
        if preview.group_status == values::group_status::SPLIT && preview.dangling_redirects > 0 {
            anyhow::bail!(
                "cannot purge a split group that other groups redirect to; \
                 resolve those redirects first"
            );
        }
        let group = self
            .groups
            .find_by_id(group_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("thread group {group_id} not found"))?;
        let members = self.members.list_all_by_group_id(group_id).await?;
        let keys: HashSet<&str> = members
            .iter()
            .map(|m| m.thread_canonical_key.as_str())
            .collect();
        let mut inactive_relation_ids = HashSet::new();
        for key in &keys {
            for relation in self
                .relations
                .list_by_child_canonical_key(key, None)
                .await?
                .into_iter()
                .chain(
                    self.relations
                        .list_by_parent_canonical_key(key, None)
                        .await?,
                )
            {
                // Only relations fully inside the group are purgeable;
                // cross-group / shared history is retained.
                if relation.state != values::relation_state::ACTIVE
                    && keys.contains(relation.parent_thread_canonical_key.as_str())
                    && keys.contains(relation.child_thread_canonical_key.as_str())
                {
                    inactive_relation_ids.insert(relation.id);
                }
            }
        }
        let audit_ids: Vec<i64> = self
            .audit
            .list_by_group_id(group_id)
            .await?
            .into_iter()
            .map(|row| row.id)
            .collect();

        let mut tx = self.pool.begin().await?;
        let now = command_utils::util::datetime::now_millis();
        let mut deleted_relations = 0usize;
        for id in inactive_relation_ids {
            if self.relations.delete_by_id_tx(&mut *tx, id).await? {
                deleted_relations += 1;
            }
        }
        let mut deleted_audit_rows = 0usize;
        for id in audit_ids {
            if self.audit.delete_by_id_tx(&mut *tx, id).await? {
                deleted_audit_rows += 1;
            }
        }
        let deleted_memberships = self
            .members
            .delete_by_group_id_tx(&mut *tx, group_id)
            .await?;

        // Marker purge runs after the memberships are gone, so the
        // reference check sees only history outside this group. A marker
        // referenced elsewhere is retained (design 4.8).
        let mut deleted_deletion_markers = 0usize;
        for member in &members {
            if member.state != values::member_state::DELETED {
                continue;
            }
            let Some(source) = member.source.as_deref() else {
                continue;
            };
            let Some(native_id) = member.native_id.as_deref() else {
                continue;
            };
            let scope = member.identity_scope.as_deref().unwrap_or_default();
            let key = SourceIdentityKey {
                owner_scope: &member.owner_scope,
                source,
                identity_scope: scope,
                native_id,
            };
            if self.markers.find_tx(&mut *tx, &key).await?.is_none() {
                continue;
            }
            if self
                .members
                .exists_source_identity_outside_group_tx(
                    &mut *tx,
                    &member.owner_scope,
                    source,
                    scope,
                    native_id,
                    group_id,
                )
                .await?
            {
                continue;
            }
            if self.markers.consume_tx(&mut *tx, &key).await? {
                deleted_deletion_markers += 1;
            }
        }

        // Flatten redirects that pointed at this group so no dangling
        // target remains.
        let mut repointed_redirects = 0usize;
        if let Some(target) = group.redirect_to_group_id {
            for redirect in self
                .groups
                .list_by_status(values::group_status::REDIRECTED, None, None)
                .await?
            {
                if redirect.redirect_to_group_id == Some(group_id) {
                    self.groups
                        .repoint_redirect_tx(&mut *tx, redirect.id, target, now)
                        .await?;
                    repointed_redirects += 1;
                }
            }
        }

        let group_deleted = self.groups.delete_tx(&mut *tx, group_id).await?;
        tx.commit().await?;
        Ok(PurgeOutcome {
            deleted_memberships,
            deleted_relations,
            deleted_audit_rows,
            deleted_deletion_markers,
            repointed_redirects,
            group_deleted,
        })
    }
}
