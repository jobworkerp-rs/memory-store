//! Inactive-history lifecycle and purge operations.

use super::ensure_thread_group_writes;
use super::prelude::*;
use crate::app::memory::{MemoryApp, MemoryAppImpl, MemoryDeletion};
use infra::infra::thread_group::memory_relation::ThreadGroupMemoryRelationRepositoryImpl;
use infra::infra::thread_label::rdb::{ThreadLabelRepository, ThreadLabelRepositoryImpl};
use infra::infra::thread_memory::rdb::ThreadMemoryRepository;
use std::sync::Arc;

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
    pub summary_memory_id: Option<i64>,
    pub delete_memory_count: i64,
    pub retain_memory_count: i64,
    pub blocked_reason: String,
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
    pub deleted_memory_ids: Vec<i64>,
}

/// Inactive-history purge. Conservative by design: deletion markers and
/// observations are retained (they may be shared with active or
/// non-purged history), and a group with active members is rejected
/// rather than partially emptied.
pub struct ThreadGroupPurgeService {
    pool: &'static RdbPool,
    groups: ThreadGroupRepositoryImpl,
    members: ThreadGroupMemberRepositoryImpl,
    memories: MemoryRepositoryImpl,
    thread_memories: infra::infra::thread_memory::rdb::ThreadMemoryRepositoryImpl,
    memory_relations: ThreadGroupMemoryRelationRepositoryImpl,
    relations: ThreadRelationRepositoryImpl,
    audit: ThreadGroupAuditRepositoryImpl,
    markers: ThreadDeletionMarkerRepositoryImpl,
    memory_app: Option<Arc<MemoryAppImpl>>,
}

impl ThreadGroupPurgeService {
    pub fn new(pool: &'static RdbPool) -> Self {
        let id_generator = IdGeneratorWrapper::new();
        Self {
            pool,
            groups: ThreadGroupRepositoryImpl::new(id_generator.clone(), pool),
            members: ThreadGroupMemberRepositoryImpl::new(pool),
            memories: MemoryRepositoryImpl::new(id_generator.clone(), pool),
            thread_memories: infra::infra::thread_memory::rdb::ThreadMemoryRepositoryImpl::new(
                pool,
            ),
            memory_relations: ThreadGroupMemoryRelationRepositoryImpl::new(pool),
            relations: ThreadRelationRepositoryImpl::new(id_generator.clone(), pool),
            audit: ThreadGroupAuditRepositoryImpl::new(id_generator, pool),
            markers: ThreadDeletionMarkerRepositoryImpl::new(pool),
            memory_app: None,
        }
    }

    pub fn with_memory_app(mut self, app: Arc<MemoryAppImpl>) -> Self {
        self.memory_app = Some(app);
        self
    }

    async fn internal_relations(
        &self,
        members: &[ThreadGroupMemberRow],
    ) -> anyhow::Result<Vec<ThreadRelationRow>> {
        let keys: HashSet<&str> = members
            .iter()
            .map(|member| member.thread_canonical_key.as_str())
            .collect();
        let mut seen = HashSet::new();
        let mut internal = Vec::new();
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
                if seen.insert(relation.id)
                    && keys.contains(relation.parent_thread_canonical_key.as_str())
                    && keys.contains(relation.child_thread_canonical_key.as_str())
                {
                    internal.push(relation);
                }
            }
        }
        Ok(internal)
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
        let mut membership_manifest = members
            .iter()
            .map(|member| {
                serde_json::json!([
                    member.thread_canonical_key,
                    member.thread_id,
                    member.state,
                    member.role,
                    member.provenance,
                    member.owner_scope,
                    member.source,
                    member.identity_scope,
                    member.native_id,
                    member.deleted_at,
                    member.created_at,
                    member.updated_at,
                ])
                .to_string()
            })
            .collect::<Vec<_>>();
        membership_manifest.sort();
        let relations = self.internal_relations(&members).await?;
        let mut relation_manifest = relations
            .iter()
            .map(|relation| {
                (
                    relation.id,
                    relation.state.clone(),
                    relation.parent_thread_canonical_key.clone(),
                    relation.child_thread_canonical_key.clone(),
                )
            })
            .collect::<Vec<_>>();
        relation_manifest.sort();
        let mut inactive_relations = 0i64;
        let mut active_relations = 0i64;
        for relation in relations {
            if relation.state == values::relation_state::ACTIVE {
                active_relations += 1;
            } else {
                inactive_relations += 1;
            }
        }
        let audits = self.audit.list_by_group_id(group_id).await?;
        let audit_rows = audits.len() as i64;
        let mut audit_ids = audits.iter().map(|audit| audit.id).collect::<Vec<_>>();
        audit_ids.sort_unstable();
        // A marker is purgeable only when no membership outside this
        // group references its identity; otherwise it is retained
        // (design 4.8).
        let mut purgeable_deletion_markers = 0i64;
        let mut retained_deletion_markers = 0i64;
        let mut purgeable_identities: Vec<String> = Vec::new();
        let mut retained_identities: Vec<String> = Vec::new();
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
            let identity =
                serde_json::json!([member.owner_scope, source, scope, native_id,]).to_string();
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
                retained_identities.push(identity);
            } else {
                purgeable_deletion_markers += 1;
                purgeable_identities.push(identity);
            }
        }
        purgeable_identities.sort();
        retained_identities.sort();
        let mut inbound_redirect_ids = self
            .groups
            .list_by_status(values::group_status::REDIRECTED, None, None)
            .await?
            .into_iter()
            .filter(|group| group.redirect_to_group_id == Some(group_id))
            .map(|group| group.id)
            .collect::<Vec<_>>();
        inbound_redirect_ids.sort_unstable();
        let dangling_redirects = inbound_redirect_ids.len() as i64;
        let memory_links = self.memory_relations.list_by_group_id(group_id).await?;
        let referring_memory_ids = self
            .memories
            .find_referring_memory_ids_for_targets_tx(
                self.pool,
                &memory_links
                    .iter()
                    .map(|link| link.memory_id)
                    .collect::<Vec<_>>(),
            )
            .await?;
        let mut memory_relation_manifest = Vec::with_capacity(memory_links.len());
        for link in &memory_links {
            let memory = self
                .memories
                .find(
                    &protobuf::llm_memory::data::MemoryId {
                        value: link.memory_id,
                    },
                    false,
                )
                .await?;
            let data = memory.as_ref().and_then(|row| row.data.as_ref());
            let mut thread_ids = self
                .thread_memories
                .find_all_threads_by_memory_tx(self.pool, link.memory_id)
                .await?;
            thread_ids.sort_unstable();
            let threads = ThreadRepositoryImpl::new(IdGeneratorWrapper::new(), self.pool);
            let mut storage_threads = Vec::new();
            for thread_id in &thread_ids {
                let thread = threads
                    .find_row_tx(
                        self.pool,
                        &protobuf::llm_memory::data::ThreadId { value: *thread_id },
                    )
                    .await?;
                let mut labels = ThreadLabelRepositoryImpl::new(self.pool)
                    .find_labels_by_thread_tx(self.pool, *thread_id)
                    .await?;
                labels.sort();
                storage_threads.push(serde_json::json!({"thread_id": thread_id, "user_id": thread.map(|t| t.user_id), "labels": labels}));
            }
            let mut default_thread_ids = threads
                .find_thread_ids_by_default_system_memory_for_update_tx(self.pool, link.memory_id)
                .await?;
            default_thread_ids.sort_unstable();
            let references = referring_memory_ids
                .get(&link.memory_id)
                .cloned()
                .unwrap_or_default();
            let sharing = self
                .memory_relations
                .list_by_memory_id(link.memory_id)
                .await?
                .into_iter()
                .map(|row| {
                    (
                        row.group_id,
                        row.purpose,
                        row.on_group_delete,
                        row.created_at,
                    )
                })
                .collect::<Vec<_>>();
            memory_relation_manifest.push(serde_json::json!({
                "memory_id": link.memory_id,
                "purpose": link.purpose,
                "on_group_delete": link.on_group_delete,
                "created_at": link.created_at,
                "exists": memory.is_some(),
                "external_id": data.and_then(|d| d.external_id.as_deref()),
                "memory_kind": data.map(|d| d.memory_kind),
                "user_id": data.and_then(|d| d.user_id.map(|u| u.value)),
                "thread_ids": thread_ids,
                "storage_threads": storage_threads,
                "default_thread_ids": default_thread_ids,
                "referring_memory_ids": references,
                "sharing": sharing,
                "content_hash": data.map(|d| sha256_hex(d.content.as_bytes())),
                "content_type": data.map(|d| d.content_type),
                "role": data.map(|d| d.role),
                "params": data.and_then(|d| d.params.as_ref()),
                "created_at": data.map(|d| d.created_at),
                "updated_at": data.map(|d| d.updated_at),
                "metadata": data.and_then(|d| d.metadata.as_ref()),
                "media_object_id": data.and_then(|d| d.media_object_id.map(|m| m.value)),
                "parent_ids": data.map(|d| d.parent_ids.iter().map(|p| p.value).collect::<Vec<_>>()),
            }));
        }
        let external_id = super::thread_group_summary_external_id(group_id);
        let mut summary_memory_id = None;
        let summary_manifest = if let Some(summary) =
            self.memories.find_by_external_id(&external_id).await?
        {
            let data = summary
                .data
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("summary memory has no data: {external_id}"))?;
            if data.memory_kind != MemoryKind::DerivedSummary as i32 {
                anyhow::bail!("memory at {external_id} has a different kind");
            }
            let memory_id = summary
                .id
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("summary memory has no ID: {external_id}"))?
                .value;
            summary_memory_id = Some(memory_id);
            let mut thread_ids = self
                .thread_memories
                .find_all_threads_by_memory_tx(self.pool, memory_id)
                .await?;
            thread_ids.sort_unstable();
            Some(serde_json::json!({
                "memory_id": memory_id,
                "memory_kind": data.memory_kind,
                "user_id": data.user_id.map(|user| user.value),
                "thread_ids": thread_ids,
                "media_object_id": data.media_object_id.map(|media| media.value),
                "content_hash": sha256_hex(data.content.as_bytes()),
                "content_type": data.content_type,
                "role": data.role,
                "params": data.params,
                "metadata": data.metadata,
                "created_at": data.created_at,
                "updated_at": data.updated_at,
                "parent_ids": data.parent_ids.iter().map(|parent| parent.value).collect::<Vec<_>>(),
            }))
        } else {
            None
        };
        let mut blocked_reason = if summary_memory_id.is_some_and(|id| {
            !memory_links
                .iter()
                .any(|link| link.memory_id == id && link.on_group_delete == "delete")
        }) {
            "legacy summary has no explicit delete relationship".to_string()
        } else {
            String::new()
        };
        for link in memory_links
            .iter()
            .filter(|link| link.on_group_delete == "delete")
        {
            let all_links = self
                .memory_relations
                .list_by_memory_id(link.memory_id)
                .await?;
            if all_links.len() != 1 {
                blocked_reason = format!("memory {} is shared across groups", link.memory_id);
                break;
            }
            if link.purpose != "summary" {
                blocked_reason = format!(
                    "memory {} has an unsupported delete purpose",
                    link.memory_id
                );
                break;
            }
            let memory = self
                .memories
                .find(
                    &protobuf::llm_memory::data::MemoryId {
                        value: link.memory_id,
                    },
                    false,
                )
                .await?;
            let data = memory.as_ref().and_then(|row| row.data.as_ref());
            if data.is_none_or(|data| {
                data.external_id.as_deref()
                    != Some(super::thread_group_summary_external_id(group_id).as_str())
            }) {
                blocked_reason = format!(
                    "memory {} has no verified group summary identity",
                    link.memory_id
                );
                break;
            }
            let data = data.expect("checked above");
            if data.memory_kind != MemoryKind::DerivedSummary as i32 {
                blocked_reason = format!("memory {} has an invalid summary kind", link.memory_id);
                break;
            }
            if let Err(error) = super::memory_relation::validate_delete_summary_identity(
                &link.purpose,
                data,
                group_id,
                link.memory_id,
            ) {
                blocked_reason = error.to_string();
                break;
            }
            let threads = ThreadRepositoryImpl::new(IdGeneratorWrapper::new(), self.pool);
            let thread_ids = self
                .thread_memories
                .find_all_threads_by_memory_tx(self.pool, link.memory_id)
                .await?;
            if thread_ids.len() != 1 {
                blocked_reason = format!(
                    "memory {} does not have a dedicated storage Thread",
                    link.memory_id
                );
                break;
            }
            let thread_id = thread_ids[0];
            let thread = threads
                .find_row_tx(
                    self.pool,
                    &protobuf::llm_memory::data::ThreadId { value: thread_id },
                )
                .await?;
            let labels = ThreadLabelRepositoryImpl::new(self.pool)
                .find_labels_by_thread_tx(self.pool, thread_id)
                .await?;
            let Some(thread) = thread else {
                blocked_reason = format!("memory {} is owned by another user", link.memory_id);
                break;
            };
            if let Err(error) =
                super::memory_relation::validate_delete_owner(data, thread.user_id, link.memory_id)
            {
                blocked_reason = error.to_string();
                break;
            }
            if let Err(error) = super::memory_relation::validate_delete_storage_labels(
                &labels,
                group_id,
                link.memory_id,
            ) {
                blocked_reason = error.to_string();
                break;
            }
            if !threads
                .find_thread_ids_by_default_system_memory_for_update_tx(self.pool, link.memory_id)
                .await?
                .is_empty()
            {
                blocked_reason = format!(
                    "memory {} is referenced as a Thread default",
                    link.memory_id
                );
                break;
            }
            if referring_memory_ids
                .get(&link.memory_id)
                .is_some_and(|referrers| !referrers.is_empty())
            {
                blocked_reason = format!(
                    "memory {} is referenced as a parent by another Memory",
                    link.memory_id
                );
                break;
            }
        }
        if blocked_reason.is_empty() && active_memberships > 0 {
            blocked_reason = "cannot purge a group with active members".into();
        }
        if blocked_reason.is_empty()
            && group.status == values::group_status::SPLIT
            && dangling_redirects > 0
        {
            blocked_reason = "cannot purge a split group with inbound redirects".into();
        }
        if blocked_reason.is_empty()
            && self.memory_app.is_none()
            && memory_links
                .iter()
                .any(|link| link.on_group_delete == "delete")
        {
            blocked_reason = "memory deletion subsystem is unavailable".into();
        }
        let digest = sha256_hex(
            serde_json::json!({
                "group_id": group_id,
                "status": group.status,
                "redirect_to_group_id": group.redirect_to_group_id,
                "membership_manifest": membership_manifest,
                "relation_manifest": relation_manifest,
                "audit_ids": audit_ids,
                "inactive_memberships": inactive_memberships,
                "active_memberships": active_memberships,
                "inactive_relations": inactive_relations,
                "active_relations": active_relations,
                "audit_rows": audit_rows,
                "purgeable_deletion_markers": purgeable_deletion_markers,
                "purgeable_identities": purgeable_identities,
                "retained_identities": retained_identities,
                "retained_deletion_markers": retained_deletion_markers,
                "inbound_redirect_ids": inbound_redirect_ids,
                "dangling_redirects": dangling_redirects,
                "summary": summary_manifest,
                "memory_relations": memory_relation_manifest,
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
            summary_memory_id,
            delete_memory_count: memory_links
                .iter()
                .filter(|link| link.on_group_delete == "delete")
                .count() as i64,
            retain_memory_count: memory_links
                .iter()
                .filter(|link| link.on_group_delete == "retain")
                .count() as i64,
            blocked_reason,
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
        let mut tx = self.pool.begin().await?;
        ThreadGroupLockRepositoryImpl::new(self.pool)
            .lock_group_membership_tx(&mut *tx)
            .await?;
        self.groups
            .reserve_purge_write_tx(&mut *tx, group_id)
            .await?;
        let preview = self
            .preview(group_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("thread group {group_id} not found"))?;
        if preview.digest != expected_digest {
            anyhow::bail!("purge preview is stale; request a new preview");
        }
        let links = self.memory_relations.list_by_group_id(group_id).await?;
        let legacy_summary = self
            .memories
            .find_by_external_id(&super::thread_group_summary_external_id(group_id))
            .await?;
        if let Some(summary) = legacy_summary {
            let memory_id = summary
                .id
                .ok_or_else(|| anyhow::anyhow!("summary has no ID"))?
                .value;
            if !links
                .iter()
                .any(|link| link.memory_id == memory_id && link.on_group_delete == "delete")
            {
                anyhow::bail!("legacy summary has no explicit delete relationship");
            }
        }
        if links.iter().any(|link| link.on_group_delete == "delete") && self.memory_app.is_none() {
            anyhow::bail!("memory deletion subsystem is unavailable");
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
        let inactive_relation_ids = self
            .internal_relations(&members)
            .await?
            .into_iter()
            .filter(|relation| relation.state != values::relation_state::ACTIVE)
            .map(|relation| relation.id)
            .collect::<HashSet<_>>();
        let audit_ids: Vec<i64> = self
            .audit
            .list_by_group_id(group_id)
            .await?
            .into_iter()
            .map(|row| row.id)
            .collect();

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

        let mut deleted_memories: Vec<(protobuf::llm_memory::data::MemoryId, MemoryDeletion)> =
            Vec::new();
        for link in &links {
            if link.on_group_delete != "delete" {
                continue;
            }
            let other_links = self
                .memory_relations
                .list_by_memory_id_tx(&mut *tx, link.memory_id)
                .await?;
            if other_links.len() != 1 || other_links[0].group_id != group_id {
                anyhow::bail!("memory {} is shared across groups", link.memory_id);
            }
            super::memory_relation::validate_delete_target_tx(
                &mut tx,
                self.pool,
                group_id,
                link.memory_id,
                &link.purpose,
            )
            .await?;
            let id = protobuf::llm_memory::data::MemoryId {
                value: link.memory_id,
            };
            let effects = self
                .memory_app
                .as_ref()
                .unwrap()
                .delete_memory_rdb_tx(&mut tx, &id)
                .await?;
            if !effects.deleted {
                anyhow::bail!("memory {} disappeared during purge", link.memory_id);
            }
            deleted_memories.push((id, effects));
        }
        self.memory_relations
            .delete_by_group_id_tx(&mut *tx, group_id)
            .await?;
        let group_deleted = self.groups.delete_tx(&mut *tx, group_id).await?;
        if !group_deleted {
            anyhow::bail!("group {group_id} disappeared during purge");
        }
        tx.commit().await?;
        for (id, effects) in deleted_memories {
            self.memory_app
                .as_ref()
                .unwrap()
                .finish_memory_deletion(&id, effects)
                .await;
        }
        let deleted_memory_ids = links
            .iter()
            .filter(|link| link.on_group_delete == "delete")
            .map(|link| link.memory_id)
            .collect();
        Ok(PurgeOutcome {
            deleted_memberships,
            deleted_relations,
            deleted_audit_rows,
            deleted_deletion_markers,
            repointed_redirects,
            group_deleted,
            deleted_memory_ids,
        })
    }
}
