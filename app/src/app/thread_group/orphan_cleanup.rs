//! Explicit global cleanup, independent of any single group purge.

use super::prelude::*;
use crate::app::memory::{MemoryApp, MemoryAppImpl, MemoryDeletion};
use infra::infra::thread_group::memory_relation::{
    ThreadGroupMemoryRelationRepositoryImpl, ThreadGroupMemoryRelationRow,
};
use std::sync::Arc;

fn has_exclusive_delete_relation(links: &[ThreadGroupMemoryRelationRow], group_id: i64) -> bool {
    links.len() == 1
        && links[0].group_id == group_id
        && links[0].purpose == "summary"
        && links[0].on_group_delete == "delete"
}

async fn delete_rechecked_index_ids<Exists, ExistsFuture, Delete, DeleteFuture>(
    ids: &[i64],
    mut exists: Exists,
    mut delete: Delete,
) -> Vec<i64>
where
    Exists: FnMut(i64) -> ExistsFuture,
    ExistsFuture: std::future::Future<Output = anyhow::Result<bool>>,
    Delete: FnMut(i64) -> DeleteFuture,
    DeleteFuture: std::future::Future<Output = anyhow::Result<()>>,
{
    let mut failed = Vec::new();
    for &id in ids {
        match exists(id).await {
            Ok(false) => {
                if let Err(error) = delete(id).await {
                    tracing::warn!(id, "global index deletion failed: {error}");
                    failed.push(id);
                }
            }
            Ok(true) => failed.push(id),
            Err(error) => {
                tracing::warn!(id, "global index RDB recheck failed: {error}");
                failed.push(id);
            }
        }
    }
    failed
}

#[derive(Debug)]
pub struct GlobalOrphanPreview {
    pub digest: String,
    pub orphan_relation_count: i64,
    pub safe_memory_ids: Vec<i64>,
    pub unsafe_memory_ids: Vec<i64>,
    pub orphan_memory_index_ids: Vec<i64>,
    pub orphan_thread_index_ids: Vec<i64>,
    pub memory_index_available: bool,
    pub thread_index_available: bool,
}

#[derive(Debug)]
pub struct GlobalOrphanOutcome {
    pub deleted_relation_count: u64,
    pub deleted_memory_ids: Vec<i64>,
    pub failed_memory_index_ids: Vec<i64>,
    pub failed_thread_index_ids: Vec<i64>,
    pub remaining_memory_index_ids: Vec<i64>,
    pub remaining_thread_index_ids: Vec<i64>,
    pub memory_index_available: bool,
    pub thread_index_available: bool,
}

struct OrphanCandidates {
    links: Vec<ThreadGroupMemoryRelationRow>,
    safe_memory_ids: Vec<i64>,
    unsafe_memory_ids: Vec<i64>,
    evidence: Vec<String>,
}

impl OrphanCandidates {
    fn digest(
        &self,
        memory_index_ids: &[i64],
        thread_index_ids: &[i64],
        memory_index_available: bool,
        thread_index_available: bool,
    ) -> String {
        sha256_hex(serde_json::json!({
            "links": self.links.iter().map(|link| (&link.group_id, &link.memory_id, &link.purpose, &link.on_group_delete, &link.created_at)).collect::<Vec<_>>(),
            "safe": self.safe_memory_ids,
            "unsafe": self.unsafe_memory_ids,
            "evidence": self.evidence,
            "memory_index": memory_index_ids,
            "thread_index": thread_index_ids,
            "memory_index_available": memory_index_available,
            "thread_index_available": thread_index_available,
        }).to_string().as_bytes())
    }
}

pub struct ThreadGroupOrphanCleanupService {
    pool: &'static RdbPool,
    groups: ThreadGroupRepositoryImpl,
    memories: MemoryRepositoryImpl,
    relations: ThreadGroupMemoryRelationRepositoryImpl,
    memory_app: Arc<MemoryAppImpl>,
    memory_vector: Option<Arc<crate::app::memory_vector::MemoryVectorAppImpl>>,
    thread_vector: Option<Arc<crate::app::thread_vector::ThreadVectorAppImpl>>,
}

impl ThreadGroupOrphanCleanupService {
    pub fn new(pool: &'static RdbPool, memory_app: Arc<MemoryAppImpl>) -> Self {
        let ids = IdGeneratorWrapper::new();
        Self {
            pool,
            groups: ThreadGroupRepositoryImpl::new(ids.clone(), pool),
            memories: MemoryRepositoryImpl::new(ids, pool),
            relations: ThreadGroupMemoryRelationRepositoryImpl::new(pool),
            memory_app,
            memory_vector: None,
            thread_vector: None,
        }
    }

    pub fn with_vectors(
        mut self,
        memory_vector: Option<Arc<crate::app::memory_vector::MemoryVectorAppImpl>>,
        thread_vector: Option<Arc<crate::app::thread_vector::ThreadVectorAppImpl>>,
    ) -> Self {
        self.memory_vector = memory_vector;
        self.thread_vector = thread_vector;
        self
    }

    async fn candidates(&self) -> anyhow::Result<OrphanCandidates> {
        let links = self.relations.list_orphaned().await?;
        let mut safe = Vec::new();
        let mut unsafe_ids = Vec::new();
        let mut evidence = Vec::new();
        let mut candidates = self.relations.list_legacy_summary_candidates().await?;
        candidates.sort_unstable();
        for (memory_id, external_id) in candidates {
            let Some(group_id) = external_id
                .strip_prefix(super::THREAD_GROUP_SUMMARY_EXTERNAL_ID_PREFIX)
                .and_then(|value| value.parse::<i64>().ok())
                .filter(|id| {
                    *id > 0 && external_id == super::thread_group_summary_external_id(*id)
                })
            else {
                unsafe_ids.push(memory_id);
                continue;
            };
            if self.groups.find_by_id(group_id).await?.is_some() {
                continue;
            }
            let memory = self
                .memories
                .find(
                    &protobuf::llm_memory::data::MemoryId { value: memory_id },
                    false,
                )
                .await?;
            let data = memory.as_ref().and_then(|row| row.data.as_ref());
            let thread_ids = infra::infra::thread_memory::rdb::ThreadMemoryRepository::find_all_threads_by_memory_tx(
                &infra::infra::thread_memory::rdb::ThreadMemoryRepositoryImpl::new(self.pool),
                self.pool, memory_id,
            ).await?;
            let referring_ids = self
                .memories
                .find_referring_memory_ids_tx(self.pool, memory_id)
                .await?;
            evidence.push(
                serde_json::json!([
                    memory_id,
                    external_id,
                    data.map(|d| sha256_hex(d.content.as_bytes())),
                    data.map(|d| d.updated_at),
                    thread_ids,
                    referring_ids,
                ])
                .to_string(),
            );
            let mut tx = self.pool.begin().await?;
            let other = self
                .relations
                .list_by_memory_id_tx(&mut *tx, memory_id)
                .await?;
            let verified = has_exclusive_delete_relation(&other, group_id)
                && super::memory_relation::validate_delete_target_tx(
                    &mut tx, self.pool, group_id, memory_id, "summary",
                )
                .await
                .is_ok();
            if verified {
                safe.push(memory_id);
            } else {
                unsafe_ids.push(memory_id);
            }
        }
        safe.sort_unstable();
        unsafe_ids.sort_unstable();
        evidence.sort();
        Ok(OrphanCandidates {
            links,
            safe_memory_ids: safe,
            unsafe_memory_ids: unsafe_ids,
            evidence,
        })
    }

    pub async fn preview(&self) -> anyhow::Result<GlobalOrphanPreview> {
        let candidates = self.candidates().await?;
        let orphan_memory_index_ids = match &self.memory_vector {
            Some(app) => app.list_orphan_index_ids().await?,
            None => Vec::new(),
        };
        let orphan_thread_index_ids = match &self.thread_vector {
            Some(app) => app.list_orphan_index_ids().await?,
            None => Vec::new(),
        };
        let digest = candidates.digest(
            &orphan_memory_index_ids,
            &orphan_thread_index_ids,
            self.memory_vector.is_some(),
            self.thread_vector.is_some(),
        );
        Ok(GlobalOrphanPreview {
            digest,
            orphan_relation_count: candidates.links.len() as i64,
            safe_memory_ids: candidates.safe_memory_ids,
            unsafe_memory_ids: candidates.unsafe_memory_ids,
            orphan_memory_index_ids,
            orphan_thread_index_ids,
            memory_index_available: self.memory_vector.is_some(),
            thread_index_available: self.thread_vector.is_some(),
        })
    }

    pub async fn execute(&self, expected_digest: &str) -> anyhow::Result<GlobalOrphanOutcome> {
        super::ensure_thread_group_writes()?;
        let mut tx = self.pool.begin().await?;
        super::memory_relation::lock_group_mutations_tx(&mut tx, self.pool).await?;
        // The no-op row update reserves SQLite's single writer before any
        // cross-connection preview reads are made.
        self.groups.reserve_purge_write_tx(&mut *tx, 0).await?;
        let preview = self.preview().await?;
        if preview.digest != expected_digest {
            anyhow::bail!("global orphan preview is stale; request a new preview");
        }
        let mut deleted_relation_count = 0;
        let mut deleted_memories: Vec<(protobuf::llm_memory::data::MemoryId, MemoryDeletion)> =
            Vec::new();
        for memory_id in &preview.safe_memory_ids {
            let memory = self
                .memories
                .find_by_ids_for_update_tx(&mut tx, &[*memory_id])
                .await?;
            let external_id = memory
                .first()
                .and_then(|row| row.data.as_ref())
                .and_then(|data| data.external_id.as_deref())
                .ok_or_else(|| anyhow::anyhow!("orphan Memory {memory_id} changed"))?;
            let group_id = external_id
                .strip_prefix(super::THREAD_GROUP_SUMMARY_EXTERNAL_ID_PREFIX)
                .and_then(|value| value.parse::<i64>().ok())
                .ok_or_else(|| anyhow::anyhow!("orphan Memory {memory_id} changed"))?;
            if self
                .groups
                .find_by_id_tx(&mut *tx, group_id)
                .await?
                .is_some()
            {
                anyhow::bail!("orphan Memory {memory_id} regained a group");
            }
            let links = self
                .relations
                .list_by_memory_id_tx(&mut *tx, *memory_id)
                .await?;
            if !has_exclusive_delete_relation(&links, group_id) {
                anyhow::bail!("orphan Memory {memory_id} has no exclusive DELETE relation");
            }
            super::memory_relation::validate_delete_target_tx(
                &mut tx, self.pool, group_id, *memory_id, "summary",
            )
            .await?;
            let id = protobuf::llm_memory::data::MemoryId { value: *memory_id };
            let effects = self.memory_app.delete_memory_rdb_tx(&mut tx, &id).await?;
            if !effects.deleted {
                anyhow::bail!("orphan Memory {memory_id} disappeared");
            }
            deleted_relation_count += links.len() as u64;
            deleted_memories.push((id, effects));
        }
        for link in self.relations.list_orphaned().await? {
            if self
                .groups
                .find_by_id_tx(&mut *tx, link.group_id)
                .await?
                .is_some()
                && self
                    .memories
                    .find_by_ids_for_update_tx(&mut tx, &[link.memory_id])
                    .await?
                    .len()
                    == 1
            {
                anyhow::bail!("orphan relationship changed during cleanup");
            }
            let memory_exists = self
                .memories
                .find_by_ids_for_update_tx(&mut tx, &[link.memory_id])
                .await?
                .len()
                == 1;
            if memory_exists
                && (link.on_group_delete == "retain"
                    || (link.on_group_delete == "delete"
                        && !preview.safe_memory_ids.contains(&link.memory_id)))
            {
                // Preserve the only durable evidence that an orphan Memory
                // was intentionally retained or could not be verified.
                continue;
            }
            deleted_relation_count += self
                .relations
                .delete_link_tx(&mut *tx, link.group_id, link.memory_id)
                .await?;
        }
        tx.commit().await?;
        let deleted_memory_ids: Vec<i64> =
            deleted_memories.iter().map(|(id, _)| id.value).collect();
        for (id, effects) in deleted_memories {
            self.memory_app.finish_memory_deletion(&id, effects).await;
        }
        let mut memory_ids = preview.orphan_memory_index_ids.clone();
        memory_ids.extend_from_slice(&deleted_memory_ids);
        memory_ids.sort_unstable();
        memory_ids.dedup();
        // Embedding jobs can be delayed; RDB existence is checked again
        // immediately before each destructive index operation.
        let mut failed_memory_index_ids = if let Some(vector) = &self.memory_vector {
            delete_rechecked_index_ids(
                &memory_ids,
                |id| async move {
                    Ok(self
                        .memories
                        .find(&protobuf::llm_memory::data::MemoryId { value: id }, false)
                        .await?
                        .is_some())
                },
                |id| async move { vector.delete_vector(id).await },
            )
            .await
        } else {
            Vec::new()
        };
        let mut failed_thread_index_ids = if let Some(vector) = &self.thread_vector {
            let threads = ThreadRepositoryImpl::new(IdGeneratorWrapper::new(), self.pool);
            delete_rechecked_index_ids(
                &preview.orphan_thread_index_ids,
                |id| {
                    let threads = &threads;
                    async move {
                        Ok(threads
                            .find(&protobuf::llm_memory::data::ThreadId { value: id })
                            .await?
                            .is_some())
                    }
                },
                |id| async move { vector.delete_thread_vector(id).await },
            )
            .await
        } else {
            Vec::new()
        };
        let remaining_memory_index_ids = if let Some(vector) = &self.memory_vector {
            match vector.list_orphan_index_ids().await {
                Ok(ids) => ids,
                Err(error) => {
                    tracing::warn!("global memory index re-scan failed: {error}");
                    failed_memory_index_ids.extend(memory_ids);
                    failed_memory_index_ids.sort_unstable();
                    failed_memory_index_ids.dedup();
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };
        let remaining_thread_index_ids = if let Some(vector) = &self.thread_vector {
            match vector.list_orphan_index_ids().await {
                Ok(ids) => ids,
                Err(error) => {
                    tracing::warn!("global thread index re-scan failed: {error}");
                    failed_thread_index_ids.extend_from_slice(&preview.orphan_thread_index_ids);
                    failed_thread_index_ids.sort_unstable();
                    failed_thread_index_ids.dedup();
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        };
        Ok(GlobalOrphanOutcome {
            deleted_relation_count,
            deleted_memory_ids,
            failed_memory_index_ids,
            failed_thread_index_ids,
            remaining_memory_index_ids,
            remaining_thread_index_ids,
            memory_index_available: self.memory_vector.is_some(),
            thread_index_available: self.thread_vector.is_some(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn orphan_deletion_requires_one_explicit_delete_relation() {
        let link = ThreadGroupMemoryRelationRow {
            group_id: 10,
            memory_id: 20,
            purpose: "summary".into(),
            on_group_delete: "delete".into(),
            created_at: 1,
        };
        assert!(!has_exclusive_delete_relation(&[], 10));
        assert!(has_exclusive_delete_relation(
            std::slice::from_ref(&link),
            10
        ));
        assert!(!has_exclusive_delete_relation(
            &[link.clone(), link.clone()],
            10
        ));
        assert!(!has_exclusive_delete_relation(&[link], 11));
    }

    #[tokio::test]
    async fn index_cleanup_reports_reappeared_rows_errors_and_delete_failures() {
        let deleted = Arc::new(Mutex::new(Vec::new()));
        let attempts = Arc::clone(&deleted);
        let failed = delete_rechecked_index_ids(
            &[1, 2, 3, 4],
            |id| async move {
                if id == 4 {
                    anyhow::bail!("RDB unavailable")
                }
                Ok(id == 2)
            },
            move |id| {
                let attempts = Arc::clone(&attempts);
                async move {
                    attempts.lock().unwrap().push(id);
                    if id == 3 {
                        anyhow::bail!("index unavailable")
                    }
                    Ok(())
                }
            },
        )
        .await;
        assert_eq!(failed, vec![2, 3, 4]);
        assert_eq!(*deleted.lock().unwrap(), vec![1, 3]);
        let empty =
            delete_rechecked_index_ids(&[], |_| async { Ok(false) }, |_| async { Ok(()) }).await;
        assert!(empty.is_empty());
    }
}
