//! Whether the RDB holds any embedding target (startup rule 3 input).
//!
//! Uses the same predicates as dispatch so "has targets" never disagrees
//! with what dispatch would embed. Stops at the first target, so the
//! common cases (empty RDB, or a first row that is a target) are cheap.

use crate::infra::embedding_dispatch::ImageSearchMode;
use crate::infra::embedding_target::{dispatch_kinds, thread_description_is_target};
use crate::infra::media_object::rdb::{MediaObjectRepository, MediaObjectRepositoryImpl};
use crate::infra::memory::rdb::{MemoryRepository, MemoryRepositoryImpl};
use crate::infra::thread::rdb::{ThreadRepository, ThreadRepositoryImpl};
use anyhow::Result;
use std::collections::HashMap;

const PAGE_SIZE: i32 = 500;

pub struct RdbTargetSources<'a> {
    /// Memory-derived targets (text, media, reflection) are counted when
    /// a memory-side vector table exists.
    pub memories: Option<(&'a MemoryRepositoryImpl, &'a MediaObjectRepositoryImpl)>,
    /// Thread description targets are counted when the thread table exists.
    pub threads: Option<&'a ThreadRepositoryImpl>,
    pub image_search_mode: ImageSearchMode,
}

pub async fn rdb_has_embedding_target(sources: &RdbTargetSources<'_>) -> Result<bool> {
    if let Some((memory_repo, media_repo)) = sources.memories
        && any_memory_target(memory_repo, media_repo, sources.image_search_mode).await?
    {
        return Ok(true);
    }
    if let Some(thread_repo) = sources.threads {
        return any_thread_target(thread_repo).await;
    }
    Ok(false)
}

async fn any_memory_target(
    memory_repo: &MemoryRepositoryImpl,
    media_repo: &MediaObjectRepositoryImpl,
    mode: ImageSearchMode,
) -> Result<bool> {
    let mut cursor = 0i64;
    loop {
        let page = memory_repo
            .find_list_by_condition_after_id_with_memory_kinds(
                PAGE_SIZE,
                cursor,
                &[],
                &[],
                None,
                None,
            )
            .await?;
        let Some(last) = page.last().and_then(|m| m.id.as_ref()).map(|id| id.value) else {
            return Ok(false);
        };
        let media_ids: Vec<i64> = page
            .iter()
            .filter_map(|m| m.data.as_ref()?.media_object_id.map(|id| id.value))
            .collect();
        let media: HashMap<i64, (i32, String)> = if media_ids.is_empty() {
            HashMap::new()
        } else {
            media_repo
                .find_by_ids(&media_ids)
                .await?
                .into_iter()
                .map(|r| (r.id, (r.kind, r.storage_backend)))
                .collect()
        };
        let found = page.iter().filter_map(|m| m.data.as_ref()).any(|d| {
            let linked = d.media_object_id.and_then(|id| media.get(&id.value));
            !dispatch_kinds(
                &d.content,
                d.role,
                d.content_type,
                linked.map(|(kind, _)| *kind),
                linked.map(|(_, backend)| backend.as_str()),
                mode,
            )
            .is_empty()
        });
        if found {
            return Ok(true);
        }
        if page.len() < PAGE_SIZE as usize {
            return Ok(false);
        }
        cursor = last;
    }
}

async fn any_thread_target(thread_repo: &ThreadRepositoryImpl) -> Result<bool> {
    let mut offset = 0i64;
    loop {
        let page = thread_repo
            .find_list(Some(&PAGE_SIZE), Some(&offset))
            .await?;
        if page
            .iter()
            .filter_map(|t| t.data.as_ref())
            .any(|d| thread_description_is_target(d.description.as_deref().unwrap_or("")))
        {
            return Ok(true);
        }
        if page.len() < PAGE_SIZE as usize {
            return Ok(false);
        }
        offset += i64::from(PAGE_SIZE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use infra_utils::infra::rdb::{RdbPool, UseRdbPool};
    use protobuf::llm_memory::data::{ContentType, MemoryData, MessageRole, UserId};

    async fn setup_pool() -> &'static RdbPool {
        use infra_utils::infra::test::setup_test_rdb_from;
        let (dir, reset) = if cfg!(feature = "postgres") {
            ("sql/postgres", "TRUNCATE TABLE memory, thread CASCADE;")
        } else {
            (
                "sql/sqlite",
                "DELETE FROM thread_memory; DELETE FROM memory; DELETE FROM thread;",
            )
        };
        let pool = setup_test_rdb_from(dir).await;
        for stmt in reset.split(';').filter(|s| !s.trim().is_empty()) {
            sqlx::query(stmt).execute(pool).await.unwrap();
        }
        pool
    }

    async fn insert(
        repo: &MemoryRepositoryImpl,
        content: &str,
        role: MessageRole,
        ct: ContentType,
    ) {
        let data = MemoryData {
            user_id: Some(UserId { value: 1 }),
            content: content.to_string(),
            content_type: ct as i32,
            role: role as i32,
            ..Default::default()
        };
        let mut tx = repo.db_pool().begin().await.unwrap();
        repo.create(&mut *tx, &data).await.unwrap();
        tx.commit().await.unwrap();
    }

    #[test]
    fn finds_targets_with_the_dispatch_predicates() -> anyhow::Result<()> {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let idg = crate::test_helper::shared_id_generator();
            let memory = MemoryRepositoryImpl::new(idg.clone(), pool);
            let media = MediaObjectRepositoryImpl::new(idg.clone(), pool);
            let sources = RdbTargetSources {
                memories: Some((&memory, &media)),
                threads: None,
                image_search_mode: ImageSearchMode::None,
            };
            assert!(!rdb_has_embedding_target(&sources).await?);

            // Not targets: tool content, blank text, disallowed role.
            insert(&memory, "Read()", MessageRole::RoleUser, ContentType::Tool).await;
            insert(&memory, "   ", MessageRole::RoleUser, ContentType::Text).await;
            insert(&memory, "x", MessageRole::RoleTool, ContentType::Text).await;
            assert!(!rdb_has_embedding_target(&sources).await?);

            insert(&memory, "hello", MessageRole::RoleUser, ContentType::Text).await;
            assert!(rdb_has_embedding_target(&sources).await?);

            let no_memories = RdbTargetSources {
                memories: None,
                ..sources
            };
            assert!(!rdb_has_embedding_target(&no_memories).await?);
            Ok(())
        })
    }
}
