#![cfg(feature = "postgres")]

use app::app::memory::MemoryAppImpl;
use app::app::thread_group::ThreadGroupPurgeService;
use app::app::thread_group::memory_relation::{
    GroupMemoryDeletePolicy, ThreadGroupMemoryRelationService,
};
use infra::infra::memory::rdb::{MemoryRepository, MemoryRepositoryImpl};
use infra::infra::memory_rating::rdb::MemoryRatingRepositoryImpl;
use infra::infra::thread::rdb::{ThreadRepository, ThreadRepositoryImpl};
use infra::infra::thread_group::group::{ThreadGroupRepository, ThreadGroupRepositoryImpl};
use infra::infra::thread_group::lock::{ThreadGroupLockRepository, ThreadGroupLockRepositoryImpl};
use infra::infra::thread_group::rows::NewThreadGroup;
use infra::infra::thread_label::rdb::{ThreadLabelRepository, ThreadLabelRepositoryImpl};
use infra::infra::thread_memory::rdb::{ThreadMemoryRepository, ThreadMemoryRepositoryImpl};
use infra_utils::infra::test::{TEST_RUNTIME, setup_test_rdb_from};
use protobuf::llm_memory::data::{MemoryData, MemoryKind, ThreadData, UserId};

#[test]
fn postgres_purge_waits_for_group_writes_and_deletes_owned_memory_atomically() -> anyhow::Result<()>
{
    TEST_RUNTIME.block_on(async {
        let pool = setup_test_rdb_from("../infra/sql/postgres").await;
        let ids = infra::test_helper::shared_id_generator();
        let groups = ThreadGroupRepositoryImpl::new(ids.clone(), pool);
        let now = command_utils::util::datetime::now_millis();
        let group_id = groups
            .create_tx(
                pool,
                &NewThreadGroup {
                    user_id: 1,
                    group_canonical_key: format!("{:064x}", now),
                    title: None,
                    status: "split".into(),
                    grouping_authority: "reconciler".into(),
                    redirect_to_group_id: None,
                    created_at: now,
                    updated_at: now,
                },
            )
            .await?;
        let thread_id = ThreadRepositoryImpl::new(ids.clone(), pool)
            .create(
                pool,
                &ThreadData {
                    user_id: Some(UserId { value: 1 }),
                    ..Default::default()
                },
            )
            .await?;
        ThreadLabelRepositoryImpl::new(pool)
            .add_labels(
                thread_id.value,
                &["thread_group_summary".into(), format!("group_{group_id}")],
                now,
            )
            .await?;
        let memory_id = MemoryRepositoryImpl::new(ids.clone(), pool)
            .create(
                pool,
                &MemoryData {
                    user_id: Some(UserId { value: 1 }),
                    content: "postgres summary".into(),
                    memory_kind: MemoryKind::DerivedSummary as i32,
                    external_id: Some(format!("thread-group-summary:{group_id}")),
                    ..Default::default()
                },
            )
            .await?;
        ThreadMemoryRepositoryImpl::new(pool)
            .insert_auto_position_tx(pool, thread_id.value, memory_id.value, now)
            .await?;
        ThreadGroupMemoryRelationService::new(pool)
            .link_existing(
                group_id,
                memory_id.value,
                "summary",
                GroupMemoryDeletePolicy::Delete,
            )
            .await?;

        let config = memory_utils::cache::stretto::MemoryCacheConfig::default();
        let app = MemoryAppImpl::new(
            MemoryRepositoryImpl::new(ids.clone(), pool),
            MemoryRatingRepositoryImpl::new(ids.clone(), pool),
            ThreadRepositoryImpl::new(ids, pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            memory_utils::cache::stretto::new_memory_cache(&config),
            memory_utils::cache::stretto::new_memory_cache(&config),
            None,
        );
        let purge = ThreadGroupPurgeService::new(pool).with_memory_app(std::sync::Arc::new(app));
        let preview = purge.preview(group_id).await?.unwrap();
        let mut blocker = pool.begin().await?;
        ThreadGroupLockRepositoryImpl::new(pool)
            .lock_group_membership_tx(&mut *blocker)
            .await?;
        let purge_task = tokio::spawn(async move { purge.delete(group_id, &preview.digest).await });
        let mut purge_task = purge_task;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut purge_task)
                .await
                .is_err()
        );
        blocker.commit().await?;
        let outcome = purge_task.await??;
        assert_eq!(outcome.deleted_memory_ids, vec![memory_id.value]);
        assert!(
            MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find(&memory_id, false)
                .await?
                .is_none()
        );
        assert!(
            ThreadRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find(&thread_id)
                .await?
                .is_some()
        );
        Ok(())
    })
}
