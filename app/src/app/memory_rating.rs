use anyhow::Result;
use async_trait::async_trait;
use infra::infra::memory_rating::rdb::{
    MemoryRatingRepository, MemoryRatingRepositoryImpl, UseMemoryRatingRepository,
};
use infra_utils::infra::rdb::UseRdbPool;
use memory_utils::cache::stretto::UseMemoryCache;
use memory_utils::lock::RwLockWithKey;
use protobuf::llm_memory::data::{
    MemoryId, MemoryRating, MemoryRatingData, MemoryRatingId, UserId,
};
use std::{sync::Arc, time::Duration};
use stretto::AsyncCache;

#[async_trait]
pub trait MemoryRatingApp:
    UseMemoryRatingRepository
    + UseMemoryCache<Arc<String>, MemoryRating>
    + Send
    + Sync
    + Sized
    + 'static
{
    async fn create_memory_rating(&self, data: &MemoryRatingData) -> Result<MemoryRatingId> {
        let db = self.memory_rating_repository().db_pool();
        let mut tx = db
            .begin()
            .await
            .map_err(infra::error::LlmMemoryError::DBError)?;
        let id = self
            .memory_rating_repository()
            .create(&mut *tx, data)
            .await?;
        tx.commit()
            .await
            .map_err(infra::error::LlmMemoryError::DBError)?;
        Ok(id)
    }

    async fn upsert_memory_rating(&self, data: &MemoryRatingData) -> Result<MemoryRatingId> {
        let db = self.memory_rating_repository().db_pool();
        let mut tx = db
            .begin()
            .await
            .map_err(infra::error::LlmMemoryError::DBError)?;
        let id = self
            .memory_rating_repository()
            .upsert(&mut *tx, data)
            .await?;
        tx.commit()
            .await
            .map_err(infra::error::LlmMemoryError::DBError)?;
        // clear cache for this rating
        let k = Arc::new(Self::find_cache_key(&id.value));
        let _ = self.delete_cache(&k).await;
        Ok(id)
    }

    async fn update_memory_rating(
        &self,
        id: &MemoryRatingId,
        data: &Option<MemoryRatingData>,
    ) -> Result<bool> {
        if let Some(d) = data {
            let pool = self.memory_rating_repository().db_pool();
            let mut tx = pool
                .begin()
                .await
                .map_err(infra::error::LlmMemoryError::DBError)?;
            self.memory_rating_repository()
                .update(&mut *tx, id, d)
                .await?;
            tx.commit()
                .await
                .map_err(infra::error::LlmMemoryError::DBError)?;
            let k = Arc::new(Self::find_cache_key(&id.value));
            let _ = self.delete_cache(&k).await;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn delete_memory_rating(&self, id: &MemoryRatingId) -> Result<bool> {
        let r = self.memory_rating_repository().delete(id).await;
        let k = Arc::new(Self::find_cache_key(&id.value));
        let _ = self.delete_cache(&k).await;
        r
    }

    fn find_cache_key(id: &i64) -> String {
        ["memory_rating_id:", &id.to_string()].join("")
    }

    async fn find_memory_rating(
        &self,
        id: &MemoryRatingId,
        ttl: Option<&Duration>,
    ) -> Result<Option<MemoryRating>>
    where
        Self: Send + 'static,
    {
        let k = Arc::new(Self::find_cache_key(&id.value));
        self.with_cache_if_some(&k, ttl, || async {
            self.memory_rating_repository().find(id).await
        })
        .await
    }

    async fn find_by_memory_id(&self, memory_id: &MemoryId) -> Result<Vec<MemoryRating>> {
        self.memory_rating_repository()
            .find_by_memory_id(memory_id)
            .await
    }

    async fn find_by_user_id(
        &self,
        user_id: &UserId,
        limit: Option<&i32>,
    ) -> Result<Vec<MemoryRating>> {
        self.memory_rating_repository()
            .find_by_user_id(user_id, limit)
            .await
    }

    async fn find_by_memory_and_user(
        &self,
        memory_id: &MemoryId,
        user_id: &UserId,
    ) -> Result<Option<MemoryRating>> {
        self.memory_rating_repository()
            .find_by_memory_and_user(memory_id, user_id)
            .await
    }

    async fn count(&self) -> Result<i64>
    where
        Self: Send + 'static,
    {
        self.memory_rating_repository()
            .count_list_tx(self.memory_rating_repository().db_pool())
            .await
    }
}

pub struct MemoryRatingAppImpl {
    memory_rating_repository: MemoryRatingRepositoryImpl,
    memory_cache: AsyncCache<Arc<String>, MemoryRating>,
    key_lock: RwLockWithKey<Arc<String>>,
    default_ttl: Duration,
}

impl MemoryRatingAppImpl {
    const DEFAULT_TTL_SEC: u64 = 60;
    pub fn new(
        memory_rating_repository: MemoryRatingRepositoryImpl,
        memory_cache: AsyncCache<Arc<String>, MemoryRating>,
    ) -> Self {
        Self {
            memory_rating_repository,
            memory_cache,
            key_lock: RwLockWithKey::new(16 * 1024),
            default_ttl: Duration::from_secs(Self::DEFAULT_TTL_SEC),
        }
    }
}

impl UseMemoryRatingRepository for MemoryRatingAppImpl {
    fn memory_rating_repository(&self) -> &MemoryRatingRepositoryImpl {
        &self.memory_rating_repository
    }
}

impl MemoryRatingApp for MemoryRatingAppImpl {}

impl UseMemoryCache<Arc<String>, MemoryRating> for MemoryRatingAppImpl {
    fn cache(&self) -> &AsyncCache<Arc<String>, MemoryRating> {
        &self.memory_cache
    }

    fn default_ttl(&self) -> Option<&Duration> {
        Some(&self.default_ttl)
    }

    fn key_lock(&self) -> &RwLockWithKey<Arc<String>> {
        &self.key_lock
    }
}
