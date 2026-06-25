use super::rows::MemoryRatingRow;
use crate::error::LlmMemoryError;
use crate::infra::IdGeneratorWrapper;
use crate::infra::UseIdGenerator;
use crate::sql::{memory_rating_columns, p, p_jsonb};
use anyhow::{Context, Result};
use async_trait::async_trait;
use infra_utils::infra::rdb::Rdb;
use infra_utils::infra::rdb::RdbPool;
use infra_utils::infra::rdb::UseRdbPool;
use itertools::Itertools;
use protobuf::llm_memory::data::{
    MemoryId, MemoryRating, MemoryRatingData, MemoryRatingId, UserId,
};
use sqlx::Executor;

const INSERT_SQL: &str = concat!(
    "INSERT INTO memory_rating (id, memory_id, user_id, rating, metadata, created_at, updated_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ",",
    p_jsonb!(5),
    ",",
    p!(6),
    ",",
    p!(7),
    ")"
);

// RETURNING clause requires SQLite >= 3.35.0 (sqlx 0.8 bundles 3.45+)
const UPSERT_SQL: &str = concat!(
    "INSERT INTO memory_rating (id, memory_id, user_id, rating, metadata, created_at, updated_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ",",
    p_jsonb!(5),
    ",",
    p!(6),
    ",",
    p!(7),
    ") ON CONFLICT (memory_id, user_id) DO UPDATE SET rating = excluded.rating, metadata = excluded.metadata, updated_at = excluded.updated_at RETURNING id"
);

const UPDATE_SQL: &str = concat!(
    "UPDATE memory_rating SET rating = ",
    p!(1),
    ", metadata = ",
    p_jsonb!(2),
    ", updated_at = ",
    p!(3),
    " WHERE id = ",
    p!(4),
    ";"
);

const DELETE_SQL: &str = concat!("DELETE FROM memory_rating WHERE id = ", p!(1), ";");

const FIND_SQL: &str = concat!(
    "SELECT ",
    memory_rating_columns!(),
    " FROM memory_rating WHERE id = ",
    p!(1),
    ";"
);

const FIND_BY_MEMORY_ID_SQL: &str = concat!(
    "SELECT ",
    memory_rating_columns!(),
    " FROM memory_rating WHERE memory_id = ",
    p!(1),
    " ORDER BY created_at DESC;"
);

const FIND_BY_USER_ID_LIMIT_SQL: &str = concat!(
    "SELECT ",
    memory_rating_columns!(),
    " FROM memory_rating WHERE user_id = ",
    p!(1),
    " ORDER BY updated_at DESC LIMIT ",
    p!(2),
    ";"
);

const FIND_BY_USER_ID_ALL_SQL: &str = concat!(
    "SELECT ",
    memory_rating_columns!(),
    " FROM memory_rating WHERE user_id = ",
    p!(1),
    " ORDER BY updated_at DESC;"
);

const FIND_BY_MEMORY_AND_USER_SQL: &str = concat!(
    "SELECT ",
    memory_rating_columns!(),
    " FROM memory_rating WHERE memory_id = ",
    p!(1),
    " AND user_id = ",
    p!(2),
    ";"
);

const DELETE_BY_MEMORY_ID_SQL: &str =
    concat!("DELETE FROM memory_rating WHERE memory_id = ", p!(1), ";");

const COUNT_SQL: &str = "SELECT count(*) as count FROM memory_rating;";

fn validate_rating(rating: f32) -> Result<()> {
    if !(-1.0..=1.0).contains(&rating) {
        return Err(LlmMemoryError::RuntimeError(format!(
            "rating must be in [-1.0, +1.0], got: {}",
            rating
        ))
        .into());
    }
    Ok(())
}

fn validate_required_fields(data: &MemoryRatingData) -> Result<()> {
    match data.memory_id {
        None => {
            return Err(LlmMemoryError::RuntimeError("memory_id is required".to_string()).into());
        }
        Some(m) if m.value == 0 => {
            return Err(
                LlmMemoryError::RuntimeError("memory_id must be non-zero".to_string()).into(),
            );
        }
        _ => {}
    }
    match data.user_id {
        None => return Err(LlmMemoryError::RuntimeError("user_id is required".to_string()).into()),
        Some(u) if u.value == 0 => {
            return Err(
                LlmMemoryError::RuntimeError("user_id must be non-zero".to_string()).into(),
            );
        }
        _ => {}
    }
    Ok(())
}

#[async_trait]
pub trait MemoryRatingRepository: UseRdbPool + UseIdGenerator + Sync + Send {
    async fn create<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        data: &MemoryRatingData,
    ) -> Result<MemoryRatingId> {
        validate_required_fields(data)?;
        validate_rating(data.rating)?;
        let id: i64 = self.id_generator().generate_id()?;
        let (created_at, updated_at) =
            crate::infra::fill_timestamps(data.created_at, data.updated_at);
        let res = sqlx::query::<Rdb>(INSERT_SQL)
            .bind(id)
            .bind(data.memory_id.unwrap().value)
            .bind(data.user_id.unwrap().value)
            .bind(data.rating as f64)
            .bind(&data.metadata)
            .bind(created_at)
            .bind(updated_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        if res.rows_affected() > 0 {
            Ok(MemoryRatingId { value: id })
        } else {
            Err(LlmMemoryError::RuntimeError(format!(
                "Cannot insert memory_rating (logic error?): {:?}",
                data
            ))
            .into())
        }
    }

    async fn upsert<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        data: &MemoryRatingData,
    ) -> Result<MemoryRatingId> {
        validate_required_fields(data)?;
        validate_rating(data.rating)?;
        let id: i64 = self.id_generator().generate_id()?;
        let (created_at, updated_at) =
            crate::infra::fill_timestamps(data.created_at, data.updated_at);
        // RETURNING id gives the actual row id atomically (new id on insert, existing id on conflict)
        let returned_id: i64 = sqlx::query_scalar::<Rdb, i64>(UPSERT_SQL)
            .bind(id)
            .bind(data.memory_id.unwrap().value)
            .bind(data.user_id.unwrap().value)
            .bind(data.rating as f64)
            .bind(&data.metadata)
            .bind(created_at)
            .bind(updated_at)
            .fetch_one(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(MemoryRatingId { value: returned_id })
    }

    /// Updates rating and metadata only. `memory_id` and `user_id` in `data` are
    /// intentionally ignored because the (memory_id, user_id) pair forms a UNIQUE constraint.
    async fn update<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &MemoryRatingId,
        data: &MemoryRatingData,
    ) -> Result<bool> {
        validate_rating(data.rating)?;
        let updated_at = crate::infra::fill_updated_at(data.updated_at);
        sqlx::query(UPDATE_SQL)
            .bind(data.rating as f64)
            .bind(&data.metadata)
            .bind(updated_at)
            .bind(id.value)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(LlmMemoryError::DBError)
            .context(format!("error in update: id = {}", id.value))
    }

    async fn delete(&self, id: &MemoryRatingId) -> Result<bool> {
        self.delete_tx(self.db_pool(), id).await
    }

    async fn delete_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &MemoryRatingId,
    ) -> Result<bool> {
        sqlx::query::<Rdb>(DELETE_SQL)
            .bind(id.value)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(LlmMemoryError::DBError)
            .context(format!("error in delete: id = {}", id.value))
    }

    async fn find(&self, id: &MemoryRatingId) -> Result<Option<MemoryRating>> {
        sqlx::query_as::<Rdb, MemoryRatingRow>(FIND_SQL)
            .bind(id.value)
            .fetch_optional(self.db_pool())
            .await
            .map(|r| r.map(|row| row.to_proto()))
            .map_err(LlmMemoryError::DBError)
            .context(format!("error in find: id = {}", id.value))
    }

    async fn find_by_memory_id(&self, memory_id: &MemoryId) -> Result<Vec<MemoryRating>> {
        sqlx::query_as::<_, MemoryRatingRow>(FIND_BY_MEMORY_ID_SQL)
            .bind(memory_id.value)
            .fetch_all(self.db_pool())
            .await
            .map(|rows| rows.into_iter().map(|r| r.to_proto()).collect_vec())
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in find_by_memory_id: memory_id = {}",
                memory_id.value
            ))
    }

    async fn find_by_user_id(
        &self,
        user_id: &UserId,
        limit: Option<&i32>,
    ) -> Result<Vec<MemoryRating>> {
        if let Some(l) = limit {
            sqlx::query_as::<_, MemoryRatingRow>(FIND_BY_USER_ID_LIMIT_SQL)
                .bind(user_id.value)
                .bind(l)
                .fetch_all(self.db_pool())
        } else {
            sqlx::query_as::<_, MemoryRatingRow>(FIND_BY_USER_ID_ALL_SQL)
                .bind(user_id.value)
                .fetch_all(self.db_pool())
        }
        .await
        .map(|rows| rows.into_iter().map(|r| r.to_proto()).collect_vec())
        .map_err(LlmMemoryError::DBError)
        .context(format!(
            "error in find_by_user_id: user_id = {}",
            user_id.value
        ))
    }

    async fn find_by_memory_and_user(
        &self,
        memory_id: &MemoryId,
        user_id: &UserId,
    ) -> Result<Option<MemoryRating>> {
        sqlx::query_as::<_, MemoryRatingRow>(FIND_BY_MEMORY_AND_USER_SQL)
            .bind(memory_id.value)
            .bind(user_id.value)
            .fetch_optional(self.db_pool())
            .await
            .map(|r| r.map(|row| row.to_proto()))
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in find_by_memory_and_user: memory_id = {}, user_id = {}",
                memory_id.value, user_id.value
            ))
    }

    async fn delete_by_memory_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
    ) -> Result<u64> {
        sqlx::query::<Rdb>(DELETE_BY_MEMORY_ID_SQL)
            .bind(memory_id)
            .execute(tx)
            .await
            .map(|r| r.rows_affected())
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in delete_by_memory_id: memory_id = {}",
                memory_id
            ))
    }

    async fn count_list_tx<'c, E: Executor<'c, Database = Rdb>>(&self, tx: E) -> Result<i64> {
        sqlx::query_scalar(COUNT_SQL)
            .fetch_one(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("error in count_list".to_string())
    }
}

pub struct MemoryRatingRepositoryImpl {
    id_generator: IdGeneratorWrapper,
    pool: &'static RdbPool,
}

pub trait UseMemoryRatingRepository {
    fn memory_rating_repository(&self) -> &MemoryRatingRepositoryImpl;
}

impl MemoryRatingRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { id_generator, pool }
    }
}

impl UseRdbPool for MemoryRatingRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for MemoryRatingRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl MemoryRatingRepository for MemoryRatingRepositoryImpl {}

#[cfg(test)]
mod test {
    use super::MemoryRatingRepository;
    use super::MemoryRatingRepositoryImpl;
    use crate::infra::memory::rdb::{MemoryRepository, MemoryRepositoryImpl};
    use anyhow::Context;
    use anyhow::Result;
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::rdb::UseRdbPool;
    use protobuf::llm_memory::data::{MemoryData, MemoryId, MemoryRatingData, UserId};

    async fn _test_crud(pool: &'static RdbPool) -> Result<()> {
        let id_gen = crate::test_helper::shared_id_generator();
        let repo = MemoryRatingRepositoryImpl::new(id_gen, pool);
        let db = repo.db_pool();

        let data = MemoryRatingData {
            memory_id: Some(MemoryId { value: 100 }),
            user_id: Some(UserId { value: 200 }),
            rating: 0.8,
            metadata: Some("\"positive\"".to_string()),
            created_at: 1000,
            updated_at: 2000,
        };

        // create
        let mut tx = db.begin().await.context("begin")?;
        let id = repo.create(&mut *tx, &data).await?;
        assert!(id.value > 0);
        tx.commit().await.context("commit")?;

        // find
        let found = repo.find(&id).await?.expect("should exist");
        let found_data = found.data.unwrap();
        assert_eq!(found_data.memory_id.unwrap().value, 100);
        assert_eq!(found_data.user_id.unwrap().value, 200);
        assert!((found_data.rating - 0.8).abs() < f32::EPSILON);
        assert_eq!(found_data.created_at, 1000);
        assert_eq!(found_data.updated_at, 2000);

        // update
        let update_data = MemoryRatingData {
            memory_id: Some(MemoryId { value: 100 }),
            user_id: Some(UserId { value: 200 }),
            rating: -0.5,
            metadata: Some("\"negative\"".to_string()),
            created_at: 0,
            updated_at: 3000,
        };
        let mut tx = db.begin().await.context("begin")?;
        let updated = repo.update(&mut *tx, &id, &update_data).await?;
        assert!(updated);
        tx.commit().await.context("commit")?;

        // verify update preserved created_at
        let found = repo.find(&id).await?.expect("should exist");
        let found_data = found.data.unwrap();
        assert!((found_data.rating - (-0.5)).abs() < f32::EPSILON);
        assert_eq!(found_data.updated_at, 3000);
        assert_eq!(found_data.created_at, 1000);

        // find_by_memory_id
        let results = repo.find_by_memory_id(&MemoryId { value: 100 }).await?;
        assert_eq!(results.len(), 1);

        // find_by_user_id
        let results = repo.find_by_user_id(&UserId { value: 200 }, None).await?;
        assert_eq!(results.len(), 1);

        // find_by_memory_and_user
        let found = repo
            .find_by_memory_and_user(&MemoryId { value: 100 }, &UserId { value: 200 })
            .await?;
        assert!(found.is_some());

        // count
        let count = repo.count_list_tx(repo.db_pool()).await?;
        assert_eq!(count, 1);

        // delete
        let deleted = repo.delete(&id).await?;
        assert!(deleted);

        let found = repo.find(&id).await?;
        assert!(found.is_none());

        Ok(())
    }

    async fn _test_upsert(pool: &'static RdbPool) -> Result<()> {
        let id_gen = crate::test_helper::shared_id_generator();
        let repo = MemoryRatingRepositoryImpl::new(id_gen, pool);
        let db = repo.db_pool();

        let data = MemoryRatingData {
            memory_id: Some(MemoryId { value: 300 }),
            user_id: Some(UserId { value: 400 }),
            rating: 1.0,
            metadata: None,
            created_at: 0,
            updated_at: 0,
        };

        // first upsert = insert
        let mut tx = db.begin().await.context("begin")?;
        let id1 = repo.upsert(&mut *tx, &data).await?;
        tx.commit().await.context("commit")?;
        assert!(id1.value > 0);

        // second upsert = update (same memory_id + user_id)
        let data2 = MemoryRatingData {
            rating: -1.0,
            metadata: Some("\"changed\"".to_string()),
            ..data.clone()
        };
        let mut tx = db.begin().await.context("begin")?;
        let id2 = repo.upsert(&mut *tx, &data2).await?;
        tx.commit().await.context("commit")?;

        // should return the same existing id
        assert_eq!(id1.value, id2.value);

        // verify only one record exists
        let results = repo.find_by_memory_id(&MemoryId { value: 300 }).await?;
        assert_eq!(results.len(), 1);
        let found_data = results[0].data.as_ref().unwrap();
        assert!((found_data.rating - (-1.0)).abs() < f32::EPSILON);

        // cleanup
        repo.delete(&id1).await?;
        Ok(())
    }

    async fn _test_validation(pool: &'static RdbPool) -> Result<()> {
        let id_gen = crate::test_helper::shared_id_generator();
        let repo = MemoryRatingRepositoryImpl::new(id_gen, pool);
        let db = repo.db_pool();

        // rating > 1.0 should fail
        let data = MemoryRatingData {
            memory_id: Some(MemoryId { value: 500 }),
            user_id: Some(UserId { value: 600 }),
            rating: 1.5,
            metadata: None,
            created_at: 0,
            updated_at: 0,
        };
        let mut tx = db.begin().await.context("begin")?;
        let result = repo.create(&mut *tx, &data).await;
        assert!(result.is_err());
        tx.rollback().await.context("rollback")?;

        // rating < -1.0 should fail
        let data2 = MemoryRatingData {
            rating: -1.1,
            ..data.clone()
        };
        let mut tx = db.begin().await.context("begin")?;
        let result = repo.create(&mut *tx, &data2).await;
        assert!(result.is_err());
        tx.rollback().await.context("rollback")?;

        // upsert should also validate
        let mut tx = db.begin().await.context("begin")?;
        let result = repo.upsert(&mut *tx, &data).await;
        assert!(result.is_err());
        tx.rollback().await.context("rollback")?;

        Ok(())
    }

    async fn _test_cascade_delete(pool: &'static RdbPool) -> Result<()> {
        let id_gen = crate::test_helper::shared_id_generator();
        let memory_repo = MemoryRepositoryImpl::new(id_gen.clone(), pool);
        let rating_repo = MemoryRatingRepositoryImpl::new(id_gen, pool);
        let db = rating_repo.db_pool();

        // create a memory record
        let memory_data = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: 1 }),
            content: "cascade test".to_string(),
            content_type: 0,
            params: None,
            metadata: None,
            created_at: 100,
            updated_at: 100,
            role: 0,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        };
        let mut tx = db.begin().await.context("begin")?;
        let memory_id = memory_repo.create(&mut *tx, &memory_data).await?;
        tx.commit().await.context("commit")?;

        // create ratings for this memory
        let rating_data = MemoryRatingData {
            memory_id: Some(memory_id),
            user_id: Some(UserId { value: 10 }),
            rating: 0.5,
            metadata: None,
            created_at: 0,
            updated_at: 0,
        };
        let mut tx = db.begin().await.context("begin")?;
        let _rating_id = rating_repo.create(&mut *tx, &rating_data).await?;
        tx.commit().await.context("commit")?;

        // verify rating exists
        let ratings = rating_repo.find_by_memory_id(&memory_id).await?;
        assert_eq!(ratings.len(), 1);

        // cascade delete: delete ratings then memory in a single transaction
        let mut tx = db.begin().await.context("begin")?;
        let deleted_count = rating_repo
            .delete_by_memory_id_tx(&mut *tx, memory_id.value)
            .await?;
        assert_eq!(deleted_count, 1);
        memory_repo.delete_tx(&mut *tx, &memory_id).await?;
        tx.commit().await.context("commit")?;

        // verify both are gone
        let ratings = rating_repo.find_by_memory_id(&memory_id).await?;
        assert!(ratings.is_empty());
        let memory = memory_repo.find(&memory_id, false).await?;
        assert!(memory.is_none());

        Ok(())
    }

    async fn _test_timestamp_auto_fill(pool: &'static RdbPool) -> Result<()> {
        let id_gen = crate::test_helper::shared_id_generator();
        let repo = MemoryRatingRepositoryImpl::new(id_gen, pool);
        let db = repo.db_pool();

        let data = MemoryRatingData {
            memory_id: Some(MemoryId { value: 700 }),
            user_id: Some(UserId { value: 800 }),
            rating: 0.0,
            metadata: None,
            created_at: 0,
            updated_at: 0,
        };

        let before = command_utils::util::datetime::now_millis();
        let mut tx = db.begin().await.context("begin")?;
        let id = repo.create(&mut *tx, &data).await?;
        tx.commit().await.context("commit")?;
        let after = command_utils::util::datetime::now_millis();

        let found = repo.find(&id).await?.expect("should exist");
        let found_data = found.data.unwrap();
        assert!(
            found_data.created_at >= before && found_data.created_at <= after,
            "created_at should be auto-filled, got {}",
            found_data.created_at
        );
        assert!(
            found_data.updated_at >= before && found_data.updated_at <= after,
            "updated_at should be auto-filled, got {}",
            found_data.updated_at
        );

        // update with updated_at=0 should auto-fill
        let update_data = MemoryRatingData {
            rating: 0.1,
            updated_at: 0,
            ..data.clone()
        };
        let before2 = command_utils::util::datetime::now_millis();
        let mut tx = db.begin().await.context("begin")?;
        repo.update(&mut *tx, &id, &update_data).await?;
        tx.commit().await.context("commit")?;
        let after2 = command_utils::util::datetime::now_millis();

        let found = repo.find(&id).await?.expect("should exist");
        let found_data = found.data.unwrap();
        assert!(
            found_data.updated_at >= before2 && found_data.updated_at <= after2,
            "updated_at should be auto-filled on update, got {}",
            found_data.updated_at
        );

        // cleanup
        repo.delete(&id).await?;
        Ok(())
    }

    async fn _test_required_fields(pool: &'static RdbPool) -> Result<()> {
        let id_gen = crate::test_helper::shared_id_generator();
        let repo = MemoryRatingRepositoryImpl::new(id_gen, pool);
        let db = repo.db_pool();

        // None memory_id should fail on create
        let data = MemoryRatingData {
            memory_id: None,
            user_id: Some(UserId { value: 1 }),
            rating: 0.5,
            metadata: None,
            created_at: 0,
            updated_at: 0,
        };
        let mut tx = db.begin().await.context("begin")?;
        let result = repo.create(&mut *tx, &data).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("memory_id is required")
        );
        tx.rollback().await.context("rollback")?;

        // None user_id should fail on create
        let data = MemoryRatingData {
            memory_id: Some(MemoryId { value: 1 }),
            user_id: None,
            rating: 0.5,
            metadata: None,
            created_at: 0,
            updated_at: 0,
        };
        let mut tx = db.begin().await.context("begin")?;
        let result = repo.create(&mut *tx, &data).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("user_id is required")
        );
        tx.rollback().await.context("rollback")?;

        // None memory_id should fail on upsert
        let data = MemoryRatingData {
            memory_id: None,
            user_id: Some(UserId { value: 1 }),
            rating: 0.5,
            metadata: None,
            created_at: 0,
            updated_at: 0,
        };
        let mut tx = db.begin().await.context("begin")?;
        let result = repo.upsert(&mut *tx, &data).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("memory_id is required")
        );
        tx.rollback().await.context("rollback")?;

        // Zero memory_id should fail on create
        let data = MemoryRatingData {
            memory_id: Some(MemoryId { value: 0 }),
            user_id: Some(UserId { value: 1 }),
            rating: 0.5,
            metadata: None,
            created_at: 0,
            updated_at: 0,
        };
        let mut tx = db.begin().await.context("begin")?;
        let result = repo.create(&mut *tx, &data).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("memory_id must be non-zero")
        );
        tx.rollback().await.context("rollback")?;

        // Zero user_id should fail on create
        let data = MemoryRatingData {
            memory_id: Some(MemoryId { value: 1 }),
            user_id: Some(UserId { value: 0 }),
            rating: 0.5,
            metadata: None,
            created_at: 0,
            updated_at: 0,
        };
        let mut tx = db.begin().await.context("begin")?;
        let result = repo.create(&mut *tx, &data).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("user_id must be non-zero")
        );
        tx.rollback().await.context("rollback")?;

        Ok(())
    }

    async fn _test_thread_cascade_delete(pool: &'static RdbPool) -> Result<()> {
        use crate::infra::thread::rdb::{ThreadRepository, ThreadRepositoryImpl};
        use crate::infra::thread_memory::rdb::{
            ThreadMemoryRepository, ThreadMemoryRepositoryImpl,
        };
        use protobuf::llm_memory::data::ThreadData;

        let id_gen = crate::test_helper::shared_id_generator();
        let thread_repo = ThreadRepositoryImpl::new(id_gen.clone(), pool);
        let memory_repo = MemoryRepositoryImpl::new(id_gen.clone(), pool);
        let rating_repo = MemoryRatingRepositoryImpl::new(id_gen, pool);
        let tm_repo = ThreadMemoryRepositoryImpl::new(pool);
        let db = rating_repo.db_pool();

        // create a thread
        let thread_data = ThreadData {
            user_id: Some(UserId { value: 1 }),
            default_system_memory_id: None,
            description: None,
            channel: None,
            embedding: None,
            embedding_dim: None,
            created_at: 100,
            updated_at: 100,
            labels: vec![],
            metadata: None,
        };
        let mut tx = db.begin().await.context("begin")?;
        let thread_id = thread_repo.create(&mut *tx, &thread_data).await?;
        tx.commit().await.context("commit")?;

        // create memories for the thread and register them in the junction
        let memory_data = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: 1 }),
            content: "thread cascade test".to_string(),
            content_type: 0,
            params: None,
            metadata: None,
            created_at: 100,
            updated_at: 100,
            role: 0,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        };
        let mut tx = db.begin().await.context("begin")?;
        let memory_id = memory_repo.create(&mut *tx, &memory_data).await?;
        tm_repo
            .insert_auto_position_tx(&mut *tx, thread_id.value, memory_id.value, 100)
            .await?;
        tx.commit().await.context("commit")?;

        // create ratings for the memory
        let rating_data = MemoryRatingData {
            memory_id: Some(memory_id),
            user_id: Some(UserId { value: 10 }),
            rating: 0.7,
            metadata: None,
            created_at: 0,
            updated_at: 0,
        };
        let mut tx = db.begin().await.context("begin")?;
        let _rating_id = rating_repo.create(&mut *tx, &rating_data).await?;
        tx.commit().await.context("commit")?;

        // verify all exist
        assert!(rating_repo.find_by_memory_id(&memory_id).await?.len() == 1);
        assert!(memory_repo.find(&memory_id, false).await?.is_some());

        // Simulate thread deletion cascade via the junction-first path that
        // `ThreadApp::delete_thread` implements in production: resolve thread
        // membership through `thread_memory`, delete ratings, then memories,
        // then the junction rows, then the thread itself.
        let mut tx = db.begin().await.context("begin")?;
        let memory_ids = tm_repo
            .find_memory_ids_by_thread_tx(&mut *tx, thread_id.value)
            .await?;
        for mid in &memory_ids {
            rating_repo
                .delete_by_memory_id_tx(&mut *tx, mid.value)
                .await?;
        }
        for mid in &memory_ids {
            memory_repo.delete_tx(&mut *tx, mid).await?;
        }
        tm_repo
            .delete_by_thread_tx(&mut *tx, thread_id.value)
            .await?;
        thread_repo.delete_tx(&mut *tx, &thread_id).await?;
        tx.commit().await.context("commit")?;

        // verify all are gone
        assert!(rating_repo.find_by_memory_id(&memory_id).await?.is_empty());
        assert!(memory_repo.find(&memory_id, false).await?.is_none());
        assert!(thread_repo.find(&thread_id).await?.is_none());

        Ok(())
    }

    fn setup_pool() -> impl std::future::Future<Output = &'static RdbPool> {
        use infra_utils::infra::test::setup_test_rdb_from;
        async {
            if cfg!(feature = "postgres") {
                let pool = setup_test_rdb_from("sql/postgres").await;
                sqlx::query("TRUNCATE TABLE memory_rating CASCADE;")
                    .execute(pool)
                    .await
                    .unwrap();
                pool
            } else {
                let pool = setup_test_rdb_from("sql/sqlite").await;
                sqlx::query("DELETE FROM memory_rating;")
                    .execute(pool)
                    .await
                    .unwrap();
                pool
            }
        }
    }

    #[test]
    fn run_crud_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_crud(pool).await
        })
    }

    #[test]
    fn run_upsert_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_upsert(pool).await
        })
    }

    #[test]
    fn run_validation_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_validation(pool).await
        })
    }

    #[test]
    fn run_cascade_delete_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_cascade_delete(pool).await
        })
    }

    #[test]
    fn run_timestamp_auto_fill_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_timestamp_auto_fill(pool).await
        })
    }

    #[test]
    fn run_required_fields_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_required_fields(pool).await
        })
    }

    #[test]
    fn run_thread_cascade_delete_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_thread_cascade_delete(pool).await
        })
    }
}
