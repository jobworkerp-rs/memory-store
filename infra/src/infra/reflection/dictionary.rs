//! `failure_mode_dictionary` access. Read-only at runtime; the app
//! layer loads the entire table into a DashMap on boot for hot-path
//! severity / mitigation lookups (spec §4.1.5). Edits / restoration
//! flow through migration files, not RPC, in this release.

use super::rows::FailureModeDictionaryRow;
use crate::error::LlmMemoryError;
use crate::sql::p;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};

const LIST_ALL_SQL: &str = "SELECT mode, description, severity, category, default_mitigation \
                            FROM failure_mode_dictionary ORDER BY mode ASC;";

const FIND_BY_MODE_SQL: &str = concat!(
    "SELECT mode, description, severity, category, default_mitigation \
     FROM failure_mode_dictionary WHERE mode = ",
    p!(1),
    ";"
);

#[async_trait]
pub trait FailureModeDictionaryRepository: UseRdbPool + Send + Sync {
    async fn list_all(&self) -> Result<Vec<FailureModeDictionaryRow>> {
        sqlx::query_as::<Rdb, FailureModeDictionaryRow>(LIST_ALL_SQL)
            .fetch_all(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    async fn find_by_mode(&self, mode: &str) -> Result<Option<FailureModeDictionaryRow>> {
        sqlx::query_as::<Rdb, FailureModeDictionaryRow>(FIND_BY_MODE_SQL)
            .bind(mode)
            .fetch_optional(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }
}

pub struct FailureModeDictionaryRepositoryImpl {
    pool: &'static RdbPool,
}

impl FailureModeDictionaryRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for FailureModeDictionaryRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl FailureModeDictionaryRepository for FailureModeDictionaryRepositoryImpl {}
