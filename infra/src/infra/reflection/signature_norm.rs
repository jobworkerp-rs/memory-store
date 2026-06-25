//! `failure_signature_indicator_norm` access. Same usage as the
//! failure mode dictionary: cached in memory at boot for the F-S7
//! distance metric path; runtime mutations flow through migrations.

use super::rows::FailureSignatureIndicatorNormRow;
use crate::error::LlmMemoryError;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};

const LIST_ALL_SQL: &str = "SELECT indicator_name, max_value, weight \
                            FROM failure_signature_indicator_norm \
                            ORDER BY indicator_name ASC;";

#[async_trait]
pub trait FailureSignatureIndicatorNormRepository: UseRdbPool + Send + Sync {
    async fn list_all(&self) -> Result<Vec<FailureSignatureIndicatorNormRow>> {
        sqlx::query_as::<Rdb, FailureSignatureIndicatorNormRow>(LIST_ALL_SQL)
            .fetch_all(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }
}

pub struct FailureSignatureIndicatorNormRepositoryImpl {
    pool: &'static RdbPool,
}

impl FailureSignatureIndicatorNormRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for FailureSignatureIndicatorNormRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl FailureSignatureIndicatorNormRepository for FailureSignatureIndicatorNormRepositoryImpl {}
