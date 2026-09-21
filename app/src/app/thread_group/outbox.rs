//! Transactional outbox delivery.

use super::prelude::*;

/// Downstream delivery target for outbox events. The production sink
/// enqueues a jobworkerp job keyed by `event_id` (its `uniq_key`), so
/// at-least-once delivery cannot create a second job.
#[async_trait::async_trait]
pub trait ThreadGroupEventSink: Send + Sync {
    async fn enqueue(&self, event_id: &str, event_type: &str, payload: &str) -> anyhow::Result<()>;
}

/// Drains the transactional outbox into a sink. Each event is deleted
/// only after the sink accepted it; a failed enqueue leaves the row for
/// the next pass (the sink's `uniq_key` dedupe makes the retry safe).
pub struct ThreadGroupOutboxDispatcher {
    pool: &'static RdbPool,
    outbox: ThreadGroupEventOutboxRepositoryImpl,
}

impl ThreadGroupOutboxDispatcher {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self {
            pool,
            outbox: ThreadGroupEventOutboxRepositoryImpl::new(pool),
        }
    }

    /// Deliver up to `limit` oldest events. Returns the number of rows
    /// deleted (successfully delivered).
    pub async fn dispatch_once(
        &self,
        sink: &dyn ThreadGroupEventSink,
        limit: Option<i64>,
    ) -> anyhow::Result<usize> {
        let rows = self.outbox.list_oldest(limit, None).await?;
        let mut delivered = 0usize;
        for row in rows {
            sink.enqueue(&row.event_id, &row.event_type, &row.payload)
                .await?;
            let mut tx = self.pool.begin().await?;
            if self.outbox.delete_tx(&mut *tx, &row.event_id).await? {
                delivered += 1;
            }
            tx.commit().await?;
        }
        Ok(delivered)
    }
}
