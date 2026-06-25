use protobuf::llm_memory::data::{Thread, ThreadData, ThreadId, UserId};

// db row definitions
//
// Phase 2 of system-prompt-as-memory migration:
//   `system_prompt_id` was renamed to `default_system_memory_id` and is now
//   nullable. The DB column type changes from `BIGINT NOT NULL` to
//   `BIGINT NULL` (see `infra/sql/{sqlite,postgres}/001_*.sql`). Old
//   deployments must run the matching `manual/003_phase2_*.sql` migration.
#[derive(sqlx::FromRow)]
pub struct ThreadRow {
    pub id: i64,
    pub default_system_memory_id: Option<i64>,
    pub user_id: i64,
    pub description: Option<String>,
    pub channel: Option<String>,
    pub embedding: Option<Vec<u8>>,
    pub embedding_dim: Option<i32>,
    pub created_at: i64,
    pub updated_at: i64,
    pub metadata: Option<String>,
}

impl ThreadRow {
    pub fn to_proto(&self) -> Thread {
        Thread {
            id: Some(ThreadId { value: self.id }),
            data: Some(ThreadData {
                // Pass `Option<i64>` through unchanged. The historical
                // "0 means unset" sentinel logic used by the old
                // `system_prompt_id` field is gone: Phase 2's manual
                // migration normalises existing 0 rows to NULL, and
                // snowflake ids never collide with 0, so no sentinel
                // translation is needed on the read path.
                default_system_memory_id: self.default_system_memory_id,
                user_id: if self.user_id == 0 {
                    None
                } else {
                    Some(UserId {
                        value: self.user_id,
                    })
                },
                description: self.description.clone(),
                channel: self.channel.clone(),
                embedding: self.embedding.clone(),
                embedding_dim: self.embedding_dim,
                created_at: self.created_at,
                updated_at: self.updated_at,
                // Labels are hydrated separately by the app layer
                labels: vec![],
                metadata: self.metadata.clone(),
            }),
        }
    }
}
