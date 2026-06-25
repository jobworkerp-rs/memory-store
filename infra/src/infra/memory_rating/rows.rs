use protobuf::llm_memory::data::{
    MemoryId, MemoryRating, MemoryRatingData, MemoryRatingId, UserId,
};

#[derive(sqlx::FromRow)]
pub struct MemoryRatingRow {
    pub id: i64,
    pub memory_id: i64,
    pub user_id: i64,
    pub rating: f64,
    pub metadata: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl MemoryRatingRow {
    pub fn to_proto(&self) -> MemoryRating {
        MemoryRating {
            id: Some(MemoryRatingId { value: self.id }),
            data: Some(MemoryRatingData {
                // memory_id and user_id are always non-zero (enforced by validate_required_fields)
                memory_id: Some(MemoryId {
                    value: self.memory_id,
                }),
                user_id: Some(UserId {
                    value: self.user_id,
                }),
                rating: self.rating as f32,
                metadata: self.metadata.clone(),
                created_at: self.created_at,
                updated_at: self.updated_at,
            }),
        }
    }
}
