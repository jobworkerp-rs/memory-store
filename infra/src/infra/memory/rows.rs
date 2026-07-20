use protobuf::llm_memory::data::{MediaObjectId, Memory, MemoryData, MemoryId, UserId};

// db row definitions
#[derive(sqlx::FromRow)]
pub struct MemoryRow {
    pub id: i64,
    pub parent_ids: sqlx::types::Json<Vec<i64>>,
    pub user_id: i64,
    pub content: String,
    pub content_type: i32,
    pub params: Option<String>,
    pub metadata: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub role: i32,
    pub external_id: Option<String>,
    pub media_object_id: Option<i64>,
    pub memory_kind: Option<i32>,
}

impl MemoryRow {
    pub fn to_proto(&self) -> Memory {
        Memory {
            id: Some(MemoryId { value: self.id }),
            data: Some(MemoryData {
                parent_ids: self
                    .parent_ids
                    .0
                    .clone()
                    .into_iter()
                    .map(|id| MemoryId { value: id })
                    .collect(),
                user_id: if self.user_id == 0 {
                    None
                } else {
                    Some(UserId {
                        value: self.user_id,
                    })
                },
                content: self.content.clone(),
                content_type: self.content_type,
                params: self.params.clone(),
                metadata: self.metadata.clone(),
                created_at: self.created_at,
                updated_at: self.updated_at,
                role: self.role,
                external_id: self.external_id.clone(),
                media_object_id: self.media_object_id.map(|value| MediaObjectId { value }),
                thread_ids: Vec::new(),
                memory_kind: self.memory_kind.unwrap_or(0),
            }),
            // Output-only enrichment. The infra layer never resolves
            // media; the app/grpc layer hydrates `media` before responding
            // (design 2/3 §7.5.5).
            media: None,
        }
    }
}

/// Row returned by `find_by_ids_with_position_tx`. Carries the memory's
/// `thread_memory.position` alongside the regular `MemoryRow` columns so
/// callers can order ancestor closures by conversation position without
/// issuing a second round trip.
#[derive(sqlx::FromRow)]
pub struct MemoryWithPositionRow {
    pub id: i64,
    pub parent_ids: sqlx::types::Json<Vec<i64>>,
    pub user_id: i64,
    pub content: String,
    pub content_type: i32,
    pub params: Option<String>,
    pub metadata: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub role: i32,
    pub external_id: Option<String>,
    pub media_object_id: Option<i64>,
    pub memory_kind: Option<i32>,
    pub position: i32,
}

impl MemoryWithPositionRow {
    /// Extract the `MemoryRow` half and convert it to proto; the `position`
    /// is returned separately so callers can decide how to surface it
    /// (either as an ancillary `MemoryWithPosition` wrapper in the app layer
    /// or as part of a proto response).
    pub fn to_proto_with_position(&self) -> (Memory, i32) {
        let row = MemoryRow {
            id: self.id,
            parent_ids: sqlx::types::Json(self.parent_ids.0.clone()),
            user_id: self.user_id,
            content: self.content.clone(),
            content_type: self.content_type,
            params: self.params.clone(),
            metadata: self.metadata.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            role: self.role,
            external_id: self.external_id.clone(),
            media_object_id: self.media_object_id,
            memory_kind: self.memory_kind,
        };
        (row.to_proto(), self.position)
    }
}

/// Row returned by `find_by_external_ids_with_position_tx`. The
/// LEFT JOIN against `thread_memory` produces a NULL position when the
/// memory exists but is not attached to the queried thread (cross-thread
/// collision detection during batch import).
#[derive(sqlx::FromRow)]
pub struct MemoryWithOptionalPositionRow {
    pub id: i64,
    pub parent_ids: sqlx::types::Json<Vec<i64>>,
    pub user_id: i64,
    pub content: String,
    pub content_type: i32,
    pub params: Option<String>,
    pub metadata: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub role: i32,
    pub external_id: Option<String>,
    pub media_object_id: Option<i64>,
    pub memory_kind: Option<i32>,
    pub attached_position: Option<i32>,
}

impl MemoryWithOptionalPositionRow {
    pub fn into_proto_with_position(self) -> (Memory, Option<i32>) {
        let pos = self.attached_position;
        let row = MemoryRow {
            id: self.id,
            parent_ids: self.parent_ids,
            user_id: self.user_id,
            content: self.content,
            content_type: self.content_type,
            params: self.params,
            metadata: self.metadata,
            created_at: self.created_at,
            updated_at: self.updated_at,
            role: self.role,
            external_id: self.external_id,
            media_object_id: self.media_object_id,
            memory_kind: self.memory_kind,
        };
        (row.to_proto(), pos)
    }
}
