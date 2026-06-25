/// LanceDB record for one reflection intent vector chunk row.
///
/// N-row schema: a reflection owns N of these keyed by
/// `(memory_id, vector_kind, chunk_index)`. `vector_kind` is always
/// "text" for intents. `content` is the per-chunk substring of the
/// `task_intent`. Filter columns (origin_user_id / origin_channel /
/// task_category / reflection_aspect / outcome) mirror the sidecar and
/// are duplicated across every chunk row so F-S3 / F-S8 can prune
/// candidates without crossing back into RDB for every hit.
#[derive(Debug, Clone)]
pub struct ReflectionIntentVectorRecord {
    pub memory_id: i64,
    pub vector_kind: String,
    pub chunk_index: i32,
    pub begin_position: i32,
    pub end_position: i32,
    pub content: String,
    pub origin_user_id: i64,
    pub origin_channel: Option<String>,
    pub task_category: i32,
    pub reflection_aspect: i32,
    pub outcome: i32,
    pub embedding: Vec<f32>,
    pub embedding_model: Option<String>,
    pub created_at: i64,
}

/// Sidecar-derived filter context shared by every chunk row of one
/// reflection. The app layer reads it once from the RDB sidecar and
/// stamps it onto each chunk record.
#[derive(Debug, Clone)]
pub struct ReflectionIntentFilterContext {
    pub origin_user_id: i64,
    pub origin_channel: Option<String>,
    pub task_category: i32,
    pub reflection_aspect: i32,
    pub outcome: i32,
    pub created_at: i64,
}

impl ReflectionIntentVectorRecord {
    /// Build one N-row chunk record from the shared filter context plus
    /// the per-chunk vector_kind / chunk_index / offsets / content /
    /// embedding. `vector_kind` is "text" for intents.
    #[allow(clippy::too_many_arguments)]
    pub fn from_chunk_with_content(
        memory_id: i64,
        ctx: &ReflectionIntentFilterContext,
        embedding: &[f32],
        embedding_model: Option<&str>,
        vector_kind: &str,
        chunk_index: i32,
        begin_position: i32,
        end_position: i32,
        content: String,
    ) -> Self {
        Self {
            memory_id,
            vector_kind: vector_kind.to_string(),
            chunk_index,
            begin_position,
            end_position,
            content,
            origin_user_id: ctx.origin_user_id,
            origin_channel: ctx.origin_channel.clone(),
            task_category: ctx.task_category,
            reflection_aspect: ctx.reflection_aspect,
            outcome: ctx.outcome,
            embedding: embedding.to_vec(),
            embedding_model: embedding_model.map(|s| s.to_string()),
            created_at: ctx.created_at,
        }
    }
}
