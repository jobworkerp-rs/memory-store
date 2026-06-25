use crate::infra::memory_vector::record::vector_kind;
use protobuf::llm_memory::data::ThreadData;

/// LanceDB record representing one embedding chunk row for a Thread.
///
/// N-row schema: a thread owns N of these keyed by
/// `(thread_id, vector_kind, chunk_index)`. `vector_kind` is always
/// "text" for threads. `content` is the per-chunk substring of the
/// description (the BM25 FTS target / matched text). The legacy
/// single-row path maps to `("text", 0, 0, len)` via
/// [`ThreadVectorRecord::from_thread_data`]. `description` / `labels` /
/// `channel` are duplicated across chunk rows (search hydrates them from
/// RDB, so the LanceDB copies are not search-load-bearing).
#[derive(Debug, Clone)]
pub struct ThreadVectorRecord {
    pub thread_id: i64,
    pub vector_kind: String,
    pub chunk_index: i32,
    pub begin_position: i32,
    pub end_position: i32,
    pub user_id: i64,
    pub content: String,
    pub description: Option<String>,
    pub labels: Vec<String>,
    pub embedding: Vec<f32>,
    pub embedding_model: Option<String>,
    pub channel: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub indexed_at: i64,
}

impl ThreadVectorRecord {
    /// Build the single-row compat record from ThreadData + labels +
    /// embedding, mapped to `("text", 0, 0, len)`. Used by the internal
    /// scalar-sync path (`sync_thread_scalars`), which only needs chunk 0.
    pub fn from_thread_data(
        thread_id: i64,
        data: &ThreadData,
        labels: Vec<String>,
        embedding: &[f32],
        embedding_model: Option<&str>,
    ) -> Self {
        let content = data.description.clone().unwrap_or_default();
        let end = content.chars().count() as i32;
        Self::from_chunk_with_content(
            thread_id,
            data,
            labels,
            embedding,
            embedding_model,
            vector_kind::TEXT,
            0,
            0,
            end,
            content,
        )
    }

    /// Build one N-row chunk record with an explicit per-chunk `content`
    /// (the description substring this embedding covers). The non-content
    /// columns (user_id / description / labels / channel / timestamps)
    /// come from the RDB `ThreadData` + labels so the row stays
    /// consistent with the thread. Used by the `rows` batch upsert path.
    #[allow(clippy::too_many_arguments)]
    pub fn from_chunk_with_content(
        thread_id: i64,
        data: &ThreadData,
        labels: Vec<String>,
        embedding: &[f32],
        embedding_model: Option<&str>,
        vector_kind: &str,
        chunk_index: i32,
        begin_position: i32,
        end_position: i32,
        content: String,
    ) -> Self {
        Self {
            thread_id,
            vector_kind: vector_kind.to_string(),
            chunk_index,
            begin_position,
            end_position,
            user_id: data.user_id.map_or(0, |u| u.value),
            content,
            description: data.description.clone(),
            labels,
            embedding: embedding.to_vec(),
            embedding_model: embedding_model.map(|s| s.to_string()),
            channel: data.channel.clone(),
            created_at: data.created_at,
            updated_at: data.updated_at,
            indexed_at: command_utils::util::datetime::now_millis(),
        }
    }
}
