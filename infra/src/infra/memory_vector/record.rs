use protobuf::llm_memory::data::{MemoryData, MemoryKind};

/// Canonical `vector_kind` values. The LanceDB column and the proto
/// `VectorRow.vector_kind` / `replace_kinds` are `Utf8`/`string` at the
/// boundary, so these stay `&str` rather than an enum; centralising them
/// here removes the scattered literals and the silent-typo risk (e.g. a
/// mistyped `"imgae"` in the score_source match would fall through to
/// text-embed unnoticed). Image memory Phase 4.
pub mod vector_kind {
    pub const TEXT: &str = "text";
    pub const IMAGE: &str = "image";
    pub const CAPTION: &str = "caption";
}

/// LanceDB record representing one embedding row for a Memory.
///
/// Phase 4 (thread): `thread_id` is not tracked here — thread
/// membership lives in the `thread_memory` junction table on the RDB
/// side. Callers scoping a vector search to a thread resolve the set of
/// `memory_id`s via the junction first, then pass them as a filter.
///
/// Image memory Phase 4 (N-row): a memory owns N of these keyed by
/// `(memory_id, vector_kind, chunk_index)`. `vector_kind` is
/// "text" | "image" | "caption"; `chunk_index` is the 0-based,
/// contiguous index within one `(memory_id, vector_kind)` (image is
/// always 0). `begin_position`/`end_position` are the embed_text
/// character offsets of this chunk (0 for image). The legacy
/// single-embedding path maps to `("text", 0, 0, 0)` via
/// [`MemoryVectorRecord::from_memory_data`] (design 1/3 §2.6.2.1).
#[derive(Debug, Clone)]
pub struct MemoryVectorRecord {
    pub memory_id: i64,
    pub vector_kind: String,
    pub chunk_index: i32,
    pub begin_position: i32,
    pub end_position: i32,
    pub user_id: i64,
    pub content: String,
    pub content_type: i32,
    pub role: i32,
    pub embedding: Vec<f32>,
    pub embedding_model: Option<String>,
    pub metadata_json: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub indexed_at: i64,
    pub memory_kind: i32,
}

impl MemoryVectorRecord {
    /// Build from MemoryData + embedding for the legacy single-row path.
    ///
    /// This is the compat mapping (design 1/3 §2.6.2.1): the old
    /// `UpsertEmbedding{memory_id, embedding, embedding_model}` carries
    /// no kind/chunk, so it is written as the single
    /// `(memory_id, "text", 0)` row with `begin_position = end_position
    /// = 0`. New N-row callers (the image/caption workflow, long-text
    /// chunker) use [`MemoryVectorRecord::from_chunk`] instead.
    pub fn from_memory_data(
        memory_id: i64,
        data: &MemoryData,
        embedding: &[f32],
        embedding_model: Option<&str>,
    ) -> Self {
        Self::from_chunk(
            memory_id,
            data,
            embedding,
            embedding_model,
            vector_kind::TEXT,
            0,
            0,
            0,
        )
    }

    /// Build one N-row chunk record whose `content` is the whole
    /// `MemoryData.content` (the legacy single-row text mapping). New
    /// N-row callers that have a per-chunk substring use
    /// [`MemoryVectorRecord::from_chunk_with_content`] instead.
    #[allow(clippy::too_many_arguments)]
    pub fn from_chunk(
        memory_id: i64,
        data: &MemoryData,
        embedding: &[f32],
        embedding_model: Option<&str>,
        vector_kind: &str,
        chunk_index: i32,
        begin_position: i32,
        end_position: i32,
    ) -> Self {
        let content = data.content.clone();
        Self::from_chunk_with_content(
            memory_id,
            data,
            embedding,
            embedding_model,
            vector_kind,
            chunk_index,
            begin_position,
            end_position,
            content,
        )
    }

    /// Build one N-row chunk record with an explicit per-chunk `content`
    /// (the substring/label this embedding actually covers — what search
    /// returns as the matched text). The non-content columns (user_id /
    /// role / content_type / metadata / timestamps) still come from the
    /// RDB `MemoryData` so the row stays consistent with the memory.
    /// Used by the `rows` BatchUpsertEmbeddings path and the image /
    /// caption workflow.
    #[allow(clippy::too_many_arguments)]
    pub fn from_chunk_with_content(
        memory_id: i64,
        data: &MemoryData,
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
            user_id: data.user_id.map_or(0, |u| u.value), // 0 = unset (RDB enforces NOT NULL)
            content,
            content_type: data.content_type,
            role: data.role,
            embedding: embedding.to_vec(),
            embedding_model: embedding_model.map(|s| s.to_string()),
            metadata_json: data.metadata.clone(),
            created_at: data.created_at,
            updated_at: data.updated_at,
            indexed_at: command_utils::util::datetime::now_millis(),
            // Legacy RDB rows decode as UNSPECIFIED. Their historical
            // semantics are RAW until the offline classifier
            // assigns a generated kind.
            memory_kind: normalized_memory_kind(data.memory_kind),
        }
    }
}

/// Legacy rows (pre-MemoryKind, or NULL from a manual migration) decode
/// as UNSPECIFIED. Their historical semantics are RAW until an
/// offline classifier assigns a generated kind. Shared across the
/// `infra` vector-store records and the `app` layer's own kind
/// resolution, which all apply the same backfill policy.
pub fn normalized_memory_kind(kind: i32) -> i32 {
    if kind == MemoryKind::Unspecified as i32 {
        MemoryKind::Raw as i32
    } else {
        kind
    }
}
