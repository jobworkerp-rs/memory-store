use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

/// Build Arrow schema for the threads LanceDB table.
/// Includes List<Utf8> labels column (unlike memory schema).
///
/// N-row schema: one thread owns N rows keyed by
/// (thread_id, vector_kind, chunk_index), mirroring memory_vector.
/// `vector_kind` is always "text" for threads (no image/caption axis).
/// `content` is the per-chunk substring of the description and is the
/// BM25 FTS target (the FTS index moved off `description` so multi-chunk
/// threads index every chunk). The four merge-key columns are placed
/// right after `thread_id` so they are contiguous; `build_record_batch`
/// MUST keep its column array in this exact order. `description` /
/// `labels` / `channel` are duplicated across chunk rows (search hydrates
/// them from RDB, so the LanceDB copies are not search-load-bearing).
pub fn thread_arrow_schema(vector_size: usize) -> Arc<Schema> {
    let fields = vec![
        Field::new("thread_id", DataType::Int64, false),
        Field::new("vector_kind", DataType::Utf8, false),
        Field::new("chunk_index", DataType::Int32, false),
        Field::new("begin_position", DataType::Int32, false),
        Field::new("end_position", DataType::Int32, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("content", DataType::Utf8, false), // per-chunk text, FTS target
        Field::new("description", DataType::Utf8, true), // nullable
        Field::new(
            "labels",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true, // nullable
        ),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                vector_size as i32,
            ),
            false,
        ),
        Field::new("embedding_model", DataType::Utf8, true), // nullable
        Field::new("channel", DataType::Utf8, true),         // nullable
        Field::new("created_at", DataType::Int64, false),
        Field::new("updated_at", DataType::Int64, false),
        Field::new("indexed_at", DataType::Int64, false),
    ];
    Arc::new(Schema::new(fields))
}
