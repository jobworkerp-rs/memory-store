use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

/// Extract the embedding dimension from an Arrow schema by looking up
/// the `embedding` field and reading the `FixedSizeList` size. Returns
/// `None` when the field is absent or has an unexpected data type, so
/// the caller can fall back to a zero/unknown value in the structured
/// error rather than panicking on a schema-shape change.
///
/// Used by `verify_table_schema_or_fail` in `memory_vector` /
/// `thread_vector` to populate `StartupError::LancedbSchemaMismatch`.
pub fn extract_embedding_dim_from_schema(schema: &Schema) -> Option<u32> {
    let field = schema.field_with_name("embedding").ok()?;
    match field.data_type() {
        DataType::FixedSizeList(_, size) => u32::try_from(*size).ok(),
        _ => None,
    }
}

/// Build Arrow schema for the memories LanceDB table.
/// Follows message-vectordb's arrow_schema() pattern.
pub fn memory_arrow_schema(vector_size: usize) -> Arc<Schema> {
    // Phase 4 (thread): `thread_id` is no longer stored on the LanceDB
    // record. Thread membership lives only in the `thread_memory`
    // junction table on the RDB side, so callers needing a thread-scoped
    // filter walk the junction first and hydrate vectors by `memory_id`.
    //
    // Image memory Phase 4 (N-row): one memory now owns N rows keyed by
    // (memory_id, vector_kind, chunk_index). `vector_kind` distinguishes
    // text / image / caption vectors; `chunk_index` is the 0-based,
    // contiguous position within one (memory_id, vector_kind) (image is
    // always 0). `begin_position`/`end_position` are the embed_text
    // character offsets of the chunk (0 for image). All four are NOT
    // NULL — the legacy single-embedding path writes them as
    // ("text", 0, 0, 0) via the compat mapping (design 1/3 §2.6.2.1).
    // The four columns are placed right after `memory_id` so the merge
    // key columns are contiguous (design 1/3 §3.3). `build_record_batch`
    // MUST keep its column array in this exact order.
    let fields = vec![
        Field::new("memory_id", DataType::Int64, false),
        Field::new("vector_kind", DataType::Utf8, false),
        Field::new("chunk_index", DataType::Int32, false),
        Field::new("begin_position", DataType::Int32, false),
        Field::new("end_position", DataType::Int32, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("content", DataType::Utf8, false),
        Field::new("content_type", DataType::Int32, false),
        Field::new("role", DataType::Int32, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                vector_size as i32,
            ),
            false,
        ),
        Field::new("embedding_model", DataType::Utf8, true), // nullable
        Field::new("metadata_json", DataType::Utf8, true),   // nullable
        Field::new("created_at", DataType::Int64, false),
        Field::new("updated_at", DataType::Int64, false),
        Field::new("indexed_at", DataType::Int64, false),
        Field::new("memory_kind", DataType::Int32, false),
    ];
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::thread_vector::schema::thread_arrow_schema;

    #[test]
    fn extracts_dim_from_memory_schema() {
        let schema = memory_arrow_schema(2048);
        assert_eq!(
            extract_embedding_dim_from_schema(schema.as_ref()),
            Some(2048)
        );
    }

    #[test]
    fn extracts_dim_from_thread_schema() {
        let schema = thread_arrow_schema(768);
        assert_eq!(
            extract_embedding_dim_from_schema(schema.as_ref()),
            Some(768)
        );
    }

    #[test]
    fn returns_none_when_embedding_field_missing() {
        let schema = Schema::new(vec![Field::new("other", DataType::Int64, false)]);
        assert_eq!(extract_embedding_dim_from_schema(&schema), None);
    }

    #[test]
    fn returns_none_when_embedding_field_is_not_fixed_size_list() {
        let schema = Schema::new(vec![Field::new("embedding", DataType::Float32, false)]);
        assert_eq!(extract_embedding_dim_from_schema(&schema), None);
    }
}
