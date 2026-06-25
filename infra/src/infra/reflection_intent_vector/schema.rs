use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

/// Build the Arrow schema for `reflection_intent_vector`.
///
/// N-row schema: one reflection owns N rows keyed by
/// (memory_id, vector_kind, chunk_index), mirroring memory_vector.
/// `vector_kind` is always "text" for intents. Intent search still runs
/// on pure vector distance against `task_intent` chunks (no FTS); the
/// `content` column carries the per-chunk substring for parity with
/// `VectorRow.content` and to surface the matched chunk on a hit. Filter
/// columns (origin_user_id / origin_channel / task_category /
/// reflection_aspect / outcome) are duplicated from the RDB sidecar onto
/// every chunk row so a 2-stage filter (RDB → memory_id IN list → Lance)
/// can prune candidates inexpensively. The four merge-key columns sit
/// right after `memory_id` so they are contiguous; `build_record_batch`
/// MUST keep its column array in this exact order.
pub fn intent_arrow_schema(vector_size: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("memory_id", DataType::Int64, false),
        Field::new("vector_kind", DataType::Utf8, false),
        Field::new("chunk_index", DataType::Int32, false),
        Field::new("begin_position", DataType::Int32, false),
        Field::new("end_position", DataType::Int32, false),
        Field::new("content", DataType::Utf8, false),
        Field::new("origin_user_id", DataType::Int64, false),
        Field::new("origin_channel", DataType::Utf8, true),
        Field::new("task_category", DataType::Int32, false),
        Field::new("reflection_aspect", DataType::Int32, false),
        Field::new("outcome", DataType::Int32, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                vector_size as i32,
            ),
            false,
        ),
        Field::new("embedding_model", DataType::Utf8, true),
        Field::new("created_at", DataType::Int64, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    #[test]
    fn schema_carries_expected_columns_and_vector_dim() {
        let schema = intent_arrow_schema(1024);
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(
            names,
            vec![
                "memory_id".to_string(),
                "vector_kind".to_string(),
                "chunk_index".to_string(),
                "begin_position".to_string(),
                "end_position".to_string(),
                "content".to_string(),
                "origin_user_id".to_string(),
                "origin_channel".to_string(),
                "task_category".to_string(),
                "reflection_aspect".to_string(),
                "outcome".to_string(),
                "embedding".to_string(),
                "embedding_model".to_string(),
                "created_at".to_string(),
            ]
        );

        // vector dim survives round-trip via FixedSizeList encoding.
        let embedding = schema.field_with_name("embedding").unwrap();
        match embedding.data_type() {
            DataType::FixedSizeList(_, dim) => assert_eq!(*dim, 1024),
            other => panic!("expected FixedSizeList, got {other:?}"),
        }
    }
}
