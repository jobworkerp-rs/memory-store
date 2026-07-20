pub mod llm_memory {
    // type alias作っておかないと proto自動生成コード内部の依存関係がおかしくなる
    // (protoの自動生成コード内でsuperでクラス参照解決していたため擬似的にdataクラスの位置関係があうようにする)
    pub mod data {
        use protobuf::llm_memory::data;
        pub type UserId = data::UserId;
        pub type MemoryId = data::MemoryId;
        pub type MemoryData = data::MemoryData;
        pub type Memory = data::Memory;
        pub type ThreadId = data::ThreadId;
        pub type ThreadData = data::ThreadData;
        pub type Thread = data::Thread;
        pub type MemoryRatingId = data::MemoryRatingId;
        pub type MemoryRatingData = data::MemoryRatingData;
        pub type MemoryRating = data::MemoryRating;
        // memory_vector types
        pub type EmbeddingVector = data::EmbeddingVector;
        pub type MemorySearchFilter = data::MemorySearchFilter;
        pub type SearchOptions = data::SearchOptions;
        pub type FullTextSearchOptions = data::FullTextSearchOptions;
        pub type HybridSearchOptions = data::HybridSearchOptions;
        pub type DistanceType = data::DistanceType;
        pub type AggregationStrategy = data::AggregationStrategy;
        pub type HybridStrategy = data::HybridStrategy;
        pub type ScoreSource = data::ScoreSource;
        // thread_vector types
        pub type LabelMatchMode = data::LabelMatchMode;
        pub type ContentType = data::ContentType;
        pub type MessageRole = data::MessageRole;
        pub type MemoryKind = data::MemoryKind;
        // media types (image memory feature). The locally generated
        // service/media.proto code references these via `super::data::*`,
        // so the shim must re-alias them even though handler code uses
        // them directly too.
        pub type MediaObjectId = data::MediaObjectId;
        pub type MediaMetadata = data::MediaMetadata;
        pub type MediaPayload = data::MediaPayload;
        pub type ThreadSearchFilter = data::ThreadSearchFilter;
        pub type ThreadSearchOptions = data::ThreadSearchOptions;
        // (P8) sort enums shared by service-side requests.
        pub type MemoryListSort = data::MemoryListSort;
        pub type ThreadListSort = data::ThreadListSort;
        // Service-side generated proto code references these via
        // `super::data::*`, so the shim above must re-alias them
        // even though they are not used directly in handler code.
        pub type HighlightField = data::HighlightField;
        pub type HighlightRange = data::HighlightRange;
        pub type HighlightSource = data::HighlightSource;
        pub type FtsTokenizerKind = data::FtsTokenizerKind;
        // Reflection request / response types are NOT generated inside
        // this crate (see `grpc-admin/build.rs`) — the
        // `reflection.proto` / `reflection_vector.proto` files are
        // compiled only by the shared `protobuf` crate so the trait
        // signatures on `ReflectionApp` line up with the server
        // handler. As a result no `Reflection*` aliases live in this
        // shim; the handler imports them directly from `protobuf::*`.
    }
    pub mod service {
        tonic::include_proto!("llm_memory.service");
    }
}

// Server-reflection metadata is read from the shared `protobuf` crate's
// `FILE_DESCRIPTOR_SET` (it covers reflection RPCs too). The grpc-admin
// build no longer emits its own descriptor file.
