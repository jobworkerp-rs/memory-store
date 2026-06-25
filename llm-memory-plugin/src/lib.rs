//! LLM Memory Plugin for the jobworkerp system.
//! Provides `store`, `find_all`, and `search` methods via `MultiMethodPluginRunner`.

use crate::protobuf::llm::LlmStoreArgs;
use ::protobuf::llm_memory::{data::Memory, service::FindListRequest};
use ::protobuf::{
    llm_memory::data::{MemoryData, UserId},
    memory_store::MemoryStoreClient,
};
use anyhow::{Context, Result, anyhow};
use futures::StreamExt;
use jobworkerp_client::plugins::MultiMethodPluginRunner;
use jobworkerp_client::schema_to_json_string;
use prost::Message;
use protobuf::{
    FindAllArgs, FindAllResult, LlmMemoryPluginSettings, MemoryItem, MemorySearchArgs,
    MemorySearchHit, MemorySearchResult as PluginSearchResult,
};
use std::collections::HashMap;
use std::io::Cursor;

const PLUGIN_NAME: &str = "LlmMemoryPlugin";

#[allow(clippy::doc_markdown, clippy::must_use_candidate)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/memory.rs"));
    #[allow(clippy::doc_markdown, clippy::must_use_candidate)]
    pub mod llm {
        include!(concat!(env!("OUT_DIR"), "/llm.rs"));
    }
}

/// # Panics
/// Panics if plugin initialization fails after `catch_unwind`.
#[allow(improper_ctypes_definitions)]
#[unsafe(no_mangle)]
pub extern "C" fn load_multi_method_plugin() -> Box<dyn MultiMethodPluginRunner + Send + Sync> {
    std::panic::catch_unwind(|| {
        dotenvy::dotenv().ok();
        let p = LlmMemoryPlugin::new();
        Box::new(p)
    })
    .inspect_err(|e| {
        tracing::error!(
            "load_multi_method_plugin panic: {:?}, try to load by default config",
            e
        );
    })
    .unwrap()
}

#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
pub extern "C" fn free_multi_method_plugin(ptr: Box<dyn MultiMethodPluginRunner + Send + Sync>) {
    drop(ptr);
}

#[derive(Debug)]
pub struct LlmMemoryPlugin {
    pub client: Option<MemoryStoreClient>,
    pub model: String,
    pub runtime: tokio::runtime::Runtime,
}

impl Default for LlmMemoryPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl LlmMemoryPlugin {
    const SERVER_URL: &'static str = "http://localhost:9010";

    /// # Panics
    /// Panics if tokio runtime creation fails.
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: None,
            model: String::new(),
            runtime: tokio::runtime::Runtime::new().unwrap(),
        }
    }

    /// # Errors
    /// Returns error if gRPC client connection fails.
    pub fn load_model_from_env(&mut self) -> Result<()> {
        let url_base = std::env::var("SERVER_URL").unwrap_or(Self::SERVER_URL.to_string());
        let client = self.runtime.block_on(async {
            MemoryStoreClient::new(&url_base, std::time::Duration::from_secs(10), false).await
        })?;
        self.client = Some(client);
        Ok(())
    }

    /// # Errors
    /// Returns error if gRPC client connection fails.
    pub async fn create_client(&mut self, settings: LlmMemoryPluginSettings) -> Result<()> {
        let client = MemoryStoreClient::new(
            settings.server_url.as_deref().unwrap_or(Self::SERVER_URL),
            std::time::Duration::from_secs(10),
            false,
        )
        .await?;
        self.client = Some(client);
        Ok(())
    }

    /// # Panics
    /// Panics if client is not initialized (call `load` or `create_client` first).
    ///
    /// # Errors
    /// Returns error if gRPC request fails.
    pub fn find_all_memory(&self) -> Result<Vec<Memory>> {
        self.runtime.block_on(async {
            let client = self.client.as_ref().unwrap();
            let mut response = client
                .memory_client()
                .await
                .find_list(FindListRequest {
                    ..Default::default()
                })
                .await
                .context("find_memory failed")?
                .into_inner();
            let mut memories = Vec::new();
            while let Some(m) = response.message().await? {
                memories.push(m);
            }
            Ok(memories)
        })
    }

    fn memory_to_item(m: &Memory) -> MemoryItem {
        let id = m.id.as_ref().map_or(0, |id| id.value);
        let data = m.data.as_ref();
        // Note: MemoryItem.system_id was removed (reserved 2) in Phase 1 and
        // MemoryItem.thread_id was removed (reserved 12) in Phase 4 of the
        // system-prompt-as-memory migration. System prompts are now
        // represented as ROLE_SYSTEM Memory entries referenced via parent_ids,
        // and thread membership lives in the `thread_memory` junction table
        // on the memories DB side.
        MemoryItem {
            id,
            parent_ids: data
                .map(|d| d.parent_ids.iter().map(|p| p.value).collect())
                .unwrap_or_default(),
            user_id: data.and_then(|d| d.user_id.as_ref()).map_or(0, |u| u.value),
            content: data.map_or_else(String::new, |d| d.content.clone()),
            content_type: data.map_or(0, |d| d.content_type),
            params: data.and_then(|d| d.params.clone()),
            metadata: data.and_then(|d| d.metadata.clone()),
            created_at: data.map_or(0, |d| d.created_at),
            updated_at: data.map_or(0, |d| d.updated_at),
        }
    }

    fn build_memory_request(args: &LlmStoreArgs) -> MemoryData {
        // Collect MemoryIds from histories (only already-stored messages have memory_id)
        let parent_ids = args
            .histories
            .iter()
            .filter_map(|c| c.memory_id)
            .map(|id| ::protobuf::llm_memory::data::MemoryId { value: id })
            .collect();
        // `args.system_prompt_id` is accepted for proto compatibility but no
        // longer mapped into MemoryData.system_id (which was removed in Phase
        // 1 of the system-prompt-as-memory migration). The proper migration
        // path is to represent the system prompt as a ROLE_SYSTEM Memory and
        // include its MemoryId in `parent_ids`. Until Phase 4 rewires the LLM
        // execution path, this field has no effect.
        //
        // We log at debug level (not warn) because it fires on every request
        // that still sets the field during the migration window, and would
        // otherwise drown out real warnings. It is explicitly intended as a
        // troubleshooting aid for integrators who notice their system prompt
        // being ignored.
        if args.system_prompt_id.is_some() {
            tracing::debug!(
                system_prompt_id = ?args.system_prompt_id,
                "LlmStoreArgs.system_prompt_id is ignored after the system-prompt-as-memory \
                 migration (Phase 1). Use a ROLE_SYSTEM Memory referenced through parent_ids \
                 instead; see memories/docs/system-prompt-as-memory-spec.md"
            );
        }
        let now = command_utils::util::datetime::now_millis();

        MemoryData {
            parent_ids,
            user_id: Some(UserId {
                value: args.user_id,
            }),
            content: args.prompt.clone(),
            content_type: 0,
            params: args
                .options
                .as_ref()
                .map(|c| serde_json::to_string(c).unwrap()),
            metadata: None,
            created_at: now,
            updated_at: now,
            role: 0,
            external_id: None,
            // The plugin store path is text-only; media is attached via
            // MediaService, not through this runner.
            media_object_id: None,
            thread_ids: Vec::new(),
        }
    }

    /// Collect streaming search results into a Vec of hits.
    async fn collect_search_stream(
        stream: &mut tonic::Streaming<::protobuf::llm_memory::service::MemorySearchResult>,
    ) -> Result<Vec<MemorySearchHit>> {
        let mut hits = Vec::new();
        while let Some(result) = stream.next().await {
            let r = result.context("search stream error")?;
            hits.push(Self::search_result_to_hit(&r));
        }
        Ok(hits)
    }

    /// Auto-detect search type and dispatch to the appropriate gRPC RPC.
    async fn execute_search(
        client: &MemoryStoreClient,
        args: &MemorySearchArgs,
    ) -> Result<Vec<u8>> {
        use ::protobuf::llm_memory::data as pb;
        use ::protobuf::llm_memory::service as svc;

        let has_vectors = !args.query_vectors.is_empty();
        let has_text = args
            .query_text
            .as_ref()
            .is_some_and(|t| !t.trim().is_empty());

        if !has_vectors && !has_text {
            anyhow::bail!("query_vectors or query_text required");
        }

        let limit = args.limit.unwrap_or(10);
        let include_content = args.include_content.unwrap_or(true);

        // Plugin's own MemorySearchFilter.system_id (reserved 3) and
        // thread_id (reserved 2) were removed as part of the
        // system-prompt-as-memory migration (Phase 1 and Phase 4). System
        // prompts are carried through ROLE_SYSTEM Memory / parent_ids, and
        // thread-scoped searches go through the `thread_memory` junction
        // table on the server side.
        let filter = args.filter.as_ref().map(|f| pb::MemorySearchFilter {
            user_id: f.user_id,
            roles: f.roles.clone(),
            content_types: f.content_types.clone(),
            created_after: f.created_after,
            created_before: f.created_before,
            ..Default::default()
        });

        let search_options = pb::SearchOptions {
            limit,
            distance_type: None,
            filter,
            aggregation_strategy: args.aggregation_strategy,
            include_content: Some(include_content),
        };

        let vectors: Vec<pb::EmbeddingVector> = args
            .query_vectors
            .iter()
            .map(|v| pb::EmbeddingVector {
                values: v.values.clone(),
            })
            .collect();

        let hits = match (has_vectors, has_text) {
            (true, true) => {
                if args.query_vectors.len() > 1 {
                    anyhow::bail!(
                        "hybrid search does not support multiple query vectors; \
                         provide exactly one vector, or use vector-only search for multi-vector aggregation"
                    );
                }
                let hybrid_opts = args.hybrid_options.as_ref();
                let request = svc::HybridSearchRequest {
                    query_vectors: vectors,
                    query_text: args.query_text.clone().unwrap_or_default(),
                    options: Some(search_options),
                    hybrid_options: Some(pb::HybridSearchOptions {
                        strategy: hybrid_opts
                            .and_then(|h| h.strategy)
                            .unwrap_or(protobuf::HybridStrategy::Rrf as i32),
                        vector_weight: hybrid_opts.and_then(|h| h.vector_weight),
                        rrf_k: None,
                    }),
                    fts_options: None,
                };
                let mut stream = client
                    .memory_vector_client()
                    .await
                    .hybrid_search(request)
                    .await
                    .context("hybrid_search failed")?
                    .into_inner();
                Self::collect_search_stream(&mut stream).await?
            }
            (true, false) => {
                let request = svc::VectorSearchRequest {
                    query_vectors: vectors,
                    options: Some(search_options),
                };
                let mut stream = client
                    .memory_vector_client()
                    .await
                    .search_by_vector(request)
                    .await
                    .context("search_by_vector failed")?
                    .into_inner();
                Self::collect_search_stream(&mut stream).await?
            }
            (false, true) => {
                let request = svc::TextSearchRequest {
                    query_text: args.query_text.clone().unwrap_or_default(),
                    options: Some(search_options),
                    fts_options: None,
                };
                let mut stream = client
                    .memory_vector_client()
                    .await
                    .search_by_text(request)
                    .await
                    .context("search_by_text failed")?
                    .into_inner();
                Self::collect_search_stream(&mut stream).await?
            }
            (false, false) => unreachable!(),
        };

        Ok(PluginSearchResult { results: hits }.encode_to_vec())
    }

    fn search_result_to_hit(
        r: &::protobuf::llm_memory::service::MemorySearchResult,
    ) -> MemorySearchHit {
        let memory = r.memory.as_ref();
        let data = memory.and_then(|m| m.data.as_ref());
        // Note: MemorySearchHit.thread_id was removed (reserved 5) in Phase 4.
        // Thread membership is tracked in the `thread_memory` junction table
        // on the memories DB side — clients that need it should resolve it
        // via thread-scoped RPCs rather than from search hits.
        MemorySearchHit {
            memory_id: memory.and_then(|m| m.id.as_ref()).map_or(0, |id| id.value),
            content: data.map(|d| d.content.clone()),
            role: data.map(|d| d.role),
            content_type: data.map(|d| d.content_type),
            score: r.score,
        }
    }
}

impl MultiMethodPluginRunner for LlmMemoryPlugin {
    fn name(&self) -> String {
        PLUGIN_NAME.to_string()
    }

    fn description(&self) -> String {
        "LLM Memory plugin: store, find_all, and search memory data".to_string()
    }

    fn load(&mut self, settings: Vec<u8>) -> Result<()> {
        let settings = LlmMemoryPluginSettings::decode(&mut Cursor::new(settings))
            .map_err(|e| anyhow!("decode error: {e}"))?;
        tracing::info!("{} loaded: {:?}", PLUGIN_NAME, &settings);
        let client = self.runtime.block_on(async {
            MemoryStoreClient::new(
                settings.server_url.as_deref().unwrap_or(Self::SERVER_URL),
                std::time::Duration::from_secs(10),
                false,
            )
            .await
        })?;
        self.client = Some(client);
        Ok(())
    }

    fn run(
        &mut self,
        arg: Vec<u8>,
        _metadata: HashMap<String, String>,
        using: Option<&str>,
    ) -> (Result<Vec<u8>>, HashMap<String, String>) {
        let method = using.unwrap_or("store");

        let Some(client) = self.client.as_ref() else {
            return (Err(anyhow!("client not initialized")), HashMap::new());
        };

        match method {
            "find_all" => {
                let find_args =
                    FindAllArgs::decode(&mut Cursor::new(arg.as_slice())).unwrap_or_default();
                tracing::debug!("{PLUGIN_NAME} run(find_all): {find_args:?}");

                let result = self.runtime.block_on(async {
                    let request = FindListRequest {
                        limit: find_args.limit,
                        offset: find_args.offset,
                    };
                    let mut response = client
                        .memory_client()
                        .await
                        .find_list(request)
                        .await
                        .context("find_all failed")?
                        .into_inner();
                    let mut items = Vec::new();
                    while let Some(m) = response.message().await? {
                        items.push(Self::memory_to_item(&m));
                    }
                    Ok(FindAllResult { memories: items }.encode_to_vec())
                });
                (result, HashMap::new())
            }
            "store" => {
                let args = match LlmStoreArgs::decode(&mut Cursor::new(arg.as_slice())) {
                    Ok(a) => a,
                    Err(e) => return (Err(anyhow!("decode error: {e}")), HashMap::new()),
                };
                tracing::debug!("{PLUGIN_NAME} run(store): {args:?}");

                let request = Self::build_memory_request(&args);
                let result = self
                    .runtime
                    .block_on(async { client.memory_client().await.create(request).await });

                match result {
                    Ok(_) => (Ok(arg), HashMap::new()),
                    Err(e) => (Err(e.into()), HashMap::new()),
                }
            }
            "search" => {
                let args = match MemorySearchArgs::decode(&mut Cursor::new(arg.as_slice())) {
                    Ok(a) => a,
                    Err(e) => return (Err(anyhow!("decode error: {e}")), HashMap::new()),
                };
                tracing::debug!("{PLUGIN_NAME} run(search): {args:?}");

                let result = self
                    .runtime
                    .block_on(async { Self::execute_search(client, &args).await });
                (result, HashMap::new())
            }
            other => (Err(anyhow!("unknown method: {other}")), HashMap::new()),
        }
    }

    fn cancel(&mut self) -> bool {
        tracing::warn!("{PLUGIN_NAME} cancel: not implemented!");
        false
    }

    fn is_canceled(&self) -> bool {
        false
    }

    fn runner_settings_proto(&self) -> String {
        include_str!("../protobuf/settings.proto").to_string()
    }

    fn method_proto_map(
        &self,
    ) -> HashMap<String, jobworkerp_client::jobworkerp::data::MethodSchema> {
        let args_proto = include_str!("../protobuf/llm/store_args.proto").to_string();
        let mut map = HashMap::new();
        map.insert(
            "store".to_string(),
            jobworkerp_client::jobworkerp::data::MethodSchema {
                args_proto: args_proto.clone(),
                result_proto: args_proto.clone(),
                description: Some("Store memory data and return the input args".to_string()),
                output_type: jobworkerp_client::jobworkerp::data::StreamingOutputType::NonStreaming
                    .into(),
                ..Default::default()
            },
        );
        map.insert(
            "find_all".to_string(),
            jobworkerp_client::jobworkerp::data::MethodSchema {
                args_proto: include_str!("../protobuf/find_all_args.proto").to_string(),
                result_proto: include_str!("../protobuf/find_all_result.proto").to_string(),
                description: Some("Find all memories with optional limit/offset".to_string()),
                output_type: jobworkerp_client::jobworkerp::data::StreamingOutputType::NonStreaming
                    .into(),
                ..Default::default()
            },
        );
        map.insert(
            "search".to_string(),
            jobworkerp_client::jobworkerp::data::MethodSchema {
                args_proto: include_str!("../protobuf/search_args.proto").to_string(),
                result_proto: include_str!("../protobuf/search_result.proto").to_string(),
                description: Some(
                    "Search memories by vector, text, or hybrid (auto-detected)".to_string(),
                ),
                output_type: jobworkerp_client::jobworkerp::data::StreamingOutputType::NonStreaming
                    .into(),
                ..Default::default()
            },
        );
        map
    }

    fn settings_schema(&self) -> String {
        schema_to_json_string!(LlmMemoryPluginSettings, "settings_schema")
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_search_args_encode_decode() {
        let args = MemorySearchArgs {
            query_vectors: vec![protobuf::EmbeddingVector {
                values: vec![0.1, 0.2, 0.3],
            }],
            query_text: Some("test query".to_string()),
            limit: Some(5),
            filter: Some(protobuf::MemorySearchFilter {
                user_id: Some(42),
                roles: vec![],
                content_types: vec![],
                created_after: None,
                created_before: None,
            }),
            include_content: Some(true),
            hybrid_options: Some(protobuf::PluginHybridSearchOptions {
                strategy: Some(protobuf::HybridStrategy::Weighted as i32),
                vector_weight: Some(0.7),
            }),
            aggregation_strategy: Some(protobuf::AggregationStrategy::Average as i32),
        };

        let encoded = args.encode_to_vec();
        let decoded = MemorySearchArgs::decode(&mut Cursor::new(&encoded)).unwrap();

        assert_eq!(decoded.query_vectors.len(), 1);
        assert_eq!(decoded.query_vectors[0].values, vec![0.1, 0.2, 0.3]);
        assert_eq!(decoded.query_text, Some("test query".to_string()));
        assert_eq!(decoded.limit, Some(5));
        assert!(decoded.filter.is_some());
        assert_eq!(decoded.filter.unwrap().user_id, Some(42));
        assert_eq!(
            decoded.hybrid_options.as_ref().unwrap().strategy,
            Some(protobuf::HybridStrategy::Weighted as i32)
        );
        assert_eq!(
            decoded.aggregation_strategy,
            Some(protobuf::AggregationStrategy::Average as i32)
        );
    }

    #[test]
    fn test_search_result_encode_decode() {
        let result = PluginSearchResult {
            results: vec![
                MemorySearchHit {
                    memory_id: 1,
                    content: Some("hello".to_string()),
                    role: Some(0),
                    content_type: Some(0),
                    score: 0.95,
                },
                MemorySearchHit {
                    memory_id: 2,
                    content: None,
                    role: None,
                    content_type: None,
                    score: 0.5,
                },
            ],
        };

        let encoded = result.encode_to_vec();
        let decoded = PluginSearchResult::decode(&mut Cursor::new(&encoded)).unwrap();

        assert_eq!(decoded.results.len(), 2);
        assert_eq!(decoded.results[0].memory_id, 1);
        assert_eq!(decoded.results[0].content, Some("hello".to_string()));
        assert!((decoded.results[0].score - 0.95).abs() < 0.001);
        assert_eq!(decoded.results[1].memory_id, 2);
        assert!(decoded.results[1].content.is_none());
    }

    #[test]
    fn test_empty_search_args_detection() {
        // Both empty → should fail
        let args = MemorySearchArgs {
            query_vectors: vec![],
            query_text: None,
            ..Default::default()
        };
        let has_vectors = !args.query_vectors.is_empty();
        let has_text = args
            .query_text
            .as_ref()
            .is_some_and(|t| !t.trim().is_empty());
        assert!(!has_vectors && !has_text);

        // Whitespace-only text → should be treated as empty
        let args2 = MemorySearchArgs {
            query_vectors: vec![],
            query_text: Some("   ".to_string()),
            ..Default::default()
        };
        let has_text2 = args2
            .query_text
            .as_ref()
            .is_some_and(|t| !t.trim().is_empty());
        assert!(!has_text2);
    }
}
