//! Image-memory Phase 5 end-to-end integration (live, #[ignore]).
//!
//! Starts the memories gRPC server in-process and drives the full
//! embedding pipeline against a running jobworkerp + minio:
//!
//!   text:  Create(text)            -> auto-embedding workflow
//!          -> BatchUpsertEmbeddings(rows) callback -> SearchSemantic
//!   image: Upload(png) -> Create(media_object_id)
//!          -> auto-image-embedding workflow -> SearchByMedia
//!
//! The workflow runs asynchronously on jobworkerp (GPU embedding), so
//! both tests POLL the search RPC until the memory shows up (or a
//! generous timeout). Self-skips unless `.env.image-test` exists at the
//! repo root, so CI / offline runs are unaffected. Run with:
//!
//!   docker compose up -d minio postgres   # minio on host :9200
//!   docker run --rm --network host --entrypoint sh minio/mc -c \
//!     'mc alias set local http://127.0.0.1:9200 minioadmin minioadmin \
//!      && mc mb -p local/memories-test'
//!   # jobworkerp running on :9000 with the MultimodalEmbeddingRunner
//!   cargo test -p grpc-admin --test image_memory_e2e -- --ignored \
//!     --test-threads=1 --nocapture

use grpc_admin::protobuf::llm_memory::data::{
    ContentType, MediaObjectId, MemoryData, ThreadData, ThreadId, UserId,
};
use grpc_admin::protobuf::llm_memory::service::{
    AddMemoriesBatchRequest, BatchMemoryInput, FindMediaRequest, FindMemoriesByThreadIdRequest,
    RegisterRequest, SemanticTextSearchRequest, ThreadUpsertByChannel, UploadHeader, UploadRequest,
    add_memories_batch_request, media_service_client::MediaServiceClient,
    memory_service_client::MemoryServiceClient,
    memory_vector_service_client::MemoryVectorServiceClient,
    thread_service_client::ThreadServiceClient, upload_request,
};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tonic::transport::Channel;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent")
        .to_path_buf()
}

/// Load `.env.image-test` into the process env. Returns false (skip)
/// when the file is absent so CI / offline runs do nothing.
fn load_env_or_skip() -> bool {
    let path = repo_root().join(".env.image-test");
    if !path.is_file() {
        eprintln!(
            "skipping: {} not found (see the module header for setup)",
            path.display()
        );
        return false;
    }
    // from_filename does not override already-set vars; fine for a
    // single-process #[ignore] run.
    dotenvy::from_filename(&path).expect("loading .env.image-test");
    true
}

/// Spawn the memories server and wait until its gRPC port accepts a
/// connection (or panic after a bound). Returns the base URL.
async fn start_memories_server() -> String {
    let addr = std::env::var("GRPC_ADDR").expect("GRPC_ADDR (.env.image-test)");
    let url = format!("http://{addr}");
    tokio::spawn(async {
        if let Err(e) = grpc_admin::setup_and_start_front_server().await {
            panic!("memories server failed to start: {e:?}");
        }
    });
    // Poll the port: AppModule init (LanceDB open, etc.) takes a moment.
    for _ in 0..60 {
        if Channel::from_shared(url.clone())
            .unwrap()
            .connect()
            .await
            .is_ok()
        {
            return url;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("memories server did not become reachable at {url} within 30s");
}

/// Poll `f` until it returns `Some` or the timeout elapses. The
/// embedding workflow runs asynchronously on jobworkerp (GPU), so a
/// generous budget is required; the poll interval keeps it responsive.
async fn poll_until<F, Fut, T>(timeout: Duration, mut f: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f().await {
            return Some(v);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// A valid 1x1 red-pixel RGB PNG (zlib-compressed IDAT) — enough for the
/// runner to fetch + decode + embed; the test asserts the pipeline, not
/// image quality. (A hand-written PNG with a bogus deflate stream makes
/// the embedding runner fail with "Corrupt deflate stream", so this is
/// generated with a real zlib encoder.)
const TINY_PNG: &[u8] = &[
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53,
    0xDE, 0x00, 0x00, 0x00, 0x0C, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0xF8, 0xCF, 0xC0, 0x00,
    0x00, 0x03, 0x01, 0x01, 0x00, 0xC9, 0xFE, 0x92, 0xEF, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E,
    0x44, 0xAE, 0x42, 0x60, 0x82,
];

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live e2e: requires jobworkerp:9000 + minio:9200 + .env.image-test"]
async fn text_rows_semantic_e2e() {
    if !load_env_or_skip() {
        return;
    }
    let url = start_memories_server().await;
    let mut memory = MemoryServiceClient::connect(url.clone())
        .await
        .expect("connect MemoryService");
    let vector = MemoryVectorServiceClient::connect(url.clone())
        .await
        .expect("connect MemoryVectorService");

    // A distinctive sentence so the semantic query unambiguously
    // retrieves THIS memory and not unrelated rows from a shared DB.
    let marker = format!("e2e-{}", chrono_now_millis());
    let content =
        format!("The {marker} satellite telemetry anomaly was traced to a thermal sensor.");
    let user_id = 91_000_001;

    let created = memory
        .create(MemoryData {
            user_id: Some(UserId { value: user_id }),
            content: content.clone(),
            content_type: ContentType::Text as i32,
            role: grpc_admin::protobuf::llm_memory::data::MessageRole::RoleUser as i32,
            ..Default::default()
        })
        .await
        .expect("Create text memory")
        .into_inner();
    let memory_id = created.id.expect("Create returns a memory id").value;
    eprintln!("created text memory id={memory_id}");

    // Poll SearchSemantic until the auto-embedding workflow has run on
    // jobworkerp and the BatchUpsertEmbeddings rows callback landed.
    let hit = poll_until(Duration::from_secs(120), || {
        let mut v = vector.clone();
        let q = content.clone();
        async move {
            let stream = v
                .search_semantic(SemanticTextSearchRequest {
                    query_text: q,
                    options: None,
                })
                .await
                .ok()?
                .into_inner();
            collect_memory_ids(stream)
                .await
                .into_iter()
                .find(|&id| id == memory_id)
        }
    })
    .await;

    assert!(
        hit.is_some(),
        "text memory {memory_id} not retrievable via SearchSemantic \
         within 120s — the auto-embedding rows pipeline did not complete"
    );
    eprintln!("text rows e2e OK: SearchSemantic found memory {memory_id}");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live e2e: requires jobworkerp:9000 + minio:9200 + .env.image-test"]
async fn image_media_e2e() {
    if !load_env_or_skip() {
        return;
    }
    let url = start_memories_server().await;
    let mut media = MediaServiceClient::connect(url.clone())
        .await
        .expect("connect MediaService");
    let mut memory = MemoryServiceClient::connect(url.clone())
        .await
        .expect("connect MemoryService");
    let vector = MemoryVectorServiceClient::connect(url.clone())
        .await
        .expect("connect MemoryVectorService");

    let media_object_id = upload_tiny_png(&mut media).await;
    eprintln!("uploaded media id={}", media_object_id.value);

    // Create a memory that references the media (image-only: empty
    // content still dispatches Media).
    let user_id = 91_000_002;
    let created = memory
        .create(MemoryData {
            user_id: Some(UserId { value: user_id }),
            content: String::new(),
            content_type: ContentType::Image as i32,
            role: grpc_admin::protobuf::llm_memory::data::MessageRole::RoleUser as i32,
            media_object_id: Some(MediaObjectId {
                value: media_object_id.value,
            }),
            ..Default::default()
        })
        .await
        .expect("Create image memory")
        .into_inner();
    let memory_id = created.id.expect("Create returns a memory id").value;
    eprintln!("created image memory id={memory_id}");

    // Poll SearchByMedia (query the SAME media) until the
    // auto-image-embedding workflow has produced the image vector.
    let hit = poll_until(Duration::from_secs(180), || {
        let mut v = vector.clone();
        let mid = media_object_id.value;
        async move {
            let stream = v
                .search_by_media(
                    grpc_admin::protobuf::llm_memory::service::MediaSearchRequest {
                        media_object_id: Some(MediaObjectId { value: mid }),
                        options: None,
                    },
                )
                .await
                .ok()?
                .into_inner();
            collect_memory_ids(stream)
                .await
                .into_iter()
                .find(|&id| id == memory_id)
        }
    })
    .await;

    assert!(
        hit.is_some(),
        "image memory {memory_id} not retrievable via SearchByMedia \
         within 180s — the auto-image-embedding pipeline did not complete"
    );
    eprintln!("image e2e OK: SearchByMedia found memory {memory_id}");
}

/// Register-path smoke (no minio object needed): a url-backed media is
/// resolvable and Create wires its ref_count. Cheap sanity that the
/// MediaService Register + Memory link path is sound end to end.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live e2e: requires .env.image-test (memories server only)"]
async fn register_url_media_link_e2e() {
    if !load_env_or_skip() {
        return;
    }
    let url = start_memories_server().await;
    let mut media = MediaServiceClient::connect(url.clone())
        .await
        .expect("connect MediaService");
    let meta = media
        .register(RegisterRequest {
            kind: ContentType::Image as i32,
            media_type: "image/png".to_string(),
            storage_uri: "https://example.invalid/e2e.png".to_string(),
            storage_backend: "url".to_string(),
            ..Default::default()
        })
        .await
        .expect("Register url media")
        .into_inner();
    assert!(
        meta.id.is_some(),
        "Register(url) must return a media_object id"
    );
    eprintln!("register url media OK: id={:?}", meta.id);
}

/// The path agent-chat-import actually uses: `ThreadService.AddMemoriesBatch`
/// (bulk insert), NOT `MemoryService.Create`. Guards the invariant that a
/// batch-inserted memory carrying `media_object_id` gets its
/// `media_object.ref_count` bumped and its image embedding dispatched —
/// without that the image is orphaned (`ref_count=0`, deferred GC would
/// delete it) and never searchable. Asserts the imported image ends up
/// ref-bumped, active, and retrievable via `SearchByMedia` WITHOUT any
/// manual `RedispatchEmbeddings`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live e2e: requires jobworkerp:9000 + minio:9200 + .env.image-test"]
async fn batch_import_image_media_e2e() {
    if !load_env_or_skip() {
        return;
    }
    let url = start_memories_server().await;
    let mut media = MediaServiceClient::connect(url.clone())
        .await
        .expect("connect MediaService");
    let mut threads = ThreadServiceClient::connect(url.clone())
        .await
        .expect("connect ThreadService");
    let vector = MemoryVectorServiceClient::connect(url.clone())
        .await
        .expect("connect MemoryVectorService");

    // Upload the image (same as the Upload step agent-chat-import's
    // client.upload_media does for an inline_base64 attachment).
    let media_object_id = upload_tiny_png(&mut media).await;
    eprintln!("batch import: uploaded media id={}", media_object_id.value);

    // Insert a memory referencing the media via AddMemoriesBatch — the
    // EXACT path the importer takes (build_memory_data sets
    // media_object_id; run_import calls add_memories_batch).
    let user_id = 91_000_010;
    let eid = format!("batch-img-e2e-{}", chrono_now_millis());
    let channel = format!("import:e2e:batch-media:{}", chrono_now_millis());
    let batch = AddMemoriesBatchRequest {
        thread_target: Some(add_memories_batch_request::ThreadTarget::Upsert(
            ThreadUpsertByChannel {
                thread_data: Some(ThreadData {
                    user_id: Some(UserId { value: user_id }),
                    channel: Some(channel.clone()),
                    description: Some("batch image import e2e".to_string()),
                    ..Default::default()
                }),
            },
        )),
        memories: vec![BatchMemoryInput {
            memory: Some(MemoryData {
                user_id: Some(UserId { value: user_id }),
                content: String::new(),
                content_type: ContentType::Image as i32,
                role: grpc_admin::protobuf::llm_memory::data::MessageRole::RoleUser as i32,
                external_id: Some(eid.clone()),
                media_object_id: Some(MediaObjectId {
                    value: media_object_id.value,
                }),
                ..Default::default()
            }),
            parent_external_ids: vec![],
        }],
        upsert_by_external_id: true,
        thread_updated_at_override: 0,
        labels: vec![],
    };
    let resp = threads
        .add_memories_batch(batch)
        .await
        .expect("AddMemoriesBatch with media_object_id")
        .into_inner();
    let memory_id = resp.outcomes[0]
        .memory_id
        .expect("batch outcome has memory id")
        .value;
    eprintln!("batch import: created memory id={memory_id} via AddMemoriesBatch");

    // (1) The media_object must be ref-bumped + active — NOT orphaned
    // (an orphaned image is GC-bait). Find returns metadata only;
    // gc_state isn't on the wire, so the durable proof is the
    // SearchByMedia hit below — an orphaned/un-embedded media never
    // surfaces there. Find is still asserted so a vanished/rejected
    // media fails loudly.
    let found = media
        .find(FindMediaRequest {
            media_object_id: Some(MediaObjectId {
                value: media_object_id.value,
            }),
        })
        .await
        .expect("media_object must still exist after batch import")
        .into_inner();
    assert!(found.id.is_some(), "Find returns the media metadata");

    // (2) Searchable via SearchByMedia WITHOUT any manual
    // RedispatchEmbeddings — proves the batch path dispatched the image
    // embedding and the workflow produced the vector.
    let hit = poll_until(Duration::from_secs(180), || {
        let mut v = vector.clone();
        let mid = media_object_id.value;
        async move {
            let stream = v
                .search_by_media(
                    grpc_admin::protobuf::llm_memory::service::MediaSearchRequest {
                        media_object_id: Some(MediaObjectId { value: mid }),
                        options: None,
                    },
                )
                .await
                .ok()?
                .into_inner();
            collect_memory_ids(stream)
                .await
                .into_iter()
                .find(|&id| id == memory_id)
        }
    })
    .await;

    assert!(
        hit.is_some(),
        "batch-imported image memory {memory_id} not retrievable via \
         SearchByMedia within 180s — AddMemoriesBatch did not bump \
         media_object.ref_count / dispatch the image embedding"
    );
    eprintln!(
        "batch import e2e OK: AddMemoriesBatch image memory {memory_id} \
         is ref-bumped + searchable (no manual redispatch)"
    );
}

// --- helpers -------------------------------------------------------

fn chrono_now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Client-streaming Upload of the embedded test PNG (header + one
/// chunk), returning the new media_object id.
async fn upload_tiny_png(media: &mut MediaServiceClient<Channel>) -> MediaObjectId {
    let header = UploadRequest {
        payload: Some(upload_request::Payload::Header(UploadHeader {
            kind: ContentType::Image as i32,
            media_type: "image/png".to_string(),
            ..Default::default()
        })),
    };
    let chunk = UploadRequest {
        payload: Some(upload_request::Payload::Chunk(TINY_PNG.to_vec())),
    };
    media
        .upload(tokio_stream::iter(vec![header, chunk]))
        .await
        .expect("Upload image")
        .into_inner()
        .media_object_id
        .expect("upload returns media_object_id")
}

/// Drain a MemorySearchResult stream into the list of memory ids it
/// returned (de-dup is server-side; this just reads what came back).
async fn collect_memory_ids(
    mut stream: tonic::Streaming<grpc_admin::protobuf::llm_memory::service::MemorySearchResult>,
) -> Vec<i64> {
    let mut ids = Vec::new();
    while let Ok(Some(item)) = stream.message().await {
        if let Some(m) = item.memory.and_then(|m| m.id) {
            ids.push(m.value);
        }
    }
    ids
}

/// Scan a MemorySearchResult stream for the target memory and return its
/// hydrated `Memory.media` (None until the embedding pipeline has run
/// and the row is searchable).
async fn find_hit_media(
    mut stream: tonic::Streaming<grpc_admin::protobuf::llm_memory::service::MemorySearchResult>,
    memory_id: i64,
) -> Option<grpc_admin::protobuf::llm_memory::data::MediaPayload> {
    while let Ok(Some(item)) = stream.message().await {
        if let Some(m) = item.memory
            && m.id.map(|i| i.value) == Some(memory_id)
        {
            return m.media;
        }
    }
    None
}

/// Search path media enrich: an image memory retrieved via
/// SearchSemantic must carry `Memory.media` with metadata AND a
/// freshly-issued presigned URL (the bug this fixes: search results
/// previously returned media=None even for image memories).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live e2e: requires jobworkerp:9000 + minio:9200 + .env.image-test"]
async fn search_by_text_returns_media_payload_e2e() {
    if !load_env_or_skip() {
        return;
    }
    let url = start_memories_server().await;
    let mut media = MediaServiceClient::connect(url.clone())
        .await
        .expect("connect MediaService");
    let mut memory = MemoryServiceClient::connect(url.clone())
        .await
        .expect("connect MemoryService");
    let vector = MemoryVectorServiceClient::connect(url.clone())
        .await
        .expect("connect MemoryVectorService");

    let media_object_id = upload_tiny_png(&mut media).await;

    // Distinctive text body so SearchSemantic unambiguously retrieves
    // THIS memory; it also carries the image so media must be hydrated.
    let marker = format!("e2e-img-text-{}", chrono_now_millis());
    let content = format!("The {marker} archived diagram shows the cooling loop layout.");
    let user_id = 91_000_020;
    let memory_id = memory
        .create(MemoryData {
            user_id: Some(UserId { value: user_id }),
            content: content.clone(),
            content_type: ContentType::Text as i32,
            role: grpc_admin::protobuf::llm_memory::data::MessageRole::RoleUser as i32,
            media_object_id: Some(MediaObjectId {
                value: media_object_id.value,
            }),
            ..Default::default()
        })
        .await
        .expect("Create image+text memory")
        .into_inner()
        .id
        .expect("Create returns a memory id")
        .value;

    let media_payload = poll_until(Duration::from_secs(120), || {
        let mut v = vector.clone();
        let q = content.clone();
        async move {
            let stream = v
                .search_semantic(SemanticTextSearchRequest {
                    query_text: q,
                    options: None,
                })
                .await
                .ok()?
                .into_inner();
            find_hit_media(stream, memory_id).await
        }
    })
    .await;

    let payload = media_payload.expect(
        "image memory not retrievable via SearchSemantic within 120s \
         (or it returned media=None — the search enrich is broken)",
    );
    assert!(
        payload.metadata.is_some(),
        "search hit must carry media metadata"
    );
    assert_eq!(payload.metadata.as_ref().unwrap().media_type, "image/png");
    assert!(!payload.unresolved, "an uploaded (s3) image is resolvable");
    assert!(
        payload
            .presigned_url
            .as_deref()
            .map(|u| u.starts_with("http"))
            .unwrap_or(false),
        "search hit must carry a freshly-issued presigned URL, got {:?}",
        payload.presigned_url
    );
    eprintln!("search media enrich e2e OK: memory {memory_id} carries presigned media");
}

/// Thread-history path media enrich: a thread containing an image
/// memory, fetched via FindMemoriesByThreadId, must carry the hydrated
/// `Memory.media` with metadata + presigned URL (same bug class as the
/// search path; the importer/UI thread view depends on this).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live e2e: requires .env.image-test (minio:9200 for presign)"]
async fn find_memories_by_thread_id_returns_media_payload_e2e() {
    if !load_env_or_skip() {
        return;
    }
    let url = start_memories_server().await;
    let mut media = MediaServiceClient::connect(url.clone())
        .await
        .expect("connect MediaService");
    let mut threads = ThreadServiceClient::connect(url.clone())
        .await
        .expect("connect ThreadService");

    let media_object_id = upload_tiny_png(&mut media).await;

    let user_id = 91_000_021;
    let eid = format!("th-media-e2e-{}", chrono_now_millis());
    let channel = format!("import:e2e:th-media:{}", chrono_now_millis());
    let resp = threads
        .add_memories_batch(AddMemoriesBatchRequest {
            thread_target: Some(add_memories_batch_request::ThreadTarget::Upsert(
                ThreadUpsertByChannel {
                    thread_data: Some(ThreadData {
                        user_id: Some(UserId { value: user_id }),
                        channel: Some(channel.clone()),
                        description: Some("thread media enrich e2e".to_string()),
                        ..Default::default()
                    }),
                },
            )),
            memories: vec![BatchMemoryInput {
                memory: Some(MemoryData {
                    user_id: Some(UserId { value: user_id }),
                    content: String::new(),
                    content_type: ContentType::Image as i32,
                    role: grpc_admin::protobuf::llm_memory::data::MessageRole::RoleUser as i32,
                    external_id: Some(eid.clone()),
                    media_object_id: Some(MediaObjectId {
                        value: media_object_id.value,
                    }),
                    ..Default::default()
                }),
                parent_external_ids: vec![],
            }],
            upsert_by_external_id: true,
            thread_updated_at_override: 0,
            labels: vec![],
        })
        .await
        .expect("AddMemoriesBatch")
        .into_inner();
    let thread_id = resp.thread_id.expect("batch returns thread_id");
    let memory_id = resp.outcomes[0]
        .memory_id
        .expect("batch outcome has memory id")
        .value;

    // No embedding wait needed: FindMemoriesByThreadId is an RDB read;
    // media enrich is synchronous (metadata hydrate + presign).
    let mut stream = threads
        .find_memories_by_thread_id(FindMemoriesByThreadIdRequest {
            thread_id: Some(ThreadId {
                value: thread_id.value,
            }),
            limit: None,
            offset: None,
            roles: vec![],
            content_types: vec![],
        })
        .await
        .expect("FindMemoriesByThreadId")
        .into_inner();

    let mut found_media = None;
    while let Ok(Some(m)) = stream.message().await {
        if m.id.map(|i| i.value) == Some(memory_id) {
            found_media = m.media;
            break;
        }
    }
    let payload =
        found_media.expect("image memory in thread history must carry Memory.media (not None)");
    assert!(
        payload.metadata.is_some(),
        "thread history hit must carry media metadata"
    );
    assert!(!payload.unresolved, "an uploaded (s3) image is resolvable");
    assert!(
        payload
            .presigned_url
            .as_deref()
            .map(|u| u.starts_with("http"))
            .unwrap_or(false),
        "thread history hit must carry a presigned URL, got {:?}",
        payload.presigned_url
    );
    eprintln!(
        "thread media enrich e2e OK: memory {memory_id} in thread \
         {} carries presigned media",
        thread_id.value
    );
}
