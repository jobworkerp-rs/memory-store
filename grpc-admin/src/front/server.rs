use crate::protobuf::llm_memory::service::media_service_server::MediaServiceServer;
use crate::protobuf::llm_memory::service::memory_rating_service_server::MemoryRatingServiceServer;
use crate::protobuf::llm_memory::service::memory_service_server::MemoryServiceServer;
use crate::protobuf::llm_memory::service::memory_vector_service_server::MemoryVectorServiceServer;
use crate::protobuf::llm_memory::service::search_index_maintenance_service_server::SearchIndexMaintenanceServiceServer;
use crate::protobuf::llm_memory::service::thread_service_server::ThreadServiceServer;
use crate::protobuf::llm_memory::service::thread_vector_service_server::ThreadVectorServiceServer;
use crate::service::media::MediaGrpcImpl;
use crate::service::memory::MemoryGrpcImpl;
use crate::service::memory_rating::MemoryRatingGrpcImpl;
use crate::service::memory_vector::MemoryVectorGrpcImpl;
use crate::service::reflection::ReflectionGrpcImpl;
use crate::service::reflection_vector::ReflectionVectorGrpcImpl;
use crate::service::search_index_maintenance::SearchIndexMaintenanceGrpcImpl;
use crate::service::thread::ThreadGrpcImpl;
use crate::service::thread_vector::ThreadVectorGrpcImpl;
// Reflection service stubs live in the shared `protobuf` crate (see
// `grpc-admin/build.rs` — reflection protos are intentionally not
// regenerated here so the trait signatures on `ReflectionApp` line up
// with the handler types). The shared crate also owns the
// `FILE_DESCRIPTOR_SET` we hand to tonic's server-reflection helper,
// because the grpc-admin descriptor lacks the reflection RPCs and
// would silently hide them from `grpcurl list` / `describe`.
use anyhow::Result;
use app::module::AppModule;
use infra::infra::module::RepositoryModule;
use protobuf::FILE_DESCRIPTOR_SET;
use protobuf::llm_memory::service::reflection_service_server::ReflectionServiceServer;
use protobuf::llm_memory::service::reflection_vector_service_server::ReflectionVectorServiceServer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic_web::GrpcWebLayer;

/// Per-RPC encode/decode ceiling, matching the importer client and the
/// existing `MemoryStoreClient` plugin. Tonic's default of 4 MiB would
/// reject AddMemoriesBatch payloads (up to 12 MiB) at the transport
/// layer before they ever reach the handler — the
/// `IMPORT_BATCH_MAX_ITEMS` pre-flight and the byte-aware client-side
/// chunker both assume the server decodes the full message and
/// returns `ResourceExhausted` from the handler if it is too big.
const MAX_GRPC_MESSAGE_SIZE: usize = 16 * 1024 * 1024 - 1;

enum MaintenanceListener {
    Cohosted,
    Separate(TcpListener),
}

enum MaintenanceServerTopology {
    Cohosted,
    Separate {
        listener: TcpListener,
        routes: tonic::service::Routes,
    },
}

fn cohosts_maintenance_listener(addr: SocketAddr, maintenance_addr: SocketAddr) -> bool {
    addr == maintenance_addr
}

fn listener_completion_result(shutdown_requested: bool, listener: &str) -> Result<()> {
    if shutdown_requested {
        Ok(())
    } else {
        anyhow::bail!("{listener} gRPC listener stopped unexpectedly")
    }
}

fn shutdown_receivers(
    shutdown_tx: &tokio::sync::broadcast::Sender<()>,
) -> (
    tokio::sync::broadcast::Receiver<()>,
    tokio::sync::broadcast::Receiver<()>,
) {
    (shutdown_tx.subscribe(), shutdown_tx.subscribe())
}

/// Startup config-consistency check. The `inline` storage backend is
/// test-only and the embedding workflow cannot read a data: URI, so
/// "inline + any image
/// search mode" is a misconfiguration that would silently never embed
/// images. Returns the panic message when the pair is inconsistent so
/// `create_server` fails fast (env is immutable; fixing it is an env
/// edit, far safer than a hard-to-detect silent degradation). Pure
/// (string args only) so the rule is unit-tested without env / a server.
fn inline_image_mode_conflict(backend: &str, image_search_mode: &str) -> Option<String> {
    let mode = infra::infra::embedding_dispatch::ImageSearchMode::parse(image_search_mode);
    if backend.trim().eq_ignore_ascii_case("inline") && mode.is_image_enabled() {
        Some(format!(
            "MEDIA_STORAGE_BACKEND=inline is incompatible with \
             MEMORY_IMAGE_SEARCH_MODE={image_search_mode} (inline is \
             test-only and the embedding workflow cannot read a data: \
             URI). Set MEMORY_IMAGE_SEARCH_MODE=none, or use the s3 / \
             file backend for image search."
        ))
    } else {
        None
    }
}

pub async fn create_server(
    addr: SocketAddr,
    maintenance_addr: SocketAddr,
    use_web: bool,
    max_frame_size: Option<u32>,
) -> Result<()> {
    // Startup fail-fast (a): config consistency.
    // env-only, so it runs before anything else — a misconfigured
    // inline+image-mode deployment should never bind a port. Surfaces
    // as a structured `MediaConfigConflict` so agent-app can name the
    // two env vars to fix instead of generic "startup failed". The safe
    // defaults (file backend / mode=none) never trip this.
    {
        let backend = std::env::var("MEDIA_STORAGE_BACKEND").unwrap_or_default();
        let mode = std::env::var("MEMORY_IMAGE_SEARCH_MODE").unwrap_or_default();
        if inline_image_mode_conflict(&backend, &mode).is_some() {
            infra::infra::startup_error::StartupError::MediaConfigConflict {
                backend,
                image_search_mode: mode,
            }
            .fatal();
        }
    }

    // When the addresses differ, bind both listeners before constructing or
    // exposing either service. A maintenance-only failure must never leave a
    // normal listener running without its paired control-plane endpoint.
    // Matching addresses deliberately cohost the maintenance service on the
    // normal listener for single-user/local deployments.
    let normal_listener = TcpListener::bind(addr)
        .await
        .map_err(|error| anyhow::anyhow!("failed to bind normal gRPC listener {addr}: {error}"))?;
    let maintenance_listener = if cohosts_maintenance_listener(addr, maintenance_addr) {
        MaintenanceListener::Cohosted
    } else {
        MaintenanceListener::Separate(TcpListener::bind(maintenance_addr).await.map_err(
            |error| {
                anyhow::anyhow!(
                    "failed to bind maintenance gRPC listener {maintenance_addr}: {error}"
                )
            },
        )?)
    };

    // reflection
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(2);
    // Subscribe before starting the signal task. A broadcast send with no
    // receivers is intentionally discarded, which would otherwise lose a
    // Ctrl-C delivered while the application modules are initializing.
    let (mut normal_shutdown, mut maintenance_shutdown) = shutdown_receivers(&shutdown_tx);
    let shutdown_signal = shutdown_tx.clone();
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let signal_shutdown_requested = Arc::clone(&shutdown_requested);
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                tracing::info!("received ctrl_c");
                signal_shutdown_requested.store(true, Ordering::Release);
                let _ = shutdown_signal.send(());
            }
            Err(e) => tracing::error!("failed to listen for ctrl_c: {:?}", e),
        }
    });

    let maintenance_config =
        infra::infra::search_index_maintenance::SearchIndexMaintenanceConfig::from_env()
            .expect("maintenance configuration was validated before server construction");
    let repository_module = RepositoryModule::new_by_env().await;
    let maintenance_executor = repository_module.search_index_maintenance_executor.clone();
    let mut app_module = AppModule::new_by_env(repository_module).await;
    let memory_rating = MemoryRatingGrpcImpl::new(app_module.memory_rating_app);
    // Share the one MediaApp Arc between MediaService and MemoryService
    // (the latter issues presigned URLs during Find enrich). No second
    // storage backend instance.
    let media_app_arc = app_module.media_app.clone();
    let media = MediaGrpcImpl::new(media_app_arc.clone());

    // Wrap vector_app in Arc for sharing between Memory/Thread/Vector gRPC services
    let vector_app_arc: Option<std::sync::Arc<app::app::memory_vector::MemoryVectorAppImpl>> =
        app_module.memory_vector_app.map(std::sync::Arc::new);
    let thread_vector_app_arc: Option<
        std::sync::Arc<app::app::thread_vector::ThreadVectorAppImpl>,
    > = app_module.thread_vector_app.take();
    let memory = MemoryGrpcImpl::new(
        app_module.memory_app,
        vector_app_arc.clone(),
        Some(media_app_arc.clone()),
    );
    let thread = ThreadGrpcImpl::new(
        app_module.thread_app,
        vector_app_arc.clone(),
        thread_vector_app_arc.clone(),
    )
    .with_media_app(media_app_arc.clone());

    // Reflection app is shared between `ReflectionService` (RDB) and
    // `ReflectionVectorService` (the latter uses it for
    // `RedispatchReflectionEmbeddings` / `UpsertIntentEmbedding`).
    let reflection_app_arc = std::sync::Arc::new(app_module.reflection_app);
    let reflection_service =
        ReflectionGrpcImpl::new(reflection_app_arc.clone(), vector_app_arc.clone());

    let mut routes = tonic::service::Routes::new(reflection)
        .add_service(
            MemoryServiceServer::new(memory)
                .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE),
        )
        .add_service(
            MemoryRatingServiceServer::new(memory_rating)
                .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE),
        )
        .add_service(
            MediaServiceServer::new(media)
                .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE),
        )
        .add_service(
            ThreadServiceServer::new(thread)
                .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE),
        )
        .add_service(
            ReflectionServiceServer::new(reflection_service)
                .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE),
        );
    tracing::info!("ReflectionService registered");

    if let Some(va) = vector_app_arc {
        // Startup fail-fast (b): embedding dimension probe. Runs in all
        // modes — even `none`, the text path uses the same mm-embedding
        // worker. A genuine MEMORY_VECTOR_SIZE vs runner-dimension drift
        // exits via the structured `EmbeddingDimensionMismatch` so
        // agent-app can offer the same recovery flow as a LanceDB dim
        // mismatch; an unreachable jobworkerp at startup does NOT block
        // (the probe self-skips with a warn).
        if let Err(e) = va.verify_embedding_dimension().await {
            infra::infra::startup_error::StartupError::fatal_anyhow(
                "verify_embedding_dimension",
                e,
            );
        }
        // Share the one MediaApp Arc so SearchByMedia can Resolve the
        // query media (same instance as MediaService / MemoryService —
        // no second storage backend).
        let memory_vector = MemoryVectorGrpcImpl::new(va).with_media_app(media_app_arc.clone());
        routes = routes.add_service(
            MemoryVectorServiceServer::new(memory_vector)
                .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE),
        );
        tracing::info!("MemoryVectorService registered");
    }

    if let Some(tva) = thread_vector_app_arc {
        let thread_vector = ThreadVectorGrpcImpl::new(tva);
        routes = routes.add_service(
            ThreadVectorServiceServer::new(thread_vector)
                .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE),
        );
        tracing::info!("ThreadVectorService registered");
    }

    {
        let reflection_vector = ReflectionVectorGrpcImpl::new(reflection_app_arc.clone());
        routes = routes.add_service(
            ReflectionVectorServiceServer::new(reflection_vector)
                .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE),
        );
        tracing::info!("ReflectionVectorService registered");
    }

    let maintenance =
        SearchIndexMaintenanceGrpcImpl::new(maintenance_executor, &maintenance_config);
    let maintenance_service = SearchIndexMaintenanceServiceServer::new(maintenance)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);
    let maintenance_server = match maintenance_listener {
        MaintenanceListener::Cohosted => {
            routes = routes.add_service(maintenance_service);
            MaintenanceServerTopology::Cohosted
        }
        MaintenanceListener::Separate(listener) => {
            let maintenance_reflection = tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
                .build_v1()
                .unwrap();
            MaintenanceServerTopology::Separate {
                listener,
                routes: tonic::service::Routes::new(maintenance_reflection)
                    .add_service(maintenance_service),
            }
        }
    };
    let normal_server = async move {
        if use_web {
            Server::builder()
                .accept_http1(true)
                .max_frame_size(max_frame_size)
                .layer(GrpcWebLayer::new())
                .add_routes(routes)
                .serve_with_incoming_shutdown(TcpListenerStream::new(normal_listener), async move {
                    let _ = normal_shutdown.recv().await;
                })
                .await?;
        } else {
            Server::builder()
                .max_frame_size(max_frame_size)
                .add_routes(routes)
                .serve_with_incoming_shutdown(TcpListenerStream::new(normal_listener), async move {
                    let _ = normal_shutdown.recv().await;
                })
                .await?;
        }
        Ok::<(), anyhow::Error>(())
    };
    match maintenance_server {
        MaintenanceServerTopology::Separate {
            listener: maintenance_listener,
            routes: maintenance_routes,
        } => {
            let maintenance_server = async move {
                Server::builder()
                    .add_routes(maintenance_routes)
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(maintenance_listener),
                        async move {
                            let _ = maintenance_shutdown.recv().await;
                        },
                    )
                    .await?;
                Ok::<(), anyhow::Error>(())
            };
            tokio::select! {
                result = normal_server => {
                    let _ = shutdown_tx.send(());
                    result?;
                    listener_completion_result(shutdown_requested.load(Ordering::Acquire), "normal")
                }
                result = maintenance_server => {
                    let _ = shutdown_tx.send(());
                    result?;
                    listener_completion_result(shutdown_requested.load(Ordering::Acquire), "maintenance")
                }
            }
        }
        MaintenanceServerTopology::Cohosted => {
            normal_server.await?;
            listener_completion_result(shutdown_requested.load(Ordering::Acquire), "normal")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        cohosts_maintenance_listener, inline_image_mode_conflict, listener_completion_result,
        shutdown_receivers,
    };
    use std::net::SocketAddr;

    #[test]
    fn cohosts_maintenance_only_for_the_same_listener_address() {
        let normal: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        assert!(cohosts_maintenance_listener(normal, normal));
        assert!(!cohosts_maintenance_listener(
            normal,
            "127.0.0.1:9001".parse().unwrap()
        ));
    }

    #[test]
    fn inline_with_image_mode_is_a_conflict() {
        // inline + any image mode → must report a conflict (fail-fast).
        for mode in ["multimodal", "vlm_caption", "both"] {
            assert!(
                inline_image_mode_conflict("inline", mode).is_some(),
                "inline + {mode} must conflict"
            );
        }
        // Case-insensitive backend match.
        assert!(inline_image_mode_conflict("INLINE", "both").is_some());
    }

    #[test]
    fn inline_with_none_or_other_backends_is_ok() {
        // inline + none is allowed (store/fetch only, no image search).
        assert!(inline_image_mode_conflict("inline", "none").is_none());
        // Unknown mode parses to None (text-only) → no conflict.
        assert!(inline_image_mode_conflict("inline", "").is_none());
        assert!(inline_image_mode_conflict("inline", "typo").is_none());
        // Non-inline backends never conflict, even with image modes.
        for backend in ["s3", "file", "url", ""] {
            assert!(
                inline_image_mode_conflict(backend, "both").is_none(),
                "backend={backend} must not conflict"
            );
        }
    }

    #[test]
    fn requested_shutdown_is_not_a_listener_failure() {
        assert!(listener_completion_result(true, "normal").is_ok());
        assert!(listener_completion_result(false, "maintenance").is_err());
    }

    #[tokio::test]
    async fn pre_registered_shutdown_receivers_observe_early_signal() {
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(2);
        let (mut normal, mut maintenance) = shutdown_receivers(&shutdown_tx);

        shutdown_tx.send(()).unwrap();

        assert!(normal.recv().await.is_ok());
        assert!(maintenance.recv().await.is_ok());
    }
}
