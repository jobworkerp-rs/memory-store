#[macro_use]
extern crate debug_stub_derive;

pub mod front;
pub mod protobuf;
pub mod service;

mod rag_tools;

use anyhow::Result;
use std::env;
use std::net::SocketAddr;

pub async fn setup_and_start_front_server() -> Result<()> {
    use infra::infra::startup_error::StartupError;

    let grpc_addr_raw = env::var("GRPC_ADDR").unwrap_or_else(|_| {
        StartupError::EnvVarInvalid {
            name: "GRPC_ADDR".into(),
            message: "must be specified".into(),
        }
        .fatal()
    });
    let grpc_addr: SocketAddr = grpc_addr_raw.parse().unwrap_or_else(|e| {
        StartupError::EnvVarInvalid {
            name: "GRPC_ADDR".into(),
            message: format!("not a valid socket address ({grpc_addr_raw:?}): {e}"),
        }
        .fatal()
    });

    // The server handles user-driven memory writes that auto-dispatch
    // embedding jobs, so the callback env *must* be valid before the
    // first request lands. Importer / dry-run binaries that don't
    // dispatch embeddings have no need for this, so the check lives
    // here rather than inside `AppModule::new_by_env`.
    let auto_embedding_enabled = env::var("MEMORY_AUTO_EMBEDDING_ENABLED")
        .unwrap_or_default()
        .eq_ignore_ascii_case("true");
    if auto_embedding_enabled && let Err(e) = infra::infra::require_grpc_callback_env() {
        StartupError::ConfigLoadFailed {
            component: "MEMORY_GRPC_HOST / MEMORY_GRPC_PORT (auto-embedding callback)".into(),
            message: format!("{e:#}"),
        }
        .fatal();
    }

    let rag_tools_enabled = env::var("MEMORY_RAG_TOOLS_ENABLED")
        .unwrap_or_default()
        .eq_ignore_ascii_case("true");

    // The RAG hybrid-search workflows reference `memories-mm-embedding`
    // by worker name. That worker is registered only when auto-embedding
    // is enabled (see `app/src/module.rs` — the embedding dispatcher
    // upserts it during `EmbeddingJobDispatcher::ensure_initialized`).
    // Without it the manifest still upserts cleanly, but every search
    // call would fail at workflow execution time. Reject the misconfig
    // at startup so the operator notices instead of debugging silent
    // recall failures later.
    if let Err(e) = validate_rag_prerequisites(rag_tools_enabled, auto_embedding_enabled) {
        StartupError::ConfigLoadFailed {
            component: "MEMORY_RAG_TOOLS_ENABLED".into(),
            message: format!("{e:#}"),
        }
        .fatal();
    }

    if rag_tools_enabled {
        // RAG workflows call back via MEMORY_GRPC_HOST/PORT, same contract
        // as auto-embedding. Skip the second validation when auto-embedding
        // already passed it.
        if !auto_embedding_enabled && let Err(e) = infra::infra::require_grpc_callback_env() {
            StartupError::ConfigLoadFailed {
                component: "MEMORY_GRPC_HOST / MEMORY_GRPC_PORT (RAG callback)".into(),
                message: format!("{e:#}"),
            }
            .fatal();
        }
        // Spawned so a slow / unreachable jobworkerp doesn't stall the
        // memories gRPC server. Failure is logged at WARN, never returned.
        tokio::spawn(rag_tools::register_on_startup());
    }

    let use_web: bool = env::var("USE_GRPC_WEB")
        .ok()
        .as_deref()
        .unwrap_or("false")
        .parse()
        .unwrap_or_else(|e| {
            StartupError::EnvVarInvalid {
                name: "USE_GRPC_WEB".into(),
                message: format!("must be 'true' or 'false': {e}"),
            }
            .fatal()
        });
    let max_frame_size: Option<u32> = env::var("MAX_FRAME_SIZE").ok().map(|raw| {
        raw.parse().unwrap_or_else(|e| {
            StartupError::EnvVarInvalid {
                name: "MAX_FRAME_SIZE".into(),
                message: format!("not a valid u32 ({raw:?}): {e}"),
            }
            .fatal()
        })
    });
    front::server::create_server(grpc_addr, use_web, max_frame_size)
        .await
        .map_err(|err| {
            tracing::error!("failed to create server: {:?}", err);
            err
        })
}

/// Reject MEMORY_RAG_TOOLS_ENABLED=true without MEMORY_AUTO_EMBEDDING_ENABLED.
///
/// Pure function (no env access) so it stays unit-testable in isolation —
/// the env-reading caller in `setup_and_start_front_server` simply forwards
/// the parsed booleans. Same style as `infra::validate_callback_host`.
fn validate_rag_prerequisites(rag_tools_enabled: bool, auto_embedding_enabled: bool) -> Result<()> {
    if rag_tools_enabled && !auto_embedding_enabled {
        anyhow::bail!(
            "MEMORY_RAG_TOOLS_ENABLED=true requires MEMORY_AUTO_EMBEDDING_ENABLED=true. \
             The RAG hybrid-search workflows (recall_memories, find_conversations) reference \
             the `memories-mm-embedding` worker, which is only registered when the \
             auto-embedding pipeline is enabled. Without it, manifest registration succeeds \
             but every search call fails at workflow-execution time."
        );
    }
    Ok(())
}

#[cfg(test)]
mod validate_rag_prerequisites_tests {
    use super::*;

    #[test]
    fn ok_when_both_disabled() {
        validate_rag_prerequisites(false, false).expect("both disabled is a valid configuration");
    }

    #[test]
    fn ok_when_only_auto_embedding_enabled() {
        validate_rag_prerequisites(false, true)
            .expect("auto-embedding without RAG tools is a valid configuration");
    }

    #[test]
    fn ok_when_both_enabled() {
        validate_rag_prerequisites(true, true).expect("the documented happy path must succeed");
    }

    #[test]
    fn err_when_rag_enabled_without_auto_embedding() {
        let err = validate_rag_prerequisites(true, false)
            .expect_err("RAG without auto-embedding must be rejected at startup")
            .to_string();
        // Operator-facing error: must name both env vars so the fix is
        // unambiguous from the log line alone.
        assert!(
            err.contains("MEMORY_RAG_TOOLS_ENABLED"),
            "error must name the offending env var: {err}"
        );
        assert!(
            err.contains("MEMORY_AUTO_EMBEDDING_ENABLED"),
            "error must name the missing prerequisite: {err}"
        );
        // Surface the runtime symptom so the operator understands why
        // this is a startup error rather than a runtime warning.
        assert!(
            err.contains("memories-mm-embedding"),
            "error must explain the underlying worker dependency: {err}"
        );
    }
}
