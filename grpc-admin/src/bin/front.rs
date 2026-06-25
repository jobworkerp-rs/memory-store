use anyhow::Result;
use dotenvy::dotenv;

// start front_server
#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // tracing init runs BEFORE the structured-error contract is active:
    // a failure here means no tracing layer exists, so we cannot emit a
    // `StartupError` event anyway. The `?` is intentional and out of
    // scope for the sidecar-failure-handling redesign.
    command_utils::util::tracing::init_from_env_and_filename("memories-front", "log").await?;

    // From this point onward, every startup failure MUST be surfaced
    // via `StartupError::fatal()`. Returning `Err` from `main` would
    // make the Rust runtime print a Debug-formatted line to stderr,
    // bypassing the tracing JSON layer that the parent process scans.
    // See `infra/src/infra/startup_error.rs` module docstring.
    if let Err(e) = grpc_admin::setup_and_start_front_server().await {
        infra::infra::startup_error::StartupError::fatal_anyhow("front", e);
    }
    command_utils::util::tracing::shutdown_tracer_provider();

    Ok(())
}
