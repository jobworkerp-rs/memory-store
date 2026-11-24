//! Applies to an RDB exactly what the v0.0.7 (commit 06ce5b4) server startup
//! does to it, then exits.
//!
//! Copied to `grpc-admin/examples/startup.rs` of a 06ce5b4 checkout and run
//! with `cargo run -p grpc-admin --example startup`. It is never compiled in
//! the current tree.
//!
//! At 06ce5b4 `front::server::create_server` calls
//! `RepositoryModule::new_by_env().await`, which opens the process-wide pool
//! and executes the embedded startup schema (`001_schema.sql` +
//! `003_reflection_schema.sql`, plus the WAL pragmas). `AppModule::new_by_env`
//! that follows only reads the RDB, so it is not needed here.
//!
//! Required environment:
//!   SQLITE_URL              e.g. `sqlite:///tmp/x/memory-kind.sqlite3?mode=rwc`
//!   SQLITE_MAX_CONNECTIONS  e.g. `1`
//! Must be unset or not `true`, so that no LanceDB store is opened:
//!   MEMORY_VECTOR_ENABLED, THREAD_VECTOR_ENABLED, REFLECTION_INTENT_VECTOR_ENABLED
//! Optional: SQLITE_DISABLE_WAL, RUST_LOG.
//!
//! `.env` is deliberately not loaded: the generator controls every variable.
//! A startup failure exits with status 1 through `StartupError::fatal`.

use infra::infra::module::RepositoryModule;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let repository_module = RepositoryModule::new_by_env().await;
    // Closing the pool checkpoints the WAL so the database file alone
    // carries the startup schema when the next tool opens it.
    repository_module.pool().close().await;
    println!("06ce5b4 startup schema applied");
}
