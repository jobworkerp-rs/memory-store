use crate::error::LlmMemoryError;
use anyhow::Result;
use infra_utils::infra::rdb::Rdb;
use infra_utils::infra::rdb::RdbConfig;
use infra_utils::infra::rdb::RdbConfigImpl;
use infra_utils::infra::rdb::RdbUrlConfigImpl;
use infra_utils::infra::redis::{RedisConfig, RedisPool};
use sqlx::Pool;

#[cfg(not(feature = "postgres"))]
const SQLITE_SCHEMA_001: &str = include_str!("../../sql/sqlite/001_schema.sql");
// 003 covers Thread Reflection. Concatenated at boot so new sqlite
// deployments get the reflection tables / dictionary out of the box,
// matching the existing convention (the 002 external_id index is
// already present in 001 for new deployments).
#[cfg(not(feature = "postgres"))]
const SQLITE_SCHEMA_003_REFLECTION: &str =
    include_str!("../../sql/sqlite/003_reflection_schema.sql");
static RDB_POOL: tokio::sync::OnceCell<Pool<Rdb>> = tokio::sync::OnceCell::const_new();

pub async fn setup_rdb_by_env() -> &'static Pool<Rdb> {
    let conf = load_db_config_from_env().unwrap_or(
        load_db_url_config_from_env().unwrap_or(RdbConfig::Separate(RdbConfigImpl::default())),
    );
    setup_rdb(&conf).await
}

// new rdb pool and store as static
// (if failed initializing, panic!)
// (if need multiple database, add RDB_POOL and setup multiple)
pub async fn setup_rdb(db_config: &RdbConfig) -> &'static Pool<Rdb> {
    sqlx::any::install_default_drivers();
    #[cfg(not(feature = "postgres"))]
    let combined_schema = format!("{SQLITE_SCHEMA_001}\n{SQLITE_SCHEMA_003_REFLECTION}");
    #[cfg(not(feature = "postgres"))]
    let schema: Option<&String> = Some(&combined_schema);
    #[cfg(feature = "postgres")]
    let schema: Option<&String> = None;
    RDB_POOL
        .get_or_init(|| async {
            infra_utils::infra::rdb::new_rdb_pool(db_config, schema)
                .await
                .unwrap_or_else(|e| {
                    crate::infra::startup_error::StartupError::RdbPoolInitFailed {
                        url_sanitized: rdb_config_url_for_log(db_config),
                        message: format!("{e:#}"),
                    }
                    .fatal()
                })
        })
        .await
}

/// Best-effort one-liner describing the configured RDB endpoint with
/// credentials stripped. Used in `RdbPoolInitFailed.url_sanitized` so
/// the agent-app surfaces *which* DB failed without exposing secrets.
///
/// Both branches collapse credentials to a literal `***`: the URL form
/// hides `user:password`, and the Separate form hides the user name
/// (the password is never on the struct path that lands here, but the
/// user name alone is still PII/secret-adjacent — `RdbConfigImpl`'s own
/// `DebugStub` redacts it as `[USER]`, so we mirror that here).
fn rdb_config_url_for_log(cfg: &RdbConfig) -> String {
    match cfg {
        RdbConfig::Url(url_cfg) => crate::infra::startup_error::sanitize_url(&url_cfg.url),
        RdbConfig::Separate(sep) => format!("***@{}:{}/{}", sep.host, sep.port, sep.dbname),
    }
}

pub fn load_db_url_config_from_env() -> Result<RdbConfig> {
    // sqlite first
    envy::prefixed("SQLITE_")
        .from_env::<RdbUrlConfigImpl>()
        .map(RdbConfig::Url)
        .or_else(|_| {
            envy::prefixed("POSTGRES_")
                .from_env::<RdbUrlConfigImpl>()
                .map(RdbConfig::Url)
        })
        .map_err(|e| {
            LlmMemoryError::RuntimeError(format!("cannot read db config from env: {:?}", e)).into()
        })
}
pub fn load_db_config_from_env() -> Option<RdbConfig> {
    // sqlite config takes priority
    envy::prefixed("SQLITE_")
        .from_env::<RdbConfig>()
        .or_else(|_| envy::prefixed("POSTGRES_").from_env::<RdbConfig>())
        .ok()
}

// TODO
static _REDIS: tokio::sync::OnceCell<RedisPool> = tokio::sync::OnceCell::const_new();

pub async fn _setup_redis_pool(config: RedisConfig) -> &'static RedisPool {
    _REDIS
        .get_or_init(|| async {
            infra_utils::infra::redis::new_redis_pool(config)
                .await
                .expect("msg")
        })
        .await
}
pub fn _load_redis_config_from_env() -> Result<RedisConfig> {
    envy::prefixed("REDIS_")
        .from_env::<RedisConfig>()
        .map_err(|e| {
            LlmMemoryError::RuntimeError(format!("cannot read redis config from env: {:?}", e))
                .into()
        })
}

#[cfg(test)]
mod rdb_config_url_for_log_tests {
    use super::*;
    use infra_utils::infra::rdb::RdbConfigImpl;

    #[test]
    fn separate_branch_redacts_user_name() {
        let cfg = RdbConfig::Separate(RdbConfigImpl {
            host: "db.internal".into(),
            port: "5432".into(),
            user: "production_admin".into(),
            password: "hunter2".into(),
            dbname: "memories".into(),
            max_connections: 8,
        });
        let out = rdb_config_url_for_log(&cfg);
        // Host / port / dbname still in plain to attribute *which* DB
        // failed; user / password must NOT appear.
        assert!(out.contains("db.internal"), "host must surface: {out}");
        assert!(out.contains("5432"), "port must surface: {out}");
        assert!(out.contains("memories"), "dbname must surface: {out}");
        assert!(
            !out.contains("production_admin"),
            "user name must be redacted: {out}"
        );
        assert!(
            !out.contains("hunter2"),
            "password must never appear: {out}"
        );
        assert!(out.starts_with("***@"), "must use `***@` prefix: {out}");
    }
}
