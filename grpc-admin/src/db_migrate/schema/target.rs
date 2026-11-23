//! Where the migration target database URL comes from, and how schema
//! migrations connect to it.

use anyhow::{Context, Result, bail};
use infra_utils::infra::rdb::{Rdb, RdbPool};

/// The variable that names the migration target.
pub const DATABASE_URL_ENV: &str = "MEMORIES_DB_MIGRATE_DATABASE_URL";
/// The name used by releases whose migration engine was Atlas. Accepted for
/// one more release so that callers can switch names independently.
pub const LEGACY_DATABASE_URL_ENV: &str = "MEMORIES_ATLAS_DATABASE_URL";
/// The server's own connection variable, used when neither name is set.
pub const SERVER_DATABASE_URL_ENV: &str = "POSTGRES_URL";

/// The resolved migration target URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDatabaseUrl {
    pub url: String,
    /// The URL came only from [`LEGACY_DATABASE_URL_ENV`]; callers warn.
    pub used_legacy_name: bool,
}

/// Resolve the target URL from `lookup` (normally the process environment).
/// An empty value counts as unset. Errors never contain a URL, because URLs
/// carry credentials.
pub fn resolve_database_url(
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<ResolvedDatabaseUrl> {
    let read = |name: &str| lookup(name).filter(|value| !value.is_empty());
    match (read(DATABASE_URL_ENV), read(LEGACY_DATABASE_URL_ENV)) {
        (Some(current), Some(legacy)) if current != legacy => bail!(
            "{DATABASE_URL_ENV} and {LEGACY_DATABASE_URL_ENV} name different databases; set only {DATABASE_URL_ENV}"
        ),
        (Some(url), _) => Ok(ResolvedDatabaseUrl {
            url,
            used_legacy_name: false,
        }),
        (None, Some(url)) => Ok(ResolvedDatabaseUrl {
            url,
            used_legacy_name: true,
        }),
        (None, None) => match read(SERVER_DATABASE_URL_ENV) {
            Some(url) => Ok(ResolvedDatabaseUrl {
                url,
                used_legacy_name: false,
            }),
            None => bail!("{DATABASE_URL_ENV} or {SERVER_DATABASE_URL_ENV} is required"),
        },
    }
}

/// Whether a PostgreSQL URL selects its schema through `search_path`
/// (directly, or as SQLx's `options[search_path]`).
pub fn names_search_path(url: &str) -> bool {
    url::Url::parse(url).is_ok_and(|url| {
        url.query_pairs()
            .any(|(key, _)| key == "search_path" || key == "options[search_path]")
    })
}

/// The pool schema migrations run on, from a URL SQLx accepts. SQLite
/// connections disable foreign keys, as the previous engine applied every
/// migration that way.
pub async fn connect_for_migration(url: &str) -> Result<RdbPool> {
    #[cfg(not(feature = "postgres"))]
    {
        use std::str::FromStr;
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(url)
            .context("parsing the SQLite target URL")?
            .foreign_keys(false);
        sqlx::Pool::<Rdb>::connect_with(options)
            .await
            .context("connecting to migration target database")
    }
    #[cfg(feature = "postgres")]
    {
        sqlx::Pool::<Rdb>::connect(url)
            .await
            .context("connecting to migration target database")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn search_path_is_recognized_in_either_spelling() {
        assert!(names_search_path(
            "postgres://u:p@h/db?search_path=memories"
        ));
        assert!(names_search_path(
            "postgres://u:p@h/db?sslmode=disable&options%5Bsearch_path%5D=m"
        ));
        assert!(!names_search_path("postgres://u:p@h/db?sslmode=disable"));
        assert!(!names_search_path("not a url"));
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn sqlite_migration_connections_disable_foreign_keys() {
        let directory = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory.path().join("target.sqlite3").display()
        );
        let pool = connect_for_migration(&url).await.unwrap();
        let enabled: bool = sqlx::query_scalar("PRAGMA foreign_keys")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(!enabled);
        pool.close().await;
    }

    fn resolve(pairs: &[(&str, &str)]) -> Result<ResolvedDatabaseUrl> {
        let environment: HashMap<String, String> = pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        resolve_database_url(|name| environment.get(name).cloned())
    }

    fn resolved(url: &str, used_legacy_name: bool) -> ResolvedDatabaseUrl {
        ResolvedDatabaseUrl {
            url: url.to_string(),
            used_legacy_name,
        }
    }

    #[test]
    fn the_current_name_wins() {
        assert_eq!(
            resolve(&[(DATABASE_URL_ENV, "sqlite:///a.db")]).unwrap(),
            resolved("sqlite:///a.db", false)
        );
        assert_eq!(
            resolve(&[
                (DATABASE_URL_ENV, "postgres://h/a"),
                (SERVER_DATABASE_URL_ENV, "postgres://h/server"),
            ])
            .unwrap(),
            resolved("postgres://h/a", false)
        );
    }

    #[test]
    fn the_legacy_name_is_accepted_and_reported() {
        assert_eq!(
            resolve(&[(LEGACY_DATABASE_URL_ENV, "sqlite:///a.db")]).unwrap(),
            resolved("sqlite:///a.db", true)
        );
        assert_eq!(
            resolve(&[
                (LEGACY_DATABASE_URL_ENV, "postgres://h/a"),
                (SERVER_DATABASE_URL_ENV, "postgres://h/server"),
            ])
            .unwrap(),
            resolved("postgres://h/a", true)
        );
    }

    #[test]
    fn both_names_must_agree() {
        assert_eq!(
            resolve(&[
                (DATABASE_URL_ENV, "sqlite:///a.db"),
                (LEGACY_DATABASE_URL_ENV, "sqlite:///a.db"),
            ])
            .unwrap(),
            resolved("sqlite:///a.db", false)
        );
        let error = resolve(&[
            (DATABASE_URL_ENV, "postgres://user:secret@h/a"),
            (LEGACY_DATABASE_URL_ENV, "postgres://user:secret@h/b"),
        ])
        .unwrap_err()
        .to_string();
        assert!(error.contains(DATABASE_URL_ENV), "{error}");
        assert!(!error.contains("secret"), "{error}");
    }

    #[test]
    fn the_server_variable_is_the_last_resort() {
        assert_eq!(
            resolve(&[(SERVER_DATABASE_URL_ENV, "postgres://h/server")]).unwrap(),
            resolved("postgres://h/server", false)
        );
        let error = resolve(&[]).unwrap_err().to_string();
        assert!(error.contains(DATABASE_URL_ENV), "{error}");
    }

    #[test]
    fn empty_values_count_as_unset() {
        assert_eq!(
            resolve(&[
                (DATABASE_URL_ENV, ""),
                (LEGACY_DATABASE_URL_ENV, "sqlite:///a.db"),
            ])
            .unwrap(),
            resolved("sqlite:///a.db", true)
        );
        assert!(resolve(&[(DATABASE_URL_ENV, ""), (SERVER_DATABASE_URL_ENV, "")]).is_err());
    }
}
