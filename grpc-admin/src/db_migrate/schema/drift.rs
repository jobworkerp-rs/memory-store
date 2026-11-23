//! Drift check: the target schema against the compiled catalog replayed
//! into a scratch database.
//!
//! Both schemas are flattened into `object -> normalized definition` maps,
//! so a difference names the object that differs. Identifier quoting, case,
//! whitespace, and schema qualification do not count as differences; the
//! migration history tables and, on PostgreSQL, relations that belong to an
//! extension are not compared.

use super::migrations;
use anyhow::{Context, Result, bail};
use infra_utils::infra::rdb::RdbPool;
use sqlparser::dialect::Dialect;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::collections::BTreeMap;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(not(feature = "postgres"))]
mod sqlite;

/// A schema flattened into comparable entries.
pub type Snapshot = BTreeMap<String, String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Difference {
    pub object: String,
    /// Definition in the replayed catalog; `None` when the target has an
    /// object the catalog does not create.
    pub expected: Option<String>,
    /// Definition in the target; `None` when it is missing.
    pub actual: Option<String>,
}

impl std::fmt::Display for Difference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.expected, &self.actual) {
            (Some(_), None) => write!(f, "missing {}", self.object),
            (None, Some(_)) => write!(f, "unexpected {}", self.object),
            (Some(expected), Some(actual)) => write!(
                f,
                "changed {}: expected `{expected}`, found `{actual}`",
                self.object
            ),
            (None, None) => write!(f, "{}", self.object),
        }
    }
}

/// Marks a failure that leaves the target unverified: the verification
/// database could not be built, or a definition could not be interpreted.
/// Such a failure counts as drift, not as an operational error.
#[derive(Debug)]
struct Unverifiable;

impl std::fmt::Display for Unverifiable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the schema could not be verified")
    }
}

fn unverifiable(error: anyhow::Error) -> anyhow::Error {
    error.context(Unverifiable)
}

/// Fail with `drift_detected` unless the target matches the catalog
/// replayed up to `version`.
pub async fn verify(pool: &RdbPool, version: &str) -> Result<()> {
    verdict(check(pool, version).await)
}

fn verdict(checked: Result<Vec<Difference>>) -> Result<()> {
    match checked {
        Ok(differences) if differences.is_empty() => Ok(()),
        Ok(differences) => {
            let listed = differences
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ");
            bail!(
                "drift_detected: {} difference(s): {listed}",
                differences.len()
            )
        }
        Err(error) if error.is::<Unverifiable>() => bail!("drift_detected: {error:#}"),
        Err(error) => Err(error),
    }
}

/// Compare the target with the catalog replayed up to `version`.
async fn check(pool: &RdbPool, version: &str) -> Result<Vec<Difference>> {
    #[cfg(feature = "postgres")]
    let (expected, actual) = postgres::snapshots(pool, version).await?;
    #[cfg(not(feature = "postgres"))]
    let (expected, actual) = sqlite::snapshots(pool, version).await?;
    Ok(diff(&expected, &actual))
}

pub fn diff(expected: &Snapshot, actual: &Snapshot) -> Vec<Difference> {
    let mut objects = expected.keys().chain(actual.keys()).collect::<Vec<_>>();
    objects.sort();
    objects.dedup();
    objects
        .into_iter()
        .filter_map(|object| {
            let (expected, actual) = (expected.get(object), actual.get(object));
            (expected != actual).then(|| Difference {
                object: object.clone(),
                expected: expected.cloned(),
                actual: actual.cloned(),
            })
        })
        .collect()
}

/// Replay the compiled catalog into the scratch database `scratch`, up to
/// and including `version`.
async fn replay(scratch: &RdbPool, version: &str) -> Result<()> {
    let version = migrations::parse_version(version)
        .context("schema version must be a 14-digit ASCII timestamp")?;
    migrations::migrator()
        .run_to(version, scratch)
        .await
        .context("replaying the migration catalog into the verification database")
}

/// The migration history tables, which drift checks leave out.
fn is_history_table(table: &str) -> bool {
    table == migrations::HISTORY_TABLE || table == migrations::LEGACY_HISTORY_TABLE
}

/// SQL text with identifier quoting, case, and whitespace normalized away.
/// String literals keep their case.
pub(crate) fn normalize_sql(dialect: &dyn Dialect, sql: &str) -> String {
    match Tokenizer::new(dialect, sql).tokenize() {
        Ok(tokens) => join_tokens(tokens.iter()),
        // Unparseable text still compares, just without normalization.
        Err(_) => sql.split_whitespace().collect::<Vec<_>>().join(" "),
    }
}

fn normalized_token(token: &Token) -> Option<String> {
    match token {
        Token::Whitespace(_) => None,
        Token::Word(word) => Some(word.value.to_lowercase()),
        other => Some(other.to_string()),
    }
}

pub(super) fn join_tokens<'a>(tokens: impl Iterator<Item = &'a Token>) -> String {
    tokens
        .filter_map(normalized_token)
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests;
