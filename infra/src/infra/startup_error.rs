//! Structured startup error vocabulary shared with the parent process
//! (agent-app sidecar host).
//!
//! Contract with the parent process:
//!
//! - `STARTUP_ERROR_TARGET = "app::startup_error"` is FROZEN. Renaming
//!   requires a coordinated change in
//!   `agent-app/src-tauri/src/sidecar/startup_error.rs`.
//! - Each variant's `code` snake_case string is FROZEN. Rename / removal
//!   is a BREAKING CHANGE. Adding a new variant is non-breaking — the
//!   parent has an `unknown` fallback that surfaces the raw message.
//! - Existing field names (e.g. `expected_dim`) are FROZEN.
//! - The parent matches on `target` + `level=ERROR` only. Other log
//!   levels are never consumed by the structured scanner.
//! - `panic!` / `std::process::abort` are FORBIDDEN on the startup path:
//!   the Rust panic handler bypasses the tracing JSON layer the parent
//!   scans, leaving the parent stuck on its 30s TCP timeout with no
//!   structured signal. Always use [`StartupError::fatal`].
//! - `main` MUST NOT return `Err`: the Rust runtime writes a
//!   Debug-formatted line to stderr for a returned `Err`, which also
//!   bypasses the tracing JSON layer. Catch in `main` and route to
//!   `fatal()`.
//!
//! ### Per-variant locator field
//!
//! The "which thing failed" key differs by variant — the parent cannot
//! assume a single uniform field name across the whole enum. Use the
//! pair below when displaying or routing in the UI:
//!
//! | variant                         | locator field(s)                |
//! |---------------------------------|---------------------------------|
//! | `LancedbSchemaMismatch`         | `table` + `uri`                 |
//! | `LancedbInitFailed`             | `uri`                           |
//! | `EmbeddingDimensionMismatch`    | `runner_name`                   |
//! | `MediaConfigConflict`           | `backend` + `image_search_mode` |
//! | `RdbPoolInitFailed`             | `url_sanitized`                 |
//! | `EnvVarInvalid`                 | `name`                          |
//! | `ConfigLoadFailed`              | `component`                     |
//! | `Other`                         | `component`                     |
//!
//! See `agent-app/ai-docs/sidecar-startup-failure-handling.md` for the
//! agent-app side of this contract.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// `target` field value attached to every structured startup-error
/// tracing event. The parent process matches on this verbatim.
pub const STARTUP_ERROR_TARGET: &str = "app::startup_error";

#[derive(Debug, Clone, Serialize, Deserialize, Error, PartialEq, Eq)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum StartupError {
    /// Existing LanceDB table's Arrow schema fingerprint differs from
    /// the expected fingerprint built from current config. Most common
    /// cause: the embedding model dimension changed (preset swap)
    /// without the old LanceDB being evacuated.
    #[error("LanceDB schema fingerprint mismatch on table='{table}' uri='{uri}'")]
    LancedbSchemaMismatch {
        table: String,
        uri: String,
        expected_dim: u32,
        actual_dim: u32,
        /// Raw fingerprint strings for diagnostic logs. Not consumed by
        /// the parent — recovery is decided from the dim pair alone.
        expected_fingerprint: String,
        actual_fingerprint: String,
    },
    /// LanceDB open / connect failed for non-schema reasons (corrupt
    /// files, permission denied, disk full, …). Parent can't auto-
    /// recover; surfaces the message verbatim.
    #[error("LanceDB initialization failed for uri='{uri}': {message}")]
    LancedbInitFailed { uri: String, message: String },
    /// The embedding runner reports a `dimension` that does not match
    /// `MEMORY_VECTOR_SIZE`. Same root cause as `LancedbSchemaMismatch`
    /// (model swap), but caught at the startup dimension probe rather
    /// than the LanceDB open. UI recovery actions are identical
    /// (evacuate / reset).
    #[error(
        "embedding dimension mismatch (runner={runner_name}): expected={expected_dim}, actual={actual_dim}"
    )]
    EmbeddingDimensionMismatch {
        expected_dim: u32,
        actual_dim: u32,
        runner_name: String,
    },
    /// `MEDIA_STORAGE_BACKEND=inline` combined with an image search
    /// mode — inline is test-only and the embedding workflow cannot
    /// read a data: URI. Parent surfaces the two env var names so the
    /// user can correct the configuration.
    #[error("media config conflict: backend={backend}, image_search_mode={image_search_mode}")]
    MediaConfigConflict {
        backend: String,
        image_search_mode: String,
    },
    /// RDB pool initialization failed (sqlx). Recovery is manual
    /// (permissions, disk full, corrupt DB file). `url_sanitized`
    /// removes credentials before logging — never feed a raw URL.
    #[error("RDB pool init failed for {url_sanitized}: {message}")]
    RdbPoolInitFailed {
        url_sanitized: String,
        message: String,
    },
    /// A single env var was missing or failed to parse (e.g.
    /// `GRPC_ADDR`, `USE_GRPC_WEB`). Distinct from `ConfigLoadFailed`
    /// because the failing unit is one env var with a known name, not
    /// a serde/envy schema.
    #[error("invalid env var {name}: {message}")]
    EnvVarInvalid { name: String, message: String },
    /// An `envy`-loaded config struct (e.g. `MemoryCacheConfig`,
    /// `VectorDBConfig`) failed to deserialize. Parent shows the
    /// component name so the operator can locate which env-var group
    /// to inspect.
    #[error("config load failed for {component}: {message}")]
    ConfigLoadFailed { component: String, message: String },
    /// Catch-all for non-classified startup failures. Parent shows the
    /// message; recovery is manual.
    #[error("startup failed at {component}: {message}")]
    Other { component: String, message: String },
}

impl StartupError {
    /// Emit this error as a single `tracing::error!` event on
    /// `STARTUP_ERROR_TARGET`. Each variant's fields are flattened to
    /// top-level keys in the tracing JSON row's `fields` block so the
    /// parent can deserialize the block directly via
    /// `#[serde(tag = "code")]`.
    pub fn emit_via_tracing(&self) {
        match self {
            Self::LancedbSchemaMismatch {
                table,
                uri,
                expected_dim,
                actual_dim,
                expected_fingerprint,
                actual_fingerprint,
            } => tracing::error!(
                target: STARTUP_ERROR_TARGET,
                code = "lancedb_schema_mismatch",
                table = %table,
                uri = %uri,
                expected_dim = expected_dim,
                actual_dim = actual_dim,
                expected_fingerprint = %expected_fingerprint,
                actual_fingerprint = %actual_fingerprint,
                "LanceDB schema fingerprint mismatch",
            ),
            // The format string is intentionally omitted here (and in
            // every other variant carrying a `message` field): tracing
            // emits the format-string body as `fields.message`, which
            // would collide with the structured `message = %message`
            // field. JSON parsers vary on which duplicate wins, so we
            // keep the structured field as the single source of truth.
            Self::LancedbInitFailed { uri, message } => tracing::error!(
                target: STARTUP_ERROR_TARGET,
                code = "lancedb_init_failed",
                uri = %uri,
                message = %message,
            ),
            Self::EmbeddingDimensionMismatch {
                expected_dim,
                actual_dim,
                runner_name,
            } => tracing::error!(
                target: STARTUP_ERROR_TARGET,
                code = "embedding_dimension_mismatch",
                expected_dim = expected_dim,
                actual_dim = actual_dim,
                runner_name = %runner_name,
                "embedding runner dimension does not match MEMORY_VECTOR_SIZE",
            ),
            Self::MediaConfigConflict {
                backend,
                image_search_mode,
            } => tracing::error!(
                target: STARTUP_ERROR_TARGET,
                code = "media_config_conflict",
                backend = %backend,
                image_search_mode = %image_search_mode,
                "MEDIA_STORAGE_BACKEND incompatible with MEMORY_IMAGE_SEARCH_MODE",
            ),
            Self::RdbPoolInitFailed {
                url_sanitized,
                message,
            } => tracing::error!(
                target: STARTUP_ERROR_TARGET,
                code = "rdb_pool_init_failed",
                url_sanitized = %url_sanitized,
                message = %message,
            ),
            Self::EnvVarInvalid { name, message } => tracing::error!(
                target: STARTUP_ERROR_TARGET,
                code = "env_var_invalid",
                name = %name,
                message = %message,
            ),
            Self::ConfigLoadFailed { component, message } => tracing::error!(
                target: STARTUP_ERROR_TARGET,
                code = "config_load_failed",
                component = %component,
                message = %message,
            ),
            Self::Other { component, message } => tracing::error!(
                target: STARTUP_ERROR_TARGET,
                code = "other",
                component = %component,
                message = %message,
            ),
        }
    }

    /// Emit the structured tracing event, flush stdout, then exit(1).
    ///
    /// `process::exit` does not run destructors, so the stdout flush is
    /// load-bearing: the `fmt::layer().json()` layer writes through a
    /// buffered stdout, and without an explicit flush the parent never
    /// sees the line.
    pub fn fatal(self) -> ! {
        use std::io::Write as _;
        self.emit_via_tracing();
        let _ = std::io::stdout().flush();
        std::process::exit(1);
    }

    /// Route an `anyhow::Error` returned by a startup-init helper into
    /// `fatal()`: downcast to a structured variant when one is wrapped
    /// inside, otherwise fall back to `Other { component, message }`.
    /// Collapses the duplicated "downcast or wrap as Other" tail that
    /// every fatal call-site otherwise re-writes by hand, and pins the
    /// "fatal returns `!`" invariant in one place so future callers
    /// cannot accidentally leak past it.
    pub fn fatal_anyhow(component: &str, e: anyhow::Error) -> ! {
        e.downcast::<StartupError>()
            .unwrap_or_else(|other| StartupError::Other {
                component: component.to_string(),
                message: format!("{other:#}"),
            })
            .fatal()
    }
}

/// Redact `user:password@` from a URL-like connection string so it is
/// safe to log. Operates lexically (no URL parser) so it works for
/// `sqlite://`, `postgres://`, `mysql://`, and anything else with a
/// `scheme://creds@host` shape. Crate-private because the parent
/// process does not consume this — only the resulting
/// `RdbPoolInitFailed.url_sanitized` value is contractual.
pub(crate) fn sanitize_url(url: &str) -> String {
    let Some((scheme_end, _)) = url.match_indices("://").next() else {
        return url.to_string();
    };
    let body_start = scheme_end + "://".len();
    let body = &url[body_start..];
    let Some(at) = body.find('@') else {
        return url.to_string();
    };
    let mut sanitized = String::with_capacity(url.len());
    sanitized.push_str(&url[..body_start]);
    sanitized.push_str("***");
    sanitized.push_str(&body[at..]);
    sanitized
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_test::traced_test;

    // ---- Per-variant tracing emission ----
    //
    // Each test invokes `emit_via_tracing` once and asserts that
    // `tracing-test`'s capture contains both the FROZEN `code` string
    // and each FROZEN field name/value. If a variant's `code` is
    // renamed, the corresponding assertion fires — protecting the
    // agent-app contract at unit-test granularity.

    #[test]
    #[traced_test]
    fn emit_lancedb_schema_mismatch_pins_code_and_dims() {
        StartupError::LancedbSchemaMismatch {
            table: "memories".into(),
            uri: "/tmp/lancedb".into(),
            expected_dim: 2048,
            actual_dim: 768,
            expected_fingerprint: "expected_fp".into(),
            actual_fingerprint: "actual_fp".into(),
        }
        .emit_via_tracing();
        assert!(logs_contain("lancedb_schema_mismatch"));
        assert!(logs_contain("expected_dim=2048"));
        assert!(logs_contain("actual_dim=768"));
        assert!(logs_contain("table=\"memories\"") || logs_contain("table=memories"));
    }

    #[test]
    #[traced_test]
    fn emit_lancedb_init_failed_pins_code() {
        StartupError::LancedbInitFailed {
            uri: "/tmp/x.lance".into(),
            message: "permission denied".into(),
        }
        .emit_via_tracing();
        assert!(logs_contain("lancedb_init_failed"));
        assert!(logs_contain("permission denied"));
    }

    #[test]
    #[traced_test]
    fn emit_embedding_dimension_mismatch_pins_code_and_dims() {
        StartupError::EmbeddingDimensionMismatch {
            expected_dim: 768,
            actual_dim: 2048,
            runner_name: "memories-mm-embedding".into(),
        }
        .emit_via_tracing();
        assert!(logs_contain("embedding_dimension_mismatch"));
        assert!(logs_contain("expected_dim=768"));
        assert!(logs_contain("actual_dim=2048"));
        assert!(logs_contain("memories-mm-embedding"));
    }

    #[test]
    #[traced_test]
    fn emit_media_config_conflict_pins_code_and_envs() {
        StartupError::MediaConfigConflict {
            backend: "inline".into(),
            image_search_mode: "both".into(),
        }
        .emit_via_tracing();
        assert!(logs_contain("media_config_conflict"));
        assert!(logs_contain("inline"));
        assert!(logs_contain("both"));
    }

    #[test]
    #[traced_test]
    fn emit_rdb_pool_init_failed_pins_code_and_url() {
        StartupError::RdbPoolInitFailed {
            url_sanitized: "sqlite:///var/data/memory.sqlite3".into(),
            message: "disk full".into(),
        }
        .emit_via_tracing();
        assert!(logs_contain("rdb_pool_init_failed"));
        assert!(logs_contain("disk full"));
        assert!(logs_contain("sqlite:///var/data/memory.sqlite3"));
    }

    #[test]
    #[traced_test]
    fn emit_env_var_invalid_pins_code_and_name() {
        StartupError::EnvVarInvalid {
            name: "GRPC_ADDR".into(),
            message: "must be specified".into(),
        }
        .emit_via_tracing();
        assert!(logs_contain("env_var_invalid"));
        assert!(logs_contain("GRPC_ADDR"));
        assert!(logs_contain("must be specified"));
    }

    #[test]
    #[traced_test]
    fn emit_config_load_failed_pins_code_and_component() {
        StartupError::ConfigLoadFailed {
            component: "MemoryCacheConfig".into(),
            message: "missing field `cache_max_cost`".into(),
        }
        .emit_via_tracing();
        assert!(logs_contain("config_load_failed"));
        assert!(logs_contain("MemoryCacheConfig"));
    }

    #[test]
    #[traced_test]
    fn emit_other_pins_code_and_component() {
        StartupError::Other {
            component: "front".into(),
            message: "unexpected init failure".into(),
        }
        .emit_via_tracing();
        assert!(logs_contain("other"));
        assert!(logs_contain("front"));
    }

    // ---- Serde round-trip ----
    //
    // The parent process deserializes the tracing row's `fields` block
    // directly into `StartupError` via `#[serde(tag = "code")]`. If a
    // field is renamed or a variant is restructured, the round-trip
    // below breaks, mirroring the failure the parent would hit at
    // runtime.

    fn round_trip(err: &StartupError) {
        let s = serde_json::to_string(err).expect("serialize");
        let back: StartupError = serde_json::from_str(&s).expect("deserialize");
        assert_eq!(*err, back, "round-trip must preserve fields: {s}");
    }

    #[test]
    fn serde_round_trip_for_each_variant() {
        for err in sample_errors() {
            round_trip(&err);
        }
    }

    /// Pin the snake_case `code` strings the parent matches on. Adding
    /// a new variant requires adding an entry here; renaming an
    /// existing one fails this test immediately rather than degrading
    /// silently in production.
    #[test]
    fn code_strings_are_stable() {
        let expected: &[(&str, &str)] = &[
            ("LancedbSchemaMismatch", "lancedb_schema_mismatch"),
            ("LancedbInitFailed", "lancedb_init_failed"),
            ("EmbeddingDimensionMismatch", "embedding_dimension_mismatch"),
            ("MediaConfigConflict", "media_config_conflict"),
            ("RdbPoolInitFailed", "rdb_pool_init_failed"),
            ("EnvVarInvalid", "env_var_invalid"),
            ("ConfigLoadFailed", "config_load_failed"),
            ("Other", "other"),
        ];
        let samples = sample_errors();
        assert_eq!(
            samples.len(),
            expected.len(),
            "sample_errors must cover every variant"
        );
        for (sample, (variant, code)) in samples.iter().zip(expected.iter()) {
            let v = serde_json::to_value(sample).expect("serialize");
            let actual = v.get("code").and_then(|c| c.as_str()).unwrap_or_default();
            assert_eq!(
                actual, *code,
                "{variant} must serialize with code={code} (got {actual})"
            );
        }
    }

    /// `StartupError` is wrapped in `anyhow::Error` at the
    /// LanceDB-init Err sites so the call-site at `module.rs` can
    /// `downcast_ref::<StartupError>()` to choose the right `fatal()`
    /// branch. This pins that the round-trip through `anyhow` works.
    #[test]
    fn anyhow_downcast_works() {
        let original = StartupError::LancedbSchemaMismatch {
            table: "memories".into(),
            uri: "/tmp/x".into(),
            expected_dim: 2048,
            actual_dim: 768,
            expected_fingerprint: "a".into(),
            actual_fingerprint: "b".into(),
        };
        let wrapped: anyhow::Error = anyhow::Error::new(original.clone());
        let downcast = wrapped
            .downcast_ref::<StartupError>()
            .expect("must downcast back to StartupError");
        assert_eq!(*downcast, original);
    }

    /// Regression test for the duplicate-`message`-key bug: every
    /// variant carrying a `message` field must emit it exactly once in
    /// the JSON-rendered `fields` block. Older code passed a free-form
    /// format string ("LanceDB initialization failed") alongside
    /// `message = %message`, and the JSON layer rendered both into
    /// `fields.message`, leaving the parent process to guess which
    /// duplicate to read.
    #[test]
    fn emit_produces_no_duplicate_message_key_in_json() {
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::{fmt, prelude::*};

        // `MakeWriter` cloning each event into our buffer.
        #[derive(Clone, Default)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for BufWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> fmt::MakeWriter<'a> for BufWriter {
            type Writer = BufWriter;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        // Drive each `message`-carrying variant through the real
        // `fmt::layer().json()` pipeline the parent process scans and
        // assert the resulting JSON row has exactly one `message`
        // field equal to the expected payload.
        for (err, expected_message) in [
            (
                StartupError::LancedbInitFailed {
                    uri: "/tmp/x".into(),
                    message: "permission denied".into(),
                },
                "permission denied",
            ),
            (
                StartupError::RdbPoolInitFailed {
                    url_sanitized: "sqlite:///x".into(),
                    message: "disk full".into(),
                },
                "disk full",
            ),
            (
                StartupError::EnvVarInvalid {
                    name: "GRPC_ADDR".into(),
                    message: "must be specified".into(),
                },
                "must be specified",
            ),
            (
                StartupError::ConfigLoadFailed {
                    component: "MemoryCacheConfig".into(),
                    message: "missing field `max_cost`".into(),
                },
                "missing field `max_cost`",
            ),
            (
                StartupError::Other {
                    component: "front".into(),
                    message: "unexpected".into(),
                },
                "unexpected",
            ),
        ] {
            let buf = BufWriter::default();
            let subscriber =
                tracing_subscriber::registry().with(fmt::layer().json().with_writer(buf.clone()));
            tracing::subscriber::with_default(subscriber, || err.emit_via_tracing());

            let bytes = buf.0.lock().unwrap().clone();
            let line = std::str::from_utf8(&bytes).expect("utf-8 JSON output");
            // Raw byte check: only one `"message":` substring inside the
            // event. Catches the duplicate even if `serde_json`'s
            // last-key-wins behaviour would otherwise mask it.
            let count = line.matches("\"message\":").count();
            assert_eq!(
                count, 1,
                "{err:?} emits more than one `message` key: {line}"
            );

            // Semantic check: the surviving message is the payload, not
            // a generic header string. `from_str` collapses duplicates
            // (last-wins on serde_json) so this would silently pass on
            // the buggy form; combined with the count check above it
            // pins both keys.
            let row: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
            let msg = row
                .get("fields")
                .and_then(|f| f.get("message"))
                .and_then(|m| m.as_str())
                .unwrap_or_default();
            assert_eq!(
                msg, expected_message,
                "fields.message should be the structured payload"
            );
        }
    }

    #[test]
    fn sanitize_url_strips_credentials_when_present() {
        assert_eq!(
            sanitize_url("postgres://user:pw@host:5432/db"),
            "postgres://***@host:5432/db"
        );
    }

    #[test]
    fn sanitize_url_passes_through_when_no_credentials() {
        assert_eq!(
            sanitize_url("sqlite:///var/data/memory.sqlite3"),
            "sqlite:///var/data/memory.sqlite3"
        );
        assert_eq!(sanitize_url("no-scheme-form"), "no-scheme-form");
    }

    fn sample_errors() -> Vec<StartupError> {
        vec![
            StartupError::LancedbSchemaMismatch {
                table: "memories".into(),
                uri: "/tmp/x".into(),
                expected_dim: 2048,
                actual_dim: 768,
                expected_fingerprint: "a".into(),
                actual_fingerprint: "b".into(),
            },
            StartupError::LancedbInitFailed {
                uri: "/tmp/x".into(),
                message: "permission denied".into(),
            },
            StartupError::EmbeddingDimensionMismatch {
                expected_dim: 768,
                actual_dim: 2048,
                runner_name: "memories-mm-embedding".into(),
            },
            StartupError::MediaConfigConflict {
                backend: "inline".into(),
                image_search_mode: "both".into(),
            },
            StartupError::RdbPoolInitFailed {
                url_sanitized: "sqlite:///x".into(),
                message: "disk full".into(),
            },
            StartupError::EnvVarInvalid {
                name: "GRPC_ADDR".into(),
                message: "must be specified".into(),
            },
            StartupError::ConfigLoadFailed {
                component: "MemoryCacheConfig".into(),
                message: "missing field".into(),
            },
            StartupError::Other {
                component: "front".into(),
                message: "unexpected".into(),
            },
        ]
    }
}
