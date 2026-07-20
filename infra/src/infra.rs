pub mod embedding_dispatch;
pub mod media_object;
pub mod media_storage;
pub mod memory;
pub mod memory_kind_migration;
pub mod memory_rating;
pub mod memory_vector;
pub mod module;
pub mod reflection;
pub mod reflection_intent_dispatch;
pub mod reflection_intent_vector;
pub mod reflection_summary_dispatch;
pub(in crate::infra) mod resource;
pub mod startup_error;
pub mod thread;
pub mod thread_label;
pub mod thread_memory;
pub mod thread_vector;

// Phase A smoke tests for the reflection schema migration. The full
// repositories land in Phase B; this file only confirms the migration
// is wired up and the seed data is present.
#[cfg(test)]
mod reflection_schema_test;

use crate::error::LlmMemoryError;
use anyhow::Result;
use command_utils::util::id_generator::{self, IDGenerator};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct IdGeneratorWrapper {
    id_generator: Arc<Mutex<IDGenerator>>,
}

impl Default for IdGeneratorWrapper {
    fn default() -> Self {
        Self::new()
    }
}

impl IdGeneratorWrapper {
    pub fn new() -> Self {
        IdGeneratorWrapper {
            id_generator: Arc::new(Mutex::new(id_generator::new_generator_by_ip())),
        }
    }
    // thread safe
    pub fn generate_id(&self) -> Result<i64> {
        self.id_generator
            .lock()
            .map_err(|e| LlmMemoryError::GenerateIdError(e.to_string()).into())
            .and_then(|mut g| g.generate())
    }
}

pub trait UseIdGenerator {
    fn id_generator(&self) -> &IdGeneratorWrapper;
}

/// Fill in server timestamp when client sends 0 (protobuf default).
pub fn fill_timestamps(created_at: i64, updated_at: i64) -> (i64, i64) {
    let now = command_utils::util::datetime::now_millis();
    (
        if created_at == 0 { now } else { created_at },
        if updated_at == 0 { now } else { updated_at },
    )
}

/// Fill in server timestamp for updated_at when client sends 0.
pub fn fill_updated_at(updated_at: i64) -> i64 {
    if updated_at == 0 {
        command_utils::util::datetime::now_millis()
    } else {
        updated_at
    }
}

/// Validate that the auto-embedding GRPC callback env vars are set
/// **and contain a usable host/port pair**, and reject the obsolete
/// `MEMORY_GRPC_ADDR` form.
///
/// Auto-embedding workflows in `workflows/auto-*-workers.yaml` reference
/// `%{MEMORY_GRPC_HOST}` / `%{MEMORY_GRPC_PORT}` *without* a `:-default`
/// fallback. A connection target is operationally critical: silently
/// defaulting to `127.0.0.1:9010` means a misconfigured deployment loses
/// every embedding write to a non-routable address, which is far worse
/// than refusing to start. So this function fails closed.
///
/// "Set" alone is not enough: `MEMORY_GRPC_HOST=` (empty) or
/// `MEMORY_GRPC_PORT=abc` would survive a presence-only check, get
/// substituted into the YAML, and surface only as obscure
/// `tonic::transport::Error` deep inside the dispatcher's lazy init —
/// where the eager-init warning path keeps the dispatcher alive and
/// retries forever. Validating shape here keeps the failure mode
/// symmetric: any mistake an operator can make in the env collapses to
/// a single, descriptive startup error.
///
/// Wildcard listen addresses (`0.0.0.0` / `::`) are rejected because the
/// callback side is a *destination*, not a bind address — accepting them
/// would silently produce un-routable callbacks, which is exactly the
/// silent-loss class of failure this validation is supposed to prevent.
///
/// We also detect the legacy `MEMORY_GRPC_ADDR=host:port` form (used in
/// the codebase before workers were moved to YAML) and emit an explicit
/// error rather than silently ignoring it — operators upgrading from an
/// older release would otherwise see their callback go to the wrong
/// place without any indication that the env var no longer applies.
///
/// Returns `Err` whenever auto-embedding is enabled but the env is in a
/// state that would silently misroute callbacks. Callers that do not
/// need auto-embedding can skip this check entirely.
///
/// **Not validated here:** DNS resolvability of the host. K8s sidecar /
/// CoreDNS readiness can lag the memories process at startup, so a
/// resolution check would refuse to boot in scenarios where the address
/// becomes valid moments later. The dispatcher's lazy retry handles
/// that case.
pub fn require_grpc_callback_env() -> Result<()> {
    if std::env::var_os("MEMORY_GRPC_ADDR").is_some() {
        anyhow::bail!(
            "MEMORY_GRPC_ADDR is no longer supported. Split into MEMORY_GRPC_HOST and \
             MEMORY_GRPC_PORT (e.g. MEMORY_GRPC_HOST=127.0.0.1, MEMORY_GRPC_PORT=9010). \
             See dot.env and yaml-workers.md for details."
        );
    }
    let host = std::env::var("MEMORY_GRPC_HOST").map_err(|_| {
        anyhow::anyhow!(
            "MEMORY_GRPC_HOST must be set when auto-embedding is enabled \
             (the embedding workflow calls back into this server via gRPC). \
             See dot.env for the required value."
        )
    })?;
    let port_raw = std::env::var("MEMORY_GRPC_PORT").map_err(|_| {
        anyhow::anyhow!(
            "MEMORY_GRPC_PORT must be set when auto-embedding is enabled \
             (the embedding workflow calls back into this server via gRPC). \
             See dot.env for the required value."
        )
    })?;
    validate_callback_host(&host)?;
    validate_callback_port(&port_raw)?;
    Ok(())
}

/// Reject host values that the YAML expander would happily substitute
/// but the GRPC callback cannot use. Pure function so the validation is
/// unit-testable without touching env. See `require_grpc_callback_env`
/// for why each rule exists.
fn validate_callback_host(host: &str) -> Result<()> {
    if host.is_empty() {
        anyhow::bail!(
            "MEMORY_GRPC_HOST is set but empty. The embedding workflow needs a routable \
             host name or IP to call back into this server."
        );
    }
    // Trim is intentional: a trailing newline / space slipped in via a
    // shell `export` is almost certainly a typo, not a hostname.
    if host.trim() != host {
        anyhow::bail!(
            "MEMORY_GRPC_HOST has leading/trailing whitespace ({host:?}); strip it before exporting."
        );
    }
    // Reject wildcards explicitly: `0.0.0.0` / `::` are listen-side
    // addresses, not destinations. Catch the textual form (we are not
    // doing DNS / IP parsing here) so an operator copy-pasting GRPC_ADDR
    // into MEMORY_GRPC_HOST gets a clear error rather than a silent
    // unroutable callback.
    if matches!(host, "0.0.0.0" | "::" | "[::]") {
        anyhow::bail!(
            "MEMORY_GRPC_HOST={host} is a wildcard listen address and cannot be used as a \
             callback destination. Use a routable host name or IP (loopback, container name, \
             k8s Service, or external IP) — this is the address jobworkerp uses to reach back \
             into this server, not the bind address."
        );
    }
    Ok(())
}

/// Reject port values that the YAML expander would substitute but the
/// GRPC client would fail to connect with. Returns the parsed port for
/// callers that want it.
fn validate_callback_port(port_raw: &str) -> Result<u16> {
    let port: u16 = port_raw.parse().map_err(|_| {
        anyhow::anyhow!(
            "MEMORY_GRPC_PORT={port_raw:?} is not a valid port number (must be 1..=65535)"
        )
    })?;
    if port == 0 {
        anyhow::bail!(
            "MEMORY_GRPC_PORT=0 is reserved (kernel-assigned port) and cannot be used as a \
             callback destination."
        );
    }
    Ok(port)
}

#[cfg(test)]
mod require_grpc_callback_env_tests {
    use super::*;
    use serial_test::serial;

    /// Restore the env to a known-clean state before every test so the
    /// suite does not depend on test execution order or external env.
    fn clear_callback_env() {
        // SAFETY: tests in this module are #[serial], no other thread
        // observes env at this moment.
        unsafe {
            std::env::remove_var("MEMORY_GRPC_HOST");
            std::env::remove_var("MEMORY_GRPC_PORT");
            std::env::remove_var("MEMORY_GRPC_ADDR");
        }
    }

    #[test]
    #[serial]
    fn rejects_deprecated_memory_grpc_addr() {
        clear_callback_env();
        // SAFETY: serial-guarded.
        unsafe {
            std::env::set_var("MEMORY_GRPC_ADDR", "host:9010");
            std::env::set_var("MEMORY_GRPC_HOST", "host");
            std::env::set_var("MEMORY_GRPC_PORT", "9010");
        }
        let err = require_grpc_callback_env().unwrap_err().to_string();
        assert!(
            err.contains("MEMORY_GRPC_ADDR is no longer supported"),
            "error must call out the deprecated env var: {err}"
        );
        clear_callback_env();
    }

    #[test]
    #[serial]
    fn requires_both_host_and_port() {
        clear_callback_env();
        // With both unset, host is checked first and surfaces its own
        // error. The companion test `requires_port_when_only_host_is_set`
        // covers the symmetric case for port.
        let err = require_grpc_callback_env().unwrap_err().to_string();
        assert!(
            err.contains("MEMORY_GRPC_HOST must be set"),
            "missing env must surface a clear error: {err}"
        );
    }

    #[test]
    #[serial]
    fn requires_port_when_only_host_is_set() {
        clear_callback_env();
        // SAFETY: serial-guarded.
        unsafe { std::env::set_var("MEMORY_GRPC_HOST", "127.0.0.1") };
        let err = require_grpc_callback_env().unwrap_err().to_string();
        assert!(
            err.contains("MEMORY_GRPC_PORT must be set"),
            "error must call out the missing var: {err}"
        );
        clear_callback_env();
    }

    #[test]
    #[serial]
    fn accepts_when_both_are_set() {
        clear_callback_env();
        // SAFETY: serial-guarded.
        unsafe {
            std::env::set_var("MEMORY_GRPC_HOST", "127.0.0.1");
            std::env::set_var("MEMORY_GRPC_PORT", "9010");
        }
        require_grpc_callback_env().expect("both set must succeed");
        clear_callback_env();
    }

    // ---- shape validation: presence is not enough ----

    #[test]
    #[serial]
    fn rejects_empty_host() {
        clear_callback_env();
        // SAFETY: serial-guarded.
        unsafe {
            std::env::set_var("MEMORY_GRPC_HOST", "");
            std::env::set_var("MEMORY_GRPC_PORT", "9010");
        }
        let err = require_grpc_callback_env().unwrap_err().to_string();
        assert!(
            err.contains("set but empty"),
            "empty host must be rejected with a clear error: {err}"
        );
        clear_callback_env();
    }

    #[test]
    #[serial]
    fn rejects_wildcard_v4_host() {
        clear_callback_env();
        // SAFETY: serial-guarded.
        unsafe {
            std::env::set_var("MEMORY_GRPC_HOST", "0.0.0.0");
            std::env::set_var("MEMORY_GRPC_PORT", "9010");
        }
        let err = require_grpc_callback_env().unwrap_err().to_string();
        assert!(
            err.contains("wildcard listen address"),
            "0.0.0.0 must be rejected explicitly: {err}"
        );
        clear_callback_env();
    }

    #[test]
    #[serial]
    fn rejects_wildcard_v6_host() {
        clear_callback_env();
        // SAFETY: serial-guarded.
        unsafe {
            std::env::set_var("MEMORY_GRPC_HOST", "::");
            std::env::set_var("MEMORY_GRPC_PORT", "9010");
        }
        let err = require_grpc_callback_env().unwrap_err().to_string();
        assert!(
            err.contains("wildcard listen address"),
            ":: must be rejected explicitly: {err}"
        );
        clear_callback_env();
    }

    #[test]
    #[serial]
    fn rejects_host_with_whitespace() {
        clear_callback_env();
        // SAFETY: serial-guarded.
        unsafe {
            std::env::set_var("MEMORY_GRPC_HOST", "127.0.0.1\n");
            std::env::set_var("MEMORY_GRPC_PORT", "9010");
        }
        let err = require_grpc_callback_env().unwrap_err().to_string();
        assert!(
            err.contains("whitespace"),
            "trailing whitespace must be flagged: {err}"
        );
        clear_callback_env();
    }

    #[test]
    #[serial]
    fn rejects_non_numeric_port() {
        clear_callback_env();
        // SAFETY: serial-guarded.
        unsafe {
            std::env::set_var("MEMORY_GRPC_HOST", "127.0.0.1");
            std::env::set_var("MEMORY_GRPC_PORT", "abc");
        }
        let err = require_grpc_callback_env().unwrap_err().to_string();
        assert!(
            err.contains("not a valid port number"),
            "non-numeric port must be rejected: {err}"
        );
        clear_callback_env();
    }

    #[test]
    #[serial]
    fn rejects_port_zero() {
        clear_callback_env();
        // SAFETY: serial-guarded.
        unsafe {
            std::env::set_var("MEMORY_GRPC_HOST", "127.0.0.1");
            std::env::set_var("MEMORY_GRPC_PORT", "0");
        }
        let err = require_grpc_callback_env().unwrap_err().to_string();
        assert!(
            err.contains("port=0") || err.contains("PORT=0"),
            "port 0 must be rejected (kernel-assigned port has no destination meaning): {err}"
        );
        clear_callback_env();
    }

    #[test]
    #[serial]
    fn rejects_port_overflow() {
        clear_callback_env();
        // SAFETY: serial-guarded.
        unsafe {
            std::env::set_var("MEMORY_GRPC_HOST", "127.0.0.1");
            std::env::set_var("MEMORY_GRPC_PORT", "70000");
        }
        let err = require_grpc_callback_env().unwrap_err().to_string();
        assert!(
            err.contains("not a valid port number"),
            "out-of-range port must be rejected via parse failure: {err}"
        );
        clear_callback_env();
    }

    // Pure-function tests for validate_callback_host / _port so the rules
    // are pinned independently of the env-driven entry point.
    #[test]
    fn validate_callback_host_accepts_routable() {
        validate_callback_host("127.0.0.1").unwrap();
        validate_callback_host("memories.svc.cluster.local").unwrap();
        validate_callback_host("[::1]").unwrap();
    }

    #[test]
    fn validate_callback_port_accepts_typical_values() {
        assert_eq!(validate_callback_port("1").unwrap(), 1);
        assert_eq!(validate_callback_port("9010").unwrap(), 9010);
        assert_eq!(validate_callback_port("65535").unwrap(), 65535);
    }
}
