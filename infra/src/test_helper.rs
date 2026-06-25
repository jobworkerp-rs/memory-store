//! Test-only helpers shared across the workspace.
//!
//! `SnowflakeIdBucket::new()` snapshots wall-clock millis into
//! `last_time_millis` at construction and then advances `idx` 0..4091
//! *without* re-reading the clock (`lazy_generate`). Two wrappers
//! constructed within the same millisecond on the same host therefore
//! share `(machine_id, node_id, last_time_millis)` and emit the *same*
//! id sequence — surfacing in CI as flaky `UNIQUE constraint failed`
//! errors on `thread.id` / `memory.id` whenever two test setups
//! (or two consecutive tests under `--test-threads=1`) each call
//! `IdGeneratorWrapper::new()`.
//!
//! `shared_id_generator()` returns a process-wide singleton whose
//! internal bucket is monotonic — back-to-back tests in the same
//! binary cannot collide.
//!
//! This module is intentionally `pub`. It is unused by the production
//! binary; gating it behind a `cfg` would force every test crate that
//! depends on `infra` to enable an extra feature, which has no payoff
//! given the body is one `OnceLock` plus a `clone()`.

use crate::infra::IdGeneratorWrapper;
use std::sync::OnceLock;

/// Returns the process-wide shared `IdGeneratorWrapper` used by every
/// integration test. Cloning is cheap (the wrapper is `Arc<Mutex<…>>`
/// inside) and all clones share the same monotonic Snowflake bucket.
pub fn shared_id_generator() -> IdGeneratorWrapper {
    static SHARED: OnceLock<IdGeneratorWrapper> = OnceLock::new();
    SHARED.get_or_init(IdGeneratorWrapper::new).clone()
}
