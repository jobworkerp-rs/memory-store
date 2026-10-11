//! ThreadGroup application facade.
//!
//! Responsibility-specific implementations live in private modules under
//! `thread_group/`; this module preserves the historical public API.

mod domain;
mod lifecycle;
pub mod memory_relation;
mod observation;
mod operator;
pub mod orphan_cleanup;
mod outbox;
mod prelude;
mod read_search;
mod reconcile;
mod shared;

pub use domain::*;
pub use lifecycle::*;
pub use observation::*;
pub use operator::*;
pub use outbox::*;
pub use read_search::*;
pub use reconcile::*;
#[cfg(test)]
pub(crate) use shared::assert_creation_canonical_key;
pub use shared::{
    EVIDENCE_FINGERPRINT_VERSION, THREAD_GROUP_POLICY_VERSION,
    set_thread_group_writes_enabled_override, thread_group_writes_enabled,
};
pub(crate) use shared::{
    assign_creation_canonical_key_tx, ensure_thread_group_writes, known_scope_value,
    membership_role_of, membership_role_str, membership_state_of, membership_state_str,
};
