//! ThreadGroup application facade.
//!
//! Responsibility-specific implementations live in private modules under
//! `thread_group/`; this module preserves the historical public API.

mod domain;
mod lifecycle;
mod observation;
mod operator;
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
pub use shared::{
    EVIDENCE_FINGERPRINT_VERSION, THREAD_GROUP_POLICY_VERSION, new_manual_thread_canonical_key,
    set_thread_group_writes_enabled_override, thread_group_writes_enabled,
};
pub(crate) use shared::{
    ensure_thread_group_writes, known_scope_value, membership_role_of, membership_role_str,
    membership_state_of, membership_state_str,
};
