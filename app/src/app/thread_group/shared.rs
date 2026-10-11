//! Shared state gates, small conversions, and the creation-time canonical
//! key assignment used across ThreadGroup and thread services.

use super::domain::{MembershipRole, MembershipState};
use super::prelude::*;

/// Server-side emergency ThreadGroup write gate (design 9.4). Default
/// enabled; set `THREAD_GROUP_WRITES_ENABLED=false` and restart to stop
/// ThreadGroup writes. The suppression check is never bypassed.
static THREAD_GROUP_WRITES_OVERRIDE: std::sync::atomic::AtomicI8 =
    std::sync::atomic::AtomicI8::new(-1);

/// Test / embedding override for the write gate. `None` restores the
/// environment-derived default.
pub fn set_thread_group_writes_enabled_override(enabled: Option<bool>) {
    let value = match enabled {
        Some(true) => 1,
        Some(false) => 0,
        None => -1,
    };
    THREAD_GROUP_WRITES_OVERRIDE.store(value, std::sync::atomic::Ordering::Relaxed);
}

pub fn thread_group_writes_enabled() -> bool {
    match THREAD_GROUP_WRITES_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed) {
        0 => false,
        1 => true,
        _ => std::env::var("THREAD_GROUP_WRITES_ENABLED")
            .map(|value| !value.eq_ignore_ascii_case("false"))
            .unwrap_or(true),
    }
}

pub(crate) fn ensure_thread_group_writes() -> anyhow::Result<()> {
    if thread_group_writes_enabled() {
        Ok(())
    } else {
        anyhow::bail!("ThreadGroup writes are disabled")
    }
}

pub(crate) fn known_scope_value(scope: &IdentityScope) -> Option<String> {
    match scope {
        IdentityScope::Known(value) => Some(value.clone()),
        IdentityScope::Unknown => None,
    }
}

/// Random 64-hex material for a manual / non-source Thread canonical
/// key. Generated once at thread creation (origin `creation_uuid`); the
/// stored value is never regenerated.
fn new_manual_thread_canonical_key() -> String {
    use rand::Rng;
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Assign a new manual / non-source Thread its `creation_uuid` canonical
/// key inside the creating transaction. Every live Thread must carry a
/// key: the canonical-key data migration re-verifies that on each
/// release and stops the deploy on a keyless Thread.
pub(crate) async fn assign_creation_canonical_key_tx(
    canonical_keys: &impl ThreadCanonicalKeyRepository,
    tx: &mut RdbTransaction<'_>,
    thread_id: i64,
    user_id: i64,
    now: i64,
) -> anyhow::Result<String> {
    let key = new_manual_thread_canonical_key();
    canonical_keys
        .assign_tx(
            &mut **tx,
            thread_id,
            user_id,
            &key,
            values::canonical_key_origin::CREATION_UUID,
            now,
        )
        .await?;
    Ok(key)
}

/// Asserts the invariant the canonical-key data migration verifies on
/// every release: the thread has its canonical key (at most one row, by
/// primary key). For a thread without a source identity it is a
/// `creation_uuid` key owned by `user_id`; returns it.
#[cfg(test)]
pub(crate) async fn assert_creation_canonical_key(
    pool: &'static RdbPool,
    thread_id: i64,
    user_id: i64,
) -> anyhow::Result<String> {
    let row = ThreadCanonicalKeyRepositoryImpl::new(pool)
        .find_by_thread_id(thread_id)
        .await?
        .unwrap_or_else(|| panic!("thread {thread_id} has no canonical key"));
    assert_eq!(row.origin, values::canonical_key_origin::CREATION_UUID);
    assert_eq!(row.user_id, user_id);
    assert_eq!(row.key.len(), 64);
    Ok(row.key)
}

/// Immutable policy version recorded on every selected relation and
/// operator decision (spec 3.4). Changing the adoption rules requires a
/// new version; existing evidence is never rewritten.
pub const THREAD_GROUP_POLICY_VERSION: &str = "thread-group-policy-v1";
/// Evidence-fingerprint attribute tuple version (spec 3.4).
pub const EVIDENCE_FINGERPRINT_VERSION: &str = "thread-group-evidence-v1";

pub(crate) fn membership_role_of(value: &str) -> MembershipRole {
    match value {
        "root" => MembershipRole::Root,
        _ => MembershipRole::Member,
    }
}

pub(crate) fn membership_role_str(role: &MembershipRole) -> String {
    match role {
        MembershipRole::Root => values::member_role::ROOT,
        MembershipRole::Member => values::member_role::MEMBER,
    }
    .to_string()
}

pub(crate) fn membership_state_of(value: &str) -> MembershipState {
    match value {
        "deleted" => MembershipState::Deleted,
        "redirected" => MembershipState::Redirected,
        _ => MembershipState::Active,
    }
}

pub(crate) fn membership_state_str(state: &MembershipState) -> String {
    match state {
        MembershipState::Active => values::member_state::ACTIVE,
        MembershipState::Deleted => values::member_state::DELETED,
        MembershipState::Redirected => values::member_state::REDIRECTED,
    }
    .to_string()
}
