use anyhow::Result;
use infra::error::LlmMemoryError;
use protobuf::llm_memory::data::{MemoryData, MemoryKind, ThreadData};
use std::sync::atomic::{AtomicU64, Ordering};

pub const UNSPECIFIED_CREATE_COMPAT_ENV: &str = "MEMORY_KIND_UNSPECIFIED_CREATE_COMPAT";
static UNSPECIFIED_CREATE_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn unspecified_create_count() -> u64 {
    UNSPECIFIED_CREATE_COUNT.load(Ordering::Relaxed)
}

fn compat_enabled() -> bool {
    std::env::var(UNSPECIFIED_CREATE_COMPAT_ENV)
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(true)
}

fn validate_explicit_kind(kind: i32) -> Result<()> {
    if (MemoryKind::Raw as i32..=MemoryKind::Reflection as i32).contains(&kind) {
        return Ok(());
    }
    Err(
        LlmMemoryError::InvalidArgument(format!("memory_kind must be one of 1..=7, got {kind}"))
            .into(),
    )
}

/// Shared policy for both `normalize_memory_for_create` and
/// `normalize_thread_for_create`: an unspecified kind is backfilled to
/// RAW while the compat flag is enabled, otherwise every kind
/// must be explicit and valid.
fn resolve_created_kind(kind: i32, entity: &str, operation: &str) -> Result<i32> {
    if kind != MemoryKind::Unspecified as i32 {
        validate_explicit_kind(kind)?;
        return Ok(kind);
    }
    if !compat_enabled() {
        return Err(LlmMemoryError::InvalidArgument(format!(
            "memory_kind is required for {operation}"
        ))
        .into());
    }
    UNSPECIFIED_CREATE_COUNT.fetch_add(1, Ordering::Relaxed);
    tracing::warn!(
        operation,
        entity,
        converted_kind = "RAW",
        "legacy memory_kind was normalized"
    );
    Ok(MemoryKind::Raw as i32)
}

pub fn normalize_memory_for_create(mut memory: MemoryData, operation: &str) -> Result<MemoryData> {
    memory.memory_kind = resolve_created_kind(memory.memory_kind, "memory", operation)?;
    Ok(memory)
}

pub fn normalize_thread_for_create(mut thread: ThreadData, operation: &str) -> Result<ThreadData> {
    thread.memory_kind = resolve_created_kind(thread.memory_kind, "thread", operation)?;
    Ok(thread)
}

/// Shared policy for both `preserve_memory_kind_for_update` and
/// `preserve_thread_kind_for_update`: the kind selects which validation and
/// storage rules apply to a row (e.g. `validate_memory_kind_matches_thread`),
/// so changing it after creation would let a row silently switch rule sets.
/// It is therefore immutable once the row has been created. Returns the
/// effective kind to persist, backfilling a legacy
/// (pre-migration) stored value of UNSPECIFIED to RAW so that
/// zero never round-trips back into storage.
///
/// Also used directly by `ThreadApp::add_memories_batch`'s
/// upsert-by-channel path, which re-validates a `ThreadData` against an
/// already row-locked existing thread outside the normal update flow.
pub(super) fn resolve_preserved_kind(
    requested_kind: i32,
    stored_kind: i32,
    operation: &str,
) -> Result<i32> {
    let effective_stored_kind = effective_kind(stored_kind);
    if requested_kind != MemoryKind::Unspecified as i32 {
        validate_explicit_kind(requested_kind)?;
        if requested_kind != effective_stored_kind {
            return Err(LlmMemoryError::InvalidArgument(format!(
                "memory_kind is immutable for {operation}: stored={effective_stored_kind}, requested={requested_kind}"
            ))
            .into());
        }
    }
    Ok(effective_stored_kind)
}

/// Preserve the stored kind on update. See `resolve_preserved_kind` for why
/// the kind is immutable once the row has been created.
pub fn preserve_memory_kind_for_update(
    mut memory: MemoryData,
    stored_kind: i32,
    operation: &str,
) -> Result<MemoryData> {
    memory.memory_kind = resolve_preserved_kind(memory.memory_kind, stored_kind, operation)?;
    Ok(memory)
}

/// Preserve the stored kind on update. See `preserve_memory_kind_for_update`.
pub fn preserve_thread_kind_for_update(
    mut thread: ThreadData,
    stored_kind: i32,
    operation: &str,
) -> Result<ThreadData> {
    thread.memory_kind = resolve_preserved_kind(thread.memory_kind, stored_kind, operation)?;
    Ok(thread)
}

/// Delegates to the same legacy-backfill policy the vector-store records
/// use (`infra::infra::memory_vector::record::normalized_memory_kind`),
/// so "unspecified means RAW" has one implementation shared
/// across the RDB and vector-store layers.
fn effective_kind(kind: i32) -> i32 {
    infra::infra::memory_vector::record::normalized_memory_kind(kind)
}

pub fn validate_memory_kind_matches_thread(thread_kind: i32, memory: &MemoryData) -> Result<()> {
    // Legacy rows (either side) may still contain the proto default until
    // the backfill runs.
    let effective_thread_kind = effective_kind(thread_kind);
    let effective_memory_kind = effective_kind(memory.memory_kind);
    if effective_memory_kind != effective_thread_kind {
        return Err(LlmMemoryError::InvalidArgument(format!(
            "memory_kind {effective_memory_kind} does not match thread memory_kind {effective_thread_kind}"
        ))
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use protobuf::llm_memory::data::{MemoryData, UserId};

    fn memory(kind: i32) -> MemoryData {
        MemoryData {
            user_id: Some(UserId { value: 10 }),
            memory_kind: kind,
            ..Default::default()
        }
    }

    #[test]
    fn normalizes_unspecified_by_default() {
        let result = normalize_memory_for_create(memory(0), "test").unwrap();
        assert_eq!(result.memory_kind, MemoryKind::Raw as i32);
    }

    #[test]
    fn allows_a_memory_author_different_from_the_thread_creator() {
        let mut different_author = memory(MemoryKind::Raw as i32);
        different_author.user_id = Some(UserId { value: 11 });
        validate_memory_kind_matches_thread(MemoryKind::Raw as i32, &different_author).unwrap();

        let error = validate_memory_kind_matches_thread(
            MemoryKind::Raw as i32,
            &memory(MemoryKind::Personality as i32),
        )
        .unwrap_err();
        assert!(error.to_string().contains("memory_kind"));
    }

    #[test]
    fn update_preserves_unspecified_input_for_non_conversation_kind() {
        let updated = preserve_memory_kind_for_update(
            memory(MemoryKind::Unspecified as i32),
            MemoryKind::Reflection as i32,
            "test",
        )
        .unwrap();
        assert_eq!(updated.memory_kind, MemoryKind::Reflection as i32);
    }

    #[test]
    fn update_rejects_kind_change() {
        let error = preserve_thread_kind_for_update(
            ThreadData {
                memory_kind: MemoryKind::Raw as i32,
                ..Default::default()
            },
            MemoryKind::Reflection as i32,
            "test",
        )
        .unwrap_err();
        assert!(error.to_string().contains("immutable"));
    }

    #[test]
    fn update_backfills_legacy_unspecified_stored_kind() {
        // A pre-migration row stored with memory_kind=0 (UNSPECIFIED) must
        // never round-trip 0 back into storage: it is treated as
        // RAW and that value is what gets persisted.
        let updated = preserve_memory_kind_for_update(
            memory(MemoryKind::Unspecified as i32),
            MemoryKind::Unspecified as i32,
            "test",
        )
        .unwrap();
        assert_eq!(updated.memory_kind, MemoryKind::Raw as i32);
    }
}
