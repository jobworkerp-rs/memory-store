//! Index bookkeeping around an embedding write: verify the result
//! against the current space and source version, drop the target's entry
//! before its rows are replaced, and record the success entry after.
//! Interrupted at any point, the index never claims more than the rows.

use super::table::{EmbeddingIndex, EntryOutcome, IndexEntry};
use super::{SourceVersion, TableLabel};
use crate::infra::embedding_space::SpaceId;
use crate::infra::embedding_space::token::{
    Accepted, DispatchToken, Rejection, check, current_write_context, record_rejection,
};
use anyhow::Result;

/// A write that passed verification.
#[derive(Debug, Clone)]
pub struct GuardedWrite {
    label: TableLabel,
    entity_id: i64,
    vector_kind: String,
    /// `None` for a token-less (legacy) write or a process without an
    /// embedding space: rows are written without an index entry.
    token: Option<DispatchToken>,
}

/// Verify a result for `(label, entity_id, replace_kinds)`. A token
/// covers exactly one vector kind, so a tokened write must replace one.
pub fn guard(
    label: TableLabel,
    entity_id: i64,
    replace_kinds: &[String],
    token: Option<&DispatchToken>,
    current_version: Option<&SourceVersion>,
    dimension: usize,
    row_dimensions: impl IntoIterator<Item = usize>,
) -> std::result::Result<GuardedWrite, Rejection> {
    let vector_kind = replace_kinds.first().cloned().unwrap_or_default();
    let unguarded = || GuardedWrite {
        label,
        entity_id,
        vector_kind: vector_kind.clone(),
        token: None,
    };
    let Some(ctx) = current_write_context(dimension) else {
        return Ok(unguarded());
    };
    if token.is_some() && replace_kinds.len() != 1 {
        record_rejection(Rejection::SourceVersion);
        return Err(Rejection::SourceVersion);
    }
    match check(&ctx, token, current_version, row_dimensions) {
        Ok(Accepted::Indexed(token)) => Ok(GuardedWrite {
            token: Some(token),
            ..unguarded()
        }),
        Ok(Accepted::Legacy) => Ok(unguarded()),
        Err(r) => {
            record_rejection(r);
            tracing::info!(
                table = label.as_str(),
                entity_id,
                vector_kind,
                reason = r.as_str(),
                "embedding write rejected"
            );
            Err(r)
        }
    }
}

impl GuardedWrite {
    /// Step 1: remove the target's entry (success or failure) so an
    /// interrupted write leaves the target stale, never complete. A
    /// token-less write removes it too: its rows are of unknown version,
    /// so the target must fall back to unverified.
    pub async fn before_rows(&self, index: Option<&EmbeddingIndex>) -> Result<()> {
        if let Some(index) = index
            && !self.vector_kind.is_empty()
        {
            index
                .delete(self.label, self.entity_id, &[self.vector_kind.as_str()])
                .await?;
        }
        Ok(())
    }

    /// Step 3: record the success entry once the rows are in place.
    pub async fn after_rows(
        &self,
        index: Option<&EmbeddingIndex>,
        chunk_count: usize,
    ) -> Result<()> {
        let (Some(index), Some(token)) = (index, &self.token) else {
            return Ok(());
        };
        index
            .put(&[IndexEntry {
                table: self.label,
                entity_id: self.entity_id,
                vector_kind: self.vector_kind.clone(),
                space_id: SpaceId(token.space_id.clone()),
                source_version: SourceVersion(token.source_version.clone()),
                generation_id: token.generation_id.clone(),
                outcome: EntryOutcome::Success {
                    chunk_count: u32::try_from(chunk_count).unwrap_or(u32::MAX),
                },
                media_digest: None,
                recorded_at: command_utils::util::datetime::now_millis(),
            }])
            .await
    }
}

/// Outcome of a failure report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureReport {
    /// A failure entry now describes the target.
    Recorded,
    /// The target already completed for this version; nothing recorded,
    /// so a late failure never discards a valid success.
    AlreadyComplete,
    Rejected(Rejection),
}

/// Transient failures (the source may be reachable later) versus
/// permanent ones (the input itself is refused). Decided from the
/// reported reason and message, since workflows only see error text.
pub fn failure_class(reason: &str, message: &str) -> &'static str {
    const TRANSIENT: [&str; 12] = [
        "timeout",
        "timed out",
        "unavailable",
        "connect",
        "temporar",
        "rate limit",
        "429",
        "502",
        "503",
        "504",
        "dns",
        "reset",
    ];
    let text = format!("{reason} {message}").to_ascii_lowercase();
    if reason == "fetch_failed" || TRANSIENT.iter().any(|t| text.contains(t)) {
        "transient"
    } else {
        "permanent"
    }
}

/// Record a generation failure for `(label, entity_id, vector_kind)`.
/// `chunk_indexes` are the target's current rows; `delete_rows` removes
/// them (old-version rows must not keep answering searches).
#[allow(clippy::too_many_arguments)]
pub async fn record_failure<F, Fut>(
    index: Option<&EmbeddingIndex>,
    label: TableLabel,
    entity_id: i64,
    vector_kind: &str,
    token: &DispatchToken,
    current_version: Option<&SourceVersion>,
    chunk_indexes: &[i32],
    reason: &str,
    message: &str,
    delete_rows: F,
) -> Result<FailureReport>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let Some(ctx) = current_write_context(0) else {
        return Ok(FailureReport::Rejected(Rejection::Space));
    };
    match check(&ctx, Some(token), current_version, std::iter::empty()) {
        Ok(_) => {}
        Err(r) => {
            record_rejection(r);
            return Ok(FailureReport::Rejected(r));
        }
    }
    let Some(index) = index else {
        return Ok(FailureReport::Rejected(Rejection::Space));
    };
    let version = current_version.expect("checked above");
    let existing = index
        .entries_in_range(label, entity_id, entity_id)
        .await?
        .into_iter()
        .find(|e| e.vector_kind == vector_kind);
    match super::classify(&ctx.space, version, existing.as_ref(), chunk_indexes) {
        super::TargetState::Complete => return Ok(FailureReport::AlreadyComplete),
        super::TargetState::Failed
            if existing.as_ref().map(|e| e.generation_id.as_str())
                == Some(token.generation_id.as_str()) =>
        {
            return Ok(FailureReport::Recorded);
        }
        _ => {}
    }
    index.delete(label, entity_id, &[vector_kind]).await?;
    delete_rows().await?;
    index
        .put(&[IndexEntry {
            table: label,
            entity_id,
            vector_kind: vector_kind.to_string(),
            space_id: SpaceId(token.space_id.clone()),
            source_version: SourceVersion(token.source_version.clone()),
            generation_id: token.generation_id.clone(),
            outcome: EntryOutcome::Failure {
                reason: reason.to_string(),
                class: failure_class(reason, message).to_string(),
            },
            media_digest: None,
            recorded_at: command_utils::util::datetime::now_millis(),
        }])
        .await?;
    Ok(FailureReport::Recorded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::embedding_space::{token, workers};
    use serial_test::serial;

    fn tok(version: &str) -> DispatchToken {
        DispatchToken {
            space_id: "ab".repeat(32),
            attempt_id: String::new(),
            source_version: version.into(),
            generation_id: "g1".into(),
        }
    }

    fn with_space<T>(f: impl FnOnce() -> T) -> T {
        workers::set_current_space(Some(SpaceId("ab".repeat(32))));
        token::set_rebuilding_attempt(None);
        token::set_legacy_accept(false);
        let out = f();
        workers::set_current_space(None);
        out
    }

    #[test]
    #[serial]
    fn tokened_write_must_replace_exactly_one_kind() {
        with_space(|| {
            let v = SourceVersion("v".into());
            let kinds = vec!["image".to_string(), "caption".to_string()];
            assert_eq!(
                guard(
                    TableLabel::Memory,
                    1,
                    &kinds,
                    Some(&tok("v")),
                    Some(&v),
                    2,
                    [2]
                )
                .unwrap_err(),
                Rejection::SourceVersion
            );
        });
    }

    #[test]
    #[serial]
    fn without_a_space_writes_pass_unindexed() {
        workers::set_current_space(None);
        let g = guard(TableLabel::Thread, 1, &["text".into()], None, None, 2, [5]).unwrap();
        assert!(g.token.is_none());
    }

    #[tokio::test]
    #[serial]
    async fn indexed_write_replaces_the_entry_with_a_success() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let index = EmbeddingIndex::open(&dir.path().to_string_lossy()).await?;
        index
            .put(&[IndexEntry {
                table: TableLabel::Memory,
                entity_id: 1,
                vector_kind: "text".into(),
                space_id: SpaceId("old".into()),
                source_version: SourceVersion("old".into()),
                generation_id: "g0".into(),
                outcome: EntryOutcome::Failure {
                    reason: "x".into(),
                    class: "transient".into(),
                },
                media_digest: None,
                recorded_at: 0,
            }])
            .await?;
        let v = SourceVersion("v".into());
        let g = with_space(|| {
            guard(
                TableLabel::Memory,
                1,
                &["text".into()],
                Some(&tok("v")),
                Some(&v),
                2,
                [2, 2],
            )
        })
        .unwrap();
        g.before_rows(Some(&index)).await?;
        assert!(
            index
                .entries_in_range(TableLabel::Memory, 1, 1)
                .await?
                .is_empty()
        );
        g.after_rows(Some(&index), 2).await?;
        let entries = index.entries_in_range(TableLabel::Memory, 1, 1).await?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].outcome, EntryOutcome::Success { chunk_count: 2 });
        assert_eq!(entries[0].generation_id, "g1");
        assert_eq!(entries[0].source_version, v);
        Ok(())
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    use crate::infra::embedding_space::{token, workers};
    use serial_test::serial;

    fn tok(version: &str, generation: &str) -> DispatchToken {
        DispatchToken {
            space_id: "ab".repeat(32),
            attempt_id: String::new(),
            source_version: version.into(),
            generation_id: generation.into(),
        }
    }

    async fn setup() -> Result<(EmbeddingIndex, tempfile::TempDir)> {
        workers::set_current_space(Some(SpaceId("ab".repeat(32))));
        token::set_rebuilding_attempt(None);
        let dir = tempfile::tempdir()?;
        Ok((
            EmbeddingIndex::open(&dir.path().to_string_lossy()).await?,
            dir,
        ))
    }

    async fn report(
        index: &EmbeddingIndex,
        t: &DispatchToken,
        current: &str,
        chunks: &[i32],
        deleted: &std::sync::atomic::AtomicUsize,
    ) -> Result<FailureReport> {
        record_failure(
            Some(index),
            TableLabel::Memory,
            1,
            "image",
            t,
            Some(&SourceVersion(current.into())),
            chunks,
            "fetch_failed",
            "connect timed out",
            || async {
                deleted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            },
        )
        .await
    }

    async fn entry(index: &EmbeddingIndex) -> Option<IndexEntry> {
        index
            .entries_in_range(TableLabel::Memory, 1, 1)
            .await
            .unwrap()
            .into_iter()
            .next()
    }

    #[tokio::test]
    #[serial]
    async fn failure_is_recorded_once_and_removes_leftover_rows() -> Result<()> {
        let (index, _dir) = setup().await?;
        let deleted = std::sync::atomic::AtomicUsize::new(0);
        let t = tok("v", "g1");
        assert_eq!(
            report(&index, &t, "v", &[0], &deleted).await?,
            FailureReport::Recorded
        );
        assert_eq!(deleted.load(std::sync::atomic::Ordering::SeqCst), 1);
        let e = entry(&index).await.unwrap();
        assert_eq!(
            e.outcome,
            EntryOutcome::Failure {
                reason: "fetch_failed".into(),
                class: "transient".into()
            }
        );
        // A resend of the same report changes nothing.
        let before = index.version().await?;
        assert_eq!(
            report(&index, &t, "v", &[], &deleted).await?,
            FailureReport::Recorded
        );
        assert_eq!(index.version().await?, before);
        workers::set_current_space(None);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn success_wins_regardless_of_arrival_order() -> Result<()> {
        let (index, _dir) = setup().await?;
        let deleted = std::sync::atomic::AtomicUsize::new(0);
        // Success first, then a late failure for the same version.
        let ok = guard(
            TableLabel::Memory,
            1,
            &["image".into()],
            Some(&tok("v", "g1")),
            Some(&SourceVersion("v".into())),
            2,
            [2],
        )
        .unwrap();
        ok.after_rows(Some(&index), 1).await?;
        assert_eq!(
            report(&index, &tok("v", "g2"), "v", &[0], &deleted).await?,
            FailureReport::AlreadyComplete
        );
        assert_eq!(deleted.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(matches!(
            entry(&index).await.unwrap().outcome,
            EntryOutcome::Success { .. }
        ));
        workers::set_current_space(None);
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn stale_failure_reports_are_not_recorded() -> Result<()> {
        let (index, _dir) = setup().await?;
        let deleted = std::sync::atomic::AtomicUsize::new(0);
        assert_eq!(
            report(&index, &tok("old", "g1"), "v", &[], &deleted).await?,
            FailureReport::Rejected(Rejection::SourceVersion)
        );
        let mut other_space = tok("v", "g1");
        other_space.space_id = "cd".repeat(32);
        assert_eq!(
            report(&index, &other_space, "v", &[], &deleted).await?,
            FailureReport::Rejected(Rejection::Space)
        );
        assert!(entry(&index).await.is_none());
        workers::set_current_space(None);
        Ok(())
    }

    #[test]
    fn failure_classes() {
        assert_eq!(failure_class("fetch_failed", ""), "transient");
        assert_eq!(
            failure_class("workflow_error", "HTTP 503 from origin"),
            "transient"
        );
        assert_eq!(
            failure_class("workflow_error", "image decode error"),
            "permanent"
        );
    }
}
