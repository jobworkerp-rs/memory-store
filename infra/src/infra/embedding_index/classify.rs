//! Classification of one embedding target (spec §3.1 "embedding 対象の
//! 状態"). Rules are evaluated in order; every target lands in exactly
//! one state.

use super::source_version::SourceVersion;
use super::table::{EntryOutcome, IndexEntry};
use crate::infra::embedding_space::SpaceId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TargetState {
    Complete,
    Failed,
    Stale,
    Unverified,
    Missing,
}

impl TargetState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Stale => "stale",
            Self::Unverified => "unverified",
            Self::Missing => "missing",
        }
    }

    /// States a reconciliation re-dispatches (failures only on request).
    pub fn needs_dispatch(self, retry_failed: bool) -> bool {
        match self {
            Self::Complete => false,
            Self::Failed => retry_failed,
            Self::Stale | Self::Unverified | Self::Missing => true,
        }
    }
}

/// Classify a target from its index entry and the chunk indexes of its
/// vector rows.
pub fn classify(
    current_space: &SpaceId,
    current_version: &SourceVersion,
    entry: Option<&IndexEntry>,
    chunk_indexes: &[i32],
) -> TargetState {
    let Some(entry) = entry else {
        return if chunk_indexes.is_empty() {
            TargetState::Missing
        } else {
            TargetState::Unverified
        };
    };
    let current = &entry.space_id == current_space && &entry.source_version == current_version;
    match &entry.outcome {
        EntryOutcome::Success { chunk_count }
            if current && chunks_exactly(chunk_indexes, *chunk_count) =>
        {
            TargetState::Complete
        }
        EntryOutcome::Failure { .. } if current && chunk_indexes.is_empty() => TargetState::Failed,
        _ => TargetState::Stale,
    }
}

/// The chunk index set is exactly `0..count`, with no duplicates.
fn chunks_exactly(chunk_indexes: &[i32], count: u32) -> bool {
    if chunk_indexes.len() != count as usize || count == 0 {
        return false;
    }
    let mut sorted = chunk_indexes.to_vec();
    sorted.sort_unstable();
    sorted
        .iter()
        .enumerate()
        .all(|(i, c)| i64::from(*c) == i as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::embedding_index::TableLabel;

    fn space(s: &str) -> SpaceId {
        SpaceId(s.into())
    }

    fn version(s: &str) -> SourceVersion {
        SourceVersion(s.into())
    }

    fn entry(space_id: &str, v: &str, outcome: EntryOutcome) -> IndexEntry {
        IndexEntry {
            table: TableLabel::Memory,
            entity_id: 1,
            vector_kind: "text".into(),
            space_id: space(space_id),
            source_version: version(v),
            generation_id: "g".into(),
            outcome,
            media_digest: None,
            recorded_at: 0,
        }
    }

    fn ok(n: u32) -> EntryOutcome {
        EntryOutcome::Success { chunk_count: n }
    }

    fn fail() -> EntryOutcome {
        EntryOutcome::Failure {
            reason: "fetch".into(),
            class: "transient".into(),
        }
    }

    #[test]
    fn rule_table() {
        use TargetState::*;
        let s = space("s");
        let v = version("v");
        let cases: Vec<(&str, Option<IndexEntry>, Vec<i32>, TargetState)> = vec![
            (
                "complete",
                Some(entry("s", "v", ok(2))),
                vec![1, 0],
                Complete,
            ),
            (
                "chunk missing",
                Some(entry("s", "v", ok(2))),
                vec![0],
                Stale,
            ),
            (
                "chunk duplicated",
                Some(entry("s", "v", ok(2))),
                vec![0, 0],
                Stale,
            ),
            (
                "chunk out of range",
                Some(entry("s", "v", ok(2))),
                vec![0, 2],
                Stale,
            ),
            (
                "extra chunk",
                Some(entry("s", "v", ok(2))),
                vec![0, 1, 2],
                Stale,
            ),
            (
                "zero chunks recorded",
                Some(entry("s", "v", ok(0))),
                vec![],
                Stale,
            ),
            (
                "old version",
                Some(entry("s", "old", ok(1))),
                vec![0],
                Stale,
            ),
            ("old space", Some(entry("old", "v", ok(1))), vec![0], Stale),
            ("failed", Some(entry("s", "v", fail())), vec![], Failed),
            (
                "failure with rows",
                Some(entry("s", "v", fail())),
                vec![0],
                Stale,
            ),
            (
                "failure of old version",
                Some(entry("s", "old", fail())),
                vec![],
                Stale,
            ),
            (
                "failure of old space",
                Some(entry("old", "v", fail())),
                vec![],
                Stale,
            ),
            ("unverified", None, vec![0, 1], Unverified),
            ("missing", None, vec![], Missing),
        ];
        for (name, e, chunks, expected) in cases {
            assert_eq!(classify(&s, &v, e.as_ref(), &chunks), expected, "{name}");
        }
    }

    #[test]
    fn dispatch_policy() {
        assert!(!TargetState::Complete.needs_dispatch(true));
        assert!(!TargetState::Failed.needs_dispatch(false));
        assert!(TargetState::Failed.needs_dispatch(true));
        for s in [
            TargetState::Stale,
            TargetState::Unverified,
            TargetState::Missing,
        ] {
            assert!(s.needs_dispatch(false));
        }
    }
}
