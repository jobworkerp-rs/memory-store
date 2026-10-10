//! Aggregation of classified targets into the counts the commands print.

use super::scan::ScanItem;
use super::{EntryOutcome, TargetState};
use std::collections::BTreeSet;

pub use super::ReportKind as CountKind;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StateCounts {
    pub required: u64,
    pub complete: u64,
    pub missing: u64,
    pub stale: u64,
    pub unverified: u64,
    pub failed_transient: u64,
    pub failed_permanent: u64,
}

impl StateCounts {
    pub fn failed(&self) -> u64 {
        self.failed_transient + self.failed_permanent
    }

    fn add(&mut self, other: &StateCounts) {
        self.required += other.required;
        self.complete += other.complete;
        self.missing += other.missing;
        self.stale += other.stale;
        self.unverified += other.unverified;
        self.failed_transient += other.failed_transient;
        self.failed_permanent += other.failed_permanent;
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counts {
    pub by_kind: [StateCounts; 4],
    /// Distinct `(table, entity, kind)` keys with rows or entries but no
    /// target.
    pub orphan: u64,
    /// Image memories whose caption has to be generated.
    pub caption_missing: u64,
}

impl Counts {
    /// Classify every target and orphan of `tables` without changing
    /// anything.
    pub async fn scan(
        pool: &'static infra_utils::infra::rdb::RdbPool,
        media: &crate::infra::media_object::rdb::MediaObjectRepositoryImpl,
        config: super::scan::ScanConfig,
        tables: Vec<super::scan::ScanTable>,
    ) -> anyhow::Result<Self> {
        let mut scanner = super::scan::Scanner::new(pool, media, config, tables);
        let mut counts = Self::default();
        while let Some(page) = scanner.next_page().await? {
            counts.add_page(&page);
        }
        Ok(counts)
    }

    pub fn kind(&self, kind: CountKind) -> &StateCounts {
        &self.by_kind[kind as usize]
    }

    pub fn total(&self) -> StateCounts {
        let mut t = StateCounts::default();
        for k in &self.by_kind {
            t.add(k);
        }
        t
    }

    pub fn add_page(&mut self, items: &[ScanItem]) {
        let mut orphans = BTreeSet::new();
        for item in items {
            match item {
                ScanItem::Target {
                    table,
                    vector_kind,
                    state,
                    entry,
                    ..
                } => {
                    let c = &mut self.by_kind[CountKind::of(*table, vector_kind) as usize];
                    c.required += 1;
                    match state {
                        TargetState::Complete => c.complete += 1,
                        TargetState::Missing => c.missing += 1,
                        TargetState::Stale => c.stale += 1,
                        TargetState::Unverified => c.unverified += 1,
                        TargetState::Failed => {
                            let transient = matches!(
                                entry.as_deref().map(|e| &e.outcome),
                                Some(EntryOutcome::Failure { class, .. }) if class == "transient"
                            );
                            if transient {
                                c.failed_transient += 1;
                            } else {
                                c.failed_permanent += 1;
                            }
                        }
                    }
                }
                ScanItem::Orphan {
                    table,
                    entity_id,
                    vector_kind,
                    ..
                } => {
                    orphans.insert((*table, *entity_id, vector_kind.clone()));
                }
                ScanItem::CaptionMissing { .. } => self.caption_missing += 1,
            }
        }
        self.orphan += orphans.len() as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::embedding_index::{IndexEntry, SourceVersion, TableLabel};
    use crate::infra::embedding_space::SpaceId;

    fn target(
        table: TableLabel,
        kind: &'static str,
        state: TargetState,
        class: Option<&str>,
    ) -> ScanItem {
        ScanItem::Target {
            table,
            entity_id: 1,
            vector_kind: kind,
            state,
            version: SourceVersion("v".into()),
            entry: class.map(|c| {
                Box::new(IndexEntry {
                    table,
                    entity_id: 1,
                    vector_kind: kind.into(),
                    space_id: SpaceId("s".into()),
                    source_version: SourceVersion("v".into()),
                    generation_id: "g".into(),
                    outcome: EntryOutcome::Failure {
                        reason: "r".into(),
                        class: c.into(),
                    },
                    media_digest: None,
                    recorded_at: 0,
                })
            }),
        }
    }

    #[test]
    fn counts_by_kind_state_and_failure_class() {
        let mut c = Counts::default();
        c.add_page(&[
            target(TableLabel::Memory, "text", TargetState::Complete, None),
            target(TableLabel::Memory, "caption", TargetState::Missing, None),
            target(
                TableLabel::Memory,
                "image",
                TargetState::Failed,
                Some("transient"),
            ),
            target(
                TableLabel::Thread,
                "text",
                TargetState::Failed,
                Some("permanent"),
            ),
            target(
                TableLabel::ReflectionIntent,
                "text",
                TargetState::Unverified,
                None,
            ),
            ScanItem::Orphan {
                table: TableLabel::Memory,
                entity_id: 9,
                vector_kind: "text".into(),
                has_rows: true,
                has_entry: true,
            },
            ScanItem::CaptionMissing {
                entity_id: 3,
                media_object_id: 4,
            },
        ]);
        assert_eq!(c.kind(CountKind::MemoryText).required, 2);
        assert_eq!(c.kind(CountKind::MemoryMedia).failed_transient, 1);
        assert_eq!(c.kind(CountKind::Thread).failed_permanent, 1);
        let t = c.total();
        assert_eq!(
            (t.required, t.complete, t.missing, t.unverified, t.failed()),
            (5, 1, 1, 1, 2)
        );
        assert_eq!((c.orphan, c.caption_missing), (1, 1));
    }
}
