//! Embedding index: per embedding target, which space, source version,
//! and generation produced its vector rows (success entry), or why the
//! generation failed (failure entry). Together with the RDB it classifies
//! every target as complete, failed, stale, unverified, or missing.

pub mod classify;
pub mod counts;
pub mod scan;
pub mod source_version;
pub mod table;
pub mod write;

pub use classify::{TargetState, classify};
pub use source_version::SourceVersion;
pub use table::{EmbeddingIndex, EntryOutcome, IndexEntry};

/// Kinds that progress and counts are broken down by: memory text (text
/// and caption rows), memory media (image rows), thread, and reflection
/// intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReportKind {
    MemoryText,
    MemoryMedia,
    Thread,
    ReflectionIntent,
}

impl ReportKind {
    pub const ALL: [ReportKind; 4] = [
        Self::MemoryText,
        Self::MemoryMedia,
        Self::Thread,
        Self::ReflectionIntent,
    ];

    pub fn of(table: TableLabel, vector_kind: &str) -> Self {
        match (table, vector_kind) {
            (TableLabel::Memory, "image") => Self::MemoryMedia,
            (TableLabel::Memory, _) => Self::MemoryText,
            (TableLabel::Thread, _) => Self::Thread,
            (TableLabel::ReflectionIntent, _) => Self::ReflectionIntent,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::MemoryText => "memory_text",
            Self::MemoryMedia => "memory_media",
            Self::Thread => "thread",
            Self::ReflectionIntent => "reflection_intent",
        }
    }
}

/// Vector table a target belongs to. Index entries carry it because
/// several vector tables may share one LanceDB directory (and index).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableLabel {
    Memory,
    Thread,
    ReflectionIntent,
}

impl TableLabel {
    pub const ALL: [TableLabel; 3] = [Self::Memory, Self::Thread, Self::ReflectionIntent];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Thread => "thread",
            Self::ReflectionIntent => "reflection_intent",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|l| l.as_str() == value)
    }

    /// Entity id column of the vector table.
    pub fn id_column(self) -> &'static str {
        match self {
            Self::Memory | Self::ReflectionIntent => "memory_id",
            Self::Thread => "thread_id",
        }
    }
}
