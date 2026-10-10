//! Dispatch tokens and the write-time check of embedding results.
//!
//! Every generation request carries the space, rebuild attempt, source
//! version, and generation ID it was issued for. A result is written only
//! when they still match, so results of a previous space, a previous
//! attempt, or a superseded input never land in the vector tables.

use super::SpaceId;
use crate::infra::embedding_index::SourceVersion;
use serde::{Deserialize, Serialize};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

static REBUILDING_ATTEMPT: RwLock<Option<String>> = RwLock::new(None);
static LEGACY_ACCEPT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Record whether every vector table accepts token-less writes.
pub fn set_legacy_accept(accept: bool) {
    LEGACY_ACCEPT.store(accept, Ordering::SeqCst);
}

/// The write context of this process, or `None` when it serves no
/// embedding space (no vector store configured).
pub fn current_write_context(dimension: usize) -> Option<WriteContext> {
    Some(WriteContext {
        space: super::workers::current_space()?,
        dimension,
        rebuilding_attempt: rebuilding_attempt(),
        legacy_accept: LEGACY_ACCEPT.load(Ordering::SeqCst),
    })
}

/// Record the rebuild attempt this process serves (rule 4 at startup).
pub fn set_rebuilding_attempt(attempt: Option<String>) {
    *REBUILDING_ATTEMPT
        .write()
        .unwrap_or_else(|e| e.into_inner()) = attempt;
}

pub fn rebuilding_attempt() -> Option<String> {
    REBUILDING_ATTEMPT
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchToken {
    pub space_id: String,
    pub attempt_id: String,
    pub source_version: String,
    pub generation_id: String,
}

impl DispatchToken {
    /// A token for a request generated from `version` in the current
    /// space, or `None` when this process serves no embedding space.
    pub fn issue(version: &SourceVersion) -> Option<Self> {
        let space = super::workers::current_space()?;
        Some(Self {
            space_id: space.to_string(),
            attempt_id: rebuilding_attempt().unwrap_or_default(),
            source_version: version.to_string(),
            generation_id: super::storage::new_identifier(),
        })
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("token serializes")
    }
}

impl From<&protobuf::llm_memory::data::DispatchToken> for DispatchToken {
    fn from(t: &protobuf::llm_memory::data::DispatchToken) -> Self {
        Self {
            space_id: t.space_id.clone(),
            attempt_id: t.attempt_id.clone(),
            source_version: t.source_version.clone(),
            generation_id: t.generation_id.clone(),
        }
    }
}

/// Why a write was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rejection {
    Space,
    Attempt,
    SourceVersion,
    Dimension,
    /// No token, and token-less writes are not accepted.
    MissingToken,
}

impl Rejection {
    pub const ALL: [Rejection; 5] = [
        Self::Space,
        Self::Attempt,
        Self::SourceVersion,
        Self::Dimension,
        Self::MissingToken,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Space => "space_mismatch",
            Self::Attempt => "attempt_mismatch",
            Self::SourceVersion => "source_version_mismatch",
            Self::Dimension => "dimension_mismatch",
            Self::MissingToken => "missing_token",
        }
    }
}

/// What the server knows when a result arrives.
#[derive(Debug, Clone)]
pub struct WriteContext {
    pub space: SpaceId,
    pub dimension: usize,
    pub rebuilding_attempt: Option<String>,
    /// Every vector table's record allows token-less writes.
    pub legacy_accept: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Accepted {
    /// Write the rows and record a success entry for this token.
    Indexed(DispatchToken),
    /// Write the rows without an index entry (they stay unverified).
    Legacy,
}

/// Decide whether a result may be written. `current_version` is the
/// source version computed from the RDB now (`None` when the entity is
/// gone or no longer an embedding target); `dimensions` are those of the
/// result's vectors.
pub fn check(
    ctx: &WriteContext,
    token: Option<&DispatchToken>,
    current_version: Option<&SourceVersion>,
    dimensions: impl IntoIterator<Item = usize>,
) -> Result<Accepted, Rejection> {
    let dims_ok = dimensions.into_iter().all(|d| d == ctx.dimension);
    let Some(token) = token else {
        return if ctx.legacy_accept && ctx.rebuilding_attempt.is_none() && dims_ok {
            Ok(Accepted::Legacy)
        } else if !dims_ok && ctx.legacy_accept && ctx.rebuilding_attempt.is_none() {
            Err(Rejection::Dimension)
        } else {
            Err(Rejection::MissingToken)
        };
    };
    if token.space_id != ctx.space.as_str() {
        return Err(Rejection::Space);
    }
    if token.attempt_id != ctx.rebuilding_attempt.clone().unwrap_or_default() {
        return Err(Rejection::Attempt);
    }
    if current_version.map(SourceVersion::as_str) != Some(token.source_version.as_str()) {
        return Err(Rejection::SourceVersion);
    }
    if !dims_ok {
        return Err(Rejection::Dimension);
    }
    Ok(Accepted::Indexed(token.clone()))
}

static REJECTIONS: [AtomicU64; 5] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

pub fn record_rejection(r: Rejection) {
    let i = Rejection::ALL.iter().position(|x| *x == r).expect("listed");
    REJECTIONS[i].fetch_add(1, Ordering::Relaxed);
}

/// Rejected writes since startup, per reason.
pub fn rejection_counts() -> Vec<(&'static str, u64)> {
    Rejection::ALL
        .iter()
        .zip(REJECTIONS.iter())
        .map(|(r, c)| (r.as_str(), c.load(Ordering::Relaxed)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> WriteContext {
        WriteContext {
            space: SpaceId("s".into()),
            dimension: 2,
            rebuilding_attempt: None,
            legacy_accept: false,
        }
    }

    fn token(space: &str, attempt: &str, version: &str) -> DispatchToken {
        DispatchToken {
            space_id: space.into(),
            attempt_id: attempt.into(),
            source_version: version.into(),
            generation_id: "g".into(),
        }
    }

    fn v(s: &str) -> SourceVersion {
        SourceVersion(s.into())
    }

    #[test]
    fn token_checks_in_order() {
        let c = ctx();
        let ok = token("s", "", "v");
        assert_eq!(
            check(&c, Some(&ok), Some(&v("v")), [2, 2]),
            Ok(Accepted::Indexed(ok.clone()))
        );
        assert_eq!(
            check(&c, Some(&token("x", "a", "w")), Some(&v("v")), [3]),
            Err(Rejection::Space)
        );
        assert_eq!(
            check(&c, Some(&token("s", "a", "w")), Some(&v("v")), [3]),
            Err(Rejection::Attempt)
        );
        assert_eq!(
            check(&c, Some(&token("s", "", "w")), Some(&v("v")), [3]),
            Err(Rejection::SourceVersion)
        );
        assert_eq!(
            check(&c, Some(&ok), None, [2]),
            Err(Rejection::SourceVersion),
            "deleted entity"
        );
        assert_eq!(
            check(&c, Some(&ok), Some(&v("v")), [2, 3]),
            Err(Rejection::Dimension)
        );
    }

    #[test]
    fn rebuild_requires_the_current_attempt() {
        let c = WriteContext {
            rebuilding_attempt: Some("a1".into()),
            ..ctx()
        };
        assert!(check(&c, Some(&token("s", "a1", "v")), Some(&v("v")), [2]).is_ok());
        assert_eq!(
            check(&c, Some(&token("s", "", "v")), Some(&v("v")), [2]),
            Err(Rejection::Attempt)
        );
        assert_eq!(
            check(&c, Some(&token("s", "a0", "v")), Some(&v("v")), [2]),
            Err(Rejection::Attempt)
        );
    }

    #[test]
    fn token_less_writes_need_legacy_acceptance_outside_rebuilds() {
        assert_eq!(
            check(&ctx(), None, Some(&v("v")), [2]),
            Err(Rejection::MissingToken)
        );
        let legacy = WriteContext {
            legacy_accept: true,
            ..ctx()
        };
        assert_eq!(check(&legacy, None, None, [2]), Ok(Accepted::Legacy));
        assert_eq!(check(&legacy, None, None, [3]), Err(Rejection::Dimension));
        let rebuilding = WriteContext {
            rebuilding_attempt: Some("a".into()),
            ..legacy
        };
        assert_eq!(
            check(&rebuilding, None, None, [2]),
            Err(Rejection::MissingToken)
        );
    }

    #[test]
    fn rejections_are_counted_per_reason() {
        let before: u64 = rejection_counts()
            .iter()
            .find(|(n, _)| *n == "attempt_mismatch")
            .unwrap()
            .1;
        record_rejection(Rejection::Attempt);
        let after = rejection_counts()
            .iter()
            .find(|(n, _)| *n == "attempt_mismatch")
            .unwrap()
            .1;
        assert_eq!(after, before + 1);
    }
}
