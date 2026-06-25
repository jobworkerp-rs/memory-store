//! `FailureMode` proto enum <-> `reflection_failure_mode.mode` /
//! `failure_mode_dictionary.mode` DB string conversion.
//!
//! The DB stores the short key seeded by
//! `infra/sql/*/003_reflection_schema.sql` (`tool_misuse`, `loop`, ...).
//! `OTHER` is the one entry whose DB form stays uppercase. prost's
//! generated `as_str_name()` returns the fully-qualified
//! `FAILURE_MODE_TOOL_MISUSE`, which does not match the DB key, so the
//! mapping is spelled out explicitly here rather than derived by
//! stripping a prefix: an explicit match makes the `OTHER`-stays-upper
//! exception and any future vocabulary drift a compile-time concern
//! (the `from_db_name` round-trip test pins both directions to the
//! literal seed values).

use protobuf::llm_memory::data::FailureMode;

/// Map an enum to the exact string persisted in
/// `reflection_failure_mode.mode` / `failure_mode_dictionary.mode`.
///
/// `Unspecified` has no DB representation (it is never inserted; callers
/// skip it) and maps to `""` so an accidental write is visibly wrong
/// rather than silently colliding with a real key.
pub fn to_db_name(mode: FailureMode) -> &'static str {
    match mode {
        FailureMode::Unspecified => "",
        FailureMode::ToolMisuse => "tool_misuse",
        FailureMode::Loop => "loop",
        FailureMode::ScopeDrift => "scope_drift",
        FailureMode::Hallucination => "hallucination",
        FailureMode::ContextOverflow => "context_overflow",
        FailureMode::DataLoss => "data_loss",
        FailureMode::PermissionIssue => "permission_issue",
        FailureMode::AmbiguousInstruction => "ambiguous_instruction",
        FailureMode::ConflictingRequirements => "conflicting_requirements",
        FailureMode::MissingContext => "missing_context",
        FailureMode::MisleadingPremise => "misleading_premise",
        FailureMode::GoalDriftByUser => "goal_drift_by_user",
        FailureMode::ToolUnavailable => "tool_unavailable",
        FailureMode::ExternalServiceFailure => "external_service_failure",
        FailureMode::RateLimit => "rate_limit",
        FailureMode::Other => "OTHER",
    }
}

/// Reverse of [`to_db_name`]. `None` for any value not in the controlled
/// vocabulary — callers fold that into `FailureMode::Other` and salvage
/// the raw text into `failure_modes_other` (backward compatibility for
/// rows written before the enum migration that may hold free text).
pub fn from_db_name(s: &str) -> Option<FailureMode> {
    match s {
        "tool_misuse" => Some(FailureMode::ToolMisuse),
        "loop" => Some(FailureMode::Loop),
        "scope_drift" => Some(FailureMode::ScopeDrift),
        "hallucination" => Some(FailureMode::Hallucination),
        "context_overflow" => Some(FailureMode::ContextOverflow),
        "data_loss" => Some(FailureMode::DataLoss),
        "permission_issue" => Some(FailureMode::PermissionIssue),
        "ambiguous_instruction" => Some(FailureMode::AmbiguousInstruction),
        "conflicting_requirements" => Some(FailureMode::ConflictingRequirements),
        "missing_context" => Some(FailureMode::MissingContext),
        "misleading_premise" => Some(FailureMode::MisleadingPremise),
        "goal_drift_by_user" => Some(FailureMode::GoalDriftByUser),
        "tool_unavailable" => Some(FailureMode::ToolUnavailable),
        "external_service_failure" => Some(FailureMode::ExternalServiceFailure),
        "rate_limit" => Some(FailureMode::RateLimit),
        "OTHER" => Some(FailureMode::Other),
        _ => None,
    }
}

/// Resolve a raw i32 (proto wire form of `repeated FailureMode`) to its
/// DB key. Unknown / `Unspecified` discriminants yield `None` so the
/// child-row insert loop can skip them without persisting a bogus mode.
pub fn db_name_from_i32(v: i32) -> Option<&'static str> {
    match FailureMode::try_from(v) {
        Ok(FailureMode::Unspecified) | Err(_) => None,
        Ok(m) => Some(to_db_name(m)),
    }
}

/// Sentinel pushed into the resolved filter when the caller specified
/// a non-empty `failure_modes` list whose every element was Unspecified
/// / out-of-range. The DB `mode` column never carries this value
/// (the controlled vocabulary is the 16-key set seeded by
/// `003_reflection_schema.sql`, none of which use leading
/// double-underscores), so a `WHERE rfm.mode = <sentinel>` clause is
/// guaranteed to match zero rows — preserving the caller's
/// filter-non-empty intent rather than silently degrading to "no filter
/// applied" (= all rows). UTF-8 only / no NUL bytes so postgres VARCHAR
/// bind accepts it.
pub const UNRESOLVABLE_MODE_SENTINEL: &str = "__failure_mode_sentinel_no_match__";

/// Map proto `FailureMode` wire values to the DB keys used in
/// `reflection_failure_mode.mode` query clauses. Unspecified /
/// out-of-range discriminants are dropped (they cannot match any
/// stored row). Used by the search filter resolver for both the
/// ALL and ANY mode lists.
///
/// When `modes` is non-empty but every element is unresolvable, returns
/// `[UNRESOLVABLE_MODE_SENTINEL]` so the downstream SQL clause stays
/// present and matches zero rows. An empty input yields an empty Vec
/// (= "no filter on this axis"), which is the existing semantics.
pub fn i32_slice_to_db_keys(modes: &[i32]) -> Vec<String> {
    if modes.is_empty() {
        return Vec::new();
    }
    let resolved: Vec<String> = modes
        .iter()
        .filter_map(|v| db_name_from_i32(*v).map(str::to_string))
        .collect();
    if resolved.is_empty() {
        return vec![UNRESOLVABLE_MODE_SENTINEL.to_string()];
    }
    resolved
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The 16 controlled-vocabulary entries (excluding UNSPECIFIED) plus
    /// their exact DB seed string. Kept as a literal table so the test
    /// fails if either direction drifts from
    /// `003_reflection_schema.sql`.
    const VOCAB: &[(FailureMode, &str)] = &[
        (FailureMode::ToolMisuse, "tool_misuse"),
        (FailureMode::Loop, "loop"),
        (FailureMode::ScopeDrift, "scope_drift"),
        (FailureMode::Hallucination, "hallucination"),
        (FailureMode::ContextOverflow, "context_overflow"),
        (FailureMode::DataLoss, "data_loss"),
        (FailureMode::PermissionIssue, "permission_issue"),
        (FailureMode::AmbiguousInstruction, "ambiguous_instruction"),
        (
            FailureMode::ConflictingRequirements,
            "conflicting_requirements",
        ),
        (FailureMode::MissingContext, "missing_context"),
        (FailureMode::MisleadingPremise, "misleading_premise"),
        (FailureMode::GoalDriftByUser, "goal_drift_by_user"),
        (FailureMode::ToolUnavailable, "tool_unavailable"),
        (
            FailureMode::ExternalServiceFailure,
            "external_service_failure",
        ),
        (FailureMode::RateLimit, "rate_limit"),
        (FailureMode::Other, "OTHER"),
    ];

    #[test]
    fn round_trips_every_vocabulary_entry() {
        for (mode, db) in VOCAB {
            assert_eq!(to_db_name(*mode), *db, "to_db_name({mode:?})");
            assert_eq!(
                from_db_name(db),
                Some(*mode),
                "from_db_name({db:?}) must round-trip"
            );
            assert_eq!(
                db_name_from_i32(*mode as i32),
                Some(*db),
                "db_name_from_i32({mode:?})"
            );
        }
    }

    #[test]
    fn other_stays_uppercase() {
        // Regression guard for the one entry that breaks the
        // lowercase-prefix-strip shortcut.
        assert_eq!(to_db_name(FailureMode::Other), "OTHER");
        assert_eq!(from_db_name("OTHER"), Some(FailureMode::Other));
        assert_eq!(from_db_name("other"), None);
    }

    #[test]
    fn unknown_and_unspecified_have_no_db_form() {
        assert_eq!(from_db_name("totally_invented_mode"), None);
        assert_eq!(from_db_name(""), None);
        assert_eq!(to_db_name(FailureMode::Unspecified), "");
        assert_eq!(db_name_from_i32(FailureMode::Unspecified as i32), None);
        // Out-of-range discriminant (LLM/wire corruption) is skipped.
        assert_eq!(db_name_from_i32(9999), None);
    }

    #[test]
    fn i32_slice_to_db_keys_preserves_filter_intent() {
        // Empty in -> empty out (= no filter on this axis).
        assert!(i32_slice_to_db_keys(&[]).is_empty());

        // All resolvable -> straight mapping, no sentinel.
        assert_eq!(
            i32_slice_to_db_keys(&[FailureMode::ToolMisuse as i32, FailureMode::Loop as i32]),
            vec!["tool_misuse".to_string(), "loop".to_string()],
        );

        // Mixed: unresolvable elements are dropped but the resolvable
        // ones still drive the filter — no sentinel needed.
        assert_eq!(
            i32_slice_to_db_keys(&[FailureMode::ToolMisuse as i32, 9999]),
            vec!["tool_misuse".to_string()],
        );

        // All unresolvable from a non-empty input -> sentinel keeps the
        // SQL clause present and matches zero rows, so the caller's
        // "filter to these modes" intent is honored instead of silently
        // degrading to "no filter".
        let only_unspec = i32_slice_to_db_keys(&[FailureMode::Unspecified as i32, 9999]);
        assert_eq!(only_unspec.len(), 1);
        assert_eq!(only_unspec[0], UNRESOLVABLE_MODE_SENTINEL);
        // The sentinel cannot collide with any real key (NUL prefix is
        // outside the controlled vocabulary).
        assert!(from_db_name(UNRESOLVABLE_MODE_SENTINEL).is_none());
    }
}
