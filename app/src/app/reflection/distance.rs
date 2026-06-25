//! F-S7 weighted Euclidean distance over `FailureSignatureIndicators`.
//!
//! Spec §4.2.4 fixpoint #25: per-indicator max-scaling normalizes inputs
//! to `[0, 1]`, then a weighted sum of squared diffs is sqrt'd. Missing
//! indicators on either side are skipped (not coerced to 0) so different
//! patterns observed at different fidelities can still be compared. A
//! same-pattern_type bonus halves the final distance.

use std::collections::HashMap;

use protobuf::llm_memory::data::{FailureSignatureIndicators, FailureSignaturePatternType};

/// (max_value, weight) keyed by indicator name. Built once at app
/// startup from `failure_signature_indicator_norm`. A `DashMap` /
/// `HashMap` choice doesn't matter for the read path because the table
/// is immutable for the process lifetime — `HashMap` keeps the type
/// minimal and avoids pulling `dashmap` into the app crate.
pub type NormTable = HashMap<String, (f32, f32)>;

/// Indicator key constants. Mirrored from
/// `infra/sql/sqlite/003_reflection_schema.sql` initial seeds; keep in
/// lockstep with the table `failure_signature_indicator_norm.indicator_name`.
pub const KEY_SAME_TOOL_REPEATED_COUNT: &str = "same_tool_repeated_count";
pub const KEY_CONSECUTIVE_ERRORS: &str = "consecutive_errors";
pub const KEY_NO_STATE_CHANGE_TURNS: &str = "no_state_change_turns";
pub const KEY_TOOL_CALLS_PER_TURN_RATIO: &str = "tool_calls_per_turn_ratio";
pub const KEY_COMPACT_BOUNDARY_COUNT: &str = "compact_boundary_count";
pub const KEY_USER_CLARIFICATION_COUNT: &str = "user_clarification_count";
pub const KEY_TURN_COUNT_AT_DETECTION: &str = "turn_count_at_detection";
pub const KEY_ELAPSED_MS_AT_DETECTION: &str = "elapsed_ms_at_detection";

fn indicator_value(indicators: &FailureSignatureIndicators, key: &str) -> Option<f32> {
    match key {
        KEY_SAME_TOOL_REPEATED_COUNT => indicators.same_tool_repeated_count.map(|v| v as f32),
        KEY_CONSECUTIVE_ERRORS => indicators.consecutive_errors.map(|v| v as f32),
        KEY_NO_STATE_CHANGE_TURNS => indicators.no_state_change_turns.map(|v| v as f32),
        KEY_TOOL_CALLS_PER_TURN_RATIO => indicators.tool_calls_per_turn_ratio,
        KEY_COMPACT_BOUNDARY_COUNT => indicators.compact_boundary_count.map(|v| v as f32),
        KEY_USER_CLARIFICATION_COUNT => indicators.user_clarification_count.map(|v| v as f32),
        KEY_TURN_COUNT_AT_DETECTION => indicators.turn_count_at_detection.map(|v| v as f32),
        KEY_ELAPSED_MS_AT_DETECTION => indicators.elapsed_ms_at_detection.map(|v| v as f32),
        _ => None,
    }
}

/// Spec §4.2.4 distance. `table` carries `(max_value, weight)` per key.
/// Indicators absent on either side are skipped — the spec is explicit
/// that "missing" must not be conflated with "zero" since zero is a
/// legitimate observation.
pub fn distance(
    input: &FailureSignatureIndicators,
    stored: &FailureSignatureIndicators,
    table: &NormTable,
) -> f32 {
    let mut sum = 0.0_f32;
    for (key, (max_v, weight)) in table.iter() {
        if *max_v <= 0.0 {
            // A zero or negative max would cause a div-by-zero or sign
            // flip; skip the entry rather than poisoning the whole sum.
            continue;
        }
        let (Some(a), Some(b)) = (indicator_value(input, key), indicator_value(stored, key)) else {
            continue;
        };
        let nx = (a / *max_v).clamp(0.0, 1.0);
        let ny = (b / *max_v).clamp(0.0, 1.0);
        sum += weight * (nx - ny).powi(2);
    }
    sum.sqrt()
}

/// Apply the same-pattern_type bonus per fixpoint #25 (0.5x distance
/// when both sides report the same pattern type).
pub fn rank_distance(
    base_distance: f32,
    input_pattern: FailureSignaturePatternType,
    stored_pattern: FailureSignaturePatternType,
) -> f32 {
    if input_pattern != FailureSignaturePatternType::FailureSignaturePatternUnspecified
        && input_pattern == stored_pattern
    {
        base_distance * 0.5
    } else {
        base_distance
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_indicators() -> FailureSignatureIndicators {
        FailureSignatureIndicators::default()
    }

    fn full_indicators() -> FailureSignatureIndicators {
        FailureSignatureIndicators {
            same_tool_repeated_count: Some(10),
            same_tool_name: None,
            consecutive_errors: Some(5),
            no_state_change_turns: Some(10),
            tool_calls_per_turn_ratio: Some(5.0),
            compact_boundary_count: Some(5),
            user_clarification_count: Some(5),
            turn_count_at_detection: Some(250),
            elapsed_ms_at_detection: Some(1_800_000),
        }
    }

    fn full_norm_table() -> NormTable {
        let mut t = NormTable::new();
        t.insert(KEY_SAME_TOOL_REPEATED_COUNT.into(), (20.0, 1.0));
        t.insert(KEY_CONSECUTIVE_ERRORS.into(), (10.0, 1.0));
        t.insert(KEY_NO_STATE_CHANGE_TURNS.into(), (20.0, 1.0));
        t.insert(KEY_TOOL_CALLS_PER_TURN_RATIO.into(), (10.0, 1.0));
        t.insert(KEY_COMPACT_BOUNDARY_COUNT.into(), (10.0, 1.0));
        t.insert(KEY_USER_CLARIFICATION_COUNT.into(), (10.0, 1.0));
        t.insert(KEY_TURN_COUNT_AT_DETECTION.into(), (500.0, 1.0));
        t.insert(KEY_ELAPSED_MS_AT_DETECTION.into(), (3_600_000.0, 1.0));
        t
    }

    #[test]
    fn distance_to_self_is_zero() {
        let table = full_norm_table();
        let v = full_indicators();
        let d = distance(&v, &v, &table);
        assert!(d.abs() < 1e-6, "expected 0, got {d}");
    }

    #[test]
    fn distance_with_all_missing_is_zero() {
        let table = full_norm_table();
        let a = empty_indicators();
        let b = full_indicators();
        // No indicator present on `a` → every comparison is skipped.
        assert!(distance(&a, &b, &table).abs() < 1e-6);
    }

    #[test]
    fn distance_max_difference_per_axis_sums_correctly() {
        // Compare zero vector vs max-value vector along every axis.
        let table = full_norm_table();
        let zero = FailureSignatureIndicators {
            same_tool_repeated_count: Some(0),
            same_tool_name: None,
            consecutive_errors: Some(0),
            no_state_change_turns: Some(0),
            tool_calls_per_turn_ratio: Some(0.0),
            compact_boundary_count: Some(0),
            user_clarification_count: Some(0),
            turn_count_at_detection: Some(0),
            elapsed_ms_at_detection: Some(0),
        };
        let max = FailureSignatureIndicators {
            same_tool_repeated_count: Some(20),
            same_tool_name: None,
            consecutive_errors: Some(10),
            no_state_change_turns: Some(20),
            tool_calls_per_turn_ratio: Some(10.0),
            compact_boundary_count: Some(10),
            user_clarification_count: Some(10),
            turn_count_at_detection: Some(500),
            elapsed_ms_at_detection: Some(3_600_000),
        };
        // 8 indicators each contributing (1.0)^2 with weight 1.0:
        // sum = 8, sqrt = 2.828...
        let d = distance(&zero, &max, &table);
        let expected = 8.0_f32.sqrt();
        assert!((d - expected).abs() < 1e-5, "expected {expected}, got {d}",);
    }

    #[test]
    fn rank_distance_applies_pattern_bonus() {
        use FailureSignaturePatternType as P;
        let d = 1.0_f32;
        let same = rank_distance(
            d,
            P::FailureSignaturePatternToolLoop,
            P::FailureSignaturePatternToolLoop,
        );
        assert!((same - 0.5).abs() < 1e-6);
        let diff = rank_distance(
            d,
            P::FailureSignaturePatternToolLoop,
            P::FailureSignaturePatternNoProgress,
        );
        assert!((diff - 1.0).abs() < 1e-6);
        // Unspecified on the input side never triggers the bonus.
        let unspec = rank_distance(
            d,
            P::FailureSignaturePatternUnspecified,
            P::FailureSignaturePatternUnspecified,
        );
        assert!((unspec - 1.0).abs() < 1e-6);
    }

    #[test]
    fn distance_skips_indicator_with_zero_max() {
        // A misconfigured norm row with max=0 must be ignored, not
        // explode with NaN.
        let mut table = NormTable::new();
        table.insert(KEY_CONSECUTIVE_ERRORS.into(), (0.0, 1.0));
        let a = FailureSignatureIndicators {
            consecutive_errors: Some(5),
            ..Default::default()
        };
        let b = FailureSignatureIndicators {
            consecutive_errors: Some(0),
            ..Default::default()
        };
        let d = distance(&a, &b, &table);
        assert!(d.is_finite());
        assert!(d.abs() < 1e-6);
    }
}
