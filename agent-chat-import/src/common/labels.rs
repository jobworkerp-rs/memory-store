//! Label truncate helpers for `thread_label.label` (VARCHAR(512)).
//!
//! Spec §5.3.6: labels are written by users for search and must remain
//! human-readable. `path:` / `dir:` keep the **leaf** (so the trailing
//! basename survives), other system labels (`tag:`, `vault:`,
//! `branch:`, `provider:`, `<key>:` from frontmatter) keep the **head**
//! (since identifiers are meaningful from the start).
//!
//! The helpers intentionally accept the prefix as `<name>:` (colon
//! included) so the caller never has to re-add the separator and so the
//! per-prefix budget computation is symmetric.

/// Maximum byte length of `thread_label.label`.
pub const MAX_LABEL_BYTES: usize = 512;

/// Marker placed at the **leading** edge of a truncated value to signal
/// that the head was clipped. Three-byte UTF-8 codepoint `…` (U+2026)
/// plus a forward slash so the result still parses as a path component.
const HEAD_CLIPPED_MARKER: &str = "…/";

/// Marker placed at the **trailing** edge of a truncated value to
/// signal that the tail was clipped. Single ellipsis (`…`, 3 bytes
/// UTF-8).
const TAIL_CLIPPED_MARKER: &str = "…";

/// Truncate a label of the form `<prefix><value>` so it fits within
/// `MAX_LABEL_BYTES`, keeping the **tail** of `value` (path-like
/// labels). Empty string is returned when `prefix` alone already
/// exceeds the budget — the caller should drop the label in that case
/// (spec §5.3.6: "no truncate possible -> skip").
///
/// `prefix` MUST already include the trailing colon
/// (e.g. `"path:"`, `"dir:"`).
pub fn truncate_label_keep_tail(prefix: &str, value: &str) -> String {
    truncate_label(prefix, value, TruncateMode::KeepTail)
}

/// Truncate a label of the form `<prefix><value>` keeping the **head**
/// of `value` (identifier-like labels). See `truncate_label_keep_tail`.
pub fn truncate_label_keep_head(prefix: &str, value: &str) -> String {
    truncate_label(prefix, value, TruncateMode::KeepHead)
}

#[derive(Copy, Clone)]
enum TruncateMode {
    KeepTail,
    KeepHead,
}

fn truncate_label(prefix: &str, value: &str, mode: TruncateMode) -> String {
    let total = prefix.len() + value.len();
    if total <= MAX_LABEL_BYTES {
        let mut s = String::with_capacity(total);
        s.push_str(prefix);
        s.push_str(value);
        return s;
    }

    let marker = match mode {
        TruncateMode::KeepTail => HEAD_CLIPPED_MARKER,
        TruncateMode::KeepHead => TAIL_CLIPPED_MARKER,
    };

    // Budget for the value content alone, after subtracting prefix and marker.
    let overhead = prefix.len().saturating_add(marker.len());
    if overhead >= MAX_LABEL_BYTES {
        // No room for any value content — caller should skip the label
        // entirely. Return empty so callers can detect this and drop.
        return String::new();
    }
    let value_budget = MAX_LABEL_BYTES - overhead;

    let kept = match mode {
        TruncateMode::KeepTail => keep_tail_bytes(value, value_budget),
        TruncateMode::KeepHead => keep_head_bytes(value, value_budget),
    };

    let mut s = String::with_capacity(prefix.len() + marker.len() + kept.len());
    s.push_str(prefix);
    match mode {
        TruncateMode::KeepTail => {
            s.push_str(marker);
            s.push_str(kept);
        }
        TruncateMode::KeepHead => {
            s.push_str(kept);
            s.push_str(marker);
        }
    }
    s
}

/// Return the longest valid UTF-8 suffix of `s` that fits in `budget`
/// bytes, aligned to a UTF-8 boundary.
fn keep_tail_bytes(s: &str, budget: usize) -> &str {
    if budget == 0 || s.is_empty() {
        return "";
    }
    let len = s.len();
    if len <= budget {
        return s;
    }
    // Walk forward from the byte at `len - budget` to find the next
    // UTF-8 char boundary; this is the smallest start that keeps the
    // suffix at most `budget` bytes long.
    let mut start = len - budget;
    while start < len && !s.is_char_boundary(start) {
        start += 1;
    }
    &s[start..]
}

/// Return the longest valid UTF-8 prefix of `s` that fits in `budget`
/// bytes, aligned to a UTF-8 boundary.
fn keep_head_bytes(s: &str, budget: usize) -> &str {
    if budget == 0 || s.is_empty() {
        return "";
    }
    crate::common::canonical::safe_truncate_at_char_boundary(s, budget)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_when_under_limit() {
        let s = truncate_label_keep_tail("path:", "/short/path");
        assert_eq!(s, "path:/short/path");
    }

    #[test]
    fn keep_tail_truncates_head_with_marker() {
        let value = "a".repeat(MAX_LABEL_BYTES);
        let out = truncate_label_keep_tail("dir:", &value);
        assert_eq!(out.len(), MAX_LABEL_BYTES);
        assert!(out.starts_with("dir:…/"));
        // The tail of the value (a's) survives.
        let suffix = &out["dir:…/".len()..];
        assert!(suffix.bytes().all(|b| b == b'a'));
    }

    #[test]
    fn keep_head_truncates_tail_with_marker() {
        let value = "z".repeat(MAX_LABEL_BYTES);
        let out = truncate_label_keep_head("tag:", &value);
        assert_eq!(out.len(), MAX_LABEL_BYTES);
        assert!(out.starts_with("tag:"));
        assert!(out.ends_with('…'));
        // The head of the value (z's) survives.
        let core = &out["tag:".len()..out.len() - "…".len()];
        assert!(core.bytes().all(|b| b == b'z'));
    }

    #[test]
    fn marker_makes_truncation_visible() {
        let value = "a".repeat(600);
        let kept_tail = truncate_label_keep_tail("path:", &value);
        assert!(kept_tail.contains('…'));
        let kept_head = truncate_label_keep_head("vault:", &value);
        assert!(kept_head.contains('…'));
    }

    #[test]
    fn empty_when_prefix_alone_exceeds_budget() {
        let huge_prefix = format!("{}:", "x".repeat(MAX_LABEL_BYTES));
        let out = truncate_label_keep_head(&huge_prefix, "value");
        assert!(out.is_empty(), "expected skip-signaling empty string");
    }

    #[test]
    fn keep_tail_aligns_to_utf8_boundary() {
        // Each ✓ is 3 bytes. budget will likely cut mid-codepoint;
        // helper must skip forward to a boundary.
        let value = "✓".repeat(300); // 900 bytes
        let out = truncate_label_keep_tail("dir:", &value);
        // Must round-trip as valid UTF-8.
        assert!(out.is_char_boundary(out.len()));
        // Body after marker is composed of complete ✓ codepoints.
        let body = out.trim_start_matches("dir:…/");
        for ch in body.chars() {
            assert_eq!(ch, '✓');
        }
    }

    #[test]
    fn keep_head_aligns_to_utf8_boundary() {
        let value = "✓".repeat(300);
        let out = truncate_label_keep_head("tag:", &value);
        assert!(out.is_char_boundary(out.len()));
        let body = out.trim_start_matches("tag:").trim_end_matches('…');
        for ch in body.chars() {
            assert_eq!(ch, '✓');
        }
    }

    #[test]
    fn boundary_512_passes_through_untouched() {
        let value = "a".repeat(MAX_LABEL_BYTES - "path:".len());
        let out = truncate_label_keep_tail("path:", &value);
        assert_eq!(out.len(), MAX_LABEL_BYTES);
        assert!(!out.contains('…'));
    }
}
