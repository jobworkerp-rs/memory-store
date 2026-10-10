//! The verdict of `embedding finalize`'s verification over the target
//! counts (spec §3.7 "検証"), kept free of I/O.

use super::counts::{CountKind, Counts};
use super::output::{ErrorCode, Resolution};

/// Why the counts do not allow the commit, or `None` when they do.
/// Re-dispatchable states take precedence over failures; failures pass
/// only when `accept` equals their number.
pub fn refusal(counts: &Counts, accept: Option<u64>) -> Option<(ErrorCode, Resolution)> {
    let total = counts.total();
    let failed = total.failed();
    if total.missing + total.stale + total.unverified + counts.orphan > 0 {
        Some((ErrorCode::RebuildIncomplete, Resolution::ResumeRebuild))
    } else if failed > 0 && accept != Some(failed) {
        Some((ErrorCode::RebuildHasFailures, Resolution::ResolveFailures))
    } else {
        None
    }
}

/// `missing_<kind>` … `failed_<kind>` for every kind, then `orphan`.
pub fn count_fields(counts: &Counts) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for kind in CountKind::ALL {
        let c = counts.kind(kind);
        for (name, n) in [
            ("missing", c.missing),
            ("stale", c.stale),
            ("unverified", c.unverified),
            ("failed", c.failed()),
        ] {
            out.push((format!("{name}_{}", kind.as_str()), n.to_string()));
        }
    }
    out.push(("orphan".into(), counts.orphan.to_string()));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(missing: u64, failed: u64, orphan: u64) -> Counts {
        let mut c = Counts::default();
        let thread = &mut c.by_kind[CountKind::Thread as usize];
        thread.missing = missing;
        thread.failed_permanent = failed;
        c.orphan = orphan;
        c
    }

    #[test]
    fn incomplete_before_failures_and_exact_acceptance() {
        let incomplete = Some((ErrorCode::RebuildIncomplete, Resolution::ResumeRebuild));
        let failures = Some((ErrorCode::RebuildHasFailures, Resolution::ResolveFailures));
        assert_eq!(refusal(&counts(0, 0, 0), None), None);
        assert_eq!(refusal(&counts(1, 0, 0), None), incomplete);
        assert_eq!(refusal(&counts(0, 0, 1), None), incomplete, "orphans");
        assert_eq!(refusal(&counts(1, 2, 0), Some(2)), incomplete);
        assert_eq!(refusal(&counts(0, 2, 0), None), failures);
        assert_eq!(refusal(&counts(0, 2, 0), Some(1)), failures);
        assert_eq!(refusal(&counts(0, 2, 0), Some(2)), None);
        // With no failures the acceptance count is not consulted.
        assert_eq!(refusal(&counts(0, 0, 0), Some(3)), None);
    }

    #[test]
    fn count_fields_cover_every_kind_then_orphan() {
        let fields = count_fields(&counts(1, 2, 3));
        assert_eq!(fields.len(), CountKind::ALL.len() * 4 + 1);
        assert!(fields.contains(&("missing_thread".into(), "1".into())));
        assert!(fields.contains(&("failed_thread".into(), "2".into())));
        assert_eq!(fields.last().unwrap(), &("orphan".into(), "3".into()));
    }
}
