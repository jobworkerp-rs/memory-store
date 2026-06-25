//! Path-prefix stripping shared between sources.
//!
//! Used by claude-code (`cwd`), codex (`session_meta.payload.cwd`),
//! and plain (`--root`) to relativize the value put behind a `path:`
//! label. Spec §5.3.6 / §3.3-§3.5.

/// Strip the longest matching base prefix from `value` on a
/// path-component boundary. Returns the original `value` when no
/// prefix matches or when stripping would yield an empty string (so
/// the label never loses information).
pub fn apply_path_prefix<'a>(value: &'a str, prefixes: &[String]) -> &'a str {
    let mut best: Option<&str> = None;
    for raw in prefixes {
        let p = raw.trim_end_matches('/');
        if p.is_empty() {
            continue;
        }
        let matches = if value == p {
            true
        } else {
            value.starts_with(p) && value.as_bytes().get(p.len()) == Some(&b'/')
        };
        if matches && best.is_none_or(|b: &str| p.len() > b.len()) {
            best = Some(p);
        }
    }
    match best {
        Some(p) => {
            let rest = value[p.len()..].trim_start_matches('/');
            if rest.is_empty() { value } else { rest }
        }
        None => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_simple() {
        assert_eq!(
            apply_path_prefix("/home/user/proj", &["/home/user".to_string()]),
            "proj"
        );
    }

    #[test]
    fn longest_wins() {
        let prefixes = vec!["/home".to_string(), "/home/user".to_string()];
        assert_eq!(apply_path_prefix("/home/user/proj", &prefixes), "proj");
    }

    #[test]
    fn boundary_only() {
        // "/home/user2" should NOT match prefix "/home/user".
        assert_eq!(
            apply_path_prefix("/home/user2/proj", &["/home/user".to_string()]),
            "/home/user2/proj"
        );
    }

    #[test]
    fn trailing_slash_in_prefix() {
        assert_eq!(
            apply_path_prefix("/home/user/proj", &["/home/user/".to_string()]),
            "proj"
        );
    }

    #[test]
    fn exact_match_falls_back() {
        // Stripping prefix == value would yield "" — keep original.
        assert_eq!(
            apply_path_prefix("/home/user", &["/home/user".to_string()]),
            "/home/user"
        );
    }

    #[test]
    fn no_match_returns_original() {
        assert_eq!(
            apply_path_prefix("/var/log", &["/home/user".to_string()]),
            "/var/log"
        );
    }

    #[test]
    fn empty_prefixes_returns_original() {
        assert_eq!(apply_path_prefix("/x/y", &[]), "/x/y");
    }

    #[test]
    fn skips_empty_entries() {
        let prefixes = vec!["".to_string(), "/x".to_string()];
        assert_eq!(apply_path_prefix("/x/y", &prefixes), "y");
    }
}
