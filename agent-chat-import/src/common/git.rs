use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use url::Url;

use crate::common::labels::truncate_label_keep_tail;

static REPO_URL_CACHE: OnceLock<Mutex<HashMap<PathBuf, Option<String>>>> = OnceLock::new();

/// Resolve the repository label for a session directory without making git a runtime dependency.
pub fn resolve_repo_label(dir: Option<&Path>) -> Option<String> {
    let dir = dir.filter(|path| !path.as_os_str().is_empty())?;
    let key = match std::fs::canonicalize(dir) {
        Ok(path) => path,
        Err(err) => {
            tracing::debug!(path = ?dir, error = %err, "could not canonicalize repository-label cache key");
            dir.to_path_buf()
        }
    };
    let cache = REPO_URL_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(entries) = cache.lock() {
        if let Some(label) = entries.get(&key) {
            return label.clone();
        }
    } else {
        tracing::debug!(path = ?dir, "repository-label cache mutex is poisoned");
    }

    // Labels must identify the repository rather than the transport used to reach it.
    let label = resolve_repo_url(dir)
        .and_then(|url| canonicalize_repo_identity(&url))
        .map(|identity| truncate_label_keep_tail("repo:", &identity))
        .filter(|label| !label.is_empty());
    match cache.lock() {
        Ok(mut entries) => {
            entries.insert(key, label.clone());
        }
        Err(_) => tracing::debug!(path = ?dir, "could not update repository-label cache"),
    }
    label
}

fn resolve_repo_url(dir: &Path) -> Option<String> {
    let git_dir = discover_git_dir(dir)?;
    read_remote_url(&git_dir)
}

fn canonicalize_repo_identity(remote: &str) -> Option<String> {
    canonicalize_scheme_url(remote).or_else(|| canonicalize_scp_like_url(remote))
}

fn canonicalize_scheme_url(remote: &str) -> Option<String> {
    let url = Url::parse(remote).ok()?;
    if !matches!(url.scheme(), "http" | "https" | "ssh" | "git") {
        return None;
    }
    let host = canonical_host(url.host_str()?)?;
    let host = format_repository_host(host);
    let port = url
        .port()
        .filter(|port| !is_default_port(url.scheme(), *port));
    let path = canonical_repository_path(url.path())?;
    match port {
        Some(port) => Some(format!("{host}:{port}/{path}")),
        None => Some(format!("{host}/{path}")),
    }
}

fn canonicalize_scp_like_url(remote: &str) -> Option<String> {
    if remote.contains("://") || is_windows_absolute_path(remote) {
        return None;
    }
    if let Some((before_at, after_at)) = remote.split_once('@')
        && let Some((host, path_before_at)) = split_scp_like_host_and_path(before_at)
    {
        if !is_unambiguous_scp_host(host) || after_at.contains(':') {
            return None;
        }
        return canonicalize_scp_like_parts(host, &format!("{path_before_at}@{after_at}"));
    }

    let host_and_path = match remote.split_once('@') {
        Some((userinfo, host_and_path)) if !userinfo.is_empty() && !userinfo.contains(':') => {
            host_and_path
        }
        Some(_) => return None,
        None => remote,
    };
    let (host, path) = split_scp_like_host_and_path(host_and_path)?;
    canonicalize_scp_like_parts(host, path)
}

fn split_scp_like_host_and_path(value: &str) -> Option<(&str, &str)> {
    if let Some(bracketed_host) = value.strip_prefix('[') {
        let closing_offset = bracketed_host.find(']')?;
        let closing_index = closing_offset + 1;
        let host = &value[..=closing_index];
        let path = value[closing_index + 1..].strip_prefix(':')?;
        Some((host, path))
    } else {
        value.split_once(':')
    }
}

fn is_unambiguous_scp_host(host: &str) -> bool {
    host == "localhost" || host.contains('.') || (host.starts_with('[') && host.ends_with(']'))
}

fn canonicalize_scp_like_parts(host: &str, path: &str) -> Option<String> {
    if host.contains(['/', '?', '#', '@']) || path.contains(['?', '#']) {
        return None;
    }
    let host = canonical_host(host)?;
    let path = canonical_repository_path(path)?;
    Some(format!("{host}/{path}"))
}

fn canonical_host(host: &str) -> Option<String> {
    let host = host.trim_end_matches('.');
    (!host.is_empty() && !host.chars().any(char::is_whitespace)).then(|| host.to_ascii_lowercase())
}

fn format_repository_host(host: String) -> String {
    if host.starts_with('[') && host.ends_with(']') {
        host
    } else if host.contains(':') {
        format!("[{host}]")
    } else {
        host
    }
}

fn canonical_repository_path(path: &str) -> Option<&str> {
    let path = path.trim_matches('/');
    let path = strip_git_suffix(path);
    (!path.is_empty() && !path.chars().any(char::is_whitespace)).then_some(path)
}

fn strip_git_suffix(path: &str) -> &str {
    let Some(suffix) = path.get(path.len().saturating_sub(".git".len())..) else {
        return path;
    };
    if suffix.eq_ignore_ascii_case(".git") {
        &path[..path.len() - suffix.len()]
    } else {
        path
    }
}

fn is_windows_absolute_path(remote: &str) -> bool {
    let bytes = remote.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'/' | b'\\')
}

fn is_default_port(scheme: &str, port: u16) -> bool {
    matches!(
        (scheme, port),
        ("http", 80) | ("https", 443) | ("ssh", 22) | ("git", 9418)
    )
}

fn discover_git_dir(dir: &Path) -> Option<PathBuf> {
    // `upwards_opts` only probes the repository layout; it does not open a repository or create locks.
    let options = gix_discover::upwards::Options {
        dot_git_only: true,
        ..Default::default()
    };
    let (discovered, _) = match gix_discover::upwards_opts(dir, options) {
        Ok(result) => result,
        Err(err) => {
            tracing::debug!(path = ?dir, error = %err, "git repository discovery failed");
            return None;
        }
    };
    let (git_dir, _) = discovered.into_repository_and_work_tree_directories();
    Some(git_dir)
}

fn read_remote_url(git_dir: &Path) -> Option<String> {
    let config_dir = match std::fs::read_to_string(git_dir.join("commondir")) {
        Ok(value) => {
            let common_dir = value.trim();
            if common_dir.is_empty() {
                tracing::debug!(path = ?git_dir, "git commondir file is empty");
                return None;
            }
            // Git specifies commondir as a path relative to the discovered git directory.
            git_dir.join(common_dir)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => git_dir.to_path_buf(),
        Err(err) => {
            tracing::debug!(path = ?git_dir, error = %err, "could not read git commondir file");
            return None;
        }
    };
    let config_path = config_dir.join("config");
    let config = match gix_config::File::from_path_no_includes(
        config_path.clone(),
        gix_config::Source::Local,
    ) {
        Ok(config) => config,
        Err(err) => {
            tracing::debug!(path = ?config_path, error = %err, "could not read git config");
            return None;
        }
    };

    let mut origin_url = None;
    let mut first_remote_url = None;
    for section in config.sections() {
        let header = section.header();
        let section_name: &[u8] = header.name().as_ref();
        if !section_name.eq_ignore_ascii_case(b"remote") {
            continue;
        }
        let url = first_usable_value(&section, "url")
            .or_else(|| first_usable_value(&section, "fetchurl"));
        let Some(url) = url else {
            continue;
        };
        if first_remote_url.is_none() {
            first_remote_url = Some(url.clone());
        }
        let is_origin = header
            .subsection_name()
            .and_then(|name| std::str::from_utf8(name.as_ref()).ok())
            .is_some_and(|name| name == "origin");
        if is_origin && origin_url.is_none() {
            origin_url = Some(url);
        }
    }
    origin_url.or(first_remote_url).or_else(|| {
        tracing::debug!(path = ?config_path, "git config contains no usable remote URL");
        None
    })
}

fn first_usable_value(section: &gix_config::file::SectionRef<'_>, name: &str) -> Option<String> {
    section.values(name).into_iter().find_map(|value| {
        let value = std::str::from_utf8(value.as_ref()).ok()?.trim();
        (!value.is_empty()).then(|| value.to_string())
    })
}

#[cfg(test)]
pub(crate) mod git_fixture {
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;
    use tempfile::tempdir;

    pub(crate) fn write_git_dir(git_dir: &Path, config: &str) {
        // gix-discover 0.55 accepts this minimal non-bare layout: HEAD plus
        // objects/ and refs/ directories; config is needed only by this URL resolver.
        fs::create_dir_all(git_dir.join("objects")).unwrap();
        fs::create_dir_all(git_dir.join("refs")).unwrap();
        fs::write(git_dir.join("HEAD"), "ref: refs/heads/main\n").unwrap();
        fs::write(git_dir.join("config"), config).unwrap();
    }

    pub(crate) fn repo_fixture(config: &str) -> (TempDir, PathBuf) {
        let temp = tempdir().unwrap();
        let repo = temp.path().join("repo");
        write_git_dir(&repo.join(".git"), config);
        (temp, repo)
    }

    pub(crate) fn origin_config(url: &str) -> String {
        format!("[core]\n    repositoryformatversion = 0\n[remote \"origin\"]\n    url = {url}\n")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        git_fixture::{origin_config, repo_fixture, write_git_dir},
        resolve_repo_label,
    };
    use std::fs;
    use std::path::Path;
    use tempfile::tempdir;

    const ORIGIN: &str = "git@git.example.com:example-org/example-repository.git";

    #[test]
    fn resolves_origin_scp_like_url() {
        let (_temp, repo) = repo_fixture(&origin_config(ORIGIN));
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:git.example.com/example-org/example-repository".to_string())
        );
    }

    #[test]
    fn resolves_https_url_to_repository_identity() {
        let url = "https://example.com/team/project.git";
        let (_temp, repo) = repo_fixture(&origin_config(url));
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:example.com/team/project".to_string())
        );
    }

    #[test]
    fn transport_variants_resolve_to_the_same_repository_identity() {
        let expected = Some("repo:git.example.com/org/repository".to_string());
        for remote in [
            "https://git.example.com/org/repository.git",
            "ssh://git@git.example.com/org/repository.git",
            "git@git.example.com:org/repository.git",
        ] {
            let (_temp, repo) = repo_fixture(&origin_config(remote));
            assert_eq!(resolve_repo_label(Some(&repo)), expected);
        }
    }

    #[test]
    fn ipv6_transport_variants_resolve_to_the_same_repository_identity() {
        let expected = Some("repo:[::1]/org/repository".to_string());
        for remote in [
            "https://[::1]/org/repository.git",
            "ssh://git@[::1]:22/org/repository.git",
            "git@[::1]:org/repository.git",
        ] {
            let (_temp, repo) = repo_fixture(&origin_config(remote));
            assert_eq!(resolve_repo_label(Some(&repo)), expected);
        }
    }

    #[test]
    fn userless_scp_like_url_resolves_to_repository_identity() {
        let (_temp, repo) = repo_fixture(&origin_config("git.example.com:org/repository.git"));
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:git.example.com/org/repository".to_string())
        );
    }

    #[test]
    fn scp_like_url_preserves_at_sign_in_repository_path() {
        let (_temp, repo) = repo_fixture(&origin_config("example.com:group@team/repository.git"));
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:example.com/group@team/repository".to_string())
        );
    }

    #[test]
    fn canonicalization_drops_url_components_outside_repository_identity() {
        let remote = "https://user:pass@GIT.EXAMPLE.COM:8443/org/repository.git/?ref=main#readme";
        let (_temp, repo) = repo_fixture(&origin_config(remote));
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:git.example.com:8443/org/repository".to_string())
        );
    }

    #[test]
    fn removes_git_suffix_case_insensitively() {
        let (_temp, repo) = repo_fixture(&origin_config("https://example.com/org/repository.GIT"));
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:example.com/org/repository".to_string())
        );
    }

    #[test]
    fn skips_unparseable_or_pathless_remote() {
        for remote in [
            "file:///tmp/repository",
            "https://example.com",
            "git@example.com:",
            "not a remote",
            "C:/repository",
            r"C:\repository",
        ] {
            let (_temp, repo) = repo_fixture(&origin_config(remote));
            assert_eq!(resolve_repo_label(Some(&repo)), None);
        }
    }

    #[test]
    fn strips_password_userinfo_from_https_url() {
        let (_temp, repo) = repo_fixture(&origin_config("https://user:pass@example.com/o/r.git"));
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:example.com/o/r".to_string())
        );
    }

    #[test]
    fn skips_url_without_repository_path_even_when_it_has_query() {
        let (_temp, repo) = repo_fixture(&origin_config("https://user:pass@example.com?x=1"));
        assert_eq!(resolve_repo_label(Some(&repo)), None);
    }

    #[test]
    fn skips_pathless_url_with_at_sign_in_query() {
        let url = "https://host.example.com?x=a@b";
        let (_temp, repo) = repo_fixture(&origin_config(url));
        assert_eq!(resolve_repo_label(Some(&repo)), None);
    }

    #[test]
    fn skips_pathless_url_with_userinfo_and_at_sign_in_query() {
        let (_temp, repo) =
            repo_fixture(&origin_config("https://user:pass@host.example.com?x=a@b"));
        assert_eq!(resolve_repo_label(Some(&repo)), None);
    }

    #[test]
    fn skips_url_without_repository_path_even_when_it_has_fragment() {
        let config = "[core]\n    repositoryformatversion = 0\n[remote \"origin\"]\n    url = \"https://user:pass@example.com#frag\"\n";
        let (_temp, repo) = repo_fixture(config);
        assert_eq!(resolve_repo_label(Some(&repo)), None);
    }

    #[test]
    fn skips_pathless_url_with_userinfo_and_at_sign_in_fragment() {
        let config = "[core]\n    repositoryformatversion = 0\n[remote \"origin\"]\n    url = \"https://user:pass@host.example.com#frag@y\"\n";
        let (_temp, repo) = repo_fixture(config);
        assert_eq!(resolve_repo_label(Some(&repo)), None);
    }

    #[test]
    fn strips_userinfo_from_https_url_with_port_path_and_query() {
        let (_temp, repo) =
            repo_fixture(&origin_config("https://user:pass@example.com:8080/q?a=1"));
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:example.com:8080/q".to_string())
        );
    }

    #[test]
    fn retains_non_default_port_in_repository_identity() {
        for (remote, expected) in [
            (
                "https://example.com:8080/o/r.git",
                "repo:example.com:8080/o/r",
            ),
            ("ssh://git@[::1]:2222/o/r.git", "repo:[::1]:2222/o/r"),
        ] {
            let (_temp, repo) = repo_fixture(&origin_config(remote));
            assert_eq!(resolve_repo_label(Some(&repo)), Some(expected.to_string()));
        }
    }

    #[test]
    fn drops_default_transport_port() {
        for remote in [
            "https://example.com:443/o/r.git",
            "ssh://git@example.com:22/o/r.git",
            "git://example.com:9418/o/r.git",
        ] {
            let (_temp, repo) = repo_fixture(&origin_config(remote));
            assert_eq!(
                resolve_repo_label(Some(&repo)),
                Some("repo:example.com/o/r".to_string())
            );
        }
    }

    #[test]
    fn strips_userinfo_but_keeps_port() {
        let (_temp, repo) =
            repo_fixture(&origin_config("https://user:pass@example.com:8080/o/r.git"));
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:example.com:8080/o/r".to_string())
        );
    }

    #[test]
    fn strips_password_userinfo_from_ssh_url() {
        let (_temp, repo) = repo_fixture(&origin_config("ssh://user:pass@example.com/o/r.git"));
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:example.com/o/r".to_string())
        );
    }

    #[test]
    fn skips_ambiguous_scp_like_url() {
        for remote in [
            "user:pass@git.example.com:o/r.git",
            "john.doe:secret@host.com:o/r.git",
            "host:o@r:x",
        ] {
            let (_temp, repo) = repo_fixture(&origin_config(remote));
            assert_eq!(resolve_repo_label(Some(&repo)), None);
        }
    }

    #[test]
    fn strips_bare_scp_like_ssh_user() {
        let url = "git@git.example.com:example-org/example-repository.git";
        let (_temp, repo) = repo_fixture(&origin_config(url));
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:git.example.com/example-org/example-repository".to_string())
        );
    }

    #[test]
    fn preserves_at_signs_that_are_only_in_the_path() {
        for (url, expected) in [
            ("https://example.com/o/r.git", "repo:example.com/o/r"),
            ("https://example.com/a@b/c.git", "repo:example.com/a@b/c"),
        ] {
            let (_temp, repo) = repo_fixture(&origin_config(url));
            assert_eq!(resolve_repo_label(Some(&repo)), Some(expected.to_string()));
        }
    }

    #[test]
    fn strips_multiple_at_signs_in_url_userinfo() {
        let url = "https://a@b@example.com/p";
        let (_temp, repo) = repo_fixture(&origin_config(url));
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:example.com/p".to_string())
        );
    }

    #[test]
    fn resolves_case_insensitive_remote_section_name() {
        let url = "https://example.com/case-insensitive.git";
        let config = format!(
            "[core]\n    repositoryformatversion = 0\n[Remote \"origin\"]\n    url = {url}\n"
        );
        let (_temp, repo) = repo_fixture(&config);
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:example.com/case-insensitive".to_string())
        );
    }

    #[test]
    fn falls_back_to_first_remote_when_origin_is_absent() {
        let config = "[core]\n    repositoryformatversion = 0\n[remote \"upstream\"]\n    url = https://example.com/upstream.git\n";
        let (_temp, repo) = repo_fixture(config);
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:example.com/upstream".to_string())
        );
    }

    #[test]
    fn falls_back_to_fetchurl_when_url_is_absent() {
        let config = "[core]\n    repositoryformatversion = 0\n[remote \"origin\"]\n    fetchurl = https://example.com/fetch.git\n";
        let (_temp, repo) = repo_fixture(config);
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:example.com/fetch".to_string())
        );
    }

    #[test]
    fn skips_existing_non_git_directory() {
        let temp = tempdir().unwrap();
        let dir = temp.path().join("plain");
        fs::create_dir(&dir).unwrap();
        assert_eq!(resolve_repo_label(Some(&dir)), None);
    }

    #[test]
    fn skips_nonexistent_path() {
        let temp = tempdir().unwrap();
        assert_eq!(resolve_repo_label(Some(&temp.path().join("missing"))), None);
    }

    #[test]
    fn skips_none_and_empty_paths() {
        assert_eq!(resolve_repo_label(None), None);
        assert_eq!(resolve_repo_label(Some(Path::new(""))), None);
    }

    #[test]
    fn resolves_worktree_gitfile_using_main_repository_config() {
        let temp = tempdir().unwrap();
        let main_repo = temp.path().join("repo");
        let main_git = main_repo.join(".git");
        write_git_dir(&main_git, &origin_config(ORIGIN));

        let worktree = temp.path().join("wt");
        let linked_git = main_git.join("worktrees/wt");
        fs::create_dir_all(&worktree).unwrap();
        fs::create_dir_all(linked_git.join("objects")).unwrap();
        fs::create_dir_all(linked_git.join("refs")).unwrap();
        fs::write(linked_git.join("HEAD"), "ref: refs/heads/main\n").unwrap();
        fs::write(linked_git.join("commondir"), "../..\n").unwrap();
        fs::write(
            worktree.join(".git"),
            format!("gitdir: {}\n", linked_git.display()),
        )
        .unwrap();

        assert_eq!(
            resolve_repo_label(Some(&worktree)),
            Some("repo:git.example.com/example-org/example-repository".to_string())
        );
    }

    #[test]
    fn nearest_nested_repository_wins() {
        let temp = tempdir().unwrap();
        let outer = temp.path().join("outer");
        write_git_dir(
            &outer.join(".git"),
            &origin_config("https://example.com/outer.git"),
        );
        let inner = outer.join("inner");
        write_git_dir(
            &inner.join(".git"),
            &origin_config("https://example.com/inner.git"),
        );

        assert_eq!(
            resolve_repo_label(Some(&inner)),
            Some("repo:example.com/inner".to_string())
        );
    }

    #[test]
    fn caches_positive_and_negative_results() {
        let (temp, repo) = repo_fixture(&origin_config("https://example.com/first.git"));
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:example.com/first".to_string())
        );
        fs::write(
            repo.join(".git/config"),
            origin_config("https://example.com/second.git"),
        )
        .unwrap();
        assert_eq!(
            resolve_repo_label(Some(&repo)),
            Some("repo:example.com/first".to_string())
        );

        let non_git = temp.path().join("not-yet-a-repo");
        fs::create_dir(&non_git).unwrap();
        assert_eq!(resolve_repo_label(Some(&non_git)), None);
        write_git_dir(
            &non_git.join(".git"),
            &origin_config("https://example.com/created-later.git"),
        );
        // Negative results are cached too, so a later filesystem change does not alter this process's result.
        assert_eq!(resolve_repo_label(Some(&non_git)), None);
    }

    #[test]
    fn truncates_long_urls_to_the_label_limit_while_keeping_prefix() {
        let url = format!("https://example.com/{}", "x".repeat(600));
        let (_temp, repo) = repo_fixture(&origin_config(&url));
        let label = resolve_repo_label(Some(&repo)).unwrap();
        assert!(label.starts_with("repo:"));
        assert!(label.len() <= 512);
    }
}
