//! Backups of the vector stores taken by `embedding switch` (spec §3.5
//! step 4) and used by `embedding restore`. A backup copies each LanceDB
//! directory as a whole (vector tables, embedding index, identifier) and
//! is complete only once its manifest says so.

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const MANIFEST: &str = "manifest.json";
const KIND: &str = "embedding";
const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupStore {
    /// Directory the store lives in.
    pub source: String,
    /// Subdirectory of the backup holding the copy.
    pub copy: String,
    /// Whether the store existed (an absent one has no copy).
    #[serde(default = "existed_default")]
    pub present: bool,
}

fn existed_default() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    pub kind: String,
    pub format_version: u32,
    pub attempt_id: String,
    pub created_at: i64,
    /// Space of the tables when the backup was taken.
    pub space_id: Option<String>,
    pub rdb_id: Option<String>,
    pub stores: Vec<BackupStore>,
    pub complete: bool,
}

#[derive(Debug)]
pub enum BackupError {
    InsufficientSpace,
    NotWritable(anyhow::Error),
    Other(anyhow::Error),
}

impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InsufficientSpace => f.write_str("not enough free space for the backup"),
            Self::NotWritable(e) | Self::Other(e) => write!(f, "{e:#}"),
        }
    }
}

impl std::error::Error for BackupError {}

pub fn backup_path(parent: &Path, attempt_id: &str) -> PathBuf {
    parent.join(format!("embedding-{attempt_id}"))
}

fn dir_size(path: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        total += if meta.is_dir() {
            dir_size(&entry.path())?
        } else {
            meta.len()
        };
    }
    Ok(total)
}

fn free_space(path: &Path) -> std::io::Result<u64> {
    use std::os::unix::ffi::OsStrExt as _;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    // SAFETY: `statvfs` fills the zeroed struct for a valid C path.
    unsafe {
        let mut s: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c.as_ptr(), &mut s) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        #[allow(clippy::unnecessary_cast)]
        Ok(s.f_bavail as u64 * s.f_frsize as u64)
    }
}

fn copy_dir(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

fn write_manifest(path: &Path, manifest: &BackupManifest) -> Result<()> {
    infra::infra::embedding_space::storage::write_atomic(
        &path.join(MANIFEST),
        &serde_json::to_vec_pretty(manifest)?,
    )
}

/// Copy `sources` (local LanceDB directories) into a fresh backup for
/// `attempt_id` under `parent`, replacing any unfinished one. The
/// manifest is marked complete last.
pub fn create(
    parent: &Path,
    attempt_id: &str,
    space_id: Option<String>,
    rdb_id: Option<String>,
    sources: &[PathBuf],
) -> std::result::Result<PathBuf, BackupError> {
    let path = backup_path(parent, attempt_id);
    if path.exists() {
        std::fs::remove_dir_all(&path)
            .with_context(|| format!("removing the unfinished backup {}", path.display()))
            .map_err(BackupError::NotWritable)?;
    }
    std::fs::create_dir_all(&path)
        .with_context(|| format!("creating {}", path.display()))
        .map_err(BackupError::NotWritable)?;
    let needed: u64 = sources
        .iter()
        .filter(|s| s.exists())
        .map(|s| dir_size(s))
        .sum::<std::io::Result<u64>>()
        .map_err(|e| BackupError::Other(e.into()))?;
    let free = free_space(&path).map_err(|e| BackupError::Other(e.into()))?;
    if needed > free {
        let _ = std::fs::remove_dir_all(&path);
        return Err(BackupError::InsufficientSpace);
    }
    let mut manifest = BackupManifest {
        kind: KIND.into(),
        format_version: FORMAT_VERSION,
        attempt_id: attempt_id.into(),
        created_at: command_utils::util::datetime::now_millis(),
        space_id,
        rdb_id,
        stores: Vec::new(),
        complete: false,
    };
    write_manifest(&path, &manifest).map_err(BackupError::NotWritable)?;
    for (i, source) in sources.iter().enumerate() {
        let copy = format!("store-{i}");
        if source.exists() {
            copy_dir(source, &path.join(&copy))
                .with_context(|| format!("copying {}", source.display()))
                .map_err(BackupError::Other)?;
        }
        manifest.stores.push(BackupStore {
            source: source.to_string_lossy().into_owned(),
            copy,
            present: source.exists(),
        });
    }
    manifest.complete = true;
    write_manifest(&path, &manifest).map_err(BackupError::Other)?;
    Ok(path)
}

/// Read and check a complete backup of `attempt_id`.
pub fn verify(path: &Path, attempt_id: &str) -> Result<BackupManifest> {
    let manifest: BackupManifest = serde_json::from_slice(
        &std::fs::read(path.join(MANIFEST))
            .with_context(|| format!("reading the manifest of {}", path.display()))?,
    )
    .context("parsing the backup manifest")?;
    anyhow::ensure!(manifest.kind == KIND, "not an embedding backup");
    anyhow::ensure!(
        manifest.format_version == FORMAT_VERSION,
        "unsupported backup format"
    );
    anyhow::ensure!(
        manifest.attempt_id == attempt_id,
        "backup of another attempt"
    );
    anyhow::ensure!(manifest.complete, "the backup is not complete");
    for s in &manifest.stores {
        let copy = path.join(&s.copy);
        anyhow::ensure!(copy.is_dir() || !s.present, "the backup lacks {}", s.copy);
    }
    Ok(manifest)
}

/// Where a store is copied from the backup before it is swapped in.
fn staged_path(live: &Path) -> PathBuf {
    let mut name = live.file_name().unwrap_or_default().to_os_string();
    name.push(".restoring");
    live.with_file_name(name)
}

fn replaced_path(live: &Path) -> PathBuf {
    let mut name = live.file_name().unwrap_or_default().to_os_string();
    name.push(".replaced");
    live.with_file_name(name)
}

/// Copy every store of the backup next to its live directory (`.restoring`
/// sibling, on the same filesystem so it can be renamed into place).
/// Returns `(live, staged)` pairs. Idempotent: earlier staged copies and
/// leftovers of an interrupted swap are discarded first.
pub fn stage_restore(path: &Path, manifest: &BackupManifest) -> Result<Vec<(PathBuf, PathBuf)>> {
    let mut out = Vec::with_capacity(manifest.stores.len());
    for s in &manifest.stores {
        let live = PathBuf::from(&s.source);
        let staged = staged_path(&live);
        for leftover in [&staged, &replaced_path(&live)] {
            if leftover.exists() {
                std::fs::remove_dir_all(leftover)
                    .with_context(|| format!("clearing {}", leftover.display()))?;
            }
        }
        let copy = path.join(&s.copy);
        if s.present {
            copy_dir(&copy, &staged).with_context(|| format!("staging {}", live.display()))?;
        } else {
            // The store did not exist when the backup was taken.
            std::fs::create_dir_all(&staged)
                .with_context(|| format!("staging {}", live.display()))?;
        }
        out.push((live, staged));
    }
    Ok(out)
}

/// Whether each live store's filesystem has room for its staged copy.
pub fn restore_fits(path: &Path, manifest: &BackupManifest) -> Result<bool> {
    for s in manifest.stores.iter().filter(|s| s.present) {
        let needed = dir_size(&path.join(&s.copy))?;
        let live = Path::new(&s.source);
        let parent = live.parent().unwrap_or(live);
        if needed > free_space(parent)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Exchange two paths atomically where the platform supports it.
#[cfg(target_os = "linux")]
fn exchange(a: &Path, b: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;
    let a = std::ffi::CString::new(a.as_os_str().as_bytes())?;
    let b = std::ffi::CString::new(b.as_os_str().as_bytes())?;
    // SAFETY: plain syscall on two valid C paths.
    let rc = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            a.as_ptr(),
            libc::AT_FDCWD,
            b.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn exchange(_a: &Path, _b: &Path) -> std::io::Result<()> {
    Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
}

/// Put a staged store in place of the live one. The exchange is atomic
/// where supported, so the live path never disappears; otherwise the live
/// directory is moved aside first and briefly absent.
pub fn swap_in(live: &Path, staged: &Path) -> Result<()> {
    if !live.exists() {
        return std::fs::rename(staged, live)
            .with_context(|| format!("moving {} into place", staged.display()));
    }
    let old = if exchange(staged, live).is_ok() {
        staged.to_path_buf()
    } else {
        let aside = replaced_path(live);
        std::fs::rename(live, &aside)
            .with_context(|| format!("moving {} aside", live.display()))?;
        std::fs::rename(staged, live)
            .with_context(|| format!("moving {} into place", staged.display()))?;
        aside
    };
    std::fs::remove_dir_all(&old).with_context(|| format!("removing {}", old.display()))
}

/// Complete embedding backups under `parent`, newest first. Anything else
/// in the directory (other kinds of backup, foreign files) is ignored.
pub fn list(parent: &Path) -> Result<Vec<(PathBuf, BackupManifest)>> {
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(anyhow::Error::new(e).context(format!("listing {}", parent.display())));
        }
    };
    let mut out = Vec::new();
    for entry in entries {
        let path = entry?.path();
        let ours = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("embedding-"));
        if !ours {
            continue;
        }
        let Ok(bytes) = std::fs::read(path.join(MANIFEST)) else {
            continue;
        };
        if let Ok(m) = serde_json::from_slice::<BackupManifest>(&bytes)
            && m.kind == KIND
            && m.complete
        {
            out.push((path, m));
        }
    }
    out.sort_by(|a, b| (b.1.created_at, &b.0).cmp(&(a.1.created_at, &a.0)));
    Ok(out)
}

/// Delete complete embedding backups beyond the newest `keep`. Backups in
/// `protect` (those an unfinished attempt refers to) are kept and still
/// count toward `keep`.
pub fn apply_retention(parent: &Path, keep: usize, protect: &[&Path]) -> Result<Vec<PathBuf>> {
    let mut removed = Vec::new();
    for (path, _) in list(parent)?.into_iter().skip(keep) {
        if protect.contains(&path.as_path()) {
            continue;
        }
        remove(&path)?;
        removed.push(path);
    }
    Ok(removed)
}

/// Delete a backup that will not be used (unfinished or abandoned).
pub fn remove(path: &Path) -> Result<()> {
    if path.exists() {
        std::fs::remove_dir_all(path).with_context(|| format!("removing {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_verify_restore_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = dir.path().join("lance");
        std::fs::create_dir_all(store.join("t.lance")).unwrap();
        std::fs::write(store.join("t.lance/data"), b"v1").unwrap();
        let parent = dir.path().join("backups");
        let path = create(
            &parent,
            "a1",
            Some("s".into()),
            None,
            std::slice::from_ref(&store),
        )
        .unwrap();
        let manifest = verify(&path, "a1").unwrap();
        assert!(verify(&path, "other").is_err());
        assert!(restore_fits(&path, &manifest).unwrap());
        // A copy that went missing is not mistaken for an absent store.
        let hidden = path.join("hidden");
        std::fs::rename(path.join("store-0"), &hidden).unwrap();
        assert!(verify(&path, "a1").is_err());
        std::fs::rename(&hidden, path.join("store-0")).unwrap();

        std::fs::write(store.join("t.lance/data"), b"v2").unwrap();
        std::fs::write(store.join("new"), b"x").unwrap();
        // An interrupted earlier attempt left a staged copy behind.
        let first = stage_restore(&path, &manifest).unwrap();
        let staged = stage_restore(&path, &manifest).unwrap();
        assert_eq!(first, staged);
        for (live, staged) in &staged {
            swap_in(live, staged).unwrap();
            assert!(!staged.exists());
        }
        assert_eq!(std::fs::read(store.join("t.lance/data")).unwrap(), b"v1");
        assert!(!store.join("new").exists());
    }

    #[test]
    fn swap_in_moves_into_a_missing_live_path() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("lance");
        let staged = staged_path(&live);
        std::fs::create_dir_all(&staged).unwrap();
        std::fs::write(staged.join("f"), b"x").unwrap();
        swap_in(&live, &staged).unwrap();
        assert_eq!(std::fs::read(live.join("f")).unwrap(), b"x");
        assert!(!staged.exists());
    }

    #[test]
    fn retention_keeps_the_newest_and_protected_embedding_backups_only() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("backups");
        let store = dir.path().join("lance");
        std::fs::create_dir_all(&store).unwrap();
        let mut paths = Vec::new();
        for id in ["a1", "a2", "a3"] {
            paths.push(create(&parent, id, None, None, std::slice::from_ref(&store)).unwrap());
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        // Not ours, or not complete: never touched.
        let foreign = parent.join("memories-backup-x");
        std::fs::create_dir_all(&foreign).unwrap();
        let unfinished = backup_path(&parent, "a4");
        std::fs::create_dir_all(&unfinished).unwrap();

        let removed = apply_retention(&parent, 1, &[paths[0].as_path()]).unwrap();
        assert_eq!(removed, vec![paths[1].clone()]);
        assert!(paths[0].exists() && paths[2].exists());
        assert!(foreign.exists() && unfinished.exists());
        assert!(
            apply_retention(&dir.path().join("absent"), 1, &[])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn unfinished_backup_does_not_verify() {
        let dir = tempfile::tempdir().unwrap();
        let path = backup_path(dir.path(), "a1");
        std::fs::create_dir_all(&path).unwrap();
        write_manifest(
            &path,
            &BackupManifest {
                kind: KIND.into(),
                format_version: FORMAT_VERSION,
                attempt_id: "a1".into(),
                created_at: 0,
                space_id: None,
                rdb_id: None,
                stores: vec![],
                complete: false,
            },
        )
        .unwrap();
        assert!(verify(&path, "a1").is_err());
    }
}
