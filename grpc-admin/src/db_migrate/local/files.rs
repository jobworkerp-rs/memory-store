//! File-system primitives shared by backups, restores and bundle identity.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};

/// A copied or shipped regular file and its content digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: PathBuf,
    pub size: u64,
    pub sha256: String,
}

/// Flush one file's data to the device. On macOS `fsync` alone suffices per
/// file because `barrier` later flushes the drive cache once for all of them;
/// a full flush per file would multiply the cost by the file count.
pub fn flush(file: &std::fs::File) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: the descriptor is owned by `file` and stays open.
        if unsafe { libc::fsync(file.as_raw_fd()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }
    #[cfg(not(target_os = "macos"))]
    {
        file.sync_all().map_err(Into::into)
    }
}

/// Make everything flushed so far durable before it is published.
pub fn barrier(path: &Path) -> Result<()> {
    std::fs::File::open(path)
        .and_then(|handle| handle.sync_all())
        .with_context(|| format!("syncing {}", path.display()))
}

/// Persist entries created, renamed or removed in `directory`.
pub fn sync_directory(directory: &Path) -> Result<()> {
    let handle = std::fs::File::open(directory)
        .with_context(|| format!("opening directory {}", directory.display()))?;
    flush(&handle).with_context(|| format!("syncing directory {}", directory.display()))
}

/// Replace `path` with `bytes` so that a crash leaves either the old or the
/// new complete content.
pub fn write_atomically(path: &Path, bytes: &[u8]) -> Result<()> {
    let directory = path
        .parent()
        .with_context(|| format!("{} has no parent directory", path.display()))?;
    let temporary = with_suffix(path, &format!(".partial-{}", std::process::id()));
    let mut file = std::fs::File::create(&temporary)
        .with_context(|| format!("creating {}", temporary.display()))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path).with_context(|| format!("replacing {}", path.display()))?;
    sync_directory(directory)
}

/// `path` with `suffix` appended to its last component.
pub fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

/// Remove a file or a directory tree; a missing path is already removed.
pub fn remove_path(path: &Path) -> Result<()> {
    let result = match path.symlink_metadata() {
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(path),
        Ok(_) => std::fs::remove_file(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => Err(error),
    };
    result.with_context(|| format!("removing {}", path.display()))
}

pub fn hash_file(path: &Path) -> Result<String> {
    let mut input =
        std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = std::io::Read::read(&mut input, &mut buffer)
            .with_context(|| format!("reading {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex_digest(hasher))
}

pub fn hex_digest(hasher: Sha256) -> String {
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Relative paths of every regular file under `root`, sorted. Anything other
/// than regular files and directories is rejected so that backups and bundle
/// digests never silently follow or skip a link.
pub fn regular_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_regular_files(root, Path::new(""), &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_regular_files(root: &Path, relative: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    let directory = root.join(relative);
    for entry in
        std::fs::read_dir(&directory).with_context(|| format!("reading {}", directory.display()))?
    {
        let entry = entry?;
        let path = relative.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_regular_files(root, &path, files)?;
        } else if file_type.is_file() {
            files.push(path);
        } else {
            bail!(
                "{} is neither a regular file nor a directory",
                root.join(&path).display()
            );
        }
    }
    Ok(())
}

/// Total size of a file or directory tree; a missing path has size zero.
pub fn total_size(path: &Path) -> Result<u64> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => regular_files(path)?
            .iter()
            .map(|relative| Ok(std::fs::metadata(path.join(relative))?.len()))
            .sum(),
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(anyhow::Error::new(error).context(format!("reading {}", path.display()))),
    }
}

/// Copy a regular file (using copy-on-write or in-kernel copies where the
/// platform offers them) and return its size and digest. The data is flushed
/// but not yet behind a `barrier`.
pub fn copy_file(source: &Path, destination: &Path) -> Result<FileEntry> {
    std::fs::copy(source, destination)
        .with_context(|| format!("copying {} to {}", source.display(), destination.display()))?;
    flush(&std::fs::File::open(destination)?)?;
    Ok(FileEntry {
        path: destination.to_path_buf(),
        size: std::fs::metadata(destination)?.len(),
        sha256: hash_file(destination)?,
    })
}

/// Copy the tree at `source` into `destination` and describe every file by
/// its path relative to `destination`.
pub fn copy_directory(source: &Path, destination: &Path) -> Result<Vec<FileEntry>> {
    std::fs::create_dir_all(destination)
        .with_context(|| format!("creating {}", destination.display()))?;
    let mut entries = Vec::new();
    let mut directories = vec![destination.to_path_buf()];
    for relative in regular_files(source)? {
        let target = destination.join(&relative);
        if let Some(parent) = target.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent)?;
            directories.push(parent.to_path_buf());
        }
        let copied = copy_file(&source.join(&relative), &target)?;
        entries.push(FileEntry {
            path: relative,
            ..copied
        });
    }
    for directory in directories {
        sync_directory(&directory)?;
    }
    Ok(entries)
}

/// Free bytes available to this process on the file system that will hold
/// `path` (the nearest existing ancestor when `path` is not created yet).
pub fn available_space(path: &Path) -> Result<u64> {
    use std::os::unix::ffi::OsStrExt;

    let existing = path
        .ancestors()
        .find(|candidate| candidate.exists())
        .context("no existing ancestor directory")?;
    let c_path = std::ffi::CString::new(existing.as_os_str().as_bytes())
        .context("path contains a NUL byte")?;
    // SAFETY: `statvfs` is a plain C struct written by the call below.
    let mut stats: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c_path` is NUL-terminated and `stats` is exclusively borrowed.
    if unsafe { libc::statvfs(c_path.as_ptr(), &mut stats) } != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("checking free space of {}", existing.display()));
    }
    #[allow(clippy::unnecessary_cast)]
    Ok(stats.f_bavail as u64 * stats.f_frsize as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_directory_reproduces_nested_files_with_digests() {
        let workspace = tempfile::tempdir().unwrap();
        let source = workspace.path().join("source");
        std::fs::create_dir_all(source.join("a/b")).unwrap();
        std::fs::write(source.join("a/b/c"), b"nested").unwrap();
        std::fs::write(source.join("top"), b"top").unwrap();
        let destination = workspace.path().join("copy");

        let entries = copy_directory(&source, &destination).unwrap();
        assert_eq!(
            entries.iter().map(|e| e.path.clone()).collect::<Vec<_>>(),
            vec![PathBuf::from("a/b/c"), PathBuf::from("top")]
        );
        assert_eq!(std::fs::read(destination.join("a/b/c")).unwrap(), b"nested");
        assert_eq!(entries[1].size, 3);
        assert_eq!(entries[1].sha256, hash_file(&source.join("top")).unwrap());
        assert_eq!(total_size(&source).unwrap(), 9);
    }

    #[test]
    fn links_are_rejected_instead_of_followed() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("real"), b"x").unwrap();
        std::os::unix::fs::symlink(workspace.path().join("real"), workspace.path().join("link"))
            .unwrap();
        assert!(regular_files(workspace.path()).is_err());
    }

    #[test]
    fn missing_paths_have_zero_size_and_are_already_removed() {
        let workspace = tempfile::tempdir().unwrap();
        let missing = workspace.path().join("missing");
        assert_eq!(total_size(&missing).unwrap(), 0);
        remove_path(&missing).unwrap();
    }
}
