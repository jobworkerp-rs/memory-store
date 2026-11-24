//! `bundle manifest` / `bundle verify`: identity and integrity of the release
//! bundle this binary belongs to, as built. Applications add files and sign the
//! executable when they package the bundle, after which only the run-time
//! check used by `local apply` applies.

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use grpc_admin::db_migrate::{
    catalog,
    local::{
        bundle::{BundleManifest, MANIFEST_FILE},
        files::regular_files,
        output::{ErrorCode, Resolution, classify},
    },
    schema::{migrations, seeds},
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Subcommand)]
pub(super) enum BundleCommand {
    /// Record the identity of the bundle this binary belongs to (build time).
    Manifest(BundleManifestArgs),
    /// Check that the bundle this binary belongs to is complete and unchanged
    /// since its manifest was written (build time; fails once an application
    /// has added files or signed the executable).
    Verify,
}

#[derive(Debug, Args)]
pub(super) struct BundleManifestArgs {
    #[arg(long)]
    source_revision: String,
    /// The bundle was built from a working tree with uncommitted changes.
    #[arg(long)]
    source_dirty: bool,
}

/// Run a bundle command, print its structured result, and return the exit code.
pub(super) fn run_bundle(command: BundleCommand) -> i32 {
    let name = match command {
        BundleCommand::Manifest(_) => "bundle_manifest",
        BundleCommand::Verify => "bundle_verify",
    };
    match bundle_result(command) {
        Ok(digest) => {
            let status = if name == "bundle_manifest" {
                "written"
            } else {
                "verified"
            };
            println!("{name} status={status} digest={digest}");
            0
        }
        Err(error) => {
            eprintln!("{error:#}");
            println!(
                "{name} status=failed error_code={}",
                ErrorCode::BundleInvalid
            );
            1
        }
    }
}

fn bundle_result(command: BundleCommand) -> Result<String> {
    let root = bundle_root()?;
    match command {
        BundleCommand::Manifest(args) => {
            Ok(
                BundleManifest::write(&root, &args.source_revision, args.source_dirty)?
                    .content_digest,
            )
        }
        BundleCommand::Verify => verify_bundle(&root),
    }
}

/// The directory that holds this binary and its `db-migrate/` catalog.
fn bundle_root() -> Result<PathBuf> {
    let executable = std::env::current_exe().context("locating this executable")?;
    executable
        .parent()
        .map(PathBuf::from)
        .context("the executable has no parent directory")
}

fn invalid(error: anyhow::Error) -> anyhow::Error {
    classify(
        error,
        ErrorCode::BundleInvalid,
        Resolution::ToolUpdateRequired,
    )
}

/// Build-time check: the shipped catalog, then that the bundle still has the
/// content its manifest records.
fn verify_bundle(root: &Path) -> Result<String> {
    catalog::load_catalog().map_err(invalid)?;
    verify_shipped_catalog(root)?;
    Ok(BundleManifest::verify(root)?.content_digest)
}

/// Check that the shipped catalog is exactly the one compiled into this
/// binary. The binary never reads the shipped copy; it exists for the
/// application's provenance records.
fn verify_shipped_catalog(root: &Path) -> Result<()> {
    let catalog_root = root.join(CATALOG_DIRECTORY);
    let compiled = compiled_files()
        .into_iter()
        .map(|(name, content)| (PathBuf::from(name), content))
        .collect::<BTreeMap<_, _>>();
    let shipped = regular_files(&catalog_root)
        .map_err(invalid)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    if let Some(name) = shipped
        .symmetric_difference(&compiled.keys().cloned().collect())
        .next()
    {
        return Err(invalid(anyhow::anyhow!(
            "the shipped catalog does not match the one compiled into this binary at {}",
            name.display()
        )));
    }
    for (name, content) in &compiled {
        let shipped = std::fs::read_to_string(catalog_root.join(name))
            .with_context(|| format!("reading the shipped {}", name.display()))
            .map_err(invalid)?;
        if shipped != *content {
            return Err(invalid(anyhow::anyhow!(
                "the shipped {} differs from the catalog compiled into this binary",
                name.display()
            )));
        }
    }
    Ok(())
}

/// Directory of the bundle that carries the release catalog.
pub(super) const CATALOG_DIRECTORY: &str = "db-migrate";

/// Every catalog file compiled into this binary, by its path below the
/// catalog directory.
pub(super) fn compiled_files() -> Vec<(String, String)> {
    let mut files = catalog::compiled_catalog_files()
        .into_iter()
        .map(|(name, content)| (name.to_string(), content.to_string()))
        .collect::<Vec<_>>();
    files.push((
        seeds::SEED_EXPECTATIONS_FILE.to_string(),
        seeds::compiled_seed_expectations().to_string(),
    ));
    files.extend(migrations::migrations().iter().map(|migration| {
        (
            format!(
                "{}/migrations/{}",
                migrations::BACKEND,
                migrations::file_name(migration)
            ),
            migration.sql.as_str().to_string(),
        )
    }));
    files
}

/// Identity of the running bundle, after the run-time checks; development
/// builds, which have no manifest, have none.
#[cfg_attr(feature = "postgres", allow(dead_code))]
pub(super) fn running_bundle_identity() -> Result<Option<String>> {
    bundle_identity_at(&bundle_root().map_err(invalid)?)
}

/// Run-time check of the bundle at `root`: the compiled catalog must be
/// consistent and the shipped copy must match it. The content digest is not
/// compared: applications add files and sign the executable after the
/// manifest is written, and an executable checking its own digest protects
/// nothing.
#[cfg_attr(feature = "postgres", allow(dead_code))]
fn bundle_identity_at(root: &Path) -> Result<Option<String>> {
    catalog::load_catalog().map_err(invalid)?;
    if !root.join(MANIFEST_FILE).exists() {
        return Ok(None);
    }
    verify_shipped_catalog(root)?;
    Ok(Some(BundleManifest::load(root)?.content_digest))
}

/// What an application does to the bundle when it packages it, after the
/// manifest is written; shared by the unit and the end-to-end tests.
#[cfg(test)]
pub(super) mod packaging {
    use std::io::Write;
    use std::path::Path;

    pub const EXECUTABLE: &str = "memories-db-migrate";

    pub fn add_provenance_record(root: &Path) {
        std::fs::write(root.join("memories-source-provenance.json"), "{}").unwrap();
    }

    /// Code signing changes the executable; appending stands in for it.
    pub fn sign_executable(root: &Path) {
        std::fs::OpenOptions::new()
            .append(true)
            .open(root.join(EXECUTABLE))
            .unwrap()
            .write_all(b"signature")
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::packaging::{EXECUTABLE, add_provenance_record, sign_executable};
    use super::*;
    use grpc_admin::db_migrate::local::{bundle::content_digest, output::LocalFailure};

    fn code(error: anyhow::Error) -> Option<ErrorCode> {
        error
            .downcast_ref::<LocalFailure>()
            .map(|failure| failure.error_code)
    }

    /// A bundle that ships the compiled catalog changed by `edit`, with a
    /// manifest written afterwards, so only the comparison with the compiled
    /// catalog can fail.
    fn bundle_with(edit: impl FnOnce(&Path)) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(EXECUTABLE), b"executable").unwrap();
        let catalog_root = root.path().join(CATALOG_DIRECTORY);
        for (name, content) in compiled_files() {
            let path = catalog_root.join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, content).unwrap();
        }
        edit(&catalog_root);
        BundleManifest::write(root.path(), "abc", false).unwrap();
        root
    }

    #[test]
    fn a_bundle_of_the_compiled_catalog_verifies() {
        let root = bundle_with(|_| {});
        let recorded = BundleManifest::load(root.path()).unwrap().content_digest;
        assert_eq!(verify_bundle(root.path()).unwrap(), recorded);
        assert_eq!(bundle_identity_at(root.path()).unwrap(), Some(recorded));
    }

    /// An application adds its provenance record and signs the executable
    /// after the manifest is written, when it packages the bundle.
    #[test]
    fn packaging_changes_pass_at_run_time_but_not_the_build_check() {
        let changes = [
            (
                "an added provenance record",
                add_provenance_record as fn(&Path),
            ),
            ("a signed executable", sign_executable),
        ];
        for (change, apply) in changes {
            let root = bundle_with(|_| {});
            apply(root.path());
            let recorded = BundleManifest::load(root.path()).unwrap().content_digest;
            assert_ne!(
                content_digest(root.path()).unwrap(),
                recorded,
                "{change} must change the bundle content"
            );
            assert_eq!(
                bundle_identity_at(root.path()).unwrap(),
                Some(recorded),
                "{change}"
            );
            assert_eq!(
                code(verify_bundle(root.path()).unwrap_err()),
                Some(ErrorCode::BundleInvalid),
                "{change}"
            );
        }
    }

    #[test]
    fn a_bundle_without_a_manifest_has_no_identity() {
        // Development builds run outside a bundle, without a shipped catalog.
        let root = tempfile::tempdir().unwrap();
        assert_eq!(bundle_identity_at(root.path()).unwrap(), None);
    }

    #[test]
    fn an_unreadable_or_foreign_manifest_is_invalid() {
        let root = bundle_with(|_| {});
        let mut foreign = BundleManifest::load(root.path()).unwrap();
        foreign.format = "memories-db-migrate-bundle-v0".to_string();
        for manifest in [b"not json".to_vec(), serde_json::to_vec(&foreign).unwrap()] {
            std::fs::write(root.path().join(MANIFEST_FILE), &manifest).unwrap();
            assert_eq!(
                code(bundle_identity_at(root.path()).unwrap_err()),
                Some(ErrorCode::BundleInvalid),
                "{}",
                String::from_utf8_lossy(&manifest)
            );
        }
    }

    #[test]
    fn a_changed_missing_or_extra_catalog_file_is_invalid() {
        let first = format!(
            "{}/migrations/{}",
            migrations::BACKEND,
            migrations::file_name(&migrations::migrations()[0])
        );
        let other = if migrations::BACKEND == "sqlite" {
            "postgres"
        } else {
            "sqlite"
        };
        let bundles = [
            bundle_with(|catalog| std::fs::write(catalog.join(&first), "-- changed\n").unwrap()),
            bundle_with(|catalog| std::fs::remove_file(catalog.join(&first)).unwrap()),
            bundle_with(|catalog| {
                std::fs::create_dir_all(catalog.join(other)).unwrap();
                std::fs::write(catalog.join(other).join("seed-expectations.json"), "{}").unwrap();
            }),
        ];
        for root in bundles {
            for error in [
                verify_bundle(root.path()).unwrap_err(),
                bundle_identity_at(root.path()).unwrap_err(),
            ] {
                assert!(format!("{error:#}").contains("shipped"), "{error:#}");
                assert_eq!(code(error), Some(ErrorCode::BundleInvalid));
            }
        }
    }
}
