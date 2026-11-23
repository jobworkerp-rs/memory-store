//! `bundle manifest` / `bundle verify`: identity and integrity of the release
//! bundle this binary belongs to.

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
    /// Check that the bundle this binary belongs to is complete and unchanged.
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

/// Check that the shipped catalog is exactly the one compiled into this
/// binary, then the bundle's recorded identity. The binary never reads the
/// shipped copy; it exists for the application's provenance records.
fn verify_bundle(root: &Path) -> Result<String> {
    catalog::load_catalog().map_err(invalid)?;
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
    Ok(BundleManifest::verify(root)?.content_digest)
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

/// Identity of the running bundle, verified; development builds, which have
/// no manifest, have none.
#[cfg_attr(feature = "postgres", allow(dead_code))]
pub(super) fn verified_bundle_digest() -> Result<Option<String>> {
    let root = bundle_root().map_err(invalid)?;
    if !root.join(MANIFEST_FILE).exists() {
        return Ok(None);
    }
    verify_bundle(&root).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A bundle that ships the compiled catalog changed by `edit`, with a
    /// manifest written afterwards, so only the comparison with the compiled
    /// catalog can fail.
    fn bundle_with(edit: impl FnOnce(&Path)) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
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
        assert_eq!(
            verify_bundle(root.path()).unwrap(),
            BundleManifest::load(root.path()).unwrap().content_digest
        );
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
            let error = verify_bundle(root.path()).unwrap_err();
            assert!(format!("{error:#}").contains("shipped"), "{error:#}");
        }
    }
}
