//! `bundle manifest` / `bundle verify`: identity and integrity of the release
//! bundle this binary belongs to.

use super::{load_atlas_tool_lock, verify_atlas_binary, verify_atlas_sum};
use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use grpc_admin::db_migrate::{
    catalog,
    local::{
        bundle::BundleManifest,
        output::{ErrorCode, Resolution, classify},
    },
};
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
pub(super) async fn run_bundle(command: BundleCommand) -> i32 {
    let name = match command {
        BundleCommand::Manifest(_) => "bundle_manifest",
        BundleCommand::Verify => "bundle_verify",
    };
    match bundle_result(command).await {
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

async fn bundle_result(command: BundleCommand) -> Result<String> {
    let root = bundle_root()?;
    match command {
        BundleCommand::Manifest(args) => {
            Ok(
                BundleManifest::write(&root, &args.source_revision, args.source_dirty)?
                    .content_digest,
            )
        }
        BundleCommand::Verify => verify_bundle(&root).await,
    }
}

/// The directory that holds this binary and its `atlas/` artifacts.
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

/// Check everything a release bundle must ship, then its recorded identity.
async fn verify_bundle(root: &Path) -> Result<String> {
    let backend = if cfg!(feature = "postgres") {
        "postgres"
    } else {
        "sqlite"
    };
    let artifact_root = root.join("atlas");
    catalog::load_catalog().map_err(invalid)?;
    for (name, compiled) in catalog::compiled_catalog_files() {
        let shipped = std::fs::read_to_string(artifact_root.join(name))
            .with_context(|| format!("reading the shipped {name}"))
            .map_err(invalid)?;
        if shipped != compiled {
            return Err(invalid(anyhow::anyhow!(
                "the shipped {name} differs from the catalog compiled into this binary"
            )));
        }
    }
    verify_atlas_sum(&artifact_root, backend).map_err(invalid)?;
    let lock = load_atlas_tool_lock(&artifact_root).map_err(invalid)?;
    verify_atlas_binary(&artifact_root.join("bin").join("atlas"), &lock)
        .await
        .map_err(invalid)?;
    let license = artifact_root.join("licenses").join("ATLAS_LICENSE");
    if std::fs::metadata(&license).map_or(0, |metadata| metadata.len()) == 0 {
        return Err(invalid(anyhow::anyhow!(
            "the Atlas license is missing from the bundle"
        )));
    }
    Ok(BundleManifest::verify(root)?.content_digest)
}

/// Identity of the running bundle; development builds have none.
#[cfg_attr(feature = "postgres", allow(dead_code))]
pub(super) fn running_bundle_digest() -> Option<String> {
    bundle_root()
        .ok()
        .and_then(|root| BundleManifest::load(&root).ok())
        .map(|manifest| manifest.content_digest)
}
