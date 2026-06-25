use clap::{ArgAction, Args, Parser, Subcommand};
use std::path::PathBuf;

#[cfg(feature = "summarize-after")]
const SUMMARIZE_INPUT_GROUP: &str = "summarize_input";
#[cfg(feature = "summarize-after")]
const SUMMARIZE_WORKFLOW_ARG: &str = "summarize_workflow";
#[cfg(feature = "personality-after")]
const PERSONALITY_INPUT_GROUP: &str = "personality_input";
#[cfg(feature = "personality-after")]
const PERSONALITY_WORKFLOW_ARG: &str = "personality_workflow";

/// Maximum byte length per label, matching the `thread_label.label`
/// `VARCHAR(512)` constraint in the DB schema. CLI labels (`--labels`) are
/// surfaced as parse errors when they exceed this limit, rather than
/// silently truncated, because user-typed identifiers must be reproducible
/// across environments. See spec §3.2 / §5.3.6.
const MAX_LABEL_BYTES: usize = 512;

/// Default `PlainArgs::source_name`. Exposed so the runtime warning in
/// `main.rs` can detect the un-customised case from the same source of
/// truth as clap's `default_value`.
pub const DEFAULT_PLAIN_SOURCE_NAME: &str = "plain";

#[derive(Parser, Debug)]
#[command(
    name = "memories-import",
    about = "Import agent chat history (Claude Code, Codex CLI, plain text trees) into memories",
    subcommand_required = true,
    arg_required_else_help = true
)]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalArgs,

    #[command(subcommand)]
    pub command: Subcmd,
}

impl Cli {
    pub fn requires_user_id(&self) -> bool {
        matches!(
            self.command,
            Subcmd::ClaudeCode(_) | Subcmd::Codex(_) | Subcmd::Plain(_)
        )
    }

    /// Resolve `--user-id` against the clap-validated state. The flag
    /// is declared as `Option<i64>` only because clap rejects
    /// `global = true` together with `required = true`; treat a
    /// missing value as a clap usage error so the binary exits with
    /// the same code (2) and rendering as any other missing required
    /// argument.
    pub fn validate_user_id(&self) -> Result<i64, clap::Error> {
        if !self.requires_user_id() {
            return Ok(0);
        }
        match self.global.user_id {
            Some(v) => Ok(v),
            None => {
                use clap::CommandFactory;
                Err(Self::command().error(
                    clap::error::ErrorKind::MissingRequiredArgument,
                    "the following required arguments were not provided:\n  --user-id <USER_ID>",
                ))
            }
        }
    }
}

/// Global options shared by every subcommand. All entries use
/// `#[arg(global = true)]` so they can appear before or after the
/// subcommand on the command line (spec §2.3.1).
#[derive(Args, Debug, Clone)]
pub struct GlobalArgs {
    /// User ID for imported data. Required, but declared as
    /// `Option<i64>` because clap forbids `global = true` together
    /// with `required = true`. Missing values are rejected via
    /// `Cli::validate_user_id` with a clap-style usage error so the
    /// exit code and help-on-error match other clap arguments.
    #[arg(short = 'u', long, global = true)]
    pub user_id: Option<i64>,

    /// Only import entries after this timestamp (ISO 8601)
    #[arg(short = 's', long, global = true)]
    pub since: Option<String>,

    /// Margin (seconds) subtracted from `--since` before comparing to a
    /// session file's mtime. Files whose mtime is older than
    /// `since - margin` are parse-skipped at the source level (spec
    /// `agent-chat-import-incremental-spec.md` §1.2). The margin keeps
    /// files that may still be in the middle of an append safe.
    #[arg(long, global = true, value_name = "SECONDS", default_value_t = 60)]
    pub mtime_margin_seconds: u64,

    /// Disable the session-level mtime filter, even when `--since` is
    /// set. Use this on filesystems where mtime is unreliable (NFS,
    /// some cloud storage backends) so every session is fully parsed
    /// regardless of file metadata.
    #[arg(long, global = true)]
    pub no_mtime_filter: bool,

    /// Additional labels (comma-separated). May be specified multiple
    /// times; values are flattened and deduplicated. Each resulting
    /// label must be <= 512 bytes (matches thread_label.label
    /// VARCHAR(512)). Spec §3.2 / §5.3.6 末尾.
    #[arg(short = 'l', long, global = true, action = ArgAction::Append, value_parser = parse_labels_csv)]
    pub labels: Vec<Vec<String>>,

    /// Dry run (no actual import)
    #[arg(short = 'n', long, global = true)]
    pub dry_run: bool,

    /// Verbose output (sets RUST_LOG=debug if not already set)
    #[arg(short = 'v', long, global = true)]
    pub verbose: bool,

    /// Progress log interval (number of memories)
    #[arg(short = 'b', long, global = true, default_value = "100")]
    pub batch_size: usize,

    /// gRPC endpoint of the memories `grpc-admin` server (e.g.
    /// `http://localhost:9010`). Required unless `--dry-run` is given.
    #[arg(long, global = true, value_name = "URL")]
    pub server_url: Option<String>,

    /// Per-RPC timeout in seconds.
    #[arg(long, global = true, value_name = "SECONDS", default_value_t = 600)]
    pub server_timeout_sec: u64,

    /// Maximum attempts (including the initial one) for a single RPC
    /// before its error bubbles up. Retries are gated on transient gRPC
    /// codes (`Unavailable`, `DeadlineExceeded`, `ResourceExhausted`,
    /// PostgreSQL serialization / deadlock SQLSTATEs). Set to 1 — or
    /// pass `--no-retry` — to restore the legacy single-attempt behaviour.
    #[arg(long, global = true, value_name = "N", default_value_t = 3)]
    pub server_retry_max: u32,

    /// Base of the exponential backoff between retries, in milliseconds.
    /// Attempt N waits `min(base * 2^(N-1), cap)` ms before retrying,
    /// further multiplied by a uniform jitter in `[1.0, 1.0 + ratio)`.
    #[arg(long, global = true, value_name = "MS", default_value_t = 1000)]
    pub server_retry_base_ms: u64,

    /// Upper bound on the per-retry backoff in milliseconds. Caps the
    /// exponential ramp so a string of failures doesn't make the final
    /// attempt wait an unreasonably long time.
    #[arg(long, global = true, value_name = "MS", default_value_t = 30_000)]
    pub server_retry_cap_ms: u64,

    /// Jitter ratio applied to each backoff. 0.0 disables jitter; 0.25
    /// spreads each delay uniformly over `[delay, delay * 1.25)`.
    #[arg(long, global = true, value_name = "RATIO", default_value_t = 0.25)]
    pub server_retry_jitter_ratio: f64,

    /// Disable RPC retries entirely. Equivalent to
    /// `--server-retry-max 1` but kept as a flag so debug runs don't
    /// have to remember the magic number.
    #[arg(long, global = true)]
    pub no_retry: bool,

    /// Maximum entries packed into a single AddMemoriesBatch call. The
    /// 200 default is intentionally smaller than the protocol cap so a
    /// cnpg transaction completes in a few seconds even under
    /// contention — the back-pressure that keeps the client honest
    /// when the server's INSERT queue grows. Use 500 (the legacy
    /// value) to restore the old behaviour.
    #[arg(long, global = true, value_name = "N", default_value_t = 200)]
    pub chunk_max_entries: usize,

    /// Soft cap on the prost-encoded size of a single
    /// AddMemoriesBatchRequest, in bytes. The default of 4 MiB keeps
    /// each batch comfortably below tonic's 16 MiB frame limit while
    /// also keeping cnpg transactions short. Pass `12582912`
    /// (12 MiB) to restore the legacy behaviour.
    #[arg(long, global = true, value_name = "BYTES", default_value_t = 4 * 1024 * 1024)]
    pub chunk_max_bytes: usize,

    /// Optional path to a self-signed CA certificate (PEM) used when
    /// `--server-url` starts with `https://`.
    #[arg(long, global = true, value_name = "PATH")]
    pub server_tls_ca: Option<PathBuf>,

    /// Bearer token attached to every request as `authorization`
    /// metadata. Phase 1 only sets up the wire — server-side enforcement
    /// is out of scope.
    #[arg(long, global = true, value_name = "TOKEN")]
    pub auth_token: Option<String>,

    /// Output language for post-import generation workflows.
    #[arg(long, global = true, value_name = "LANG", value_parser = parse_output_language)]
    pub output_language: Option<String>,

    /// JSON file holding input for thread-summary-batch.yaml. The
    /// file's `user_id` and (when --since is given) `updated_after_ms`
    /// fields are overridden from import options. Mutually exclusive
    /// with --summarize-after-json. Requires --summarize-workflow.
    #[cfg(feature = "summarize-after")]
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        group = SUMMARIZE_INPUT_GROUP,
        requires = SUMMARIZE_WORKFLOW_ARG
    )]
    pub summarize_after_file: Option<PathBuf>,

    /// Inline JSON for thread-summary-batch.yaml. Mutually exclusive
    /// with --summarize-after-file. Requires --summarize-workflow.
    #[cfg(feature = "summarize-after")]
    #[arg(
        long,
        global = true,
        value_name = "JSON",
        group = SUMMARIZE_INPUT_GROUP,
        requires = SUMMARIZE_WORKFLOW_ARG
    )]
    pub summarize_after_json: Option<String>,

    /// Path to thread-summary-batch.yaml. Required when any
    /// --summarize-after-* option is given.
    #[cfg(feature = "summarize-after")]
    #[arg(long, global = true, value_name = "PATH")]
    pub summarize_workflow: Option<PathBuf>,

    /// Optional jobworkerp channel for the summarize workflow.
    #[cfg(feature = "summarize-after")]
    #[arg(long, global = true, value_name = "CHANNEL")]
    pub summarize_channel: Option<String>,

    /// Job timeout (seconds) for the summarize workflow. The
    /// jobworkerp default of 1200s is too short for batch summarization
    /// across many threads — the workflow YAML itself permits up to
    /// 24h, so default to 86400s here as well.
    #[cfg(feature = "summarize-after")]
    #[arg(long, global = true, value_name = "SECONDS", default_value = "86400")]
    pub summarize_timeout_sec: u32,

    /// JSON file holding input for thread-personality-batch.yaml. The
    /// file's `user_id` and (when --since is given) `updated_after_ms`
    /// fields are overridden from import options. Mutually exclusive
    /// with --extract-personality-after-json. Requires
    /// --personality-workflow.
    #[cfg(feature = "personality-after")]
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        group = PERSONALITY_INPUT_GROUP,
        requires = PERSONALITY_WORKFLOW_ARG
    )]
    pub extract_personality_after_file: Option<PathBuf>,

    /// Inline JSON for thread-personality-batch.yaml. Mutually exclusive
    /// with --extract-personality-after-file. Requires
    /// --personality-workflow.
    #[cfg(feature = "personality-after")]
    #[arg(
        long,
        global = true,
        value_name = "JSON",
        group = PERSONALITY_INPUT_GROUP,
        requires = PERSONALITY_WORKFLOW_ARG
    )]
    pub extract_personality_after_json: Option<String>,

    /// Path to thread-personality-batch.yaml. Required when any
    /// --extract-personality-after-* option is given.
    #[cfg(feature = "personality-after")]
    #[arg(long, global = true, value_name = "PATH")]
    pub personality_workflow: Option<PathBuf>,

    /// Optional jobworkerp channel for the personality workflow.
    #[cfg(feature = "personality-after")]
    #[arg(long, global = true, value_name = "CHANNEL")]
    pub personality_channel: Option<String>,

    /// Job timeout (seconds) for the personality workflow. Same
    /// rationale as --summarize-timeout-sec — batch extraction across
    /// many threads benefits from a long ceiling.
    #[cfg(feature = "personality-after")]
    #[arg(long, global = true, value_name = "SECONDS", default_value = "86400")]
    pub personality_timeout_sec: u32,
}

#[derive(Subcommand, Debug)]
pub enum Subcmd {
    /// Upsert language-specific generation workflow workers.
    UpsertGenerationWorkers(UpsertGenerationWorkersArgs),
    /// Import Claude Code JSONL transcripts.
    ClaudeCode(ClaudeCodeArgs),
    /// Import OpenAI Codex CLI rollout JSONL.
    Codex(CodexArgs),
    /// Import a directory tree of `.md` / `.txt` files.
    Plain(PlainArgs),
}

#[derive(Args, Debug, Clone)]
pub struct UpsertGenerationWorkersArgs {
    /// Feature to register. Use `all` to register every supported feature.
    #[arg(long, default_value = "all", value_name = "FEATURE")]
    pub feature: String,

    /// Language to register. Use `all` to register ja and en.
    #[arg(long, default_value = "all", value_name = "LANG")]
    pub language: String,

    /// Channel assigned to the generated WORKFLOW workers.
    #[arg(long, default_value = "workflow_lang", value_name = "CHANNEL")]
    pub channel: String,

    /// jobworkerp connection timeout in seconds.
    #[arg(long, default_value_t = 30, value_name = "SECONDS")]
    pub timeout_sec: u32,

    /// agent-chat-import crate directory containing workers/. Defaults to this checkout.
    #[arg(long, value_name = "PATH")]
    pub repo_root: Option<PathBuf>,
}

#[derive(Args, Debug, Clone)]
#[command(group(clap::ArgGroup::new("claude_input").required(true).multiple(false)))]
pub struct ClaudeCodeArgs {
    /// Single JSONL session file to import
    #[arg(short = 'f', long, group = "claude_input")]
    pub session_file: Option<PathBuf>,

    /// Project directory containing JSONL files
    #[arg(short = 'p', long, group = "claude_input")]
    pub project_dir: Option<PathBuf>,

    /// Import all projects under <claude-dir>/projects/
    #[arg(long, group = "claude_input")]
    pub all_projects: bool,

    /// Claude configuration directory
    #[arg(short = 'd', long, default_value = "~/.claude")]
    pub claude_dir: PathBuf,

    /// Entry types to import (comma-separated). Default keeps every
    /// kind defined by the canonical schema so individual users can
    /// decide what to surface; `metadata.kind` is set per record.
    /// Spec §3.3 / §4.2.2.1.
    #[arg(
        short = 't',
        long,
        default_value = "user,assistant,tool_call,tool_output,system,reasoning,attachment"
    )]
    pub include_types: String,

    /// Base paths to strip from `path:` labels (comma-separated).
    /// When `cwd` is under any of these, the corresponding `path:` label
    /// becomes a relative path. Multiple prefixes are tried longest-first.
    #[arg(short = 'P', long, value_name = "PATHS")]
    pub strip_path_prefix: Option<String>,

    /// Whitelist policy for `type=attachment` JSONL events. The Claude
    /// Code transcript surfaces 14+ attachment subtypes; `default` only
    /// keeps the high-value ones (`task_reminder`, `diagnostics`,
    /// `edited_text_file`, `nested_memory`). `all` admits every
    /// subtype, `none` drops them altogether, and a comma-separated
    /// list (e.g. `task_reminder,skill_listing`) declares an explicit
    /// whitelist. Spec §4.2.2.4 / §3.3.
    #[arg(
        long,
        value_name = "POLICY",
        default_value = "default",
        help = "default | all | none | comma-separated subtype whitelist"
    )]
    pub attachment_subtypes: String,
}

impl ClaudeCodeArgs {
    pub fn include_types_set(&self) -> std::collections::HashSet<String> {
        self.include_types
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Parse the `--attachment-subtypes` policy string into the typed
    /// representation consumed by `source/claude_code.rs`. The CLI keeps
    /// the raw string to retain `clap`'s default-value semantics.
    pub fn attachment_subtypes_policy(
        &self,
    ) -> Result<crate::source::claude_code::AttachmentSubtypePolicy, String> {
        crate::source::claude_code::AttachmentSubtypePolicy::from_cli(&self.attachment_subtypes)
    }

    pub fn path_prefixes(&self) -> Vec<String> {
        self.strip_path_prefix
            .as_deref()
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().trim_end_matches('/').to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn resolved_claude_dir(&self) -> PathBuf {
        expand_tilde(&self.claude_dir)
    }
}

#[derive(Args, Debug, Clone)]
#[command(group(clap::ArgGroup::new("codex_input").required(true).multiple(false)))]
pub struct CodexArgs {
    /// Single rollout JSONL file to import.
    #[arg(short = 'f', long, group = "codex_input")]
    pub session_file: Option<PathBuf>,

    /// Restrict to one day's directory under
    /// `<codex-dir>/sessions/<yyyy>/<mm>/<dd>/`.
    #[arg(long, group = "codex_input")]
    pub day_dir: Option<PathBuf>,

    /// Walk every rollout under `<codex-dir>/sessions/`.
    #[arg(long, group = "codex_input")]
    pub all_sessions: bool,

    /// Codex configuration directory.
    #[arg(short = 'd', long, default_value = "~/.codex")]
    pub codex_dir: PathBuf,

    /// Entry types to import (comma-separated). Default keeps every
    /// observed record class so individual users can decide what to
    /// surface in search; `metadata.kind` is set per record.
    /// `attachment` covers `input_image` content blocks (§4.2.2.4).
    /// Spec §3.4.
    #[arg(
        short = 't',
        long,
        default_value = "user,assistant,tool_call,tool_output,system,reasoning,attachment"
    )]
    pub include_types: String,

    /// Base paths to strip from `path:` labels (comma-separated).
    /// Mirrors claude-code's `-P` semantics. Spec §3.4.
    #[arg(short = 'P', long, value_name = "PATHS")]
    pub strip_path_prefix: Option<String>,

    /// Persist the full `reasoning.encrypted_content` blob in memory
    /// metadata. Off by default: the blob can be large and is
    /// considered sensitive (it lands in DB / dumps / backups), so we
    /// store only its SHA-256 + byte size unless the operator opts in.
    #[arg(long)]
    pub include_encrypted_reasoning: bool,

    /// Deprecated: encrypted reasoning is now excluded by default.
    /// Accepted as a no-op so existing scripts keep working.
    #[arg(long, hide = true)]
    pub exclude_encrypted_reasoning: bool,

    /// Resolve `function_call_output.parent_ids` to the matching
    /// `function_call` (same `call_id`). On by default; pair with
    /// `--no-link-tool-calls` to disable. The two flags share a
    /// `BoolishValueParser`-style override via clap's `overrides_with`
    /// — see `effective_link_tool_calls()` for resolution.
    #[arg(long, action = ArgAction::SetTrue, overrides_with = "no_link_tool_calls")]
    #[allow(dead_code)] // Resolved via effective_link_tool_calls()
    pub link_tool_calls: bool,
    /// Disable tool-call linkage. See `--link-tool-calls`.
    #[arg(long, action = ArgAction::SetTrue, overrides_with = "link_tool_calls")]
    #[allow(dead_code)] // Resolved via effective_link_tool_calls()
    pub no_link_tool_calls: bool,
}

impl CodexArgs {
    pub fn include_types_set(&self) -> std::collections::HashSet<String> {
        self.include_types
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect()
    }

    pub fn path_prefixes(&self) -> Vec<String> {
        self.strip_path_prefix
            .as_deref()
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().trim_end_matches('/').to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn resolved_codex_dir(&self) -> PathBuf {
        expand_tilde(&self.codex_dir)
    }

    /// Resolve `--link-tool-calls` / `--no-link-tool-calls` into a
    /// single bool. Default is `true` (spec §3.4); `--no-link-tool-calls`
    /// wins when present.
    pub fn effective_link_tool_calls(&self) -> bool {
        if self.no_link_tool_calls {
            return false;
        }
        true
    }
}

#[derive(Args, Debug, Clone)]
pub struct PlainArgs {
    /// Root directory of the tree to import (recursive).
    #[arg(short = 'r', long)]
    pub root: PathBuf,

    /// Source name. Used as the prefix of every channel /
    /// `external_id` / `agent:<source-name>` label / metadata.source
    /// field. Spec §5.3.7: must match `^[a-z0-9_-]{1,32}$`.
    ///
    /// WARNING: vaults with the same `--source-name` share an
    /// identifier namespace. Importing two different vaults under the
    /// same name lets identical relative paths collide on the same
    /// thread. Use the default `plain` only when you have a single
    /// vault; for multiple vaults pick distinct names like
    /// `obsidian-work`, `obsidian-private`, `notes-archive`.
    #[arg(long, default_value = DEFAULT_PLAIN_SOURCE_NAME, value_parser = parse_source_name)]
    pub source_name: String,

    /// Comma-separated list of extensions to import (case-insensitive,
    /// leading dot allowed). Spec §3.5.
    #[arg(long, default_value = "md,txt")]
    pub ext: String,

    /// Glob patterns to exclude. May appear multiple times; OR-combined.
    /// `gitignore`-like (e.g. `.git/**`). Spec §5.3.1.
    #[arg(long = "exclude-glob", value_name = "PATTERN", action = ArgAction::Append)]
    pub exclude_glob: Vec<String>,

    /// Follow symlinks. Off by default to avoid loops.
    #[arg(long, action = ArgAction::SetTrue, overrides_with = "no_follow_symlinks")]
    #[allow(dead_code)]
    pub follow_symlinks: bool,
    /// Disable symlink following.
    #[arg(long, action = ArgAction::SetTrue, overrides_with = "follow_symlinks")]
    #[allow(dead_code)]
    pub no_follow_symlinks: bool,

    /// Thread grouping strategy. Spec §5.3.4.
    #[arg(long, default_value = "per-file", value_parser = parse_thread_strategy)]
    pub thread_strategy: crate::source::plain::ThreadStrategy,

    /// Encoding mode for raw file bytes. Spec §3.5.
    #[arg(long, default_value = "utf8-lossy", value_parser = parse_encoding)]
    pub encoding: crate::source::plain::EncodingMode,

    /// Parse YAML frontmatter from `.md` files. Default on; pair with
    /// `--no-frontmatter` to disable.
    #[arg(long, action = ArgAction::SetTrue, overrides_with = "no_frontmatter")]
    #[allow(dead_code)]
    pub frontmatter: bool,
    /// Disable frontmatter parsing.
    #[arg(long, action = ArgAction::SetTrue, overrides_with = "frontmatter")]
    #[allow(dead_code)]
    pub no_frontmatter: bool,

    /// Skip files larger than this many bytes (warn).
    #[arg(long, value_name = "N", default_value_t = 1_048_576)]
    pub max_file_size_bytes: u64,

    /// Comma-separated list of frontmatter keys to lift into thread
    /// labels. Only effective with `--thread-strategy=per-file`. Spec
    /// §5.3.6.
    #[arg(long)]
    pub label_from_frontmatter: Option<String>,

    /// Respect `.gitignore` (via the `ignore` crate). Default on.
    #[arg(long, action = ArgAction::SetTrue, overrides_with = "no_respect_gitignore")]
    #[allow(dead_code)]
    pub respect_gitignore: bool,
    /// Ignore `.gitignore` files.
    #[arg(long, action = ArgAction::SetTrue, overrides_with = "respect_gitignore")]
    #[allow(dead_code)]
    pub no_respect_gitignore: bool,

    /// Base paths to strip from `path:` labels (comma-separated).
    /// Mirrors claude-code's `-P` semantics.
    #[arg(short = 'P', long, value_name = "PATHS")]
    pub strip_path_prefix: Option<String>,

    /// Delete memories whose source file no longer exists on disk.
    /// Identifies them via `external_id` set diff (`--source-name:`
    /// prefix on the server, `metadata.path` for the on-disk match).
    /// Phase A: rename / move are NOT detected — the old memory is
    /// deleted and a new one is added under the new path.
    ///
    /// Vault invariant: keep `--source-name` AND `--root` stable for a
    /// given vault. Re-rooting changes every `external_id` and prunes
    /// the entire prior import. See README "ファイル移動・削除の取り扱い".
    #[arg(long, action = ArgAction::SetTrue)]
    pub prune_missing: bool,

    /// When prune leaves a thread with zero memories, delete the empty
    /// thread too. Default true; pair with `--no-prune-orphan-threads`
    /// to keep empty threads (e.g. when external workflows expect them).
    #[arg(long, action = ArgAction::SetTrue, overrides_with = "no_prune_orphan_threads")]
    #[allow(dead_code)]
    pub prune_orphan_threads: bool,
    #[arg(long, action = ArgAction::SetTrue, overrides_with = "prune_orphan_threads")]
    #[allow(dead_code)]
    pub no_prune_orphan_threads: bool,

    /// Suppress the interactive `Continue? [y/N]` prompt that prune
    /// shows by default. Required for non-TTY runs (cron, CI) — the
    /// importer aborts rather than silently proceed when it detects no
    /// stdin TTY and `--no-interactive` is missing.
    #[arg(long, action = ArgAction::SetTrue)]
    pub no_interactive: bool,
}

impl PlainArgs {
    pub fn path_prefixes(&self) -> Vec<String> {
        self.strip_path_prefix
            .as_deref()
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().trim_end_matches('/').to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Resolve `--frontmatter` / `--no-frontmatter` into a single bool.
    /// Default `true`; `--no-frontmatter` wins when present.
    pub fn effective_frontmatter(&self) -> bool {
        !self.no_frontmatter
    }

    pub fn effective_respect_gitignore(&self) -> bool {
        !self.no_respect_gitignore
    }

    /// Default `false` (avoid symlink loops); `--follow-symlinks` wins
    /// when present. Pair flag resolution mirrors the other plain
    /// boolean pairs.
    pub fn effective_follow_symlinks(&self) -> bool {
        self.follow_symlinks && !self.no_follow_symlinks
    }

    /// Default `true`; `--no-prune-orphan-threads` wins when present.
    pub fn effective_prune_orphan_threads(&self) -> bool {
        !self.no_prune_orphan_threads
    }
}

fn parse_source_name(raw: &str) -> Result<String, String> {
    if raw.is_empty() {
        return Err("--source-name must not be empty".to_string());
    }
    if raw.len() > 32 {
        return Err(format!(
            "--source-name must be at most 32 bytes ({} bytes given)",
            raw.len()
        ));
    }
    if !raw
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
    {
        return Err(format!(
            "--source-name '{raw}' must match ^[a-z0-9_-]{{1,32}}$"
        ));
    }
    Ok(raw.to_string())
}

fn parse_thread_strategy(raw: &str) -> Result<crate::source::plain::ThreadStrategy, String> {
    raw.parse()
}

fn parse_encoding(raw: &str) -> Result<crate::source::plain::EncodingMode, String> {
    raw.parse()
}

fn parse_output_language(raw: &str) -> Result<String, String> {
    // Keep CLI parsing available even when summarize-after is disabled.
    // Dispatch resolvers reuse the same common whitelist.
    let trimmed = raw.trim();
    if crate::common::language::SUPPORTED_LANGUAGES.contains(&trimmed) {
        Ok(trimmed.to_string())
    } else {
        Err(format!(
            "--output-language must be one of {} (got `{trimmed}`)",
            crate::common::language::SUPPORTED_LANGUAGES.join(",")
        ))
    }
}

impl GlobalArgs {
    /// Parse --labels into a deduplicated, validated flat vector.
    pub fn extra_labels(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for occurrence in &self.labels {
            for label in occurrence {
                if label.is_empty() {
                    continue;
                }
                if seen.insert(label.clone()) {
                    out.push(label.clone());
                }
            }
        }
        out
    }

    pub fn since_millis(&self) -> anyhow::Result<Option<i64>> {
        match &self.since {
            None => Ok(None),
            Some(s) => {
                let dt = chrono::DateTime::parse_from_rfc3339(s)
                    .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ"))
                    .map_err(|e| anyhow::anyhow!("Invalid --since timestamp '{}': {}", s, e))?;
                Ok(Some(dt.timestamp_millis()))
            }
        }
    }

    /// Effective threshold for the source-side mtime filter. Returns
    /// `None` when the filter is inapplicable (no `--since` given or
    /// `--no-mtime-filter` requested), in which case every session
    /// MUST be fully parsed. Otherwise returns `since_millis - margin`,
    /// where `margin = mtime_margin_seconds * 1000`. Sources should
    /// short-circuit `read_session` when a file's mtime is strictly
    /// older than this value.
    pub fn since_millis_with_margin(&self) -> anyhow::Result<Option<i64>> {
        if self.no_mtime_filter {
            return Ok(None);
        }
        Ok(self.since_millis()?.map(|ms| {
            let margin_ms = (self.mtime_margin_seconds as i64).saturating_mul(1000);
            ms.saturating_sub(margin_ms)
        }))
    }

    #[cfg(feature = "summarize-after")]
    pub fn summarize_after_raw(&self) -> anyhow::Result<Option<String>> {
        if let Some(path) = &self.summarize_after_file {
            let raw = std::fs::read_to_string(path).map_err(|e| {
                anyhow::anyhow!("read --summarize-after-file {}: {e}", path.display())
            })?;
            Ok(Some(raw))
        } else if let Some(s) = &self.summarize_after_json {
            Ok(Some(s.clone()))
        } else {
            Ok(None)
        }
    }

    #[cfg(feature = "personality-after")]
    pub fn extract_personality_after_raw(&self) -> anyhow::Result<Option<String>> {
        if let Some(path) = &self.extract_personality_after_file {
            let raw = std::fs::read_to_string(path).map_err(|e| {
                anyhow::anyhow!(
                    "read --extract-personality-after-file {}: {e}",
                    path.display()
                )
            })?;
            Ok(Some(raw))
        } else if let Some(s) = &self.extract_personality_after_json {
            Ok(Some(s.clone()))
        } else {
            Ok(None)
        }
    }
}

fn expand_tilde(path: &std::path::Path) -> PathBuf {
    let s = path.to_string_lossy();
    if let Some(rest) = s.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(rest);
    }
    path.to_path_buf()
}

/// Parse a single `--labels` occurrence (CSV) into a vector and validate
/// each label's byte length. Surfaced via `clap`'s `value_parser` so
/// errors include the offending occurrence and per-label byte count.
fn parse_labels_csv(raw: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for (idx, part) in raw.split(',').enumerate() {
        let label = part.trim();
        if label.is_empty() {
            continue;
        }
        let bytes = label.len();
        if bytes > MAX_LABEL_BYTES {
            return Err(format!(
                "--labels[{idx}]: label length {bytes} bytes exceeds {MAX_LABEL_BYTES}-byte limit (DB column thread_label.label is VARCHAR(512))"
            ));
        }
        out.push(label.to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once(&"memories-import").chain(args.iter()))
    }

    #[test]
    fn subcommand_post_position() {
        let cli = parse(&["claude-code", "-u", "1", "--all-projects"]).unwrap();
        assert_eq!(cli.global.user_id, Some(1));
        match cli.command {
            Subcmd::ClaudeCode(args) => {
                assert!(args.all_projects);
                assert!(args.session_file.is_none());
            }
            other => panic!("expected ClaudeCode, got {other:?}"),
        }
    }

    #[test]
    fn subcommand_pre_position() {
        let cli = parse(&["-u", "1", "claude-code", "--all-projects"]).unwrap();
        assert_eq!(cli.global.user_id, Some(1));
        match cli.command {
            Subcmd::ClaudeCode(args) => assert!(args.all_projects),
            other => panic!("expected ClaudeCode, got {other:?}"),
        }
    }

    #[test]
    fn codex_subcommand_post_position() {
        let cli = parse(&["codex", "-u", "1", "--all-sessions"]).unwrap();
        assert_eq!(cli.global.user_id, Some(1));
        match cli.command {
            Subcmd::Codex(args) => {
                assert!(args.all_sessions);
                assert!(
                    args.effective_link_tool_calls(),
                    "default link_tool_calls=true"
                );
                assert!(!args.exclude_encrypted_reasoning);
            }
            other => panic!("expected Codex, got {other:?}"),
        }
    }

    #[test]
    fn codex_no_link_tool_calls_pair_flag() {
        let cli = parse(&["-u", "1", "codex", "--all-sessions", "--no-link-tool-calls"]).unwrap();
        match cli.command {
            Subcmd::Codex(args) => assert!(!args.effective_link_tool_calls()),
            other => panic!("expected Codex, got {other:?}"),
        }
    }

    #[test]
    fn plain_subcommand_basic() {
        let cli = parse(&["-u", "1", "plain", "--root", "/tmp/v"]).unwrap();
        match cli.command {
            Subcmd::Plain(args) => {
                assert_eq!(args.root, std::path::PathBuf::from("/tmp/v"));
                assert_eq!(args.source_name, "plain");
                assert!(args.effective_frontmatter());
                assert!(args.effective_respect_gitignore());
            }
            other => panic!("expected Plain, got {other:?}"),
        }
    }

    #[test]
    fn plain_root_required() {
        let err = parse(&["-u", "1", "plain"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn upsert_generation_workers_does_not_require_user_id() {
        let cli = parse(&["upsert-generation-workers"]).unwrap();
        assert!(!cli.requires_user_id());
        assert_eq!(cli.validate_user_id().unwrap(), 0);
        match cli.command {
            Subcmd::UpsertGenerationWorkers(args) => {
                assert_eq!(args.feature, "all");
                assert_eq!(args.language, "all");
                assert_eq!(args.channel, "workflow_lang");
            }
            other => panic!("expected UpsertGenerationWorkers, got {other:?}"),
        }
    }

    #[test]
    fn import_subcommands_still_require_user_id() {
        let cli = parse(&["claude-code", "--all-projects"]).unwrap();
        assert!(cli.requires_user_id());
        let err = cli.validate_user_id().unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn plain_source_name_validation_rejects_empty() {
        let err =
            parse(&["-u", "1", "plain", "--root", "/tmp/v", "--source-name", ""]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn plain_source_name_validation_rejects_uppercase() {
        let err = parse(&[
            "-u",
            "1",
            "plain",
            "--root",
            "/tmp/v",
            "--source-name",
            "Obsidian",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn plain_source_name_validation_rejects_too_long() {
        let long = "a".repeat(33);
        let err = parse(&[
            "-u",
            "1",
            "plain",
            "--root",
            "/tmp/v",
            "--source-name",
            &long,
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn plain_source_name_accepts_hyphens_underscores_digits() {
        let cli = parse(&[
            "-u",
            "1",
            "plain",
            "--root",
            "/tmp/v",
            "--source-name",
            "obsidian-private_2",
        ])
        .unwrap();
        match cli.command {
            Subcmd::Plain(args) => assert_eq!(args.source_name, "obsidian-private_2"),
            other => panic!("expected Plain, got {other:?}"),
        }
    }

    #[test]
    fn plain_no_frontmatter_pair_flag() {
        let cli = parse(&["-u", "1", "plain", "--root", "/tmp/v", "--no-frontmatter"]).unwrap();
        match cli.command {
            Subcmd::Plain(args) => assert!(!args.effective_frontmatter()),
            other => panic!("expected Plain, got {other:?}"),
        }
    }

    #[test]
    fn codex_explicit_link_tool_calls_overrides_no() {
        // post-positional: --no-link-tool-calls then --link-tool-calls.
        let cli = parse(&[
            "-u",
            "1",
            "codex",
            "--all-sessions",
            "--no-link-tool-calls",
            "--link-tool-calls",
        ])
        .unwrap();
        match cli.command {
            Subcmd::Codex(args) => assert!(args.effective_link_tool_calls()),
            other => panic!("expected Codex, got {other:?}"),
        }
    }

    #[test]
    fn codex_input_required() {
        let err = parse(&["-u", "1", "codex"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn codex_input_exclusive() {
        let err = parse(&["-u", "1", "codex", "--all-sessions", "-f", "/tmp/x.jsonl"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn subcommand_required() {
        let err = parse(&["-u", "1"]).unwrap_err();
        // clap returns DisplayHelpOnMissingArgumentOrSubcommand for missing subcommand
        assert!(matches!(
            err.kind(),
            clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
                | clap::error::ErrorKind::MissingSubcommand
        ));
    }

    #[test]
    fn empty_args_show_help() {
        let err = parse(&[]).unwrap_err();
        assert!(matches!(
            err.kind(),
            clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
                | clap::error::ErrorKind::MissingSubcommand
        ));
    }

    #[test]
    fn claude_input_required() {
        let err = parse(&["-u", "1", "claude-code"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn claude_input_exclusive() {
        let err = parse(&[
            "-u",
            "1",
            "claude-code",
            "--all-projects",
            "-f",
            "/tmp/x.jsonl",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn labels_multi_occurrence_flatten() {
        let cli = parse(&[
            "-u",
            "1",
            "--labels",
            "foo,bar",
            "--labels",
            "baz",
            "claude-code",
            "--all-projects",
        ])
        .unwrap();
        let labels = cli.global.extra_labels();
        assert_eq!(labels, vec!["foo", "bar", "baz"]);
    }

    #[test]
    fn labels_multi_occurrence_dedup() {
        let cli = parse(&[
            "-u",
            "1",
            "--labels",
            "foo",
            "--labels",
            "foo,bar",
            "claude-code",
            "--all-projects",
        ])
        .unwrap();
        assert_eq!(cli.global.extra_labels(), vec!["foo", "bar"]);
    }

    #[test]
    fn labels_oversize_rejected() {
        let big = "a".repeat(MAX_LABEL_BYTES + 1);
        let arg = format!("ok,{big}");
        let err =
            parse(&["-u", "1", "--labels", &arg, "claude-code", "--all-projects"]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("exceeds"), "expected length error: {msg}");
        assert!(
            msg.contains(&format!("{}", MAX_LABEL_BYTES + 1)),
            "expected reported byte count: {msg}"
        );
    }

    #[test]
    fn labels_boundary_512_accepted() {
        let big = "a".repeat(MAX_LABEL_BYTES);
        let cli = parse(&["-u", "1", "--labels", &big, "claude-code", "--all-projects"]).unwrap();
        assert_eq!(cli.global.extra_labels().len(), 1);
        assert_eq!(cli.global.extra_labels()[0].len(), MAX_LABEL_BYTES);
    }

    #[test]
    fn labels_oversize_in_later_occurrence() {
        let big = "a".repeat(MAX_LABEL_BYTES + 1);
        let arg = format!("ok,{big}");
        let err = parse(&[
            "-u",
            "1",
            "--labels",
            "first",
            "--labels",
            &arg,
            "claude-code",
            "--all-projects",
        ])
        .unwrap_err();
        // Index inside the second occurrence is 1 ("ok" at 0, big at 1).
        assert!(
            err.to_string().contains("--labels[1]"),
            "expected index hint: {err}"
        );
    }

    #[test]
    fn dry_run_with_user_id_and_input_group() {
        // dry-run still requires --user-id and the input group.
        let cli = parse(&["-u", "1", "--dry-run", "claude-code", "--all-projects"]).unwrap();
        assert!(cli.global.dry_run);
        assert_eq!(cli.global.user_id, Some(1));
    }

    #[test]
    fn output_language_accepts_supported_values() {
        let cli = parse(&[
            "-u",
            "1",
            "--output-language",
            "en",
            "claude-code",
            "--all-projects",
        ])
        .unwrap();
        assert_eq!(cli.global.output_language.as_deref(), Some("en"));
    }

    #[test]
    fn output_language_rejects_unsupported_value() {
        let err = parse(&[
            "-u",
            "1",
            "--output-language",
            "../en",
            "claude-code",
            "--all-projects",
        ])
        .unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }
}
