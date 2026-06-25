/// LanceDB vector storage configuration
#[derive(Debug, Clone)]
pub struct VectorDBConfig {
    pub uri: String,
    pub table_name: String,
    pub vector_size: usize,
    pub distance_type: DistanceType,
    pub optimize: OptimizeConfig,
    pub fts: FtsConfig,
    pub vector_index: VectorIndexConfig,
}

#[derive(Debug, Clone, Copy)]
pub enum DistanceType {
    Cosine,
    L2,
    Dot,
}

impl VectorDBConfig {
    /// Build from environment variables. MEMORY_VECTOR_SIZE is required.
    pub fn from_env() -> anyhow::Result<Self> {
        let vector_size: usize = std::env::var("MEMORY_VECTOR_SIZE")
            .map_err(|_| {
                anyhow::anyhow!("MEMORY_VECTOR_SIZE is required when MEMORY_VECTOR_ENABLED=true")
            })?
            .parse()?;

        let cfg = Self {
            uri: std::env::var("MEMORY_LANCEDB_URI")
                .unwrap_or_else(|_| "data/lancedb/memories.lancedb".to_string()),
            table_name: std::env::var("MEMORY_LANCEDB_TABLE")
                .unwrap_or_else(|_| "memories".to_string()),
            vector_size,
            distance_type: match std::env::var("MEMORY_DISTANCE_TYPE")
                .unwrap_or_else(|_| "cosine".to_string())
                .as_str()
            {
                "l2" => DistanceType::L2,
                "dot" => DistanceType::Dot,
                _ => DistanceType::Cosine,
            },
            optimize: OptimizeConfig::from_env_with_prefixes(&["MEMORY_"]),
            fts: FtsConfig::from_env()?,
            vector_index: VectorIndexConfig::from_env_with_prefixes(&["MEMORY_"]),
        };
        warn_if_deprecated_auto_optimize_interval(&["MEMORY_"]);
        Ok(cfg)
    }
}

/// Warn when the retired `*_AUTO_OPTIMIZE_INTERVAL` env var is still set.
///
/// That single knob drove the old `OptimizeAction::All` path; it has been
/// replaced by the separate `*_OPTIMIZE_COMPACT_INTERVAL` /
/// `*_OPTIMIZE_PRUNE_INTERVAL` thresholds. We log (rather than fail) so a
/// stale value in a long-lived `.env` does not break startup, but the
/// operator is told it is now a no-op and how to migrate.
pub(crate) fn warn_if_deprecated_auto_optimize_interval(prefixes: &[&str]) {
    for p in prefixes {
        let name = format!("{p}AUTO_OPTIMIZE_INTERVAL");
        if std::env::var(&name).is_ok() {
            tracing::warn!(
                "{name} is deprecated and ignored. Use {p}OPTIMIZE_COMPACT_INTERVAL \
                 (heavy compaction/index cadence) and {p}OPTIMIZE_PRUNE_INTERVAL \
                 (version pruning cadence) instead."
            );
        }
    }
}

impl From<DistanceType> for lancedb::DistanceType {
    fn from(dt: DistanceType) -> Self {
        match dt {
            DistanceType::Cosine => lancedb::DistanceType::Cosine,
            DistanceType::L2 => lancedb::DistanceType::L2,
            DistanceType::Dot => lancedb::DistanceType::Dot,
        }
    }
}

impl std::fmt::Display for DistanceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistanceType::Cosine => write!(f, "cosine"),
            DistanceType::L2 => write!(f, "l2"),
            DistanceType::Dot => write!(f, "dot"),
        }
    }
}

// proto enum interop. The proto enum
// `protobuf::llm_memory::data::DistanceType` is wire-stable, so the
// mapping is a simple variant-by-variant copy. Used by the
// IndexStatsResponse / ThreadIndexStatsResponse RPC handlers to drop
// the legacy `string distance_type` representation.
impl From<DistanceType> for protobuf::llm_memory::data::DistanceType {
    fn from(dt: DistanceType) -> Self {
        match dt {
            DistanceType::Cosine => protobuf::llm_memory::data::DistanceType::Cosine,
            DistanceType::L2 => protobuf::llm_memory::data::DistanceType::L2,
            DistanceType::Dot => protobuf::llm_memory::data::DistanceType::Dot,
        }
    }
}

// ===== Vector (ANN) index configuration =====

/// Hard lower bound on the row count required to build the `IvfPq` index.
///
/// PQ training runs KMeans over `2^num_bits` centroids per sub-vector;
/// with `lance-index`'s default `num_bits = 8` that is 256 centroids,
/// and KMeans requires at least one sample per centroid. Building below
/// this errors with "Not enough rows to train PQ" instead of indexing.
/// We therefore never attempt a build under this floor regardless of the
/// configured `min_rows`, and stay on brute-force (which is fast at this
/// scale anyway). Keep in sync with `lance-index` if its PQ default
/// changes (currently `num_bits = 8`).
pub const VECTOR_INDEX_HARD_MIN_ROWS: usize = 256;

/// Configuration for the LanceDB vector (ANN) index on the `embedding`
/// column. Shared by both `VectorDBConfig` (memory) and
/// `ThreadVectorDBConfig` (thread) so the index policy stays identical
/// across the two tables.
///
/// Without an ANN index LanceDB falls back to a brute-force exhaustive
/// kNN scan on every vector query, which becomes the dominant cost once
/// the corpus grows past a few thousand rows. Building an `IvfPq` index
/// turns that scan into an approximate partitioned lookup.
#[derive(Debug, Clone, Copy)]
pub struct VectorIndexConfig {
    /// Master switch. When false, no ANN index is ever created and every
    /// query stays on the brute-force path (matches the legacy behavior).
    pub enabled: bool,
    /// Minimum row count before an ANN index is built. IVF training needs
    /// enough samples to cluster effectively; below this threshold a
    /// brute-force scan is both fast enough and the only correct choice
    /// (training on too few rows fails or degrades recall badly).
    ///
    /// Use [`effective_min_rows`](Self::effective_min_rows) at the build
    /// site: a value below [`VECTOR_INDEX_HARD_MIN_ROWS`] would make PQ
    /// training fail, so the effective threshold is floored at that bound.
    pub min_rows: usize,
    /// Number of IVF partitions to probe at query time. Only takes effect
    /// once an index exists; ignored on the brute-force path. Higher
    /// values trade latency for recall.
    pub nprobes: usize,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            // Operational default for *when* to switch to ANN. Independent
            // from `VECTOR_INDEX_HARD_MIN_ROWS` (the PQ training floor) —
            // they happen to share the value 256 but mean different things,
            // and `effective_min_rows` enforces the floor separately.
            min_rows: 256,
            nprobes: 20,
        }
    }
}

impl VectorIndexConfig {
    /// The build-decision threshold, floored at [`VECTOR_INDEX_HARD_MIN_ROWS`]
    /// so a misconfigured `min_rows` below the PQ training minimum cannot
    /// trigger a build that LanceDB would reject. Below the floor the table
    /// stays on brute-force (correct and fast at that scale).
    pub fn effective_min_rows(&self) -> usize {
        self.min_rows.max(VECTOR_INDEX_HARD_MIN_ROWS)
    }

    /// Read the vector-index settings from environment variables, trying
    /// each name in `prefixes` order (e.g. thread config tries
    /// `THREAD_*` first, then falls back to `MEMORY_*`). Unset or
    /// unparsable values fall back to [`Default`].
    pub fn from_env_with_prefixes(prefixes: &[&str]) -> Self {
        let default = Self::default();
        let lookup = |suffix: &str| -> Option<String> {
            prefixes
                .iter()
                .find_map(|p| std::env::var(format!("{p}{suffix}")).ok())
        };
        Self {
            enabled: lookup("VECTOR_INDEX_ENABLED")
                .and_then(|v| v.parse().ok())
                .unwrap_or(default.enabled),
            min_rows: lookup("VECTOR_INDEX_MIN_ROWS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(default.min_rows),
            nprobes: lookup("VECTOR_INDEX_NPROBES")
                .and_then(|v| v.parse().ok())
                .unwrap_or(default.nprobes),
        }
    }
}

// ===== LanceDB table maintenance (optimize) configuration =====

/// Policy for LanceDB table maintenance, split into the three distinct
/// LanceDB `OptimizeAction` operations so each runs on its own cadence.
///
/// # What prune touches (and what it must never touch)
///
/// LanceDB's cleanup has two *independent* deletion paths
/// (`lance-4.0.0/src/dataset/cleanup.rs`):
///
/// 1. **Old manifest files** (`_versions/*.manifest`) — controlled by
///    `older_than` (= [`prune_older_than_secs`](Self::prune_older_than_secs)).
///    These are version-history pointers. The latest manifest is always
///    kept regardless of age, so deleting old manifests only drops the
///    ability to *time-travel* to past versions — it never affects the
///    live data the latest version references.
/// 2. **Unreferenced data / index / delete files** — protected by a
///    hardcoded **7-day floor** (`UNVERIFIED_THRESHOLD_DAYS`), and *only*
///    when `delete_unverified=false`. We always pass `Some(false)` (see
///    [`crate::infra::memory_vector::repository`]'s `prune`), matching
///    LanceDB's official `auto_cleanup`, so live fragments are never
///    deleted. A prior version of this config exposed `delete_unverified`
///    with a `true` default and a 1h retention; that combination removed
///    the 7-day floor and caused a production data-loss incident — hence
///    the knob no longer exists.
///
/// # Why this split exists
///
/// Each write (upsert/delete/compact) appends a new manifest to
/// `_versions/`. At thousands–tens-of-thousands of writes/day this dir
/// grows without bound, and `open_table` scans it all at boot — pushing
/// startup into the tens-of-minutes range. Pruning old manifests is the
/// only thing that shrinks `_versions/` (compaction merges data fragments
/// but still *adds* a manifest), so prune runs frequently with a short
/// retention. We never use time travel, so a short manifest retention has
/// no functional cost. Compaction (the heavy path) handles data-fragment
/// fragmentation separately.
///
/// Shared by all three vector stores (memory / thread / reflection) via
/// [`from_env_with_prefixes`](Self::from_env_with_prefixes), mirroring
/// [`VectorIndexConfig`].
#[derive(Debug, Clone, Copy)]
pub struct OptimizeConfig {
    /// Operation-count threshold for the heavy path (file compaction +
    /// index optimization). Compaction rewrites data files; index-optimize
    /// folds unindexed rows into the ANN/FTS indices. Both are expensive,
    /// so they run infrequently. `0` disables the heavy path entirely.
    pub compact_interval: usize,

    /// Operation-count threshold for pruning (old-manifest cleanup).
    /// Pruning is cheap and is the only thing that keeps `_versions/` from
    /// exploding (and the startup `open_table` fast), so it runs far more
    /// often than compaction. `0` disables auto-prune.
    pub prune_interval: usize,

    /// Manifests older than this (in seconds) are eligible for pruning.
    ///
    /// This bounds *time-travel history only* — the latest manifest, and
    /// thus all live data it references, is always kept regardless of this
    /// value (see the struct docs). We do not use time travel, so a short
    /// retention is safe and keeps `_versions/` small for fast startup.
    /// Default 5 minutes. Live data/index files are protected separately by
    /// LanceDB's hardcoded 7-day floor, independent of this setting.
    pub prune_older_than_secs: u64,

    /// Run one prune pass at startup to clear the manifest backlog so the
    /// next boot's `open_table` scan is fast.
    pub prune_on_startup: bool,
}

impl Default for OptimizeConfig {
    fn default() -> Self {
        Self {
            compact_interval: 1000,
            prune_interval: 100,
            // 5 minutes: prune old manifests aggressively to keep
            // `_versions/` small. Safe because live data is protected by
            // LanceDB's 7-day floor (we pass delete_unverified=false), and
            // we never time-travel. A long retention here would let
            // `_versions/` accumulate and slow startup back down.
            prune_older_than_secs: 300,
            prune_on_startup: true,
        }
    }
}

/// Parse a boolean env value, falling back to `default` for both unset and
/// *unrecognized* values (the latter is warned about).
///
/// This deliberately does NOT follow the repo's `eq_ignore_ascii_case("true")`
/// convention, which maps any non-`true` string (including typos like `treu`
/// or `1`) to `false`. That is fine for `false`-default flags, but
/// `prune_on_startup` defaults to `true`: a typo silently flipping it to
/// `false` would disable the startup manifest cleanup. Mirroring the integer
/// fields, an unparsable value falls back to the default instead of to
/// `false`, keeping the documented "unparsable values fall back to Default"
/// contract honest.
fn parse_bool_or_default(value: Option<String>, name: &str, default: bool) -> bool {
    match value {
        None => default,
        Some(v) if v.eq_ignore_ascii_case("true") => true,
        Some(v) if v.eq_ignore_ascii_case("false") => false,
        Some(v) => {
            tracing::warn!(
                "{name}={v:?} is not a valid boolean (expected true/false); \
                 falling back to default {default}."
            );
            default
        }
    }
}

impl OptimizeConfig {
    /// Read from environment variables, trying each prefix in order
    /// (e.g. thread config tries `THREAD_*` then falls back to `MEMORY_*`).
    /// Unset or unparsable values fall back to [`Default`]. Mirrors
    /// [`VectorIndexConfig::from_env_with_prefixes`].
    pub fn from_env_with_prefixes(prefixes: &[&str]) -> Self {
        let default = Self::default();
        let lookup = |suffix: &str| -> Option<String> {
            prefixes
                .iter()
                .find_map(|p| std::env::var(format!("{p}{suffix}")).ok())
        };
        Self {
            compact_interval: lookup("OPTIMIZE_COMPACT_INTERVAL")
                .and_then(|v| v.parse().ok())
                .unwrap_or(default.compact_interval),
            prune_interval: lookup("OPTIMIZE_PRUNE_INTERVAL")
                .and_then(|v| v.parse().ok())
                .unwrap_or(default.prune_interval),
            prune_older_than_secs: lookup("OPTIMIZE_PRUNE_OLDER_THAN_SECS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(default.prune_older_than_secs),
            prune_on_startup: parse_bool_or_default(
                lookup("OPTIMIZE_PRUNE_ON_STARTUP"),
                "OPTIMIZE_PRUNE_ON_STARTUP",
                default.prune_on_startup,
            ),
        }
    }

    /// The `OptimizeAction::Prune` to run, capturing this crate's entire
    /// prune policy in one place (the SSOT for "what prune is allowed to
    /// delete"). All three vector stores call this so the policy can never
    /// drift between them.
    ///
    /// `delete_unverified` is **always** `Some(false)`: we keep LanceDB's
    /// hardcoded 7-day floor that protects live data/index fragments, so
    /// prune only ever removes old manifests, never live data. The opposite
    /// (`Some(true)`) once removed that floor and caused a production
    /// data-loss incident, which is why the knob no longer exists.
    /// `older_than` only bounds time-travel history (unused here).
    pub fn prune_action(&self) -> lancedb::table::OptimizeAction {
        // Retention as a Duration; fall back to 1h only if the configured
        // seconds overflow `i64`/`Duration` (absurd values far beyond any
        // real retention need — this is a LanceDB `Duration` limit, not a
        // policy choice).
        let older_than = i64::try_from(self.prune_older_than_secs)
            .ok()
            .and_then(lancedb::table::Duration::try_seconds)
            .unwrap_or_else(|| {
                lancedb::table::Duration::try_hours(1).expect("1h is a valid Duration")
            });
        lancedb::table::OptimizeAction::Prune {
            older_than: Some(older_than),
            delete_unverified: Some(false),
            error_if_tagged_old_versions: None,
        }
    }
}

// ===== FTS (Full-Text Search) Tokenizer configuration =====

/// Resolved `lance-index` crate version captured at build time from the
/// workspace `Cargo.lock` (see `memories/infra/build.rs`).
///
/// This value is part of the FTS fingerprint input so that `lance-index`
/// bumps automatically trigger index rebuilds — a safety net against
/// breaking changes to the tokenizer output or the inverted-index on-disk
/// format. The build script refuses to fall back to a placeholder string:
/// if the version cannot be resolved from either `Cargo.lock` or the
/// workspace `Cargo.toml`, `memories/infra/build.rs` fails the build
/// outright rather than silently embedding `"unknown"`, because a
/// placeholder would make future `lance-index` upgrades a no-op
/// fingerprint change.
pub const LANCE_INDEX_VERSION: &str = env!("MEMORIES_LANCE_INDEX_VERSION");

/// Current schema version of the FTS fingerprint stored in the LanceDB
/// table manifest config (keys `jobworkerp.fts.schema_version` and
/// `jobworkerp.fts.fingerprint`). Bump when changing the on-disk format
/// or the fingerprint composition so that previously-written entries are
/// detected as incompatible and trigger a rebuild.
pub const FTS_FINGERPRINT_SCHEMA_VERSION: u32 = 1;

/// Tokenizer kind for LanceDB FTS (BM25) inverted index.
///
/// The string forms match the `base_tokenizer` value understood by
/// `lance-index`. `lindera/*` requires building with `--features lindera`
/// (propagated to `lance-index?/tokenizer-lindera`).
///
/// # Serialization stability (load-bearing)
///
/// **The `#[serde(rename = "...")]` strings below are part of the
/// fingerprint input** (`FtsConfig::fingerprint` serializes self via
/// `serde_json::to_value`). Changing any of these strings would make
/// every existing fingerprint mismatch, triggering a forced rebuild of
/// every FTS index on every deployment after the upgrade. Do not edit
/// these strings unless you explicitly want that rebuild, or unless
/// you also bump `FTS_FINGERPRINT_SCHEMA_VERSION`.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum FtsTokenizerKind {
    #[serde(rename = "simple")]
    Simple,
    #[serde(rename = "whitespace")]
    Whitespace,
    #[serde(rename = "raw")]
    Raw,
    #[serde(rename = "ngram")]
    Ngram,
    #[serde(rename = "lindera/ipadic")]
    LinderaIpadic,
    #[serde(rename = "lindera/unidic")]
    LinderaUnidic,
    #[serde(rename = "lindera/ko-dic")]
    LinderaKoDic,
}

impl FtsTokenizerKind {
    /// Parse a user-facing tokenizer name (as accepted in `MEMORY_FTS_TOKENIZER`).
    ///
    /// Returns an error listing all valid values on failure so that
    /// operators can correct env-var typos without having to consult docs.
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s {
            "simple" => Ok(Self::Simple),
            "whitespace" => Ok(Self::Whitespace),
            "raw" => Ok(Self::Raw),
            "ngram" => Ok(Self::Ngram),
            "lindera/ipadic" => Ok(Self::LinderaIpadic),
            "lindera/unidic" => Ok(Self::LinderaUnidic),
            "lindera/ko-dic" => Ok(Self::LinderaKoDic),
            other => Err(anyhow::anyhow!(
                "invalid MEMORY_FTS_TOKENIZER={other}, valid values: \
                 simple, whitespace, raw, ngram, \
                 lindera/ipadic, lindera/unidic, lindera/ko-dic"
            )),
        }
    }

    /// String form passed to `FtsIndexBuilder::base_tokenizer(..)`.
    pub fn base_tokenizer_str(&self) -> &'static str {
        match self {
            Self::Simple => "simple",
            Self::Whitespace => "whitespace",
            Self::Raw => "raw",
            Self::Ngram => "ngram",
            Self::LinderaIpadic => "lindera/ipadic",
            Self::LinderaUnidic => "lindera/unidic",
            Self::LinderaKoDic => "lindera/ko-dic",
        }
    }

    /// Whether this kind requires the `lindera` Cargo feature (and Lindera
    /// dictionary files to be present at runtime).
    pub fn requires_lindera(&self) -> bool {
        matches!(
            self,
            Self::LinderaIpadic | Self::LinderaUnidic | Self::LinderaKoDic
        )
    }
}

impl std::fmt::Display for FtsTokenizerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.base_tokenizer_str())
    }
}

// proto enum interop. The Rust enum's `serde rename` strings are part
// of the FTS fingerprint input and cannot be repurposed for proto
// serialization, so a hand-written variant-by-variant mapping is used
// instead (see the "Serialization stability" warning above).
impl From<FtsTokenizerKind> for protobuf::llm_memory::data::FtsTokenizerKind {
    fn from(k: FtsTokenizerKind) -> Self {
        use protobuf::llm_memory::data::FtsTokenizerKind as P;
        match k {
            FtsTokenizerKind::Simple => P::Simple,
            FtsTokenizerKind::Whitespace => P::Whitespace,
            FtsTokenizerKind::Raw => P::Raw,
            FtsTokenizerKind::Ngram => P::Ngram,
            FtsTokenizerKind::LinderaIpadic => P::LinderaIpadic,
            FtsTokenizerKind::LinderaUnidic => P::LinderaUnidic,
            FtsTokenizerKind::LinderaKoDic => P::LinderaKoDic,
        }
    }
}

/// Effective FTS index configuration (post-preset, post-override).
///
/// This struct carries the full set of knobs that influence how the LanceDB
/// BM25 inverted index is built. `force_rebuild` is intentionally excluded
/// from fingerprint input (`#[serde(skip)]`) because it is an operational
/// switch, not a semantic property of the index itself.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct FtsConfig {
    pub tokenizer: FtsTokenizerKind,
    // ngram-only: ignored when tokenizer != Ngram (a warning is logged)
    pub ngram_min: u32,
    pub ngram_max: u32,
    pub ngram_prefix_only: bool,
    // Effective values after preset + override
    pub lower_case: bool,
    pub stem: bool,
    pub remove_stop_words: bool,
    pub ascii_folding: bool,
    pub with_position: bool,
    pub max_token_length: Option<usize>,
    /// Operational flag to force index rebuild regardless of fingerprint.
    /// Excluded from fingerprint input so toggling it does not itself
    /// cause a mismatch on subsequent boots.
    #[serde(skip)]
    pub force_rebuild: bool,
}

impl FtsConfig {
    /// Build an `FtsConfig` from process environment variables.
    ///
    /// Resolution order:
    /// 1. `MEMORY_FTS_TOKENIZER` explicit value (if set)
    /// 2. Default based on build features:
    ///    - `lindera` feature ON → `lindera/ipadic` (best for Japanese)
    ///    - `lindera` feature OFF → `ngram` (zero-dependency 2-gram fallback)
    ///
    /// Then applies the tokenizer preset and overlays any explicit override
    /// env vars. Unknown tokenizer names or `lindera/*` without the feature
    /// fail process startup with an actionable error message.
    pub fn from_env() -> anyhow::Result<Self> {
        let kind = match std::env::var("MEMORY_FTS_TOKENIZER") {
            Ok(v) if !v.is_empty() => {
                let parsed = FtsTokenizerKind::parse(&v)?;
                #[cfg(not(feature = "lindera"))]
                if parsed.requires_lindera() {
                    anyhow::bail!(
                        "MEMORY_FTS_TOKENIZER={v} requires building with \
                         `--features lindera` (propagates to \
                         lance-index/tokenizer-lindera). Rebuild with the \
                         feature enabled or choose another tokenizer: \
                         simple, whitespace, raw, ngram."
                    );
                }
                parsed
            }
            _ => default_tokenizer_for_build(),
        };

        let mut cfg = Self::apply_preset(kind);

        // Individual override env vars. Values not explicitly set preserve
        // the preset defaults.
        if let Some(v) = read_bool_env("MEMORY_FTS_LOWER_CASE") {
            cfg.lower_case = v;
        }
        if let Some(v) = read_bool_env("MEMORY_FTS_ASCII_FOLDING") {
            cfg.ascii_folding = v;
        }
        if let Ok(raw) = std::env::var("MEMORY_FTS_MAX_TOKEN_LENGTH") {
            cfg.max_token_length = if raw.is_empty() {
                None
            } else {
                Some(raw.parse::<usize>().map_err(|e| {
                    anyhow::anyhow!("invalid MEMORY_FTS_MAX_TOKEN_LENGTH={raw}: {e}")
                })?)
            };
        }

        let ngram_min_set = std::env::var("MEMORY_FTS_NGRAM_MIN").ok();
        let ngram_max_set = std::env::var("MEMORY_FTS_NGRAM_MAX").ok();
        let ngram_prefix_set = std::env::var("MEMORY_FTS_NGRAM_PREFIX_ONLY").ok();

        if matches!(cfg.tokenizer, FtsTokenizerKind::Ngram) {
            if let Some(raw) = &ngram_min_set {
                cfg.ngram_min = raw
                    .parse()
                    .map_err(|e| anyhow::anyhow!("invalid MEMORY_FTS_NGRAM_MIN={raw}: {e}"))?;
            }
            if let Some(raw) = &ngram_max_set {
                cfg.ngram_max = raw
                    .parse()
                    .map_err(|e| anyhow::anyhow!("invalid MEMORY_FTS_NGRAM_MAX={raw}: {e}"))?;
            }
            if let Some(raw) = &ngram_prefix_set {
                cfg.ngram_prefix_only = raw.eq_ignore_ascii_case("true");
            }
            if cfg.ngram_min == 0 {
                anyhow::bail!("MEMORY_FTS_NGRAM_MIN must be >= 1");
            }
            if cfg.ngram_min > cfg.ngram_max {
                anyhow::bail!(
                    "MEMORY_FTS_NGRAM_MIN ({}) must be <= MEMORY_FTS_NGRAM_MAX ({})",
                    cfg.ngram_min,
                    cfg.ngram_max
                );
            }
        } else {
            // Surface silent misconfiguration: ngram-specific vars are
            // meaningless for non-ngram tokenizers, so warn rather than
            // quietly swallowing the value.
            if ngram_min_set.is_some() || ngram_max_set.is_some() || ngram_prefix_set.is_some() {
                tracing::warn!(
                    "MEMORY_FTS_NGRAM_* env vars are ignored because \
                     MEMORY_FTS_TOKENIZER={} is not 'ngram'",
                    cfg.tokenizer
                );
            }
        }

        cfg.force_rebuild = read_bool_env("MEMORY_FTS_FORCE_REBUILD").unwrap_or(false);

        Ok(cfg)
    }

    /// Build a `lancedb::index::scalar::FtsIndexBuilder` that reflects
    /// the effective FTS configuration.
    ///
    /// `language()` is intentionally not called — tantivy's `Language`
    /// enum has no Japanese variant (see spec §3.R4), and our stemmer /
    /// stop-word settings are driven purely by the preset. Leaving the
    /// builder's default `language` unmodified avoids the `Result`-returning
    /// setter and keeps the chain simple.
    pub fn to_builder(&self) -> lancedb::index::scalar::FtsIndexBuilder {
        let mut builder = lancedb::index::scalar::FtsIndexBuilder::default()
            .base_tokenizer(self.tokenizer.base_tokenizer_str().to_string())
            .stem(self.stem)
            .remove_stop_words(self.remove_stop_words)
            .lower_case(self.lower_case)
            .ascii_folding(self.ascii_folding)
            .with_position(self.with_position)
            .max_token_length(self.max_token_length);

        if matches!(self.tokenizer, FtsTokenizerKind::Ngram) {
            builder = builder
                .ngram_min_length(self.ngram_min)
                .ngram_max_length(self.ngram_max)
                .ngram_prefix_only(self.ngram_prefix_only);
        }

        builder
    }

    /// Compute a SHA-256 fingerprint over the effective config, the
    /// `lance-index` version, and the FTS column name.
    ///
    /// The fingerprint is used to decide whether a previously-built
    /// inverted index on disk is still compatible with the current
    /// configuration. Any semantic change to the tokenizer, preset, or
    /// index format must produce a different fingerprint.
    ///
    /// JSON canonicalization: we route through `serde_json::Value` and
    /// recursively rebuild every object with `BTreeMap` key ordering, so
    /// the resulting string is deterministic across serde versions and
    /// platform-dependent HashMap iteration order. We deliberately avoid
    /// pulling in a canonical-JSON crate for such a small need.
    ///
    /// `force_rebuild` is skipped via `#[serde(skip)]` and therefore does
    /// not influence the fingerprint — toggling it is an operational
    /// knob, not a semantic change.
    ///
    /// `ngram_min` / `ngram_max` / `ngram_prefix_only` are removed from
    /// the fingerprint input when the active tokenizer is not `Ngram`,
    /// because the underlying `FtsIndexBuilder` silently ignores those
    /// fields in that case (`to_builder()` only calls
    /// `ngram_*_length()` / `ngram_prefix_only()` for `Ngram`). Keeping
    /// them in the fingerprint would mean that a future default bump on
    /// the ngram preset values would appear as a fingerprint mismatch on
    /// every non-ngram tokenizer and force a pointless rebuild. The
    /// ngram fields stay in the fingerprint when `Ngram` is active, so
    /// legitimate ngram configuration changes still trigger a rebuild.
    pub fn fingerprint(&self, lance_index_version: &str, index_column: &str) -> String {
        use sha2::{Digest, Sha256};
        use std::collections::BTreeMap;

        let fts_value =
            serde_json::to_value(self).expect("FtsConfig is statically serializable to JSON");
        let mut canonical_cfg = canonicalize_json(fts_value);
        if !matches!(self.tokenizer, FtsTokenizerKind::Ngram)
            && let serde_json::Value::Object(obj) = &mut canonical_cfg
        {
            obj.remove("ngram_min");
            obj.remove("ngram_max");
            obj.remove("ngram_prefix_only");
        }

        let mut root: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        root.insert("fts_config".to_string(), canonical_cfg);
        root.insert(
            "index_column".to_string(),
            serde_json::Value::String(index_column.to_string()),
        );
        root.insert(
            "lance_index_version".to_string(),
            serde_json::Value::String(lance_index_version.to_string()),
        );

        let canonical = serde_json::to_string(&root)
            .expect("BTreeMap<String, Value> is statically serializable");

        let digest = Sha256::digest(canonical.as_bytes());
        format!("sha256:{digest:x}")
    }

    /// Build an `FtsConfig` from a preset table keyed by tokenizer kind.
    ///
    /// The preset table reflects the recommendations in
    /// `memories/docs/fts-tokenizer-config-spec.md` §3.R4:
    /// - English-style tokenizers (simple/whitespace) stem + remove stop words
    /// - `ngram` is a language-agnostic fallback; stem/stop-words are off
    ///   because they have no meaning without a real `Language`
    /// - `lindera/*` are language-aware morphological tokenizers; stemming
    ///   is delegated to the dictionary, so outer stem/stop-words are off
    /// - `raw` bypasses all normalization for exact-match use cases
    pub fn apply_preset(kind: FtsTokenizerKind) -> Self {
        match kind {
            FtsTokenizerKind::Simple | FtsTokenizerKind::Whitespace => Self {
                tokenizer: kind,
                ngram_min: 2,
                ngram_max: 3,
                ngram_prefix_only: false,
                lower_case: true,
                stem: true,
                remove_stop_words: true,
                ascii_folding: true,
                with_position: false,
                max_token_length: Some(40),
                force_rebuild: false,
            },
            FtsTokenizerKind::Raw => Self {
                tokenizer: kind,
                ngram_min: 2,
                ngram_max: 3,
                ngram_prefix_only: false,
                lower_case: false,
                stem: false,
                remove_stop_words: false,
                ascii_folding: false,
                with_position: false,
                max_token_length: Some(40),
                force_rebuild: false,
            },
            FtsTokenizerKind::Ngram => Self {
                tokenizer: kind,
                // 2/3 chosen to cover both Japanese 2-char partial matches
                // and English 3-char short words like "fox" — measured
                // against tantivy's NgramTokenizer, see spec §3.R3.
                ngram_min: 2,
                ngram_max: 3,
                ngram_prefix_only: false,
                lower_case: true,
                stem: false,
                remove_stop_words: false,
                ascii_folding: true,
                with_position: false,
                max_token_length: None,
                force_rebuild: false,
            },
            FtsTokenizerKind::LinderaIpadic
            | FtsTokenizerKind::LinderaUnidic
            | FtsTokenizerKind::LinderaKoDic => Self {
                tokenizer: kind,
                ngram_min: 2,
                ngram_max: 3,
                ngram_prefix_only: false,
                lower_case: true,
                stem: false,
                remove_stop_words: false,
                // Lindera dictionaries already handle script normalization;
                // ascii-folding would unnecessarily perturb CJK content.
                ascii_folding: false,
                with_position: false,
                max_token_length: None,
                force_rebuild: false,
            },
        }
    }
}

/// Recursively rebuild every JSON object with `BTreeMap` ordering so the
/// resulting value serializes deterministically regardless of the source
/// iteration order. Arrays preserve order (positional); primitives pass
/// through unchanged.
fn canonicalize_json(v: serde_json::Value) -> serde_json::Value {
    use serde_json::{Map, Value};
    use std::collections::BTreeMap;
    match v {
        Value::Object(obj) => {
            let sorted: BTreeMap<String, Value> = obj
                .into_iter()
                .map(|(k, vv)| (k, canonicalize_json(vv)))
                .collect();
            let mut out = Map::with_capacity(sorted.len());
            for (k, vv) in sorted {
                out.insert(k, vv);
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(canonicalize_json).collect()),
        other => other,
    }
}

/// Manifest config key holding the FTS fingerprint format schema version.
///
/// Values are stringified unsigned integers; the only currently accepted
/// value is `"1"`. A mismatch (e.g. `"2"` after a future format change)
/// forces a rebuild.
pub const FTS_MANIFEST_KEY_SCHEMA_VERSION: &str = "jobworkerp.fts.schema_version";

/// Manifest config key holding the FTS fingerprint (`sha256:<hex>`).
///
/// Used by `rebuild_fts_index_locked` to decide whether the existing
/// FTS inverted index on this table is compatible with the currently
/// loaded `FtsConfig`.
pub const FTS_MANIFEST_KEY_FINGERPRINT: &str = "jobworkerp.fts.fingerprint";

/// Build-time default tokenizer selected when `MEMORY_FTS_TOKENIZER` is unset.
///
/// This differs from `FtsConfig::default()`: the latter always returns the
/// `simple` preset for backward compatibility of tests and manual
/// constructions, whereas this function picks the Japanese-friendly default
/// appropriate for production deployments.
#[cfg(feature = "lindera")]
fn default_tokenizer_for_build() -> FtsTokenizerKind {
    FtsTokenizerKind::LinderaIpadic
}

#[cfg(not(feature = "lindera"))]
fn default_tokenizer_for_build() -> FtsTokenizerKind {
    FtsTokenizerKind::Ngram
}

/// Parse a bool-ish env var. Returns `None` if the var is unset, following
/// the existing repo convention of `v.eq_ignore_ascii_case("true")` for
/// positive matching.
fn read_bool_env(name: &str) -> Option<bool> {
    std::env::var(name)
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true"))
}

impl Default for FtsConfig {
    /// Default preserves the legacy pre-tokenizer-config behavior (the
    /// `simple` tokenizer with English stemming). This is intentional for
    /// backward compatibility of tests and code paths that construct a
    /// `VectorDBConfig` manually; production callers should use
    /// `FtsConfig::from_env()` which picks the Japanese-friendly default
    /// (`lindera/ipadic` or `ngram`).
    fn default() -> Self {
        Self::apply_preset(FtsTokenizerKind::Simple)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn parses_all_known_tokenizers() {
        assert_eq!(
            FtsTokenizerKind::parse("simple").unwrap(),
            FtsTokenizerKind::Simple
        );
        assert_eq!(
            FtsTokenizerKind::parse("whitespace").unwrap(),
            FtsTokenizerKind::Whitespace
        );
        assert_eq!(
            FtsTokenizerKind::parse("raw").unwrap(),
            FtsTokenizerKind::Raw
        );
        assert_eq!(
            FtsTokenizerKind::parse("ngram").unwrap(),
            FtsTokenizerKind::Ngram
        );
        assert_eq!(
            FtsTokenizerKind::parse("lindera/ipadic").unwrap(),
            FtsTokenizerKind::LinderaIpadic
        );
        assert_eq!(
            FtsTokenizerKind::parse("lindera/unidic").unwrap(),
            FtsTokenizerKind::LinderaUnidic
        );
        assert_eq!(
            FtsTokenizerKind::parse("lindera/ko-dic").unwrap(),
            FtsTokenizerKind::LinderaKoDic
        );
    }

    #[test]
    fn rejects_unknown_tokenizer_with_enumeration() {
        let err = FtsTokenizerKind::parse("kuromoji").unwrap_err();
        let msg = err.to_string();
        // Error must both mention the bad value and enumerate valid choices
        // so operators can self-correct an env var typo.
        assert!(msg.contains("kuromoji"), "error missing bad value: {msg}");
        assert!(msg.contains("simple"), "error missing choices: {msg}");
        assert!(msg.contains("ngram"), "error missing choices: {msg}");
        assert!(
            msg.contains("lindera/ipadic"),
            "error missing choices: {msg}"
        );
    }

    #[test]
    fn preset_simple_enables_stemming_and_stop_words() {
        let cfg = FtsConfig::apply_preset(FtsTokenizerKind::Simple);
        assert!(cfg.stem);
        assert!(cfg.remove_stop_words);
        assert!(cfg.lower_case);
        assert!(cfg.ascii_folding);
        assert_eq!(cfg.max_token_length, Some(40));
    }

    #[test]
    fn preset_ngram_disables_stemming_and_stop_words() {
        let cfg = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        // `ngram` + language-aware stem/stop-words is meaningless because
        // tantivy's stemmer/stop-words filter depends on a `Language`,
        // and we fix `Language::English`. Spec §3.R4.
        assert!(!cfg.stem);
        assert!(!cfg.remove_stop_words);
        assert_eq!(cfg.ngram_min, 2);
        assert_eq!(cfg.ngram_max, 3);
        assert!(!cfg.ngram_prefix_only);
    }

    #[test]
    fn preset_lindera_disables_outer_stemming() {
        let cfg = FtsConfig::apply_preset(FtsTokenizerKind::LinderaIpadic);
        assert!(!cfg.stem);
        assert!(!cfg.remove_stop_words);
        // ASCII folding is off for CJK content — dictionary handles it.
        assert!(!cfg.ascii_folding);
    }

    #[test]
    fn default_fts_config_is_simple_preset_for_backward_compat() {
        let default_cfg = FtsConfig::default();
        let simple_preset = FtsConfig::apply_preset(FtsTokenizerKind::Simple);
        assert_eq!(default_cfg, simple_preset);
    }

    #[test]
    fn requires_lindera_flag() {
        assert!(FtsTokenizerKind::LinderaIpadic.requires_lindera());
        assert!(FtsTokenizerKind::LinderaUnidic.requires_lindera());
        assert!(FtsTokenizerKind::LinderaKoDic.requires_lindera());
        assert!(!FtsTokenizerKind::Simple.requires_lindera());
        assert!(!FtsTokenizerKind::Ngram.requires_lindera());
        assert!(!FtsTokenizerKind::Raw.requires_lindera());
        assert!(!FtsTokenizerKind::Whitespace.requires_lindera());
    }

    // === from_env tests ===
    //
    // These tests manipulate process env vars. #[serial] from serial_test
    // guarantees no concurrent access across tests.

    /// Exhaustively clear every env var this module reads so that each
    /// `from_env` test starts from a known blank state regardless of test
    /// order or prior process-level leakage.
    fn clear_fts_env() {
        for name in [
            "MEMORY_FTS_TOKENIZER",
            "MEMORY_FTS_LOWER_CASE",
            "MEMORY_FTS_ASCII_FOLDING",
            "MEMORY_FTS_MAX_TOKEN_LENGTH",
            "MEMORY_FTS_NGRAM_MIN",
            "MEMORY_FTS_NGRAM_MAX",
            "MEMORY_FTS_NGRAM_PREFIX_ONLY",
            "MEMORY_FTS_FORCE_REBUILD",
        ] {
            // SAFETY: #[serial] guarantees no concurrent env access
            unsafe {
                std::env::remove_var(name);
            }
        }
    }

    fn set_env(name: &str, value: &str) {
        // SAFETY: #[serial] guarantees no concurrent env access
        unsafe {
            std::env::set_var(name, value);
        }
    }

    #[cfg(feature = "lindera")]
    #[test]
    #[serial]
    fn from_env_defaults_to_lindera_ipadic_when_feature_enabled() {
        clear_fts_env();
        let cfg = FtsConfig::from_env().unwrap();
        assert_eq!(cfg.tokenizer, FtsTokenizerKind::LinderaIpadic);
    }

    #[cfg(not(feature = "lindera"))]
    #[test]
    #[serial]
    fn from_env_defaults_to_ngram_when_feature_disabled() {
        clear_fts_env();
        let cfg = FtsConfig::from_env().unwrap();
        assert_eq!(cfg.tokenizer, FtsTokenizerKind::Ngram);
        // Ngram preset values should be picked up
        assert_eq!(cfg.ngram_min, 2);
        assert_eq!(cfg.ngram_max, 3);
    }

    #[cfg(not(feature = "lindera"))]
    #[test]
    #[serial]
    fn from_env_rejects_lindera_when_feature_disabled() {
        clear_fts_env();
        set_env("MEMORY_FTS_TOKENIZER", "lindera/ipadic");
        let err = FtsConfig::from_env().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("--features lindera"),
            "error should suggest rebuilding with feature: {msg}"
        );
        clear_fts_env();
    }

    #[test]
    #[serial]
    fn from_env_explicit_simple_overrides_default() {
        clear_fts_env();
        set_env("MEMORY_FTS_TOKENIZER", "simple");
        let cfg = FtsConfig::from_env().unwrap();
        assert_eq!(cfg.tokenizer, FtsTokenizerKind::Simple);
        assert!(cfg.stem);
        assert!(cfg.remove_stop_words);
        clear_fts_env();
    }

    #[test]
    #[serial]
    fn from_env_individual_override_only_changes_targeted_field() {
        clear_fts_env();
        set_env("MEMORY_FTS_TOKENIZER", "simple");
        set_env("MEMORY_FTS_LOWER_CASE", "false");
        let cfg = FtsConfig::from_env().unwrap();
        // Targeted override wins
        assert!(!cfg.lower_case);
        // Other simple-preset values preserved
        assert!(cfg.stem);
        assert!(cfg.remove_stop_words);
        assert!(cfg.ascii_folding);
        assert_eq!(cfg.max_token_length, Some(40));
        clear_fts_env();
    }

    #[test]
    #[serial]
    fn from_env_ngram_overrides_respected() {
        clear_fts_env();
        set_env("MEMORY_FTS_TOKENIZER", "ngram");
        set_env("MEMORY_FTS_NGRAM_MIN", "3");
        set_env("MEMORY_FTS_NGRAM_MAX", "5");
        set_env("MEMORY_FTS_NGRAM_PREFIX_ONLY", "true");
        let cfg = FtsConfig::from_env().unwrap();
        assert_eq!(cfg.ngram_min, 3);
        assert_eq!(cfg.ngram_max, 5);
        assert!(cfg.ngram_prefix_only);
        clear_fts_env();
    }

    #[test]
    #[serial]
    fn from_env_ngram_min_gt_max_fails() {
        clear_fts_env();
        set_env("MEMORY_FTS_TOKENIZER", "ngram");
        set_env("MEMORY_FTS_NGRAM_MIN", "5");
        set_env("MEMORY_FTS_NGRAM_MAX", "3");
        assert!(FtsConfig::from_env().is_err());
        clear_fts_env();
    }

    #[test]
    #[serial]
    fn from_env_max_token_length_empty_means_none() {
        clear_fts_env();
        set_env("MEMORY_FTS_TOKENIZER", "simple");
        set_env("MEMORY_FTS_MAX_TOKEN_LENGTH", "");
        let cfg = FtsConfig::from_env().unwrap();
        assert_eq!(cfg.max_token_length, None);
        clear_fts_env();
    }

    #[test]
    #[serial]
    fn from_env_force_rebuild_flag() {
        clear_fts_env();
        set_env("MEMORY_FTS_TOKENIZER", "simple");
        set_env("MEMORY_FTS_FORCE_REBUILD", "true");
        let cfg = FtsConfig::from_env().unwrap();
        assert!(cfg.force_rebuild);
        clear_fts_env();
    }

    // === fingerprint tests ===

    #[test]
    fn fingerprint_stable_for_identical_config() {
        let a = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        let b = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        assert_eq!(
            a.fingerprint("4.0.0", "content"),
            b.fingerprint("4.0.0", "content")
        );
    }

    #[test]
    fn fingerprint_differs_for_different_tokenizer() {
        let a = FtsConfig::apply_preset(FtsTokenizerKind::Simple);
        let b = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        assert_ne!(
            a.fingerprint("4.0.0", "content"),
            b.fingerprint("4.0.0", "content")
        );
    }

    #[test]
    fn fingerprint_ignores_force_rebuild_flag() {
        let mut a = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        let mut b = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        a.force_rebuild = false;
        b.force_rebuild = true;
        // force_rebuild is an operational knob, not a semantic property of
        // the index, so toggling it must not perturb the fingerprint.
        assert_eq!(
            a.fingerprint("4.0.0", "content"),
            b.fingerprint("4.0.0", "content")
        );
    }

    #[test]
    fn fingerprint_changes_with_lance_index_version() {
        let cfg = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        let v1 = cfg.fingerprint("4.0.0", "content");
        let v2 = cfg.fingerprint("5.0.0", "content");
        assert_ne!(v1, v2);
    }

    #[test]
    fn fingerprint_changes_with_index_column() {
        let cfg = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        let a = cfg.fingerprint("4.0.0", "content");
        let b = cfg.fingerprint("4.0.0", "title");
        assert_ne!(a, b);
    }

    #[test]
    fn fingerprint_has_sha256_prefix() {
        let cfg = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        let fp = cfg.fingerprint("4.0.0", "content");
        assert!(fp.starts_with("sha256:"));
        // 64 hex chars after prefix
        assert_eq!(fp.len(), "sha256:".len() + 64);
    }

    #[test]
    fn fingerprint_sensitive_to_ngram_bounds() {
        let mut a = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        let mut b = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        a.ngram_min = 2;
        a.ngram_max = 3;
        b.ngram_min = 3;
        b.ngram_max = 3;
        assert_ne!(
            a.fingerprint("4.0.0", "content"),
            b.fingerprint("4.0.0", "content")
        );
    }

    #[test]
    fn fingerprint_ignores_ngram_fields_for_non_ngram_tokenizers() {
        // Regression guard for the preset-leak bug: the non-ngram presets
        // happen to carry `ngram_min=2, ngram_max=3, ngram_prefix_only=false`
        // as placeholder values. Those fields are not fed to
        // `FtsIndexBuilder` unless the tokenizer is Ngram, so they must
        // not influence the fingerprint either — otherwise a future default
        // bump on the ngram placeholder values would force a needless rebuild
        // on every non-ngram deployment.
        for kind in [
            FtsTokenizerKind::Simple,
            FtsTokenizerKind::Whitespace,
            FtsTokenizerKind::Raw,
            FtsTokenizerKind::LinderaIpadic,
            FtsTokenizerKind::LinderaUnidic,
            FtsTokenizerKind::LinderaKoDic,
        ] {
            let mut a = FtsConfig::apply_preset(kind);
            let mut b = FtsConfig::apply_preset(kind);
            a.ngram_min = 2;
            a.ngram_max = 3;
            a.ngram_prefix_only = false;
            b.ngram_min = 7;
            b.ngram_max = 11;
            b.ngram_prefix_only = true;
            assert_eq!(
                a.fingerprint("4.0.0", "content"),
                b.fingerprint("4.0.0", "content"),
                "ngram_* must not affect fingerprint for tokenizer={kind:?}"
            );
        }
    }

    #[test]
    fn fingerprint_still_sensitive_to_ngram_prefix_only_for_ngram() {
        // The companion invariant: for Ngram, ngram_prefix_only IS a real
        // knob and must continue to affect the fingerprint even after the
        // non-ngram exclusion was introduced.
        let mut a = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        let mut b = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        a.ngram_prefix_only = false;
        b.ngram_prefix_only = true;
        assert_ne!(
            a.fingerprint("4.0.0", "content"),
            b.fingerprint("4.0.0", "content")
        );
    }

    // === to_builder sanity tests ===
    //
    // We can't introspect `FtsIndexBuilder` fields directly — they're
    // private on the lance-index side. So we assert that `to_builder`
    // simply runs to completion for every tokenizer preset and for
    // non-default field combinations. The real functional validation
    // happens in the integration tests that actually create an index
    // and query it (Step 6).

    #[test]
    fn to_builder_runs_for_every_preset() {
        for kind in [
            FtsTokenizerKind::Simple,
            FtsTokenizerKind::Whitespace,
            FtsTokenizerKind::Raw,
            FtsTokenizerKind::Ngram,
            FtsTokenizerKind::LinderaIpadic,
            FtsTokenizerKind::LinderaUnidic,
            FtsTokenizerKind::LinderaKoDic,
        ] {
            let cfg = FtsConfig::apply_preset(kind);
            let _ = cfg.to_builder();
        }
    }

    #[test]
    fn to_builder_runs_with_ngram_overrides() {
        let mut cfg = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        cfg.ngram_min = 2;
        cfg.ngram_max = 5;
        cfg.ngram_prefix_only = true;
        cfg.max_token_length = None;
        let _ = cfg.to_builder();
    }

    #[test]
    fn vector_index_config_default_values() {
        let cfg = VectorIndexConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.min_rows, 256);
        assert_eq!(cfg.nprobes, 20);
    }

    #[test]
    fn vector_index_effective_min_rows_is_floored_at_pq_minimum() {
        // A too-low configured value is raised to the PQ training floor so
        // a build is never attempted below it.
        let cfg = VectorIndexConfig {
            min_rows: 16,
            ..VectorIndexConfig::default()
        };
        assert_eq!(cfg.effective_min_rows(), VECTOR_INDEX_HARD_MIN_ROWS);

        // A larger configured value is honored as-is.
        let cfg = VectorIndexConfig {
            min_rows: 5000,
            ..VectorIndexConfig::default()
        };
        assert_eq!(cfg.effective_min_rows(), 5000);
    }

    fn clear_vector_index_env() {
        for key in [
            "THREAD_VECTOR_INDEX_ENABLED",
            "THREAD_VECTOR_INDEX_MIN_ROWS",
            "THREAD_VECTOR_INDEX_NPROBES",
            "MEMORY_VECTOR_INDEX_ENABLED",
            "MEMORY_VECTOR_INDEX_MIN_ROWS",
            "MEMORY_VECTOR_INDEX_NPROBES",
        ] {
            // SAFETY: #[serial] guarantees no concurrent env access
            unsafe {
                std::env::remove_var(key);
            }
        }
    }

    #[test]
    #[serial]
    fn vector_index_config_reads_env_with_prefix_precedence() {
        clear_vector_index_env();

        // MEMORY_* provides the fallback; THREAD_* overrides where present.
        set_env("MEMORY_VECTOR_INDEX_MIN_ROWS", "1000");
        set_env("MEMORY_VECTOR_INDEX_NPROBES", "5");
        set_env("THREAD_VECTOR_INDEX_ENABLED", "false");
        set_env("THREAD_VECTOR_INDEX_NPROBES", "40");

        let cfg = VectorIndexConfig::from_env_with_prefixes(&["THREAD_", "MEMORY_"]);
        // enabled: only THREAD_ set -> false
        assert!(!cfg.enabled);
        // min_rows: only MEMORY_ set -> falls back to it
        assert_eq!(cfg.min_rows, 1000);
        // nprobes: THREAD_ wins over MEMORY_
        assert_eq!(cfg.nprobes, 40);

        clear_vector_index_env();
    }

    #[test]
    #[serial]
    fn vector_index_config_unparsable_env_falls_back_to_default() {
        clear_vector_index_env();
        set_env("MEMORY_VECTOR_INDEX_MIN_ROWS", "not-a-number");

        let cfg = VectorIndexConfig::from_env_with_prefixes(&["MEMORY_"]);
        assert_eq!(cfg.min_rows, VectorIndexConfig::default().min_rows);

        clear_vector_index_env();
    }

    // === OptimizeConfig tests ===

    fn clear_optimize_env() {
        for key in [
            "THREAD_OPTIMIZE_COMPACT_INTERVAL",
            "THREAD_OPTIMIZE_PRUNE_INTERVAL",
            "THREAD_OPTIMIZE_PRUNE_OLDER_THAN_SECS",
            "THREAD_OPTIMIZE_PRUNE_ON_STARTUP",
            "MEMORY_OPTIMIZE_COMPACT_INTERVAL",
            "MEMORY_OPTIMIZE_PRUNE_INTERVAL",
            "MEMORY_OPTIMIZE_PRUNE_OLDER_THAN_SECS",
            "MEMORY_OPTIMIZE_PRUNE_ON_STARTUP",
        ] {
            // SAFETY: #[serial] guarantees no concurrent env access
            unsafe {
                std::env::remove_var(key);
            }
        }
    }

    #[test]
    fn optimize_config_default_values() {
        let cfg = OptimizeConfig::default();
        assert_eq!(cfg.compact_interval, 1000);
        assert_eq!(cfg.prune_interval, 100);
        // 5-minute manifest retention; live data protected by LanceDB's
        // 7-day floor independently of this value.
        assert_eq!(cfg.prune_older_than_secs, 300);
        assert!(cfg.prune_on_startup);
    }

    #[test]
    #[serial]
    fn optimize_config_reads_env_with_prefix_precedence() {
        clear_optimize_env();

        // MEMORY_* provides the fallback; THREAD_* overrides where present.
        set_env("MEMORY_OPTIMIZE_COMPACT_INTERVAL", "2000");
        set_env("MEMORY_OPTIMIZE_PRUNE_INTERVAL", "50");
        set_env("THREAD_OPTIMIZE_PRUNE_INTERVAL", "20");

        let cfg = OptimizeConfig::from_env_with_prefixes(&["THREAD_", "MEMORY_"]);
        // compact_interval: only MEMORY_ set -> falls back to it
        assert_eq!(cfg.compact_interval, 2000);
        // prune_interval: THREAD_ wins over MEMORY_
        assert_eq!(cfg.prune_interval, 20);

        clear_optimize_env();
    }

    #[test]
    #[serial]
    fn optimize_config_invalid_bool_falls_back_to_default_not_false() {
        // Regression guard for `parse_bool_or_default`: a typo (e.g. "treu",
        // "1") must fall back to the field default, not silently to false.
        // prune_on_startup defaults to true, so a typo must keep it true —
        // otherwise the startup manifest cleanup would be silently disabled.
        clear_optimize_env();
        for bad in ["treu", "1", "yes", ""] {
            set_env("MEMORY_OPTIMIZE_PRUNE_ON_STARTUP", bad);
            assert!(
                OptimizeConfig::from_env_with_prefixes(&["MEMORY_"]).prune_on_startup,
                "invalid value {bad:?} must fall back to the true default, not false"
            );
        }
        // The same flag is honored when an operator explicitly turns it off.
        set_env("MEMORY_OPTIMIZE_PRUNE_ON_STARTUP", "false");
        assert!(!OptimizeConfig::from_env_with_prefixes(&["MEMORY_"]).prune_on_startup);
        clear_optimize_env();
    }

    #[test]
    #[serial]
    fn optimize_config_unparsable_env_falls_back_to_default() {
        clear_optimize_env();
        set_env("MEMORY_OPTIMIZE_COMPACT_INTERVAL", "not-a-number");
        let cfg = OptimizeConfig::from_env_with_prefixes(&["MEMORY_"]);
        assert_eq!(
            cfg.compact_interval,
            OptimizeConfig::default().compact_interval
        );
        clear_optimize_env();
    }

    #[test]
    #[serial]
    fn optimize_config_zero_intervals_are_preserved_as_disable() {
        clear_optimize_env();
        set_env("MEMORY_OPTIMIZE_COMPACT_INTERVAL", "0");
        set_env("MEMORY_OPTIMIZE_PRUNE_INTERVAL", "0");
        let cfg = OptimizeConfig::from_env_with_prefixes(&["MEMORY_"]);
        // 0 is a valid value meaning "disabled" — it must not be coerced
        // to the default by the parse-or-default fallback.
        assert_eq!(cfg.compact_interval, 0);
        assert_eq!(cfg.prune_interval, 0);
        clear_optimize_env();
    }

    /// Destructure the `OptimizeAction::Prune` that `prune_action()` returns
    /// into `(older_than, delete_unverified)` for assertions.
    fn prune_params(cfg: &OptimizeConfig) -> (Option<lancedb::table::Duration>, Option<bool>) {
        match cfg.prune_action() {
            lancedb::table::OptimizeAction::Prune {
                older_than,
                delete_unverified,
                ..
            } => (older_than, delete_unverified),
            _ => panic!("prune_action must return OptimizeAction::Prune"),
        }
    }

    #[test]
    fn prune_action_always_keeps_the_seven_day_floor() {
        // SSOT guard: delete_unverified must never be Some(true) — that is
        // the value that caused the production data-loss incident.
        for secs in [0, 300, 3600, u64::MAX] {
            let cfg = OptimizeConfig {
                prune_older_than_secs: secs,
                ..OptimizeConfig::default()
            };
            assert_eq!(prune_params(&cfg).1, Some(false));
        }
    }

    #[test]
    fn prune_action_converts_retention_seconds() {
        let cfg = OptimizeConfig {
            prune_older_than_secs: 3600,
            ..OptimizeConfig::default()
        };
        assert_eq!(
            prune_params(&cfg).0,
            Some(lancedb::table::Duration::try_seconds(3600).unwrap())
        );
    }

    #[test]
    fn prune_action_retention_zero_is_valid_boundary() {
        let cfg = OptimizeConfig {
            prune_older_than_secs: 0,
            ..OptimizeConfig::default()
        };
        // 0s retention is a legitimate "prune every eligible manifest" request.
        assert_eq!(
            prune_params(&cfg).0,
            Some(lancedb::table::Duration::try_seconds(0).unwrap())
        );
    }

    #[test]
    fn prune_action_retention_overflow_falls_back_to_one_hour() {
        let cfg = OptimizeConfig {
            // u64::MAX seconds overflows chrono::Duration -> 1h fallback.
            prune_older_than_secs: u64::MAX,
            ..OptimizeConfig::default()
        };
        assert_eq!(
            prune_params(&cfg).0,
            Some(lancedb::table::Duration::try_hours(1).unwrap())
        );
    }
}
