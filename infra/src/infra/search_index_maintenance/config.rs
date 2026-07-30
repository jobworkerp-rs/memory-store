use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableMaintenanceConfig {
    pub index_update_interval: Duration,
    pub compaction_interval: Duration,
    pub prune_interval: Duration,
    pub prune_older_than: Duration,
    pub index_update_unindexed_rows: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchIndexMaintenanceConfig {
    pub memory: TableMaintenanceConfig,
    pub thread: TableMaintenanceConfig,
    pub check_deadline: Duration,
    pub backoff_initial: Duration,
    pub backoff_multiplier: f64,
    pub backoff_max: Duration,
    pub task_history_limit: usize,
}

impl SearchIndexMaintenanceConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        reject_legacy_environment()?;
        let config = Self {
            memory: TableMaintenanceConfig::from_env("MEMORY_")?,
            thread: TableMaintenanceConfig::from_env("THREAD_")?,
            check_deadline: positive_seconds("SEARCH_INDEX_MAINTENANCE_CHECK_DEADLINE_SECS")?,
            backoff_initial: positive_seconds("SEARCH_INDEX_MAINTENANCE_BACKOFF_INITIAL_SECS")?,
            backoff_multiplier: required("SEARCH_INDEX_MAINTENANCE_BACKOFF_MULTIPLIER")?
                .parse::<f64>()
                .map_err(|e| {
                    anyhow::anyhow!(
                        "SEARCH_INDEX_MAINTENANCE_BACKOFF_MULTIPLIER must be a number: {e}"
                    )
                })?,
            backoff_max: positive_seconds("SEARCH_INDEX_MAINTENANCE_BACKOFF_MAX_SECS")?,
            task_history_limit: optional_positive_usize(
                "SEARCH_INDEX_MAINTENANCE_TASK_HISTORY_LIMIT",
                256,
            )?,
        };
        if !config.backoff_multiplier.is_finite() || config.backoff_multiplier < 1.0 {
            anyhow::bail!("SEARCH_INDEX_MAINTENANCE_BACKOFF_MULTIPLIER must be finite and >= 1");
        }
        if config.backoff_max < config.backoff_initial {
            anyhow::bail!(
                "SEARCH_INDEX_MAINTENANCE_BACKOFF_MAX_SECS must be >= SEARCH_INDEX_MAINTENANCE_BACKOFF_INITIAL_SECS"
            );
        }
        Ok(config)
    }
}

impl TableMaintenanceConfig {
    fn from_env(prefix: &str) -> anyhow::Result<Self> {
        Ok(Self {
            index_update_interval: nonnegative_seconds(&format!(
                "{prefix}INDEX_UPDATE_INTERVAL_SECS"
            ))?,
            compaction_interval: nonnegative_seconds(&format!("{prefix}COMPACTION_INTERVAL_SECS"))?,
            prune_interval: nonnegative_seconds(&format!("{prefix}PRUNE_INTERVAL_SECS"))?,
            prune_older_than: nonnegative_seconds(&format!("{prefix}PRUNE_OLDER_THAN_SECS"))?,
            index_update_unindexed_rows: required(&format!("{prefix}INDEX_UPDATE_UNINDEXED_ROWS"))?
                .parse()
                .map_err(|e| {
                    anyhow::anyhow!(
                        "{prefix}INDEX_UPDATE_UNINDEXED_ROWS must be a non-negative integer: {e}"
                    )
                })?,
        })
    }
}

pub fn reject_legacy_environment() -> anyhow::Result<()> {
    for prefix in ["MEMORY_", "THREAD_"] {
        for suffix in [
            "AUTO_OPTIMIZE_INTERVAL",
            "OPTIMIZE_COMPACT_INTERVAL",
            "OPTIMIZE_PRUNE_INTERVAL",
            "OPTIMIZE_PRUNE_OLDER_THAN_SECS",
            "OPTIMIZE_PRUNE_ON_STARTUP",
        ] {
            let name = format!("{prefix}{suffix}");
            if std::env::var_os(&name).is_some() {
                anyhow::bail!(
                    "{name} is no longer supported; configure duration-based search index maintenance instead"
                );
            }
        }
    }
    Ok(())
}

fn required(name: &str) -> anyhow::Result<String> {
    let value = std::env::var(name).map_err(|_| anyhow::anyhow!("{name} is required"))?;
    if value.is_empty() {
        anyhow::bail!("{name} is required");
    }
    Ok(value)
}

fn nonnegative_seconds(name: &str) -> anyhow::Result<Duration> {
    let seconds = required(name)?.parse::<u64>().map_err(|e| {
        anyhow::anyhow!("{name} must be a non-negative integer number of seconds: {e}")
    })?;
    Ok(Duration::from_secs(seconds))
}

fn positive_seconds(name: &str) -> anyhow::Result<Duration> {
    let duration = nonnegative_seconds(name)?;
    if duration.is_zero() {
        anyhow::bail!("{name} must be greater than zero");
    }
    Ok(duration)
}

fn optional_positive_usize(name: &str, default: usize) -> anyhow::Result<usize> {
    match std::env::var(name) {
        Ok(value) => {
            let value = value
                .parse::<usize>()
                .map_err(|e| anyhow::anyhow!("{name} must be a positive integer: {e}"))?;
            if value == 0 {
                anyhow::bail!("{name} must be greater than zero");
            }
            Ok(value)
        }
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(e) => Err(anyhow::anyhow!("cannot read {name}: {e}")),
    }
}
