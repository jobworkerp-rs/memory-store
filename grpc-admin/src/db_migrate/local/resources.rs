//! Mapping task resource declarations to the directories a backup must copy.

use super::backup::BackupResource;
use crate::db_migrate::{TaskResource, thread_message_times_v1::thread_vectors_enabled};
use anyhow::Result;
use infra::infra::thread_vector::config::ThreadVectorDBConfig;

/// Directories to back up for the given task resources, read from `env`.
pub fn backup_resources(
    resources: &[TaskResource],
    env: impl Fn(&str) -> Option<String>,
) -> Result<Vec<BackupResource>> {
    let mut unique = resources.to_vec();
    unique.sort();
    unique.dedup();
    let mut backups = Vec::new();
    for resource in unique {
        match resource {
            TaskResource::ThreadLanceDb => {
                // The task leaves LanceDB untouched while thread vectors are disabled.
                if thread_vectors_enabled(&env) {
                    let uri = ThreadVectorDBConfig::lancedb_uri_from(&env);
                    backups.push(BackupResource::from_uri("thread_lancedb", &uri)?);
                }
            }
        }
    }
    Ok(backups)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_migrate::TaskResource;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map = pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<HashMap<_, _>>();
        move |key| map.get(key).cloned()
    }

    #[test]
    fn thread_lancedb_is_backed_up_only_when_thread_vectors_are_enabled() {
        let disabled = env(&[("THREAD_LANCEDB_URI", "/data/lancedb")]);
        assert!(
            backup_resources(&[TaskResource::ThreadLanceDb], disabled)
                .unwrap()
                .is_empty()
        );
        let enabled = env(&[
            ("THREAD_VECTOR_ENABLED", "true"),
            ("THREAD_LANCEDB_URI", "/data/lancedb"),
        ]);
        let resources = backup_resources(
            &[TaskResource::ThreadLanceDb, TaskResource::ThreadLanceDb],
            enabled,
        )
        .unwrap();
        assert_eq!(resources.len(), 1, "duplicates collapse");
        assert_eq!(resources[0].name, "thread_lancedb");
        assert_eq!(
            resources[0].source,
            std::path::PathBuf::from("/data/lancedb")
        );
    }

    #[test]
    fn enabled_flag_is_read_exactly_like_the_task_that_changes_lancedb() {
        let upper_case = env(&[
            ("THREAD_VECTOR_ENABLED", "TRUE"),
            ("THREAD_LANCEDB_URI", "/data/lancedb"),
        ]);
        assert_eq!(
            backup_resources(&[TaskResource::ThreadLanceDb], upper_case)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn thread_lancedb_falls_back_like_the_server_configuration() {
        let shared = env(&[
            ("THREAD_VECTOR_ENABLED", "true"),
            ("MEMORY_LANCEDB_URI", "/data/shared"),
        ]);
        assert_eq!(
            backup_resources(&[TaskResource::ThreadLanceDb], shared).unwrap()[0].source,
            std::path::PathBuf::from("/data/shared")
        );
        let default = env(&[("THREAD_VECTOR_ENABLED", "true")]);
        assert_eq!(
            backup_resources(&[TaskResource::ThreadLanceDb], default).unwrap()[0].source,
            std::path::PathBuf::from("data/lancedb/memories.lancedb")
        );
    }
}
