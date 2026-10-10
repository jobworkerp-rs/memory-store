use anyhow::{Context, Result, bail};
use grpc_admin::protobuf::llm_memory::service::{
    ReconcileSearchIndicesRequest,
    search_index_maintenance_service_client::SearchIndexMaintenanceServiceClient,
};
use protobuf::llm_memory::service::{
    GetEmbeddingRebuildProgressRequest, GetEmbeddingReconcileProgressRequest,
    StartEmbeddingRebuildRequest, StartEmbeddingReconcileRequest,
    embedding_maintenance_service_client::EmbeddingMaintenanceServiceClient,
};

fn management_endpoint(address: &str) -> Result<String> {
    let address = address.trim();
    if address.is_empty() {
        bail!("SEARCH_INDEX_MAINTENANCE_GRPC_ADDR must not be empty");
    }
    if address.starts_with("http://") || address.starts_with("https://") {
        Ok(address.to_owned())
    } else {
        Ok(format!("http://{address}"))
    }
}

const USAGE: &str = "usage: memories-maintenance reconcile-search-indices\n\
       memories-maintenance reconcile-embeddings [--retry-failed] [--force [--table <memory|thread|reflection_intent> --ids <id,...>]]\n\
       memories-maintenance embedding-reconcile-progress <task-id>\n\
       memories-maintenance rebuild-embeddings --attempt <attempt-id> [--retry-failed]\n\
       memories-maintenance embedding-rebuild-progress <attempt-id>";

/// Parse `rebuild-embeddings` flags.
fn rebuild_request(args: &[String]) -> Result<StartEmbeddingRebuildRequest> {
    let mut req = StartEmbeddingRebuildRequest::default();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--retry-failed" => req.retry_failed = true,
            "--attempt" => req.attempt_id = it.next().context("--attempt needs a value")?.clone(),
            other => bail!("unknown option {other}\n{USAGE}"),
        }
    }
    if req.attempt_id.is_empty() {
        bail!("--attempt is required\n{USAGE}");
    }
    Ok(req)
}

/// Parse `reconcile-embeddings` flags.
fn reconcile_request(args: &[String]) -> Result<StartEmbeddingReconcileRequest> {
    let mut req = StartEmbeddingReconcileRequest::default();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--retry-failed" => req.retry_failed = true,
            "--force" => req.force = true,
            "--table" => req.force_table = it.next().context("--table needs a value")?.clone(),
            "--ids" => {
                req.force_entity_ids = it
                    .next()
                    .context("--ids needs a value")?
                    .split(',')
                    .map(|v| v.trim().parse::<i64>().context("--ids takes integers"))
                    .collect::<Result<_>>()?;
            }
            other => bail!("unknown option {other}\n{USAGE}"),
        }
    }
    Ok(req)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = args.first().map(String::as_str) else {
        bail!("{USAGE}");
    };
    let address = std::env::var("SEARCH_INDEX_MAINTENANCE_GRPC_ADDR")
        .context("SEARCH_INDEX_MAINTENANCE_GRPC_ADDR is required")?;
    let endpoint = management_endpoint(&address)?;
    match command {
        "reconcile-search-indices" => {
            let mut client = SearchIndexMaintenanceServiceClient::connect(endpoint)
                .await
                .context("connecting to the maintenance gRPC listener")?;
            let response = client
                .reconcile_search_indices(ReconcileSearchIndicesRequest {})
                .await
                .context("calling ReconcileSearchIndices")?
                .into_inner();
            if let Some(task_id) = response.started_task_id {
                println!("accepted maintenance task {task_id}");
            } else {
                println!("no maintenance task started");
            }
        }
        "reconcile-embeddings" => {
            let request = reconcile_request(&args[1..])?;
            let mut client = EmbeddingMaintenanceServiceClient::connect(endpoint)
                .await
                .context("connecting to the maintenance gRPC listener")?;
            let response = client
                .start_reconcile(request)
                .await
                .context("calling StartReconcile")?
                .into_inner();
            println!("embedding reconcile task {}", response.task_id);
        }
        "embedding-reconcile-progress" => {
            let task_id = args.get(1).context(USAGE)?.clone();
            let mut client = EmbeddingMaintenanceServiceClient::connect(endpoint)
                .await
                .context("connecting to the maintenance gRPC listener")?;
            let p = client
                .get_reconcile_progress(GetEmbeddingReconcileProgressRequest { task_id })
                .await
                .context("calling GetReconcileProgress")?
                .into_inner();
            println!(
                "status={} captions_requested={}",
                p.status, p.captions_requested
            );
            for k in p.kinds {
                println!(
                    "kind={} missing={} stale={} unverified={} failed={} orphan={} dispatched={} complete={}",
                    k.kind,
                    k.missing,
                    k.stale,
                    k.unverified,
                    k.failed,
                    k.orphan,
                    k.dispatched,
                    k.complete
                );
            }
            if let Some(e) = p.error {
                println!("error={e}");
            }
        }
        "rebuild-embeddings" => {
            let request = rebuild_request(&args[1..])?;
            let mut client = EmbeddingMaintenanceServiceClient::connect(endpoint)
                .await
                .context("connecting to the maintenance gRPC listener")?;
            let response = client
                .start_rebuild(request)
                .await
                .context("calling StartRebuild")?
                .into_inner();
            println!(
                "embedding rebuild attempt {} task {}",
                response.attempt_id, response.task_id
            );
        }
        "embedding-rebuild-progress" => {
            let attempt_id = args.get(1).context(USAGE)?.clone();
            let mut client = EmbeddingMaintenanceServiceClient::connect(endpoint)
                .await
                .context("connecting to the maintenance gRPC listener")?;
            let p = client
                .get_rebuild_progress(GetEmbeddingRebuildProgressRequest { attempt_id })
                .await
                .context("calling GetRebuildProgress")?
                .into_inner();
            println!(
                "attempt={} status={} completion_expected={} failed={} orphan={} caption_missing={}",
                p.attempt_id,
                p.status,
                p.completion_expected,
                p.failed,
                p.orphan,
                p.caption_missing
            );
            for k in p.kinds {
                println!(
                    "kind={} targets={} complete={} missing={} stale={} unverified={} failed_transient={} failed_permanent={} dispatched={} failed_to_dispatch={}",
                    k.kind,
                    k.targets,
                    k.complete,
                    k.missing,
                    k.stale,
                    k.unverified,
                    k.failed_transient,
                    k.failed_permanent,
                    k.dispatched,
                    k.failed_to_dispatch
                );
            }
            let mut rejections: Vec<_> = p.write_rejections.into_iter().collect();
            rejections.sort();
            for (reason, n) in rejections {
                println!("write_rejections {reason}={n}");
            }
            if let Some(e) = p.error {
                println!("error={e}");
            }
        }
        _ => bail!("{USAGE}"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{management_endpoint, rebuild_request, reconcile_request};

    #[test]
    fn rebuild_flags_require_an_attempt() {
        let args = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let req = rebuild_request(&args(&["--attempt", "a1", "--retry-failed"])).unwrap();
        assert_eq!((req.attempt_id.as_str(), req.retry_failed), ("a1", true));
        assert!(rebuild_request(&args(&["--retry-failed"])).is_err());
        assert!(rebuild_request(&args(&["--attempt"])).is_err());
        assert!(rebuild_request(&args(&["--bogus"])).is_err());
    }

    #[test]
    fn reconcile_flags_map_to_the_request() {
        let args: Vec<String> = [
            "--retry-failed",
            "--force",
            "--table",
            "thread",
            "--ids",
            "1, 2",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let req = reconcile_request(&args).unwrap();
        assert!(req.retry_failed && req.force);
        assert_eq!(req.force_table, "thread");
        assert_eq!(req.force_entity_ids, vec![1, 2]);
        assert!(reconcile_request(&["--bogus".to_string()]).is_err());
    }

    #[test]
    fn adds_http_scheme_only_when_absent() {
        assert_eq!(
            management_endpoint("memories-maintenance:9001").unwrap(),
            "http://memories-maintenance:9001"
        );
        assert_eq!(
            management_endpoint("https://maintenance.example:9001").unwrap(),
            "https://maintenance.example:9001"
        );
        assert!(management_endpoint(" ").is_err());
    }
}
