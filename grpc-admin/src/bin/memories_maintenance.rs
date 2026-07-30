use anyhow::{Context, Result, bail};
use grpc_admin::protobuf::llm_memory::service::{
    ReconcileSearchIndicesRequest,
    search_index_maintenance_service_client::SearchIndexMaintenanceServiceClient,
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

#[tokio::main]
async fn main() -> Result<()> {
    let command = std::env::args().nth(1);
    if command.as_deref() != Some("reconcile-search-indices") {
        bail!("usage: memories-maintenance reconcile-search-indices");
    }
    let address = std::env::var("SEARCH_INDEX_MAINTENANCE_GRPC_ADDR")
        .context("SEARCH_INDEX_MAINTENANCE_GRPC_ADDR is required")?;
    let endpoint = management_endpoint(&address)?;
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::management_endpoint;

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
