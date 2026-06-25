use crate::llm_memory::service::{
    memory_service_client::MemoryServiceClient,
    memory_vector_service_client::MemoryVectorServiceClient,
    thread_service_client::ThreadServiceClient,
};
use anyhow::Result;
use jobworkerp_client::grpc::GrpcConnection;
use std::time::Duration;

// // proto files
// pub mod data {
//     // tonic::include_proto!("llm_memory.data.rs");
//     include!(concat!(env!("OUT_DIR"), "/llm_memory.data.rs"));
// }
// pub mod service {
//     tonic::include_proto!("llm_memory.service");
//     // include!(concat!(env!("OUT_DIR"), "/llm_memory.service.rs"));
// }

/// Client for interacting with the memory store service via gRPC
#[derive(Debug, Clone)]
pub struct MemoryStoreClient {
    connection: GrpcConnection,
}

impl MemoryStoreClient {
    /// Creates a new MemoryStoreClient with the specified address and timeout.
    ///
    /// # Arguments
    /// * `addr` - The address of the gRPC server
    /// * `timeout` - The timeout duration for gRPC operations
    pub async fn new(addr: &str, timeout: Duration, use_tls: bool) -> Result<Self> {
        Ok(Self {
            connection: GrpcConnection::new(addr.to_string(), Some(timeout), use_tls).await?,
        })
    }
    /// Initializes or reinitializes the gRPC connection.
    ///
    /// This method attempts to reconnect to the gRPC server if the connection is lost.
    pub async fn init_grpc_connection(&self) -> Result<()> {
        // TODO create new conection only when connection test failed
        self.connection.reconnect().await
    }
    /// Returns a new MemoryServiceClient instance connected to the gRPC channel.
    ///
    /// This method provides access to the memory service functionality through the gRPC connection.
    pub async fn memory_client(&self) -> MemoryServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        MemoryServiceClient::new(cell.clone())
    }
    /// Returns a new ThreadServiceClient instance connected to the gRPC channel.
    pub async fn thread_client(&self) -> ThreadServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        ThreadServiceClient::new(cell.clone())
    }
    /// Returns a new MemoryVectorServiceClient instance connected to the gRPC channel.
    pub async fn memory_vector_client(
        &self,
    ) -> MemoryVectorServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        MemoryVectorServiceClient::new(cell.clone())
    }
}
