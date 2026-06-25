pub mod memory_store;
pub mod llm_memory {
    pub mod data {
        tonic::include_proto!("llm_memory.data");
    }
    pub mod service {
        tonic::include_proto!("llm_memory.service");
    }
}
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("llm_memory_descriptor");
