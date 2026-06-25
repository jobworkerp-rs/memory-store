use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join("llm_memory_descriptor.bin")) // for reflection
        .compile_protos(
            &[
                // TODO proto file path
                "protobuf/llm_memory/data/common.proto",
                "protobuf/llm_memory/data/memory.proto",
                "protobuf/llm_memory/data/media.proto",
                "protobuf/llm_memory/data/thread.proto",
                "protobuf/llm_memory/data/memory_rating.proto",
                // for implement client (in grpc-admin, for server )
                "protobuf/llm_memory/service/common.proto",
                "protobuf/llm_memory/service/memory.proto",
                "protobuf/llm_memory/service/media.proto",
                "protobuf/llm_memory/service/thread.proto",
                "protobuf/llm_memory/service/memory_rating.proto",
                "protobuf/llm_memory/data/highlight.proto",
                "protobuf/llm_memory/data/search_filter.proto",
                "protobuf/llm_memory/data/memory_vector.proto",
                "protobuf/llm_memory/data/thread_vector.proto",
                "protobuf/llm_memory/service/memory_vector.proto",
                "protobuf/llm_memory/service/thread_vector.proto",
                // thread-reflection (ai-docs/thread-reflection-spec.md)
                "protobuf/llm_memory/data/reflection.proto",
                "protobuf/llm_memory/data/reflection_filter.proto",
                "protobuf/llm_memory/service/reflection.proto",
                "protobuf/llm_memory/service/reflection_vector.proto",
            ],
            &["protobuf"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));

    Ok(())
}
