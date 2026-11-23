fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `sqlx::migrate!` embeds these directories; stable Rust cannot track
    // them from the macro, so a changed migration must rerun the build.
    println!("cargo:rerun-if-changed=../infra/db-migrate/sqlite/migrations");
    println!("cargo:rerun-if-changed=../infra/db-migrate/postgres/migrations");

    // Server-reflection metadata is supplied by the shared `protobuf`
    // crate (its descriptor covers the reflection RPCs that grpc-admin
    // does not regenerate locally), so this build emits handler
    // bindings only — no descriptor file.
    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                "../protobuf/protobuf/llm_memory/service/memory.proto",
                "../protobuf/protobuf/llm_memory/service/media.proto",
                "../protobuf/protobuf/llm_memory/service/thread.proto",
                "../protobuf/protobuf/llm_memory/service/thread_group.proto",
                "../protobuf/protobuf/llm_memory/service/memory_rating.proto",
                "../protobuf/protobuf/llm_memory/service/memory_vector.proto",
                "../protobuf/protobuf/llm_memory/service/thread_vector.proto",
                "../protobuf/protobuf/llm_memory/service/search_index_maintenance.proto",
            ],
            &["../protobuf/protobuf/"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));

    Ok(())
}
