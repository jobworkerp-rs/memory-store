//! Build script for generating Protocol Buffers code.
//! This crate handles the compilation of .proto files during the build process.

fn main() {
    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(
            ".",
            "#[derive(serde::Serialize, serde::Deserialize, schemars::JsonSchema)]",
        )
        .compile_protos(
            &[
                "settings.proto",
                "find_all_args.proto",
                "find_all_result.proto",
                "search_args.proto",
                "search_result.proto",
                "llm/store_args.proto",
            ],
            &["protobuf"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {e:?}"));
}
