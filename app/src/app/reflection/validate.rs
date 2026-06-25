//! Entry validation for `FinalizeReflectionRequest`.
//!
//! Checks that survive into the RDB layer would either be enforced by
//! schema constraints (NOT NULL columns) or surface as opaque
//! `DBError`s. Pulling them up here gives clean
//! `LlmMemoryError::InvalidArgument` responses with field-specific
//! messages for the workflow / gRPC client.

use anyhow::Result;
use infra::error::LlmMemoryError;
use protobuf::llm_memory::data::ReflectionFact;
use protobuf::llm_memory::service::FinalizeReflectionRequest;

pub fn validate_request(req: &FinalizeReflectionRequest) -> Result<()> {
    if req.origin_thread_id.is_none() {
        return Err(invalid("origin_thread_id is required"));
    }
    if req.parsed_output.is_none() {
        return Err(invalid("parsed_output is required"));
    }
    if req.reflector_id.is_empty() {
        return Err(invalid("reflector_id must not be empty"));
    }
    if req.prompt_version.is_empty() {
        return Err(invalid("prompt_version must not be empty"));
    }

    let parsed = req.parsed_output.as_ref().unwrap();
    // Workflow contract (§4.2.7.5): every fact must arrive with its
    // anchor already resolved into a memory_id. The proto keeps it
    // optional because the same message is reused at the LLM stage
    // where only `turn_index` is known.
    for (i, fact) in parsed.facts.iter().enumerate() {
        validate_fact(i, fact)?;
    }
    Ok(())
}

fn validate_fact(index: usize, fact: &ReflectionFact) -> Result<()> {
    if fact.anchor_memory_id.is_none() {
        return Err(invalid(&format!(
            "facts[{index}].anchor_memory_id must be set (workflow must resolve turn_index \
             to memory_id before calling FinalizeReflection)"
        )));
    }
    Ok(())
}

fn invalid(msg: &str) -> anyhow::Error {
    LlmMemoryError::InvalidArgument(msg.to_string()).into()
}
