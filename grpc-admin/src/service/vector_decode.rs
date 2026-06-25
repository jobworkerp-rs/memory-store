//! Shared decode helpers for vector / hybrid request fields.
//!
//! Phase 5-2 introduced `query_vectors` / `hybrid_options` on the Count
//! handlers, doubling the number of sites that translate
//! `protobuf::llm_memory::data::HybridSearchOptions` into the app-level
//! `infra::infra::memory_vector::repository::HybridOptions`. The
//! existing search handlers (`search_by_hybrid` on memory + thread)
//! still keep their own slightly different decode shapes; collapsing
//! those is out of scope for the Phase 5-2 PR but the helper here is
//! the natural target for follow-up consolidation.

use infra::infra::memory_vector::repository::{HybridOptions, HybridStrategy};

/// Translate an optional protobuf `HybridSearchOptions` into the app
/// representation, returning `None` when the proto field is unset.
///
/// Unknown / unspecified `strategy` enum tags fall back to `Rrf` to
/// keep the count behaviour aligned with `search_by_hybrid`'s default.
pub(crate) fn decode_hybrid_options(
    proto: Option<&protobuf::llm_memory::data::HybridSearchOptions>,
) -> Option<HybridOptions> {
    proto.map(|h| HybridOptions {
        strategy: match protobuf::llm_memory::data::HybridStrategy::try_from(h.strategy).ok() {
            Some(protobuf::llm_memory::data::HybridStrategy::Weighted) => HybridStrategy::Weighted,
            Some(protobuf::llm_memory::data::HybridStrategy::VectorThenFts) => {
                HybridStrategy::VectorThenFts
            }
            Some(protobuf::llm_memory::data::HybridStrategy::FtsThenVector) => {
                HybridStrategy::FtsThenVector
            }
            _ => HybridStrategy::Rrf,
        },
        vector_weight: h.vector_weight,
        rrf_k: h.rrf_k,
    })
}
