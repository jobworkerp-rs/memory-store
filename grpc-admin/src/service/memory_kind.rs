//! Validates and normalizes `memory_kinds` filter input (sort, dedup, range
//! check) at the gRPC boundary, before it reaches the app/infra layers.

use crate::protobuf::llm_memory::data::{MemoryKind, MemorySearchFilter, ThreadSearchFilter};

pub fn normalize_memory_kinds(memory_kinds: &mut Vec<i32>) -> Result<(), tonic::Status> {
    if memory_kinds
        .iter()
        .any(|kind| !(MemoryKind::Raw as i32..=MemoryKind::Reflection as i32).contains(kind))
    {
        return Err(tonic::Status::invalid_argument(
            "memory_kinds must contain only values 1..=7",
        ));
    }
    memory_kinds.sort_unstable();
    memory_kinds.dedup();
    Ok(())
}

pub fn normalize_thread_search_filter(
    filter: &mut ThreadSearchFilter,
) -> Result<(), tonic::Status> {
    normalize_memory_kinds(&mut filter.memory_kinds)
}

pub fn normalize_memory_search_filter(
    filter: &mut MemorySearchFilter,
) -> Result<(), tonic::Status> {
    normalize_memory_kinds(&mut filter.memory_kinds)?;
    if let Some(thread_filter) = filter.thread_filter.as_mut() {
        normalize_thread_search_filter(thread_filter)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_duplicates_and_accepts_empty() {
        let mut kinds = vec![
            MemoryKind::Reflection as i32,
            MemoryKind::Raw as i32,
            MemoryKind::Raw as i32,
        ];
        normalize_memory_kinds(&mut kinds).unwrap();
        assert_eq!(
            kinds,
            vec![MemoryKind::Raw as i32, MemoryKind::Reflection as i32]
        );
        normalize_memory_kinds(&mut Vec::new()).unwrap();
    }

    #[test]
    fn rejects_unspecified_and_unknown() {
        for mut kinds in [vec![0], vec![99]] {
            let error = normalize_memory_kinds(&mut kinds).unwrap_err();
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
        }
    }
}
