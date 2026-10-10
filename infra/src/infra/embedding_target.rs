//! The single definition of which source rows are embedding targets.
//!
//! Dispatch, reconciliation counts, and state classification must agree
//! on what "should have a vector" means, so every path asks these
//! predicates instead of re-deriving the conditions.

use crate::infra::embedding_dispatch::{DispatchKind, ImageSearchMode};
use crate::infra::embedding_index::TableLabel;
use crate::infra::embedding_index::source_version::{self, SourceVersion, TextSource};
use crate::infra::memory_vector::record::vector_kind;
use smallvec::{SmallVec, smallvec};
use std::borrow::Cow;

/// The first `max` characters of `content`: exactly what is sent to the
/// embedding worker, and therefore what a source version covers.
pub fn truncate_chars(content: &str, max: usize) -> Cow<'_, str> {
    match content.char_indices().nth(max) {
        Some((byte, _)) => Cow::Owned(content[..byte].to_string()),
        None => Cow::Borrowed(content),
    }
}

/// Decide which embedding pipelines a memory row should be dispatched to.
///
/// Two independent axes:
/// - **Text**: `role ∈ {USER,ASSISTANT,SYSTEM} ∧ content_type ≠ TOOL ∧
///   non-empty content`. TOOL content is excluded to keep tool-call /
///   tool-output previews out of the text vector space (the historical
///   `content_type=TEXT` narrowing carried this intent; role alone would
///   let ASSISTANT-role tool_use rows pollute the index).
/// - **Media**: the linked `media_object.kind == IMAGE` ∧ its
///   `storage_backend ∉ {unresolvable, inline}` ∧ `mode != none`.
///   Independent of `content_type` (any content_type may carry media —
///   a TOOL memory's screenshot is still embeddable). AUDIO/VIDEO are
///   out of scope. `unresolvable` has no bytes to embed (promoted later);
///   `inline` is a test-only backend the embedding workflow cannot read.
///
/// `ROLE_REFLECTION` is excluded via the role allow-list: reflection
/// memories travel through their own dispatchers, and dispatching them
/// here too would double-dispatch with status-update gaps.
/// Whether a memory's linked media would be dispatched to the image
/// (Media) pipeline. The three conjuncts are exactly the Media axis of
/// [`dispatch_kinds`]: `kind == IMAGE` ∧ `storage_backend ∉
/// {unresolvable, inline}` ∧ image mode enabled. `unresolvable` has no
/// bytes to embed; `inline` is a test-only backend the embedding
/// workflow cannot read; `mode=none` disables the image pipeline
/// entirely. Exposed as a standalone predicate so callers that must
/// reason about "will this media produce image/caption vectors?" (e.g.
/// the Update path deciding whether old image rows are now orphaned)
/// share one definition with `dispatch_kinds` instead of re-deriving a
/// subset of the conditions.
pub fn media_axis_dispatchable(
    media_kind: Option<i32>,
    media_storage_backend: Option<&str>,
    mode: ImageSearchMode,
) -> bool {
    use protobuf::llm_memory::data::ContentType;
    let is_image = matches!(
        media_kind.and_then(|k| ContentType::try_from(k).ok()),
        Some(ContentType::Image)
    );
    let media_embeddable = !matches!(media_storage_backend, Some("unresolvable") | Some("inline"));
    is_image && media_embeddable && mode.is_image_enabled()
}

pub fn dispatch_kinds(
    content: &str,
    role: i32,
    content_type: i32,
    media_kind: Option<i32>,
    media_storage_backend: Option<&str>,
    mode: ImageSearchMode,
) -> SmallVec<[DispatchKind; 2]> {
    use protobuf::llm_memory::data::{ContentType, MessageRole};
    let role_ok = matches!(
        MessageRole::try_from(role),
        Ok(MessageRole::RoleUser
            | MessageRole::RoleAssistant
            | MessageRole::RoleSystem
            | MessageRole::RoleReflection)
    );
    if !role_ok {
        return SmallVec::new();
    }
    let mut kinds: SmallVec<[DispatchKind; 2]> = smallvec![];
    let is_tool = matches!(ContentType::try_from(content_type), Ok(ContentType::Tool));
    if !is_tool && !content.trim().is_empty() {
        kinds.push(DispatchKind::Text);
    }
    if media_axis_dispatchable(media_kind, media_storage_backend, mode) {
        kinds.push(DispatchKind::Media);
    }
    kinds
}

/// The media linked to a memory, as far as target derivation needs it.
#[derive(Debug, Clone, Copy)]
pub struct LinkedMedia<'a> {
    pub id: i64,
    pub kind: i32,
    pub storage_backend: &'a str,
    pub sha256: Option<&'a str>,
    pub url: Option<&'a str>,
}

/// One embedding target: the vector table and kind its rows have, and the
/// version of the input they must be generated from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedTarget {
    pub table: TableLabel,
    pub vector_kind: &'static str,
    pub version: SourceVersion,
}

/// Every embedding target a memory row yields: text, image, and caption
/// rows in the memory table, and the intent row of a reflection memory in
/// the reflection intent table.
#[allow(clippy::too_many_arguments)]
pub fn memory_targets(
    content: &str,
    role: i32,
    content_type: i32,
    metadata: Option<&str>,
    media: Option<LinkedMedia<'_>>,
    mode: ImageSearchMode,
    max_content_len: usize,
) -> SmallVec<[DerivedTarget; 3]> {
    let mut out = SmallVec::new();
    let kinds = dispatch_kinds(
        content,
        role,
        content_type,
        media.map(|m| m.kind),
        media.map(|m| m.storage_backend),
        mode,
    );
    let text = truncate_chars(content, max_content_len);
    if kinds.contains(&DispatchKind::Text) {
        out.push(DerivedTarget {
            table: TableLabel::Memory,
            vector_kind: vector_kind::TEXT,
            version: source_version::text(TextSource::MemoryText, &text),
        });
    }
    if let Some(m) = media
        && kinds.contains(&DispatchKind::Media)
    {
        if matches!(mode, ImageSearchMode::Multimodal | ImageSearchMode::Both) {
            out.push(DerivedTarget {
                table: TableLabel::Memory,
                vector_kind: vector_kind::IMAGE,
                version: source_version::media(m.id, m.sha256, m.url),
            });
        }
        // The caption is the memory body; until one exists there is
        // nothing to embed, only a caption to generate.
        if matches!(mode, ImageSearchMode::VlmCaption | ImageSearchMode::Both)
            && !content.trim().is_empty()
        {
            out.push(DerivedTarget {
                table: TableLabel::Memory,
                vector_kind: vector_kind::CAPTION,
                version: source_version::text(TextSource::Caption, &text),
            });
        }
    }
    if is_reflection(role) {
        let intent = reflection_task_intent(metadata);
        if reflection_text_is_target(&intent) {
            out.push(DerivedTarget {
                table: TableLabel::ReflectionIntent,
                vector_kind: vector_kind::TEXT,
                version: source_version::text(
                    TextSource::ReflectionIntent,
                    &truncate_chars(&intent, max_content_len),
                ),
            });
        }
    }
    out
}

fn is_reflection(role: i32) -> bool {
    use protobuf::llm_memory::data::MessageRole;
    matches!(MessageRole::try_from(role), Ok(MessageRole::RoleReflection))
}

/// The intent text of a reflection memory (`metadata.eval.task_intent`).
pub fn reflection_task_intent(metadata: Option<&str>) -> String {
    metadata
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .and_then(|v| {
            v.pointer("/eval/task_intent")
                .and_then(|x| x.as_str())
                .map(str::to_string)
        })
        .unwrap_or_default()
}

/// The embedding target of a thread description, if any.
pub fn thread_target(description: &str, max_content_len: usize) -> Option<DerivedTarget> {
    thread_description_is_target(description).then(|| DerivedTarget {
        table: TableLabel::Thread,
        vector_kind: vector_kind::TEXT,
        version: source_version::text(
            TextSource::ThreadDescription,
            &truncate_chars(description, max_content_len),
        ),
    })
}

/// A thread's description is embedded when it is non-empty.
pub fn thread_description_is_target(description: &str) -> bool {
    !description.is_empty()
}

/// A reflection summary or intent text is embedded when it is non-empty.
pub fn reflection_text_is_target(text: &str) -> bool {
    !text.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use protobuf::llm_memory::data::{ContentType, MessageRole};

    const IMAGE: i32 = ContentType::Image as i32;
    const TEXT: i32 = ContentType::Text as i32;

    fn media(sha: Option<&str>) -> LinkedMedia<'_> {
        LinkedMedia {
            id: 9,
            kind: IMAGE,
            storage_backend: "file",
            sha256: sha,
            url: Some("file:///m.png"),
        }
    }

    fn kinds(t: &[DerivedTarget]) -> Vec<(&'static str, &'static str)> {
        t.iter()
            .map(|d| (d.table.as_str(), d.vector_kind))
            .collect()
    }

    #[test]
    fn truncation_counts_characters() {
        assert_eq!(truncate_chars("あいうえお", 3), "あいう");
        assert_eq!(truncate_chars("abc", 3), "abc");
        assert_eq!(truncate_chars("", 0), "");
    }

    #[test]
    fn memory_targets_per_mode() {
        let user = MessageRole::RoleUser as i32;
        let derive = |content, mode| {
            memory_targets(content, user, TEXT, None, Some(media(Some("d"))), mode, 100)
        };
        assert_eq!(
            kinds(&derive("hi", ImageSearchMode::None)),
            vec![("memory", "text")]
        );
        assert_eq!(
            kinds(&derive("hi", ImageSearchMode::Multimodal)),
            vec![("memory", "text"), ("memory", "image")]
        );
        assert_eq!(
            kinds(&derive("hi", ImageSearchMode::Both)),
            vec![
                ("memory", "text"),
                ("memory", "image"),
                ("memory", "caption")
            ]
        );
        // No body yet: the caption has to be generated first.
        assert_eq!(kinds(&derive("", ImageSearchMode::VlmCaption)), vec![]);
        assert_eq!(
            kinds(&derive("", ImageSearchMode::Both)),
            vec![("memory", "image")]
        );
    }

    #[test]
    fn versions_follow_the_embedded_input() {
        let user = MessageRole::RoleUser as i32;
        let v = |content: &str, max| {
            memory_targets(content, user, TEXT, None, None, ImageSearchMode::None, max)[0]
                .version
                .clone()
        };
        assert_eq!(
            v("abcdef", 3),
            v("abcxyz", 3),
            "only the truncated text matters"
        );
        assert_ne!(v("abcdef", 3), v("abcdef", 4));
        let img = |sha| {
            memory_targets(
                "",
                user,
                TEXT,
                None,
                Some(media(sha)),
                ImageSearchMode::Multimodal,
                10,
            )[0]
            .version
            .clone()
        };
        assert_ne!(img(Some("a")), img(Some("b")));
    }

    #[test]
    fn reflection_memories_also_target_the_intent_table() {
        let role = MessageRole::RoleReflection as i32;
        let meta = r#"{"eval":{"task_intent":"fix the build"}}"#;
        let t = memory_targets(
            "summary",
            role,
            TEXT,
            Some(meta),
            None,
            ImageSearchMode::None,
            100,
        );
        assert_eq!(
            kinds(&t),
            vec![("memory", "text"), ("reflection_intent", "text")]
        );
        let none = memory_targets(
            "summary",
            role,
            TEXT,
            Some("{}"),
            None,
            ImageSearchMode::None,
            100,
        );
        assert_eq!(kinds(&none), vec![("memory", "text")]);
        assert_eq!(reflection_task_intent(Some("not json")), "");
    }

    #[test]
    fn thread_targets() {
        assert!(thread_target("", 10).is_none());
        let t = thread_target("topic", 10).unwrap();
        assert_eq!((t.table, t.vector_kind), (TableLabel::Thread, "text"));
    }

    #[test]
    fn thread_description_target_table() {
        for (desc, expected) in [("", false), ("a", true), (" ", true), ("説明", true)] {
            assert_eq!(thread_description_is_target(desc), expected, "{desc:?}");
        }
    }

    #[test]
    fn reflection_text_target_table() {
        for (text, expected) in [("", false), ("intent", true), ("\n", true)] {
            assert_eq!(reflection_text_is_target(text), expected, "{text:?}");
        }
    }
}
