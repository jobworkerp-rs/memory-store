use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityScope {
    Known(String),
    Unknown,
}

impl IdentityScope {
    pub fn known(value: impl Into<String>) -> Self {
        Self::Known(value.into())
    }

    pub const fn unknown() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceIdentity {
    pub user_id: i64,
    pub source: String,
    pub identity_scope: IdentityScope,
    pub native_id: String,
}

impl SourceIdentity {
    pub fn new(
        user_id: i64,
        source: impl Into<String>,
        identity_scope: IdentityScope,
        native_id: impl Into<String>,
    ) -> Self {
        Self {
            user_id,
            source: source.into(),
            identity_scope,
            native_id: native_id.into(),
        }
    }
}

pub fn canonical_serialize_v2(fields: &[Option<&str>]) -> Vec<u8> {
    let mut output = Vec::new();
    for field in fields {
        match field {
            Some(value) => {
                let normalized = value.nfc().collect::<String>();
                output.extend_from_slice(normalized.len().to_string().as_bytes());
                output.push(b':');
                output.extend_from_slice(normalized.as_bytes());
            }
            None => output.extend_from_slice(b"-:"),
        }
    }
    output
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn source_thread_canonical_key(identity: &SourceIdentity) -> Option<String> {
    let IdentityScope::Known(identity_scope) = &identity.identity_scope else {
        return None;
    };
    let owner_scope = legacy_owner_scope(identity.user_id);
    Some(hash_fields(&[
        Some("thread-source"),
        Some(&owner_scope),
        Some(&identity.source),
        Some(identity_scope),
        Some(&identity.native_id),
    ]))
}

pub fn reconciler_group_canonical_key(root_thread_canonical_key: &str) -> String {
    hash_fields(&[Some("thread-group-root"), Some(root_thread_canonical_key)])
}

pub fn split_group_canonical_key(source_group_canonical_key: &str, members: &[&str]) -> String {
    let mut sorted_members = members.to_vec();
    sorted_members.sort_unstable();
    sorted_members.dedup();

    let mut fields = Vec::with_capacity(sorted_members.len() + 2);
    fields.push(Some("thread-group-split"));
    fields.push(Some(source_group_canonical_key));
    fields.extend(sorted_members.into_iter().map(Some));
    hash_fields(&fields)
}

#[allow(clippy::too_many_arguments)]
pub fn evidence_fingerprint(
    fingerprint_version: &str,
    origin: &str,
    adapter_version: &str,
    subject: &SourceIdentity,
    candidate_parent: Option<&SourceIdentity>,
    relation_kind: &str,
    evidence_kind: &str,
    polarity: &str,
    source_confidence: &str,
    source_record_locator: &str,
) -> String {
    let mut fields = vec![
        Some(fingerprint_version),
        Some(origin),
        Some(adapter_version),
    ];
    let subject_owner_scope = legacy_owner_scope(subject.user_id);
    let candidate_parent_owner_scope =
        candidate_parent.map(|parent| legacy_owner_scope(parent.user_id));
    append_identity_fields(&mut fields, subject, &subject_owner_scope);
    fields.push(Some(if candidate_parent.is_some() {
        "present"
    } else {
        "absent"
    }));
    if let Some(parent) = candidate_parent {
        append_identity_fields(
            &mut fields,
            parent,
            candidate_parent_owner_scope
                .as_deref()
                .expect("the owner encoding exists for a present parent"),
        );
    }
    fields.extend([
        Some(relation_kind),
        Some(evidence_kind),
        Some(polarity),
        Some(source_confidence),
        Some(source_record_locator),
    ]);
    hash_fields(&fields)
}

/// Compatibility encoding used only where persisted key and fingerprint
/// versions require the historical `user:<id>` bytes.
pub fn legacy_owner_scope(user_id: i64) -> String {
    format!("user:{user_id}")
}

/// Parse the historical owner-scope representation without accepting
/// alternate spellings that could disagree across identity boundaries.
pub fn parse_legacy_owner_scope(owner_scope: &str) -> Option<i64> {
    let user_id = owner_scope.strip_prefix("user:")?.parse::<i64>().ok()?;
    (legacy_owner_scope(user_id) == owner_scope).then_some(user_id)
}

fn append_identity_fields<'a>(
    fields: &mut Vec<Option<&'a str>>,
    identity: &'a SourceIdentity,
    legacy_owner_scope: &'a str,
) {
    fields.extend([
        Some(identity.source.as_str()),
        Some(match identity.identity_scope {
            IdentityScope::Known(_) => "known",
            IdentityScope::Unknown => "unknown",
        }),
        match &identity.identity_scope {
            IdentityScope::Known(value) => Some(value.as_str()),
            IdentityScope::Unknown => None,
        },
        Some(legacy_owner_scope),
        Some(identity.native_id.as_str()),
    ]);
}

fn hash_fields(fields: &[Option<&str>]) -> String {
    sha256_hex(&canonical_serialize_v2(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(user_id: i64) -> SourceIdentity {
        SourceIdentity::new(
            user_id,
            "codex",
            IdentityScope::known("scope-α"),
            "native-1",
        )
    }

    #[test]
    fn typed_user_id_keeps_the_existing_canonical_key_bytes() {
        assert_eq!(
            source_thread_canonical_key(&identity(7)).as_deref(),
            Some("eef41a24b54b4997e61136aa34f9e7b22e0242f5e0470f83abc102dee079a5c2")
        );
    }

    #[test]
    fn identical_source_identity_is_partitioned_by_user_id() {
        assert_ne!(
            source_thread_canonical_key(&identity(7)),
            source_thread_canonical_key(&identity(8))
        );
    }

    #[test]
    fn typed_user_id_keeps_the_existing_evidence_fingerprint_bytes() {
        assert_eq!(
            evidence_fingerprint(
                "thread-group-evidence-v1",
                "adapter",
                "adapter@1",
                &identity(7),
                None,
                "delegated",
                "source_event",
                "supports",
                "exact",
                "record",
            ),
            "2001d50a4f1976d7af340f5f9e9267e1c33fe061b20615f7bb4904d834522db2"
        );
    }

    #[test]
    fn legacy_owner_scope_parser_accepts_only_canonical_typed_ids() {
        assert_eq!(parse_legacy_owner_scope("user:42"), Some(42));
        for invalid in ["", "42", "user:abc", "user:042", " user:42"] {
            assert_eq!(parse_legacy_owner_scope(invalid), None, "{invalid:?}");
        }
    }
}
