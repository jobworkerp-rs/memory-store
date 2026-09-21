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
    pub owner_scope: String,
    pub source: String,
    pub identity_scope: IdentityScope,
    pub native_id: String,
}

impl SourceIdentity {
    pub fn new(
        owner_scope: impl Into<String>,
        source: impl Into<String>,
        identity_scope: IdentityScope,
        native_id: impl Into<String>,
    ) -> Self {
        Self {
            owner_scope: owner_scope.into(),
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
    Some(hash_fields(&[
        Some("thread-source"),
        Some(&identity.owner_scope),
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
    append_identity_fields(&mut fields, subject);
    fields.push(Some(if candidate_parent.is_some() {
        "present"
    } else {
        "absent"
    }));
    if let Some(parent) = candidate_parent {
        append_identity_fields(&mut fields, parent);
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

fn append_identity_fields<'a>(fields: &mut Vec<Option<&'a str>>, identity: &'a SourceIdentity) {
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
        Some(identity.owner_scope.as_str()),
        Some(identity.native_id.as_str()),
    ]);
}

fn hash_fields(fields: &[Option<&str>]) -> String {
    sha256_hex(&canonical_serialize_v2(fields))
}
