use common::thread_group_key::{
    IdentityScope, SourceIdentity, canonical_serialize_v2, evidence_fingerprint,
    reconciler_group_canonical_key, sha256_hex, source_thread_canonical_key,
    split_group_canonical_key,
};

#[test]
fn canonical_v2_distinguishes_null_empty_separator_and_normalizes_nfc() {
    let normalized = canonical_serialize_v2(&[Some("é"), Some("a++b")]);
    let decomposed = canonical_serialize_v2(&[Some("e\u{301}"), Some("a++b")]);
    let null_value = canonical_serialize_v2(&[None]);
    let empty_value = canonical_serialize_v2(&[Some("")]);

    assert_eq!(normalized, decomposed);
    assert_ne!(null_value, empty_value);
    assert_eq!(canonical_serialize_v2(&[Some("a"), Some("b")]), b"1:a1:b");
    assert_ne!(
        canonical_serialize_v2(&[Some("1"), Some("a")]),
        canonical_serialize_v2(&[Some("1:a")])
    );
    assert_eq!(
        sha256_hex(b"1:a1:b"),
        "facdde7abf1eac5b301273ab2e282f79bab0100f058833a7b8bdf9f80741e149"
    );
}

#[test]
fn source_thread_key_is_owner_local_and_requires_known_scope() {
    let identity = SourceIdentity::new(
        "user:1",
        "claude_code",
        IdentityScope::known("project-é"),
        "session-1",
    );
    let same_normalized = SourceIdentity::new(
        "user:1",
        "claude_code",
        IdentityScope::known("project-e\u{301}"),
        "session-1",
    );
    let other_owner = SourceIdentity::new(
        "user:2",
        "claude_code",
        IdentityScope::known("project-é"),
        "session-1",
    );
    let unknown_scope = SourceIdentity::new(
        "user:1",
        "claude_code",
        IdentityScope::unknown(),
        "session-1",
    );

    assert_eq!(
        source_thread_canonical_key(&identity),
        source_thread_canonical_key(&same_normalized)
    );
    assert_ne!(
        source_thread_canonical_key(&identity),
        source_thread_canonical_key(&other_owner)
    );
    assert_eq!(source_thread_canonical_key(&unknown_scope), None);
}

#[test]
fn group_keys_are_stable_and_split_partition_order_is_irrelevant() {
    let root_key = "a".repeat(64);
    let source_group_key = reconciler_group_canonical_key(&root_key);

    assert_eq!(source_group_key, reconciler_group_canonical_key(&root_key));
    assert_ne!(
        source_group_key,
        reconciler_group_canonical_key(&"b".repeat(64))
    );
    assert_eq!(
        split_group_canonical_key(&source_group_key, &["member-b", "member-a"]),
        split_group_canonical_key(&source_group_key, &["member-a", "member-b"])
    );
    assert_eq!(
        split_group_canonical_key(&source_group_key, &["member-a", "member-a", "member-b"]),
        split_group_canonical_key(&source_group_key, &["member-a", "member-b"])
    );
}

#[test]
fn evidence_fingerprint_preserves_unknown_scope_and_parent_presence() {
    let subject = SourceIdentity::new("user:1", "codex", IdentityScope::known(""), "child");
    let unknown_parent = SourceIdentity::new("user:1", "codex", IdentityScope::unknown(), "parent");
    let known_empty_parent =
        SourceIdentity::new("user:1", "codex", IdentityScope::known(""), "parent");
    let unknown_subject = SourceIdentity::new("user:1", "codex", IdentityScope::unknown(), "child");

    assert_ne!(
        evidence_fingerprint(
            "v1",
            "adapter",
            "v1",
            &subject,
            Some(&unknown_parent),
            "delegated",
            "source_event",
            "supports",
            "exact",
            "record-1",
        ),
        evidence_fingerprint(
            "v1",
            "adapter",
            "v1",
            &subject,
            Some(&known_empty_parent),
            "delegated",
            "source_event",
            "supports",
            "exact",
            "record-1",
        )
    );
    assert_ne!(
        evidence_fingerprint(
            "v1",
            "adapter",
            "v1",
            &subject,
            None,
            "delegated",
            "source_event",
            "supports",
            "exact",
            "record-1",
        ),
        evidence_fingerprint(
            "v1",
            "adapter",
            "v1",
            &subject,
            Some(&known_empty_parent),
            "delegated",
            "source_event",
            "supports",
            "exact",
            "record-1",
        )
    );
    assert_ne!(
        evidence_fingerprint(
            "v1",
            "adapter",
            "v1",
            &subject,
            None,
            "delegated",
            "source_event",
            "supports",
            "exact",
            "record-1",
        ),
        evidence_fingerprint(
            "v1",
            "adapter",
            "v1",
            &unknown_subject,
            None,
            "delegated",
            "source_event",
            "supports",
            "exact",
            "record-1",
        )
    );
}
