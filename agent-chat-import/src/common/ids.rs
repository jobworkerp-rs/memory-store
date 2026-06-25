//! Identifier hashing helpers for `memory.external_id` (VARCHAR(512)).
//!
//! Spec §6.3: identifier columns are not human-read, so they collapse
//! arbitrarily long inputs to a fixed-length sha256 prefix when over a
//! threshold. claude-code uses this for the `session_id` fallback path
//! (when `parser.rs` falls back to filename stem); plain uses it for
//! `session_id` and entry_uid hash components; codex uses sha1 prefixes
//! for its line-ordinal entry_uids (§5.2.1).

use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::borrow::Cow;

/// Threshold (in bytes) over which an identifier is replaced with its
/// sha256[:32] hash. Chosen so that normal UUIDv4 inputs (36 bytes)
/// pass through verbatim, but pathological filename-stem fallbacks
/// (which can exceed the 512 byte external_id budget when combined
/// with prefix/uuid) are bounded.
pub const ID_SHA256_THRESHOLD: usize = 256;

/// If `value.len() > max_byte`, return `<sha256(value)[:32]>` (32 hex
/// chars = 32 bytes), otherwise return `value` unchanged.
pub fn truncate_id_for_external(value: &str, max_byte: usize) -> Cow<'_, str> {
    if value.len() <= max_byte {
        Cow::Borrowed(value)
    } else {
        Cow::Owned(sha256_hex_prefix(value.as_bytes(), 32))
    }
}

/// Compute `sha256(input)` and return the first `n` hex chars (`n` <=
/// 64). Accepts arbitrary bytes so callers can hash either UTF-8
/// strings (via `as_bytes()`) or raw file contents.
pub fn sha256_hex_prefix(input: &[u8], n: usize) -> String {
    debug_assert!(n <= 64, "sha256 hex prefix must fit in 64 chars");
    hash_hex_prefix::<Sha256>(input, n)
}

/// Compute `sha1(input)` and return the first `n` hex chars (`n` <=
/// 40). Used for codex entry_uid composition (§5.2.1) where 64-bit
/// fingerprints suffice because line_ordinal already disambiguates.
pub fn sha1_hex_prefix(input: &[u8], n: usize) -> String {
    debug_assert!(n <= 40, "sha1 hex prefix must fit in 40 chars");
    hash_hex_prefix::<Sha1>(input, n)
}

fn hash_hex_prefix<D: Digest>(input: &[u8], n: usize) -> String {
    let mut hasher = D::new();
    hasher.update(input);
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(n);
    for byte in digest.iter().take(n.div_ceil(2)) {
        use std::fmt::Write;
        let _ = write!(hex, "{byte:02x}");
    }
    hex.truncate(n);
    hex
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_value_passes_through() {
        let v = "abc";
        let out = truncate_id_for_external(v, ID_SHA256_THRESHOLD);
        assert!(matches!(out, Cow::Borrowed(_)));
        assert_eq!(&*out, "abc");
    }

    #[test]
    fn long_value_hashes_to_32_hex() {
        let v = "a".repeat(ID_SHA256_THRESHOLD + 1);
        let out = truncate_id_for_external(&v, ID_SHA256_THRESHOLD);
        assert!(matches!(out, Cow::Owned(_)));
        assert_eq!(out.len(), 32);
        assert!(out.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn deterministic_hash() {
        let v = "x".repeat(ID_SHA256_THRESHOLD + 10);
        let a = truncate_id_for_external(&v, ID_SHA256_THRESHOLD);
        let b = truncate_id_for_external(&v, ID_SHA256_THRESHOLD);
        assert_eq!(a, b);
    }

    #[test]
    fn boundary_threshold_value_passes_through() {
        let v = "a".repeat(ID_SHA256_THRESHOLD);
        let out = truncate_id_for_external(&v, ID_SHA256_THRESHOLD);
        assert!(matches!(out, Cow::Borrowed(_)));
    }

    #[test]
    fn sha256_prefix_known_vector() {
        // sha256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        assert_eq!(sha256_hex_prefix(b"abc", 16), "ba7816bf8f01cfea");
        assert_eq!(
            sha256_hex_prefix(b"abc", 32),
            "ba7816bf8f01cfea414140de5dae2223"
        );
    }

    #[test]
    fn sha256_prefix_distinct_for_distinct_inputs() {
        assert_ne!(sha256_hex_prefix(b"a", 16), sha256_hex_prefix(b"b", 16));
    }

    #[test]
    fn sha1_prefix_known_vector() {
        // sha1("abc") = a9993e364706816aba3e25717850c26c9cd0d89d
        assert_eq!(sha1_hex_prefix(b"abc", 16), "a9993e364706816a");
    }
}
