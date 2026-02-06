//! Canonical JSON support for deterministic hashing.
//!
//! Provides RFC 8785 (JCS) compliant canonical JSON encoding for deterministic
//! content-addressable hashing. This module uses `serde_json_canonicalizer` for
//! canonicalization, producing output with the following properties:
//!
//! - Objects with lexicographically sorted keys
//! - No whitespace between tokens
//! - Numbers serialized without unnecessary precision
//! - Floating point numbers supported per RFC 8785
//! - Consistent Unicode handling
//!
//! # Example
//!
//! ```
//! use dataset_authoring::canonical::{canonicalize, hash_canonical};
//! use serde_json::json;
//!
//! let value = json!({"b": 1, "a": 2});
//! let canonical = canonicalize(&value).unwrap();
//! assert_eq!(canonical, r#"{"a":2,"b":1}"#);
//!
//! let hash = hash_canonical(&value).unwrap();
//! // hash is a datasets_common::Hash
//! ```

use datasets_common::hash::Hash;

/// Errors that occur during JSON canonicalization.
#[derive(Debug, thiserror::Error)]
pub enum CanonicalizeError {
    /// Canonicalization failed.
    ///
    /// This can occur if the value contains types that cannot be serialized
    /// to canonical JSON form.
    #[error("failed to canonicalize JSON")]
    Canonicalize(#[source] serde_json::Error),
}

/// Canonicalizes a JSON value to a deterministic string form per RFC 8785 (JCS).
///
/// The canonical form has:
/// - Object keys sorted lexicographically
/// - No whitespace between tokens
/// - Numbers serialized per RFC 8785 (no unnecessary precision)
/// - Consistent encoding
///
/// # Errors
///
/// Returns [`CanonicalizeError::Canonicalize`] if the value cannot be serialized.
pub fn canonicalize(value: &serde_json::Value) -> Result<String, CanonicalizeError> {
    let bytes = serde_json_canonicalizer::to_vec(value).map_err(CanonicalizeError::Canonicalize)?;
    // SAFETY: JCS output is valid UTF-8 JSON
    Ok(String::from_utf8(bytes).expect("JCS output should be valid UTF-8"))
}

/// Canonicalizes and hashes a JSON value.
///
/// This function:
/// 1. Serializes the value to canonical JSON per RFC 8785 (JCS)
/// 2. Computes the SHA-256 hash of the canonical bytes
///
/// This is the standard way to compute content-addressable identities for
/// JSON manifests in the dataset authoring workflow.
///
/// # Errors
///
/// Returns [`CanonicalizeError::Canonicalize`] if the value cannot be serialized.
pub fn hash_canonical(value: &serde_json::Value) -> Result<Hash, CanonicalizeError> {
    let canonical = canonicalize(value)?;
    Ok(datasets_common::hash::hash(canonical.as_bytes()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn canonicalize_with_unsorted_keys_produces_sorted_output() {
        //* Given
        let value = json!({"z": 1, "a": 2, "m": 3});

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("canonicalization should succeed");
        assert_eq!(canonical, r#"{"a":2,"m":3,"z":1}"#);
    }

    #[test]
    fn canonicalize_with_nested_objects_sorts_all_levels() {
        //* Given
        let value = json!({
            "outer": {"z": 1, "a": 2},
            "another": 3
        });

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("canonicalization should succeed");
        assert_eq!(canonical, r#"{"another":3,"outer":{"a":2,"z":1}}"#);
    }

    #[test]
    fn canonicalize_with_arrays_preserves_order() {
        //* Given
        let value = json!({"arr": [3, 1, 2]});

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("canonicalization should succeed");
        assert_eq!(canonical, r#"{"arr":[3,1,2]}"#);
    }

    #[test]
    fn canonicalize_removes_whitespace() {
        //* Given
        let value = json!({
            "key": "value",
            "number": 42
        });

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("canonicalization should succeed");
        assert!(
            !canonical.contains(' '),
            "canonical JSON should have no spaces"
        );
        assert!(
            !canonical.contains('\n'),
            "canonical JSON should have no newlines"
        );
    }

    #[test]
    fn canonicalize_handles_empty_object() {
        //* Given
        let value = json!({});

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("canonicalization should succeed");
        assert_eq!(canonical, "{}");
    }

    #[test]
    fn canonicalize_handles_empty_array() {
        //* Given
        let value = json!([]);

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("canonicalization should succeed");
        assert_eq!(canonical, "[]");
    }

    #[test]
    fn canonicalize_handles_null() {
        //* Given
        let value = json!(null);

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("canonicalization should succeed");
        assert_eq!(canonical, "null");
    }

    #[test]
    fn canonicalize_handles_boolean_values() {
        //* Given
        let value = json!({"t": true, "f": false});

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("canonicalization should succeed");
        assert_eq!(canonical, r#"{"f":false,"t":true}"#);
    }

    #[test]
    fn canonicalize_handles_integer_numbers() {
        //* Given
        let value = json!({"pos": 42, "neg": -17, "zero": 0});

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("canonicalization should succeed");
        assert_eq!(canonical, r#"{"neg":-17,"pos":42,"zero":0}"#);
    }

    #[test]
    fn canonicalize_handles_string_with_special_chars() {
        //* Given
        // Test with quotes and backslashes which require escaping
        let value = json!({"text": "hello\"world", "path": "a\\b"});

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("canonicalization should succeed");
        // Quotes and backslashes must be escaped
        assert!(
            canonical.contains(r#"\"#),
            "should contain escaped characters"
        );
    }

    #[test]
    fn canonicalize_handles_unicode_strings() {
        //* Given
        let value = json!({"emoji": "\u{1F600}", "text": "caf\u{00E9}"});

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("canonicalization should succeed");
        // Unicode characters should be preserved
        assert!(canonical.contains('\u{1F600}') || canonical.contains("\\u"));
    }

    #[test]
    fn hash_canonical_produces_consistent_hash() {
        //* Given
        let value = json!({"name": "test", "version": "1.0.0"});

        //* When
        let hash1 = hash_canonical(&value);
        let hash2 = hash_canonical(&value);

        //* Then
        let h1 = hash1.expect("first hash should succeed");
        let h2 = hash2.expect("second hash should succeed");
        assert_eq!(h1, h2, "same input should produce same hash");
    }

    #[test]
    fn hash_canonical_differs_for_different_values() {
        //* Given
        let value1 = json!({"a": 1});
        let value2 = json!({"a": 2});

        //* When
        let hash1 = hash_canonical(&value1);
        let hash2 = hash_canonical(&value2);

        //* Then
        let h1 = hash1.expect("first hash should succeed");
        let h2 = hash2.expect("second hash should succeed");
        assert_ne!(h1, h2, "different inputs should produce different hashes");
    }

    #[test]
    fn hash_canonical_is_order_independent() {
        //* Given
        // Same logical content, different key order in source
        let value1 = json!({"b": 2, "a": 1});
        let value2 = json!({"a": 1, "b": 2});

        //* When
        let hash1 = hash_canonical(&value1);
        let hash2 = hash_canonical(&value2);

        //* Then
        let h1 = hash1.expect("first hash should succeed");
        let h2 = hash2.expect("second hash should succeed");
        assert_eq!(h1, h2, "canonicalization should make key order irrelevant");
    }

    // RFC 8785 (JCS) behavior tests

    #[test]
    fn canonicalize_handles_floating_point_numbers() {
        //* Given
        // RFC 8785 allows floating point numbers
        let value = json!({"number": 1.5, "whole": 2.0});

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("floating point numbers should be accepted");
        // 1.5 stays as 1.5, 2.0 becomes 2 (no unnecessary precision per RFC 8785)
        assert_eq!(canonical, r#"{"number":1.5,"whole":2}"#);
    }

    #[test]
    fn canonicalize_accepts_integer_numbers() {
        //* Given
        // Integer numbers (including large ones) should work
        let value = json!({
            "numbers": [0, 1, -1, 1000000000, -999999999]
        });

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("integer canonicalization should succeed");
        assert_eq!(canonical, r#"{"numbers":[0,1,-1,1000000000,-999999999]}"#);
    }

    #[test]
    fn canonicalize_sorts_keys_lexicographically() {
        //* Given
        // Keys should be sorted by byte value (lexicographic)
        let value = json!({
            "z": 1,
            "A": 2,
            "a": 3,
            "1": 4
        });

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("canonicalization should succeed");
        // ASCII sort order: digits < uppercase < lowercase
        assert_eq!(canonical, r#"{"1":4,"A":2,"a":3,"z":1}"#);
    }

    #[test]
    fn canonicalize_handles_literals() {
        //* Given
        let value = json!({
            "literals": [null, true, false]
        });

        //* When
        let result = canonicalize(&value);

        //* Then
        let canonical = result.expect("canonicalization should succeed");
        assert_eq!(canonical, r#"{"literals":[null,true,false]}"#);
    }
}
