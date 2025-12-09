//! Hash reference types combining fully qualified name and content hash.
//!
//! This module provides the `HashReference` type that represents a dataset reference
//! with a specific content hash, providing a fully resolved and immutable reference
//! to a dataset version.

use crate::{
    fqn::{FullyQualifiedName, FullyQualifiedNameError},
    hash::{Hash, HashError},
    name::Name,
    namespace::Namespace,
    reference::Reference,
    revision::Revision,
};

/// A fully resolved dataset reference combining FQN and content hash.
///
/// This type represents a dataset reference that has been resolved to a specific
/// content hash, providing an immutable and deterministic reference to a dataset
/// version. Unlike `Reference` which can use symbolic revisions like "latest" or
/// semantic versions, `HashReference` always points to a specific content hash.
///
/// ## Format
///
/// The string representation follows the format: `<namespace>/<name>@<hash>`
///
/// ## Examples
///
/// ```text
/// my_namespace/my_dataset@b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
/// _/eth_blocks@a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890
/// ```
///
/// ## Use Cases
///
/// - Pre-resolved dependencies in derived dataset execution
/// - Storing resolved references in metadata
/// - Ensuring deterministic dataset resolution
/// - Cache keys for dataset content
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(with = "String"))]
#[cfg_attr(
    feature = "schemars",
    schemars(
        extend("pattern" = r"^[a-z0-9_]+/[a-z_][a-z0-9_]*@[0-9a-f]{64}$"),
        extend("examples" = [
            "my_namespace/my_dataset@b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
            "_/eth_blocks@a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890"
        ])
    )
)]
pub struct HashReference(FullyQualifiedName, Hash);

impl HashReference {
    /// Create a new HashReference from its components.
    pub fn new(namespace: Namespace, name: Name, hash: Hash) -> Self {
        Self(FullyQualifiedName::new(namespace, name), hash)
    }

    /// Access the namespace component.
    pub fn namespace(&self) -> &Namespace {
        self.0.namespace()
    }

    /// Access the name component.
    pub fn name(&self) -> &Name {
        self.0.name()
    }

    /// Access the hash component.
    pub fn hash(&self) -> &Hash {
        &self.1
    }

    /// Get a reference to the fully qualified name (namespace and name without hash).
    pub fn as_fqn(&self) -> &FullyQualifiedName {
        &self.0
    }

    /// Consume the reference and return the FQN and hash components.
    pub fn into_fqn_and_hash(self) -> (FullyQualifiedName, Hash) {
        (self.0, self.1)
    }

    /// Consume the reference and return all components.
    pub fn into_parts(self) -> (Namespace, Name, Hash) {
        let (namespace, name) = self.0.into_parts();
        (namespace, name, self.1)
    }

    /// Convert this hash reference to a regular `Reference`.
    ///
    /// The hash will be wrapped in a `Revision::Hash` variant.
    pub fn to_reference(&self) -> Reference {
        Reference::new(
            self.namespace().clone(),
            self.name().clone(),
            Revision::Hash(self.hash().clone()),
        )
    }

    /// Convert this hash reference to a regular `Reference` (consuming).
    pub fn into_reference(self) -> Reference {
        let (namespace, name, hash) = self.into_parts();
        Reference::new(namespace, name, Revision::Hash(hash))
    }

    /// Same as normal display, but hash only shows first 7 characters.
    pub fn short_display(&self) -> String {
        format!(
            "{}/{}@{}",
            self.namespace().as_str(),
            self.name().as_str(),
            &self.hash().as_str()[..7]
        )
    }
}

impl std::fmt::Display for HashReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.0, self.1)
    }
}

impl std::str::FromStr for HashReference {
    type Err = HashReferenceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Expected format: namespace/name@hash
        let (fqn_str, hash_str) = s
            .split_once('@')
            .ok_or_else(|| HashReferenceError::MissingHashSeparator(s.to_string()))?;

        let fqn: FullyQualifiedName = fqn_str
            .parse()
            .map_err(|err| HashReferenceError::InvalidFqn(fqn_str.to_string(), err))?;

        let hash: Hash = hash_str
            .parse()
            .map_err(|err| HashReferenceError::InvalidHash(hash_str.to_string(), err))?;

        Ok(HashReference(fqn, hash))
    }
}

impl serde::Serialize for HashReference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for HashReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

// Conversion from Reference (when it contains a hash)
impl TryFrom<Reference> for HashReference {
    type Error = HashReferenceConversionError;

    fn try_from(reference: Reference) -> Result<Self, Self::Error> {
        let (fqn, revision) = reference.into_fqn_and_revision();
        match revision {
            Revision::Hash(hash) => Ok(HashReference(fqn, hash)),
            other => Err(HashReferenceConversionError::NotHashRevision(other)),
        }
    }
}

// Conversion from (FullyQualifiedName, Hash) tuple
impl From<(FullyQualifiedName, Hash)> for HashReference {
    fn from((fqn, hash): (FullyQualifiedName, Hash)) -> Self {
        HashReference(fqn, hash)
    }
}

// Conversion from (Namespace, Name, Hash) tuple
impl From<(Namespace, Name, Hash)> for HashReference {
    fn from((namespace, name, hash): (Namespace, Name, Hash)) -> Self {
        HashReference(FullyQualifiedName::new(namespace, name), hash)
    }
}

// Comparison with Reference
impl PartialEq<Reference> for HashReference {
    fn eq(&self, other: &Reference) -> bool {
        self.to_reference() == *other
    }
}

impl PartialEq<HashReference> for Reference {
    fn eq(&self, other: &HashReference) -> bool {
        *self == other.to_reference()
    }
}

// AsRef implementation for ergonomic API usage
impl AsRef<HashReference> for HashReference {
    fn as_ref(&self) -> &HashReference {
        self
    }
}

/// Errors that can occur when parsing a hash reference string.
///
/// This error type is used when parsing strings in the format `namespace/name@hash`
/// into [`HashReference`] instances. All variants include the invalid portion of the
/// input string for debugging purposes.
#[derive(Debug, thiserror::Error)]
pub enum HashReferenceError {
    /// Missing '@' separator between FQN and hash
    ///
    /// This occurs when the input string does not contain an '@' character to separate
    /// the fully qualified name (namespace/name) from the content hash.
    ///
    /// Expected format: `namespace/name@hash`
    ///
    /// Common causes:
    /// - Omitting the '@' separator entirely (e.g., `namespace/name`)
    /// - Using wrong separator (e.g., `namespace/name:hash`)
    #[error("Missing '@' separator in hash reference: {0}")]
    MissingHashSeparator(String),

    /// Invalid fully qualified name component
    ///
    /// This occurs when the portion before the '@' separator cannot be parsed as a
    /// valid fully qualified name (FQN). The FQN must be in the format `namespace/name`
    /// where both parts are valid identifiers.
    ///
    /// Common causes:
    /// - Missing namespace (e.g., `@hash`)
    /// - Invalid characters in namespace or name
    /// - Missing '/' separator between namespace and name
    /// - Empty namespace or name component
    #[error("Invalid fully qualified name '{0}': {1}")]
    InvalidFqn(String, FullyQualifiedNameError),

    /// Invalid hash component
    ///
    /// This occurs when the portion after the '@' separator cannot be parsed as a
    /// valid 32-byte SHA-256 hash (64 hexadecimal characters).
    ///
    /// Common causes:
    /// - Hash too short or too long (must be exactly 64 characters)
    /// - Non-hexadecimal characters in hash
    /// - Empty hash string
    #[error("Invalid hash '{0}': {1}")]
    InvalidHash(String, HashError),
}

/// Errors that can occur when converting a Reference to HashReference.
#[derive(Debug, thiserror::Error)]
pub enum HashReferenceConversionError {
    /// The reference does not contain a hash revision
    ///
    /// This occurs when attempting to convert a `Reference` with a non-hash revision
    /// (e.g., Version, Latest, Dev) into a `HashReference`.
    ///
    /// Only references with `Revision::Hash` variants can be converted to `HashReference`.
    #[error("Reference does not contain a hash revision, found: {0}")]
    NotHashRevision(Revision),
}
