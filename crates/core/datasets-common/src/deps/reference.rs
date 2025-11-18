//! Dependency reference types for explicit version/hash references.
//!
//! This module provides types for referencing dataset dependencies with explicit
//! versions or content hashes, excluding symbolic references like "latest" or "dev".

use crate::{
    fqn::FullyQualifiedName, hash::Hash, name::Name, namespace::Namespace, version::Version,
};

/// Dependency reference combining fully qualified name and hash-or-version.
///
/// This is similar to `Reference` but only accepts explicit versions or hashes,
/// excluding symbolic references like "latest" or "dev".
///
/// Format: `namespace/name@version` or `namespace/name@hash`
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(with = "String"))]
#[cfg_attr(
    feature = "schemars",
    schemars(
        extend("pattern" = r"^[a-z0-9_]+/[a-z_][a-z0-9_]*@.+$"),
        extend("examples" = [
            "my_namespace/my_dataset@1.0.0",
            "my_namespace/my_dataset@0.4.10-rc.1+20230502",
            "my_namespace/my_dataset@b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        ])
    )
)]
pub struct DepReference(FullyQualifiedName, HashOrVersion);

impl DepReference {
    /// Create a new DepReference from its components.
    pub fn new(namespace: Namespace, name: Name, revision: HashOrVersion) -> Self {
        Self(FullyQualifiedName::new(namespace, name), revision)
    }

    /// Access the fully qualified name component.
    pub fn fqn(&self) -> &FullyQualifiedName {
        &self.0
    }

    /// Access the hash or version component.
    pub fn revision(&self) -> &HashOrVersion {
        &self.1
    }

    /// Consume self and return the FQN and HashOrVersion components.
    pub fn into_fqn_and_hash_or_version(self) -> (FullyQualifiedName, HashOrVersion) {
        (self.0, self.1)
    }

    /// Convert to a regular Reference (consuming).
    pub fn into_reference(self) -> crate::reference::Reference {
        let (fqn, hash_or_version) = self.into_fqn_and_hash_or_version();
        let (namespace, name) = fqn.into_parts();
        let revision = match hash_or_version {
            HashOrVersion::Hash(hash) => crate::revision::Revision::Hash(hash),
            HashOrVersion::Version(version) => crate::revision::Revision::Version(version),
        };
        crate::reference::Reference::new(namespace, name, revision)
    }

    /// Convert to a regular Reference (non-consuming).
    pub fn to_reference(&self) -> crate::reference::Reference {
        self.clone().into_reference()
    }
}

// Implement PartialEq between DepReference and Reference for comparisons
impl PartialEq<crate::reference::Reference> for DepReference {
    fn eq(&self, other: &crate::reference::Reference) -> bool {
        self.to_reference() == *other
    }
}

impl PartialEq<DepReference> for crate::reference::Reference {
    fn eq(&self, other: &DepReference) -> bool {
        *self == other.to_reference()
    }
}

impl std::fmt::Display for DepReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.0, self.1)
    }
}

impl std::str::FromStr for DepReference {
    type Err = DepReferenceParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Expected format: namespace/name@hash_or_version
        let (fqn_str, revision_str) = s
            .split_once('@')
            .ok_or_else(|| DepReferenceParseError::MissingAt(s.to_string()))?;

        let fqn: FullyQualifiedName = fqn_str
            .parse()
            .map_err(|err| DepReferenceParseError::InvalidFqn(fqn_str.to_string(), err))?;

        let revision: HashOrVersion = revision_str.parse().map_err(|err| {
            DepReferenceParseError::InvalidRevision(revision_str.to_string(), err)
        })?;

        Ok(DepReference(fqn, revision))
    }
}

impl serde::Serialize for DepReference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for DepReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Hash or semantic version (no symbolic references like "latest" or "dev").
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum HashOrVersion {
    /// A 32-byte SHA-256 content hash
    Hash(Hash),
    /// A semantic version tag
    Version(Version),
}

impl std::fmt::Display for HashOrVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HashOrVersion::Hash(hash) => write!(f, "{}", hash),
            HashOrVersion::Version(version) => write!(f, "{}", version),
        }
    }
}

impl std::str::FromStr for HashOrVersion {
    type Err = HashOrVersionParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Reject symbolic references
        if s == "latest" || s == "dev" {
            return Err(HashOrVersionParseError::SymbolicReference(s.to_string()));
        }

        // Try parsing as hash first
        if let Ok(hash) = s.parse::<Hash>() {
            return Ok(HashOrVersion::Hash(hash));
        }

        // Try parsing as version
        if let Ok(version) = s.parse::<Version>() {
            return Ok(HashOrVersion::Version(version));
        }

        Err(HashOrVersionParseError::Invalid(s.to_string()))
    }
}

/// Errors that occur when parsing a dependency reference string
///
/// This error type is used when parsing strings in the format `namespace/name@revision`
/// into [`DepReference`] instances. All variants include the invalid portion of the
/// input string for debugging purposes.
#[derive(Debug, thiserror::Error)]
pub enum DepReferenceParseError {
    /// Missing '@' separator between FQN and revision
    ///
    /// This occurs when the input string does not contain an '@' character to separate
    /// the fully qualified name (namespace/name) from the revision (hash or version).
    ///
    /// Expected format: `namespace/name@version` or `namespace/name@hash`
    ///
    /// Common causes:
    /// - Omitting the '@' separator entirely (e.g., `namespace/name`)
    /// - Using wrong separator (e.g., `namespace/name:version`)
    #[error("Missing '@' separator in reference: {0}")]
    MissingAt(String),

    /// Invalid fully qualified name component
    ///
    /// This occurs when the portion before the '@' separator cannot be parsed as a
    /// valid fully qualified name (FQN). The FQN must be in the format `namespace/name`
    /// where both parts are valid identifiers.
    ///
    /// Common causes:
    /// - Missing namespace (e.g., `@version`)
    /// - Invalid characters in namespace or name
    /// - Missing '/' separator between namespace and name
    /// - Empty namespace or name component
    #[error("Invalid fully qualified name '{0}': {1}")]
    InvalidFqn(String, crate::fqn::FullyQualifiedNameError),

    /// Invalid revision component (hash or version)
    ///
    /// This occurs when the portion after the '@' separator cannot be parsed as either
    /// a valid hash or a valid semantic version.
    ///
    /// Common causes:
    /// - Using symbolic references like "latest" or "dev" (not allowed)
    /// - Invalid hash format (must be 64 hex characters)
    /// - Invalid version format (must follow semantic versioning: MAJOR.MINOR.PATCH)
    /// - Empty revision string
    #[error("Invalid revision '{0}': {1}")]
    InvalidRevision(String, HashOrVersionParseError),
}

/// Errors that occur when parsing a hash or version string
///
/// This error type is used when parsing strings into [`HashOrVersion`] instances.
/// Unlike general revision references, this type explicitly rejects symbolic references
/// like "latest" or "dev", requiring only concrete hashes or semantic versions.
#[derive(Debug, thiserror::Error)]
pub enum HashOrVersionParseError {
    /// Symbolic reference not allowed
    ///
    /// This occurs when the input string is a symbolic reference like "latest" or "dev".
    /// The schema endpoint requires explicit versions or content hashes to ensure
    /// deterministic and reproducible schema analysis.
    ///
    /// Symbolic references are intentionally rejected because:
    /// - They can point to different manifests over time
    /// - Schema analysis results would be non-deterministic
    /// - They make it unclear which exact dataset version was used
    ///
    /// Use an explicit version (e.g., "1.2.3") or hash instead.
    #[error("Symbolic reference '{0}' not allowed (use explicit version or hash)")]
    SymbolicReference(String),

    /// Invalid hash or version format
    ///
    /// This occurs when the input string cannot be parsed as either:
    /// - A valid 32-byte SHA-256 hash (64 hexadecimal characters)
    /// - A valid semantic version (MAJOR.MINOR.PATCH format)
    ///
    /// Common causes:
    /// - Partial or truncated hash
    /// - Non-hexadecimal characters in hash
    /// - Invalid version format (e.g., missing components, non-numeric parts)
    /// - Empty string or whitespace-only input
    #[error("Invalid hash or version: {0}")]
    Invalid(String),
}
