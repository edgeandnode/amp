//! Partial dataset reference types with optional namespace and revision.
//!
//! This module provides the `PartialReference` type that represents dataset references
//! where the namespace and/or revision may be omitted. This is useful for parsing
//! user input where these components are optional.

use crate::{
    name::{Name, NameError},
    namespace::{Namespace, NamespaceError},
    reference::Reference,
    revision::{Revision, RevisionParseError},
};

/// A partial dataset reference with optional namespace and revision.
///
/// This type allows parsing references where:
/// - Namespace is optional (if omitted, will need to be inferred)
/// - Revision is optional (if omitted, typically means "latest")
///
/// ## Supported Formats
///
/// ```text
/// namespace/name@revision  (full reference)
/// namespace/name           (no revision)
/// name@revision            (no namespace)
/// name                     (only name)
/// ```
///
/// ## Examples
///
/// ```text
/// _/eth_rpc@1.0.0         (full)
/// _/eth_rpc               (no revision)
/// eth_rpc@1.0.0           (no namespace)
/// eth_rpc                 (only name)
/// ```
#[derive(
    Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct PartialReference {
    namespace: Option<Namespace>,
    name: Name,
    revision: Option<Revision>,
}

impl PartialReference {
    /// Create a new partial reference from components.
    pub fn new(
        namespace: impl Into<Option<Namespace>>,
        name: Name,
        revision: impl Into<Option<Revision>>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            name,
            revision: revision.into(),
        }
    }

    /// Access the namespace component if present.
    pub fn namespace(&self) -> Option<&Namespace> {
        self.namespace.as_ref()
    }

    /// Access the name component.
    pub fn name(&self) -> &Name {
        &self.name
    }

    /// Access the revision component if present.
    pub fn revision(&self) -> Option<&Revision> {
        self.revision.as_ref()
    }

    /// Consume the partial reference and return the inner components.
    pub fn into_parts(self) -> (Option<Namespace>, Name, Option<Revision>) {
        (self.namespace, self.name, self.revision)
    }

    /// Convert this partial reference to a full `Reference` by filling in defaults.
    ///
    /// - Missing namespace defaults to global namespace (`_`).
    /// - Missing revision defaults to latest version (`latest`).
    pub fn to_full_reference(&self) -> Reference {
        let namespace = self.namespace.clone().unwrap_or_else(Namespace::global);
        let name = self.name.clone();
        let revision = self.revision.clone().unwrap_or(Revision::Latest);
        Reference::new(namespace, name, revision)
    }
}

impl From<Reference> for PartialReference {
    fn from(value: Reference) -> Self {
        let (namespace, name, revision) = value.into_parts();
        Self {
            namespace: Some(namespace),
            name,
            revision: Some(revision),
        }
    }
}

impl From<PartialReference> for Reference {
    fn from(value: PartialReference) -> Self {
        let namespace = value.namespace.unwrap_or_else(Namespace::global);
        let revision = value.revision.unwrap_or(Revision::Latest);
        Self::new(namespace, value.name, revision)
    }
}

/// Displays the partial reference in `[namespace/]name[@revision]` format.
///
/// # Alternate Format
///
/// Use `{:#}` for compact display. When the revision is a hash, it will be shortened to 7 characters:
///
/// ```no_run
/// format!("{}", name_only);  // "my_dataset"
/// format!("{}", full_ref);   // "my_namespace/my_dataset@b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
/// format!("{:#}", full_ref); // "my_namespace/my_dataset@b94d27b"
/// ```
impl std::fmt::Display for PartialReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(namespace) = &self.namespace {
            write!(f, "{}/", namespace)?;
        }
        write!(f, "{}", self.name)?;
        if let Some(revision) = &self.revision {
            if f.alternate() {
                write!(f, "@{:#}", revision)?;
            } else {
                write!(f, "@{}", revision)?;
            }
        }
        Ok(())
    }
}

impl std::str::FromStr for PartialReference {
    type Err = PartialReferenceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // First, check if there's an '@' for revision
        let (before_at, revision) = if let Some((before, rev_str)) = s.split_once('@') {
            let revision = rev_str
                .parse::<Revision>()
                .map_err(PartialReferenceError::InvalidRevision)?;
            (before, Some(revision))
        } else {
            (s, None)
        };

        // Then check if there's a '/' for namespace
        let (namespace, name) = if let Some((ns_str, name_str)) = before_at.split_once('/') {
            let namespace = ns_str
                .parse::<Namespace>()
                .map_err(PartialReferenceError::InvalidNamespace)?;
            let name = name_str
                .parse::<Name>()
                .map_err(PartialReferenceError::InvalidName)?;
            (Some(namespace), name)
        } else {
            // No namespace, just name
            let name = before_at
                .parse::<Name>()
                .map_err(PartialReferenceError::InvalidName)?;
            (None, name)
        };

        Ok(PartialReference {
            namespace,
            name,
            revision,
        })
    }
}

/// Errors that can occur when parsing a [`PartialReference`] from string.
#[derive(Debug, thiserror::Error)]
pub enum PartialReferenceError {
    /// The namespace component is invalid
    #[error("invalid namespace in partial reference: {0}")]
    InvalidNamespace(#[source] NamespaceError),

    /// The name component is invalid
    #[error("invalid name in partial reference: {0}")]
    InvalidName(#[source] NameError),

    /// The revision component is invalid
    #[error("invalid revision in partial reference: {0}")]
    InvalidRevision(#[source] RevisionParseError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_reference() {
        let input = "_/eth_rpc@1.0.0";
        let result: Result<PartialReference, _> = input.parse();
        assert!(result.is_ok());
        let partial = result.unwrap();
        assert!(partial.namespace.is_some());
        assert_eq!(partial.name.as_str(), "eth_rpc");
        assert!(partial.revision.is_some());
        assert!(partial.revision.unwrap().is_version());
    }

    #[test]
    fn parse_no_revision() {
        let input = "_/eth_rpc";
        let result: Result<PartialReference, _> = input.parse();
        assert!(result.is_ok());
        let partial = result.unwrap();
        assert!(partial.namespace.is_some());
        assert_eq!(partial.name.as_str(), "eth_rpc");
        assert!(partial.revision.is_none());
    }

    #[test]
    fn parse_no_namespace() {
        let input = "eth_rpc@1.0.0";
        let result: Result<PartialReference, _> = input.parse();
        assert!(result.is_ok());
        let partial = result.unwrap();
        assert!(partial.namespace.is_none());
        assert_eq!(partial.name.as_str(), "eth_rpc");
        assert!(partial.revision.is_some());
    }

    #[test]
    fn parse_only_name() {
        let input = "eth_rpc";
        let result: Result<PartialReference, _> = input.parse();
        assert!(result.is_ok());
        let partial = result.unwrap();
        assert!(partial.namespace.is_none());
        assert_eq!(partial.name.as_str(), "eth_rpc");
        assert!(partial.revision.is_none());
    }

    #[test]
    fn display_full_reference() {
        let partial = PartialReference {
            namespace: Some("_".parse().unwrap()),
            name: "eth_rpc".parse().unwrap(),
            revision: Some(Revision::Version("1.0.0".parse().unwrap())),
        };
        assert_eq!(partial.to_string(), "_/eth_rpc@1.0.0");
    }

    #[test]
    fn display_no_revision() {
        let partial = PartialReference {
            namespace: Some("_".parse().unwrap()),
            name: "eth_rpc".parse().unwrap(),
            revision: None,
        };
        assert_eq!(partial.to_string(), "_/eth_rpc");
    }

    #[test]
    fn display_with_hash_revision_default_format_shows_full_hash() {
        //* Given
        let hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        let partial: PartialReference = format!("my_namespace/my_dataset@{}", hash)
            .parse()
            .expect("should parse valid partial reference");

        //* When
        let result = format!("{}", partial);

        //* Then
        assert_eq!(
            result,
            format!("my_namespace/my_dataset@{}", hash),
            "default format should show full 64-character hash in revision"
        );
    }

    #[test]
    fn display_with_hash_revision_alternate_format_shows_short_hash() {
        //* Given
        let hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        let partial: PartialReference = format!("my_namespace/my_dataset@{}", hash)
            .parse()
            .expect("should parse valid partial reference");

        //* When
        let result = format!("{:#}", partial);

        //* Then
        assert_eq!(
            result, "my_namespace/my_dataset@b94d27b",
            "alternate format should show 7-character short hash in revision"
        );
    }
}
