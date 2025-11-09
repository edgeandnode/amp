//! Partial dataset reference types with optional namespace and revision.
//!
//! This module provides the `PartialReference` type that represents dataset references
//! where the namespace and/or revision may be omitted. This is useful for parsing
//! user input where these components are optional.

use std::str::FromStr;

use crate::{
    name::{Name, NameError},
    namespace::{Namespace, NamespaceError},
    reference::Reference,
    revision::{Revision, RevisionParseError},
};

const GLOBAL_NAMESPACE: &str = "_";

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
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct PartialReference {
    pub namespace: Option<Namespace>,
    pub name: Name,
    pub revision: Option<Revision>,
}

impl PartialReference {
    /// Create a new partial reference from components.
    pub fn new(namespace: Option<Namespace>, name: Name, revision: Option<Revision>) -> Self {
        Self {
            namespace,
            name,
            revision,
        }
    }

    pub fn name(&self) -> &Name {
        &self.name
    }

    pub fn revision_or_latest(&self) -> &Revision {
        self.revision.as_ref().unwrap_or(&Revision::Latest)
    }

    /// Returns the namespace if present, or the global namespace otherwise.
    pub fn namespace_or_global(&self) -> Namespace {
        self.namespace
            .clone()
            .unwrap_or_else(|| Namespace::from_str(GLOBAL_NAMESPACE).unwrap())
    }

    /// Convert this partial reference to a full `Reference` by filling in defaults.
    ///
    /// - Missing namespace defaults to `_` (global namespace)
    /// - Missing revision defaults to `latest`
    ///
    /// # Example
    /// ```ignore
    /// let partial = "my_dataset@1.0.0".parse::<PartialReference>().unwrap();
    /// let full = partial.to_full_reference();
    /// // full is now: _/my_dataset@1.0.0
    /// ```
    pub fn to_full_reference(&self) -> crate::reference::Reference {
        crate::reference::Reference::new(
            self.namespace_or_global(),
            self.name.clone(),
            self.revision_or_latest().clone(),
        )
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

impl From<&Reference> for PartialReference {
    fn from(value: &Reference) -> Self {
        Self::from(value.clone())
    }
}

impl std::fmt::Display for PartialReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(namespace) = &self.namespace {
            write!(f, "{}/", namespace)?;
        }
        write!(f, "{}", self.name)?;
        if let Some(revision) = &self.revision {
            write!(f, "@{}", revision)?;
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
}
