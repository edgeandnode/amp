//! Worker node ID new-type wrapper for database values
//!
//! This module provides a [`NodeId`] new-type wrapper around [`Cow<str>`] that maintains
//! worker node ID invariants for database operations. The type provides efficient handling
//! with support for both borrowed and owned strings.
//!
//! ## Validation Strategy
//!
//! This type **maintains invariants but does not validate** input data. Validation occurs
//! at system boundaries through types like [`worker::NodeId`], which enforce the required
//! format before converting into this database-layer type. Database values are trusted as
//! already valid, following the principle of "validate at boundaries, trust database data."
//!
//! Types that convert into [`NodeId`] are responsible for ensuring invariants are met:
//! - Worker node IDs must start with a letter (`a-z`, `A-Z`)
//! - Worker node IDs must contain only alphanumeric characters, underscores, hyphens, and dots
//! - Worker node IDs must not be empty

use std::borrow::Cow;

/// An owned worker node ID type for database return values and owned storage scenarios.
///
/// This is a type alias for `NodeId<'static>`, specifically intended for use as a return type from
/// database queries or in any context where a worker node ID with owned storage is required.
/// Prefer this alias when working with IDs that need to be stored or returned from the database,
/// rather than just representing a worker node ID with owned storage in general.
pub type NodeIdOwned = NodeId<'static>;

/// A worker node ID wrapper for database values.
///
/// This new-type wrapper around `Cow<str>` maintains worker node ID invariants for database
/// operations. It supports both borrowed and owned strings through copy-on-write semantics,
/// enabling efficient handling without unnecessary allocations.
///
/// The type trusts that values are already validated. Validation must occur at system
/// boundaries before conversion into this type.
///
/// ## Format Requirements
///
/// A valid worker node ID must:
/// - **Start** with a letter (`a-z`, `A-Z`)
/// - **Contain** only alphanumeric characters, underscores (`_`), hyphens (`-`), and dots (`.`)
/// - **Not be empty** (minimum length of 1 character)
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId<'a>(Cow<'a, str>);

impl<'a> NodeId<'a> {
    /// Create a new NodeId wrapper from a reference to str (borrowed)
    ///
    /// # Safety
    /// The caller must ensure the provided ID upholds the worker node ID invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_ref_unchecked(id: &'a str) -> Self {
        Self(Cow::Borrowed(id))
    }

    /// Create a new NodeId wrapper from an owned String
    ///
    /// # Safety
    /// The caller must ensure the provided ID upholds the worker node ID invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_owned_unchecked(id: String) -> NodeIdOwned {
        NodeId(Cow::Owned(id))
    }

    /// Consume and return the inner String (owned)
    pub fn into_inner(self) -> String {
        match self {
            NodeId(Cow::Owned(id)) => id,
            NodeId(Cow::Borrowed(id)) => id.to_owned(),
        }
    }

    /// Get an owned version of this NodeId
    pub fn to_owned(&self) -> NodeIdOwned {
        // SAFETY: Source already upholds invariants; conversion maintains them.
        Self::from_owned_unchecked(self.0.to_string())
    }

    /// Get a reference to the inner str
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'a> std::ops::Deref for NodeId<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> AsRef<str> for NodeId<'a> {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl<'a> PartialEq<&str> for NodeId<'a> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl<'a> PartialEq<NodeId<'a>> for &str {
    fn eq(&self, other: &NodeId<'a>) -> bool {
        *self == other.as_str()
    }
}

impl<'a> std::fmt::Display for NodeId<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> std::fmt::Debug for NodeId<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl sqlx::Type<sqlx::Postgres> for NodeId<'_> {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl<'a> sqlx::Encode<'_, sqlx::Postgres> for NodeId<'a> {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for NodeIdOwned {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        // SAFETY: Database values are trusted to uphold invariants; validation occurs at boundaries before insertion.
        Ok(NodeId::from_owned_unchecked(s))
    }
}

impl<'a> From<&'a str> for NodeId<'a> {
    fn from(s: &'a str) -> Self {
        // SAFETY: Internal use within metadata-db where caller ensures invariants are upheld.
        NodeId::from_ref_unchecked(s)
    }
}

impl From<String> for NodeIdOwned {
    fn from(s: String) -> Self {
        // SAFETY: Internal use within metadata-db where caller ensures invariants are upheld.
        NodeId::from_owned_unchecked(s)
    }
}

impl<'a> From<&'a NodeId<'a>> for NodeId<'a> {
    fn from(value: &'a NodeId<'a>) -> Self {
        // Create a borrowed Cow variant pointing to the data inside the input ID.
        // This works for both Cow::Borrowed and Cow::Owned without cloning the underlying data.
        // SAFETY: The input ID already upholds invariants, so the referenced data is valid.
        NodeId::from_ref_unchecked(value.as_ref())
    }
}
