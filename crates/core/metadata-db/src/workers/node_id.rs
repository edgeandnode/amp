//! Worker node ID type wrapper for efficient storage and manipulation
//!
//! This module provides a [`NodeId`] _new-type_ wrapper around [`Cow<str>`] that ensures
//! efficient handling of worker node IDs with support for both borrowed and owned strings.

use std::borrow::Cow;

/// An owned worker node ID type for database return values and owned storage scenarios.
///
/// This is a type alias for `NodeId<'static>`, specifically intended for use as a return type from
/// database queries or in any context where a worker node ID with owned storage is required.
/// Prefer this alias when working with IDs that need to be stored or returned from the database,
/// rather than just representing a worker node ID with owned storage in general.
pub type NodeIdOwned = NodeId<'static>;

/// A worker node ID wrapper that provides efficient string handling.
///
/// This _new-type_ wrapper around `Cow<str>` provides efficient handling of worker node IDs
/// with support for both borrowed and owned strings. It can handle both owned and borrowed
/// IDs efficiently through the use of copy-on-write semantics.
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
    pub fn from_ref(id: &'a str) -> Self {
        Self(Cow::Borrowed(id))
    }

    /// Create a new NodeId wrapper from an owned String
    pub fn from_owned(id: String) -> NodeIdOwned {
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
        Self::from_owned(self.0.to_string())
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
        Ok(NodeId::from_owned(s))
    }
}

impl<'a> From<&'a str> for NodeId<'a> {
    fn from(s: &'a str) -> Self {
        NodeId::from_ref(s)
    }
}

impl From<String> for NodeIdOwned {
    fn from(s: String) -> Self {
        NodeId::from_owned(s)
    }
}

impl<'a> From<&'a NodeIdOwned> for NodeId<'a> {
    fn from(id: &'a NodeIdOwned) -> Self {
        NodeId::from_ref(id.as_str())
    }
}

impl serde::Serialize for NodeId<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_str().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for NodeIdOwned {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(NodeId::from_owned(s))
    }
}
