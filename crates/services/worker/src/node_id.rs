/// A worker node ID.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(String);

impl NodeId {
    /// Returns the worker ID as a string slice
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the [`NodeId`] and returns the inner String
    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl PartialEq<String> for NodeId {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<NodeId> for String {
    fn eq(&self, other: &NodeId) -> bool {
        *self == other.0
    }
}

impl PartialEq<str> for NodeId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<NodeId> for str {
    fn eq(&self, other: &NodeId) -> bool {
        *self == other.0
    }
}

impl PartialEq<&str> for NodeId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == **other
    }
}

impl PartialEq<NodeId> for &str {
    fn eq(&self, other: &NodeId) -> bool {
        **self == other.0
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for NodeId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<String> for NodeId {
    type Error = InvalidIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_node_id(&value)?;
        Ok(NodeId(value))
    }
}

impl From<NodeId> for String {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for NodeId {
    type Err = InvalidIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate_node_id(s)?;
        Ok(NodeId(s.to_string()))
    }
}

impl serde::Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.try_into().map_err(serde::de::Error::custom)
    }
}

/// Validates that a worker node ID follows the required format:
/// - Must start with a letter
/// - Can only contain alphanumeric characters, underscores, hyphens, and dots
/// - Must not be empty
fn validate_node_id(id: &str) -> Result<(), InvalidIdError> {
    if id.is_empty() {
        return Err(InvalidIdError {
            id: id.to_string(),
            reason: "empty string".into(),
        });
    }

    if let Some(c) = id.chars().next()
        && !c.is_alphabetic()
    {
        return Err(InvalidIdError {
            id: id.to_string(),
            reason: "must start with a letter".into(),
        });
    }

    if let Some(c) = id
        .chars()
        .find(|c| !c.is_alphanumeric() && *c != '_' && *c != '-' && *c != '.')
    {
        return Err(InvalidIdError {
            id: id.to_string(),
            reason: format!("invalid character '{c}'").into(),
        });
    }

    Ok(())
}

impl From<metadata_db::WorkerNodeIdOwned> for NodeId {
    fn from(value: metadata_db::WorkerNodeIdOwned) -> Self {
        // Convert to string - this should always pass since WorkerNodeIdOwned
        // comes from the database and should already be valid
        NodeId(value.into_inner())
    }
}

impl From<NodeId> for metadata_db::WorkerNodeIdOwned {
    fn from(value: NodeId) -> Self {
        // SAFETY: NodeId is validated at construction via TryFrom/FromStr, ensuring invariants are upheld.
        metadata_db::WorkerNodeId::from_owned_unchecked(value.0)
    }
}

impl<'a> From<&'a NodeId> for metadata_db::WorkerNodeId<'a> {
    fn from(value: &'a NodeId) -> Self {
        // SAFETY: NodeId is validated at construction via TryFrom/FromStr, ensuring invariants are upheld.
        metadata_db::WorkerNodeId::from_ref_unchecked(&value.0)
    }
}

/// Error returned when a worker ID is invalid.
#[derive(Debug, thiserror::Error)]
#[error("Invalid worker ID '{id}': {reason}")]
pub struct InvalidIdError {
    id: String,
    #[source]
    reason: Box<dyn std::error::Error + Send + Sync>,
}
