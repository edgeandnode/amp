/// A type-erased dataset kind identifier.
///
/// This is a string wrapper representing the kind of a dataset (e.g., `"evm-rpc"`,
/// `"solana"`, `"manifest"` (for derived), etc.). It provides a common type for dataset kinds across
/// different extractor crates without requiring dependencies on specific extractors.
///
/// Each extractor crate defines its own strongly-typed kind (e.g., `EvmRpcDatasetKind`)
/// that can be converted to [`DatasetKindStr`] via the `From` trait.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct DatasetKindStr(String);

impl DatasetKindStr {
    /// Creates a new [`DatasetKindStr`] from a string identifier.
    pub fn new(kind: String) -> Self {
        Self(kind)
    }

    /// Returns the dataset kind as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DatasetKindStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for DatasetKindStr {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

impl PartialEq<&str> for DatasetKindStr {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<DatasetKindStr> for &str {
    fn eq(&self, other: &DatasetKindStr) -> bool {
        *self == other.0
    }
}
