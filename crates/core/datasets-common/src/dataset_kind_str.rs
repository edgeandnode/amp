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
    /// Creates a new [`DatasetKindStr`] from a string identifier without validation.
    ///
    /// # Safety
    /// The caller must ensure the provided string is a valid dataset kind identifier
    /// (e.g., originates from a strongly-typed ZST kind or a trusted database value).
    pub fn new_unchecked(kind: String) -> Self {
        Self(kind)
    }

    /// Returns the dataset kind as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the [`DatasetKindStr`] and returns the inner [`String`].
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for DatasetKindStr {
    fn as_ref(&self) -> &str {
        self.0.as_str()
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

#[cfg(feature = "metadata-db")]
impl From<metadata_db::manifests::ManifestKindOwned> for DatasetKindStr {
    fn from(value: metadata_db::manifests::ManifestKindOwned) -> Self {
        // SAFETY: ManifestKindOwned values originate from the database, which only stores
        // validated kind strings inserted at system boundaries.
        DatasetKindStr::new_unchecked(value.into_inner())
    }
}

#[cfg(feature = "metadata-db")]
impl From<DatasetKindStr> for metadata_db::manifests::ManifestKindOwned {
    fn from(value: DatasetKindStr) -> Self {
        // SAFETY: DatasetKindStr values originate from validated domain types (ZST kind types),
        // so invariants are upheld.
        metadata_db::manifests::ManifestKind::from_owned_unchecked(value.0)
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> From<&'a DatasetKindStr> for metadata_db::manifests::ManifestKind<'a> {
    fn from(value: &'a DatasetKindStr) -> Self {
        // SAFETY: DatasetKindStr values originate from validated domain types (ZST kind types),
        // so invariants are upheld.
        metadata_db::manifests::ManifestKind::from_ref_unchecked(value.as_str())
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<metadata_db::manifests::ManifestKind<'a>> for DatasetKindStr {
    fn eq(&self, other: &metadata_db::manifests::ManifestKind<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<DatasetKindStr> for metadata_db::manifests::ManifestKind<'a> {
    fn eq(&self, other: &DatasetKindStr) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<metadata_db::manifests::ManifestKind<'a>> for &DatasetKindStr {
    fn eq(&self, other: &metadata_db::manifests::ManifestKind<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> PartialEq<&DatasetKindStr> for metadata_db::manifests::ManifestKind<'a> {
    fn eq(&self, other: &&DatasetKindStr) -> bool {
        self.as_str() == other.as_str()
    }
}
