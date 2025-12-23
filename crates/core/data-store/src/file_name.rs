/// Block number type alias.
pub type BlockNum = u64;

/// A validated file name for parquet files.
///
/// File names must be non-empty and not exceed filesystem limits.
/// This type validates at system boundaries and converts to/from the
/// database transport type `metadata_db::files::FileName`.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct FileName(String);

impl FileName {
    /// Create a new parquet filename with a specific suffix.
    ///
    /// The filename format is `{block_num:09}-{suffix:016x}.parquet`:
    /// - Block number padded to 9 digits for lexicographical sorting
    /// - Provided suffix formatted as 16-char hex
    pub fn new(start: BlockNum, suffix: u64) -> Self {
        Self(format!("{:09}-{:016x}.parquet", start, suffix))
    }

    /// Create a new parquet filename with a random suffix.
    ///
    /// Generates a random 64-bit suffix to avoid naming conflicts
    /// from chain reorgs.
    ///
    /// The filename format is `{block_num:09}-{suffix:016x}.parquet`:
    /// - Block number padded to 9 digits for lexicographical sorting
    /// - 64-bit random hex suffix to avoid conflicts
    ///
    /// Example: `000000001-a1b2c3d4e5f6g7h8.parquet`
    pub fn new_with_random_suffix(start: BlockNum) -> Self {
        use rand::RngCore as _;
        Self::new(start, rand::rng().next_u64())
    }

    /// Create a new FileName from a String without validation.
    ///
    /// This constructor trusts that the caller provides a valid filename.
    pub fn new_unchecked(name: String) -> Self {
        Self(name)
    }

    /// Returns a reference to the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the FileName and returns the inner String.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for FileName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<FileName> for FileName {
    #[inline(always)]
    fn as_ref(&self) -> &FileName {
        self
    }
}

impl std::ops::Deref for FileName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<str> for FileName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<FileName> for str {
    fn eq(&self, other: &FileName) -> bool {
        *self == other.0
    }
}

impl PartialEq<&str> for FileName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == **other
    }
}

impl PartialEq<String> for FileName {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<FileName> for String {
    fn eq(&self, other: &FileName) -> bool {
        *self == other.0
    }
}

impl std::fmt::Display for FileName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<metadata_db::files::FileNameOwned> for FileName {
    fn from(value: metadata_db::files::FileNameOwned) -> Self {
        // Convert to string and wrap - this should always be valid since FileNameOwned
        // comes from the database and should already be validated
        FileName(value.into_inner())
    }
}

impl From<FileName> for metadata_db::files::FileNameOwned {
    fn from(value: FileName) -> Self {
        // SAFETY: FileName maintains invariants through its constructor. It is the constructor's
        // responsibility to ensure invariants hold at creation time.
        metadata_db::files::FileName::from_owned_unchecked(value.0)
    }
}

impl<'a> From<&'a FileName> for metadata_db::files::FileName<'a> {
    fn from(value: &'a FileName) -> Self {
        // SAFETY: FileName maintains invariants through its constructor. It is the constructor's
        // responsibility to ensure invariants hold at creation time.
        metadata_db::files::FileName::from_ref_unchecked(&value.0)
    }
}

impl<'a> PartialEq<metadata_db::files::FileName<'a>> for FileName {
    fn eq(&self, other: &metadata_db::files::FileName<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> PartialEq<FileName> for metadata_db::files::FileName<'a> {
    fn eq(&self, other: &FileName) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> PartialEq<&metadata_db::files::FileName<'a>> for FileName {
    fn eq(&self, other: &&metadata_db::files::FileName<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> PartialEq<FileName> for &metadata_db::files::FileName<'a> {
    fn eq(&self, other: &FileName) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> PartialEq<&FileName> for metadata_db::files::FileName<'a> {
    fn eq(&self, other: &&FileName) -> bool {
        self.as_str() == other.as_str()
    }
}

impl<'a> PartialEq<metadata_db::files::FileName<'a>> for &FileName {
    fn eq(&self, other: &metadata_db::files::FileName<'a>) -> bool {
        self.as_str() == other.as_str()
    }
}

impl serde::Serialize for FileName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for FileName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // SAFETY: Serialized filenames originate from our constructors
        // and are stored in trusted sources (database, parquet metadata).
        Ok(Self::new_unchecked(String::deserialize(deserializer)?))
    }
}
