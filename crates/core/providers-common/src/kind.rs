/// A type-erased provider kind identifier.
///
/// This is a string wrapper representing the kind of a provider (e.g., `"evm-rpc"`,
/// `"solana"`, `"firehose"`, etc.). It provides a common type for provider kinds across
/// different provider crates without requiring dependencies on specific providers.
///
/// Each provider crate defines its own strongly-typed kind (e.g., `EvmRpcProviderKind`)
/// that can be converted to [`ProviderKindStr`] via the `From` trait.
///
/// # Invariants
///
/// Provider kind strings must be non-empty. This invariant is enforced at construction time
/// through the [`FromStr`] implementation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProviderKindStr(String);

impl ProviderKindStr {
    /// Creates a new [`ProviderKindStr`] without validation.
    ///
    /// # Safety
    ///
    /// The caller must ensure the string is non-empty to maintain type invariants.
    /// Violating this invariant may lead to logic errors in code that assumes
    /// provider kinds are always non-empty.
    ///
    /// For validated construction, use [`FromStr`] instead.
    pub fn new_unchecked(kind: String) -> Self {
        Self(kind)
    }

    /// Returns the provider kind as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ProviderKindStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for ProviderKindStr {
    type Err = ProviderKindStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate_provider_kind_str(s)?;
        Ok(Self(s.to_string()))
    }
}

impl serde::Serialize for ProviderKindStr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for ProviderKindStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

impl PartialEq<&str> for ProviderKindStr {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<ProviderKindStr> for &str {
    fn eq(&self, other: &ProviderKindStr) -> bool {
        *self == other.0
    }
}

/// Validates that a provider kind string is non-empty.
///
/// Checks:
/// - Not empty
pub fn validate_provider_kind_str(kind: &str) -> Result<(), ProviderKindStrError> {
    if kind.is_empty() {
        return Err(ProviderKindStrError);
    }
    Ok(())
}

/// Error type for [`ProviderKindStr`] parsing failures.
///
/// This error is returned when attempting to parse an empty string as a provider kind.
#[derive(Debug, thiserror::Error)]
#[error("provider kind cannot be empty")]
pub struct ProviderKindStrError;
