use std::{fmt, str::FromStr};

use amp_providers_common::kind::ProviderKindStr;

/// The canonical string identifier for Solana providers.
///
/// This constant defines the string representation used in provider configurations
/// to identify providers that interact with Solana RPC endpoints.
const PROVIDER_KIND: &str = "solana";

/// Zero-sized type representing the Solana provider kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(inline))]
#[cfg_attr(
    feature = "schemars",
    schemars(schema_with = "solana_provider_kind_schema")
)]
pub struct SolanaProviderKind;

impl SolanaProviderKind {
    /// Returns the canonical string identifier for this provider kind.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        PROVIDER_KIND
    }
}

impl AsRef<str> for SolanaProviderKind {
    fn as_ref(&self) -> &str {
        PROVIDER_KIND
    }
}

impl From<SolanaProviderKind> for ProviderKindStr {
    fn from(value: SolanaProviderKind) -> Self {
        // SAFETY: The constant PROVIDER_KIND is "solana", which is non-empty
        ProviderKindStr::new_unchecked(value.to_string())
    }
}

#[cfg(feature = "schemars")]
fn solana_provider_kind_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
    let schema_obj = serde_json::json!({
        "const": PROVIDER_KIND
    });
    serde_json::from_value(schema_obj).unwrap()
}

impl FromStr for SolanaProviderKind {
    type Err = SolanaProviderKindError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == PROVIDER_KIND {
            Ok(SolanaProviderKind)
        } else {
            Err(SolanaProviderKindError::InvalidKind(s.to_string()))
        }
    }
}

impl fmt::Display for SolanaProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl serde::Serialize for SolanaProviderKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for SolanaProviderKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

// PartialEq implementations for comparison with string types
impl PartialEq<str> for SolanaProviderKind {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for SolanaProviderKind {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for SolanaProviderKind {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<ProviderKindStr> for SolanaProviderKind {
    fn eq(&self, other: &ProviderKindStr) -> bool {
        PROVIDER_KIND == other.as_str()
    }
}

// Reverse comparisons
impl PartialEq<SolanaProviderKind> for str {
    fn eq(&self, other: &SolanaProviderKind) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<SolanaProviderKind> for &str {
    fn eq(&self, other: &SolanaProviderKind) -> bool {
        *self == other.as_str()
    }
}

impl PartialEq<SolanaProviderKind> for String {
    fn eq(&self, other: &SolanaProviderKind) -> bool {
        self.as_str() == other.as_str()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SolanaProviderKindError {
    #[error("invalid Solana provider kind: {0}")]
    InvalidKind(String),
}
