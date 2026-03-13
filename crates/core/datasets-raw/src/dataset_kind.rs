//! Raw dataset kind types generated via macro.
//!
//! Each dataset kind is a zero-sized type with full serde, display, parse,
//! and comparison support. The `define_dataset_kind!` macro eliminates
//! boilerplate across EVM-RPC, Firehose, and Solana dataset kinds.

use datasets_common::dataset_kind_str::DatasetKindStr;

/// Generates a zero-sized dataset kind type with all standard trait implementations.
///
/// For each invocation, this macro produces:
/// - A zero-sized struct with `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `PartialOrd`, `Ord`, `Hash`
/// - `as_str()` returning the canonical string identifier
/// - `Display`, `FromStr`, `Serialize`, `Deserialize` implementations
/// - `From<T> for DatasetKindStr` conversion
/// - `PartialEq` impls for `str`, `&str`, `String`, `DatasetKindStr`, `&DatasetKindStr`
/// - A dedicated `${Name}Error` type for parse failures
/// - Optional `schemars::JsonSchema` support behind the `schemars` feature
macro_rules! define_dataset_kind {
    (
        $(#[$meta:meta])*
        $vis:vis struct $Name:ident => $kind_str:literal $(,)?
    ) => {
        pastey::paste! {
            $(#[$meta])*
            #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
            $vis struct $Name;

            impl $Name {
                /// Returns the canonical string identifier for this dataset kind.
                #[inline]
                pub const fn as_str(self) -> &'static str {
                    $kind_str
                }
            }

            impl From<$Name> for DatasetKindStr {
                fn from(value: $Name) -> Self {
                    // SAFETY: $Name is a strongly-typed ZST whose Display impl produces
                    // a valid dataset kind string.
                    DatasetKindStr::new_unchecked(value.to_string())
                }
            }

            #[cfg(feature = "schemars")]
            impl schemars::JsonSchema for $Name {
                fn schema_name() -> std::borrow::Cow<'static, str> {
                    std::borrow::Cow::Borrowed(stringify!($Name))
                }

                fn json_schema(_gen: &mut schemars::SchemaGenerator) -> schemars::Schema {
                    let schema_obj = serde_json::json!({
                        "const": $kind_str
                    });
                    serde_json::from_value(schema_obj).unwrap()
                }
            }

            impl std::str::FromStr for $Name {
                type Err = [<$Name Error>];

                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    if s != $kind_str {
                        return Err([<$Name Error>](s.to_string()));
                    }
                    Ok($Name)
                }
            }

            impl std::fmt::Display for $Name {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    $kind_str.fmt(f)
                }
            }

            impl serde::Serialize for $Name {
                fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                where
                    S: serde::Serializer,
                {
                    serializer.serialize_str($kind_str)
                }
            }

            impl<'de> serde::Deserialize<'de> for $Name {
                fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: serde::Deserializer<'de>,
                {
                    let s = String::deserialize(deserializer)?;
                    s.parse().map_err(serde::de::Error::custom)
                }
            }

            impl PartialEq<str> for $Name {
                fn eq(&self, other: &str) -> bool {
                    $kind_str == other
                }
            }

            impl PartialEq<$Name> for str {
                fn eq(&self, _other: &$Name) -> bool {
                    self == $kind_str
                }
            }

            impl PartialEq<&str> for $Name {
                fn eq(&self, other: &&str) -> bool {
                    $kind_str == *other
                }
            }

            impl PartialEq<$Name> for &str {
                fn eq(&self, _other: &$Name) -> bool {
                    *self == $kind_str
                }
            }

            impl PartialEq<String> for $Name {
                fn eq(&self, other: &String) -> bool {
                    $kind_str == other.as_str()
                }
            }

            impl PartialEq<$Name> for String {
                fn eq(&self, _other: &$Name) -> bool {
                    self.as_str() == $kind_str
                }
            }

            impl PartialEq<DatasetKindStr> for $Name {
                fn eq(&self, other: &DatasetKindStr) -> bool {
                    $kind_str == other.as_str()
                }
            }

            impl PartialEq<$Name> for DatasetKindStr {
                fn eq(&self, _other: &$Name) -> bool {
                    self.as_str() == $kind_str
                }
            }

            impl PartialEq<$Name> for &DatasetKindStr {
                fn eq(&self, _other: &$Name) -> bool {
                    self.as_str() == $kind_str
                }
            }

            /// Error returned when parsing an invalid dataset kind string.
            #[derive(Debug, thiserror::Error)]
            #[error("invalid dataset kind: {}, expected: {}", .0, $kind_str)]
            $vis struct [<$Name Error>](String);
        }
    };
}

define_dataset_kind! {
    /// Type-safe representation of the EVM-RPC dataset kind.
    ///
    /// This zero-sized type represents the "evm-rpc" dataset kind, which extracts
    /// blockchain data directly from Ethereum-compatible JSON-RPC endpoints.
    pub struct EvmRpcDatasetKind => "evm-rpc",
}

define_dataset_kind! {
    /// Type-safe representation of the Firehose dataset kind.
    ///
    /// This zero-sized type represents the "firehose" dataset kind, which extracts
    /// blockchain data directly from Firehose endpoints.
    pub struct FirehoseDatasetKind => "firehose",
}

define_dataset_kind! {
    /// Type-safe representation of the Solana dataset kind.
    ///
    /// This zero-sized type represents the "solana" dataset kind, which extracts
    /// blockchain data from Solana sources (Old Faithful archives).
    pub struct SolanaDatasetKind => "solana",
}
