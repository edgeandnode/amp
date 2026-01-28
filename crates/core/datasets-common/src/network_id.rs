//! Network ID validation and types.
//!
//! This module provides the `NetworkId` type for validated network identifiers that enforce
//! basic constraints required across the system.

/// A validated network identifier that enforces basic constraints.
///
/// Network identifiers are used to distinguish between different blockchain networks
/// (e.g., "mainnet", "base", "arbitrum-one"). This type provides compile-time guarantees
/// that all instances contain valid network identifiers.
///
/// Network IDs should follow the values defined in The Graph's networks registry:
/// <https://github.com/graphprotocol/networks-registry/blob/main/docs/networks-table.md>
///
/// ## Format Requirements
///
/// A valid network identifier must:
/// - **Not be empty** (minimum length of 1 character)
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schemars", schemars(transparent))]
pub struct NetworkId(#[cfg_attr(feature = "schemars", schemars(length(min = 1)))] String);

impl NetworkId {
    /// Returns a reference to the inner string value
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the NetworkId and returns the inner String
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl PartialEq<String> for NetworkId {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<NetworkId> for String {
    fn eq(&self, other: &NetworkId) -> bool {
        *self == other.0
    }
}

impl PartialEq<str> for NetworkId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<NetworkId> for str {
    fn eq(&self, other: &NetworkId) -> bool {
        *self == other.0
    }
}

impl PartialEq<&str> for NetworkId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == **other
    }
}

impl PartialEq<NetworkId> for &str {
    fn eq(&self, other: &NetworkId) -> bool {
        **self == other.0
    }
}

impl AsRef<NetworkId> for NetworkId {
    #[inline(always)]
    fn as_ref(&self) -> &NetworkId {
        self
    }
}

impl TryFrom<String> for NetworkId {
    type Error = InvalidNetworkIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_network_id(&value)?;
        Ok(NetworkId(value))
    }
}

impl std::fmt::Display for NetworkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for NetworkId {
    type Err = InvalidNetworkIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate_network_id(s)?;
        Ok(NetworkId(s.to_string()))
    }
}

impl serde::Serialize for NetworkId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for NetworkId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.try_into().map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "bincode")]
impl bincode::Encode for NetworkId {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        bincode::Encode::encode(&self.0, encoder)
    }
}

#[cfg(feature = "bincode")]
impl<C> bincode::Decode<C> for NetworkId {
    fn decode<D: bincode::de::Decoder<Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let s = String::decode(decoder)?;
        s.try_into()
            .map_err(|err| bincode::error::DecodeError::OtherString(format!("{err}")))
    }
}

#[cfg(feature = "bincode")]
impl<'de, C> bincode::BorrowDecode<'de, C> for NetworkId {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let s = String::borrow_decode(decoder)?;
        s.try_into()
            .map_err(|err| bincode::error::DecodeError::OtherString(format!("{err}")))
    }
}

/// Validates that a network identifier is not empty.
pub fn validate_network_id(network_id: &str) -> Result<(), InvalidNetworkIdError> {
    if network_id.is_empty() {
        return Err(InvalidNetworkIdError);
    }

    Ok(())
}

/// Error type for [`NetworkId`] parsing failures
#[derive(Debug, thiserror::Error)]
#[error("network id cannot be empty")]
pub struct InvalidNetworkIdError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str_with_mainnet_succeeds() {
        //* Given
        let network_str = "mainnet";

        //* When
        let result = network_str.parse::<NetworkId>();

        //* Then
        let network_id = result.expect("mainnet should parse successfully");
        assert_eq!(network_id.as_str(), "mainnet");
    }

    #[test]
    fn from_str_with_empty_string_fails() {
        //* Given
        let network_str = "";

        //* When
        let result = network_str.parse::<NetworkId>();

        //* Then
        assert!(result.is_err(), "empty string should fail to parse");
    }

    #[test]
    fn serialize_with_avalanche_returns_json_string() {
        //* Given
        let network_str = "avalanche";
        let network_id: NetworkId = network_str
            .parse()
            .expect("avalanche should parse successfully");

        //* When
        let serialized = serde_json::to_string(&network_id).expect("serialization should succeed");

        //* Then
        assert_eq!(serialized, r#""avalanche""#);
    }

    #[test]
    fn deserialize_with_valid_json_succeeds() {
        //* Given
        let json = r#""bsc""#;

        //* When
        let result = serde_json::from_str::<NetworkId>(json);

        //* Then
        let network_id = result.expect("deserialization should succeed");
        assert_eq!(network_id.as_str(), "bsc");
    }

    #[test]
    fn deserialize_with_empty_string_fails() {
        //* Given
        let json = r#""""#;

        //* When
        let result = serde_json::from_str::<NetworkId>(json);

        //* Then
        assert!(result.is_err(), "empty string should fail deserialization");
    }
}
