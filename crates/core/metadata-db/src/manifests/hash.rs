//! Dataset version hash new-type wrapper for database values
//!
//! This module provides a [`Hash`] new-type wrapper around `[u8; 64]` that maintains
//! version hash invariants for database operations. The type stores the hash as raw bytes
//! internally and converts to/from hex strings at system boundaries.
//!
//! ## Validation Strategy
//!
//! This type **validates input data** when constructing from strings. Version hashes must:
//! - Be exactly 64 characters long (64 hex digits representing 32 bytes)
//! - Contain only valid hex digits (`0-9`, `a-f`, `A-F`)
//!
//! Database values are trusted as already valid, following the principle of "validate at
//! boundaries, trust database data."

use hex::FromHexError;

/// Error type for Hash validation failures
#[derive(Debug, thiserror::Error)]
pub enum HashError {
    /// Hash string must be exactly 64 hex characters
    #[error("Hash must be exactly 64 hex characters, got {0} characters")]
    InvalidLength(usize),
    /// Hash contains invalid hex characters
    #[error("Hash contains invalid hex characters: {0}")]
    InvalidHex(#[from] FromHexError),
}

/// A version hash wrapper for database values.
///
/// This new-type wrapper around `[u8; 64]` maintains version hash invariants for database
/// operations. It stores the hash as raw bytes internally and converts to/from hex strings
/// at system boundaries.
///
/// The type validates input when constructing from strings and trusts database values.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hash([u8; 64]);

impl Hash {
    /// Create a new Hash from a hex string (borrowed or owned)
    ///
    /// This method validates that the input is exactly 64 hex characters.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The string is not exactly 64 characters long
    /// - The string contains non-hex characters
    pub fn from_hex(hash: impl AsRef<str>) -> Result<Self, HashError> {
        let hash_str = hash.as_ref();
        if hash_str.len() != 64 {
            return Err(HashError::InvalidLength(hash_str.len()));
        }

        let mut bytes = [0u8; 64];
        // We validate hex and store the ASCII hex characters directly
        for (i, c) in hash_str.chars().enumerate() {
            if !c.is_ascii_hexdigit() {
                return Err(HashError::InvalidHex(FromHexError::InvalidHexCharacter {
                    c,
                    index: i,
                }));
            }
            bytes[i] = c as u8;
        }

        Ok(Self(bytes))
    }

    /// Get a reference to the raw bytes
    pub fn as_bytes(&self) -> &[u8; 64] {
        &self.0
    }

    pub fn as_str(&self) -> &str {
        self.as_ref()
    }
}

impl AsRef<str> for Hash {
    fn as_ref(&self) -> &str {
        // SAFETY: bytes are guaranteed to be valid ASCII hex digits
        std::str::from_utf8(&self.0).unwrap()
    }
}

impl PartialEq<&str> for Hash {
    fn eq(&self, other: &&str) -> bool {
        self.as_ref() == *other
    }
}

impl PartialEq<Hash> for &str {
    fn eq(&self, other: &Hash) -> bool {
        *self == other.as_ref()
    }
}

impl PartialEq<str> for Hash {
    fn eq(&self, other: &str) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq<Hash> for str {
    fn eq(&self, other: &Hash) -> bool {
        self == other.as_ref()
    }
}

impl PartialEq<String> for Hash {
    fn eq(&self, other: &String) -> bool {
        self.as_ref() == other.as_str()
    }
}

impl PartialEq<Hash> for String {
    fn eq(&self, other: &Hash) -> bool {
        self.as_str() == other.as_ref()
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::fmt::Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Hash").field(&self.as_ref()).finish()
    }
}

impl sqlx::Type<sqlx::Postgres> for Hash {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl sqlx::Encode<'_, sqlx::Postgres> for Hash {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Postgres as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let s = self.as_ref();
        <&str as sqlx::Encode<'_, sqlx::Postgres>>::encode_by_ref(&s, buf)
    }
}

impl sqlx::Decode<'_, sqlx::Postgres> for Hash {
    fn decode(value: sqlx::postgres::PgValueRef<'_>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(Hash::from_hex(&s)?)
    }
}
