//! EVM-specific types and Arrow schema definitions for blockchain data.
//!
//! This module provides Rust type aliases and corresponding Arrow [`DataType`] constants
//! for representing common EVM data structures in columnar format.

use arrow::{
    array::FixedSizeBinaryArray,
    datatypes::{DECIMAL128_MAX_PRECISION, DataType},
};

/// Helper utilities for EVM data processing.
pub mod helpers;
/// Table definitions for EVM blockchain data (blocks, transactions, logs, etc.).
pub mod tables;

/// A 32-byte array representing EVM hashes (e.g., transaction hashes, block hashes).
pub type Bytes32 = [u8; 32];

/// A 20-byte array representing an Ethereum address.
pub type EvmAddress = [u8; 20];

/// Payment amount in the EVM. Used for gas or value transfers.
pub type EvmCurrency = i128;

/// Arrow [`DataType`] for 32-byte fixed binary columns (hashes, etc.).
pub const BYTES32_TYPE: DataType = DataType::FixedSizeBinary(32);

/// Arrow array type for [`Bytes32`] columns.
pub type Bytes32ArrayType = FixedSizeBinaryArray;

/// Arrow [`DataType`] for 20-byte fixed binary columns (addresses).
pub const EVM_ADDRESS_TYPE: DataType = DataType::FixedSizeBinary(20);

/// Arrow array type for [`EvmAddress`] columns.
pub type EvmAddressArrayType = FixedSizeBinaryArray;

/// Arrow [`DataType`] for EVM currency values (gas, value transfers).
///
/// Uses [`Decimal128`](DataType::Decimal128) with maximum precision and zero scale
/// to represent wei values without loss of precision.
pub const EVM_CURRENCY_TYPE: DataType = DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0);
