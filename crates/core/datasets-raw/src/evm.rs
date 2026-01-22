use arrow::{
    array::FixedSizeBinaryArray,
    datatypes::{DECIMAL128_MAX_PRECISION, DataType},
};

pub mod helpers;
pub mod tables;

pub type Bytes32 = [u8; 32];
pub type EvmAddress = [u8; 20];
pub type EvmCurrency = i128; // Payment amount in the EVM. Used for gas or value transfers.

pub const BYTES32_TYPE: DataType = DataType::FixedSizeBinary(32);
pub type Bytes32ArrayType = FixedSizeBinaryArray;

pub const EVM_ADDRESS_TYPE: DataType = DataType::FixedSizeBinary(20);
pub type EvmAddressArrayType = FixedSizeBinaryArray;

/// Payment amount in the EVM. Used for gas or value transfers.
pub const EVM_CURRENCY_TYPE: DataType = DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0);
