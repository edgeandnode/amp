pub use datafusion::arrow;

use arrow::datatypes::{DataType, Schema};
use datafusion::arrow::datatypes::DECIMAL128_MAX_PRECISION;

pub type BlockNum = u64;
pub type Bytes32 = [u8; 32];
pub type Bytes = Box<[u8]>;
pub type EvmAddress = [u8; 20];
pub type EvmCurrency = i128; // Payment amount in the EVM. Used for gas or value transfers.

pub const BYTES32_TYPE: DataType = DataType::FixedSizeBinary(32);
pub const EVM_ADDRESS_TYPE: DataType = DataType::FixedSizeBinary(20);
pub const EVM_CURRENCY_TYPE: DataType = DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0);

pub trait Table {
    fn schema() -> Schema;
}
