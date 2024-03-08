use std::sync::Arc;

pub use datafusion::arrow;

use arrow::datatypes::{DataType, Schema};
use datafusion::{
    arrow::datatypes::DECIMAL128_MAX_PRECISION,
    common::{parsers::CompressionTypeVariant, Constraints},
    logical_expr::{CreateExternalTable, DdlStatement, LogicalPlan},
};

pub type BlockNum = u64;
pub type Bytes32 = [u8; 32];
pub type Bytes = Box<[u8]>;
pub type EvmAddress = [u8; 20];
pub type EvmCurrency = i128; // Payment amount in the EVM. Used for gas or value transfers.

pub const BYTES32_TYPE: DataType = DataType::FixedSizeBinary(32);
pub const EVM_ADDRESS_TYPE: DataType = DataType::FixedSizeBinary(20);
pub const EVM_CURRENCY_TYPE: DataType = DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0);

pub trait Table {
    const TABLE_NAME: &'static str;

    fn schema() -> Schema;
}

pub fn create_table_at<T: Table>(location: String) -> LogicalPlan {
    let schema = T::schema();
    let command = CreateExternalTable {
        // This tables are not really external, as we will ingest and store them in our preferred format.
        // And we like Parquet.
        file_type: "PARQUET".to_string(),
        name: T::TABLE_NAME.into(),

        // Unwrap: The schema is static, any panics would be caught in basic testing.
        schema: Arc::new(schema.try_into().unwrap()),

        location,

        // Up to our preference, but maybe it's more robust for the caller to check existence.
        if_not_exists: false,

        // Assume parquet, which has native compression, so we don't need to compress the file.
        file_compression_type: CompressionTypeVariant::UNCOMPRESSED,

        // Wen streaming?
        unbounded: false,

        // Things we don't currently use.
        table_partition_cols: vec![],
        order_exprs: vec![],
        options: Default::default(),
        constraints: Constraints::empty(),
        column_defaults: Default::default(),
        definition: None,

        // CSV specific
        has_header: false,
        delimiter: ',',
    };

    LogicalPlan::Ddl(DdlStatement::CreateExternalTable(command))
}
