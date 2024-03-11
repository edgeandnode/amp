pub mod arrow_helpers;

pub use datafusion::arrow;

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion::{
    arrow::datatypes::DECIMAL128_MAX_PRECISION,
    common::{parsers::CompressionTypeVariant, Constraints},
    logical_expr::{CreateExternalTable, DdlStatement, LogicalPlan},
    sql::TableReference,
};

pub type BlockNum = u64;
pub type Bytes32 = [u8; 32];
pub type Bytes = Box<[u8]>;
pub type EvmAddress = [u8; 20];
pub type EvmCurrency = i128; // Payment amount in the EVM. Used for gas or value transfers.

pub const BYTES32_TYPE: DataType = DataType::FixedSizeBinary(32);
pub type Bytes32ArrayType = arrow::array::FixedSizeBinaryArray;

pub const EVM_ADDRESS_TYPE: DataType = DataType::FixedSizeBinary(20);
pub type EvmAddressArrayType = arrow::array::FixedSizeBinaryArray;

pub const EVM_CURRENCY_TYPE: DataType = DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0);
pub type EvmCurrencyArrayType = arrow::array::Decimal128Array;

pub enum DataSourceKind {
    Chain,
}

/// Identifies a data source and its data schema.
pub struct DataSource {
    pub kind: DataSourceKind,
    pub name: String,
    pub network: String,
    pub data_schema: DataSchema,
}

pub struct DataSchema {
    pub format_spec: String,
    pub tables: Vec<Table>,
}

pub struct Table {
    pub name: String,
    pub schema: Schema,
}

pub fn create_table_at(table: Table, namespace: String, location: String) -> LogicalPlan {
    let command = CreateExternalTable {
        // This tables are not really external, as we will ingest and store them in our preferred format.
        // And we like Parquet.
        file_type: "PARQUET".to_string(),

        // DataFusion gives us two namespace levels: catalog and schema. The term 'schema' here is a
        // different thing from an Arrow data schema. We choose to only utilize the schema level, and
        // we call it a 'namespace' to avoid confusion.
        //
        // Many DataSources may share a same DataSchema. Users are then able to conveniently point
        // the same namespace name to different compatible data schemas according to configuration.
        // This is useful for sharing code across networks and data schema versions.
        name: TableReference::partial(namespace, table.name),

        // Unwrap: The schema is static, any panics would be caught in basic testing.
        schema: Arc::new(table.schema.try_into().unwrap()),

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
