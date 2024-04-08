pub mod arrow_helpers;

use std::time::Duration;

use arrow::datatypes::DataType;
use arrow_helpers::TableRow;
pub use datafusion::arrow;
use datafusion::common::{DFSchemaRef, OwnedTableReference};
use datafusion::logical_expr::{col, DdlStatement, LogicalPlan};
use datafusion::{
    arrow::datatypes::{SchemaRef, TimeUnit, DECIMAL128_MAX_PRECISION},
    common::{parsers::CompressionTypeVariant, Constraints, ToDFSchema},
    execution::context::SessionContext,
    logical_expr::CreateExternalTable,
    sql::TableReference,
};
use url::Url;

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

#[derive(Clone, Copy, Debug, Default)]
pub struct Timestamp(pub Duration);

// Note: We choose a 'nanosecond' precision for the timestamp, even though many blockchains expose
// only 'second' precision for block timestamps. A couple justifications for 'nanosecond':
// 1. We found that the `date_bin` scalar function in DataFusion expects a nanosecond precision
//    origin parameter. If we patch DataFusion to fix this, we can revisit this decision.
// 2. There is no performance hit for choosing higher precision, it's all i64 at the Arrow level.
// 3. It's the most precise timestamp, so it's maximally future-compatible with data sources that use
//    more precise timestamps.
pub fn timestamp_type() -> DataType {
    let timezone = Some("+00:00".into());
    DataType::Timestamp(TimeUnit::Nanosecond, timezone)
}

/// Remember to call `.with_timezone_utc()` after creating a Timestamp array.
pub type TimestampArrayType = arrow::array::TimestampNanosecondArray;

/// Identifies a dataset and its data schema.
pub struct DataSet {
    pub name: String,
    pub network: String,
    pub data_schema: DataSchema,
}

pub struct DataSchema {
    pub tables: Vec<Table>,
}

impl DataSchema {
    /// `base_url` is expected to be a directory and theferore must end with a `/`.
    pub async fn register_tables(
        &self,
        ctx: SessionContext,
        base_url: Url,
    ) -> Result<(), anyhow::Error> {
        for table in &self.tables {
            // We will eventually want to leverage namespacing.
            let table_reference = TableReference::bare(table.name.clone());

            let schema = table.schema.as_ref().clone().to_dfschema_ref()?;
            let url = base_url.join(&table.name)?;
            let command = create_external_table(table_reference, schema, &url);
            ctx.execute_logical_plan(command).await?;
        }
        Ok(())
    }
}

pub struct Table {
    pub name: String,
    pub schema: SchemaRef,
}

pub struct TableRows {
    pub table: Table,
    pub rows: Vec<Box<dyn TableRow>>,
}

pub fn create_external_table(
    name: OwnedTableReference,
    schema: DFSchemaRef,
    url: &Url,
) -> LogicalPlan {
    let command = CreateExternalTable {
        // Assume parquet, which has native compression.
        file_type: "PARQUET".to_string(),
        file_compression_type: CompressionTypeVariant::UNCOMPRESSED,

        name,
        schema,
        location: url.to_string(),

        // TODO:
        // - Make this less hardcoded to accomodate non-blockchain data.
        // - Have a consistency check that the data really is sorted.
        // - Add other sorted columns that may be relevant such as `ordinal`.
        // - Do we want to address and leverage https://github.com/apache/arrow-datafusion/issues/4177?
        order_exprs: vec![
            vec![col("block_num").sort(true, false)],
            vec![col("timestamp").sort(true, false)],
        ],

        // Up to our preference, but maybe it's more robust for the caller to check existence.
        if_not_exists: false,

        // Wen streaming?
        unbounded: false,

        // Things we don't currently use.
        table_partition_cols: vec![],
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
