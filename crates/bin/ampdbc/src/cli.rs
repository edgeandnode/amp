use adbc_snowflake::database::AuthType as SnowflakeAuthType;
use clap::Parser;
use serde::{Deserialize, Serialize};

const LONG_ABOUT: &str = r#"ampdbc - ADBC-powered data pipeline service

ampdbc enables high-performance data pipelines from Amp blockchain datasets directly
into modern data warehouses and analytical databases. It executes SQL queries against
Amp data sources and streams results efficiently via ADBC (Arrow Database Connectivity).

SUPPORTED DESTINATIONS:
  • Snowflake      - Cloud data warehouse

ROADMAP:
  • PostgreSQL     - Popular open-source RDBMS
  • BigQuery       - Google's serverless data warehouse
  • MySQL          - Widely-used relational database
  

QUERY MODES:
  Batch Mode      - Query historical data with start/end bounds
  Streaming Mode  - Continuously ingest new data as it arrives

SQL SYNTAX:
  Queries use Apache DataFusion SQL syntax. See the DataFusion SQL reference:
  https://datafusion.apache.org/user-guide/sql/select.html

USAGE EXAMPLES:
  # Batch historical blocks to Snowflake
  ampdbc \
    --query "SELECT * FROM eth_mainnet.blocks WHERE number BETWEEN 20000000 AND 22000000" \
    --table "analytics.eth_blocks" \
    --config /etc/amp/config.toml \
    --ampdbc-config /etc/amp/snowflake.toml

  # Stream new transactions (continuous ingestion)
  ampdbc \
    --query "SELECT * FROM eth_mainnet.transactions WHERE block_number > 22000000" \
    --table "raw.eth_txs" \
    --config $AMP_CONFIG \
    --ampdbc-config $AMPDBC_CONFIG \
    --write-mode append

  # Overwrite table with fresh data
  ampdbc -q "SELECT * FROM eth_mainnet.logs WHERE block_number > 21000000" \
    -t "staging.logs" \
    --config ./amp.toml \
    --ampdbc-config ./bigquery.toml \
    --create-mode create-or-replace \
    --write-mode overwrite

CONFIGURATION:
  Config files can be specified via flags or environment variables:
    --config           or AMP_CONFIG           (Amp dataset/provider config)
    --ampdbc-config    or AMPDBC_CONFIG        (ADBC driver/destination config)
    --create-mode      or AMPDBC_CREATE_MODE   (if-not-exists | create-or-replace)
    --write-mode       or AMPDBC_WRITE_MODE    (append | overwrite)

DEPLOYMENT:
  ampdbc is designed to run as a containerized service, typically colocated with
  an Amp cluster. Each ampdbc process handles one query pipeline, which can broadcast
  results to multiple destination databases simultaneously.

For more information, visit: https://github.com/graphprotocol/amp
"#;

/// ADBC-powered service for batch, bulk, and stream pipelines from Amp to data warehouses
#[derive(Debug, Parser)]
#[command(
    version,
    about = "ADBC-powered service for batch, bulk, and stream pipelines from Amp to data warehouses",
    long_about = LONG_ABOUT
)]
pub struct EntryPoint {
    /// SQL query to execute against Amp data sources
    ///
    /// The query uses Apache DataFusion SQL syntax and can operate in two modes:
    ///
    /// BATCH MODE (historical data):
    ///   Query a bounded range of blockchain data. Ideal for backfills, analytics,
    ///   and one-time exports.
    ///
    ///   Example: "SELECT * FROM eth_mainnet.blocks WHERE block_num BETWEEN 20000000 AND 22000000"
    ///
    /// STREAMING MODE (real-time ingestion):
    ///   Continuously process new data as it arrives on-chain. Ideal for real-time
    ///   processing at chain head, monitoring, and alerting.
    ///
    ///   Example: "SELECT * FROM eth_mainnet.transactions WHERE block_num > 22000000"
    ///
    /// SQL Reference: https://datafusion.apache.org/user-guide/sql/select.html
    #[arg(long, short = 'q', value_name = "SQL")]
    pub query: String,

    /// Destination table name where query results will be written
    ///
    /// The table will be created automatically if it doesn't exist (controlled by --create-mode).
    /// Schema is inferred from the query result.
    ///
    /// Format depends on destination database:
    ///   • Snowflake:   "DATABASE.SCHEMA.TABLE" or "SCHEMA.TABLE"
    ///   • BigQuery:    "project.dataset.table" or "dataset.table"
    ///   • PostgreSQL:  "schema.table" or "table"
    ///   • MySQL:       "database.table" or "table"
    ///
    /// Examples: "analytics.eth_blocks", "raw.transactions", "eth_logs"
    #[arg(long, short = 't', value_name = "TABLE")]
    pub table: String,

    /// Path to Amp configuration file
    ///
    /// This TOML file defines:
    ///   • Dataset definitions (where to find blockchain data)
    ///   • Provider configurations (RPC endpoints, Firehose, etc.)
    ///   • Query engine settings
    ///
    /// The same config file used by `ampd` and other Amp services.
    ///
    /// Example: /etc/amp/config.toml
    #[arg(long, env = "AMP_CONFIG", value_name = "PATH")]
    pub config: String,

    /// Path to ADBC configuration file
    ///
    /// This TOML file defines:
    ///   • Destination database connection settings
    ///   • ADBC driver options
    ///   • Authentication credentials (or references to secret managers)
    ///
    /// Can specify multiple destinations - results will be broadcast to all.
    ///
    /// Example: /etc/amp/destinations.toml
    #[arg(long, env = "AMPDBC_CONFIG", value_name = "PATH")]
    pub ampdbc_config: String,

    /// Table creation behavior when the destination table doesn't exist
    ///
    /// Options:
    ///   • if-not-exists      (default, safe) - Create only if missing, preserves existing data
    ///   • create-or-replace  (destructive)   - Drop and recreate, destroys existing data
    ///
    /// ⚠️  WARNING: create-or-replace will permanently delete existing table data!
    #[arg(long, value_enum, default_value = "if-not-exists")]
    pub create_mode: CreateMode,

    /// Write behavior when the destination table already exists
    ///
    /// Options:
    ///   • append    (default, safe) - Add new rows to existing data
    ///   • overwrite (destructive)   - Truncate table before writing
    ///
    /// Note: In streaming mode, 'append' is the typical choice for continuous ingestion.
    /// In batch mode, 'overwrite' can be useful for periodic refreshes.
    #[arg(long, value_enum, default_value = "append", env = "AMPDBC_WRITE_MODE")]
    pub write_mode: WriteMode,
    #[arg(long, value_enum, default_value = "user-password")]
    pub auth_type: AuthType,
}

/// Table creation mode for destination tables.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
#[non_exhaustive]
pub enum CreateMode {
    /// Create table only if it doesn't exist (preserves existing data).
    IfNotExists,
    /// Drop and recreate the table (destroys existing data).
    CreateOrReplace,
    /// Yolo - fail if table exists.
    FailIfExists,
}

/// Write mode for destination tables.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum WriteMode {
    /// Append new rows to existing table data.
    Append,
    /// Truncate table before writing (replaces all data).
    Overwrite,
    /// Create and append new data, creating the table if it doesn't exist.
    CreateAppend,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum, Deserialize, Serialize, PartialEq, Eq)]
pub enum AuthType {
    UserPassword,
    ExternalBrowser,
}

impl AuthType {
    pub fn is_external_browser(&self) -> bool {
        matches!(self, AuthType::ExternalBrowser)
    }
}

impl Default for AuthType {
    fn default() -> Self {
        AuthType::UserPassword
    }
}

impl Into<adbc_snowflake::database::AuthType> for AuthType {
    fn into(self) -> adbc_snowflake::database::AuthType {
        use AuthType::*;
        match self {
            UserPassword => SnowflakeAuthType::Snowflake,
            ExternalBrowser => SnowflakeAuthType::ExternalBrowser,
        }
    }
}
