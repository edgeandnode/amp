use std::{
    hash::{Hash, Hasher},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

use crate::{Schema, arrow::SchemaRef, schema::TableRef};

#[derive(Debug, Clone, Deserialize, Eq, Serialize)]
#[serde(tag = "vendor", rename_all = "lowercase")]
pub enum DriverOpts {
    BigQuery {
        /// GCP project ID
        project_id: String,
        /// Optional path to a service account key file (if not using default credentials)
        key_file: Option<PathBuf>,
        /// Target catalog name
        catalog: String,
        /// Target table name
        table: String,
    },
    Postgres {
        /// Database server
        host: String,
        /// Port number
        port: u16,
        /// Optional path to a Unix domain socket
        socket: Option<PathBuf>,
        /// Username for authentication
        username: String,
        #[serde(skip)]
        password: Option<String>,
        /// Target database name
        database: String,
        /// Target schema name (default: public)
        schema: String,
        /// Taget table name
        table: String,
        /// Statement cache capacity (optional)
        statement_cache_capacity: Option<usize>,
        /// Application name (optional)
        application_name: Option<String>,
    },
    Snowflake {
        /// Snowflake account identifier (e.g., "xy12345.us-east-1")
        account: String,
        /// Username for authentication
        username: String,
        /// Password for authentication (keep secure!)
        #[serde(skip)]
        password: Option<String>,
        /// Target database name
        database: String,
        /// Target schema name (default: PUBLIC)
        schema: String,
        /// Target table name
        table: String,
        /// Compute warehouse to use
        warehouse: String,
        /// Optional role to assume after connection
        #[serde(skip)]
        role: Option<String>,
        /// adbc.snowflake.statement.ingest_writer_concurrency
        #[serde(default)]
        ingest_writer_concurrency: usize,
        /// adbc.snowflake.statement.ingest_upload_concurrency
        #[serde(default)]
        ingest_upload_concurrency: usize,
        /// adbc.snowflake.statement.ingest_copy_concurrency
        #[serde(default)]
        ingest_copy_concurrency: usize,
        /// adbc.snowflake.statement.ingest_target_file_size
        #[serde(default)]
        ingest_target_file_size: usize,
    },
    Sqlite,
}

impl Hash for DriverOpts {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use DriverOpts::*;
        match self {
            BigQuery {
                project_id,
                catalog,
                table,
                ..
            } => {
                project_id.hash(state);
                catalog.hash(state);
                table.hash(state);
            }
            Postgres {
                host,
                port,
                database,
                schema,
                table,
                ..
            } => {
                host.hash(state);
                port.hash(state);
                database.hash(state);
                schema.hash(state);
                table.hash(state);
            }
            Snowflake {
                account,
                database,
                schema,
                table,
                warehouse,
                ..
            } => {
                account.hash(state);
                database.hash(state);
                schema.hash(state);
                table.hash(state);
                warehouse.hash(state);
            }
            Sqlite => {
                // No fields to hash
            }
        }
    }
}

impl PartialEq for DriverOpts {
    fn eq(&self, other: &Self) -> bool {
        use DriverOpts::*;

        match (self, other) {
            (
                BigQuery {
                    project_id: pid_left,
                    catalog: c_left,
                    table: t_left,
                    ..
                },
                BigQuery {
                    project_id: pid_right,
                    catalog: c_right,
                    table: t_right,
                    ..
                },
            ) => pid_left == pid_right && c_left == c_right && t_left == t_right,
            (
                Postgres {
                    host: h_left,
                    port: p_left,
                    database: d_left,
                    schema: sch_left,
                    table: t_left,
                    ..
                },
                Postgres {
                    host: h_right,
                    port: p_right,
                    database: d_right,
                    schema: sch_right,
                    table: t_right,
                    ..
                },
            ) => {
                h_left == h_right
                    && p_left == p_right
                    && d_left == d_right
                    && sch_left == sch_right
                    && t_left == t_right
            }
            (
                Snowflake {
                    account: a_left,
                    database: d_left,
                    schema: sch_left,
                    table: t_left,
                    warehouse: w_left,
                    ..
                },
                Snowflake {
                    account: a_right,
                    database: d_right,
                    schema: sch_right,
                    table: t_right,
                    warehouse: w_right,
                    ..
                },
            ) => {
                a_left == a_right
                    && d_left == d_right
                    && sch_left == sch_right
                    && t_left == t_right
                    && w_left == w_right
            }
            (Sqlite, Sqlite) => true,
            _ => false,
        }
    }
}

impl DriverOpts {
    pub fn build_schema(&self, schema: SchemaRef) -> Schema {
        let table_ref = self.table_ref();

        use DriverOpts::*;
        match self {
            BigQuery { .. } => Schema::bigquery(&schema, table_ref, Default::default()),
            Postgres { .. } => Schema::postgres(&schema, table_ref, Default::default()),
            Snowflake { .. } => Schema::snowflake(&schema, table_ref, Default::default()),
            Sqlite => unimplemented!("build_schema is not implemented for Sqlite"),
        }
    }

    pub fn table_ref(&self) -> TableRef {
        use DriverOpts::*;
        match self {
            BigQuery { catalog, table, .. } => TableRef {
                database: catalog.to_string(),
                schema: None,
                table: table.to_string(),
            },
            Postgres {
                database,
                schema,
                table,
                ..
            } => TableRef {
                database: database.to_string(),
                schema: Some(schema.to_string()),
                table: table.to_string(),
            },
            Snowflake {
                database,
                schema,
                table,
                ..
            } => TableRef {
                database: database.to_string(),
                schema: Some(schema.to_string()),
                table: table.to_string(),
            },
            _ => unimplemented!("table_ref is not implemented for this driver"),
        }
    }
}

impl Default for DriverOpts {
    fn default() -> Self {
        DriverOpts::Sqlite
    }
}
