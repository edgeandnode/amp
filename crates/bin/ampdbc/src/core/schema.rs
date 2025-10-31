use std::{collections::HashMap, str::FromStr, sync::Arc};

use crate::{
    ConnectionExt, Error, StatementExt, SupportedVendor,
    adbc::error::AdbcError,
    arrow::{
        DataType, Schema as ArrowSchema, SchemaRef,
        datatypes::{Field, FieldRef, TimeUnit},
    },
    sql::DDLSafety,
};
pub use crate::{
    bigquery::schema as bigquery, postgres::schema as postgres, snowflake::schema as snowflake,
    sqlite::schema as sqlite,
};

#[derive(Clone, PartialEq, Eq)]
pub struct TableName(String);

impl std::fmt::Debug for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}


impl TableName {
    pub fn new(name: &str) -> Self {
        Self(name.to_uppercase())
    }

    pub fn name(&self) -> String {
        self.0.clone()
    }
}

impl From<&str> for TableName {
    fn from(s: &str) -> Self {
        TableName::new(s)
    }
}

impl From<&String> for TableName {
    fn from(s: &String) -> Self {
        TableName::new(s)
    }
}

impl From<String> for TableName {
    fn from(s: String) -> Self {
        TableName::new(&s)
    }
}

impl FromStr for TableName {
    type Err = crate::Error;

    fn from_str(s: &str) -> crate::Result<Self> {
        Ok(TableName::new(s))
    }
}

impl AsRef<str> for TableName {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl std::ops::Deref for TableName {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TableName {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}


impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct TableRef {
    pub database: String,
    pub schema: Option<String>,
    pub table: TableName,
}

impl TableRef {
    pub fn new(database: &str, schema: Option<&str>, table: &str) -> Self {
        Self {
            database: database.to_string(),
            schema: schema.map(ToString::to_string),
            table: table.into(),
        }
    }
    pub fn name(&self) -> String {
        self.table.to_string()
    }

    pub fn full_name(&self) -> String {
        match &self.schema {
            Some(schema) => {
                let mut full_name = String::new();
                full_name.push_str(&self.database);
                full_name.push('.');
                full_name.push_str(schema);
                full_name.push('.');
                full_name.push_str(&self.table);
                full_name
            },
            None => {
                let mut full_name = String::new();
                full_name.push_str(&self.database);
                full_name.push('.');
                full_name.push_str(&self.table);
                full_name
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableKind {
    Data,
    History,
    Watermark,
}

impl Default for TableKind {
    fn default() -> Self {
        TableKind::Data
    }
}

#[derive(Clone, Debug)]
pub enum Schema {
    BigQuery(bigquery::Schema),
    Postgres(postgres::Schema),
    Snowflake(snowflake::Schema),
    Sqlite(sqlite::Schema),
    Empty,
}

impl Schema {
    pub fn bigquery(schema: &SchemaRef, table_ref: TableRef, table_kind: TableKind) -> Self {
        let schema = Arc::clone(schema);
        Schema::BigQuery(bigquery::Schema {
            schema,
            table_ref,
            table_kind,
        })
    }

    pub fn postgres(schema: &SchemaRef, table_ref: TableRef, table_kind: TableKind) -> Self {
        let schema = Arc::clone(schema);
        Schema::Postgres(postgres::Schema {
            schema,
            table_ref,
            table_kind,
        })
    }

    pub fn snowflake(
        schema: &SchemaRef,
        table_ref: TableRef,
        table_kind: TableKind,
        ddl_safety: DDLSafety,
    ) -> Self {
        let schema = Arc::clone(schema);
        Schema::Snowflake(snowflake::Schema {
            schema,
            table_ref,
            table_kind,
            ddl_safety,
        })
    }

    pub fn sqlite(schema: &SchemaRef, table_ref: TableRef, table_kind: TableKind) -> Self {
        let schema = Arc::clone(schema);
        Schema::Sqlite(sqlite::Schema {
            schema,
            table_ref,
            table_kind,
        })
    }

    pub fn schema_ref(&self) -> SchemaRef {
        match self {
            Schema::BigQuery(schema) => Arc::clone(&schema.schema),
            Schema::Postgres(schema) => Arc::clone(&schema.schema),
            Schema::Snowflake(schema) => Arc::clone(&schema.schema),
            Schema::Sqlite(schema) => Arc::clone(&schema.schema),
            _ => Arc::new(ArrowSchema::empty()),
        }
    }

    pub fn ddl_safety(&self) -> DDLSafety {
        match self {
            Schema::Snowflake(schema) => schema.ddl_safety.clone(),
            _ => DDLSafety::default(),
        }
    }
}

pub trait SchemaExt: Sized + Clone
where
    crate::Error: From<Self::ErrorType>,
{
    type Statement: StatementExt<SchemaType = Self>;
    type ErrorType: std::error::Error + From<AdbcError>;

    fn new(schema: SchemaRef, table_ref: TableRef, table_kind: TableKind) -> Self;

    fn new_from_known_vendor(
        _vendor: SupportedVendor,
        schema: SchemaRef,
        table_ref: TableRef,
        table_kind: TableKind,
    ) -> Self {
        Self::new(schema, table_ref, table_kind)
    }

    fn as_external_types(&self) -> std::result::Result<Vec<(String, String)>, Self::ErrorType>;

    fn as_transfer_history(&self) -> Self {
        if self.history() {
            self.clone()
        } else {
            let TableRef {
                database,
                schema,
                table,
            } = self.table_ref();
            let table_ref = TableRef {
                database: database.clone(),
                schema: schema.clone(),
                table: (table.to_string() + "_history").into(),
            };
            let schema = transfer_history_schema();

            Self::new(schema, table_ref, TableKind::History)
        }
    }

    fn as_watermark(&self) -> Self {
        if self.watermark() {
            self.clone()
        } else {
            let TableRef {
                database,
                schema,
                table,
            } = self.table_ref();
            let table_ref = TableRef {
                database: database.clone(),
                schema: schema.clone(),
                table: (table.to_string() + "_watermark").into(),
            };
            let schema = watermark_schema();

            Self::new(schema, table_ref, TableKind::Watermark)
        }
    }

    fn as_insert_stmt(&self) -> std::result::Result<String, Self::ErrorType>;
    fn as_table_ddl(&self) -> std::result::Result<String, Self::ErrorType>;
    fn as_truncate_stmt(&self) -> std::result::Result<String, Self::ErrorType> {
        let stmt = format!("TRUNCATE TABLE {};", self.table_ref().full_name());
        Ok(stmt)
    }

    fn table_kind(&self) -> &TableKind;

    fn history(&self) -> bool {
        matches!(self.table_kind(), TableKind::History)
    }

    fn watermark(&self) -> bool {
        matches!(self.table_kind(), TableKind::Watermark)
    }

    fn prepare_table_ddl_stmt(
        self,
        connection: &mut <Self::Statement as StatementExt>::ConnectionType,
    ) -> crate::Result<Self::Statement> {
        let mut stmt: Self::Statement = connection.create_statement().map_err(Error::from)?;

        stmt.prepare_create_table(&self).map_err(Error::from)?;
        Ok(stmt)
    }

    fn table_ref(&self) -> &crate::schema::TableRef;
}

impl SchemaExt for Schema {
    type Statement = crate::Statement;
    type ErrorType = crate::DriverError;

    fn new(_schema: SchemaRef, _table_ref: TableRef, _table_kind: TableKind) -> Self {
        panic!("Use specific driver constructors to create Schema instances")
    }

    fn new_from_known_vendor(
        vendor: crate::SupportedVendor,
        schema: SchemaRef,
        table_ref: TableRef,
        table_kind: TableKind,
    ) -> Self {
        match vendor {
            crate::SupportedVendor::BigQuery => Schema::bigquery(&schema, table_ref, table_kind),
            crate::SupportedVendor::Postgres => Schema::postgres(&schema, table_ref, table_kind),
            crate::SupportedVendor::Snowflake => {
                Schema::snowflake(&schema, table_ref, table_kind, Default::default())
            }
            crate::SupportedVendor::Sqlite => Schema::sqlite(&schema, table_ref, table_kind),
        }
    }

    fn as_external_types(&self) -> std::result::Result<Vec<(String, String)>, Self::ErrorType> {
        match self {
            Schema::BigQuery(schema) => Ok(schema.as_external_types()?),
            Schema::Postgres(schema) => Ok(schema.as_external_types()?),
            Schema::Snowflake(schema) => Ok(schema.as_external_types()?),
            Schema::Sqlite(schema) => Ok(schema.as_external_types()?),
            _ => Ok(vec![]),
        }
    }

    fn as_insert_stmt(&self) -> std::result::Result<String, Self::ErrorType> {
        match self {
            Schema::BigQuery(schema) => Ok(schema.as_insert_stmt()?),
            Schema::Postgres(schema) => Ok(schema.as_insert_stmt()?),
            Schema::Snowflake(schema) => Ok(schema.as_insert_stmt()?),
            Schema::Sqlite(schema) => Ok(schema.as_insert_stmt()?),
            _ => Ok("".into()),
        }
    }

    fn as_transfer_history(&self) -> Self {
        match self {
            Schema::BigQuery(schema) => Schema::BigQuery(schema.as_transfer_history()),
            Schema::Postgres(schema) => Schema::Postgres(schema.as_transfer_history()),
            Schema::Snowflake(schema) => Schema::Snowflake(schema.as_transfer_history()),
            Schema::Sqlite(schema) => Schema::Sqlite(schema.as_transfer_history()),
            _ => self.clone(),
        }
    }

    fn as_watermark(&self) -> Self {
        match self {
            Schema::BigQuery(schema) => Schema::BigQuery(schema.as_watermark()),
            Schema::Postgres(schema) => Schema::Postgres(schema.as_watermark()),
            Schema::Snowflake(schema) => Schema::Snowflake(schema.as_watermark()),
            Schema::Sqlite(schema) => Schema::Sqlite(schema.as_watermark()),
            _ => self.clone(),
        }
    }

    fn as_table_ddl(&self) -> std::result::Result<String, Self::ErrorType> {
        match self {
            Schema::BigQuery(schema) => Ok(schema.as_table_ddl()?),
            Schema::Postgres(schema) => Ok(schema.as_table_ddl()?),
            Schema::Snowflake(schema) => Ok(schema.as_table_ddl()?),
            Schema::Sqlite(schema) => Ok(schema.as_table_ddl()?),
            _ => Ok("".into()),
        }
    }

    fn table_kind(&self) -> &TableKind {
        use Schema::*;
        match self {
            BigQuery(schema) => &schema.table_kind(),
            Postgres(schema) => &schema.table_kind(),
            Snowflake(schema) => &schema.table_kind(),
            Sqlite(schema) => &schema.table_kind(),
            _ => panic!("No table_kind for Empty schema"),
        }
    }

    fn history(&self) -> bool {
        use Schema::*;
        match self {
            BigQuery(schema) => schema.history(),
            Postgres(schema) => schema.history(),
            Snowflake(schema) => schema.history(),
            Sqlite(schema) => schema.history(),
            _ => false,
        }
    }

    fn watermark(&self) -> bool {
        use Schema::*;
        match self {
            BigQuery(schema) => schema.watermark(),
            Postgres(schema) => schema.watermark(),
            Snowflake(schema) => schema.watermark(),
            Sqlite(schema) => schema.watermark(),
            _ => false,
        }
    }

    fn table_ref(&self) -> &TableRef {
        use Schema::*;
        match self {
            BigQuery(schema) => &schema.table_ref,
            Postgres(schema) => &schema.table_ref,
            Snowflake(schema) => &schema.table_ref,
            Sqlite(schema) => &schema.table_ref,
            _ => panic!("No table_ref for Empty schema"),
        }
    }
}

impl Default for Schema {
    fn default() -> Self {
        Schema::Empty
    }
}

pub fn transfer_history_schema() -> SchemaRef {
    let field_metadata = HashMap::from([
        ("primary_key".to_string(), "false".to_string()),
        ("generated".to_string(), "false".to_string()),
        ("unique".to_string(), "false".to_string()),
    ]);

    let created_at_metadata = HashMap::from([
        ("primary_key".to_string(), "false".to_string()),
        ("generated".to_string(), "default_now".to_string()),
        ("unique".to_string(), "false".to_string()),
    ]);

    let fields: Vec<FieldRef> = vec![
        // The unique ID for the record
        Field::new("transfer_id", DataType::Int64, false)
            .with_metadata(field_metadata.clone())
            .into(),
        // The operation type: INSERT, UPDATE, DELETE
        Field::new("operation", DataType::Utf8, false)
            .with_metadata(field_metadata.clone())
            .into(),
        // The min and max block range for this record
        Field::new("network", DataType::Utf8, false)
            .with_metadata(field_metadata.clone())
            .into(),
        Field::new("block_range_start", DataType::Int64, false)
            .with_metadata(field_metadata.clone())
            .into(),
        Field::new("block_range_end", DataType::Int64, false)
            .with_metadata(field_metadata.clone())
            .into(),
        // The timestamp when the record was created
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )
        .with_metadata(created_at_metadata)
        .into(),
    ];

    ArrowSchema::new(fields).into()
}

pub fn watermark_schema() -> SchemaRef {
    let field_metadata = HashMap::from([
        ("primary_key".to_string(), "false".to_string()),
        ("generated".to_string(), "false".to_string()),
        ("unique".to_string(), "false".to_string()),
    ]);

    let block_hash_metadata = HashMap::from([
        ("primary_key".to_string(), "false".to_string()),
        ("generated".to_string(), "false".to_string()),
        ("unique".to_string(), "true".to_string()),
    ]);

    let created_at_metadata = HashMap::from([
        ("primary_key".to_string(), "false".to_string()),
        ("generated".to_string(), "default_now".to_string()),
        ("unique".to_string(), "false".to_string()),
    ]);

    let fields: Vec<FieldRef> = vec![
        Field::new("transfer_id", DataType::Int64, false)
            .with_metadata(field_metadata.clone())
            .into(),
        Field::new("network", DataType::Utf8, false)
            .with_metadata(field_metadata.clone())
            .into(),
        Field::new("block_num", DataType::Int64, false)
            .with_metadata(field_metadata.clone())
            .into(),
        Field::new("block_hash", DataType::FixedSizeBinary(32), false)
            .with_metadata(block_hash_metadata)
            .into(),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )
        .with_metadata(created_at_metadata)
        .into(),
    ];

    ArrowSchema::new(fields).into()
}
