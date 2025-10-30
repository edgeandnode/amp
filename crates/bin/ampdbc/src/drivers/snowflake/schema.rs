use std::{
    error::Error as StdError,
    fmt::{Display, Formatter, Result as FmtResult},
};

use common::arrow::datatypes::Fields;

use crate::{
    SchemaExt, StatementExt, arrow::{DataType, SchemaRef}, schema::{TableKind, TableRef}, snowflake::{connection::Connection, statement::Statement}, sql::DDLSafety
};

#[derive(Clone, Debug)]
pub struct Schema {
    pub schema: SchemaRef,
    pub table_ref: TableRef,
    pub table_kind: TableKind,
}

impl StatementExt for Statement {
    type ConnectionType = Connection;
    type SchemaType = Schema;
}

impl SchemaExt for Schema {
    type Statement = Statement;
    type ErrorType = super::error::Error;
    fn new(schema: SchemaRef, table_ref: TableRef, table_kind: TableKind) -> Self {
        Self {
            schema,
            table_ref,
            table_kind,
        }
    }

    fn as_external_types(&self) -> Result<Vec<(String, String)>, Self::ErrorType> {
        let (external_types, unsupported_fields): (Vec<(String, String)>, Vec<(String, DataType)>) =
            self.schema.fields().iter().cloned().fold(
                (Vec::new(), Vec::new()),
                move |(mut external_types, mut unsupported_fields), field| {
                    let arrow_type = field.data_type();
                    let metadata = field.metadata();
                    let primary_key = metadata.get("primary_key").is_some_and(|value| value.to_lowercase() == "true");
                    let auto_incremented = metadata.get("generated").is_some_and(|value| value.to_lowercase() == "auto_increment");
                    if primary_key && auto_incremented {
                        // We have an identity column, good news, we include IDENTITY keyword in Snowflake
                        external_types.push((quote_identifier_if_needed(field.name()), "NUMBER AUTOINCREMENT".to_string()));
                        return (external_types, unsupported_fields);
                    }
                    let default_now = metadata.get("generated").is_some_and(|value| value.to_lowercase() == "default_now");
                    match arrow_to_snowflake_type(arrow_type) {
                        Ok(sf_type) if !default_now => external_types.push((quote_identifier_if_needed(field.name()), sf_type)),
                        Ok(sf_type) if default_now => external_types.push((quote_identifier_if_needed(field.name()), format!("{} DEFAULT CURRENT_TIMESTAMP", sf_type))),
                        Err(SchemaError::UnsupportedDataType { data_type }) => {
                            unsupported_fields.push((field.name().to_string(), data_type.clone()));
                        }
                        _ => unreachable!(),
                    };
                    (external_types, unsupported_fields)
                },
            );

        if !unsupported_fields.is_empty() {
            return Err(SchemaError::UnsupportedTypes {
                fields: unsupported_fields,
            }.into());
        }

        Ok(external_types)
    }

    fn as_insert_stmt(&self) -> std::result::Result<String, Self::ErrorType> {
        use TableKind::*;
        match self.table_kind {
            Data => Ok(data_insert_statement(&self.table_ref, self.schema.fields())),
            History => todo!(),
            Watermark => todo!(),
        }
    }

    fn as_table_ddl(&self, ddl_safety: DDLSafety) -> std::result::Result<String, Self::ErrorType> {
        let mut stmt = String::new();
        stmt.push_str(ddl_safety.to_sql_clause());

        stmt.push_str(&self.table_ref.full_name());
        stmt.push_str(" (");
        let columns = 
            self.as_external_types()?
                .into_iter()
                .map(|(name, ty)| format!("{} {}", name, ty))
                .collect::<Vec<_>>()
                .join(", ");
        stmt.push_str(&columns);
        stmt.push_str(");");

        Ok(stmt)
    }
    
    fn table_kind(&self) -> &TableKind {
        &self.table_kind
    }

    fn table_ref(&self) -> &TableRef {
        &self.table_ref
    }
}

#[derive(Debug)]
pub enum SchemaError {
    ConversionError { data_type: DataType },
    UnsupportedDataType { data_type: DataType },
    UnsupportedTypes { fields: Vec<(String, DataType)> },
}

impl Display for SchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            SchemaError::ConversionError { data_type } => {
                write!(f, "Failed to convert field type: {:?}", data_type)
            }
            SchemaError::UnsupportedDataType { data_type } => {
                write!(f, "Unsupported data type: {:?}", data_type)
            }
            SchemaError::UnsupportedTypes { fields } => {
                write!(f, "Unsupported field types: {:?}", fields)
            }
        }
    }
}

impl StdError for SchemaError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        use SchemaError::*;
        match self {
            UnsupportedDataType { .. } | UnsupportedTypes { .. } | ConversionError { .. } => None,
        }
    }
}

fn data_insert_statement(table_ref: &TableRef, fields: &Fields) -> String {
    let mut stmt = String::new();

    stmt.push_str("INSERT INTO ");
    stmt.push_str(&table_ref.full_name());
    
    let (names, placeholders): (Vec<String>, Vec<String>) = fields
    .iter()
    .map(|f| (quote_identifier_if_needed(f.name()), "?".to_string()))
    .unzip();



    stmt.push_str(" (");
    stmt.push_str(&names.join(", "));
    stmt.push_str(") VALUES (");
    
    stmt.push_str(&placeholders.join(", "));
    stmt.push_str(");");

    stmt
}

/// Convert an Arrow data type to a Snowflake SQL type.
///
/// # Arrow → Snowflake Type Mapping
///
/// - Integer types → INTEGER, BIGINT, NUMBER
/// - Float types → FLOAT, DOUBLE
/// - String types → VARCHAR
/// - Binary types → BINARY
/// - Boolean → BOOLEAN
/// - Date/Time → DATE, TIME, TIMESTAMP
/// - Decimal → NUMBER(precision, scale)
/// - Complex types (List, Struct, Map) → VARIANT
///
/// # Examples
///
/// ```no_run
/// use arrow::datatypes::DataType;
/// use snowflake_adbc::schema_mapping::arrow_to_snowflake_type;
///
/// assert_eq!(arrow_to_snowflake_type(&DataType::Int64)?, "BIGINT");
/// assert_eq!(arrow_to_snowflake_type(&DataType::Utf8)?, "VARCHAR");
/// assert_eq!(arrow_to_snowflake_type(&DataType::Boolean)?, "BOOLEAN");
/// # Ok::<(), snowflake_adbc::Error>(())
/// ```
pub fn arrow_to_snowflake_type(arrow_type: &DataType) -> Result<String, SchemaError> {
    if arrow_type.is_null() {

    }
    let sf_type = match arrow_type {
        // Integer types
        DataType::Int8 | DataType::Int16 | DataType::Int32 => "NUMBER(10,0)",
        DataType::Int64 => "NUMBER",

        // Unsigned integers - Snowflake doesn't have unsigned types, use NUMBER
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => "NUMBER(10,0)",
        DataType::UInt64 => "NUMBER",

        // Floating point types
        DataType::Float16 | DataType::Float32 => "DOUBLE",
        DataType::Float64 => "DOUBLE",

        // String types
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",

        // Binary types
        DataType::Binary | DataType::LargeBinary => "BINARY",
        DataType::FixedSizeBinary(size) => return Ok(format!("BINARY({size})")),

        // Boolean
        DataType::Boolean => "BOOLEAN",

        // Date and time types
        DataType::Date32 | DataType::Date64 => "DATE",

        DataType::Time32(_) | DataType::Time64(_) => "TIME",

        // Timestamp without timezone - use TIMESTAMP_NTZ (not timezone aware)
        DataType::Timestamp(_unit, None) => {
            return Ok("TIMESTAMP_NTZ".to_string());
        }

        // Timestamp with timezone - use TIMESTAMP_TZ (timezone aware)
        DataType::Timestamp(_unit, Some(_tz)) => {
            return Ok("TIMESTAMP_TZ".to_string());
        }

        DataType::Interval(_) => {
            return Err(SchemaError::ConversionError {
                data_type: arrow_type.clone(),
            });
        }

        // Numeric with precision
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            return Ok(format!("NUMBER({},{})", precision, scale));
        }

        // Complex types - Snowflake VARIANT type can store JSON-like data
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => "VARIANT",

        DataType::Struct(_) => "VARIANT",

        DataType::Map(_, _) => "VARIANT",

        DataType::Union(_, _) => "VARIANT",

        // Dictionary encoded - use the value type
        DataType::Dictionary(_, value_type) => return arrow_to_snowflake_type(value_type),

        // Null type - default to VARCHAR
        DataType::Null => "VARCHAR",

        // Unsupported types
        _ => {
            return Err(SchemaError::UnsupportedDataType {
                data_type: arrow_type.clone(),
            });
        }
    };

    Ok(sf_type.to_string())
}


/// Compile-time perfect hash set of reserved SQL keywords.
///
/// If a column in the derived arrow schema exists in this set, it needs to be wrapped in quotes,
/// or the CREATE TABLE call will fail. Using phf (perfect hash function) provides O(1) lookups
/// with zero runtime initialization cost and no heap allocations.
const RESERVED_KEYWORDS: phf::Set<&'static str> = phf::phf_set! {
    "as",
    "from",
    "to",
    "select",
    "array",
    "sum",
    "all",
    "allocate",
    "alter",
    "table",
    "blob",
};

pub fn quote_identifier_if_needed(identifier: &str) -> String {
    if RESERVED_KEYWORDS.contains(identifier.to_lowercase().as_str()) {
        format!("\"{}\"", identifier)
    } else {
        identifier.to_string()
    }
}