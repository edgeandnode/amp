#[derive(Debug, Clone, PartialEq)]
pub enum TypeSize {
    Fixed(usize),
    Variable,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PostgresType {
    Bool,
    Bytea,
    Int8,
    Int2,
    Int4,
    Char,
    Text,
    Json,
    Jsonb,
    Float4,
    Float8,
    Date,
    Time,
    /// Timestamp type with optional timezone information
    /// If timezone is Some(tz_string), generates TIMESTAMPTZ in DDL
    Timestamp {
        timezone: Option<String>,
    },
    Interval,
    /// Numeric type with optional precision and scale
    /// - precision: total number of digits
    /// - scale: number of digits after decimal point
    ///
    /// For UInt64, typically use precision=20 to handle full range
    Numeric {
        precision: Option<u8>,
        scale: Option<i8>,
    },
    List(Box<Column>),
}

impl PostgresType {
    pub const fn size(&self) -> TypeSize {
        match &self {
            PostgresType::Bool => TypeSize::Fixed(1),
            PostgresType::Bytea => TypeSize::Variable,
            PostgresType::Int2 => TypeSize::Fixed(2),
            PostgresType::Int4 => TypeSize::Fixed(4),
            PostgresType::Int8 => TypeSize::Fixed(8),
            PostgresType::Char => TypeSize::Fixed(2),
            PostgresType::Text => TypeSize::Variable,
            PostgresType::Json => TypeSize::Variable,
            PostgresType::Jsonb => TypeSize::Variable,
            PostgresType::Float4 => TypeSize::Fixed(4),
            PostgresType::Float8 => TypeSize::Fixed(8),
            PostgresType::Date => TypeSize::Fixed(4),
            PostgresType::Time => TypeSize::Fixed(8),
            PostgresType::Timestamp { .. } => TypeSize::Fixed(8),
            PostgresType::Interval => TypeSize::Fixed(16),
            PostgresType::Numeric { .. } => TypeSize::Variable,
            PostgresType::List(_) => TypeSize::Variable,
        }
    }

    /// Convert PostgresType to DDL-compatible type string
    ///
    /// Returns the PostgreSQL type name as used in CREATE TABLE statements.
    /// Includes precision/scale for Numeric types and timezone info for Timestamps.
    pub fn to_ddl_string(&self) -> String {
        match self {
            PostgresType::Bool => "BOOLEAN".to_string(),
            PostgresType::Bytea => "BYTEA".to_string(),
            PostgresType::Int2 => "SMALLINT".to_string(),
            PostgresType::Int4 => "INTEGER".to_string(),
            PostgresType::Int8 => "BIGINT".to_string(),
            PostgresType::Char => "CHAR".to_string(),
            PostgresType::Text => "TEXT".to_string(),
            PostgresType::Json => "JSON".to_string(),
            PostgresType::Jsonb => "JSONB".to_string(),
            PostgresType::Float4 => "REAL".to_string(),
            PostgresType::Float8 => "DOUBLE PRECISION".to_string(),
            PostgresType::Date => "DATE".to_string(),
            PostgresType::Time => "TIME".to_string(),
            PostgresType::Timestamp { timezone } => {
                if timezone.is_some() {
                    "TIMESTAMPTZ".to_string()
                } else {
                    "TIMESTAMP".to_string()
                }
            }
            PostgresType::Interval => "INTERVAL".to_string(),
            PostgresType::Numeric { precision, scale } => match (precision, scale) {
                (Some(p), Some(s)) => format!("NUMERIC({}, {})", p, s),
                (Some(p), None) => format!("NUMERIC({})", p),
                _ => "NUMERIC".to_string(),
            },
            PostgresType::List(inner) => {
                format!("{}[]", inner.data_type.to_ddl_string())
            }
        }
    }
    pub fn oid(&self) -> Option<u32> {
        match &self {
            PostgresType::Bool => Some(16),
            PostgresType::Bytea => Some(17),
            PostgresType::Int8 => Some(20),
            PostgresType::Int2 => Some(21),
            PostgresType::Int4 => Some(23),
            PostgresType::Char => Some(18),
            PostgresType::Text => Some(25),
            PostgresType::Json => Some(3802),
            PostgresType::Jsonb => Some(3802),
            PostgresType::Float4 => Some(700),
            PostgresType::Float8 => Some(701),
            PostgresType::Date => Some(1082),
            PostgresType::Time => Some(1083),
            PostgresType::Timestamp { .. } => Some(1114),
            PostgresType::Interval => Some(1186),
            PostgresType::Numeric { .. } => Some(1700),
            PostgresType::List(_) => None,
        }
    }
    pub fn name(&self) -> Option<String> {
        let v = match &self {
            PostgresType::Bool => "BOOL".to_string(),
            PostgresType::Bytea => "BYTEA".to_string(),
            PostgresType::Int8 => "INT8".to_string(),
            PostgresType::Int2 => "INT2".to_string(),
            PostgresType::Int4 => "INT4".to_string(),
            PostgresType::Char => "CHAR".to_string(),
            PostgresType::Text => "TEXT".to_string(),
            PostgresType::Json => "JSON".to_string(),
            PostgresType::Jsonb => "JSONB".to_string(),
            PostgresType::Float4 => "FLOAT4".to_string(),
            PostgresType::Float8 => "FLOAT8".to_string(),
            PostgresType::Date => "DATE".to_string(),
            PostgresType::Time => "TIME".to_string(),
            PostgresType::Timestamp { .. } => "TIMESTAMP".to_string(),
            PostgresType::Interval => "INTERVAL".to_string(),
            PostgresType::Numeric { .. } => "NUMERIC".to_string(),
            PostgresType::List(inner) => {
                // arrays of structs and such are not supported
                let inner_tp = inner.data_type.name().unwrap();
                format!("{inner_tp}[]")
            }
        };
        Some(v)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub data_type: PostgresType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct PostgresSchema {
    pub columns: Vec<(String, Column)>,
}

impl PostgresSchema {
    /// Generate a DDL column definition string (e.g., "name TEXT NOT NULL")
    ///
    /// # Arguments
    /// * `column_name` - The name of the column
    /// * `column` - The column definition with type and nullability
    ///
    /// # Returns
    /// A string like "column_name TYPE [NOT NULL]"
    pub fn column_to_ddl(column_name: &str, column: &Column) -> String {
        let type_str = column.data_type.to_ddl_string();
        let nullable_str = if column.nullable { "" } else { " NOT NULL" };
        format!("{} {}{}", column_name, type_str, nullable_str)
    }
}
