//! Schema generation utilities for creating markdown documentation from table schemas.
//!
//! This module provides functions to generate human-readable markdown documentation
//! from DataFusion table schemas. It's primarily used during build-time code generation
//! to create schema documentation files.

use std::sync::Arc;

// Re-export Table type from common for convenience
pub use common::Table;
use datafusion::arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
    util::pretty::pretty_format_batches,
};

/// Convert a collection of tables into a markdown-formatted schema document.
///
/// This function generates a markdown document with a header and sections for each table,
/// where each section contains the table name and its formatted schema information.
///
/// ## Example Output
/// ```markdown
/// # Schema
/// Auto-generated file. See `to_markdown` in `datasets-raw/src/schema.rs`.
///
/// ## table_name
/// ````
/// +-------------+------------------+-------------+
/// | column_name | data_type        | is_nullable |
/// +-------------+------------------+-------------+
/// | id          | Int64            | NO          |
/// | name        | Utf8             | YES         |
/// +-------------+------------------+-------------+
/// ````
/// ```
pub async fn to_markdown(tables: Vec<Table>) -> String {
    let mut markdown = String::new();
    markdown.push_str("# Schema\n");
    markdown.push_str(&format!(
        "Auto-generated file. See `to_markdown` in `{}`.\n\n",
        file!()
    ));
    for table in tables {
        markdown.push_str(&format!("## {}\n", table.name()));
        markdown.push_str("````\n");
        markdown.push_str(&print_schema(&table).await);
        markdown.push_str("\n````\n");
    }

    markdown
}

/// Formats an Arrow DataType in a compact, human-readable form.
///
/// This function creates single-line representations of complex nested types
/// by omitting metadata fields (dict_id, dict_is_ordered, metadata) that are
/// typically default values and not relevant for documentation.
///
/// ## Examples
/// - Simple: `Int64` → `Int64`
/// - List: `List(Field { ... })` → `List(Int64)`
/// - Struct: `Struct([...])` → `Struct(field1: Type1, field2: Type2)`
/// - Nested: `List(Field { ... Struct(...) })` → `List(Struct(...))`
fn format_datatype_compact(data_type: &DataType) -> String {
    match data_type {
        // Simple types - use default string representation
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Date32
        | DataType::Date64
        | DataType::Duration(_)
        | DataType::Interval(_) => format!("{}", data_type),

        // Timestamp - include timezone
        DataType::Timestamp(unit, tz) => {
            format!(
                "Timestamp({:?}, {})",
                unit,
                tz.as_ref().map_or("None", |s| s)
            )
        }

        // FixedSizeBinary - include size
        DataType::FixedSizeBinary(size) => format!("FixedSizeBinary({})", size),

        // Decimal128 - include precision and scale
        DataType::Decimal128(precision, scale) => format!("Decimal128({}, {})", precision, scale),

        // Decimal256 - include precision and scale
        DataType::Decimal256(precision, scale) => format!("Decimal256({}, {})", precision, scale),

        // List - format as List(inner_type)
        DataType::List(field) => {
            let inner = format_datatype_compact(field.data_type());
            format!("List({})", inner)
        }

        // LargeList - format as LargeList(inner_type)
        DataType::LargeList(field) => {
            let inner = format_datatype_compact(field.data_type());
            format!("LargeList({})", inner)
        }

        // FixedSizeList - format as FixedSizeList(inner_type, size)
        DataType::FixedSizeList(field, size) => {
            let inner = format_datatype_compact(field.data_type());
            format!("FixedSizeList({}, {})", inner, size)
        }

        // Struct - format as Struct(field1: type1, field2: type2, ...)
        DataType::Struct(fields) => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|f| format!("{}: {}", f.name(), format_datatype_compact(f.data_type())))
                .collect();
            format!("Struct({})", field_strs.join(", "))
        }

        // Union - format as Union(fields...)
        DataType::Union(fields, _mode) => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|(_id, f)| format!("{}: {}", f.name(), format_datatype_compact(f.data_type())))
                .collect();
            format!("Union({})", field_strs.join(", "))
        }

        // Map - format as Map(key, value)
        DataType::Map(field, _sorted) => {
            if let DataType::Struct(struct_fields) = field.data_type()
                && struct_fields.len() == 2
            {
                let key = format_datatype_compact(struct_fields[0].data_type());
                let value = format_datatype_compact(struct_fields[1].data_type());
                return format!("Map({}, {})", key, value);
            }
            format!("Map({})", format_datatype_compact(field.data_type()))
        }

        // Dictionary - format as Dictionary(key, value)
        DataType::Dictionary(key, value) => {
            format!(
                "Dictionary({}, {})",
                format_datatype_compact(key.as_ref()),
                format_datatype_compact(value.as_ref())
            )
        }

        // RunEndEncoded - format as RunEndEncoded(run_ends, values)
        DataType::RunEndEncoded(run_ends, values) => {
            format!(
                "RunEndEncoded({}, {})",
                format_datatype_compact(run_ends.data_type()),
                format_datatype_compact(values.data_type())
            )
        }

        // Fallback for any other types
        _ => format!("{}", data_type),
    }
}

/// Format a single table's schema as a pretty-printed string with compact type notation.
///
/// This function directly formats the table schema into a markdown-friendly table format,
/// using compact notation for complex nested types. This function is only used during
/// build-time code generation, where failures should be immediately visible.
async fn print_schema(table: &Table) -> String {
    // Build our own RecordBatch with compact formatting
    let schema_ref = table.schema();
    let fields = schema_ref.fields();

    // Create arrays for each column
    let column_names: StringArray = fields.iter().map(|f| Some(f.name().as_str())).collect();

    let data_types: StringArray = fields
        .iter()
        .map(|f| Some(format_datatype_compact(f.data_type())))
        .collect();

    let is_nullable: StringArray = fields
        .iter()
        .map(|f| Some(if f.is_nullable() { "YES" } else { "NO" }))
        .collect();

    // Create the DESCRIBE output schema
    let describe_schema = Schema::new(vec![
        Field::new("column_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("is_nullable", DataType::Utf8, false),
    ]);

    // Create RecordBatch
    let batch = RecordBatch::try_new(
        Arc::new(describe_schema),
        vec![
            Arc::new(column_names) as ArrayRef,
            Arc::new(data_types) as ArrayRef,
            Arc::new(is_nullable) as ArrayRef,
        ],
    )
    .unwrap();

    // Pretty format the batch
    pretty_format_batches(&[batch]).unwrap().to_string()
}
