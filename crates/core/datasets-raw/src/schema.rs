//! Schema generation utilities for creating markdown documentation from table schemas.
//!
//! This module provides functions to generate human-readable markdown documentation
//! from DataFusion table schemas. It's primarily used during build-time code generation
//! to create schema documentation files.

use std::sync::Arc;

// Re-export Table type from common for convenience
pub use common::Table;
use datafusion::{
    arrow::util::pretty::pretty_format_batches,
    logical_expr::{DescribeTable, LogicalPlan},
    prelude::SessionContext,
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

/// Format a single table's schema as a pretty-printed string.
///
/// This function uses DataFusion's `DESCRIBE` logical plan to extract schema
/// information and formats it using the pretty print utility. This function is only
/// used during build-time code generation, where failures should be immediately visible.
async fn print_schema(table: &Table) -> String {
    let plan = LogicalPlan::DescribeTable(DescribeTable {
        schema: table.schema().clone(),
        output_schema: Arc::new(LogicalPlan::describe_schema().try_into().unwrap()),
    });
    let ctx = SessionContext::new();

    // Unwraps: No reason for a `describe` to fail.
    let df = ctx.execute_logical_plan(plan).await.unwrap();
    let batches = df.collect().await.unwrap();
    pretty_format_batches(&batches).unwrap().to_string()
}
