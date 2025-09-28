use std::ops::Deref;

use bytes::BytesMut;
use common::{
    BoxError,
    arrow::array::RecordBatch,
    manifest::derived::{ArrowSchema, Field},
};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use nozzle_client::InvalidationRange;
use pgpq::ArrowToPostgresBinaryEncoder;
use sqlx::{Pool, Postgres};

use crate::conn::DbConnPool;

/// Convert Arrow schema to PostgreSQL CREATE TABLE statement
pub fn arrow_schema_to_postgres_ddl(
    table_name: &str,
    schema: &ArrowSchema,
) -> Result<String, BoxError> {
    let mut columns = Vec::new();

    for field in &schema.fields {
        let column_def = arrow_field_to_postgres_column(field)?;
        columns.push(column_def);
    }

    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {} (\n  {}\n)",
        table_name,
        columns.join(",\n  ")
    );

    Ok(ddl)
}

/// Convert a single Arrow field to PostgreSQL column definition
fn arrow_field_to_postgres_column(field: &Field) -> Result<String, BoxError> {
    let pg_type = arrow_type_to_postgres_type(field.type_.as_arrow())?;
    let nullable = if field.nullable { "" } else { " NOT NULL" };

    Ok(format!("{} {}{}", field.name, pg_type, nullable))
}

/// Map Arrow DataType to PostgreSQL type
fn arrow_type_to_postgres_type(data_type: &ArrowDataType) -> Result<String, BoxError> {
    let pg_type = match data_type {
        // Integer types
        ArrowDataType::Int8 => "SMALLINT",
        ArrowDataType::Int16 => "SMALLINT",
        ArrowDataType::Int32 => "INTEGER",
        ArrowDataType::Int64 => "BIGINT",
        ArrowDataType::UInt8 => "SMALLINT",
        ArrowDataType::UInt16 => "INTEGER",
        ArrowDataType::UInt32 => "BIGINT",
        ArrowDataType::UInt64 => "NUMERIC(20)", // PostgreSQL doesn't have unsigned 64-bit

        // Floating point types
        ArrowDataType::Float16 => "REAL",
        ArrowDataType::Float32 => "REAL",
        ArrowDataType::Float64 => "DOUBLE PRECISION",

        // String types
        ArrowDataType::Utf8 => "TEXT",
        ArrowDataType::LargeUtf8 => "TEXT",

        // Binary types
        ArrowDataType::Binary => "BYTEA",
        ArrowDataType::LargeBinary => "BYTEA",
        ArrowDataType::FixedSizeBinary(_) => "BYTEA",

        // Boolean
        ArrowDataType::Boolean => "BOOLEAN",

        // Date and time types
        ArrowDataType::Date32 => "DATE",
        ArrowDataType::Date64 => "DATE",
        ArrowDataType::Time32(_) => "TIME",
        ArrowDataType::Time64(_) => "TIME",
        ArrowDataType::Timestamp(_, timezone) => match timezone {
            Some(_) => "TIMESTAMPTZ",
            None => "TIMESTAMP",
        },

        // Decimal types
        ArrowDataType::Decimal128(precision, scale) => {
            return Ok(format!("NUMERIC({}, {})", precision, scale));
        }

        // List and struct types - map to JSONB for flexibility
        ArrowDataType::List(_) => "JSONB",
        ArrowDataType::LargeList(_) => "JSONB",
        ArrowDataType::FixedSizeList(_, _) => "JSONB",
        ArrowDataType::Struct(_) => "JSONB",
        ArrowDataType::Map(_, _) => "JSONB",

        // Other types
        ArrowDataType::Null => "TEXT", // Fallback for null type
        ArrowDataType::Dictionary(_, value_type) => {
            // For dictionary encoding, use the value type
            return arrow_type_to_postgres_type(value_type);
        }

        // Unsupported types - fall back to TEXT
        _ => "TEXT",
    };

    Ok(pg_type.to_string())
}

#[derive(Clone)]
pub struct AmpsyncDbEngine {
    pool: Pool<Postgres>,
}

impl AmpsyncDbEngine {
    pub fn new(pool: &DbConnPool) -> Self {
        Self {
            pool: pool.deref().clone(),
        }
    }

    /// Create a database table based on Arrow schema
    pub async fn create_table_from_schema(
        &self,
        table_name: &str,
        schema: &ArrowSchema,
    ) -> Result<(), BoxError> {
        let ddl = arrow_schema_to_postgres_ddl(table_name, schema)?;

        tracing::info!("Ensuring table '{}' exists with DDL: {}", table_name, ddl);

        sqlx::query(&ddl)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("Failed to create table '{}': {}", table_name, e))?;

        Ok(())
    }

    /// Insert a RecordBatch into a PostgreSQL table using high-performance bulk copy
    pub async fn insert_record_batch(
        &self,
        table_name: &str,
        batch: &RecordBatch,
    ) -> Result<(), BoxError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // Create the pgpq encoder for this batch's schema
        let mut encoder = ArrowToPostgresBinaryEncoder::try_new(batch.schema().as_ref())
            .map_err(|e| format!("Failed to create pgpq encoder: {:?}", e))?;

        // Encode the batch to PostgreSQL binary format
        let mut buffer = BytesMut::with_capacity(batch.num_rows() * 1024); // Estimate capacity

        // Write header
        encoder.write_header(&mut buffer);

        // Write the batch data
        encoder
            .write_batch(batch, &mut buffer)
            .map_err(|e| format!("Failed to encode batch with pgpq: {:?}", e))?;

        // Write footer
        encoder
            .write_footer(&mut buffer)
            .map_err(|e| format!("Failed to write pgpq footer: {:?}", e))?;

        // Get column names for the COPY command
        let schema_fields = batch.schema();
        let column_names: Vec<&str> = schema_fields
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let columns_clause = column_names.join(", ");

        // Execute COPY FROM STDIN with binary format
        let copy_query = format!(
            "COPY {} ({}) FROM STDIN WITH (FORMAT BINARY)",
            table_name, columns_clause
        );

        // Get a connection from the pool and execute the COPY command
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| format!("Failed to acquire database connection: {}", e))?;

        // Use PostgreSQL's COPY protocol for maximum performance
        let mut copy_in = conn
            .copy_in_raw(&copy_query)
            .await
            .map_err(|e| format!("Failed to initiate COPY command: {}", e))?;

        // Send the binary data
        copy_in
            .send(buffer.freeze())
            .await
            .map_err(|e| format!("Failed to send binary data: {}", e))?;

        // Finish the copy operation
        let rows_affected = copy_in
            .finish()
            .await
            .map_err(|e| format!("Failed to finish COPY operation: {}", e))?;

        tracing::debug!(
            "Successfully bulk inserted {} rows into table '{}' (rows affected: {})",
            batch.num_rows(),
            table_name,
            rows_affected
        );

        Ok(())
    }

    /// Handle blockchain reorganization by deleting affected rows
    pub async fn handle_reorg(
        &self,
        table_name: &str,
        invalidation_ranges: &[InvalidationRange],
    ) -> Result<(), BoxError> {
        if invalidation_ranges.is_empty() {
            return Ok(());
        }

        // Build a WHERE clause that covers all invalidation ranges
        let mut where_conditions = Vec::new();
        for range in invalidation_ranges {
            let condition = format!(
                "(block_num >= {} AND block_num <= {})",
                range.numbers.start(),
                range.numbers.end()
            );
            where_conditions.push(condition);
        }

        let where_clause = where_conditions.join(" OR ");
        let delete_query = format!("DELETE FROM {} WHERE {}", table_name, where_clause);

        tracing::info!(
            "Handling reorg for table '{}' with {} invalidation ranges: {}",
            table_name,
            invalidation_ranges.len(),
            delete_query
        );

        let result = sqlx::query(&delete_query)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                format!(
                    "Failed to delete invalidated rows from table '{}': {}",
                    table_name, e
                )
            })?;

        let rows_deleted = result.rows_affected();
        if rows_deleted > 0 {
            tracing::warn!(
                "Deleted {} rows from table '{}' due to blockchain reorganization",
                rows_deleted,
                table_name
            );
        } else {
            tracing::debug!(
                "No rows needed deletion in table '{}' for the invalidation ranges",
                table_name
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType as ArrowDataType;

    use super::*;

    #[test]
    fn test_arrow_type_mappings() {
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Int32).unwrap(),
            "INTEGER"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Utf8).unwrap(),
            "TEXT"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Boolean).unwrap(),
            "BOOLEAN"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Float64).unwrap(),
            "DOUBLE PRECISION"
        );
        assert_eq!(
            arrow_type_to_postgres_type(&ArrowDataType::Binary).unwrap(),
            "BYTEA"
        );
    }
}
