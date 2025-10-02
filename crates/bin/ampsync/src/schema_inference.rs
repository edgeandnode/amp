//! Schema inference by querying Nozzle server.
//!
//! This module infers Arrow schemas for SQL queries by executing them
//! with LIMIT 0 on the Nozzle server and extracting the schema from
//! the result metadata.

use common::BoxError;
use datasets_derived::manifest::ArrowSchema;
use nozzle_client::SqlClient;

/// Infer the Arrow schema for a SQL query by querying the Nozzle server with LIMIT 1.
///
/// This executes the query without retrieving data, only metadata including the schema.
/// Uses a SELECT subquery approach to provide the dataset context.
///
/// # Arguments
/// * `nozzle_client` - Connected Nozzle SQL client
/// * `sql` - The SQL query to infer schema for
/// * `dataset_name` - The dataset name (currently unused, kept for API compatibility)
/// * `table_name` - The table name (used in error messages)
///
/// # Returns
/// * `Result<ArrowSchema, BoxError>` - Inferred Arrow schema or error
pub async fn infer_schema(
    nozzle_client: &mut SqlClient,
    sql: &str,
    _dataset_name: &str,
    table_name: &str,
) -> Result<ArrowSchema, BoxError> {
    // Execute the user's SQL directly with LIMIT 1 to get the schema
    // We use LIMIT 1 to ensure we get at least one batch with the schema
    // The user's SQL already contains the dataset references (e.g., anvil.blocks)
    let schema_query = format!("{} LIMIT 1", sql);

    // Execute the query
    let mut result_stream = nozzle_client
        .query(&schema_query, None, None)
        .await
        .map_err(|e| {
            format!(
                "Failed to execute schema query for table '{}': {}",
                table_name, e
            )
        })?;

    // Get the first batch (should be empty due to LIMIT 0)
    use futures::StreamExt;

    if let Some(result) = result_stream.next().await {
        let batch = result.map_err(|e| {
            format!(
                "Failed to get result batch for table '{}': {}",
                table_name, e
            )
        })?;

        // Extract the Arrow schema from the batch's data field
        let arrow_schema = batch.data.schema();

        // Convert to our ArrowSchema format
        convert_arrow_schema_to_manifest_schema(arrow_schema.as_ref())
    } else {
        Err(format!(
            "No result batch received from Nozzle server for table '{}'",
            table_name
        )
        .into())
    }
}

/// Convert an Arrow Schema to our manifest ArrowSchema format.
///
/// This transforms from the Arrow library's Schema type to our serializable
/// manifest representation. Converts nanosecond timestamps to microsecond
/// precision for PostgreSQL compatibility.
fn convert_arrow_schema_to_manifest_schema(
    schema: &arrow_schema::Schema,
) -> Result<ArrowSchema, BoxError> {
    use arrow_schema::TimeUnit;
    use datasets_common::manifest::DataType;
    use datasets_derived::manifest::Field;

    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = field.data_type();
            // Convert nanosecond timestamps to microsecond precision for PostgreSQL compatibility
            let converted_type = match data_type {
                arrow_schema::DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                    arrow_schema::DataType::Timestamp(TimeUnit::Microsecond, tz.clone())
                }
                other => other.clone(),
            };

            Field {
                name: field.name().clone(),
                type_: DataType(converted_type),
                nullable: field.is_nullable(),
            }
        })
        .collect();

    Ok(ArrowSchema { fields })
}
