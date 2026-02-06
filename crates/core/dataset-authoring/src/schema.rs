//! Schema inference via DataFusion.
//!
//! Infers Arrow schemas by planning CTAS SELECT statements against dependency
//! table schemas. This module provides the tooling to validate SQL queries and
//! extract their output schemas without executing them.
//!
//! # Overview
//!
//! During dataset authoring, we need to infer the output schema of each table
//! from its CTAS statement. This is done by:
//!
//! 1. Creating a planning context with dependency table schemas
//! 2. Planning the SELECT portion of the CTAS statement
//! 3. Extracting the schema from the logical plan
//!
//! # Example
//!
//! ```ignore
//! use dataset_authoring::schema::{SchemaContext, DependencyTable};
//! use datafusion::arrow::datatypes::SchemaRef;
//!
//! // Create context with dependency tables
//! let mut ctx = SchemaContext::new();
//! ctx.register_table(DependencyTable {
//!     schema_name: "source".to_string(),
//!     table_name: "transactions".parse().unwrap(),
//!     schema: schema_ref,
//! });
//!
//! // Infer schema from SELECT query - returns Arrow SchemaRef
//! let schema: SchemaRef = ctx.infer_schema(&query).await?;
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{MemorySchemaProvider, Session, TableProvider},
    common::DFSchemaRef,
    datasource::TableType,
    error::DataFusionError,
    execution::{SessionStateBuilder, config::SessionConfig, context::SessionContext},
    logical_expr::{Expr, ScalarUDF},
    physical_plan::ExecutionPlan,
    sql::sqlparser::ast,
};
use datasets_common::table_name::TableName;

/// A dependency table to register for schema inference.
///
/// Represents a table from a dependency dataset that can be referenced
/// in SQL queries during schema inference.
#[derive(Debug, Clone)]
pub struct DependencyTable {
    /// The schema name under which the table is registered (e.g., the dependency alias).
    pub schema_name: String,
    /// The table name within the schema.
    pub table_name: TableName,
    /// The Arrow schema of the table.
    pub schema: SchemaRef,
}

/// Context for inferring schemas from SQL queries.
///
/// Holds dependency table schemas and provides methods to plan SQL
/// queries and extract their output schemas.
pub struct SchemaContext {
    session_config: SessionConfig,
    tables: Vec<DependencyTable>,
    udfs: Vec<ScalarUDF>,
}

impl Default for SchemaContext {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaContext {
    /// Creates a new empty schema context.
    pub fn new() -> Self {
        let session_config = SessionConfig::from_env().unwrap_or_default().set(
            "datafusion.catalog.default_catalog",
            &common::query_context::default_catalog_name(),
        );

        Self {
            session_config,
            tables: Vec::new(),
            udfs: Vec::new(),
        }
    }

    /// Registers a dependency table for use in schema inference.
    pub fn register_table(&mut self, table: DependencyTable) {
        self.tables.push(table);
    }

    /// Registers a custom UDF for schema inference.
    pub fn register_udf(&mut self, udf: ScalarUDF) {
        self.udfs.push(udf);
    }

    /// Infers the output schema of a SELECT query.
    ///
    /// Plans the query against the registered dependency tables and
    /// returns the Arrow schema of the query result.
    ///
    /// # Arguments
    ///
    /// * `query` - The SELECT query to plan (the inner query from a CTAS statement).
    ///
    /// # Returns
    ///
    /// * `Ok(SchemaRef)` - The inferred Arrow schema of the query result.
    /// * `Err(SchemaInferenceError)` - If planning fails.
    pub async fn infer_schema(
        &self,
        query: &ast::Query,
    ) -> Result<SchemaRef, SchemaInferenceError> {
        let ctx = self.create_session_context()?;

        // Create a SELECT statement from the query
        let stmt = datafusion::sql::parser::Statement::Statement(Box::new(ast::Statement::Query(
            Box::new(query.clone()),
        )));

        // Plan the statement to get the logical plan
        let plan = ctx
            .state()
            .statement_to_plan(stmt)
            .await
            .map_err(SchemaInferenceError::Planning)?;

        // Extract schema from the logical plan - convert DFSchema to Arrow Schema
        let df_schema: DFSchemaRef = plan.schema().clone();
        Ok(Arc::new(df_schema.as_arrow().clone()))
    }

    /// Creates a DataFusion session context with all registered tables.
    ///
    /// This is useful when you need access to the session context for
    /// additional validation (e.g., incremental constraint checking).
    ///
    /// # Errors
    ///
    /// Returns [`SchemaInferenceError::TableRegistration`] if table registration fails.
    pub fn create_session_for_validation(&self) -> Result<SessionContext, SchemaInferenceError> {
        self.create_session_context()
    }

    /// Creates a DataFusion session context with all registered tables.
    fn create_session_context(&self) -> Result<SessionContext, SchemaInferenceError> {
        let state = SessionStateBuilder::new()
            .with_config(self.session_config.clone())
            .with_runtime_env(Default::default())
            .with_default_features()
            .build();

        let ctx = SessionContext::new_with_state(state);

        // Register each dependency table
        for table in &self.tables {
            // Create the schema (namespace) if it doesn't exist
            self.ensure_schema_exists(&ctx, &table.schema_name);

            // Create a schema-only table provider
            let provider = Arc::new(SchemaOnlyTable {
                schema: table.schema.clone(),
            });

            // Build the table reference
            let table_ref = datafusion::sql::TableReference::partial(
                table.schema_name.clone(),
                table.table_name.as_str().to_string(),
            );

            ctx.register_table(table_ref, provider)
                .map_err(SchemaInferenceError::TableRegistration)?;
        }

        // Register built-in UDFs
        self.register_udfs(&ctx);

        Ok(ctx)
    }

    /// Ensures a schema (namespace) exists in the catalog.
    fn ensure_schema_exists(&self, ctx: &SessionContext, schema_name: &str) {
        let catalog_names = ctx.catalog_names();
        let catalog_name = catalog_names.first().expect("default catalog should exist");

        if let Some(catalog) = ctx.catalog(catalog_name)
            && catalog.schema(schema_name).is_none()
        {
            let schema_provider = Arc::new(MemorySchemaProvider::new());
            // Ignoring the result - if registration fails, table registration will fail later
            let _ = catalog.register_schema(schema_name, schema_provider);
        }
    }

    /// Registers built-in UDFs for query planning.
    fn register_udfs(&self, ctx: &SessionContext) {
        // Register the same UDFs that are available in the runtime
        for udf in common::query_context::udfs() {
            ctx.register_udf(udf);
        }
        for udaf in common::query_context::udafs() {
            ctx.register_udaf(udaf);
        }
        for udf in &self.udfs {
            ctx.register_udf(udf.clone());
        }
    }
}

/// A minimal TableProvider that only provides schema information.
///
/// Used for planning queries without execution. The `scan` method is
/// unreachable as this provider is only used for schema inference.
#[derive(Debug)]
struct SchemaOnlyTable {
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for SchemaOnlyTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        unreachable!("SchemaOnlyTable should never be scanned; it is only used for planning")
    }
}

/// Errors that can occur during schema inference.
#[derive(Debug, thiserror::Error)]
pub enum SchemaInferenceError {
    /// Failed to plan the SQL query.
    #[error("failed to plan SQL query")]
    Planning(#[source] DataFusionError),

    /// Failed to register a table in the catalog.
    #[error("failed to register table in catalog")]
    TableRegistration(#[source] DataFusionError),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    /// Creates a simple test schema with the given fields.
    fn test_schema(fields: Vec<(&str, DataType, bool)>) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, dtype, nullable)| Field::new(name, dtype, nullable))
                .collect::<Vec<_>>(),
        ))
    }

    mod infer_schema_tests {
        use datafusion::sql::sqlparser::parser::Parser;

        use super::*;

        /// Helper to parse a SELECT query from SQL string.
        fn parse_select_query(sql: &str) -> ast::Query {
            let dialect = datafusion::sql::sqlparser::dialect::GenericDialect {};
            let mut statements = Parser::parse_sql(&dialect, sql).expect("SQL should parse");
            match statements.pop().expect("should have one statement") {
                ast::Statement::Query(query) => *query,
                other => panic!("expected SELECT query, got: {:?}", other),
            }
        }

        #[tokio::test]
        async fn infers_simple_select_all() {
            //* Given
            let mut ctx = SchemaContext::new();
            ctx.register_table(DependencyTable {
                schema_name: "source".to_string(),
                table_name: "transactions".parse().unwrap(),
                schema: test_schema(vec![
                    ("block_num", DataType::UInt64, false),
                    ("tx_hash", DataType::Utf8, false),
                    ("value", DataType::UInt64, true),
                ]),
            });

            let query = parse_select_query("SELECT * FROM source.transactions");

            //* When
            let result = ctx.infer_schema(&query).await;

            //* Then
            let schema = result.expect("schema inference should succeed");
            assert_eq!(schema.fields().len(), 3);
            assert_eq!(schema.field(0).name(), "block_num");
            assert_eq!(schema.field(1).name(), "tx_hash");
            assert_eq!(schema.field(2).name(), "value");
        }

        #[tokio::test]
        async fn infers_projected_columns() {
            //* Given
            let mut ctx = SchemaContext::new();
            ctx.register_table(DependencyTable {
                schema_name: "source".to_string(),
                table_name: "transactions".parse().unwrap(),
                schema: test_schema(vec![
                    ("block_num", DataType::UInt64, false),
                    ("tx_hash", DataType::Utf8, false),
                    ("value", DataType::UInt64, true),
                    ("gas_used", DataType::UInt64, true),
                ]),
            });

            let query = parse_select_query("SELECT block_num, tx_hash FROM source.transactions");

            //* When
            let result = ctx.infer_schema(&query).await;

            //* Then
            let schema = result.expect("schema inference should succeed");
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "block_num");
            assert_eq!(schema.field(1).name(), "tx_hash");
        }

        #[tokio::test]
        async fn infers_aliased_columns() {
            //* Given
            let mut ctx = SchemaContext::new();
            ctx.register_table(DependencyTable {
                schema_name: "source".to_string(),
                table_name: "transactions".parse().unwrap(),
                schema: test_schema(vec![
                    ("block_num", DataType::UInt64, false),
                    ("value", DataType::UInt64, true),
                ]),
            });

            let query = parse_select_query(
                "SELECT block_num AS block, value AS amount FROM source.transactions",
            );

            //* When
            let result = ctx.infer_schema(&query).await;

            //* Then
            let schema = result.expect("schema inference should succeed");
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "block");
            assert_eq!(schema.field(1).name(), "amount");
        }

        #[tokio::test]
        async fn infers_filtered_query() {
            //* Given
            let mut ctx = SchemaContext::new();
            ctx.register_table(DependencyTable {
                schema_name: "source".to_string(),
                table_name: "transactions".parse().unwrap(),
                schema: test_schema(vec![
                    ("block_num", DataType::UInt64, false),
                    ("value", DataType::UInt64, true),
                ]),
            });

            let query = parse_select_query("SELECT * FROM source.transactions WHERE value > 1000");

            //* When
            let result = ctx.infer_schema(&query).await;

            //* Then
            let schema = result.expect("schema inference should succeed");
            assert_eq!(schema.fields().len(), 2);
        }

        #[tokio::test]
        async fn infers_join_schema() {
            //* Given
            let mut ctx = SchemaContext::new();
            ctx.register_table(DependencyTable {
                schema_name: "source".to_string(),
                table_name: "transactions".parse().unwrap(),
                schema: test_schema(vec![
                    ("block_num", DataType::UInt64, false),
                    ("tx_hash", DataType::Utf8, false),
                ]),
            });
            ctx.register_table(DependencyTable {
                schema_name: "source".to_string(),
                table_name: "blocks".parse().unwrap(),
                schema: test_schema(vec![
                    ("number", DataType::UInt64, false),
                    ("timestamp", DataType::UInt64, false),
                ]),
            });

            let query = parse_select_query(
                "SELECT t.tx_hash, b.timestamp \
                 FROM source.transactions t \
                 JOIN source.blocks b ON t.block_num = b.number",
            );

            //* When
            let result = ctx.infer_schema(&query).await;

            //* Then
            let schema = result.expect("schema inference should succeed");
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "tx_hash");
            assert_eq!(schema.field(1).name(), "timestamp");
        }

        #[tokio::test]
        async fn infers_cte_schema() {
            //* Given
            let mut ctx = SchemaContext::new();
            ctx.register_table(DependencyTable {
                schema_name: "source".to_string(),
                table_name: "transactions".parse().unwrap(),
                schema: test_schema(vec![
                    ("block_num", DataType::UInt64, false),
                    ("value", DataType::UInt64, true),
                ]),
            });

            let query = parse_select_query(
                "WITH filtered AS (SELECT * FROM source.transactions WHERE value > 0) \
                 SELECT block_num FROM filtered",
            );

            //* When
            let result = ctx.infer_schema(&query).await;

            //* Then
            let schema = result.expect("schema inference should succeed");
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).name(), "block_num");
        }

        #[tokio::test]
        async fn preserves_nullability() {
            //* Given
            let mut ctx = SchemaContext::new();
            ctx.register_table(DependencyTable {
                schema_name: "source".to_string(),
                table_name: "data".parse().unwrap(),
                schema: test_schema(vec![
                    ("required_col", DataType::UInt64, false),
                    ("optional_col", DataType::UInt64, true),
                ]),
            });

            let query = parse_select_query("SELECT required_col, optional_col FROM source.data");

            //* When
            let result = ctx.infer_schema(&query).await;

            //* Then
            let schema = result.expect("schema inference should succeed");
            assert!(
                !schema.field(0).is_nullable(),
                "required_col should not be nullable"
            );
            assert!(
                schema.field(1).is_nullable(),
                "optional_col should be nullable"
            );
        }

        #[tokio::test]
        async fn infers_expression_types() {
            //* Given
            let mut ctx = SchemaContext::new();
            ctx.register_table(DependencyTable {
                schema_name: "source".to_string(),
                table_name: "data".parse().unwrap(),
                schema: test_schema(vec![
                    ("a", DataType::Int64, false),
                    ("b", DataType::Int64, false),
                ]),
            });

            let query =
                parse_select_query("SELECT a + b AS sum, a * 2 AS doubled FROM source.data");

            //* When
            let result = ctx.infer_schema(&query).await;

            //* Then
            let schema = result.expect("schema inference should succeed");
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "sum");
            assert_eq!(schema.field(1).name(), "doubled");
        }

        #[tokio::test]
        async fn fails_on_missing_table() {
            //* Given
            let ctx = SchemaContext::new(); // No tables registered

            let query = parse_select_query("SELECT * FROM missing.table");

            //* When
            let result = ctx.infer_schema(&query).await;

            //* Then
            assert!(result.is_err(), "should fail when table doesn't exist");
            let err = result.unwrap_err();
            assert!(
                matches!(err, SchemaInferenceError::Planning(_)),
                "expected Planning error, got: {:?}",
                err
            );
        }

        #[tokio::test]
        async fn fails_on_missing_column() {
            //* Given
            let mut ctx = SchemaContext::new();
            ctx.register_table(DependencyTable {
                schema_name: "source".to_string(),
                table_name: "data".parse().unwrap(),
                schema: test_schema(vec![("existing_col", DataType::UInt64, false)]),
            });

            let query = parse_select_query("SELECT nonexistent_col FROM source.data");

            //* When
            let result = ctx.infer_schema(&query).await;

            //* Then
            assert!(result.is_err(), "should fail when column doesn't exist");
        }

        #[tokio::test]
        async fn handles_multiple_schemas() {
            //* Given
            let mut ctx = SchemaContext::new();
            ctx.register_table(DependencyTable {
                schema_name: "eth".to_string(),
                table_name: "blocks".parse().unwrap(),
                schema: test_schema(vec![("number", DataType::UInt64, false)]),
            });
            ctx.register_table(DependencyTable {
                schema_name: "base".to_string(),
                table_name: "blocks".parse().unwrap(),
                schema: test_schema(vec![("block_number", DataType::UInt64, false)]),
            });

            let query = parse_select_query(
                "SELECT e.number, b.block_number \
                 FROM eth.blocks e \
                 JOIN base.blocks b ON e.number = b.block_number",
            );

            //* When
            let result = ctx.infer_schema(&query).await;

            //* Then
            let schema = result.expect("schema inference should succeed");
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "number");
            assert_eq!(schema.field(1).name(), "block_number");
        }

        #[tokio::test]
        async fn arrow_cast_produces_explicit_type() {
            //* Given
            let mut ctx = SchemaContext::new();
            ctx.register_table(DependencyTable {
                schema_name: "source".to_string(),
                table_name: "data".parse().unwrap(),
                schema: test_schema(vec![
                    ("int_col", DataType::Int64, false),
                    ("str_col", DataType::Utf8, false),
                ]),
            });

            // Use arrow_cast to explicitly convert Int64 to UInt32
            let query = parse_select_query(
                "SELECT arrow_cast(int_col, 'UInt32') AS casted_col FROM source.data",
            );

            //* When
            let result = ctx.infer_schema(&query).await;

            //* Then
            let schema = result.expect("schema inference should succeed");
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).name(), "casted_col");
            assert_eq!(
                schema.field(0).data_type(),
                &DataType::UInt32,
                "arrow_cast should produce the explicitly specified type"
            );
        }

        #[tokio::test]
        async fn arrow_cast_works_with_complex_types() {
            //* Given
            let mut ctx = SchemaContext::new();
            ctx.register_table(DependencyTable {
                schema_name: "source".to_string(),
                table_name: "data".parse().unwrap(),
                schema: test_schema(vec![("value", DataType::Int64, false)]),
            });

            // Cast to Decimal type with precision and scale
            let query = parse_select_query(
                "SELECT arrow_cast(value, 'Decimal128(18, 8)') AS decimal_col FROM source.data",
            );

            //* When
            let result = ctx.infer_schema(&query).await;

            //* Then
            let schema = result.expect("schema inference should succeed");
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).name(), "decimal_col");
            // Verify the type is Decimal128 with the specified precision and scale
            assert!(
                matches!(schema.field(0).data_type(), DataType::Decimal128(18, 8)),
                "arrow_cast should produce Decimal128(18, 8), got: {:?}",
                schema.field(0).data_type()
            );
        }
    }
}
