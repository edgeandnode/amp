use std::sync::Arc;

use axum::http::StatusCode;
use axum::{extract::State, Json};
use common::manifest::TableSchema;
use common::query_context::parse_sql;
use common::query_context::Error as QueryContextError;
use dataset_store::DatasetStore;
use http_common::{BoxRequestError, RequestError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::instrument;

use crate::ServiceState;

#[derive(Debug, Deserialize)]
pub struct OutputSchemaRequest {
    sql_query: String,
}

#[derive(Debug, Serialize)]
pub struct OutputSchemaResponse {
    schema: TableSchema,
}

#[derive(Debug, Error)]
enum OutputSchemaError {
    #[error("SQL parse error: {0}")]
    SqlParseError(QueryContextError),
    #[error("Dataset store error: {0}")]
    DatasetStoreError(dataset_store::DatasetError),
    #[error("Planning error: {0}")]
    PlanningError(QueryContextError),
}

impl RequestError for OutputSchemaError {
    fn error_code(&self) -> &'static str {
        match self {
            OutputSchemaError::SqlParseError(_) => "SQL_PARSE_ERROR",
            OutputSchemaError::DatasetStoreError(_) => "DATASET_STORE_ERROR",
            OutputSchemaError::PlanningError(_) => "PLANNING_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            OutputSchemaError::SqlParseError(_) => StatusCode::BAD_REQUEST,
            OutputSchemaError::DatasetStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            OutputSchemaError::PlanningError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[instrument(skip_all, err)]
pub async fn output_schema_handler(
    State(state): State<Arc<ServiceState>>,
    Json(payload): Json<OutputSchemaRequest>,
) -> Result<Json<OutputSchemaResponse>, BoxRequestError> {
    use OutputSchemaError::*;

    let dataset_store = DatasetStore::new(state.config.clone(), state.metadata_db.clone());

    let stmt = parse_sql(&payload.sql_query).map_err(SqlParseError)?;
    let ctx = dataset_store
        .planning_ctx_for_sql(&stmt)
        .await
        .map_err(DatasetStoreError)?;
    let schema = ctx.sql_output_schema(stmt).await.map_err(PlanningError)?;

    Ok(Json(OutputSchemaResponse {
        schema: schema.into(),
    }))
}
