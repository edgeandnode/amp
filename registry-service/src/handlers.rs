use std::{collections::BTreeSet, sync::Arc};

use axum::{extract::State, http::StatusCode, Json};
use common::{
    arrow::datatypes::{DataType, Field},
    manifest::TableSchema,
    query_context::{parse_sql, Error as QueryContextError},
    SPECIAL_BLOCK_NUM,
};
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
    networks: Vec<String>,
}

#[derive(Debug, Error)]
enum Error {
    #[error("SQL parse error: {0}")]
    SqlParseError(QueryContextError),
    #[error("Dataset store error: {0}")]
    DatasetStoreError(dataset_store::DatasetError),
    #[error("Planning error: {0}")]
    PlanningError(QueryContextError),
}

impl RequestError for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::SqlParseError(_) => "SQL_PARSE_ERROR",
            Error::DatasetStoreError(_) => "DATASET_STORE_ERROR",
            Error::PlanningError(_) => "PLANNING_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::SqlParseError(_) => StatusCode::BAD_REQUEST,
            Error::DatasetStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::PlanningError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[instrument(skip_all, err)]
pub async fn output_schema_handler(
    State(state): State<Arc<ServiceState>>,
    Json(payload): Json<OutputSchemaRequest>,
) -> Result<Json<OutputSchemaResponse>, BoxRequestError> {
    use Error::*;

    let stmt = parse_sql(&payload.sql_query).map_err(SqlParseError)?;
    let ctx = state
        .dataset_store
        .clone()
        .planning_ctx_for_sql(&stmt)
        .await
        .map_err(DatasetStoreError)?;
    let schema = ctx
        .sql_output_schema(
            stmt,
            &[Field::new(SPECIAL_BLOCK_NUM, DataType::UInt64, false)],
        )
        .await
        .map_err(PlanningError)?;

    let networks: BTreeSet<String> = ctx
        .catalog()
        .iter()
        .map(|t| t.table().network().to_string())
        .collect();
    Ok(Json(OutputSchemaResponse {
        schema: schema.into(),
        networks: networks.into_iter().collect(),
    }))
}
