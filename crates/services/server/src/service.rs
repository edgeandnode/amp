use std::{
    pin::Pin,
    sync::{Arc, LazyLock},
};

use arrow_flight::{
    Action, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, SchemaAsIpc, Ticket,
    encode::{FlightDataEncoderBuilder, GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES},
    error::FlightError,
    sql::{
        ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
        ActionCreatePreparedStatementResult, Any, CommandGetCatalogs, CommandGetDbSchemas,
        CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
        CommandPreparedStatementQuery, CommandStatementQuery, DoPutPreparedStatementResult,
        Nullable, ProstMessageExt, Searchable, SqlInfo, TicketStatementQuery, XdbcDataType,
        metadata::{SqlInfoDataBuilder, XdbcTypeInfo, XdbcTypeInfoData, XdbcTypeInfoDataBuilder},
        server::{FlightSqlService, PeekableFlightDataStream},
    },
};
use async_stream::stream;
use async_trait::async_trait;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response as AxumResponse},
};
use bytes::{BufMut, Bytes, BytesMut};
use common::{
    DetachedLogicalPlan, PlanningContext, QueryContext, SPECIAL_BLOCK_NUM,
    arrow::{
        self,
        array::RecordBatch,
        datatypes::SchemaRef,
        ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
    },
    catalog::physical::Catalog,
    config::Config,
    metadata::segments::{BlockRange, ResumeWatermark},
    notification_multiplexer::{self, NotificationMultiplexerHandle},
    plan_visitors::IncrementalCheck,
    query_context::{Error as CoreError, QueryEnv, parse_sql},
};
use datafusion::{
    common::{DFSchema, tree_node::TreeNodeRecursion},
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    logical_expr::LogicalPlan,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use dataset_store::{
    CatalogForSqlError, DatasetStore, GetDatasetError, GetPhysicalCatalogError,
    PlanningCtxForSqlError, manifests::DatasetManifestsStore, providers::ProviderConfigsStore,
};
use dump::streaming_query::{QueryMessage, StreamingQuery};
use futures::{
    Stream, StreamExt as _, TryStreamExt,
    stream::{self, BoxStream},
};
use http_common::{BoxRequestError, RequestError};
use metadata_db::MetadataDb;
use prost::Message as _;
use serde_json::json;
use thiserror::Error;
use tonic::{Request, Response, Status, Streaming};

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;
// TODO: Make this configurable via environment variable or config file
const NOZZLE_CATALOG_NAME: &str = "nozzle";

/// Statement handle that can be either a prepared SQL query or a serialized plan
#[derive(Clone, Debug, bincode::Encode, bincode::Decode)]
enum StatementHandle {
    PreparedStatement {
        query: String,
        schema_bytes: Vec<u8>,
    },
    SerializedPlan(Vec<u8>),
}

static NOZZLE_XDBC_DATA: LazyLock<XdbcTypeInfoData> = LazyLock::new(|| {
    let mut builder = XdbcTypeInfoDataBuilder::new();

    // BIGINT - for Int64 columns
    builder.append(XdbcTypeInfo {
        type_name: "BIGINT".into(),
        data_type: XdbcDataType::XdbcBigint,
        column_size: Some(20),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: false,
        searchable: Searchable::Full,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_increment: None,
        local_type_name: Some("BIGINT".into()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcBigint,
        datetime_subcode: None,
        num_prec_radix: Some(10),
        interval_precision: None,
    });

    // INTEGER - for Int32 columns
    builder.append(XdbcTypeInfo {
        type_name: "INTEGER".into(),
        data_type: XdbcDataType::XdbcInteger,
        column_size: Some(11),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: false,
        searchable: Searchable::Full,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_increment: None,
        local_type_name: Some("INTEGER".into()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcInteger,
        datetime_subcode: None,
        num_prec_radix: Some(10),
        interval_precision: None,
    });

    // VARCHAR - for Utf8 columns
    builder.append(XdbcTypeInfo {
        type_name: "VARCHAR".into(),
        data_type: XdbcDataType::XdbcVarchar,
        column_size: Some(i32::MAX),
        literal_prefix: Some("'".into()),
        literal_suffix: Some("'".into()),
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: true,
        searchable: Searchable::Full,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_increment: None,
        local_type_name: Some("VARCHAR".into()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcVarchar,
        datetime_subcode: None,
        num_prec_radix: None,
        interval_precision: None,
    });

    // BOOLEAN - for Boolean columns
    builder.append(XdbcTypeInfo {
        type_name: "BOOLEAN".into(),
        data_type: XdbcDataType::XdbcBit,
        column_size: Some(1),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: false,
        searchable: Searchable::Basic,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_increment: None,
        local_type_name: Some("BOOLEAN".into()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcBit,
        datetime_subcode: None,
        num_prec_radix: None,
        interval_precision: None,
    });

    // DOUBLE - for Float64 columns
    builder.append(XdbcTypeInfo {
        type_name: "DOUBLE".into(),
        data_type: XdbcDataType::XdbcDouble,
        column_size: Some(15),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: false,
        searchable: Searchable::Full,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_increment: None,
        local_type_name: Some("DOUBLE".into()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcDouble,
        datetime_subcode: None,
        num_prec_radix: Some(2),
        interval_precision: None,
    });

    // TIMESTAMP - for Timestamp columns
    builder.append(XdbcTypeInfo {
        type_name: "TIMESTAMP".into(),
        data_type: XdbcDataType::XdbcTimestamp,
        column_size: Some(23),
        literal_prefix: Some("'".into()),
        literal_suffix: Some("'".into()),
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: false,
        searchable: Searchable::Full,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_increment: None,
        local_type_name: Some("TIMESTAMP".into()),
        minimum_scale: Some(0),
        maximum_scale: Some(9),
        sql_data_type: XdbcDataType::XdbcTimestamp,
        datetime_subcode: None,
        num_prec_radix: None,
        interval_precision: None,
    });

    // VARBINARY - for binary data like hashes, addresses
    builder.append(XdbcTypeInfo {
        type_name: "VARBINARY".into(),
        data_type: XdbcDataType::XdbcVarbinary,
        column_size: Some(65535),
        literal_prefix: Some("0x".into()),
        literal_suffix: None,
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: false,
        searchable: Searchable::Basic,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_increment: None,
        local_type_name: Some("VARBINARY".into()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcVarbinary,
        datetime_subcode: None,
        num_prec_radix: None,
        interval_precision: None,
    });

    builder
        .build()
        .expect("Failed to build XDBC type info data")
});

async fn extract_parameter_values(
    request: Request<PeekableFlightDataStream>,
) -> Result<Vec<datafusion::common::ScalarValue>, Status> {
    use arrow_flight::decode::FlightRecordBatchStream;
    use datafusion::common::ScalarValue;
    use futures::StreamExt;

    let mut batch_stream = FlightRecordBatchStream::new_from_flight_data(
        request
            .into_inner()
            .map(|result| result.map_err(FlightError::from)),
    );

    let mut all_params = Vec::new();

    while let Some(batch_result) = batch_stream.next().await {
        let batch =
            batch_result.map_err(|e| Status::invalid_argument(format!("Decode error: {}", e)))?;
        if batch.num_rows() > 0 {
            let batch_params: Result<Vec<_>, Status> = batch
                .columns()
                .iter()
                .map(|col| {
                    ScalarValue::try_from_array(col, 0)
                        .map_err(|e| Status::invalid_argument(format!("Extract error: {}", e)))
                })
                .collect();
            all_params.extend(batch_params?);
        }
    }

    Ok(all_params)
}

/// SQL LIKE pattern matching for Flight SQL filters
fn sql_pattern_matches(text: &str, pattern: &str) -> bool {
    use arrow::array::StringArray;
    use arrow_string::like::like;

    let text_array = StringArray::from(vec![text]);
    let pattern_array = StringArray::from(vec![pattern]);

    match like(&text_array, &pattern_array) {
        Ok(result_array) => result_array.value(0),
        Err(_) => {
            tracing::warn!(
                pattern,
                "Failed to execute arrow-string LIKE operation, falling back to exact match"
            );
            text == pattern
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("ProtocolBuffers decoding error: {0}")]
    PbDecodeError(String),

    #[error("unsupported flight descriptor type: {0}")]
    UnsupportedFlightDescriptorType(String),

    #[error("unsupported flight descriptor command: {0}")]
    UnsupportedFlightDescriptorCommand(String),

    #[error("query execution error: {0}")]
    ExecutionError(DataFusionError),

    #[error("error looking up datasets: {0}")]
    DatasetStoreError(#[from] GetDatasetError),

    #[error("error loading catalog for SQL: {0}")]
    CatalogForSqlError(#[from] CatalogForSqlError),

    #[error("error loading physical catalog: {0}")]
    GetPhysicalCatalogError(#[from] GetPhysicalCatalogError),

    #[error("error creating planning context: {0}")]
    PlanningCtxForSqlError(#[from] PlanningCtxForSqlError),

    #[error(transparent)]
    CoreError(#[from] CoreError),

    #[error("invalid query: {0}")]
    InvalidQuery(String),

    #[error("streaming query execution error: {0}")]
    StreamingExecutionError(String),
}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        Error::PbDecodeError(e.to_string())
    }
}

fn datafusion_error_to_status(outer: &Error, e: &DataFusionError) -> Status {
    match e {
        DataFusionError::ResourcesExhausted(_) => Status::resource_exhausted(outer.to_string()),
        _ => Status::internal(outer.to_string()),
    }
}

impl RequestError for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::PbDecodeError(_) => "PB_DECODE_ERROR",
            Error::UnsupportedFlightDescriptorType(_) => "UNSUPPORTED_FLIGHT_DESCRIPTOR_TYPE",
            Error::UnsupportedFlightDescriptorCommand(_) => "UNSUPPORTED_FLIGHT_DESCRIPTOR_COMMAND",
            Error::ExecutionError(_) => "EXECUTION_ERROR",
            Error::DatasetStoreError(_) => "DATASET_STORE_ERROR",
            Error::CatalogForSqlError(_) => "CATALOG_FOR_SQL_ERROR",
            Error::GetPhysicalCatalogError(_) => "LOAD_PHYSICAL_CATALOG_ERROR",
            Error::PlanningCtxForSqlError(_) => "PLANNING_CTX_FOR_SQL_ERROR",
            Error::CoreError(CoreError::InvalidPlan(_)) => "INVALID_PLAN",
            Error::CoreError(CoreError::SqlParseError(_)) => "SQL_PARSE_ERROR",
            Error::CoreError(CoreError::PlanEncodingError(_)) => "PLAN_ENCODING_ERROR",
            Error::CoreError(CoreError::PlanDecodingError(_)) => "PLAN_DECODING_ERROR",
            Error::CoreError(CoreError::DatasetError(_)) => "DATASET_ERROR",
            Error::CoreError(CoreError::ConfigError(_)) => "CONFIG_ERROR",
            Error::CoreError(CoreError::PlanningError(_)) => "PLANNING_ERROR",
            Error::CoreError(CoreError::ExecutionError(_)) => "CORE_EXECUTION_ERROR",
            Error::CoreError(CoreError::MetaTableError(_)) => "META_TABLE_ERROR",
            Error::CoreError(CoreError::TableNotFoundError(_)) => "TABLE_NOT_FOUND_ERROR",
            Error::InvalidQuery(_) => "INVALID_QUERY",
            Error::StreamingExecutionError(_) => "STREAMING_EXECUTION_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::CoreError(
                CoreError::InvalidPlan(_)
                | CoreError::SqlParseError(_)
                | CoreError::PlanEncodingError(_)
                | CoreError::PlanDecodingError(_),
            ) => StatusCode::BAD_REQUEST,
            Error::CoreError(
                CoreError::DatasetError(_)
                | CoreError::ConfigError(_)
                | CoreError::PlanningError(_)
                | CoreError::ExecutionError(_)
                | CoreError::MetaTableError(_),
            ) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::CoreError(CoreError::TableNotFoundError(_)) => StatusCode::NOT_FOUND,
            Error::ExecutionError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::StreamingExecutionError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DatasetStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::CatalogForSqlError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::GetPhysicalCatalogError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::PlanningCtxForSqlError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::PbDecodeError(_) => StatusCode::BAD_REQUEST,
            Error::UnsupportedFlightDescriptorType(_) => StatusCode::BAD_REQUEST,
            Error::UnsupportedFlightDescriptorCommand(_) => StatusCode::BAD_REQUEST,
            Error::InvalidQuery(_) => StatusCode::BAD_REQUEST,
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> AxumResponse {
        BoxRequestError::from(self).into_response()
    }
}

impl From<Error> for Status {
    fn from(e: Error) -> Self {
        match &e {
            Error::PbDecodeError(_) => Status::invalid_argument(e.to_string()),
            Error::UnsupportedFlightDescriptorType(_) => Status::invalid_argument(e.to_string()),
            Error::UnsupportedFlightDescriptorCommand(_) => Status::invalid_argument(e.to_string()),
            Error::DatasetStoreError(_) => Status::internal(e.to_string()),

            Error::CoreError(CoreError::InvalidPlan(_)) => Status::invalid_argument(e.to_string()),
            Error::CoreError(CoreError::SqlParseError(_)) => {
                Status::invalid_argument(e.to_string())
            }
            Error::CoreError(CoreError::PlanEncodingError(_) | CoreError::PlanDecodingError(_)) => {
                Status::invalid_argument(e.to_string())
            }
            Error::CoreError(CoreError::DatasetError(_)) => Status::internal(e.to_string()),
            Error::CoreError(CoreError::ConfigError(_)) => Status::internal(e.to_string()),
            Error::CoreError(CoreError::TableNotFoundError(_)) => Status::not_found(e.to_string()),

            Error::CoreError(
                CoreError::PlanningError(df)
                | CoreError::ExecutionError(df)
                | CoreError::MetaTableError(df),
            ) => datafusion_error_to_status(&e, df),

            Error::ExecutionError(df) => datafusion_error_to_status(&e, df),
            Error::StreamingExecutionError(_) => Status::internal(e.to_string()),
            Error::CatalogForSqlError(_) => Status::internal(e.to_string()),
            Error::GetPhysicalCatalogError(_) => Status::internal(e.to_string()),
            Error::PlanningCtxForSqlError(_) => Status::internal(e.to_string()),
            Error::InvalidQuery(_) => Status::invalid_argument(e.to_string()),
        }
    }
}

pub enum QueryResultStream {
    NonIncremental(SendableRecordBatchStream),
    Incremental(BoxStream<'static, Result<QueryMessage, Error>>),
}

impl QueryResultStream {
    pub fn record_batches(self) -> BoxStream<'static, Result<RecordBatch, Error>> {
        match self {
            Self::NonIncremental(stream) => stream
                .map_err(|err| Error::StreamingExecutionError(err.to_string()))
                .boxed(),
            Self::Incremental(stream) => stream
                .filter_map(async |result| match result {
                    Err(err) => Some(Err(err)),
                    Ok(QueryMessage::Data(batch)) => Some(Ok(batch)),
                    Ok(_) => None,
                })
                .boxed(),
        }
    }
}

#[derive(Clone)]
pub struct Service {
    config: Arc<Config>,
    env: QueryEnv,
    dataset_store: Arc<DatasetStore>,
    notification_multiplexer: Arc<NotificationMultiplexerHandle>,
    metrics: Option<Arc<crate::metrics::MetricsRegistry>>,
}

impl Service {
    pub async fn new(
        config: Arc<Config>,
        metadata_db: MetadataDb,
        meter: Option<&monitoring::telemetry::metrics::Meter>,
    ) -> Result<Self, Error> {
        let env = config.make_query_env().map_err(Error::ExecutionError)?;
        let dataset_store = {
            let provider_configs_store =
                ProviderConfigsStore::new(config.providers_store.prefixed_store());
            let dataset_manifests_store = DatasetManifestsStore::new(
                metadata_db.clone(),
                config.dataset_defs_store.prefixed_store(),
            );
            DatasetStore::new(
                metadata_db.clone(),
                provider_configs_store,
                dataset_manifests_store,
            )
        };
        let notification_multiplexer =
            Arc::new(notification_multiplexer::spawn(metadata_db.clone()));
        let metrics = meter.map(|m| Arc::new(crate::metrics::MetricsRegistry::new(m)));

        Ok(Self {
            config,
            env,
            dataset_store,
            notification_multiplexer,
            metrics,
        })
    }

    pub async fn execute_query(&self, sql: &str) -> Result<QueryResultStream, Error> {
        let query = parse_sql(sql).map_err(Error::from)?;
        let dataset_store = self.dataset_store.clone();
        let catalog = dataset_store
            .catalog_for_sql(&query, self.env.clone())
            .await?;

        let ctx = PlanningContext::new(catalog.logical().clone());
        let plan = ctx.plan_sql(query.clone()).await.map_err(Error::from)?;

        let is_streaming = common::stream_helpers::is_streaming(&query);
        let result = self
            .execute_plan(catalog, dataset_store, plan, is_streaming, None)
            .await;

        // Record execution error
        if result.is_err()
            && let Some(metrics) = &self.metrics
        {
            let error_code = result
                .as_ref()
                .err()
                .map(|e| e.error_code())
                .unwrap_or("UNKNOWN_ERROR");
            metrics.record_query_error(error_code);
        }

        result
    }

    #[tracing::instrument(skip_all, err)]
    pub async fn execute_plan(
        &self,
        catalog: Catalog,
        dataset_store: Arc<DatasetStore>,
        plan: DetachedLogicalPlan,
        is_streaming: bool,
        resume_watermark: Option<ResumeWatermark>,
    ) -> Result<QueryResultStream, Error> {
        let query_start_time = std::time::Instant::now();

        // is_incremental returns an error if query contains DDL, DML, etc.
        let incremental_check = plan
            .is_incremental()
            .map_err(|e| Error::InvalidQuery(e.to_string()))?;
        if is_streaming && let IncrementalCheck::NonIncremental(op) = incremental_check {
            return Err(Error::InvalidQuery(format!(
                "non-incremental queries are not supported for streaming, query contains non-incremental operation: `{}`",
                op
            )));
        }

        // If not streaming or metadata db is not available, execute once
        if !is_streaming {
            let original_schema = plan.schema().clone();
            let should_transform = should_transform_plan(&plan).map_err(Error::ExecutionError)?;
            let plan = if should_transform {
                let mut plan = plan.propagate_block_num().map_err(Error::ExecutionError)?;
                // If the user did not request `_block_num` column, we omit it from the final output.
                if !original_schema
                    .fields()
                    .iter()
                    .any(|f| f.name() == SPECIAL_BLOCK_NUM)
                {
                    plan = plan
                        .unproject_special_block_num_column()
                        .map_err(Error::ExecutionError)?;
                }
                plan
            } else {
                plan
            };
            let ctx = QueryContext::for_catalog(catalog, self.env.clone(), false).await?;
            let plan = plan.attach_to(&ctx)?;
            let record_baches = ctx.execute_plan(plan, true).await?;
            let stream = QueryResultStream::NonIncremental(record_baches);

            if let Some(metrics) = &self.metrics {
                Ok(track_query_metrics(stream, metrics, query_start_time))
            } else {
                Ok(stream)
            }
        } else {
            // As an optimization, start the stream from the minimum start block across all tables.
            // Otherwise starting from `0` would spend time scanning ranges known to be empty.
            let earliest_block = catalog
                .earliest_block()
                .await
                .map_err(|err| Error::StreamingExecutionError(err.to_string()))?;

            // If no tables are synced, we return an empty stream.
            let Some(earliest_block) = earliest_block else {
                let schema = plan.schema().as_ref().clone().into();
                let empty_stream = RecordBatchStreamAdapter::new(schema, stream::empty());
                return Ok(QueryResultStream::NonIncremental(Box::pin(empty_stream)));
            };

            let query = StreamingQuery::spawn(
                self.env.clone(),
                catalog,
                dataset_store,
                plan,
                earliest_block,
                None,
                resume_watermark,
                &self.notification_multiplexer,
                None,
                self.config.microbatch_max_interval,
            )
            .await
            .map_err(|e| Error::StreamingExecutionError(e.to_string()))?;

            let stream = QueryResultStream::Incremental(
                query
                    .as_stream()
                    .map_err(|err| Error::StreamingExecutionError(err.to_string()))
                    .boxed(),
            );

            if let Some(metrics) = &self.metrics {
                Ok(track_query_metrics(stream, metrics, query_start_time))
            } else {
                Ok(stream)
            }
        }
    }
}

fn should_transform_plan(plan: &DetachedLogicalPlan) -> Result<bool, DataFusionError> {
    let mut result = true;
    plan.apply(|node| {
        match node {
            // Trying to propagate the `SPECIAL_BLOCK_NUM` will probably cause problems for these.
            LogicalPlan::EmptyRelation(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Join(_) => {
                result = false;
                Ok(TreeNodeRecursion::Stop)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        }
    })?;
    Ok(result)
}

impl Service {
    async fn plan_sql_query(
        &self,
        sql: &str,
        resume_watermark: Option<ResumeWatermark>,
    ) -> Result<(Bytes, Arc<DFSchema>), Error> {
        self.plan_sql_query_with_params(sql, Vec::new(), resume_watermark)
            .await
    }

    // The magic that turns a SQL string into a DataFusion logical plan that can be
    // sent back over the wire:
    // - Parse the SQL query.
    // - Infer depedencies and collect them into a catalog.
    // - Build a DataFusion query context with empty tables from that catalog.
    // - Use that context to plan the SQL query.
    // - Serialize the plan to bytes using datafusion-protobufs.
    async fn plan_sql_query_with_params(
        &self,
        sql: &str,
        param_values: Vec<datafusion::common::ScalarValue>,
        resume_watermark: Option<ResumeWatermark>,
    ) -> Result<(Bytes, Arc<DFSchema>), Error> {
        let parsed_query = parse_sql(sql)?;
        let query_ctx = self
            .dataset_store
            .clone()
            .planning_ctx_for_sql(&parsed_query)
            .await?;
        query_ctx
            .sql_to_remote_plan(parsed_query, param_values, resume_watermark)
            .await
            .map_err(Error::from)
    }

    async fn execute_remote_plan(
        &self,
        serialized_plan: &Bytes,
    ) -> Result<TonicStream<FlightData>, Error> {
        let remote_plan = common::remote_plan_from_bytes(serialized_plan)?;
        let is_streaming = remote_plan.is_streaming;
        let resume_watermark = remote_plan.resume_watermark.map(Into::into);
        let table_refs = remote_plan.table_refs.into_iter().map(|t| t.into());

        let catalog = self
            .dataset_store
            .get_physical_catalog(table_refs, remote_plan.function_refs, &self.env)
            .await?;
        let query_ctx = PlanningContext::new(catalog.logical().clone());
        let plan = query_ctx
            .plan_from_bytes(&remote_plan.serialized_plan)
            .await?;
        let schema: SchemaRef = plan.schema().as_ref().clone().into();

        let dataset_store = self.dataset_store.clone();
        let stream = self
            .execute_plan(catalog, dataset_store, plan, is_streaming, resume_watermark)
            .await?;

        Ok(flight_data_stream(stream, schema))
    }
}

fn ipc_schema(schema: &DFSchema) -> Bytes {
    use arrow::ipc::writer::DictionaryTracker;

    let ipc_opts = &Default::default();
    let mut dictionary_tracker = DictionaryTracker::new(true);
    let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        &schema.into(),
        &mut dictionary_tracker,
        ipc_opts,
    );

    // Unwrap: writing to `BytesMut` never fails.
    let mut bytes = BytesMut::new().writer();
    arrow::ipc::writer::write_message(&mut bytes, encoded, ipc_opts).unwrap();
    bytes.into_inner().into()
}

/// Wrap a query result stream with metrics tracking
fn track_query_metrics(
    stream: QueryResultStream,
    metrics: &Arc<crate::metrics::MetricsRegistry>,
    start_time: std::time::Instant,
) -> QueryResultStream {
    let metrics = metrics.clone();

    match stream {
        QueryResultStream::NonIncremental(record_batch_stream) => {
            let schema = record_batch_stream.schema();
            let wrapped_stream = stream! {
                let mut total_rows = 0u64;
                let mut total_bytes = 0u64;

                for await result in record_batch_stream {
                    match result {
                        Ok(batch) => {
                            let batch_rows = batch.num_rows() as u64;
                            let batch_bytes = batch.get_array_memory_size() as u64;

                            // Track cumulative totals
                            total_rows += batch_rows;
                            total_bytes += batch_bytes;

                            yield Ok(batch);
                        }
                        Err(e) => {
                            // Record metrics on error
                            let duration = start_time.elapsed().as_millis() as f64;
                            let err_msg = e.to_string();
                            metrics.record_query_error(&err_msg);
                            metrics.record_query_execution(duration, total_rows, total_bytes);

                            yield Err(e);
                            return;
                        }
                    }
                }

                // Stream completed successfully, record metrics
                let duration = start_time.elapsed().as_millis() as f64;
                metrics.record_query_execution(duration, total_rows, total_bytes);
            };

            let wrapped: SendableRecordBatchStream =
                Box::pin(RecordBatchStreamAdapter::new(schema, wrapped_stream));

            QueryResultStream::NonIncremental(wrapped)
        }
        QueryResultStream::Incremental(message_stream) => {
            // Increment active streaming query counter
            metrics.streaming_queries_active.inc();
            metrics.streaming_queries_started.inc();

            let wrapped = stream! {
                let mut microbatch_start: Option<std::time::Instant> = None;

                for await result in message_stream {
                    match result {
                        Ok(message) => {
                            match message {
                                QueryMessage::MicrobatchStart { .. } => {
                                    microbatch_start = Some(std::time::Instant::now());
                                }
                                QueryMessage::Data(ref batch) => {
                                    let batch_rows = batch.num_rows() as u64;
                                    let batch_bytes = batch.get_array_memory_size() as u64;
                                    // Record incremental throughput per batch (counters track cumulative totals)
                                    metrics.record_streaming_batch(batch_rows, batch_bytes);
                                }
                                QueryMessage::MicrobatchEnd(_) => {
                                    if let Some(start) = microbatch_start.take() {
                                        let duration = start.elapsed().as_millis() as f64;
                                        metrics.record_streaming_microbatch_duration(duration);
                                    }
                                }
                                QueryMessage::BlockComplete(_) => {}
                            }

                            yield Ok(message);
                        }
                        Err(e) => {
                            // Record metrics on error
                            let duration = start_time.elapsed().as_millis() as f64;
                            metrics.streaming_queries_completed.inc();
                            metrics.streaming_queries_active.dec();
                            metrics.record_streaming_lifetime(duration);

                            yield Err(e);
                            return;
                        }
                    }
                }

                // Record metrics on success
                let duration = start_time.elapsed().as_millis() as f64;
                metrics.streaming_queries_completed.inc();
                metrics.streaming_queries_active.dec();
                metrics.record_streaming_lifetime(duration);
            }
            .boxed();

            QueryResultStream::Incremental(wrapped)
        }
    }
}

fn flight_data_stream(
    query_result_stream: QueryResultStream,
    schema: SchemaRef,
) -> TonicStream<FlightData> {
    let mut incremental_stream = match query_result_stream {
        QueryResultStream::Incremental(incremental_stream) => incremental_stream,
        QueryResultStream::NonIncremental(non_incremental_stream) => {
            return FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(
                    non_incremental_stream
                        .map_err(Error::ExecutionError)
                        .map_err(Status::from)
                        .err_into(),
                )
                .map_err(Status::from)
                .boxed();
        }
    };
    stream! {
        let mut dictionary_tracker = DictionaryTracker::new(true);
        let mut ranges: Vec<BlockRange> = Default::default();
        let mut first_message = true;
        while let Some(result) = incremental_stream.next().await {
            match result {
                Err(err) => {
                    yield Err(Error::StreamingExecutionError(err.to_string()).into());
                    break;
                }
                Ok(message) => match message {
                    QueryMessage::MicrobatchStart { range, is_reorg: _ } => {
                        assert!(ranges.is_empty());
                        ranges.push(range);

                        if first_message {
                            first_message = false;
                            let schema_message =
                                FlightData::from(SchemaAsIpc::new(&schema, &IpcWriteOptions::default()));
                            yield Ok(schema_message);
                        }
                    }
                    QueryMessage::Data(batch) => {
                        let app_metadata = json!({
                            "ranges": &ranges,
                            "ranges_complete": false,
                        });
                        match encode_record_batch(batch, &app_metadata, &mut dictionary_tracker) {
                            Ok(encoded) => {
                                for message in encoded {
                                    yield Ok(message);
                                }
                            }
                            Err(err) => {
                                yield Err(err);
                                return;
                            }
                        };
                    }
                    QueryMessage::BlockComplete(_) => (),
                    QueryMessage::MicrobatchEnd(_) => {
                        assert!(ranges.len() > 0);
                        let empty_batch = RecordBatch::new_empty(schema.clone());
                        let app_metadata = json!({
                            "ranges": &ranges,
                            "ranges_complete": true,
                        });
                        match encode_record_batch(empty_batch, &app_metadata, &mut dictionary_tracker) {
                            Ok(encoded) => {
                                for message in encoded {
                                    yield Ok(message);
                                }
                            }
                            Err(err) => {
                                yield Err(err);
                                return;
                            }
                        };
                        ranges.clear();
                    }
                }
            }
        }
    }
    .boxed()
}

pub fn encode_record_batch(
    batch: RecordBatch,
    app_metadata: &serde_json::Value,
    dictionary_tracker: &mut DictionaryTracker,
) -> Result<Vec<FlightData>, Status> {
    let ipc = IpcDataGenerator::default();
    let options = IpcWriteOptions::default();
    let mut encoded: Vec<FlightData> = Default::default();
    let app_metadata = serde_json::to_string(app_metadata).unwrap();
    for batch in split_batch_for_grpc_response(batch, GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES) {
        let (encoded_dictionaries, encoded_batch) = ipc
            .encoded_batch(&batch, dictionary_tracker, &options)
            .map_err(FlightError::from)?;
        for encoded_dictionary in encoded_dictionaries {
            encoded.push(encoded_dictionary.into());
        }
        encoded.push(FlightData::from(encoded_batch).with_app_metadata(app_metadata.clone()));
    }
    Ok(encoded)
}

/// Split [`RecordBatch`] so it hopefully fits into a gRPC response.
///
/// Data is zero-copy sliced into batches.
///
/// Note: this method does not take into account already sliced
/// arrays: <https://github.com/apache/arrow-rs/issues/3407>
///
/// Implementation adapted from https://github.com/apache/arrow-rs/blob/4b62c8004f5c617fc6b552c7fce73fc93c8fab04/arrow-flight/src/encode.rs#L614-L638.
fn split_batch_for_grpc_response(
    batch: RecordBatch,
    max_flight_data_size: usize,
) -> Vec<RecordBatch> {
    // Original imlementation would return an empty vec for an empty batch. We need to send empty
    // batches to send metadata to clients on microbatch end.
    if batch.num_rows() == 0 {
        return vec![batch];
    }

    let size = batch
        .columns()
        .iter()
        .map(|col| col.get_buffer_memory_size())
        .sum::<usize>();

    let n_batches =
        (size / max_flight_data_size + usize::from(size % max_flight_data_size != 0)).max(1);
    let rows_per_batch = (batch.num_rows() / n_batches).max(1);
    let mut out = Vec::with_capacity(n_batches + 1);

    let mut offset = 0;
    while offset < batch.num_rows() {
        let length = (rows_per_batch).min(batch.num_rows() - offset);
        out.push(batch.slice(offset, length));

        offset += length;
    }

    out
}

#[async_trait]
impl FlightSqlService for Service {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        tracing::info!(method = "do_handshake", "FlightSQL method called");
        // TODO: Implement proper authentication
        Ok(Response::new(Box::pin(stream::empty())))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::info!(method = "get_flight_info_statement", sql = %query.query, "FlightSQL method called");

        let resume_watermark = request
            .metadata()
            .get("nozzle-resume")
            .and_then(|v| serde_json::from_slice(v.as_bytes()).ok());

        let (serialized_plan, schema) =
            self.plan_sql_query(&query.query, resume_watermark)
                .await
                .map_err(|e| Status::invalid_argument(format!("SQL planning error: {}", e)))?;

        let handle = StatementHandle::SerializedPlan(serialized_plan.to_vec());
        let handle_bytes = Bytes::from(
            bincode::encode_to_vec(&handle, bincode::config::standard()).map_err(|e| {
                Status::internal(format!("Failed to encode statement handle: {}", e))
            })?,
        );
        let ticket_query = TicketStatementQuery {
            statement_handle: handle_bytes,
        };
        let any_ticket = Any::pack(&ticket_query)
            .map_err(|e| Status::internal(format!("Failed to pack ticket: {}", e)))?;
        let ticket = Ticket::new(any_ticket.encode_to_vec());

        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
            expiration_time: None,
            app_metadata: Bytes::new(),
        };

        let info = FlightInfo {
            flight_descriptor: Some(request.into_inner()),
            schema: ipc_schema(&schema),
            endpoint: vec![endpoint],
            ordered: false,
            total_records: -1,
            total_bytes: -1,
            app_metadata: Bytes::new(),
        };

        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>, Status>
    {
        let (handle, _): (StatementHandle, usize) =
            bincode::decode_from_slice(&ticket.statement_handle, bincode::config::standard())
                .map_err(|e| {
                    Status::invalid_argument(format!("Invalid statement handle: {}", e))
                })?;

        let stream = match handle {
            StatementHandle::PreparedStatement { query, .. } => {
                let (serialized_plan, _schema) = self
                    .plan_sql_query(&query, None)
                    .await
                    .map_err(|e| Status::internal(format!("SQL planning error: {}", e)))?;

                self.execute_remote_plan(&serialized_plan)
                    .await
                    .map_err(|e| Status::internal(format!("SQL execution error: {}", e)))?
            }
            StatementHandle::SerializedPlan(plan) => self
                .execute_remote_plan(&Bytes::from(plan))
                .await
                .map_err(|e| Status::internal(format!("Plan execution error: {}", e)))?,
        };

        Ok(Response::new(stream))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::info!(
            method = "get_flight_info_catalogs",
            "FlightSQL method called"
        );
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| Status::internal(format!("Unable to encode schema: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(flight_info))
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>, Status>
    {
        tracing::info!(method = "do_get_catalogs", "FlightSQL method called");
        let mut builder = query.into_builder();
        let catalog_name = NOZZLE_CATALOG_NAME;
        builder.append(catalog_name);
        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("Failed to build catalog batch: {}", e)))?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { Ok(batch) }))
            .map_err(|e| Status::internal(format!("Failed to encode flight data: {}", e)));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::info!(
            method = "get_flight_info_schemas",
            catalog = query.catalog.as_deref().unwrap_or("<none>"),
            db_schema_filter = query
                .db_schema_filter_pattern
                .as_deref()
                .unwrap_or("<none>"),
            "FlightSQL method called"
        );
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| Status::internal(format!("Unable to encode schema: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(flight_info))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>, Status>
    {
        tracing::info!(method = "do_get_schemas", "FlightSQL method called");
        let catalog_filter = query.catalog.clone();
        let schema_filter = query.db_schema_filter_pattern.clone();

        let datasets = self
            .dataset_store
            .get_all_datasets()
            .await
            .map_err(|e| Status::internal(format!("Failed to list datasets: {}", e)))?;

        let mut builder = query.into_builder();
        let catalog_name = NOZZLE_CATALOG_NAME;

        let catalog_matches = if let Some(ref catalog_filter_val) = catalog_filter {
            catalog_name == catalog_filter_val
        } else {
            true
        };

        if catalog_matches {
            for dataset in datasets {
                let schema_name = &dataset.name;

                let schema_matches = if let Some(ref schema_pattern) = schema_filter {
                    sql_pattern_matches(schema_name, schema_pattern)
                } else {
                    true
                };

                if schema_matches {
                    builder.append(catalog_name, schema_name);
                }
            }
        }

        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("Failed to build schema batch: {}", e)))?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { Ok(batch) }))
            .map_err(|e| Status::internal(format!("Failed to encode flight data: {}", e)));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::info!(
            method = "get_flight_info_tables",
            catalog = query.catalog.as_deref().unwrap_or("<none>"),
            db_schema = query
                .db_schema_filter_pattern
                .as_deref()
                .unwrap_or("<none>"),
            table = query
                .table_name_filter_pattern
                .as_deref()
                .unwrap_or("<none>"),
            "FlightSQL method called"
        );
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| Status::internal(format!("Unable to encode schema: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(flight_info))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>, Status>
    {
        tracing::info!(method = "do_get_tables", "FlightSQL method called");
        let catalog_filter = query.catalog.clone();
        let schema_filter = query.db_schema_filter_pattern.clone();
        let table_filter = query.table_name_filter_pattern.clone();

        let datasets = self
            .dataset_store
            .get_all_datasets()
            .await
            .map_err(|e| Status::internal(format!("Failed to list datasets: {}", e)))?;

        let mut builder = query.into_builder();

        let catalog_matches = catalog_filter
            .as_ref()
            .map_or(true, |filter| filter == NOZZLE_CATALOG_NAME);

        if catalog_matches {
            for dataset in datasets {
                let dataset_arc = Arc::new(dataset);
                for resolved_table in dataset_arc.resolved_tables() {
                    let schema_name = resolved_table.catalog_schema();
                    let table_name = resolved_table.name();
                    let table_schema = resolved_table.schema();

                    let schema_matches = schema_filter
                        .as_ref()
                        .map_or(true, |pattern| sql_pattern_matches(schema_name, pattern));

                    let table_matches = table_filter
                        .as_ref()
                        .map_or(true, |pattern| sql_pattern_matches(table_name, pattern));

                    if schema_matches && table_matches {
                        let schema_ref: &arrow::datatypes::Schema = table_schema.as_ref();

                        if let Err(e) = builder.append(
                            NOZZLE_CATALOG_NAME,
                            schema_name,
                            table_name,
                            "TABLE",
                            schema_ref,
                        ) {
                            tracing::warn!(
                                "Failed to append table {}.{}: {}",
                                schema_name,
                                table_name,
                                e
                            );
                            continue;
                        }
                    }
                }
            }
        }

        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("Failed to build table batch: {}", e)))?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { Ok(batch) }))
            .map_err(|e| Status::internal(format!("Failed to encode flight data: {}", e)));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::info!(
            method = "get_flight_info_table_types",
            "FlightSQL method called"
        );
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| Status::internal(format!("Unable to encode schema: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(flight_info))
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>, Status>
    {
        tracing::info!(method = "do_get_table_types", "FlightSQL method called");
        let mut builder = query.into_builder();
        builder.append("TABLE");
        let schema = builder.schema();
        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("Failed to build table types batch: {}", e)))?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { Ok(batch) }))
            .map_err(|e| Status::internal(format!("Failed to encode flight data: {}", e)));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::info!(
            method = "get_flight_info_sql_info",
            sql_info_count = query.info.len(),
            "FlightSQL method called"
        );
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        let mut sql_info_builder = SqlInfoDataBuilder::new();
        sql_info_builder.append(SqlInfo::FlightSqlServerName, "Nozzle Flight SQL Server");
        sql_info_builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        let sql_info_data = sql_info_builder
            .build()
            .map_err(|e| Status::internal(format!("Failed to create SQL info data: {}", e)))?;

        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&sql_info_data).schema().as_ref())
            .map_err(|e| Status::internal(format!("Unable to encode schema: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);
        Ok(Response::new(flight_info))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>, Status>
    {
        tracing::info!(method = "do_get_sql_info", "FlightSQL method called");
        let mut sql_info_builder = SqlInfoDataBuilder::new();
        sql_info_builder.append(SqlInfo::FlightSqlServerName, "Nozzle Flight SQL Server");
        sql_info_builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
        let sql_info_data = sql_info_builder
            .build()
            .map_err(|e| Status::internal(format!("Failed to create SQL info data: {}", e)))?;

        let builder = query.into_builder(&sql_info_data);
        let schema = builder.schema();
        let batch = builder.build();

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(|e| Status::internal(format!("Failed to encode flight data: {}", e)));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::info!(
            method = "get_flight_info_xdbc_type_info",
            "FlightSQL method called"
        );
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        // Use our static XDBC data to get the schema
        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&*NOZZLE_XDBC_DATA).schema().as_ref())
            .map_err(|e| Status::internal(format!("Unable to encode XDBC schema: {}", e)))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn do_get_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>, Status>
    {
        tracing::info!(method = "do_get_xdbc_type_info", "FlightSQL method called");
        // Build the XDBC type info batch using our static data
        let builder = query.into_builder(&*NOZZLE_XDBC_DATA);
        let schema = builder.schema();
        let batch = builder.build().map_err(|e| {
            Status::internal(format!("Failed to build XDBC type info batch: {}", e))
        })?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { Ok(batch) }))
            .map_err(|e| Status::internal(format!("Failed to encode XDBC flight data: {}", e)));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        tracing::info!(
            method = "do_action_create_prepared_statement",
            query = %query.query,
            "FlightSQL method called"
        );

        let parsed_query = parse_sql(&query.query)
            .map_err(|e| Status::invalid_argument(format!("SQL parsing error: {}", e)))?;

        let query_ctx = self
            .dataset_store
            .clone()
            .planning_ctx_for_sql(&parsed_query)
            .await
            .map_err(|e| Status::internal(format!("Failed to create planning context: {}", e)))?;

        let dataset_schema = query_ctx
            .sql_output_schema(parsed_query)
            .await
            .map_err(|e| Status::internal(format!("Failed to get dataset schema: {}", e)))?;

        // Return empty parameter schema
        // TODO: This is a hack, we need to infer the parameter schema with datafusion
        // but datafusion cannot infer all parameters at this point, leaving this empty is FlightSQL compliant
        let empty_schema = Arc::new(arrow::datatypes::Schema::empty());
        let parameter_schema = Arc::new(
            DFSchema::try_from(empty_schema.as_ref().clone()).map_err(|e| {
                Status::internal(format!("Failed to create empty parameter schema: {}", e))
            })?,
        );

        let dataset_schema_bytes = ipc_schema(&dataset_schema);
        let parameter_schema_bytes = ipc_schema(&parameter_schema);

        let handle = StatementHandle::PreparedStatement {
            query: query.query.clone(),
            schema_bytes: dataset_schema_bytes.to_vec(),
        };
        let handle_bytes = Bytes::from(
            bincode::encode_to_vec(&handle, bincode::config::standard()).map_err(|e| {
                Status::internal(format!("Failed to encode prepared statement handle: {}", e))
            })?,
        );

        tracing::debug!(
            "Created prepared statement with {} parameter fields",
            parameter_schema.fields().len()
        );

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle_bytes,
            dataset_schema: dataset_schema_bytes,
            parameter_schema: parameter_schema_bytes,
        })
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        tracing::info!(
            method = "do_put_prepared_statement_query",
            "FlightSQL method called"
        );

        let (handle, _): (StatementHandle, usize) = bincode::decode_from_slice(
            &query.prepared_statement_handle,
            bincode::config::standard(),
        )
        .map_err(|e| Status::invalid_argument(format!("Invalid statement handle: {}", e)))?;

        let sql_query = match handle {
            StatementHandle::PreparedStatement { query, .. } => query,
            StatementHandle::SerializedPlan(_) => {
                return Err(Status::internal(
                    "Expected PreparedStatement handle, got SerializedPlan",
                ));
            }
        };

        tracing::debug!(query = %sql_query, "Executing prepared statement with parameters");

        // Extract parameter values directly from FlightData stream and apply them using DataFusion's parameter binding
        let param_values = extract_parameter_values(request).await?;

        tracing::debug!(
            "Extracted {} parameters from FlightData stream",
            param_values.len()
        );
        let (serialized_plan, _schema) = self
            .plan_sql_query_with_params(&sql_query, param_values, None)
            .await
            .map_err(|e| Status::internal(format!("SQL planning error: {}", e)))?;

        let handle = StatementHandle::SerializedPlan(serialized_plan.to_vec());
        let handle_bytes = Bytes::from(
            bincode::encode_to_vec(&handle, bincode::config::standard()).map_err(|e| {
                Status::internal(format!("Failed to encode prepared statement handle: {}", e))
            })?,
        );

        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: Some(handle_bytes),
        })
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::info!(
            method = "get_flight_info_prepared_statement",
            "FlightSQL method called"
        );
        let (handle, _): (StatementHandle, usize) =
            bincode::decode_from_slice(&cmd.prepared_statement_handle, bincode::config::standard())
                .map_err(|e| {
                    Status::invalid_argument(format!("Invalid prepared statement handle: {}", e))
                })?;

        let flight_descriptor = request.into_inner();

        let handle_bytes = Bytes::from(
            bincode::encode_to_vec(&handle, bincode::config::standard()).map_err(|e| {
                Status::internal(format!(
                    "Failed to encode statement handle for ticket: {}",
                    e
                ))
            })?,
        );
        let ticket_query = TicketStatementQuery {
            statement_handle: handle_bytes,
        };
        let any_ticket = Any::pack(&ticket_query).map_err(|e| {
            Status::internal(format!("Failed to pack prepared statement ticket: {}", e))
        })?;
        let ticket = Ticket::new(any_ticket.encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let schema_bytes = match &handle {
            StatementHandle::PreparedStatement { schema_bytes, .. } => {
                Bytes::from(schema_bytes.clone())
            }
            _ => {
                return Err(Status::invalid_argument(
                    "Expected PreparedStatement handle for prepared statement query",
                ));
            }
        };

        let flight_info = FlightInfo {
            flight_descriptor: Some(flight_descriptor),
            schema: schema_bytes,
            endpoint: vec![endpoint],
            ordered: false,
            total_records: -1,
            total_bytes: -1,
            app_metadata: Bytes::new(),
        };

        Ok(Response::new(flight_info))
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>, Status>
    {
        tracing::info!(
            method = "do_get_prepared_statement",
            "FlightSQL method called"
        );
        let (handle, _): (StatementHandle, usize) = bincode::decode_from_slice(
            &query.prepared_statement_handle,
            bincode::config::standard(),
        )
        .map_err(|e| {
            Status::invalid_argument(format!("Invalid prepared statement handle: {}", e))
        })?;

        let stream = match handle {
            StatementHandle::PreparedStatement { query, .. } => {
                if query.is_empty() {
                    return Err(Status::internal("Prepared statement query is empty"));
                }

                let (serialized_plan, _schema) =
                    self.plan_sql_query(&query, None).await.map_err(|e| {
                        Status::internal(format!("Prepared statement planning error: {}", e))
                    })?;

                self.execute_remote_plan(&serialized_plan)
                    .await
                    .map_err(|e| {
                        Status::internal(format!("Prepared statement execution error: {}", e))
                    })?
            }
            StatementHandle::SerializedPlan(_) => {
                return Err(Status::internal(
                    "Expected PreparedStatement for prepared statement, got SerializedPlan",
                ));
            }
        };

        Ok(Response::new(stream))
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        tracing::info!(
            method = "do_action_close_prepared_statement",
            "FlightSQL method called"
        );
        Ok(())
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        // No-op for now
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_pattern_matching() {
        assert!(sql_pattern_matches("eth_rpc", "eth\\_rpc"));

        // Test basic exact matching
        assert!(sql_pattern_matches("test", "test"));
        assert!(!sql_pattern_matches("test", "other"));

        // Test wildcard patterns
        assert!(sql_pattern_matches("test", "%"));
        assert!(sql_pattern_matches("test", "te%"));
        assert!(sql_pattern_matches("test", "%st"));
        assert!(sql_pattern_matches("test", "t%t"));

        // Test single character wildcard
        assert!(sql_pattern_matches("test", "te_t"));
        assert!(sql_pattern_matches("test", "_est"));
        assert!(!sql_pattern_matches("test", "te_"));

        // Test escaped percent
        assert!(sql_pattern_matches("te%st", "te\\%st"));
        assert!(!sql_pattern_matches("test", "te\\%st"));

        // Test complex patterns
        assert!(sql_pattern_matches("eth_rpc_test", "eth\\_rpc_%"));
        assert!(sql_pattern_matches("transactions", "trans%"));
        assert!(sql_pattern_matches("logs", "log_"));
    }
}
