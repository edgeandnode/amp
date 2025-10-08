use std::{pin::Pin, sync::Arc};

use arrow_flight::{
    ActionType, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaAsIpc, Ticket,
    encode::{FlightDataEncoderBuilder, GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES},
    error::FlightError,
    flight_descriptor::DescriptorType,
    flight_service_server::FlightService,
    sql::{Any, CommandStatementQuery},
};
use async_stream::stream;
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
use tonic::{Request, Response, Status};
use tracing::instrument;

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

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

#[async_trait::async_trait]
impl FlightService for Service {
    type HandshakeStream = TonicStream<HandshakeResponse>;
    type ListFlightsStream = TonicStream<FlightInfo>;
    type DoGetStream = TonicStream<FlightData>;
    type DoPutStream = TonicStream<PutResult>;
    type DoActionStream = TonicStream<arrow_flight::Result>;
    type ListActionsStream = TonicStream<ActionType>;
    type DoExchangeStream = TonicStream<FlightData>;

    async fn poll_flight_info(
        &self,
        _request: Request<arrow_flight::FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        return Err(Status::unimplemented("poll_flight_info"));
    }

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        return Err(Status::unimplemented("do_exchange"));
    }

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        unimplemented!()
    }

    async fn list_flights(
        &self,
        _request: Request<arrow_flight::Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        return Err(Status::unimplemented("list_flights"));
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let resume_watermark = request
            .metadata()
            .get("nozzle-resume")
            .and_then(|v| serde_json::from_slice(v.as_bytes()).ok());
        let descriptor = request.into_inner();
        let info = self.get_flight_info(descriptor, resume_watermark).await?;
        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        _request: Request<arrow_flight::FlightDescriptor>,
    ) -> Result<Response<arrow_flight::SchemaResult>, Status> {
        return Err(Status::unimplemented("get_schema"));
    }

    async fn do_get(
        &self,
        request: Request<arrow_flight::Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let data_stream = self.do_get(ticket).await?;
        Ok(Response::new(data_stream))
    }

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        return Err(Status::unimplemented("do_put"));
    }

    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        return Err(Status::unimplemented("list_actions"));
    }

    async fn do_action(
        &self,
        _request: Request<arrow_flight::Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        return Err(Status::unimplemented("do_action"));
    }
}

impl Service {
    #[instrument(skip(self))]
    async fn get_flight_info(
        &self,
        descriptor: FlightDescriptor,
        resume_watermark: Option<ResumeWatermark>,
    ) -> Result<FlightInfo, Error> {
        let (serialized_plan, schema) = match DescriptorType::try_from(descriptor.r#type)
            .map_err(|e| Error::PbDecodeError(e.to_string()))?
        {
            DescriptorType::Cmd => {
                let msg = Any::decode(descriptor.cmd.as_ref())?;
                if let Some(sql_query) = msg
                    .unpack::<CommandStatementQuery>()
                    .map_err(|e| Error::PbDecodeError(e.to_string()))?
                {
                    // The magic that turns a SQL string into a DataFusion logical plan that can be
                    // sent back over the wire:
                    // - Parse the SQL query.
                    // - Infer depedencies and collect them into a catalog.
                    // - Build a DataFusion query context with empty tables from that catalog.
                    // - Use that context to plan the SQL query.
                    // - Serialize the plan to bytes using datafusion-protobufs.
                    let query = parse_sql(&sql_query.query)?;
                    let query_ctx = self
                        .dataset_store
                        .clone()
                        .planning_ctx_for_sql(&query)
                        .await?;
                    query_ctx
                        .sql_to_remote_plan(query, resume_watermark)
                        .await?
                } else {
                    return Err(Error::UnsupportedFlightDescriptorCommand(msg.type_url));
                }
            }
            DescriptorType::Path | DescriptorType::Unknown => {
                return Err(Error::UnsupportedFlightDescriptorType(
                    descriptor.r#type.to_string(),
                ));
            }
        };

        let endpoint = FlightEndpoint {
            ticket: Some(Ticket::new(serialized_plan)),

            // We may eventually want to leverage the load-balancing capabilities of Arrow Flight.
            // But for now leaving `location` this empty is fine, per the docs
            // https://arrow.apache.org/docs/format/Flight.html:
            //
            // > If the list is empty, the expectation is that the ticket can only be redeemed on the
            // > current service where the ticket was generated.
            location: vec![],

            // We don't currently have anything to expire.
            expiration_time: None,

            app_metadata: Bytes::new(),
        };

        let info = FlightInfo {
            flight_descriptor: Some(descriptor),
            schema: ipc_schema(&schema),
            endpoint: vec![endpoint],

            // Not important.
            ordered: false,
            total_records: -1,
            total_bytes: -1,
            app_metadata: Bytes::new(),
        };

        Ok(info)
    }

    #[instrument(skip_all)]
    async fn do_get(&self, ticket: arrow_flight::Ticket) -> Result<TonicStream<FlightData>, Error> {
        let remote_plan = common::remote_plan_from_bytes(&ticket.ticket)?;
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
