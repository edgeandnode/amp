use std::{pin::Pin, sync::Arc};

use arrow_flight::{
    ActionType, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, Ticket,
    encode::FlightDataEncoderBuilder,
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
    BoxError, SPECIAL_BLOCK_NUM,
    arrow::{self, array::RecordBatch, datatypes::SchemaRef, ipc::writer::IpcDataGenerator},
    catalog::physical::Catalog,
    config::Config,
    metadata::segments::BlockRange,
    notification_multiplexer::{self, NotificationMultiplexerHandle},
    query_context::{
        DetachedLogicalPlan, Error as CoreError, PlanningContext, QueryContext, QueryEnv, parse_sql,
    },
};
use datafusion::{
    common::{DFSchema, tree_node::TreeNodeRecursion},
    error::DataFusionError,
    logical_expr::LogicalPlan,
};
use dataset_store::{DatasetError, DatasetStore};
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
    DatasetStoreError(#[from] DatasetError),

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
            Error::DatasetStoreError(e) if e.is_not_found() => StatusCode::NOT_FOUND,
            Error::DatasetStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
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
            Error::DatasetStoreError(e) if e.is_not_found() => Status::not_found(e.to_string()),
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
            Error::InvalidQuery(_) => Status::invalid_argument(e.to_string()),
        }
    }
}

pub type QueryResultStream = BoxStream<'static, Result<(RecordBatch, Option<BlockRange>), Error>>;

#[derive(Clone)]
pub struct Service {
    config: Arc<Config>,
    env: QueryEnv,
    dataset_store: Arc<DatasetStore>,
    notification_multiplexer: Arc<NotificationMultiplexerHandle>,
}

impl Service {
    pub async fn new(config: Arc<Config>, metadata_db: Arc<MetadataDb>) -> Result<Self, Error> {
        let env = config.make_query_env().map_err(Error::ExecutionError)?;
        let dataset_store = DatasetStore::new(config.clone(), metadata_db.clone());
        let notification_multiplexer =
            Arc::new(notification_multiplexer::spawn((*metadata_db).clone()));
        Ok(Self {
            config,
            env,
            dataset_store,
            notification_multiplexer,
        })
    }

    pub async fn execute_query(&self, sql: &str) -> Result<QueryResultStream, Error> {
        let query = parse_sql(sql).map_err(|err| Error::from(err))?;
        let dataset_store = self.dataset_store.clone();
        let catalog = dataset_store
            .catalog_for_sql(&query, self.env.clone())
            .await
            .map_err(|err| Error::DatasetStoreError(err))?;

        let ctx = PlanningContext::new(catalog.logical().clone());
        let plan = ctx
            .plan_sql(query.clone())
            .await
            .map_err(|err| Error::from(err))?;
        let is_streaming = common::stream_helpers::is_streaming(&query);
        self.execute_plan(catalog, dataset_store, plan, is_streaming)
            .await
    }

    #[tracing::instrument(skip_all, err)]
    pub async fn execute_plan(
        &self,
        catalog: Catalog,
        dataset_store: Arc<DatasetStore>,
        plan: DetachedLogicalPlan,
        is_streaming: bool,
    ) -> Result<QueryResultStream, Error> {
        // is_incremental returns an error if query contains DDL, DML, etc.
        let is_incr = plan
            .is_incremental()
            .map_err(|e| Error::InvalidQuery(e.to_string()))?;
        if is_streaming && !is_incr {
            return Err(Error::InvalidQuery(
                "not incremental queries are not supported for streaming".to_string(),
            ));
        }

        // If not streaming or metadata db is not available, execute once
        if !is_streaming {
            let original_schema = plan.schema().clone();
            let should_transform =
                dbg!(should_transform_plan(&plan).map_err(Error::ExecutionError)?);
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
            Ok(record_baches
                .map_ok(|batch| (batch, None))
                .map_err(|err| Error::StreamingExecutionError(err.to_string()))
                .boxed())
        } else {
            // As an optimization, start the stream from the minimum start block across all tables.
            // Otherwise starting from `0` would spend time scanning ranges known to be empty.
            let earliest_block = catalog
                .earliest_block()
                .await
                .map_err(|e| Error::StreamingExecutionError(e.to_string()))?;

            // If no tables are synced, we return an empty stream.
            let Some(earliest_block) = earliest_block else {
                return Ok(Box::pin(stream::empty()));
            };

            let query = StreamingQuery::spawn(
                self.env.clone(),
                catalog,
                dataset_store,
                plan,
                earliest_block,
                None,
                &self.notification_multiplexer,
                false,
                self.config.microbatch_max_interval,
            )
            .await
            .map_err(|e| Error::StreamingExecutionError(e.to_string()))?;

            Ok(query_result_stream(query.as_stream()))
        }
    }
}

fn should_transform_plan(plan: &DetachedLogicalPlan) -> Result<bool, DataFusionError> {
    let mut result = true;
    plan.apply(|node| {
        match node {
            LogicalPlan::EmptyRelation(_) | LogicalPlan::Aggregate(_) => {
                // If any node is an empty relation or aggregate function, trying to propagate the `SPECIAL_BLOCK_NUM` will
                // probably cause problems.
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
        let descriptor = request.into_inner();
        let info = self.get_flight_info(descriptor).await?;
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
    async fn get_flight_info(&self, descriptor: FlightDescriptor) -> Result<FlightInfo, Error> {
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
                    query_ctx.sql_to_remote_plan(query).await?
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
        let remote_plan = common::query_context::remote_plan_from_bytes(&ticket.ticket)?;
        let table_refs = remote_plan.table_refs.into_iter().map(|t| t.into());

        let catalog = self
            .dataset_store
            .load_physical_catalog(table_refs, remote_plan.function_refs, &self.env)
            .await?;
        let query_ctx = PlanningContext::new(catalog.logical().clone());
        let plan = query_ctx
            .plan_from_bytes(&remote_plan.serialized_plan)
            .await?;
        let schema: SchemaRef = plan.schema().as_ref().clone().into();

        let dataset_store = self.dataset_store.clone();
        let stream = self
            .execute_plan(catalog, dataset_store, plan, remote_plan.is_streaming)
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

fn query_result_stream(
    mut message_stream: BoxStream<'static, Result<QueryMessage, BoxError>>,
) -> QueryResultStream {
    stream! {
        let mut current_range: Option<BlockRange> = None;
        while let Some(result) = message_stream.next().await {
            match result {
                Ok(message) => {
                    match message {
                        QueryMessage::MicrobatchStart(range) => {
                            assert_eq!(current_range, None);
                            current_range = Some(range);
                        }
                        QueryMessage::Data(record_batch) => {
                            assert!(current_range.is_some());
                            yield Ok((record_batch, current_range.clone()));
                        }
                        QueryMessage::MicrobatchEnd(_) => {
                            assert!(current_range.is_some());
                            current_range = None;
                        }
                    }
                }
                Err(err) => {
                    yield Err(Error::StreamingExecutionError(err.to_string()));
                }
            }
        }
    }
    .boxed()
}

fn flight_data_stream(
    mut query_result_stream: QueryResultStream,
    schema: SchemaRef,
) -> TonicStream<FlightData> {
    // The FlightDataEncoderBuilder interface doesn't allow us to set the metadata per record
    // batch. And there doesn't seem to be an interface that allows us to cleanly encode the
    // output stream ourselves. So we need to be careful to send schema messages and metadata
    // properly.
    stream! {
        let mut schema_sent = false;
        while let Some(result) = query_result_stream.next().await {
            let (batch, range) = match result {
                Ok(result) => result,
                Err(err) => {
                    yield Err(err.into());
                    return;
                }
            };
            let metadata = serde_json::to_string(&json!({"range": range})).unwrap();

            if !schema_sent {
                // Send schema message.
                let mut schema_encoder = FlightDataEncoderBuilder::new()
                    .with_schema(schema.clone())
                    .build(futures::stream::empty());
                if let Some(schema_result) = schema_encoder.next().await {
                    match schema_result {
                        Ok(flight_data) => yield Ok(flight_data),
                        Err(err) => {
                            yield Err(err.into());
                            return;
                        }
                    }
                }
                schema_sent = true;
            }

            // Create encoder for this single batch with custom metadata, skipping schema messages.
            let mut batch_encoder = FlightDataEncoderBuilder::new()
                .build(futures::stream::once(async { Ok(batch) }));
            let mut first_message = true;
            while let Some(result) = batch_encoder.next().await {
                let flight_data = match result {
                    Ok(flight_data) => flight_data,
                    Err(err) => {
                        yield Err(err.into());
                        return;
                    }
                };
                let schema_message = first_message && !flight_data.data_header.is_empty() && flight_data.data_body.is_empty();
                first_message = false;
                if schema_message {
                    continue;
                } else {
                    yield Ok(flight_data.with_app_metadata(metadata.clone()));
                }
            }
        }
    }
    .boxed()
}
