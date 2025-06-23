use std::{pin::Pin, sync::Arc};

use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    flight_descriptor::DescriptorType,
    flight_service_server::FlightService,
    sql::{Any, CommandStatementQuery},
    ActionType, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, Ticket,
};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response as AxumResponse},
    Json,
};
use bytes::{BufMut, Bytes, BytesMut};
use common::{
    arrow::{self, ipc::writer::IpcDataGenerator},
    config::Config,
    plan_visitors::{
        forbid_underscore_prefixed_aliases, is_incremental, propagate_block_num,
        unproject_special_block_num_column,
    },
    query_context::{parse_sql, Error as CoreError, QueryContext, QueryEnv},
    BlockNum,
};
use datafusion::{
    arrow::array::RecordBatch,
    common::{
        tree_node::{TreeNode, TreeNodeRecursion},
        DFSchema,
    },
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    logical_expr::LogicalPlan,
};
use dataset_store::{DatasetError, DatasetStore};
use futures::{channel::mpsc, SinkExt, Stream, StreamExt as _, TryStreamExt};
use metadata_db::MetadataDb;
use prost::Message as _;
use thiserror::Error;
use tonic::{Request, Response, Status};

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

impl IntoResponse for Error {
    fn into_response(self) -> AxumResponse {
        let error_message = self.to_string();
        let status = match self {
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
            Error::ExecutionError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::StreamingExecutionError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DatasetStoreError(e) if e.is_not_found() => StatusCode::NOT_FOUND,
            Error::DatasetStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::PbDecodeError(_) => StatusCode::BAD_REQUEST,
            Error::UnsupportedFlightDescriptorType(_) => StatusCode::BAD_REQUEST,
            Error::UnsupportedFlightDescriptorCommand(_) => StatusCode::BAD_REQUEST,
            Error::InvalidQuery(_) => StatusCode::BAD_REQUEST,
        };
        let body = serde_json::json!({
            "error": error_message,
        });
        (status, Json(body)).into_response()
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

#[derive(Clone)]
pub struct Service {
    env: QueryEnv,
    dataset_store: Arc<DatasetStore>,
}

impl Service {
    pub async fn new(config: Arc<Config>, metadata_db: Arc<MetadataDb>) -> Result<Self, Error> {
        let env = config.make_query_env().map_err(Error::ExecutionError)?;
        let dataset_store = DatasetStore::new(config.clone(), metadata_db);
        Ok(Self { env, dataset_store })
    }

    pub async fn execute_query(&self, sql: &str) -> Result<SendableRecordBatchStream, Error> {
        let query = parse_sql(sql).map_err(|err| Error::from(err))?;
        let ctx = self
            .dataset_store
            .ctx_for_sql(&query, self.env.clone())
            .await
            .map_err(|err| Error::DatasetStoreError(err))?;
        let plan = ctx
            .plan_sql(query.clone())
            .await
            .map_err(|err| Error::from(err))?;
        let is_streaming = common::stream_helpers::is_streaming(&query);

        let ctx = Arc::new(ctx);
        self.execute_plan(ctx, plan, is_streaming).await
    }

    async fn execute_plan_for_range_and_send_results_to_stream(
        plan: LogicalPlan,
        ctx: &QueryContext,
        start: BlockNum,
        end: BlockNum,
        mut tx: mpsc::Sender<datafusion::error::Result<RecordBatch>>,
    ) -> () {
        let mut stream =
            dataset_store::sql_datasets::execute_plan_for_range(plan, ctx, start, end, false)
                .await
                .unwrap();

        while let Some(batch) = stream.next().await {
            match batch {
                Ok(batch) => {
                    if tx.send(Ok(batch)).await.is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            }
        }
    }

    pub async fn execute_plan(
        &self,
        ctx: Arc<QueryContext>,
        plan: LogicalPlan,
        is_streaming: bool,
    ) -> Result<SendableRecordBatchStream, Error> {
        use futures::channel::mpsc;

        // is_incremental returns an error if query contains EXPLAIN, DML, etc.
        let is_incr = is_incremental(&plan).map_err(|e| Error::InvalidQuery(e.to_string()))?;
        if is_streaming && !is_incr {
            return Err(Error::InvalidQuery(
                "not incremental queries are not supported for streaming".to_string(),
            ));
        }

        // If not streaming or metadata db is not available, execute once
        if !is_streaming {
            let original_schema = plan.schema().clone();
            let should_transform = should_transform_plan(&plan).map_err(Error::ExecutionError)?;
            forbid_underscore_prefixed_aliases(&plan).map_err(Error::ExecutionError)?;
            let plan = if should_transform {
                let plan = propagate_block_num(plan).map_err(Error::ExecutionError)?;
                let plan = unproject_special_block_num_column(plan, original_schema)
                    .map_err(|e| Error::ExecutionError(e))?;
                plan
            } else {
                plan
            };
            return ctx.execute_plan(plan).await.map_err(|err| Error::from(err));
        }

        // Start infinite stream
        let first_range = dataset_store::sql_datasets::synced_blocks_for_plan(&plan, &ctx)
            .await
            .map_err(|e| Error::CoreError(CoreError::DatasetError(e)))?
            .first();
        let mut current_end_block = first_range.map(|(_, end)| end);

        let (tx, rx) = mpsc::channel(1);

        let schema = plan.schema().clone().as_ref().clone().into();

        // Execute initial ranges
        if let Some((start, end)) = first_range {
            // Execute the first range and return an error if a query is not valid
            let mut stream = dataset_store::sql_datasets::execute_plan_for_range(
                plan.clone(),
                &ctx,
                start,
                end,
                false,
            )
            .await
            .map_err(|e| Error::CoreError(CoreError::DatasetError(e)))?;

            let mut tx_first_range = tx.clone();
            tokio::spawn(async move {
                while let Some(batch) = stream.next().await {
                    let send_result = tx_first_range.send(batch).await;
                    if send_result.is_err() {
                        return Err(Error::StreamingExecutionError(
                            "failed to return a next batch".to_string(),
                        ));
                    }
                }

                Ok(())
            });
        }

        let ctx = ctx.clone();
        let metadata_db = self.dataset_store.metadata_db.as_ref().clone();

        // async listen
        tokio::spawn(async move {
            let locations = ctx.catalog().tables().iter().map(|t| t.location_id());

            let mut notifications = Vec::new();
            for location in locations {
                let channel = common::stream_helpers::change_tracking_pg_channel(location);
                let stream = metadata_db.listen(&channel).await.unwrap();
                notifications.push(stream);
            }
            let mut notifications = futures::stream::select_all(notifications);

            while let Some(Ok(_)) = notifications.next().await {
                let end = dataset_store::sql_datasets::max_end_block(&plan, &ctx)
                    .await
                    .unwrap();

                let (start, end) = match (current_end_block, end) {
                    (Some(start), Some(end)) if end > start => (start + 1, end),
                    (None, Some(end)) => (0, end),
                    (_, _) => continue,
                };

                Self::execute_plan_for_range_and_send_results_to_stream(
                    plan.clone(),
                    &ctx,
                    start,
                    end,
                    tx.clone(),
                )
                .await;

                current_end_block = Some(end);
            }
        });

        let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, rx);

        Ok(Box::pin(adapter) as SendableRecordBatchStream)
    }
}

fn should_transform_plan(plan: &LogicalPlan) -> Result<bool, DataFusionError> {
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

    async fn do_get(&self, ticket: arrow_flight::Ticket) -> Result<TonicStream<FlightData>, Error> {
        let remote_plan = common::query_context::remote_plan_from_bytes(&ticket.ticket)?;
        let table_refs = remote_plan.table_refs.into_iter().map(|t| t.into());

        let catalog = self
            .dataset_store
            .load_physical_catalog(table_refs, remote_plan.function_refs, &self.env)
            .await?;
        let query_ctx = QueryContext::for_catalog(catalog, self.env.clone())?;
        let plan = query_ctx
            .plan_from_bytes(&remote_plan.serialized_plan)
            .await?;

        let query_ctx = Arc::new(query_ctx);
        let stream = self
            .execute_plan(query_ctx, plan, remote_plan.is_streaming)
            .await?;

        Ok(FlightDataEncoderBuilder::new()
            .with_schema(stream.schema())
            .build(
                stream
                    .map_err(Error::ExecutionError)
                    .map_err(Status::from)
                    .err_into(),
            )
            .map_err(Status::from)
            .boxed())
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
