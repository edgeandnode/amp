use axum::{
    http::StatusCode,
    response::{IntoResponse, Response as AxumResponse},
    Json,
};
use common::{
    arrow::{self, ipc::writer::IpcDataGenerator},
    catalog::collect_scanned_tables,
    config::Config,
    query_context::{parse_sql, Error as CoreError, QueryContext},
};
use datafusion::{
    common::DFSchema,
    error::DataFusionError,
    execution::{runtime_env::RuntimeEnv, SendableRecordBatchStream},
};
use dataset_store::{DatasetError, DatasetStore};
use futures::{Stream, StreamExt as _, TryStreamExt};
use metadata_db::MetadataDb;
use std::{pin::Pin, sync::Arc};
use thiserror::Error;

use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    flight_descriptor::DescriptorType,
    flight_service_server::FlightService,
    sql::{Any, CommandStatementQuery},
    ActionType, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, Ticket,
};
use bytes::{BufMut, Bytes, BytesMut};
use dataset_store::sql_datasets::is_incremental;
use prost::Message as _;
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

    #[error("not incremental query: {0}")]
    NotIncrementalQuery(String),
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
                | CoreError::PlanEncodingError(_),
            ) => StatusCode::BAD_REQUEST,
            Error::CoreError(
                CoreError::DatasetError(_)
                | CoreError::ConfigError(_)
                | CoreError::PlanningError(_)
                | CoreError::ExecutionError(_)
                | CoreError::MetaTableError(_),
            ) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::ExecutionError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::DatasetStoreError(e) if e.is_not_found() => StatusCode::NOT_FOUND,
            Error::DatasetStoreError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::PbDecodeError(_) => StatusCode::BAD_REQUEST,
            Error::UnsupportedFlightDescriptorType(_) => StatusCode::BAD_REQUEST,
            Error::UnsupportedFlightDescriptorCommand(_) => StatusCode::BAD_REQUEST,
            Error::NotIncrementalQuery(_) => StatusCode::BAD_REQUEST,
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
            Error::CoreError(CoreError::PlanEncodingError(_)) => {
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

            Error::NotIncrementalQuery(_) => Status::invalid_argument(e.to_string()),
        }
    }
}

#[derive(Clone)]
pub struct Service {
    env: Arc<RuntimeEnv>,
    dataset_store: Arc<DatasetStore>,
}

impl Service {
    pub fn new(
        config: Arc<Config>,
        metadata_db: Option<MetadataDb>,
    ) -> Result<Self, DataFusionError> {
        let env = Arc::new(config.make_runtime_env()?);
        Ok(Self {
            env,
            dataset_store: DatasetStore::new(config, metadata_db),
        })
    }

    pub async fn execute_query(&self, sql: &str) -> Result<SendableRecordBatchStream, Error> {
        let query = parse_sql(sql).map_err(|err| Error::from(err))?;
        let ctx = self
            .dataset_store
            .clone()
            .ctx_for_sql(&query, self.env.clone())
            .await
            .map_err(|err| Error::DatasetStoreError(err))?;
        let plan = ctx.plan_sql(query).await.map_err(|err| Error::from(err))?;
        let stream = ctx
            .execute_plan(plan)
            .await
            .map_err(|err| Error::from(err))?;
        Ok(stream)
    }

    pub async fn execute_stream_query(
        &self,
        sql: &str,
    ) -> Result<SendableRecordBatchStream, Error> {
        use futures::channel::mpsc;

        let query = parse_sql(&sql).map_err(|err| Error::from(err))?;
        let ctx = self
            .dataset_store
            .clone()
            .ctx_for_sql(&query, self.env.clone())
            .await
            .map_err(|err| Error::DatasetStoreError(err))?;
        let plan = ctx
            .plan_sql(query.clone())
            .await
            .map_err(|err| Error::from(err))?;
        // let mut synced_ranges = dataset_store::sql_datasets::synced_blocks_for_query(
        //     &query,
        //     self.dataset_store.clone(),
        //     self.env.clone(),
        // )
        // .await
        // .map_err(|_| {
        //     Error::ExecutionError(DataFusionError::Execution(
        //         "???".to_string(), // TODO: need meaningful error here
        //     ))
        // })?;
        // let mut synced_ranges = MultiRange::from_values(&[0, 1]).unwrap(); // TODO: remove after tests
        let mut current_end_block = dataset_store::sql_datasets::max_end_block(
            &query,
            self.dataset_store.clone(),
            self.env.clone(),
        )
        .await
        .unwrap();

        if let Ok(false) = is_incremental(&plan) {
            return Err(Error::NotIncrementalQuery(sql.to_string()));
        }

        let (tx, rx) = mpsc::unbounded();

        let schema = plan.schema().clone().as_ref().clone().into();

        // initial ranges
        //for (start, end) in &synced_ranges.ranges {
        if let Some(end) = current_end_block {
            let mut stream = dataset_store::sql_datasets::execute_query_for_range(
                query.clone(),
                self.dataset_store.clone(),
                self.env.clone(),
                0,
                end,
            )
            .await
            .map_err(|_| {
                Error::ExecutionError(DataFusionError::Execution(
                    "???".to_string(), // TODO: need meaningful error here
                ))
            })?;
            // let schema = stream.schema().clone();

            while let Some(batch) = stream.next().await {
                match batch {
                    Ok(batch) => {
                        if tx.unbounded_send(Ok(batch)).is_err() {
                            return Err(Error::ExecutionError(DataFusionError::Execution(
                                "???".to_string(), // TODO: need meaningful error here
                            )));
                        }
                    }
                    Err(e) => {
                        let _ = tx.unbounded_send(Err(e));
                        return Err(Error::ExecutionError(DataFusionError::Execution(
                            "???".to_string(), // TODO: need meaningful error here
                        )));
                    }
                }
            }
        }

        // async notify
        // let ds_store = self.dataset_store.clone();
        // tokio::spawn(async move {
        //     match &ds_store.metadata_db {
        //         Some(mdb) => {
        //             let mut i: u64 = 0;
        //             loop {
        //                 // TODO: check sigterm, sigkill, etc.
        //                 // TODO: doesn't stop if client stops (because waiting for the whole server to stop)
        //                 tokio::select! {
        //                     _ = tokio::signal::ctrl_c() => return,
        //                     _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
        //                 }
        //                 //tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        //                 let msg = serde_json::to_string(&(i, i + 2)).unwrap();
        //                 mdb.notify("qwe", &msg).await.unwrap();
        //                 i += 3;
        //
        //                 // stop
        //                 if i > 10 {
        //                     return;
        //                 }
        //             }
        //         }
        //         _ => return,
        //     }
        // });

        // async listen
        let sql = sql.to_string();
        // let service = self.clone();
        let ds_store = self.dataset_store.clone();
        let env = self.env.clone();

        tokio::spawn(async move {
            match &ds_store.metadata_db {
                Some(mdb) => {
                    let query = parse_sql(&sql).map_err(|err| Error::from(err)).unwrap();
                    let datasets = dataset_store::sql_datasets::queried_datasets(&query)
                        .await
                        .unwrap();

                    let mut notifications = Vec::new();
                    for dataset_name in datasets {
                        let channel = common::cdc_helpers::cdc_pg_channel(&dataset_name);
                        let stream = mdb.listen(&channel).await.unwrap();
                        notifications.push(stream);
                    }
                    let mut notifications = futures::stream::select_all(notifications);

                    loop {
                        tokio::select! {
                            _ = tokio::signal::ctrl_c() => return,
                            notification = notifications.next() => {
                                match notification {
                                    Some(Ok(_)) => {
                                        let end = dataset_store::sql_datasets::max_end_block(
                                            &query,
                                            ds_store.clone(),
                                            env.clone(),
                                        ).await.unwrap();

                                        let (start, end) = match (current_end_block, end) {
                                            (Some(start), Some(end)) if end > start => (start + 1, end),
                                            (None, Some(end)) => (0, end),
                                            (_, _) => continue,
                                        };

                                        //let (start, end) = serde_json::from_str::<(u64, u64)>(&notification.payload()).unwrap();
                                        //let compliment = synced_ranges.complement(start, end);
                                        //println!("COMPLIMENT: {:?}", compliment);


                                        // for (start, end) in &compliment.ranges {
                                            let mut stream = dataset_store::sql_datasets::execute_query_for_range(
                                                query.clone(),
                                                ds_store.clone(),
                                                env.clone(),
                                                start,
                                                end,
                                            ).await.unwrap();

                                            while let Some(batch) = stream.next().await {
                                                match batch {
                                                    Ok(batch) => {
                                                        if tx.unbounded_send(Ok(batch)).is_err() {
                                                            return;
                                                        }
                                                    }
                                                    Err(e) => {
                                                        let _ = tx.unbounded_send(Err(e));
                                                        return;
                                                    }
                                                }
                                            }
                                        //}

                                        current_end_block = Some(end);
                                    },
                                    _ => return,
                                }
                            },
                        }
                    }
                }
                _ => return,
            }
        });

        let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, rx);

        Ok(Box::pin(adapter) as SendableRecordBatchStream)
    }
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
        //  DataFusion requires a context for initially deserializing a logical plan. That context is used a
        // `FunctionRegistry` for UDFs. That context must include all UDFs that may be used in the
        // query.
        let ctx = QueryContext::for_catalog(
            self.dataset_store.initial_catalog().await?,
            self.env.clone(),
        )?;
        let plan = ctx.plan_from_bytes(&ticket.ticket).await?;

        // The deserialized plan references empty tables, so we need to load the actual tables from the catalog.
        let table_refs = collect_scanned_tables(&plan);
        let catalog = self
            .dataset_store
            .clone()
            .load_catalog_for_table_refs(table_refs.iter())
            .await?;
        let query_ctx = QueryContext::for_catalog(catalog, self.env.clone())?;
        let stream = query_ctx.execute_remote_plan(plan).await?;

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
