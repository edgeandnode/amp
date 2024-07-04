use common::{
    arrow::{self, ipc::writer::IpcDataGenerator},
    catalog::{collect_scanned_tables, physical::Catalog, resolve_table_references},
    config::Config,
    query_context::{parse_sql, Error as CoreError, QueryContext},
    BoxError,
};
use datafusion::{
    common::DFSchema, error::DataFusionError, execution::runtime_env::RuntimeEnv,
    sql::TableReference,
};
use dataset_store::{DatasetError, DatasetStore};
use futures::{Stream, StreamExt as _, TryStreamExt};
use log::debug;
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
use prost::Message as _;
use tonic::{Request, Response, Status};

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Error, Debug)]
enum Error {
    #[error("ProtocolBuffers decoding error: {0}")]
    PbDecodeError(String),

    #[error("unsupported flight descriptor type: {0}")]
    UnsupportedFlightDescriptorType(String),

    #[error("unsupported flight descriptor command: {0}")]
    UnsupportedFlightDescriptorCommand(String),

    #[error("query execution error: {0}")]
    ExecutionError(DataFusionError),

    #[error("SQL parse error: {0}")]
    SqlParseError(BoxError),

    #[error("dataset '{0}' not found")]
    DatasetNotFound(String),

    #[error("error looking up datasets: {0}")]
    DatasetStoreError(DatasetError),

    #[error(transparent)]
    CoreError(#[from] CoreError),
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

impl From<Error> for Status {
    fn from(e: Error) -> Self {
        match &e {
            Error::PbDecodeError(_) => Status::invalid_argument(e.to_string()),
            Error::UnsupportedFlightDescriptorType(_) => Status::invalid_argument(e.to_string()),
            Error::UnsupportedFlightDescriptorCommand(_) => Status::invalid_argument(e.to_string()),
            Error::DatasetNotFound(_) => Status::not_found(e.to_string()),
            Error::DatasetStoreError(_) => Status::internal(e.to_string()),

            Error::CoreError(CoreError::InvalidPlan(_)) => Status::invalid_argument(e.to_string()),
            Error::CoreError(CoreError::SqlParseError(_)) | Error::SqlParseError(_) => {
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
        }
    }
}

pub(super) struct Service {
    config: Config,
    env: Arc<RuntimeEnv>,
}

impl Service {
    pub fn new(config: Config) -> Result<Self, DataFusionError> {
        let env = Arc::new(config.make_runtime_env()?);
        Ok(Self { config, env })
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
        let (serialized_plan, schema) = match DescriptorType::try_from(descriptor.r#type)? {
            DescriptorType::Cmd => {
                let msg = Any::decode(descriptor.cmd.as_ref())?;
                if let Some(sql_query) = msg
                    .unpack::<CommandStatementQuery>()
                    .map_err(|e| Error::PbDecodeError(e.to_string()))?
                {
                    // The magic that turns a SQL string into a DataFusion logical plan that can be
                    // sent back over the wire:
                    // - Parse the SQL query.
                    // - Collect table references in the query.
                    // - Assume that in `foo.bar`, `foo` is a dataset name.
                    // - Look up those dataset names in the configured dataset store.
                    // - Collect those datasets into a catalog.
                    // - Build a DataFusion query context with empty tables from that catalog.
                    // - Use that context to plan the SQL query.
                    // - Serialize the plan to bytes using datafusion-protobufs.
                    let statement = parse_sql(&sql_query.query).map_err(Error::SqlParseError)?;
                    let (tables, _) = resolve_table_references(&statement, true)
                        .map_err(|e| CoreError::SqlParseError(e.into()))?;
                    let catalog = load_catalog_for_table_refs(tables, &self.config).await?;
                    let query_ctx = QueryContext::for_catalog(catalog, self.env.clone()).await?;
                    query_ctx.sql_to_remote_plan(statement).await?
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
        // `FunctionRegistry` for UDFs. So using a `QueryContext::empty` works as that includes UDFs.
        let ctx = QueryContext::empty(self.env.clone()).await?;
        let plan = ctx.plan_from_bytes(&ticket.ticket).await?;

        // The deserialized plan references empty tables, so we need to load the actual tables from the catalog.
        let table_refs = collect_scanned_tables(&plan);
        let catalog = load_catalog_for_table_refs(table_refs, &self.config).await?;
        let query_ctx = QueryContext::for_catalog(catalog, self.env.clone()).await?;
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
    let ipc_opts = &Default::default();
    let encoded = IpcDataGenerator::default().schema_to_bytes(&schema.into(), ipc_opts);

    // Unwrap: writing to `BytesMut` never fails.
    let mut bytes = BytesMut::new().writer();
    arrow::ipc::writer::write_message(&mut bytes, encoded, ipc_opts).unwrap();
    bytes.into_inner().into()
}

async fn load_catalog_for_table_refs(
    table_refs: impl IntoIterator<Item = TableReference>,
    config: &Config,
) -> Result<Catalog, Error> {
    use Error::*;

    let dataset_store = DatasetStore::new(&config);
    let catalog = match dataset_store.load_catalog_for_table_refs(table_refs).await {
        Ok(dataset) => dataset,
        Err(e) if e.is_not_found() => {
            debug!("dataset not found, full error: {}", e);
            return Err(DatasetNotFound(e.dataset.unwrap()));
        }
        Err(e) => return Err(Error::DatasetStoreError(e)),
    };

    Ok(catalog)
}
