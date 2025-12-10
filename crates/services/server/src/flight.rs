//! Arrow Flight RPC server implementation
//!
//! This module provides the Arrow Flight protocol implementation for high-performance
//! binary query execution. It includes the Service struct that handles query execution
//! and the FlightService trait implementation for the Arrow Flight protocol.

use std::{collections::BTreeMap, pin::Pin, sync::Arc};

use arrow_flight::{
    ActionType, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaAsIpc, Ticket,
    encode::GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES,
    error::FlightError,
    flight_descriptor::DescriptorType,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{Any, CommandStatementQuery},
};
use async_stream::stream;
use axum::{Router, http::StatusCode, response::IntoResponse};
use bytes::{BufMut, Bytes, BytesMut};
use common::{
    BlockNum, DetachedLogicalPlan, PlanningContext, QueryContext,
    arrow::{
        self,
        array::RecordBatch,
        datatypes::SchemaRef,
        ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
    },
    catalog::{
        errors::{CatalogForSqlError, GetPhysicalCatalogError, PlanningCtxForSqlError},
        physical::Catalog,
        sql::{catalog_for_sql, planning_ctx_for_sql},
    },
    metadata::segments::{BlockRange, ResumeWatermark},
    query_context::{Error as CoreError, QueryEnv},
    sql_str::SqlStr,
    utils::error_with_causes,
};
use datafusion::{
    common::DFSchema, error::DataFusionError, physical_plan::stream::RecordBatchStreamAdapter,
};
use dataset_store::{DatasetStore, GetDatasetError};
use dump::streaming_query::{QueryMessage, StreamingQuery};
use futures::{
    Stream, StreamExt as _, TryStreamExt,
    stream::{self, BoxStream},
};
use metadata_db::{MetadataDb, NotificationMultiplexerHandle, notification_multiplexer};
use monitoring::telemetry::metrics::Meter;
use prost::Message as _;
use serde_json::json;
use thiserror::Error;
use tonic::{Request, Response, Status, service::Routes};
use tracing::instrument;

use crate::{
    config::Config, metrics::MetricsRegistry,
    non_empty_record_batch_stream::NonEmptyRecordBatchStream, service::InitError,
};

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct Service {
    config: Arc<Config>,
    env: QueryEnv,
    dataset_store: DatasetStore,
    notification_multiplexer: Arc<NotificationMultiplexerHandle>,
    metrics: Option<Arc<MetricsRegistry>>,
    metadata_db: MetadataDb,
}

impl Service {
    pub async fn create(
        config: Arc<Config>,
        metadata_db: MetadataDb,
        dataset_store: DatasetStore,
        meter: Option<Meter>,
    ) -> Result<Self, InitError> {
        let env = config.make_query_env().map_err(InitError::QueryEnv)?;
        let notification_multiplexer =
            Arc::new(notification_multiplexer::spawn(metadata_db.clone()));
        let metrics = meter.as_ref().map(|m| Arc::new(MetricsRegistry::new(m)));

        Ok(Self {
            config,
            env,
            dataset_store,
            notification_multiplexer,
            metrics,
            metadata_db,
        })
    }

    pub async fn execute_query(
        &self,
        sql: impl AsRef<SqlStr>,
        is_streaming: Option<bool>,
        resume_watermark: Option<ResumeWatermark>,
    ) -> Result<QueryResultStream, Error> {
        let query = common::sql::parse(sql.as_ref())
            .map_err(|err| Error::CoreError(CoreError::SqlParseError(err)))?;
        let catalog = catalog_for_sql(
            &self.dataset_store,
            &self.metadata_db,
            &query,
            self.env.clone(),
        )
        .await
        .map_err(Error::CatalogForSqlError)?;

        let ctx = PlanningContext::new(catalog.logical().clone());
        let plan = ctx
            .plan_sql(query.clone())
            .await
            .map_err(Error::CoreError)?;

        let is_streaming =
            is_streaming.unwrap_or_else(|| common::stream_helpers::is_streaming(&query));
        let result = self
            .execute_plan(
                catalog,
                &self.dataset_store,
                plan,
                is_streaming,
                resume_watermark,
            )
            .await;

        // Record execution error
        if result.is_err()
            && let Some(metrics) = &self.metrics
        {
            let error_code = result
                .as_ref()
                .err()
                .map(|err| err.error_code())
                .unwrap_or("UNKNOWN_ERROR");
            metrics.record_query_error(error_code);
        }

        result
    }

    #[tracing::instrument(skip_all, err)]
    pub async fn execute_plan(
        &self,
        catalog: Catalog,
        dataset_store: &DatasetStore,
        plan: DetachedLogicalPlan,
        is_streaming: bool,
        resume_watermark: Option<ResumeWatermark>,
    ) -> Result<QueryResultStream, Error> {
        let query_start_time = std::time::Instant::now();
        let schema: SchemaRef = plan.schema().as_ref().clone().into();

        // If not streaming or metadata db is not available, execute once
        if !is_streaming {
            let ctx = QueryContext::for_catalog(catalog, self.env.clone(), false)
                .await
                .map_err(Error::CoreError)?;
            let plan = plan.attach_to(&ctx).map_err(Error::CoreError)?;

            let block_range = ctx
                .common_range(&plan)
                .await
                .map_err(|e| Error::CoreError(CoreError::DatasetError(e)))?;

            if let Some(ref range) = block_range {
                tracing::debug!("execute range [{}-{}]", range.start(), range.end());
            }

            let record_batches = ctx
                .execute_plan(plan, true)
                .await
                .map_err(Error::CoreError)?;
            let stream = QueryResultStream::NonIncremental {
                stream: NonEmptyRecordBatchStream::new(record_batches).boxed(),
                schema,
                block_range,
            };

            if let Some(metrics) = &self.metrics {
                Ok(track_query_metrics(stream, metrics, query_start_time))
            } else {
                Ok(stream)
            }
        } else {
            // is_incremental returns an error if query contains DDL, DML, etc.
            if let Err(e) = plan.is_incremental() {
                return Err(Error::InvalidQuery(format!(
                    "non-incremental queries are not supported for streaming, {e}",
                )));
            }

            // As an optimization, start the stream from the minimum start block across all tables.
            // Otherwise starting from `0` would spend time scanning ranges known to be empty.
            let earliest_block = catalog
                .earliest_block()
                .await
                .map_err(|err| Error::StreamingExecutionError(err.to_string()))?;

            // If no tables are synced, we return an empty stream.
            let Some(earliest_block) = earliest_block else {
                let empty_stream = RecordBatchStreamAdapter::new(schema.clone(), stream::empty());
                return Ok(QueryResultStream::NonIncremental {
                    stream: Box::pin(empty_stream),
                    schema,
                    block_range: None,
                });
            };

            let query = StreamingQuery::spawn(
                self.metadata_db.clone(),
                self.env.clone(),
                catalog,
                dataset_store,
                plan,
                earliest_block,
                None,
                resume_watermark,
                &self.notification_multiplexer,
                None,
                self.config.server_microbatch_max_interval,
                self.config.keep_alive_interval,
            )
            .await
            .map_err(|err| Error::StreamingExecutionError(err.to_string()))?;

            let stream = QueryResultStream::Incremental {
                stream: query
                    .as_stream()
                    .map_err(|err| Error::StreamingExecutionError(err.to_string()))
                    .boxed(),
                schema,
            };

            if let Some(metrics) = &self.metrics {
                Ok(track_query_metrics(stream, metrics, query_start_time))
            } else {
                Ok(stream)
            }
        }
    }
}

impl Service {
    #[instrument(skip(self))]
    async fn get_flight_info(
        &self,
        descriptor: FlightDescriptor,
        resume_watermark: Option<ResumeWatermark>,
        streaming_override: Option<bool>,
    ) -> Result<FlightInfo, Error> {
        let (ticket, schema) = match DescriptorType::try_from(descriptor.r#type)
            .map_err(|err| Error::PbDecodeError(err.to_string()))?
        {
            DescriptorType::Cmd => {
                let msg = Any::decode(descriptor.cmd.as_ref())?;
                if let Some(sql_query) = msg
                    .unpack::<CommandStatementQuery>()
                    .map_err(|err| Error::PbDecodeError(err.to_string()))?
                {
                    // The magic that turns a SQL string into a DataFusion logical plan that can be
                    // sent back over the wire:
                    // - Validate SQL string (non-empty, meaningful content)
                    // - Parse the SQL query.
                    // - Infer depedencies and collect them into a catalog.
                    // - Build a DataFusion query context with empty tables from that catalog.
                    // - Use that context to plan the SQL query.
                    // - Serialize the plan to bytes using datafusion-protobufs.

                    // Validate SQL string (non-empty, meaningful content)
                    let sql_str = sql_query
                        .query
                        .parse::<SqlStr>()
                        .map_err(|err| Error::InvalidQuery(err.to_string()))?;
                    let query = common::sql::parse(&sql_str)
                        .map_err(|err| Error::CoreError(CoreError::SqlParseError(err)))?;
                    let plan_ctx = planning_ctx_for_sql(&self.dataset_store, &query)
                        .await
                        .map_err(Error::PlanningCtxForSqlError)?;
                    let is_streaming = streaming_override
                        .unwrap_or_else(|| common::stream_helpers::is_streaming(&query));
                    let schema = plan_ctx
                        .sql_output_schema(query)
                        .await
                        .map_err(Error::CoreError)?;
                    let ticket = AmpTicket {
                        query: sql_query.query,
                        is_streaming,
                        resume_watermark: resume_watermark.map(Into::into),
                    };
                    (ticket, schema)
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

        let ticket = Bytes::from(
            bincode::encode_to_vec(&ticket, bincode::config::standard()).map_err(|err| {
                Error::TicketEncodingError(DataFusionError::Plan(format!(
                    "Failed to serialize remote plan: {}",
                    err
                )))
            })?,
        );
        let endpoint = FlightEndpoint {
            ticket: Some(Ticket::new(ticket)),

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
    async fn do_get(&self, ticket: Ticket) -> Result<TonicStream<FlightData>, Error> {
        let (ticket, _) =
            bincode::decode_from_slice::<AmpTicket, _>(&ticket.ticket, bincode::config::standard())
                .map_err(|err| {
                    Error::TicketDecodingError(DataFusionError::Plan(format!(
                        "Failed to deserialize remote plan: {}",
                        err
                    )))
                })?;

        // Validate SQL string (non-empty, meaningful content)
        let sql_str = ticket
            .query
            .parse::<SqlStr>()
            .map_err(|err| Error::InvalidQuery(err.to_string()))?;

        let stream = self
            .execute_query(
                &sql_str,
                Some(ticket.is_streaming),
                ticket.resume_watermark.map(Into::into),
            )
            .await?;

        Ok(flight_data_stream(stream))
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
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
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
        Err(Status::unimplemented("list_flights"))
    }

    #[instrument(skip(self, request), fields(request_id = tracing::field::Empty))]
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Create a request ID using the Cloudflare ray-id or generate random hex string if not
        // present.
        let request_id = request
            .metadata()
            .get("cf-ray")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{:016x}", rand::random::<u64>()));
        tracing::Span::current().record("request_id", &request_id);

        let resume_watermark = request
            .metadata()
            .get("amp-resume")
            .and_then(|v| serde_json::from_slice(v.as_bytes()).ok());

        // Parse amp-stream header to allow controlling streaming via header instead of SQL
        let streaming_override = request
            .metadata()
            .get("amp-stream")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| match s.to_lowercase().as_str() {
                "true" | "1" => Some(true),
                "false" | "0" => Some(false),
                _ => None,
            });

        let descriptor = request.into_inner();
        let info = self
            .get_flight_info(descriptor, resume_watermark, streaming_override)
            .await?;
        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    #[instrument(skip(self, request), fields(request_id = tracing::field::Empty))]
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // Create a request ID using the Cloudflare ray-id or generate random hex string if not
        // present.
        let request_id = request
            .metadata()
            .get("cf-ray")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{:016x}", rand::random::<u64>()));
        tracing::Span::current().record("request_id", &request_id);

        let ticket = request.into_inner();
        let data_stream = self.do_get(ticket).await?;
        Ok(Response::new(data_stream))
    }

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_action(
        &self,
        _request: Request<arrow_flight::Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }
}

// Flight service ticket
#[derive(bincode::Encode, bincode::Decode)]
pub struct AmpTicket {
    pub query: String,
    pub is_streaming: bool,
    pub resume_watermark: Option<BTreeMap<String, (BlockNum, [u8; 32])>>,
}

pub enum QueryResultStream {
    NonIncremental {
        stream: BoxStream<'static, Result<RecordBatch, DataFusionError>>,
        schema: SchemaRef,
        block_range: Option<BlockRange>,
    },
    Incremental {
        stream: BoxStream<'static, Result<QueryMessage, Error>>,
        schema: SchemaRef,
    },
}

impl QueryResultStream {
    pub fn record_batches(self) -> BoxStream<'static, Result<RecordBatch, Error>> {
        match self {
            Self::NonIncremental {
                stream,
                schema: _,
                block_range: _,
            } => stream
                .map_err(|err| Error::StreamingExecutionError(err.to_string()))
                .boxed(),
            Self::Incremental { stream, schema: _ } => stream
                .filter_map(async |result| match result {
                    Err(err) => Some(Err(err)),
                    Ok(QueryMessage::Data(batch)) => Some(Ok(batch)),
                    Ok(_) => None,
                })
                .boxed(),
        }
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
    metrics: &Arc<MetricsRegistry>,
    start_time: std::time::Instant,
) -> QueryResultStream {
    let metrics = metrics.clone();

    match stream {
        QueryResultStream::NonIncremental {
            stream: record_batch_stream,
            schema,
            block_range,
        } => {
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

            QueryResultStream::NonIncremental {
                stream: wrapped_stream.boxed(),
                schema,
                block_range,
            }
        }
        QueryResultStream::Incremental {
            stream: message_stream,
            schema,
        } => {
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

            QueryResultStream::Incremental {
                stream: wrapped,
                schema,
            }
        }
    }
}

fn flight_data_stream(query_result_stream: QueryResultStream) -> TonicStream<FlightData> {
    match query_result_stream {
        QueryResultStream::NonIncremental {
            stream: non_incremental_stream,
            schema,
            block_range,
        } => {
            // Use manual encoding for NonIncremental streams to preserve empty batches
            // (arrow-flight's FlightDataEncoderBuilder filters them out)
            stream! {
                let mut dictionary_tracker = DictionaryTracker::new(true);

                // Send schema first
                let schema_message = FlightData::from(SchemaAsIpc::new(&schema, &IpcWriteOptions::default()));
                yield Ok(schema_message);

                // Prepare metadata for data batches (ranges_complete: false)
                let data_metadata = block_range.as_ref().map(|range| {
                    json!({
                        "ranges": [range],
                        "ranges_complete": false,
                    })
                });

                // Encode each data batch with ranges_complete: false
                let mut non_incremental_stream = non_incremental_stream;
                while let Some(result) = non_incremental_stream.next().await {
                    match result {
                        Ok(batch) => {
                            match encode_record_batch(batch, data_metadata.as_ref(), &mut dictionary_tracker) {
                                Ok(encoded) => {
                                    for message in encoded {
                                        yield Ok(message);
                                    }
                                }
                                Err(err) => {
                                    yield Err(err);
                                    return;
                                }
                            }
                        }
                        Err(err) => {
                            yield Err(Error::ExecutionError(err).into());
                            return;
                        }
                    }
                }

                // Send final empty batch with ranges_complete: true (if we have a block_range)
                if let Some(range) = block_range {
                    let empty_batch = RecordBatch::new_empty(schema.clone());
                    let completion_metadata = json!({
                        "ranges": [range],
                        "ranges_complete": true,
                    });
                    match encode_record_batch(empty_batch, Some(&completion_metadata), &mut dictionary_tracker) {
                        Ok(encoded) => {
                            for message in encoded {
                                yield Ok(message);
                            }
                        }
                        Err(err) => {
                            yield Err(err);
                            return;
                        }
                    }
                }
            }
            .boxed()
        }
        QueryResultStream::Incremental {
            stream: mut incremental_stream,
            schema,
        } => {
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
                        match encode_record_batch(batch, Some(&app_metadata), &mut dictionary_tracker) {
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
                        assert!(!ranges.is_empty());
                        let empty_batch = RecordBatch::new_empty(schema.clone());
                        let app_metadata = json!({
                            "ranges": &ranges,
                            "ranges_complete": true,
                        });
                        match encode_record_batch(empty_batch, Some(&app_metadata), &mut dictionary_tracker) {
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
    }
}

#[expect(clippy::result_large_err)]
pub fn encode_record_batch(
    batch: RecordBatch,
    app_metadata: Option<&serde_json::Value>,
    dictionary_tracker: &mut DictionaryTracker,
) -> Result<Vec<FlightData>, Status> {
    let ipc = IpcDataGenerator::default();
    let options = IpcWriteOptions::default();
    let mut encoded: Vec<FlightData> = Default::default();
    let app_metadata_string = app_metadata.map(|m| serde_json::to_string(m).unwrap());

    for batch in split_batch_for_grpc_response(batch, GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES) {
        let (encoded_dictionaries, encoded_batch) = ipc
            .encoded_batch(&batch, dictionary_tracker, &options)
            .map_err(FlightError::from)?;
        for encoded_dictionary in encoded_dictionaries {
            encoded.push(encoded_dictionary.into());
        }

        let mut flight_data = FlightData::from(encoded_batch);
        if let Some(ref metadata) = app_metadata_string {
            flight_data = flight_data.with_app_metadata(metadata.clone());
        }
        encoded.push(flight_data);
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
        let length = rows_per_batch.min(batch.num_rows() - offset);
        out.push(batch.slice(offset, length));

        offset += length;
    }

    out
}

/// Errors that can occur during query execution
#[derive(Error, Debug)]
#[expect(clippy::enum_variant_names)]
pub enum Error {
    #[error("ProtocolBuffers decoding error: {0}")]
    PbDecodeError(String),

    #[error("unsupported flight descriptor type: {0}")]
    UnsupportedFlightDescriptorType(String),

    #[error("unsupported flight descriptor command: {0}")]
    UnsupportedFlightDescriptorCommand(String),

    #[error("query execution error")]
    ExecutionError(#[source] DataFusionError),

    #[error("error looking up datasets")]
    DatasetStoreError(#[source] GetDatasetError),

    #[error("error loading catalog for SQL")]
    CatalogForSqlError(#[source] CatalogForSqlError),

    #[error("error loading physical catalog")]
    GetPhysicalCatalogError(#[source] GetPhysicalCatalogError),

    #[error("error creating planning context")]
    PlanningCtxForSqlError(#[source] PlanningCtxForSqlError),

    #[error(transparent)]
    CoreError(CoreError),

    #[error("invalid query: {0}")]
    InvalidQuery(String),

    #[error("streaming query execution error: {0}")]
    StreamingExecutionError(String),

    #[error("ticket encoding error")]
    TicketEncodingError(#[source] DataFusionError),

    #[error("ticket decoding error")]
    TicketDecodingError(#[source] DataFusionError),
}

impl Error {
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
            Error::CoreError(CoreError::DatasetError(_)) => "DATASET_ERROR",
            Error::CoreError(CoreError::ConfigError(_)) => "CONFIG_ERROR",
            Error::CoreError(CoreError::PlanningError(_)) => "PLANNING_ERROR",
            Error::CoreError(CoreError::ExecutionError(_)) => "CORE_EXECUTION_ERROR",
            Error::CoreError(CoreError::MetaTableError(_)) => "META_TABLE_ERROR",
            Error::CoreError(CoreError::TableNotFoundError(_)) => "TABLE_NOT_FOUND_ERROR",
            Error::InvalidQuery(_) => "INVALID_QUERY",
            Error::StreamingExecutionError(_) => "STREAMING_EXECUTION_ERROR",
            Error::TicketEncodingError(_) => "TICKET_ENCODING_ERROR",
            Error::TicketDecodingError(_) => "TICKET_DECODING_ERROR",
        }
    }
}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        Error::PbDecodeError(e.to_string())
    }
}

fn datafusion_error_to_status(e: &DataFusionError, message: String) -> Status {
    match e {
        DataFusionError::ResourcesExhausted(_) => Status::resource_exhausted(message),
        _ => Status::internal(message),
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status_code = match self {
            Error::CoreError(CoreError::InvalidPlan(_) | CoreError::SqlParseError(_)) => {
                StatusCode::BAD_REQUEST
            }
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
            Error::TicketEncodingError(_) => StatusCode::BAD_REQUEST,
            Error::TicketDecodingError(_) => StatusCode::BAD_REQUEST,
        };
        let res = json!({
            "error_code": self.error_code(),
            "error_message": error_with_causes(&self)
        });

        (status_code, res.to_string()).into_response()
    }
}

impl From<Error> for Status {
    fn from(e: Error) -> Self {
        let message = error_with_causes(&e);
        match &e {
            Error::PbDecodeError(_) => Status::invalid_argument(message),
            Error::UnsupportedFlightDescriptorType(_) => Status::invalid_argument(message),
            Error::UnsupportedFlightDescriptorCommand(_) => Status::invalid_argument(message),
            Error::DatasetStoreError(_) => Status::internal(message),

            Error::CoreError(CoreError::InvalidPlan(_)) => Status::invalid_argument(message),
            Error::CoreError(CoreError::SqlParseError(_)) => Status::invalid_argument(message),
            Error::CoreError(CoreError::DatasetError(_)) => Status::internal(message),
            Error::CoreError(CoreError::ConfigError(_)) => Status::internal(message),
            Error::CoreError(CoreError::TableNotFoundError(_)) => Status::not_found(message),

            Error::CoreError(
                CoreError::PlanningError(df)
                | CoreError::ExecutionError(df)
                | CoreError::MetaTableError(df),
            ) => datafusion_error_to_status(df, message),

            Error::ExecutionError(df) => datafusion_error_to_status(df, message),
            Error::StreamingExecutionError(_) => Status::internal(message),
            Error::CatalogForSqlError(_) => Status::internal(message),
            Error::GetPhysicalCatalogError(_) => Status::internal(message),
            Error::PlanningCtxForSqlError(_) => Status::internal(message),
            Error::InvalidQuery(_) => Status::invalid_argument(message),
            Error::TicketEncodingError(_) => Status::invalid_argument(message),
            Error::TicketDecodingError(_) => Status::invalid_argument(message),
        }
    }
}

/// Build the Arrow Flight gRPC router
///
/// Creates an axum router with the Flight service integrated via tonic Routes.
pub fn build_router(service: Service) -> Router {
    let mut grpc_builder = Routes::builder();
    grpc_builder.add_service(FlightServiceServer::new(service));
    grpc_builder.routes().into_axum_router()
}
