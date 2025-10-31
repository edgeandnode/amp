use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::AtomicI64},
};

use adbc_core::{
    Optionable,
    options::{IngestMode, OptionStatement, OptionValue},
};
use amp_client::{InvalidationRange, Metadata, ResponseBatchWithReorg, SqlClient};
use common::{
    PlanningContext, QueryContext, SPECIAL_BLOCK_NUM,
    arrow::{
        array::{Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray, TimestampMicrosecondArray},
        compute,
        datatypes::{Field, Int64Type},
    },
    metadata::segments::{ResumeWatermark, Watermark},
    query_context::parse_sql,
};
use datafusion_expr::{BinaryExpr, Expr, LogicalPlan, LogicalPlanBuilder, col};
use datafusion_sql::{parser::Statement as QueryStatement, unparser::plan_to_sql};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use sqlx::types::chrono;
use tokio::{
    sync::broadcast::Sender,
    task::{JoinHandle, JoinSet},
};
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    Connection, ConnectionExt, Error, Schema, SchemaExt, Statement,
    adbc::{AdbcConnection, AdbcStatement},
    arrow::{
        RecordBatch, Schema as ArrowSchema,
        array::{AsArray, Int64Array, StringArray},
        datatypes::{DataType, Decimal128Type},
    },
    config::AmpdbcConfig,
    datafusion::ScalarValue,
    error::Result,
    schema::TableRef,
};

/// Approximate size of Parquet files written during ingestion. Actual size will be 
/// slightly larger, depending on size of footer/metadata. Default is 10 MB. If set to 
/// 0, file size has no limit. Cannot be negative.
pub const INGEST_TARGET_FILE_SIZE_OPT: &str = "adbc.snowflake.statement.ingest_target_file_size";
/// Maximum number of COPY operations to run concurrently. Bulk ingestion performance is 
/// optimized by executing COPY queries as files are still being uploaded. Snowflake COPY 
/// speed scales with warehouse size, so smaller warehouses may benefit from setting this 
/// value higher to ensure long-running COPY queries do not block newly uploaded files from 
/// being loaded. Default is 4. If set to 0, only a single COPY query will be executed as 
/// part of ingestion, once all files have finished uploading. Cannot be negative.
pub const INGEST_COPY_CONCURRENCY_OPT: &str = "adbc.snowflake.statement.ingest_copy_concurrency";
/// Number of Parquet files to upload in parallel. Greater concurrency can smooth out TCP congestion
/// and help make use of available network bandwith, but will increase memory utilization. 
/// Default is 8. If set to 0, default value is used. Cannot be negative.
pub const INGEST_UPLOAD_CONCURRENCY_OPT: &str = "adbc.snowflake.statement.ingest_upload_concurrency";
///Number of Parquet files to write in parallel. Default attempts to maximize workers based 
/// on logical cores detected, but may need to be adjusted if running in a constrained environment. 
/// If set to 0, default value is used. Cannot be negative.
pub const INGEST_WRITER_CONCURRENCY_OPT: &str = "adbc.snowflake.statement.ingest_writer_concurrency";

pub struct Pipe {
    pub connection: Connection,
    pub schema: Schema,
    pub watermark: Option<ResumeWatermark>,
    pub transfer_id: Arc<AtomicI64>,
    pub data_handle: Option<JoinHandle<Result<()>>>,
    pub data_stream: Option<BroadcastStream<(RecordBatch, Metadata)>>,
    pub reorg_handle: Option<JoinHandle<Result<()>>>,
    pub reorg_stream: Option<BroadcastStream<Arc<[InvalidationRange]>>>,
    pub watermark_handle: Option<JoinHandle<Result<()>>>,
    pub watermark_stream: Option<BroadcastStream<Arc<ResumeWatermark>>>,
}

impl Pipe {
    pub fn new((schema, connection): (Schema, Connection)) -> Self {
        let transfer_id = Arc::new(AtomicI64::new(0));
        Self {
            connection,
            schema,
            watermark: None,
            transfer_id,
            data_handle: None,
            data_stream: None,
            reorg_handle: None,
            reorg_stream: None,
            watermark_handle: None,
            watermark_stream: None,
        }
    }

    pub fn build_tasks(&mut self, join_set: &mut JoinSet<Result<()>>) -> Result<()> {
        let connection = &mut self.connection;
        // Update the current transfer ID
        let mut current_transfer_id_stmt = connection.create_statement()?;
        let transfer_history_schema = self.schema.as_transfer_history();
        let transfer_history_table = transfer_history_schema.table_ref().name();
        current_transfer_id_stmt.set_sql_query(format!(
            "SELECT MAX(transfer_id) FROM {}",
            transfer_history_table
        ))?;
        let cursor = current_transfer_id_stmt.execute()?;

        let current_transfer_id = cursor
            .into_iter()
            .filter_map(|batch| {
                batch
                    .map_err(Error::stream_read)
                    .and_then(|b| {
                        b.column(0)
                            .as_primitive_opt::<Decimal128Type>()
                            .ok_or_else(|| {
                                tracing::warn!(
                                    "Transfer ID column is not INTEGER in table {}. Expected Decimal128Type(38, 0). Found {:?}",
                                    transfer_history_table,
                                    b.column(0).data_type(),
                                );
                                Error::type_mismatch(
                                    transfer_history_table.clone(),
                                    "transfer_id".to_string(),
                                    DataType::Int64,
                                    b.column(0).data_type().clone(),
                                )
                            })
                            .map(|array| compute::max(&array))
                    })
                    .ok()
                    .flatten()
            })
            .max()
            .unwrap_or(0) as i64;

        self.transfer_id
            .fetch_max(current_transfer_id, std::sync::atomic::Ordering::SeqCst);

        if let Some(data_stream) = self.data_stream.take() {
            let schema = &self.schema;
            let transfer_id_state = Arc::clone(&self.transfer_id);
            join_set.spawn({
                let mut connection = connection.clone();
                let data_schema = schema.clone();
                let history_schema = data_schema.as_transfer_history();
                async move {
                    tokio::pin!(data_stream);

                    let mut enriched_stream = data_stream.map_err(Error::stream_recv);

                    while let Some((mut batch, metadata)) = enriched_stream.try_next().await? {
                        let transfer_id =
                            transfer_id_state.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        batch.include_transfer_id(transfer_id)?;

                        let metadata_batch = metadata.as_record_batch(
                            &history_schema,
                            "INSERT",
                            transfer_id,
                        )?;

                        let mut insert_stmt = connection
                            .new_statement()
                            .inspect_err(|e| tracing::error!("141 {e}"))?;

                        insert_stmt
                            .set_option(
                                OptionStatement::TargetTable,
                                OptionValue::String(data_schema.table_ref().name()),
                            )
                            .inspect_err(|e| tracing::error!("148 {e}"))?;

                        insert_stmt
                            .set_option(OptionStatement::IngestMode, IngestMode::Append.into())
                            .inspect_err(|e| tracing::error!("156 {e}"))?;

                        insert_stmt
                            .set_option(OptionStatement::Other(INGEST_COPY_CONCURRENCY_OPT.into()), OptionValue::Int(0)) 
                            .inspect_err(|e| tracing::error!("364 {e}"))?;

                        insert_stmt
                            .set_option(OptionStatement::Other(INGEST_UPLOAD_CONCURRENCY_OPT.into()), OptionValue::Int(64)) 
                            .inspect_err(|e| tracing::error!("364 {e}"))?;

                        insert_stmt
                            .bind(batch)
                            .inspect_err(|e| tracing::error!("152 {e}"))?;
                        

                        // We now have a transaction open, so we need to make sure to rollback on errors

                        if let Some(rows_affected @ 1..) = insert_stmt
                            .execute_update()
                            .inspect_err(|e| tracing::error!("158 {e}"))?
                        // .map_err(Error::rollback(
                        //     &mut connection,
                        //     format!(
                        //         "Failed to execute insert statement for {}",
                        //         data_schema.table_ref().name()
                        //     ),
                        // ))?
                        {
                            tracing::info!(
                                "Inserted {} rows into table {}",
                                rows_affected,
                                data_schema.table_ref().name()
                            );
                        }

                        let mut insert_stmt = connection
                            .new_statement()
                            .inspect_err(|e| tracing::error!("176 {e}"))?;
                        // .map_err(Error::rollback(
                        //     &mut connection,
                        //     "Failed to create history insert statement",
                        // ))?;

                        insert_stmt
                            .set_option(
                                OptionStatement::TargetTable,
                                OptionValue::String(history_schema.table_ref().name()),
                            )
                            .inspect_err(|e| tracing::error!("187 {e}"))?;
                        // .map_err(Error::rollback(
                        //     &mut connection,
                        //     "Failed to set history insert statement target table",
                        // ))?;

                        insert_stmt
                            .set_option(OptionStatement::IngestMode, IngestMode::Append.into())
                            .inspect_err(|e| tracing::error!("203 {e}"))?;

                        insert_stmt
                            .bind(metadata_batch)
                            .inspect_err(|e| tracing::error!("195 {e}"))?;
                        // .map_err(Error::rollback(
                        //     &mut connection,
                        //     "Failed to bind metadata batch to history insert statement",
                        // ))?;

                        if let Some(rows_affected @ 1..) = insert_stmt.execute_update()?
                        // .map_err(Error::rollback(
                        //     &mut connection,
                        //     "Failed to execute history insert statement",
                        // ))?
                        {
                            tracing::info!(
                                "Inserted {} rows into history table {}",
                                rows_affected,
                                history_schema.table_ref().name()
                            );
                        }

                        // connection.commit().map_err(Error::rollback(
                        //     &mut connection,
                        //     "Failed to commit transaction",
                        // ))?;
                    }

                    Ok::<(), Error>(())
                }
            });
        }

        if let Some(reorg_stream) = self.reorg_stream.take() {
            let transfer_id_state = Arc::clone(&self.transfer_id);

            join_set.spawn({
                let mut connection = self.connection.clone();
                let reorg_schema = self.schema.as_transfer_history().clone();

                async move {
                    tokio::pin!(reorg_stream);

                    while let Some(invalidation_ranges) = reorg_stream
                        .next()
                        .await
                        .transpose()
                        .map_err(Error::stream_recv)?
                    {
                        let transfer_id =
                            transfer_id_state.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                        let reorg_batch = invalidation_ranges.as_record_batch(
                            &reorg_schema,
                            "REORG",
                            transfer_id,
                        )?;

                        let mut insert_stmt = connection
                            .new_statement()
                            .inspect_err(|e| tracing::error!("265 {e}"))?;
                        // .map_err(Error::rollback(
                        //     &mut connection,
                        //     "Failed to create reorg insert statement",
                        // ))?;

                        insert_stmt
                            .set_option(
                                OptionStatement::TargetTable,
                                OptionValue::String(reorg_schema.table_ref().name()),
                            )
                            .inspect_err(|e| tracing::error!("276 {e}"))?;
                        // .map_err(Error::rollback(
                        //     &mut connection,
                        //     format!(
                        //         "Failed to set target table for reorg insert statement: {}",
                        //         reorg_schema.table_ref().name()
                        //     ),
                        // ))?;

                        insert_stmt
                            .set_option(OptionStatement::IngestMode, IngestMode::Append.into())
                            .inspect_err(|e| tracing::error!("287 {e}"))?;

                        insert_stmt
                            .bind(reorg_batch)
                            .inspect_err(|e| tracing::error!("{e}"))?;
                        // .map_err(Error::rollback(
                        //     &mut connection,
                        //     "Failed to bind reorg batch to history insert statement",
                        // ))?;

                        if let Some(rows_affected @ 1..) = insert_stmt
                            .execute_update()
                            .inspect_err(|e| tracing::error!("{e}"))?
                        // .map_err(Error::rollback(
                        //     &mut connection,
                        //     "Failed to execute history insert statement",
                        // ))?
                        {
                            tracing::info!(
                                "Inserted {} rows into history table {}",
                                rows_affected,
                                reorg_schema.table_ref().name()
                            );
                        }

                        // connection
                        //     .commit()
                        //     .inspect_err(|e| tracing::error!("{e}"))
                        //     .map_err(Error::rollback(
                        //         &mut connection,
                        //         "Failed to commit transaction",
                        //     ))?;
                    }

                    Ok::<(), Error>(())
                }
            });
        }

        if let Some(watermark_stream) = self.watermark_stream.take() {
            join_set.spawn({
                let mut connection = self.connection.clone();
                let watermark_schema = self.schema.as_watermark().clone();
                let transfer_id_state = Arc::clone(&self.transfer_id);

                async move {
                    tokio::pin!(watermark_stream);
                    
                    while let Some(watermark) = watermark_stream
                    .try_next()
                    .await
                    .map_err(Error::stream_recv)?
                    {
                        let transfer_id = transfer_id_state.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let watermark_batch =
                        watermark.as_record_batch(&watermark_schema, "WATERMARK", transfer_id)?;
                        
                        let mut insert_stmt = connection
                        .new_statement()
                            .inspect_err(|e| tracing::error!("327 {e}"))?;
                        // .map_err(Error::rollback(
                        //     &mut connection,
                        //     "Failed to create new statement for watermark insert",
                        // ))?;

                        insert_stmt
                            .set_option(
                                OptionStatement::TargetTable,
                                OptionValue::String(watermark_schema.table_ref().name()),
                            )
                            .inspect_err(|e| tracing::error!("338 {e}"))?;
                        // .map_err(Error::rollback(
                        //     &mut connection,
                        //     "Failed to set target table for watermark insert statement",
                        // ))?;

                        insert_stmt
                            .set_option(OptionStatement::IngestMode, IngestMode::Append.into())
                            .inspect_err(|e| tracing::error!("363 {e}"))?;
                        insert_stmt
                            .set_option(OptionStatement::Other(INGEST_COPY_CONCURRENCY_OPT.into()), OptionValue::Int(0)) 
                            .inspect_err(|e| tracing::error!("364 {e}"))?;
                        insert_stmt
                            .set_option(OptionStatement::Other(INGEST_UPLOAD_CONCURRENCY_OPT.into()), OptionValue::Int(64)) 
                            .inspect_err(|e| tracing::error!("364 {e}"))?;


                        insert_stmt
                            .bind(watermark_batch)
                            .inspect_err(|e| tracing::error!("368 {e}"))?;
                        // .map_err(Error::rollback(
                        //     &mut connection,
                        //     "Failed to bind watermark batch to insert statement",
                        // ))?;

                        if let Some(rows_affected @ 1..) = insert_stmt
                            .execute_update()
                            .inspect_err(|e| tracing::error!("354 {e}"))?
                        // .map_err(Error::rollback(
                        //     &mut connection,
                        //     "Failed to execute watermark insert statement",
                        // ))?
                        {
                            tracing::info!(
                                "Inserted {} rows into watermark table {}",
                                rows_affected,
                                watermark_schema.table_ref().name()
                            );
                        }

                        // connection
                        //     .commit()
                        //     .inspect_err(|e| tracing::error!("{e}"))
                        //     .map_err(Error::rollback(
                        //         &mut connection,
                        //         "Failed to commit transaction",
                        //     ))?;
                    }

                    Ok::<(), Error>(())
                }
            });
        }
        Ok(())
    }

    fn reset_streams(
        &mut self,
        data_tx: &Sender<(RecordBatch, Metadata)>,
        reorg_tx: &Sender<Arc<[InvalidationRange]>>,
        watermark_tx: &Sender<Arc<ResumeWatermark>>,
        watermark: Option<ResumeWatermark>,
    ) -> Result<()> {
        let data_rx = data_tx.subscribe();
        let watermark_rx = watermark_tx.subscribe();
        let reorg_rx = reorg_tx.subscribe();
        let _ = std::mem::replace(&mut self.watermark, watermark);
        let _ = std::mem::replace(&mut self.data_stream, Some(BroadcastStream::new(data_rx)));
        let _ = std::mem::replace(&mut self.reorg_stream, Some(BroadcastStream::new(reorg_rx)));
        let _ = std::mem::replace(
            &mut self.watermark_stream,
            Some(BroadcastStream::new(watermark_rx)),
        );
        Ok(())
    }

    pub fn ddl_statements(
        &mut self,
    ) -> Result<(
        (Statement, Schema),
        (Statement, Schema),
        (Statement, Schema),
    )> {
        let connection = &mut self.connection;

        let data_schema = self.schema.clone();
        let history_schema = data_schema.as_transfer_history();
        let watermark_schema = data_schema.as_watermark();

        let data_table = connection.create_table_ddl_statement(&data_schema)?;

        let history_table = connection.create_table_ddl_statement(&history_schema)?;

        let watermark_table = connection.create_table_ddl_statement(&watermark_schema)?;

        Ok((
            (data_table, data_schema),
            (history_table, history_schema),
            (watermark_table, watermark_schema),
        ))
    }

    fn truncate_statements(
        &mut self,
    ) -> Result<(
        (Statement, TableRef),
        (Statement, TableRef),
        (Statement, TableRef),
    )> {
        let connection = &mut self.connection;
        let data_schema = self.schema.clone();
        let history_schema = data_schema.as_transfer_history();
        let watermark_schema = data_schema.as_watermark();
        let mut data_table = connection.create_statement()?;
        let mut history_table = connection.create_statement()?;
        let mut watermark_table = connection.create_statement()?;
        data_table.set_sql_query(data_schema.as_truncate_stmt()?)?;
        history_table.set_sql_query(history_schema.as_truncate_stmt()?)?;
        watermark_table.set_sql_query(watermark_schema.as_truncate_stmt()?)?;
        let data_table_ref = data_schema.table_ref().clone();
        let history_table_ref = history_schema.table_ref().clone();
        let watermark_table_ref = watermark_schema.table_ref().clone();
        Ok((
            (data_table, data_table_ref),
            (history_table, history_table_ref),
            (watermark_table, watermark_table_ref),
        ))
    }

    pub fn truncate_tables(&mut self) -> Result<()> {
        // self.connection
        //     .set_option(OptionConnection::AutoCommit, "true".into())?;

        let (
            (mut truncate_data, data_table_ref),
            (mut truncate_history, history_table_ref),
            (mut truncate_watermark, watermark_table_ref),
        ) = self.truncate_statements()?;

        if let Some(rows_affected) = truncate_data.execute_update()? {
            tracing::info!(
                "Truncated table {}, rows affected: {}",
                data_table_ref.name(),
                rows_affected
            );
        }

        if let Some(rows_affected) = truncate_history.execute_update()? {
            tracing::info!(
                "Truncated table {}, rows affected: {}",
                history_table_ref.name(),
                rows_affected
            );
        }

        if let Some(rows_affected) = truncate_watermark.execute_update()? {
            tracing::info!(
                "Truncated table {}, rows affected: {}",
                watermark_table_ref.name(),
                rows_affected
            );
        }
        // self.connection.commit().map_err(Error::rollback(
        //     &mut self.connection,
        //     "Unable to commit after truncating tables",
        // ))?;

        Ok(())
    }
}

pub type Pipes = Vec<Pipe>;

pub struct QueryPipeline {
    pub client: SqlClient,
    pub context: QueryContext,
    pub pipes: Pipes,
    pub pipe_tasks: JoinSet<Result<()>>,
    pub plan: LogicalPlan,
    pub statement: QueryStatement,
    pub resume_point: ResumePoint,
    pub data_tx: Sender<(RecordBatch, Metadata)>,
    pub reorg_tx: Sender<Arc<[InvalidationRange]>>,
    pub watermark_tx: Sender<Arc<ResumeWatermark>>,
    pub stream_broadcast_task: Option<JoinHandle<Result<()>>>,
}

impl QueryPipeline {
    pub async fn try_new(config: &mut AmpdbcConfig, sql: &str) -> Result<Self> {
        let connections = config.connections()?;

        let client = config.sql_client().await?;
        let dataset_store = config.dataset_store().await?;

        let statement = parse_sql(sql)?;

        let catalog = dataset_store
            .catalog_for_sql(&statement, config.query_env()?)
            .await?;

        let planning_context = PlanningContext::new(catalog.logical().clone());

        let detached_plan = planning_context.plan_sql(statement.clone()).await?;

        detached_plan.is_incremental().map_err(Error::incremental)?;

        let context = QueryContext::for_catalog(catalog, config.query_env()?, false).await?;

        let plan = detached_plan.attach_to(&context)?;

        let df_schema = plan.schema();
        let arrow_schema = df_schema.as_arrow();

        let schemas = config.schemas(arrow_schema);

        let pipes: Pipes = schemas.zip(connections).map(Pipe::new).collect();

        let (data_tx, _) = tokio::sync::broadcast::channel(1024);
        let (reorg_tx, _) = tokio::sync::broadcast::channel(1024);
        let (watermark_tx, _) = tokio::sync::broadcast::channel(1024);

        let resume_point = ResumePoint::default();
        let pipe_tasks = JoinSet::new();

        Ok(Self {
            client,
            context,
            statement,
            plan,
            pipes,
            pipe_tasks,
            data_tx,
            reorg_tx,
            watermark_tx,
            resume_point,
            stream_broadcast_task: None,
        })
    }

    pub fn initialize(&mut self) -> Result<()> {
        for pipe in &mut self.pipes {
            tracing::info!(
                "Creating tables for dataset {}",
                pipe.schema.table_ref().name()
            );
            let (
                (mut data_table_stmt, data_schema),
                (mut history_table_stmt, history_schema),
                (mut watermark_table_stmt, watermark_schema),
            ) = pipe.ddl_statements()?;

            if let Some(rows_affected) = data_table_stmt.execute_update()? {
                tracing::info!(
                    "Created data table {}, rows affected: {}",
                    data_schema.table_ref().full_name(),
                    rows_affected
                );
            } else {
                tracing::info!(
                    "Created data table {}",
                    data_schema.table_ref().full_name()
                );
            }

            if let Some(rows_affected) = history_table_stmt.execute_update()? {
                tracing::info!(
                    "Created history table {}, rows affected: {}",
                    history_schema.table_ref().full_name(),
                    rows_affected
                );
            } else {
                tracing::info!(
                    "Created history table {}",
                    history_schema.table_ref().full_name()
                );
            }

            if let Some(rows_affected) = watermark_table_stmt.execute_update()? {
                tracing::info!(
                    "Created watermark table {}, rows affected: {}",
                    watermark_schema.table_ref().full_name(),
                    rows_affected
                );
            } else {
                tracing::info!(
                    "Created watermark table {}",
                    watermark_schema.table_ref().full_name()
                );
            }
        }
        Ok(())
    }

    fn reset_streams(&mut self, watermark: Option<ResumeWatermark>) -> Result<()> {
        for pipe in &mut self.pipes {
            pipe.reset_streams(
                &self.data_tx,
                &self.reorg_tx,
                &self.watermark_tx,
                watermark.clone(),
            )?;
        }
        Ok(())
    }

    fn set_resume_point(&mut self) -> Result<()> {
        if !self.resume_point.is_unknown() {
            return Ok(());
        }
        // Unwrap: safe because there is always at least one pipe
        let pipe = self.pipes.first_mut().unwrap();
        let watermark_table = pipe.schema.as_watermark().table_ref().name();
        let history_table = pipe.schema.as_transfer_history().table_ref().name();
        // pipe.connection
        //     .set_option(OptionConnection::AutoCommit, "true".into())?;

        let mut watermark_stmt = pipe.connection.new_statement()?;

        watermark_stmt.set_sql_query(format!(
                "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY network ORDER BY transfer_id DESC) AS rn FROM {watermark_table}) WHERE rn = 1;"
            ))?;

        let mut cursor = watermark_stmt.execute()?;

        let resume_watermark = if let Some(Ok(batch)) = cursor.next()
            && batch.num_rows() > 0
        {
            let Some(id_array) = batch.column(0).as_primitive_opt::<Decimal128Type>() else {
                // pipe.connection.commit().map_err(Error::rollback(
                //     &mut pipe.connection,
                //     "Unable to commit after being unable to cast id column to Decimal128 in watermark table.",
                // ))?;

                return Err(Error::type_mismatch(
                    watermark_table,
                    "id".to_string(),
                    DataType::UInt64,
                    batch.column(0).data_type().clone(),
                ));
            };

            let network_array = batch.column(1).as_string::<i32>();

            let Some(block_num_array) = batch.column(2).as_primitive_opt::<Decimal128Type>() else {
                // pipe.connection.commit().map_err(Error::rollback(
                //     &mut pipe.connection,
                //     "Unable to commit after being unable to cast block_num column to Decimal128 in watermark table.",
                // ))?;

                return Err(Error::type_mismatch(
                    watermark_table,
                    "block_num".to_string(),
                    DataType::Decimal128(38, 0),
                    batch.column(2).data_type().clone(),
                ));
            };

            let Some(block_hash_array) = batch.column(3).as_binary_opt::<i32>() else {
                // pipe.connection.commit().map_err(Error::rollback(
                //     &mut pipe.connection,
                //     "Unable to commit after being unable to cast block_hash column to Binary in watermark table.",
                // ))?;

                return Err(Error::type_mismatch(
                    watermark_table,
                    "block_hash".to_string(),
                    DataType::Binary,
                    batch.column(3).data_type().clone(),
                ));
            };

            let mut missing = false;

            let watermarks = id_array
                .iter()
                .zip(network_array)
                .zip(block_num_array)
                .zip(block_hash_array)
                .fold(
                    BTreeMap::new(),
                    |mut map, (((id, network), block_num), block_hash)| {
                        if let (Some(_), Some(network), Some(block_num), Some(block_hash)) =
                            (id, network, block_num, block_hash)
                            && let Ok(block_hash) = block_hash.try_into()
                        {
                            let watermark = Watermark {
                                number: block_num as u64,
                                hash: block_hash,
                            };
                            map.insert(network.to_string(), watermark);
                        } else {
                            missing = true;
                        }
                        map
                    },
                );
            if missing {
                None
            } else {
                Some(ResumeWatermark(watermarks))
            }
        } else {
            // pipe.connection.commit().map_err(Error::rollback(
            //     &mut pipe.connection,
            //     "Unable to commit after being unable to fetch resume point",
            // ))?;

            self.resume_point = ResumePoint::None;
            return Ok(());
        };

        // pipe.connection.commit().map_err(Error::rollback(
        //     &mut pipe.connection,
        //     "Unable to commit after fetching resume watermark",
        // ))?;

        if let Some(resume_watermark) = resume_watermark {
            self.resume_point = ResumePoint::Watermark(resume_watermark);
        } else {
            let mut max_block_num_stmt = pipe.connection.new_statement()?;

            max_block_num_stmt.set_sql_query(format!(
                "SELECT network
                      , MAX(block_range_end)
                   FROM {history_table} 
               GROUP BY network
               ORDER BY 2 DESC
                  LIMIT 1;"
            ))?;

            let mut cursor = max_block_num_stmt.execute()?;

            let mut max_block_num: Option<u64> = None;
            let mut max_network: Option<String> = None;

            while let Some(batch) = cursor.next().transpose().map_err(Error::stream_read)? {
                if batch.num_rows() == 0 {
                    continue;
                }

                let (network, block_num) = if batch.num_rows() == 1 {
                    let network_array = batch.column(0).as_string::<i32>();
                    let Some(block_num_array) =
                        batch.column(1).as_primitive_opt::<Decimal128Type>()
                    else {
                        // pipe.connection.commit().map_err(Error::rollback(
                        //     &mut pipe.connection,
                        //     "Unable to commit after being unable to cast block_range_end column to Decimal128 in history table.",
                        // ))?;
                        return Err(Error::type_mismatch(
                            history_table,
                            "block_range_end".to_string(),
                            DataType::Decimal128(38, 0),
                            batch.column(1).data_type().clone(),
                        ));
                    };

                    (
                        network_array.value(0).to_string(),
                        block_num_array.value(0) as u64,
                    )
                } else {
                    let network_array = batch.column(0).as_string::<i32>();
                    let Some(block_num_array) =
                        batch.column(1).as_primitive_opt::<Decimal128Type>()
                    else {
                        pipe.connection.commit().map_err(Error::rollback(
                            &mut pipe.connection,
                            "Unable to commit after being unable to cast block_range_end column to Decimal128 in history table.",
                        ))?;

                        return Err(Error::type_mismatch(
                            history_table,
                            "block_range_end".to_string(),
                            DataType::Decimal128(38, 0),
                            batch.column(1).data_type().clone(),
                        ));
                    };

                    if let Some(max_index) = block_num_array.iter().position_max() {
                        (
                            network_array.value(max_index).to_string(),
                            block_num_array.value(max_index) as u64,
                        )
                    } else {
                        continue;
                    }
                };

                if let Some(current_max) = max_block_num.take()
                    && current_max >= block_num
                {
                    max_block_num.replace(current_max);
                } else {
                    max_block_num.replace(block_num);
                    max_network.replace(network);
                }
            }

            // pipe.connection.commit().map_err(Error::rollback(
            //     &mut pipe.connection,
            //     "Unable to commit after fetching max block number from history table",
            // ))?;

            if let Some(network) = max_network
                && let Some(max_block_num) = max_block_num
            {
                let max_block_num = ScalarValue::UInt64(Some(max_block_num as u64));
                self.resume_point = ResumePoint::Incremental {
                    network,
                    max_block_num,
                };
            } else {
                for pipe in &mut self.pipes {
                    pipe.truncate_tables()?;
                }
                self.resume_point = ResumePoint::None;
            }
        }
        Ok(())
    }

    pub async fn resume_or_start(&mut self) -> Result<()> {
        self.set_resume_point()?;
        let resume_point = self.resume_point.clone();
        let watermark = resume_point.as_watermark();

        if let Some(logical_plan) = resume_point.as_incremental(&self.plan)? {
            self.plan = logical_plan;
            self.statement =
                QueryStatement::Statement(plan_to_sql(&self.plan).map_err(Error::planning)?.into());
        }

        self.reset_streams(watermark.cloned())?;

        self.build_tasks()?;

        let result_stream = self
            .client
            .query(self.statement.clone(), None, watermark)
            .await
            .map_err(Error::stream_start)?;

        let reorg_stream = amp_client::with_reorg(result_stream);

        self.pipe_tasks.spawn({
            let data_tx = self.data_tx.clone();
            assert!(data_tx.same_channel(&self.data_tx));
            let watermark_tx = self.watermark_tx.clone();
            assert!(watermark_tx.same_channel(&self.watermark_tx));
            let reorg_tx = self.reorg_tx.clone();
            assert!(reorg_tx.same_channel(&self.reorg_tx));

            async move {
                tokio::pin!(reorg_stream);

                while let Some(result) = reorg_stream.next().await {
                    match result {
                        Ok(ResponseBatchWithReorg::Batch { data, metadata }) => {
                            data_tx.send((data, metadata))?;
                        }
                        Ok(ResponseBatchWithReorg::Watermark(watermark)) => {
                            let watermark = Arc::new(watermark);
                            watermark_tx.send(watermark)?;
                        }
                        Ok(ResponseBatchWithReorg::Reorg { invalidation }) => {
                            let invalidation = invalidation.into();
                            reorg_tx.send(invalidation)?;
                        }
                        Err(e) => {
                            return Err(e.into());
                        }
                    }
                }

                Ok::<(), Error>(())
            }
        });
        Ok(())
    }

    pub fn build_tasks(&mut self) -> Result<()> {
        for pipe in &mut self.pipes {
            pipe.build_tasks(&mut self.pipe_tasks)?;
        }
        Ok(())
    }

    pub async fn run(mut self) -> Result<()> {
        self.initialize()?;

        self.resume_or_start().await?;

        while let Some(res) = self.pipe_tasks.join_next().await {
            match res {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::error!("Pipeline task failed, shutting down.");
                    return Err(e);
                }
                Err(e) => {
                    tracing::error!("Pipeline task failed, shutting down.");
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ResumePoint {
    /// Hash-verified watermark resumption (preferred)
    /// Pass this to SqlClient::query() for server-side resumption
    Watermark(ResumeWatermark),

    /// Best-effort incremental checkpoint resumption
    /// Use WHERE block_num > max_block_num in the SQL query
    Incremental {
        network: String,
        max_block_num: ScalarValue,
    },

    /// No checkpoint exists - start from the beginning
    None,

    /// Unknown resumption point
    Unknown,
}

impl ResumePoint {
    pub fn is_unknown(&self) -> bool {
        matches!(self, ResumePoint::Unknown)
    }
    pub fn is_none(&self) -> bool {
        matches!(self, ResumePoint::None)
    }
    pub fn is_incremental(&self) -> bool {
        matches!(self, ResumePoint::Incremental { .. })
    }
    pub fn is_watermark(&self) -> bool {
        matches!(self, ResumePoint::Watermark(_))
    }
    pub fn as_watermark(&self) -> Option<&ResumeWatermark> {
        if let ResumePoint::Watermark(watermark) = self {
            Some(watermark)
        } else {
            None
        }
    }
    pub fn as_incremental(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        if let ResumePoint::Incremental { max_block_num, .. } = self {
            let left = col(SPECIAL_BLOCK_NUM);
            let right = Expr::Literal(max_block_num.clone(), None);
            let predicate =
                BinaryExpr::new(left.into(), datafusion_expr::Operator::Gt, right.into());
            Ok(LogicalPlanBuilder::new(plan.clone())
                .filter(Expr::BinaryExpr(predicate))
                .map_err(Error::planning)?
                .build()
                .ok())
        } else {
            Ok(None)
        }
    }
}

impl Default for ResumePoint {
    fn default() -> Self {
        ResumePoint::Unknown
    }
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct WatermarkRecord {
    pub id: i64,
    pub network: String,
    pub block_num: i64,
    pub block_hash: [u8; 32],
    pub timestamp: i64,
}

impl WatermarkRecord {
    pub fn as_watermark(&self) -> (String, Watermark) {
        let water_mark = Watermark {
            number: self.block_num as u64,
            hash: self.block_hash.into(),
        };
        (self.network.clone(), water_mark)
    }
}

impl From<WatermarkRecord> for ResumeWatermark {
    fn from(record: WatermarkRecord) -> Self {
        let mut btree_map = BTreeMap::new();
        let water_mark = Watermark {
            number: record.block_num as u64,
            hash: record.block_hash.into(),
        };
        btree_map.insert(record.network.clone(), water_mark);
        ResumeWatermark(btree_map)
    }
}

pub trait AsRecordBatch {
    const GENERATED_FIELDS: [&str; 2];
    fn as_record_batch(&self, schema: &Schema, op: &str, transfer_id: i64) -> Result<RecordBatch>;
}

impl AsRecordBatch for Arc<ResumeWatermark> {
    const GENERATED_FIELDS: [&str; 2] = ["id", "created_at"];
    fn as_record_batch(&self, schema: &Schema, _op: &str, transfer_id: i64) -> Result<RecordBatch> {
        let (network, block_num, block_hash) = self.0.iter().fold(
            (Vec::new(), Vec::new(), Vec::<[u8; 32]>::new()),
            |(mut network, mut block_num, mut block_hash), (net, watermark)| {
                network.push(net.clone());
                block_num.push(watermark.number as i64);
                block_hash.push(watermark.hash.try_into().unwrap());
                (network, block_num, block_hash)
            },
        );

        let id_array = Int64Array::from_value(transfer_id, network.len());
        let now = chrono::Utc::now().timestamp_micros();
        let created_at_array = TimestampMicrosecondArray::from_value(now, network.len());
        let network_array = StringArray::from(network);
        let block_num_array = Int64Array::from(block_num);
        let block_hash_array =
            common::arrow::array::FixedSizeBinaryArray::try_from_iter(block_hash.into_iter())?;

        let columns: Vec<common::arrow::array::ArrayRef> = vec![
            Arc::new(id_array),
            Arc::new(network_array),
            Arc::new(block_num_array),
            Arc::new(block_hash_array),
            Arc::new(created_at_array),
        ];

        assert_eq!(
            columns.len(),
            schema.schema_ref().fields().len(),
            "Number of columns does not match schema fields"
        );

        // let new_fields = schema
        //     .schema_ref()
        //     .fields()
        //     .into_iter()
        //     .filter(|f| !Self::GENERATED_FIELDS.contains(&f.name().as_str()))
        //     .cloned()
        //     .collect::<Vec<_>>();

        // let schema_ref = Arc::new(ArrowSchema::new(new_fields));
        let schema_ref = schema.schema_ref();

        assert_eq!(
            columns.len(),
            schema_ref.fields().len(),
            "Number of columns does not match schema fields"
        );

        assert!(
            columns
                .iter()
                .skip(1)
                .all(|col| col.len() == columns[0].len()),
            "All columns must have the same length"
        );
        assert!(
            columns
                .iter()
                .zip(schema_ref.fields())
                .all(|(col, field)| col.data_type() == field.data_type()),
            "Column data types must match schema field data types"
        );

        // Unwrap: safe because we have validated the schema and columns
        Ok(RecordBatch::try_new(schema_ref, columns)?)
    }
}

impl AsRecordBatch for Arc<[InvalidationRange]> {
    const GENERATED_FIELDS: [&str; 2] = ["id", "created_at"];
    fn as_record_batch(&self, schema: &Schema, op: &str, transfer_id: i64) -> Result<RecordBatch> {
        let (network, block_range_start, block_range_end) = self.iter().fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut network, mut range_start, mut range_end), range| {
                let start = *range.numbers.start() as i64;
                let end = *range.numbers.end() as i64;
                range_start.push(start);
                range_end.push(end);
                network.push(range.network.clone());
                (network, range_start, range_end)
            },
        );
        let now = chrono::Utc::now().timestamp_micros();
        let created_at_array = TimestampMicrosecondArray::from_value(now, network.len());
        let network_array = StringArray::from(network);
        let block_range_start_array = Int64Array::from(block_range_start);
        let block_range_end_array = Int64Array::from(block_range_end);
        let transfer_id_array = Int64Array::from_value(transfer_id, network_array.len());
        let operation_array =
            StringArray::from_iter_values(std::iter::repeat_n(op.to_string(), network_array.len()));

        let columns: Vec<common::arrow::array::ArrayRef> = vec![
            Arc::new(transfer_id_array),
            Arc::new(operation_array),
            Arc::new(network_array),
            Arc::new(block_range_start_array),
            Arc::new(block_range_end_array),
            Arc::new(created_at_array),
        ];

        assert_eq!(
            columns.len(),
            schema.schema_ref().fields().len(),
            "Number of columns does not match schema fields"
        );
        let schema_ref = schema.schema_ref();

        assert_eq!(
            columns.len(),
            schema_ref.fields().len(),
            "Number of columns does not match schema fields"
        );
        assert!(
            columns
                .iter()
                .skip(1)
                .all(|col| col.len() == columns[0].len()),
            "All columns must have the same length"
        );
        assert!(
            columns
                .iter()
                .zip(schema_ref.fields())
                .all(|(col, field)| col.data_type() == field.data_type()),
            "Column data types must match schema field data types"
        );

        // Unwrap: safe because we have validated the schema and columns
        Ok(RecordBatch::try_new(schema_ref, columns)?)
    }
}

impl AsRecordBatch for Metadata {
    const GENERATED_FIELDS: [&str; 2] = ["id", "created_at"];

    fn as_record_batch(&self, schema: &Schema, op: &str, transfer_id: i64) -> Result<RecordBatch> {
        let (network, block_range_start, block_range_end) = self.ranges.iter().fold(
            (Vec::new(), Vec::new(), Vec::new()),
            |(mut network, mut range_start, mut range_end), range| {
                let start = *range.numbers.start() as i64;
                let end = *range.numbers.end() as i64;
                range_start.push(start);
                range_end.push(end);
                network.push(range.network.clone());
                (network, range_start, range_end)
            },
        );
        let now = chrono::Utc::now().timestamp_micros();
        let created_at_array = TimestampMicrosecondArray::from_value(now, network.len());
        let network_array = StringArray::from(network);
        let block_range_start_array = Int64Array::from(block_range_start);
        let block_range_end_array = Int64Array::from(block_range_end);
        let transfer_id_array = Int64Array::from_value(transfer_id, network_array.len());
        let operation_array =
            StringArray::from_iter_values(std::iter::repeat_n(op.to_string(), network_array.len()));

        let columns: Vec<common::arrow::array::ArrayRef> = vec![
            Arc::new(transfer_id_array),
            Arc::new(operation_array),
            Arc::new(network_array),
            Arc::new(block_range_start_array),
            Arc::new(block_range_end_array),
            Arc::new(created_at_array),
        ];

        assert_eq!(
            columns.len(),
            schema.schema_ref().fields().len(),
            "Number of columns does not match schema fields"
        );

        let schema_ref = schema.schema_ref();

        assert_eq!(
            columns.len(),
            schema_ref.fields().len(),
            "Number of columns does not match schema fields"
        );
        assert!(
            columns
                .iter()
                .skip(1)
                .all(|col| col.len() == columns[0].len()),
            "All columns must have the same length"
        );

        assert!(
            columns
                .iter()
                .zip(schema_ref.fields())
                .all(|(col, field)| col.data_type() == field.data_type()),
            "Column data types must match schema field data types"
        );

        let record_batch = RecordBatch::try_new(schema_ref, columns)?;

        Ok(record_batch)
    }
}

pub trait RecordBatchExt {
    fn push_scalar_into_batch<T: ArrowPrimitiveType>(
        &mut self,
        value: T::Native,
        column_name: &str,
    ) -> Result<()>;

    fn include_transfer_id(&mut self, transfer_id: i64) -> Result<()> {
        self.push_scalar_into_batch::<Int64Type>(transfer_id, "transfer_id")
    }
}

impl RecordBatchExt for RecordBatch {
    fn push_scalar_into_batch<T: ArrowPrimitiveType>(
        &mut self,
        value: T::Native,
        column_name: &str,
    ) -> Result<()> {
        let array = PrimitiveArray::<T>::from_value(value, self.num_rows());
        let data_type = array.data_type().clone();
        let array_ref: ArrayRef = Arc::new(array);
        let mut columns = self.columns().to_vec();
        let mut fields = self.schema().fields().to_vec();
        columns.push(array_ref);
        fields.push(Field::new(column_name, data_type, false).into());
        let new_schema = Arc::new(ArrowSchema::new(fields));
        *self = RecordBatch::try_new(new_schema, columns)?;
        Ok(())
    }
}
