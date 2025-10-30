use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::AtomicI64},
};

use adbc_core::{
    Optionable,
    options::{IngestMode, OptionConnection, OptionStatement},
};
use amp_client::{InvalidationRange, Metadata, ResponseBatchWithReorg, SqlClient};
use common::{
    PlanningContext, QueryContext, SPECIAL_BLOCK_NUM,
    arrow::{
        array::{Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray},
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
use tokio::{sync::broadcast::Sender, task::JoinHandle};
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    Connection, ConnectionExt, Error, Schema, SchemaExt, Statement,
    adbc::{AdbcConnection, AdbcStatement},
    arrow::{
        RecordBatch, Schema as ArrowSchema,
        array::{AsArray, Int64Array, StringArray},
    },
    config::Config,
    datafusion::ScalarValue,
    error::Result,
    schema::TableRef,
    sql::DDLSafety,
};

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

    pub fn build_tasks(&mut self) -> Result<()> {
        let connection = &mut self.connection;
        // Update the current transfer ID
        let mut current_transfer_id_stmt = connection.create_statement()?;
        current_transfer_id_stmt.set_sql_query(format!(
            "SELECT MAX(id) FROM {}",
            self.schema.as_transfer_history().table_ref().full_name()
        ))?;
        let cursor = current_transfer_id_stmt.execute()?;

        let current_transfer_id = cursor
            .into_iter()
            .filter_map(|batch| {
                batch
                    .map(|b| compute::max(b.column(0).as_primitive::<Int64Type>()))
                    .ok()
                    .flatten()
            })
            .max()
            .unwrap_or(0);

        connection.commit()?;

        self.transfer_id
            .fetch_max(current_transfer_id, std::sync::atomic::Ordering::SeqCst);

        if let Some(data_stream) = self.data_stream.take() {
            let schema = &self.schema;
            let transfer_id = Arc::clone(&self.transfer_id);
            let data_handle = tokio::spawn({
                let mut connection = connection.clone();
                let data_schema = schema.clone();
                let history_schema = data_schema.as_transfer_history();
                async move {
                    tokio::pin!(data_stream);

                    let mut enriched_stream = data_stream.map_err(Error::stream_recv);

                    while let Some((mut batch, metadata)) = enriched_stream.try_next().await? {
                        let current_transfer_id =
                            transfer_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        batch.include_transfer_id(current_transfer_id)?;

                        connection.set_option(OptionConnection::AutoCommit, "false".into())?;

                        let mut insert_stmt = connection.new_statement()?;

                        insert_stmt
                            .set_option(OptionStatement::IngestMode, IngestMode::Append.into())?;

                        insert_stmt.set_option(
                            OptionStatement::TargetTable,
                            data_schema.table_ref().full_name().into(),
                        )?;

                        insert_stmt.bind(batch)?;

                        // We now have a transaction open, so we need to make sure to rollback on errors

                        if let Some(rows_affected) =
                            insert_stmt.execute_update().map_err(Error::rollback(
                                &mut connection,
                                format!(
                                    "Failed to execute insert statement for {}",
                                    data_schema.table_ref().full_name()
                                ),
                            ))?
                        {
                            tracing::info!(
                                "Inserted {} rows into table {}",
                                rows_affected,
                                data_schema.table_ref().full_name()
                            );
                        }

                        let metadata_batch = metadata.as_record_batch(
                            &history_schema,
                            "INSERT",
                            current_transfer_id,
                        )?;

                        let mut insert_stmt =
                            connection.new_statement().map_err(Error::rollback(
                                &mut connection,
                                "Failed to create history insert statement",
                            ))?;

                        insert_stmt
                            .set_option(OptionStatement::IngestMode, IngestMode::Append.into())
                            .map_err(Error::rollback(
                                &mut connection,
                                "Failed to set history insert statement ingest mode",
                            ))?;

                        insert_stmt
                            .set_option(
                                OptionStatement::TargetTable,
                                history_schema.table_ref().full_name().into(),
                            )
                            .map_err(Error::rollback(
                                &mut connection,
                                "Failed to set history insert statement target table",
                            ))?;

                        insert_stmt.bind(metadata_batch).map_err(Error::rollback(
                            &mut connection,
                            "Failed to bind metadata batch to history insert statement",
                        ))?;

                        if let Some(rows_affected) =
                            insert_stmt.execute_update().map_err(Error::rollback(
                                &mut connection,
                                "Failed to execute history insert statement",
                            ))?
                        {
                            tracing::info!(
                                "Inserted {} rows into history table {}",
                                rows_affected,
                                history_schema.table_ref().full_name()
                            );
                        }

                        connection.commit().map_err(Error::rollback(
                            &mut connection,
                            "Failed to commit transaction",
                        ))?;
                    }

                    Ok::<(), Error>(())
                }
            });

            self.data_handle.replace(data_handle);
        }

        if let Some(reorg_stream) = self.reorg_stream.take() {
            let transfer_id = Arc::clone(&self.transfer_id);

            let reorg_handle = tokio::spawn({
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
                        let current_transfer_id =
                            transfer_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                        let reorg_batch = invalidation_ranges.as_record_batch(
                            &reorg_schema,
                            "REORG",
                            current_transfer_id,
                        )?;

                        let mut insert_stmt =
                            connection.new_statement().map_err(Error::rollback(
                                &mut connection,
                                "Failed to create reorg insert statement",
                            ))?;

                        insert_stmt
                            .set_option(OptionStatement::IngestMode, IngestMode::Append.into())
                            .map_err(Error::rollback(
                                &mut connection,
                                "Failed to set reorg insert statement ingest mode",
                            ))?;

                        insert_stmt
                            .set_option(
                                OptionStatement::TargetTable,
                                reorg_schema.table_ref().full_name().into(),
                            )
                            .map_err(Error::rollback(
                                &mut connection,
                                format!(
                                    "Failed to set target table for reorg insert statement: {}",
                                    reorg_schema.table_ref().full_name()
                                ),
                            ))?;

                        insert_stmt.bind(reorg_batch).map_err(Error::rollback(
                            &mut connection,
                            "Failed to bind reorg batch to history insert statement",
                        ))?;

                        if let Some(rows_affected) =
                            insert_stmt.execute_update().map_err(Error::rollback(
                                &mut connection,
                                "Failed to execute history insert statement",
                            ))?
                        {
                            tracing::info!(
                                "Inserted {} rows into history table {}",
                                rows_affected,
                                reorg_schema.table_ref().full_name()
                            );
                        }

                        connection.commit().map_err(Error::rollback(
                            &mut connection,
                            "Failed to commit transaction",
                        ))?;
                    }

                    Ok::<(), Error>(())
                }
            });
            self.reorg_handle.replace(reorg_handle);
        }

        if let Some(watermark_stream) = self.watermark_stream.take() {
            let watermark_handle = tokio::spawn({
                let mut connection = self.connection.clone();
                let watermark_schema = self.schema.as_watermark().clone();

                async move {
                    tokio::pin!(watermark_stream);

                    while let Some(watermark) = watermark_stream
                        .try_next()
                        .await
                        .map_err(Error::stream_recv)?
                    {
                        let watermark_batch =
                            watermark.as_record_batch(&watermark_schema, "", 0)?;
                        let mut insert_stmt =
                            connection.new_statement().map_err(Error::rollback(
                                &mut connection,
                                "Failed to create new statement for watermark insert",
                            ))?;

                        insert_stmt
                            .set_option(OptionStatement::IngestMode, IngestMode::Append.into())
                            .map_err(Error::rollback(
                                &mut connection,
                                "Failed to set ingest mode for watermark insert statement",
                            ))?;

                        insert_stmt
                            .set_option(
                                OptionStatement::TargetTable,
                                watermark_schema.table_ref().full_name().into(),
                            )
                            .map_err(Error::rollback(
                                &mut connection,
                                "Failed to set target table for watermark insert statement",
                            ))?;

                        insert_stmt.bind(watermark_batch).map_err(Error::rollback(
                            &mut connection,
                            "Failed to bind watermark batch to insert statement",
                        ))?;

                        if let Some(rows_affected) =
                            insert_stmt.execute_update().map_err(Error::rollback(
                                &mut connection,
                                "Failed to execute watermark insert statement",
                            ))?
                        {
                            tracing::info!(
                                "Inserted {} rows into watermark table {}",
                                rows_affected,
                                watermark_schema.table_ref().full_name()
                            );
                        }

                        connection.commit().map_err(Error::rollback(
                            &mut connection,
                            "Failed to commit transaction",
                        ))?;
                    }

                    Ok::<(), Error>(())
                }
            });

            self.watermark_handle.replace(watermark_handle);
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

        let data_table =
            connection.create_table_ddl_statement(data_schema.clone(), DDLSafety::IfNotExists)?;

        let history_table = connection
            .create_table_ddl_statement(history_schema.clone(), DDLSafety::IfNotExists)?;

        let watermark_table = connection
            .create_table_ddl_statement(watermark_schema.clone(), DDLSafety::IfNotExists)?;

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
        self.connection
            .set_option(OptionConnection::AutoCommit, "true".into())?;

        let (
            (mut truncate_data, data_table_ref),
            (mut truncate_history, history_table_ref),
            (mut truncate_watermark, watermark_table_ref),
        ) = self.truncate_statements()?;

        if let Some(rows_affected) = truncate_data.execute_update()? {
            tracing::info!(
                "Truncated table {}, rows affected: {}",
                data_table_ref.full_name(),
                rows_affected
            );
        }

        if let Some(rows_affected) = truncate_history.execute_update()? {
            tracing::info!(
                "Truncated table {}, rows affected: {}",
                history_table_ref.full_name(),
                rows_affected
            );
        }

        if let Some(rows_affected) = truncate_watermark.execute_update()? {
            tracing::info!(
                "Truncated table {}, rows affected: {}",
                watermark_table_ref.full_name(),
                rows_affected
            );
        }

        Ok(())
    }
}

pub type Pipes = Vec<Pipe>;

pub struct QueryPipeline {
    pub client: SqlClient,
    pub context: QueryContext,
    pub pipes: Pipes,
    pub plan: LogicalPlan,
    pub statement: QueryStatement,
    pub resume_point: ResumePoint,
    pub data_tx: Sender<(RecordBatch, Metadata)>,
    pub reorg_tx: Sender<Arc<[InvalidationRange]>>,
    pub watermark_tx: Sender<Arc<ResumeWatermark>>,
    pub stream_broadcast_task: Option<JoinHandle<Result<()>>>,
}

impl QueryPipeline {
    pub async fn try_new(config: &mut Config, sql: &str) -> Result<Self> {
        let connections = config.connections()?;

        let (client, dataset_store) =
            tokio::try_join!(config.sql_client(), config.dataset_store())?;

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

        Ok(Self {
            client,
            context,
            statement,
            plan,
            pipes,
            data_tx,
            reorg_tx,
            watermark_tx,
            resume_point,
            stream_broadcast_task: None,
        })
    }

    pub fn initialize(&mut self) -> Result<()> {
        for pipe in &mut self.pipes {
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
                    "Data table {} already exists",
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
                    "History table {} already exists",
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
                    "Watermark table {} already exists",
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
        let watermark_table = pipe.schema.as_watermark().table_ref().full_name();
        let history_table = pipe.schema.as_transfer_history().table_ref().full_name();
        pipe.connection
            .set_option(OptionConnection::AutoCommit, "true".into())?;

        let mut watermark_stmt = pipe.connection.create_statement()?;

        watermark_stmt.set_sql_query(format!(
                "SELECT a.* FROM {watermark_table} a JOIN (SELECT MAX(id) AS id FROM {watermark_table} GROUP BY network) b ON a.id = b.id;"
            ))?;

        let mut cursor = watermark_stmt.execute()?;

        let resume_watermark = if let Some(Ok(batch)) = cursor.next()
            && batch.num_rows() > 0
        {
            let id_array = batch
                .column(0)
                .as_primitive::<common::arrow::datatypes::Int64Type>();
            let network_array = batch.column(1).as_string::<i32>();
            let block_num_array = batch
                .column(2)
                .as_primitive::<common::arrow::datatypes::Int64Type>();
            let block_hash_array = batch.column(3).as_fixed_size_binary();
            let mut missing = false;

            let watermarks = id_array
                .iter()
                .zip(network_array.iter())
                .zip(block_num_array.iter().zip(block_hash_array.iter()))
                .fold(
                    BTreeMap::new(),
                    |mut map, ((id, network), (block_num, block_hash))| {
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
            self.resume_point = ResumePoint::None;
            return Ok(());
        };

        if let Some(resume_watermark) = resume_watermark {
            self.resume_point = ResumePoint::Watermark(resume_watermark);
        } else {
            let mut max_block_num_stmt = pipe.connection.create_statement()?;

            max_block_num_stmt.set_sql_query(format!(
                "SELECT network
                      , MAX(block_range_end)
                   FROM {history_table} 
               GROUP BY network
               ORDER BY 2 DESC
                  LIMIT 1;"
            ))?;

            let mut cursor = max_block_num_stmt.execute()?;

            let mut max_block_num: Option<i64> = None;
            let mut max_network: Option<String> = None;

            while let Some(batch) = cursor.next().transpose().map_err(Error::stream_read)? {
                if batch.num_rows() == 0 {
                    continue;
                }

                let (network, block_num) = if batch.num_rows() == 1 {
                    let network_array = batch.column(0).as_string::<i32>();
                    let block_num_array = batch
                        .column(1)
                        .as_primitive::<common::arrow::datatypes::Int64Type>();

                    (network_array.value(0).to_string(), block_num_array.value(0))
                } else {
                    let network_array = batch.column(0).as_string::<i32>();
                    let block_num_array = batch
                        .column(1)
                        .as_primitive::<common::arrow::datatypes::Int64Type>();

                    if let Some(max_index) = block_num_array.iter().position_max() {
                        (
                            network_array.value(max_index).to_string(),
                            block_num_array.value(max_index),
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

        let stream_broadcast_task = tokio::spawn({
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

        if let Some(old_task) =
            std::mem::replace(&mut self.stream_broadcast_task, Some(stream_broadcast_task))
        {
            old_task.abort();
            drop(old_task);

            // Unwrap: safe because we just set it to Some
            let new_task = self.stream_broadcast_task.take().unwrap();
            new_task.abort();
            drop(new_task);
        };
        Ok(())
    }

    pub fn build_tasks(&mut self) -> Result<()> {
        for pipe in &mut self.pipes {
            pipe.build_tasks()?;
        }
        Ok(())
    }

    pub async fn run(mut self) -> Result<()> {
        self.initialize()?;

        self.resume_or_start().await?;

        if let Some(stream_broadcast_task) = &mut self.stream_broadcast_task {
            // Wait for cancellation signal or completion
            // Do this by selecting on a cancellation signal channel or monitoring the tasks

            tokio::select! {
                _ = stream_broadcast_task => {
                    let (data_tx, _) = tokio::sync::broadcast::channel(1);
                    let _ = std::mem::replace(&mut self.data_tx, data_tx);
                    let (reorg_tx, _) = tokio::sync::broadcast::channel(1);
                    let _ = std::mem::replace(&mut self.reorg_tx, reorg_tx);
                    let (watermark_tx, _) = tokio::sync::broadcast::channel(1);
                    let _ = std::mem::replace(&mut self.watermark_tx, watermark_tx);

                    tracing::info!("Stream broadcast task completed.");
                }
                _ = tokio::signal::ctrl_c() => {
                    // Cancellation signal received
                    tracing::info!("Cancellation signal received, shutting down...");
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
    fn as_record_batch(&self, schema: &Schema, op: &str, transfer_id: i64) -> Result<RecordBatch>;
}

impl AsRecordBatch for Arc<ResumeWatermark> {
    fn as_record_batch(
        &self,
        schema: &Schema,
        _op: &str,
        _transfer_id: i64,
    ) -> Result<RecordBatch> {
        let (network, block_num, block_hash) = self.0.iter().fold(
            (Vec::new(), Vec::new(), Vec::<[u8; 32]>::new()),
            |(mut network, mut block_num, mut block_hash), (net, watermark)| {
                network.push(net.clone());
                block_num.push(watermark.number as i64);
                block_hash.push(watermark.hash.try_into().unwrap());
                (network, block_num, block_hash)
            },
        );

        let network_array = StringArray::from(network);
        let block_num_array = Int64Array::from(block_num);
        let block_hash_array =
            common::arrow::array::FixedSizeBinaryArray::try_from_iter(block_hash.into_iter())?;

        let columns: Vec<common::arrow::array::ArrayRef> = vec![
            Arc::new(network_array),
            Arc::new(block_num_array),
            Arc::new(block_hash_array),
        ];

        assert!(columns.len() == schema.schema_ref().fields().len());

        let new_fields = schema
            .schema_ref()
            .fields()
            .into_iter()
            .filter(|f| f.name() != "id" || f.name() == "created_at")
            .cloned()
            .collect::<Vec<_>>();

        let schema_ref = Arc::new(ArrowSchema::new(new_fields));

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
        ];

        assert!(columns.len() == schema.schema_ref().fields().len());

        let new_fields = schema
            .schema_ref()
            .fields()
            .into_iter()
            .filter(|f| f.name() != "id" || f.name() == "created_at")
            .cloned()
            .collect::<Vec<_>>();

        let schema_ref = Arc::new(ArrowSchema::new(new_fields));

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
        ];

        assert!(columns.len() == schema.schema_ref().fields().len());

        let new_fields = schema
            .schema_ref()
            .fields()
            .into_iter()
            .filter(|f| f.name() != "id" || f.name() == "created_at")
            .cloned()
            .collect::<Vec<_>>();

        let schema_ref = Arc::new(ArrowSchema::new(new_fields));

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
