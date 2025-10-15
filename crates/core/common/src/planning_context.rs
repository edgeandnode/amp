use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use bincode::{Decode, Encode, config};
use bytes::Bytes;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::{
        DFSchemaRef,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    datasource::{DefaultTableSource, TableType},
    error::DataFusionError,
    execution::{SessionStateBuilder, config::SessionConfig, context::SessionContext},
    logical_expr::{Expr, LogicalPlan, TableScan},
    physical_plan::ExecutionPlan,
    sql::parser,
};
use datafusion_proto::bytes::{
    logical_plan_from_bytes_with_extension_codec, logical_plan_to_bytes_with_extension_codec,
};
use tracing::instrument;

use crate::{
    BlockNum, BoxError, LogicalCatalog, QueryContext, ResolvedTable,
    metadata::segments::ResumeWatermark,
    plan_visitors::{IncrementalCheck, is_incremental, order_by_block_num, propagate_block_num},
    query_context::{Error, TableProviderCodec, default_catalog_name},
    stream_helpers::is_streaming,
};

/// A context for planning SQL queries.
pub struct PlanningContext {
    session_config: SessionConfig,
    catalog: LogicalCatalog,
}

// Serialized plan with additional options
#[derive(Encode, Decode)]
pub struct RemotePlan {
    pub serialized_plan: Vec<u8>,
    pub is_streaming: bool,
    pub resume_watermark: Option<BTreeMap<String, (BlockNum, [u8; 32])>>,
    pub table_refs: Vec<String>,
    pub function_refs: Vec<String>,
}

pub fn remote_plan_from_bytes(bytes: &Bytes) -> Result<RemotePlan, Error> {
    let (remote_plan, _) = bincode::decode_from_slice(bytes, config::standard()).map_err(|e| {
        Error::PlanDecodingError(DataFusionError::Plan(format!(
            "Failed to serialize remote plan: {}",
            e
        )))
    })?;
    Ok(remote_plan)
}

impl PlanningContext {
    pub fn new(catalog: LogicalCatalog) -> Self {
        let session_config = SessionConfig::from_env().unwrap().set(
            "datafusion.catalog.default_catalog",
            &default_catalog_name(),
        );

        Self {
            session_config,
            catalog,
        }
    }

    /// Infers the output schema of the query by planning it against empty tables.
    pub async fn sql_output_schema(&self, query: parser::Statement) -> Result<DFSchemaRef, Error> {
        let ctx = self.datafusion_ctx()?;
        let plan = crate::query_context::sql_to_plan(&ctx, query).await?;
        Ok(plan.schema().clone())
    }

    /// This will plan the query against empty tables, and then serialize that plan using
    /// datafusion-proto. This is useful for sending the plan to a remote server for execution.
    ///
    /// Returns the serialized plan and its output schema.
    pub async fn sql_to_remote_plan(
        &self,
        query: parser::Statement,
        resume_watermark: Option<ResumeWatermark>,
    ) -> Result<(Bytes, DFSchemaRef), Error> {
        let is_streaming = is_streaming(&query);
        let ctx = self.datafusion_ctx()?;
        let plan = crate::query_context::sql_to_plan(&ctx, query).await?;
        let schema = plan.schema().clone();
        let serialized_plan =
            logical_plan_to_bytes_with_extension_codec(&plan, &TableProviderCodec)
                .map_err(Error::PlanEncodingError)?;
        let LogicalCatalog { tables, udfs } = &self.catalog;
        let table_refs = tables.iter().map(|t| t.table_ref().to_string()).collect();
        let remote_plan = RemotePlan {
            serialized_plan: serialized_plan.to_vec(),
            is_streaming,
            resume_watermark: resume_watermark.map(Into::into),
            table_refs,
            function_refs: udfs.iter().map(|f| f.name().to_string()).collect(),
        };
        let serialized_plan = Bytes::from(
            bincode::encode_to_vec(&remote_plan, config::standard()).map_err(|e| {
                Error::PlanEncodingError(DataFusionError::Plan(format!(
                    "Failed to serialize remote plan: {}",
                    e
                )))
            })?,
        );

        Ok((serialized_plan, schema))
    }

    fn datafusion_ctx(&self) -> Result<SessionContext, Error> {
        let state = SessionStateBuilder::new()
            .with_config(self.session_config.clone())
            .with_runtime_env(Default::default())
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        for table in &self.catalog.tables {
            // The catalog schema needs to be explicitly created or table creation will fail.
            crate::query_context::create_catalog_schema(&ctx, table.catalog_schema().to_string());
            let planning_table = PlanningTable(table.clone());
            ctx.register_table(table.table_ref().clone(), Arc::new(planning_table))
                .map_err(|e| Error::DatasetError(e.into()))?;
        }
        self.register_udfs(&ctx);
        Ok(ctx)
    }

    fn register_udfs(&self, ctx: &SessionContext) {
        for udf in crate::query_context::udfs() {
            ctx.register_udf(udf);
        }
        for udaf in crate::query_context::udafs() {
            ctx.register_udaf(udaf);
        }
        for udf in self.catalog.udfs.iter() {
            ctx.register_udf(udf.clone());
        }
    }

    pub fn catalog(&self) -> &[ResolvedTable] {
        &self.catalog.tables
    }

    pub async fn optimize_plan(
        &self,
        plan: &DetachedLogicalPlan,
    ) -> Result<DetachedLogicalPlan, Error> {
        self.datafusion_ctx()?
            .state()
            .optimize(&plan.0)
            .map_err(Error::PlanningError)
            .map(DetachedLogicalPlan)
    }

    pub async fn plan_sql(&self, query: parser::Statement) -> Result<DetachedLogicalPlan, Error> {
        let ctx = self.datafusion_ctx()?;
        let plan = crate::query_context::sql_to_plan(&ctx, query).await?;
        Ok(DetachedLogicalPlan(plan))
    }

    pub async fn plan_from_bytes(&self, bytes: &[u8]) -> Result<DetachedLogicalPlan, Error> {
        let ctx = self.datafusion_ctx()?;
        let plan = logical_plan_from_bytes_with_extension_codec(bytes, &ctx, &TableProviderCodec)
            .map_err(Error::PlanDecodingError)?;
        crate::query_context::verify_plan(&plan)?;
        Ok(DetachedLogicalPlan(plan))
    }
}

#[derive(Clone, Debug)]
struct PlanningTable(ResolvedTable);

#[async_trait]
impl TableProvider for PlanningTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.0.schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        unreachable!("PlanningTable should never be scanned")
    }
}

/// A plan that has `PlanningTable` for its `TableProvider`s. It cannot be executed before being
/// first "attached" to a `QueryContext`.
#[derive(Debug, Clone)]
pub struct DetachedLogicalPlan(LogicalPlan);

impl DetachedLogicalPlan {
    #[instrument(skip_all, err)]
    pub fn attach_to(self, ctx: &QueryContext) -> Result<LogicalPlan, Error> {
        Ok(self
            .0
            .transform(|mut node| match &mut node {
                // Insert the clauses in non-view table scans
                LogicalPlan::TableScan(TableScan {
                    table_name, source, ..
                }) if source.table_type() == TableType::Base
                    && source.get_logical_plan().is_none() =>
                {
                    let provider = ctx
                        .get_table(table_name)
                        .map_err(|e| DataFusionError::External(e.into()))?;
                    *source = Arc::new(DefaultTableSource::new(provider));
                    Ok(Transformed::yes(node))
                }
                _ => Ok(Transformed::no(node)),
            })
            .map_err(Error::PlanningError)?
            .data)
    }

    pub fn is_incremental(&self) -> Result<IncrementalCheck, BoxError> {
        is_incremental(&self.0)
    }

    pub fn schema(&self) -> DFSchemaRef {
        self.0.schema().clone()
    }

    pub fn propagate_block_num(self) -> Result<Self, DataFusionError> {
        Ok(Self(propagate_block_num(self.0)?))
    }

    pub fn order_by_block_num(self) -> Self {
        Self(order_by_block_num(self.0))
    }

    pub fn apply<F>(&self, f: F) -> Result<TreeNodeRecursion, DataFusionError>
    where
        F: FnMut(&LogicalPlan) -> Result<TreeNodeRecursion, DataFusionError>,
    {
        self.0.apply(f)
    }
}
