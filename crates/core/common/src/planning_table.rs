use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};

use crate::catalog::logical::LogicalTable;

/// A placeholder table provider used during SQL planning.
///
/// Provides schema information for logical plan construction but cannot be scanned.
/// Must be replaced with actual `TableSnapshot` providers via
/// `DetachedLogicalPlan::attach_to` before execution.
#[derive(Clone, Debug)]
pub struct PlanningTable(LogicalTable);

impl PlanningTable {
    /// Wraps a logical table as a planning-only table provider.
    pub(crate) fn new(table: LogicalTable) -> Self {
        Self(table)
    }
}

#[async_trait]
impl TableProvider for PlanningTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.0.table().schema().clone()
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
        Err(DataFusionError::External(
            "PlanningTable should never be scanned".into(),
        ))
    }
}
