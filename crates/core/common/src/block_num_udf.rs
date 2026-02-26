use std::any::Any;

use datafusion::{
    arrow::datatypes::DataType,
    error::Result,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
        expr::physical_name,
    },
    prelude::Expr,
};
use datasets_common::block_num::RESERVED_BLOCK_NUM_COLUMN_NAME;

/// A planning-time sentinel UDF that gets replaced with the appropriate `_block_num`
/// expression during `BlockNumPropagator::f_up`.  Panics if it reaches execution.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct BlockNumUdf {
    signature: Signature,
}

impl Default for BlockNumUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockNumUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

const BLOCK_NUM_UDF_NAME: &str = "block_num";

impl ScalarUDFImpl for BlockNumUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        BLOCK_NUM_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        panic!("block_num() is a planning-time sentinel; must be replaced before execution")
    }
}

// ── Expr helpers ──────────────────────────────────────────────────────────────

/// Returns `true` if `expr` is a `block_num()` sentinel UDF call.
pub(crate) fn is_block_num_udf(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(f)
        if f.func.name() == BLOCK_NUM_UDF_NAME && f.args.is_empty())
}

/// The column name DataFusion assigns to a `block_num()` call when used as an
/// Aggregate group key: `columnize_expr` (called by the SQL planner and
/// `LogicalPlanBuilder::project`) converts the outer Projection's ScalarFunction
/// to `col("block_num()")`.  `replace_block_num_udf` matches both forms.
pub(crate) const BLOCK_NUM_UDF_SCHEMA_NAME: &str = "block_num()";

pub(crate) fn is_block_num_udf_or_normalized(expr: &Expr) -> bool {
    is_block_num_udf(expr)
        || matches!(expr, Expr::Column(c)
            if c.relation.is_none() && c.name == BLOCK_NUM_UDF_SCHEMA_NAME)
}

/// Returns `true` if `expr` will produce a block-num value as its output column.
///
/// Covers both the pre-propagation form (`block_num()` sentinel) and the two
/// post-propagation forms:
/// - `_block_num` — the auto-prepended column added when the user did not
///   explicitly write `block_num()` in their SELECT.
/// - `block_num()` — the preserved output name when the user *did* write
///   `block_num()` explicitly; propagation aliases the replacement so the
///   schema is identical before and after.
///
/// # Invariant dependency
///
/// The output-name matches are only sound after the plan has been validated by
/// [`forbid_underscore_prefixed_aliases`]: that check ensures no user alias can
/// produce a `_block_num` output name, so any match is a genuine propagated
/// value.  `BlockNumPropagator` uses this to skip prepending a duplicate.
///
/// [`forbid_underscore_prefixed_aliases`]: crate::plan_visitors::forbid_underscore_prefixed_aliases
pub fn expr_outputs_block_num(expr: &Expr) -> bool {
    is_block_num_udf(expr)
        || physical_name(expr).is_ok_and(|name| name == RESERVED_BLOCK_NUM_COLUMN_NAME)
}
