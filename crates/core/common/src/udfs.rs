//! JS UDF lifecycle types for planning and execution phases.
//!
//! This module implements the plan-vs-exec split for JavaScript user-defined
//! functions, mirroring the existing `PlanTable` / `QueryableSnapshot` pattern
//! used for table lifecycle management.
//!
//! - [`plan::PlanJsUdf`]: planning-phase UDF — holds signature and source but
//!   no runtime resources. Panics if DataFusion attempts to invoke it.
//! - [`exec::ExecJsUdf`]: execution-phase UDF — produced by attaching an
//!   `IsolatePool` to a `PlanJsUdf`.

use std::sync::Arc;

use datafusion::logical_expr::ScalarUDF;

pub mod block_num;
pub mod eth_call;
mod evm_common;
pub mod evm_decode_hex;
pub mod evm_decode_log;
pub mod evm_decode_type;
pub mod evm_encode_hex;
pub mod evm_encode_type;
pub mod evm_function_params;
pub mod evm_topic;
pub mod exec;
pub mod plan;
pub mod shift_units;

use self::{
    block_num::BlockNumUdf,
    evm_decode_hex::EvmDecodeHex,
    evm_decode_log::EvmDecodeLog,
    evm_decode_type::EvmDecodeType,
    evm_encode_hex::EvmEncodeHex,
    evm_encode_type::EvmEncodeType,
    evm_function_params::{EvmDecodeParams, EvmEncodeParams},
    evm_topic::EvmTopic,
    shift_units::ShiftUnits,
};

/// Returns the built-in scalar UDFs registered in every session state.
///
/// Called by [`PlanContextBuilder`](crate::context::plan::PlanContextBuilder)
/// and [`ExecContextBuilder`](crate::context::exec::ExecContextBuilder) to
/// supply UDFs to the session state builder, keeping the session state layer
/// generic and unaware of specific UDF types.
pub fn builtin_udfs() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(BlockNumUdf::new().into()),
        Arc::new(EvmDecodeLog::new().into()),
        Arc::new(EvmDecodeLog::new().with_deprecated_name().into()),
        Arc::new(EvmTopic::new().into()),
        Arc::new(EvmEncodeParams::new().into()),
        Arc::new(EvmDecodeParams::new().into()),
        Arc::new(EvmEncodeType::new().into()),
        Arc::new(EvmDecodeType::new().into()),
        Arc::new(EvmEncodeHex::new().into()),
        Arc::new(EvmDecodeHex::new().into()),
        Arc::new(ShiftUnits::new().into()),
    ]
}
