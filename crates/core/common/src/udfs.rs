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

pub mod block_num;
pub mod exec;
pub mod plan;

pub use exec::ExecJsUdf;
pub use plan::PlanJsUdf;
