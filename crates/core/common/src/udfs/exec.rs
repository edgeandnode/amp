//! Execution-phase JS UDF representation.
//!
//! [`ExecJsUdf`] is produced by attaching runtime resources (`IsolatePool`)
//! to a [`PlanJsUdf`](super::plan::PlanJsUdf). It wraps an executable
//! `ScalarUDF` that DataFusion can invoke during query execution.

use std::sync::Arc;

use datafusion::logical_expr::{ScalarUDF, ScalarUDFImpl, async_udf::AsyncScalarUDF};
use js_runtime::{isolate_pool::IsolatePool, js_udf::JsUdf};

use super::plan::PlanJsUdf;

/// Execution-ready JavaScript user-defined function.
///
/// Created exclusively via [`PlanJsUdf::attach`]. Holds runtime resources
/// (`IsolatePool`) and produces an executable [`ScalarUDF`] for DataFusion.
#[derive(Debug)]
pub struct ExecJsUdf {
    scalar_udf: ScalarUDF,
}

impl ExecJsUdf {
    /// Build an `ExecJsUdf` by attaching runtime resources to a planning UDF.
    ///
    /// This is the only constructor — enforces the plan→exec lifecycle.
    pub(super) fn from_plan(plan: &PlanJsUdf, pool: IsolatePool) -> Self {
        let sig = plan.signature();
        let input_types = match &sig.type_signature {
            datafusion::logical_expr::TypeSignature::Exact(types) => types.clone(),
            _ => unreachable!("PlanJsUdf always uses TypeSignature::Exact"),
        };

        let js_udf = JsUdf::new(
            pool,
            plan.schema_name().map(String::from),
            plan.source_code().clone(),
            Arc::clone(plan.filename()),
            Arc::clone(plan.function_name()),
            input_types,
            plan.output_type().clone(),
        );

        let scalar_udf = AsyncScalarUDF::new(Arc::new(js_udf)).into_scalar_udf();

        Self { scalar_udf }
    }

    /// The executable `ScalarUDF` for registration in DataFusion catalogs.
    pub fn scalar_udf(&self) -> &ScalarUDF {
        &self.scalar_udf
    }

    /// Consume self and return the inner `ScalarUDF`.
    pub fn into_scalar_udf(self) -> ScalarUDF {
        self.scalar_udf
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{arrow::datatypes::DataType, logical_expr::TypeSignature};
    use datasets_derived::function::Function;
    use js_runtime::isolate_pool::IsolatePool;

    use super::*;

    #[test]
    fn attach_with_valid_function_produces_executable_udf() {
        //* Given
        let function = sample_function();
        let plan = PlanJsUdf::from_function("my_func", &function, None);

        //* When
        let exec = plan.attach(IsolatePool::new());

        //* Then
        let udf = exec.scalar_udf();
        assert_eq!(udf.name(), "my_func");
        let inner_sig = udf.inner().signature();
        assert!(
            matches!(&inner_sig.type_signature, TypeSignature::Exact(types) if types == &[DataType::Utf8, DataType::Int64]),
        );
    }

    #[test]
    fn attach_with_schema_qualified_function_preserves_name() {
        //* Given
        let function = sample_function();
        let plan = PlanJsUdf::from_function("my_func", &function, Some("ns/ds@1.0"));

        //* When
        let exec = plan.attach(IsolatePool::new());

        //* Then
        assert_eq!(exec.scalar_udf().name(), plan.name());
        assert_eq!(exec.scalar_udf().name(), "\"ns/ds@1.0\".my_func");
    }

    #[test]
    fn attach_with_distinct_schemas_produces_distinct_names() {
        //* Given
        let function = sample_function();
        let plan_a = PlanJsUdf::from_function("func", &function, Some("schema_a"));
        let plan_b = PlanJsUdf::from_function("func", &function, Some("schema_b"));

        //* When
        let exec_a = plan_a.attach(IsolatePool::new());
        let exec_b = plan_b.attach(IsolatePool::new());

        //* Then
        assert_ne!(
            exec_a.scalar_udf().name(),
            exec_b.scalar_udf().name(),
            "different schemas must produce different runtime UDF names"
        );
        assert_eq!(exec_a.scalar_udf().name(), plan_a.name());
        assert_eq!(exec_b.scalar_udf().name(), plan_b.name());
    }

    #[test]
    fn into_scalar_udf_with_attached_exec_returns_udf() {
        //* Given
        let function = sample_function();
        let exec = PlanJsUdf::from_function("my_func", &function, None).attach(IsolatePool::new());

        //* When
        let udf = exec.into_scalar_udf();

        //* Then
        assert_eq!(udf.name(), "my_func");
    }

    /// Builds a minimal [`Function`] with a `(Utf8, Int64) -> Boolean` signature.
    fn sample_function() -> Function {
        serde_json::from_value(serde_json::json!({
            "inputTypes": ["Utf8", "Int64"],
            "outputType": "Boolean",
            "source": {
                "source": "function my_func(a, b) { return true; }",
                "filename": "test.js"
            }
        }))
        .expect("test function should deserialize")
    }
}
