//! Planning-phase JS UDF representation.
//!
//! [`PlanJsUdf`] implements [`ScalarUDFImpl`] so it can participate in
//! DataFusion logical planning and type-checking, but it carries **no**
//! runtime resources (`IsolatePool`). Any attempt by DataFusion to invoke
//! the UDF before it has been attached to an execution context will panic.

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::DataType,
    common::utils::quote_identifier,
    error::DataFusionError,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
    },
};
use datasets_derived::function::Function;
use js_runtime::isolate_pool::IsolatePool;

use super::exec::ExecJsUdf;

/// Planning-phase representation of a JavaScript user-defined function.
///
/// This type holds the UDF signature and source code needed for logical
/// planning and type-checking. It implements [`ScalarUDFImpl`] so DataFusion
/// can resolve the function during planning, but its `invoke_with_args`
/// implementation panics because execution requires runtime resources
/// attached via [`ExecJsUdf`].
///
/// Construct via [`PlanJsUdf::from_function`].
#[derive(Debug)]
pub struct PlanJsUdf {
    /// Schema-qualified UDF name (e.g. `"ns/dataset@0.0.0".func`).
    udf_name: String,
    /// DataFusion signature built from the function's Arrow input types.
    signature: Signature,
    /// Arrow return type.
    return_type: DataType,
    /// JS source code, retained for the attach phase.
    source_code: Arc<str>,
    /// Filename where the function is defined, retained for the attach phase.
    filename: Arc<str>,
    /// Bare function name (unqualified), retained for the attach phase.
    function_name: Arc<str>,
    /// Schema name (if any), retained for the attach phase so the runtime
    /// UDF preserves the same qualified identity as the planning UDF.
    schema_name: Option<String>,
}

impl PlanJsUdf {
    /// Create a planning JS UDF from a [`Function`] definition.
    ///
    /// The `schema_name` is used to build a schema-qualified UDF name that
    /// matches how DataFusion resolves qualified function references
    /// (e.g. `"namespace/dataset@0.0.0".my_func`).
    pub fn from_function(name: &str, function: &Function, schema_name: Option<&str>) -> Self {
        let schema_owned = match schema_name {
            Some(s) if !s.is_empty() => Some(s.to_string()),
            _ => None,
        };

        let udf_name = match &schema_owned {
            Some(schema) => {
                format!("{}.{}", quote_identifier(schema), name)
            }
            None => name.to_string(),
        };

        let input_types: Vec<DataType> = function
            .input_types
            .iter()
            .map(|dt| dt.clone().into_arrow())
            .collect();
        let signature = Signature::new(TypeSignature::Exact(input_types), Volatility::Immutable);

        Self {
            udf_name,
            signature,
            return_type: function.output_type.clone().into_arrow(),
            source_code: function.source.source.clone(),
            filename: function.source.filename.clone(),
            function_name: Arc::from(name),
            schema_name: schema_owned,
        }
    }

    /// The JS source code, needed at attach time to construct the runtime UDF.
    pub fn source_code(&self) -> &Arc<str> {
        &self.source_code
    }

    /// The filename where the function is defined, needed at attach time.
    pub fn filename(&self) -> &Arc<str> {
        &self.filename
    }

    /// The bare (unqualified) function name.
    pub fn function_name(&self) -> &Arc<str> {
        &self.function_name
    }

    /// The Arrow output type, needed at attach time to construct the runtime UDF.
    pub fn output_type(&self) -> &DataType {
        &self.return_type
    }

    /// The schema name (if any), needed at attach time to preserve qualified
    /// UDF identity in the runtime.
    pub fn schema_name(&self) -> Option<&str> {
        self.schema_name.as_deref()
    }

    /// Attach runtime resources to produce an executable JS UDF.
    ///
    /// This is the **only** way to obtain an [`ExecJsUdf`]. The returned
    /// value wraps an executable `ScalarUDF` backed by the given
    /// `IsolatePool`.
    pub fn attach(&self, pool: IsolatePool) -> ExecJsUdf {
        ExecJsUdf::from_plan(self, pool)
    }
}

impl PartialEq for PlanJsUdf {
    fn eq(&self, other: &Self) -> bool {
        self.udf_name == other.udf_name
            && self.return_type == other.return_type
            && self.signature == other.signature
            && self.function_name == other.function_name
            && self.schema_name == other.schema_name
    }
}

impl Eq for PlanJsUdf {}

impl std::hash::Hash for PlanJsUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.udf_name.hash(state);
        self.return_type.hash(state);
        self.signature.hash(state);
        self.function_name.hash(state);
        self.schema_name.hash(state);
    }
}

impl ScalarUDFImpl for PlanJsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.udf_name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        unreachable!(
            "PlanJsUdf '{}' must be attached to an execution context before invocation",
            self.udf_name,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::datatypes::DataType,
        logical_expr::{ScalarUDF, ScalarUDFImpl, TypeSignature},
    };
    use datasets_derived::function::Function;

    use super::*;

    #[test]
    fn from_function_without_schema_uses_bare_name() {
        //* Given
        let function = sample_function();

        //* When
        let plan = PlanJsUdf::from_function("my_func", &function, None);

        //* Then
        assert_eq!(plan.name(), "my_func");
        assert_eq!(
            plan.return_type(&[]).expect("return_type should succeed"),
            DataType::Boolean
        );
        assert_eq!(plan.function_name().as_ref(), "my_func");
        let sig = plan.signature();
        assert!(
            matches!(&sig.type_signature, TypeSignature::Exact(types) if types == &[DataType::Utf8, DataType::Int64]),
            "unexpected signature: {:?}",
            sig.type_signature
        );
    }

    #[test]
    fn from_function_with_schema_produces_qualified_name() {
        //* Given
        let function = sample_function();

        //* When
        let plan = PlanJsUdf::from_function("my_func", &function, Some("ns/dataset@0.0.0"));

        //* Then
        assert_eq!(plan.name(), "\"ns/dataset@0.0.0\".my_func");
    }

    #[test]
    fn from_function_with_empty_schema_uses_bare_name() {
        //* Given
        let function = sample_function();

        //* When
        let plan = PlanJsUdf::from_function("my_func", &function, Some(""));

        //* Then
        assert_eq!(plan.name(), "my_func");
    }

    #[test]
    #[should_panic(expected = "must be attached")]
    fn invoke_with_args_before_attach_panics() {
        //* Given
        use datafusion::{arrow::datatypes::Field, config::ConfigOptions};

        let function = sample_function();
        let plan = PlanJsUdf::from_function("my_func", &function, None);
        let udf = ScalarUDF::new_from_impl(plan);
        let args = ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        //* When
        let _ = udf.inner().invoke_with_args(args);

        //* Then — should_panic verifies the panic message
    }

    #[test]
    fn source_with_valid_function_retains_original_source() {
        //* Given
        let function = sample_function();

        //* When
        let plan = PlanJsUdf::from_function("my_func", &function, None);

        //* Then
        assert_eq!(plan.filename().as_ref(), "test.js");
        assert_eq!(
            plan.source_code().as_ref(),
            "function f(a, b) { return true; }"
        );
    }

    /// Builds a minimal [`Function`] with a `(Utf8, Int64) -> Boolean` signature.
    fn sample_function() -> Function {
        serde_json::from_value(serde_json::json!({
            "inputTypes": ["Utf8", "Int64"],
            "outputType": "Boolean",
            "source": {
                "source": "function f(a, b) { return true; }",
                "filename": "test.js"
            }
        }))
        .expect("test function should deserialize")
    }
}
