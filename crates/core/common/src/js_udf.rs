use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::DataType,
    error::DataFusionError,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
        async_udf::AsyncScalarUDFImpl,
    },
    scalar::ScalarValue,
};
use js_runtime::{convert::ToV8, isolate_pool::IsolatePool};

#[derive(Debug)]
pub(crate) struct JsUdf {
    udf_name: String,
    isolate_pool: IsolatePool,
    code: Arc<str>,          // JS source code
    script_name: Arc<str>,   // For displaying in error stack traces
    function_name: Arc<str>, // Name of the JS function to be called
    return_type: DataType,
    signature: Signature,
}

impl PartialEq for JsUdf {
    fn eq(&self, other: &Self) -> bool {
        self.udf_name == other.udf_name
            && self.code == other.code
            && self.script_name == other.script_name
            && self.function_name == other.function_name
            && self.return_type == other.return_type
            && self.signature == other.signature
    }
}

impl Eq for JsUdf {}

impl std::hash::Hash for JsUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.udf_name.hash(state);
        self.code.hash(state);
        self.script_name.hash(state);
        self.function_name.hash(state);
        self.return_type.hash(state);
        self.signature.hash(state);
    }
}

impl JsUdf {
    pub fn new(
        isolate_pool: IsolatePool,
        catalog_schema: String,
        code: Arc<str>,
        script_name: Arc<str>,
        function_name: Arc<str>,
        input_types: Vec<DataType>,
        return_type: DataType,
    ) -> Self {
        let signature = Signature {
            type_signature: TypeSignature::Exact(input_types),
            volatility: Volatility::Immutable,
        };
        Self {
            isolate_pool,
            udf_name: format!("{}.{}", catalog_schema, function_name),
            code,
            script_name,
            function_name,
            return_type,
            signature,
        }
    }
}

impl ScalarUDFImpl for JsUdf {
    fn name(&self) -> &str {
        &self.udf_name
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(self.return_type.clone())
    }

    /// Since this is an async UDF, the `invoke_with_args` method will not be called.
    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        unreachable!("is only called as async UDF");
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for JsUdf {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        // First, transpose the arguments row format
        let mut js_params_batch: Vec<Vec<Box<dyn ToV8>>> = vec![];
        let num_rows = args.number_rows;
        for i in 0..num_rows {
            let mut row = vec![];
            for arg in args.args.iter() {
                row.push(Box::new(columnar_to_scalar(arg, i)?) as Box<dyn ToV8>);
            }
            js_params_batch.push(row);
        }

        // This use of `invoke_batch`, while optimal for performance, may result in non-determinism
        // as the global JS state is shared across all invocations.
        //
        // A way to force determinism would be to not batch. The ideal solution would be to somehow
        // have deterministic batch sizes.
        let outputs = self
            .isolate_pool
            .invoke_batch::<ScalarValue>(
                self.script_name.as_ref(),
                self.code.as_ref(),
                self.function_name.as_ref(),
                js_params_batch,
            )
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;

        ScalarValue::iter_to_array(outputs).map(ColumnarValue::Array)
    }
}

/// From `ColumnarValue` to `ScalarValue`.
fn columnar_to_scalar(
    columnar_value: &ColumnarValue,
    row_idx: usize,
) -> Result<ScalarValue, DataFusionError> {
    match columnar_value {
        ColumnarValue::Scalar(scalar) => Ok(scalar.clone()),
        ColumnarValue::Array(array) => ScalarValue::try_from_array(array, row_idx),
    }
}

#[tokio::test]
async fn js_udf_smoke_test() {
    use datafusion::{
        arrow::datatypes::{DataType, Field},
        logical_expr::ReturnFieldArgs,
    };

    pub const TEST_JS: &str = indoc::indoc! { r#"
        function int_in_int_out(i32) {
            if (i32 != 42) {
                throw new Error(
                    `assert_eq failed:\n  actual:   ${format(actual)}\n  expected: ${format(expected)}`,
                );
            }
            return 43;
        }
"#};

    let isolate_pool = IsolatePool::new();
    let udf = JsUdf::new(
        isolate_pool,
        String::new(),
        TEST_JS.into(),
        "test.js".into(),
        "int_in_int_out".into(),
        vec![DataType::Int32],
        DataType::Int32,
    );
    let arg_scalar = ScalarValue::Int32(Some(42));
    let args = vec![ColumnarValue::Scalar(arg_scalar.clone())];
    let arg_fields = vec![Arc::new(Field::new("js_in", DataType::Int32, false))];
    let ret_args = ReturnFieldArgs {
        arg_fields: &arg_fields,
        scalar_arguments: &[Some(&arg_scalar)],
    };
    let return_field = udf.return_field_from_args(ret_args).unwrap();

    let args = ScalarFunctionArgs {
        args,
        arg_fields,
        return_field,
        number_rows: 1,
        config_options: Default::default(),
    };
    let out = udf.invoke_async_with_args(args).await.unwrap();
    let array = match out {
        ColumnarValue::Array(arr) => arr,
        ColumnarValue::Scalar(_) => panic!("expected array"),
    };
    assert_eq!(
        ScalarValue::try_from_array(&array, 0).unwrap(),
        ScalarValue::Int32(Some(43))
    );
}
