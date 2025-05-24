use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use async_udf::{async_func::AsyncScalarFunctionArgs, functions::AsyncScalarUDFImpl};
use datafusion::{
    arrow::{array::ArrayRef, datatypes::DataType},
    config::ConfigOptions,
    error::DataFusionError,
    logical_expr::{ColumnarValue, Signature, TypeSignature, Volatility},
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
}

impl JsUdf {
    pub fn new(
        isolate_pool: IsolatePool,
        dataset_name: &str,
        code: Arc<str>,
        script_name: Arc<str>,
        function_name: Arc<str>,
        return_type: DataType,
    ) -> Self {
        Self {
            isolate_pool,
            udf_name: format!("{}.{}", dataset_name, function_name),
            code,
            script_name,
            function_name,
            return_type,
        }
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for JsUdf {
    fn name(&self) -> &str {
        &self.udf_name
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn signature(&self) -> &Signature {
        &Signature {
            type_signature: TypeSignature::VariadicAny,
            volatility: Volatility::Immutable,
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(self.return_type.clone())
    }

    async fn invoke_async_with_args(
        &self,
        args: AsyncScalarFunctionArgs,
        _option: &ConfigOptions,
    ) -> Result<ArrayRef, DataFusionError> {
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

        ScalarValue::iter_to_array(outputs)
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
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    pub const TEST_JS: &str = r#"
function int_in_int_out(i32) {
	if (i32 != 42) {
		throw new Error(
			`assert_eq failed:\n  actual:   ${format(actual)}\n  expected: ${format(expected)}`,
		);
	}
    return 43;
}
"#;

    let isolate_pool = IsolatePool::new();
    let udf = JsUdf::new(
        isolate_pool,
        "",
        TEST_JS.into(),
        "test.js".into(),
        "int_in_int_out".into(),
        DataType::Int32,
    );
    let args = vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(42)))];
    let args = AsyncScalarFunctionArgs {
        args: args.into(),
        number_rows: 1,
        schema: Schema::new(vec![Field::new("js_in", DataType::Int32, false)]).into(),
    };
    let out = udf
        .invoke_async_with_args(args, &ConfigOptions::default())
        .await
        .unwrap();
    assert_eq!(
        ScalarValue::try_from_array(&out, 0).unwrap(),
        ScalarValue::Int32(Some(43))
    );
}
