use datafusion::{
    arrow::datatypes::{i256, DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION},
    scalar::ScalarValue,
};

use crate::{convert::ToV8, isolate::Isolate};

pub const TEST_JS: &str = include_str!("scripts/test.js");

#[test]
fn no_params_no_ret() {
    let mut isolate = Isolate::new();
    let ret: i32 = isolate
        .invoke("test.js", TEST_JS, "no_params_no_ret", &[])
        .unwrap();

    assert_eq!(ret, 36);
}

#[test]
fn throws() {
    let mut isolate = Isolate::new();
    let err = isolate
        .invoke::<()>("test.js", TEST_JS, "throws", &[])
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        r#"exception in script: Uncaught: Error: test exception
 --> test.js:6:1-2
	throw new Error("test exception");
 ^


Stack trace:
Error: test exception
    at throws (test.js:6:8)
"#
    );
}

#[test]
fn param_types() {
    let mut isolate = Isolate::new();

    let utf8 = &ScalarValue::Utf8(Some("data 🇧🇷🇵🇹".to_string())) as &dyn ToV8;
    let binary = &ScalarValue::Binary(Some(
        hex::decode("c944E90C64B2c07662A292be6244BDf05Cda44a7").unwrap(),
    )) as &dyn ToV8;
    let decimal128 =
        &ScalarValue::Decimal128(Some(i128::MAX), DECIMAL128_MAX_PRECISION, 0) as &dyn ToV8;
    let decimal256 =
        &ScalarValue::Decimal256(Some(i256::MAX), DECIMAL256_MAX_PRECISION, 0) as &dyn ToV8;

    let params = vec![
        &ScalarValue::Null as &dyn ToV8,
        &ScalarValue::Boolean(Some(true)) as &dyn ToV8,
        &ScalarValue::Float32(Some(32.5)) as &dyn ToV8,
        &ScalarValue::Float64(Some(64.5)) as &dyn ToV8,
        &ScalarValue::Int8(Some(-128)) as &dyn ToV8,
        &ScalarValue::Int16(Some(-32768)) as &dyn ToV8,
        &ScalarValue::UInt8(Some(255)) as &dyn ToV8,
        &ScalarValue::UInt16(Some(65535)) as &dyn ToV8,
        &ScalarValue::UInt32(Some(4294967295)) as &dyn ToV8,
        &ScalarValue::UInt64(Some(18446744073709551615)) as &dyn ToV8,
        &ScalarValue::Int32(Some(-2147483648)) as &dyn ToV8,
        &ScalarValue::Int64(Some(-9223372036854775808)) as &dyn ToV8,
        utf8,
        binary,
        decimal128,
        decimal256,
    ];

    isolate
        .invoke::<()>("test.js", TEST_JS, "param_types", params.as_slice())
        .unwrap();
}
