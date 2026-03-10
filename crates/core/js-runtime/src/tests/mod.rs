use std::{iter, sync::Arc};

use datafusion::{
    arrow::{
        array::{
            ArrayRef, BooleanArray, Int32Array, Int32Builder, ListBuilder, MapBuilder, StringArray,
            StringBuilder, StructArray,
        },
        datatypes::{
            DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION, DataType, Field, IntervalDayTime,
            i256,
        },
    },
    scalar::ScalarValue,
};
use half::f16;

use crate::{convert::ToV8, isolate::Isolate};

pub const TEST_JS: &str = include_str!("scripts/test.js");

#[test]
fn no_params_no_ret() {
    let mut isolate = Isolate::new();
    let ret: i32 = isolate
        .invoke("test.js", TEST_JS, "no_params_no_ret", iter::empty())
        .unwrap();

    assert_eq!(ret, 36);
}

#[test]
fn throws() {
    let mut isolate = Isolate::new();
    let err = isolate
        .invoke::<()>("test.js", TEST_JS, "throws", iter::empty())
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "exception in script: Uncaught: Error: test exception\n --> test.js:6:2-3\n  throw new Error(\"test exception\")\n  ^\n\n\nStack trace:\nError: test exception\n    at throws (test.js:6:9)\n"
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
    let f16_val = ScalarValue::Float16(Some(f16::from_f32(1.5)));
    let interval_day_time_val = ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(5, 3000)));

    let params = vec![
        &ScalarValue::Null as &dyn ToV8,
        &ScalarValue::Boolean(Some(true)) as &dyn ToV8,
        &f16_val,
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
        &ScalarValue::Date32(Some(19000)),
        &ScalarValue::Date64(Some(1_700_000_000_000)),
        &ScalarValue::Time32Second(Some(3661)),
        &ScalarValue::Time32Millisecond(Some(3_661_500)),
        &ScalarValue::Time64Microsecond(Some(3_661_500_000)),
        &ScalarValue::Time64Nanosecond(Some(3_661_500_000_000)),
        &ScalarValue::TimestampSecond(Some(1_700_000_000), None),
        &ScalarValue::DurationMillisecond(Some(86_400_000)),
        &ScalarValue::IntervalYearMonth(Some(14)),
        &interval_day_time_val,
        &ScalarValue::Float16(None),
        &ScalarValue::Date32(None),
        &ScalarValue::TimestampSecond(None, None),
        &ScalarValue::IntervalDayTime(None),
    ];

    isolate
        .invoke::<()>("test.js", TEST_JS, "param_types", params.into_iter())
        .unwrap();
}

#[test]
fn obj_param() {
    let mut isolate = Isolate::new();
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["perf"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Boolean, false)),
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
        ),
    ]);
    let value = ScalarValue::Struct(Arc::new(struct_array));
    let params = iter::once(&value as &dyn ToV8);
    isolate
        .invoke::<()>("test.js", TEST_JS, "obj_param", params)
        .unwrap();
}

#[test]
fn list_param() {
    let mut isolate = Isolate::new();
    let string_builder = StringBuilder::with_capacity(0, 0);
    let mut builder = ListBuilder::new(string_builder);
    builder.values().append_value("1");
    builder.values().append_value("2");
    builder.values().append_value("3");
    builder.append(true);
    let list = ScalarValue::List(Arc::new(builder.finish()));
    let params = iter::once(&list as &dyn ToV8);
    isolate
        .invoke::<()>("test.js", TEST_JS, "list_param", params)
        .unwrap();
}

#[test]
fn return_types() {
    let mut isolate = Isolate::new();
    let ret: ScalarValue = isolate
        .invoke("test.js", TEST_JS, "return_types", iter::empty())
        .unwrap();

    let arrow_struct = match ret {
        ScalarValue::Struct(s) => s,
        _ => panic!("Expected a struct"),
    };

    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(0), 0).unwrap(),
        ScalarValue::Boolean(Some(true))
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(1), 0).unwrap(),
        ScalarValue::Decimal256(
            Some(i256::from_i128(i128::MAX)),
            DECIMAL256_MAX_PRECISION,
            0
        )
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(2), 0).unwrap(),
        ScalarValue::Decimal256(Some(i256::MAX), DECIMAL256_MAX_PRECISION, 0)
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(3), 0).unwrap(),
        ScalarValue::Float64(Some(32.5))
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(4), 0).unwrap(),
        ScalarValue::Float64(Some(64.5))
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(5), 0).unwrap(),
        ScalarValue::Int32(Some(-32768))
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(6), 0).unwrap(),
        ScalarValue::Int32(Some(-2147483648))
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(7), 0).unwrap(),
        ScalarValue::Decimal256(
            Some(i256::from(-9223372036854775808i64)),
            DECIMAL256_MAX_PRECISION,
            0
        )
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(8), 0).unwrap(),
        ScalarValue::Int32(Some(-128))
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(9), 0).unwrap(),
        ScalarValue::Null
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(10), 0).unwrap(),
        ScalarValue::Utf8(Some("data 🇧🇷🇵🇹".to_string()))
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(11), 0).unwrap(),
        ScalarValue::Int32(Some(65535))
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(12), 0).unwrap(),
        ScalarValue::UInt32(Some(4294967295))
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(13), 0).unwrap(),
        ScalarValue::Decimal256(
            Some(i256::from_i128(18446744073709551615)),
            DECIMAL256_MAX_PRECISION,
            0
        )
    );
    assert_eq!(
        ScalarValue::try_from_array(arrow_struct.column(14), 0).unwrap(),
        ScalarValue::Int32(Some(255))
    );
}

#[test]
fn map_param() {
    let mut isolate = Isolate::new();

    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    builder.keys().append_value("x");
    builder.values().append_value(10);
    builder.keys().append_value("y");
    builder.values().append_value(20);
    builder.append(true).unwrap();

    let map = ScalarValue::Map(Arc::new(builder.finish()));
    let params = iter::once(&map as &dyn ToV8);
    isolate
        .invoke::<()>("test.js", TEST_JS, "map_param", params)
        .unwrap();
}

#[test]
fn dictionary_param() {
    let mut isolate = Isolate::new();

    let inner = ScalarValue::Utf8(Some("hello".to_string()));
    let dict = ScalarValue::Dictionary(Box::new(DataType::Int32), Box::new(inner));
    let params = iter::once(&dict as &dyn ToV8);
    isolate
        .invoke::<()>("test.js", TEST_JS, "dictionary_param", params)
        .unwrap();
}
