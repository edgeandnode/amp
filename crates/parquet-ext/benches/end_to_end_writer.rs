use std::{marker, sync::Arc};

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    StructArray,
    builder::{ArrayBuilder, Int32Builder, ListBuilder, StringBuilder},
};
use arrow_schema::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
#[cfg(feature = "brotli")]
use parquet::basic::BrotliLevel;
#[cfg(feature = "flate2")]
use parquet::basic::GzipLevel;
#[cfg(feature = "zstd")]
use parquet::basic::ZstdLevel;
use parquet::{
    arrow::AsyncArrowWriter as ParquetAsyncArrowWriter,
    basic::Compression,
    file::properties::{WriterProperties, WriterPropertiesPtr},
};
use parquet_ext::arrow::async_writer::AsyncArrowWriter as OurAsyncArrowWriter;
use rand::Rng as _;
use tokio::runtime::Runtime;

enum Cardinality {
    Low,
    #[allow(dead_code)]
    High,
}

enum CardinalityGen<T> {
    Low(marker::PhantomData<T>),
    High(marker::PhantomData<T>),
}

impl<T> From<&Cardinality> for CardinalityGen<T> {
    fn from(cardinality: &Cardinality) -> Self {
        match cardinality {
            Cardinality::Low => CardinalityGen::Low(marker::PhantomData),
            Cardinality::High => CardinalityGen::High(marker::PhantomData),
        }
    }
}

impl CardinalityGen<i64> {
    fn gen_range(&self, rng: &mut impl rand::Rng) -> i64 {
        match self {
            CardinalityGen::Low(_) => rng.random_range(0..100), // Low cardinality: 100 unique values
            CardinalityGen::High(_) => rng.random_range(0..1_000_000), // High cardinality: many unique values
        }
    }
}

impl CardinalityGen<String> {
    fn gen_string(&self, rng: &mut impl rand::Rng) -> String {
        match self {
            CardinalityGen::Low(_) => {
                // Low cardinality: choose from a small set of strings
                let choices = ["apple", "banana", "cherry", "date", "elderberry"];
                choices[rng.random_range(0..choices.len())].to_string()
            }
            CardinalityGen::High(_) => {
                // High cardinality: generate random strings
                let len = rng.random_range(5..20);
                (0..len)
                    .map(|_| rng.random_range(b'a'..=b'z') as char)
                    .collect()
            }
        }
    }
}

impl CardinalityGen<f64> {
    fn gen_range(&self, rng: &mut impl rand::Rng) -> f64 {
        match self {
            CardinalityGen::Low(_) => rng.random_range(0.0..100.0), // Low cardinality
            CardinalityGen::High(_) => rng.random_range(0.0..1_000_000.0), // High cardinality
        }
    }
}

impl CardinalityGen<bool> {
    fn gen_bool(&self, rng: &mut impl rand::Rng) -> bool {
        match self {
            CardinalityGen::Low(_) => rng.random_bool(0.9), // 90% chance
            CardinalityGen::High(_) => rng.random_bool(0.5), // 50% chance
        }
    }
}

impl CardinalityGen<i32> {
    fn gen_range(&self, rng: &mut impl rand::Rng) -> i32 {
        match self {
            CardinalityGen::Low(_) => rng.random_range(0..100), // Low cardinality: 100 unique values
            CardinalityGen::High(_) => rng.random_range(0..1_000_000), // High cardinality: many unique values
        }
    }
}

#[allow(dead_code)]
enum SchemaKind {
    /// Simple schema with the given number of columns.
    Simple(usize),
    /// Complex schema with the given number of columns including nested types.
    Complex(usize),
    /// Schema with all supported, non-nested types.
    All,
}

impl SchemaKind {
    fn as_str(&self) -> &'static str {
        match self {
            SchemaKind::Simple(_) => "simple",
            SchemaKind::Complex(_) => "complex",
            SchemaKind::All => "all",
        }
    }

    #[allow(dead_code)]
    fn generate_schema(&self) -> Arc<Schema> {
        match self {
            SchemaKind::Simple(num_columns) => generate_simple_schema(*num_columns),
            SchemaKind::Complex(num_columns) => generate_complex_schema(*num_columns),
            SchemaKind::All => {
                let fields: [Field; 30] = [
                    Field::new("int64_col", DataType::Int64, false),
                    Field::new("int64_col_opt", DataType::Int64, true),
                    Field::new("int32_col", DataType::Int32, false),
                    Field::new("int32_col_opt", DataType::Int32, true),
                    Field::new("uint64_col", DataType::UInt64, false),
                    Field::new("uint64_col_opt", DataType::UInt64, true),
                    Field::new("uint32_col", DataType::UInt32, false),
                    Field::new("uint32_col_opt", DataType::UInt32, true),
                    Field::new("str_col", DataType::Utf8, false),
                    Field::new("str_col_opt", DataType::Utf8, true),
                    Field::new("bin_col", DataType::Binary, false),
                    Field::new("bin_col_opt", DataType::Binary, true),
                    Field::new("bin_20_col", DataType::FixedSizeBinary(20), false),
                    Field::new("bin_20_col_opt", DataType::FixedSizeBinary(20), true),
                    Field::new("float64_col", DataType::Float64, false),
                    Field::new("float64_col_opt", DataType::Float64, true),
                    Field::new("float32_col", DataType::Float32, false),
                    Field::new("float32_col_opt", DataType::Float32, true),
                    Field::new("bool_col", DataType::Boolean, false),
                    Field::new("bool_col_opt", DataType::Boolean, true),
                    Field::new(
                        "ts_millis_col",
                        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new(
                        "ts_millis_col_tz",
                        DataType::Timestamp(
                            arrow_schema::TimeUnit::Millisecond,
                            Some("UTC".into()),
                        ),
                        false,
                    ),
                    Field::new(
                        "ts_millis_col_opt",
                        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                        true,
                    ),
                    Field::new(
                        "ts_micros_col",
                        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                        false,
                    ),
                    Field::new(
                        "ts_micros_col_tz",
                        DataType::Timestamp(
                            arrow_schema::TimeUnit::Microsecond,
                            Some("UTC".into()),
                        ),
                        false,
                    ),
                    Field::new(
                        "ts_micros_col_opt",
                        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                        true,
                    ),
                    Field::new("date64_col", DataType::Date64, false),
                    Field::new("date64_col_opt", DataType::Date64, true),
                    Field::new("date32_col", DataType::Date32, false),
                    Field::new("date32_col_opt", DataType::Date32, true),
                ];
                Arc::new(Schema::new(fields.to_vec()))
            }
        }
    }
}

/// Compression configurations to benchmark.
fn compression_configs() -> Vec<(&'static str, Compression, usize)> {
    vec![
        ("uncompressed", Compression::UNCOMPRESSED, 100),
        #[cfg(feature = "snap")]
        ("snappy", Compression::SNAPPY, 100),
        #[cfg(feature = "flate2")]
        ("gzip", Compression::GZIP(GzipLevel::default()), 10),
        #[cfg(feature = "zstd")]
        ("zstd_1", Compression::ZSTD(ZstdLevel::default()), 100),
        #[cfg(feature = "zstd")]
        (
            "zstd_3",
            Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            50,
        ),
        // #[cfg(feature = "zstd")]
        // ("zstd_5", Compression::ZSTD(ZstdLevel::try_new(5).unwrap())),
        #[cfg(feature = "brotli")]
        ("brotli_1", Compression::BROTLI(BrotliLevel::default()), 100),
        #[cfg(feature = "brotli")]
        (
            "brotli_5",
            Compression::BROTLI(BrotliLevel::try_new(5).unwrap()),
            10,
        ),
        #[cfg(feature = "lz4")]
        ("lz4", Compression::LZ4_RAW, 100),
    ]
}

/// Generate a schema with the given number of columns.
/// Alternates between Int64 and String columns.
fn generate_simple_schema(num_columns: usize) -> Arc<Schema> {
    let fields: Vec<Field> = (0..num_columns)
        .map(|i| {
            if i % 2 == 0 {
                Field::new(format!("col_{i}"), DataType::Int64, false)
            } else {
                Field::new(format!("col_{i}"), DataType::Utf8, false)
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Generate a complex schema with the given number of columns
/// including nested types (i.e. lists, structs) and simple types.
fn generate_complex_schema(num_columns: usize) -> Arc<Schema> {
    let mut fields = Vec::with_capacity(num_columns);

    for i in 0..num_columns {
        let field = match i % 6 {
            // Simple Int64
            0 => Field::new(format!("int_col_{i}"), DataType::Int64, false),
            // Simple String
            1 => Field::new(format!("str_col_{i}"), DataType::Utf8, true),
            // List of Int32
            2 => Field::new(
                format!("list_int_col_{i}"),
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            ),
            // List of Strings
            3 => Field::new(
                format!("list_str_col_{i}"),
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                true,
            ),
            // Struct with Int64 and String fields
            4 => Field::new(
                format!("struct_col_{i}"),
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int64, false),
                        Field::new("name", DataType::Utf8, false),
                        Field::new("value", DataType::Float64, false),
                    ]
                    .into(),
                ),
                false,
            ),
            // Boolean
            5 => Field::new(format!("bool_col_{i}"), DataType::Boolean, false),
            _ => unreachable!(),
        };
        fields.push(field);
    }

    Arc::new(Schema::new(fields))
}

/// Generate a random string of lowercase letters with the given length range.
fn generate_random_string(
    rng: &mut impl rand::Rng,
    min_len: usize,
    max_len: usize,
    cardinality: &CardinalityGen<String>,
) -> String {
    match cardinality {
        CardinalityGen::Low(_) => cardinality.gen_string(rng),
        CardinalityGen::High(_) => {
            let len = rng.random_range(min_len..max_len);
            (0..len)
                .map(|_| rng.random_range(b'a'..=b'z') as char)
                .collect()
        }
    }
}

/// Generate a single batch with the given number of rows.
fn generate_batch(
    schema: &Arc<Schema>,
    num_rows: usize,
    cardinality: &Cardinality,
    rng: &mut impl rand::Rng,
    total_bytes: &mut usize,
) -> RecordBatch {
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| generate_array_for_type(field.data_type(), cardinality, num_rows, rng))
        .collect();

    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let batch_bytes = batch.get_array_memory_size();

    *total_bytes += batch_bytes;

    batch
}

/// Generate an array of the given type with the specified number of rows.
fn generate_array_for_type(
    data_type: &DataType,
    cardinality: &Cardinality,
    num_rows: usize,
    rng: &mut impl rand::Rng,
) -> ArrayRef {
    match data_type {
        DataType::Int64 => {
            let card_gen = CardinalityGen::<i64>::from(cardinality);
            let values: Vec<i64> = (0..num_rows).map(|_| card_gen.gen_range(rng)).collect();
            Arc::new(Int64Array::from(values)) as ArrayRef
        }
        DataType::Int32 => {
            let card_gen = CardinalityGen::<i32>::from(cardinality);
            let values: Vec<i32> = (0..num_rows).map(|_| card_gen.gen_range(rng)).collect();
            Arc::new(Int32Array::from(values)) as ArrayRef
        }
        DataType::Float64 => {
            let card_gen = CardinalityGen::<f64>::from(cardinality);
            let values: Vec<f64> = (0..num_rows).map(|_| card_gen.gen_range(rng)).collect();
            Arc::new(Float64Array::from(values)) as ArrayRef
        }
        DataType::Boolean => {
            let card_gen = CardinalityGen::<bool>::from(cardinality);
            let values: Vec<bool> = (0..num_rows).map(|_| card_gen.gen_bool(rng)).collect();
            Arc::new(BooleanArray::from(values)) as ArrayRef
        }
        DataType::Utf8 => {
            let card_gen = CardinalityGen::<String>::from(cardinality);
            let values: Vec<String> = (0..num_rows)
                .map(|_| generate_random_string(rng, 5, 50, &card_gen))
                .collect();
            Arc::new(StringArray::from(values)) as ArrayRef
        }
        DataType::List(field) => {
            // Generate variable-length lists (0-10 elements each)

            let mut list_builder = match field.data_type() {
                DataType::Int32 => {
                    let value_builder = Int32Builder::new();
                    Box::new(ListBuilder::new(value_builder)) as Box<dyn ArrayBuilder>
                }
                DataType::Utf8 => {
                    let value_builder = StringBuilder::new();
                    Box::new(ListBuilder::new(value_builder)) as Box<dyn ArrayBuilder>
                }
                _ => panic!(
                    "Unsupported list value type in generate_array_for_type: {:?}",
                    field.data_type()
                ),
            };
            for _ in 0..num_rows {
                if rng.random_bool(0.1) {
                    match field.data_type() {
                        DataType::Int32 => {
                            list_builder
                                .as_any_mut()
                                .downcast_mut::<ListBuilder<Int32Builder>>()
                                .unwrap()
                                .append_null();
                        }
                        DataType::Utf8 => {
                            list_builder
                                .as_any_mut()
                                .downcast_mut::<ListBuilder<StringBuilder>>()
                                .unwrap()
                                .append_null();
                        }
                        _ => panic!(
                            "Unsupported list value type in generate_array_for_type: {:?}",
                            field.data_type()
                        ),
                    }
                } else {
                    let list_size = rng.random_range(1..=10) as usize;
                    match field.data_type() {
                        DataType::Int32 => {
                            let card_gen = CardinalityGen::<i32>::from(cardinality);
                            let values: Vec<Option<i32>> = (0..list_size)
                                .map(|_| card_gen.gen_range(rng))
                                .map(Some)
                                .collect();
                            list_builder
                                .as_any_mut()
                                .downcast_mut::<ListBuilder<Int32Builder>>()
                                .unwrap()
                                .append_value(values);
                        }
                        DataType::Utf8 => {
                            let card_gen = CardinalityGen::<String>::from(cardinality);
                            let values: Vec<Option<String>> = (0..list_size)
                                .map(|_| generate_random_string(rng, 5, 50, &card_gen))
                                .map(Some)
                                .collect();
                            list_builder
                                .as_any_mut()
                                .downcast_mut::<ListBuilder<StringBuilder>>()
                                .unwrap()
                                .append_value(values);
                        }
                        _ => panic!(
                            "Unsupported list value type in generate_array_for_type: {:?}",
                            field.data_type()
                        ),
                    };
                }
            }

            Arc::new(list_builder.finish()) as ArrayRef
        }
        DataType::Struct(fields) => {
            let arrays: Vec<ArrayRef> = fields
                .iter()
                .map(|f| generate_array_for_type(f.data_type(), cardinality, num_rows, rng))
                .collect();

            Arc::new(StructArray::new(fields.clone(), arrays, None)) as ArrayRef
        }
        _ => panic!(
            "Unsupported data type in generate_array_for_type: {:?}",
            data_type
        ),
    }
}

/// Generate multiple batches of random lengths that sum to total_rows.
fn generate_batches(
    schema: &Arc<Schema>,
    num_rows: usize,
    cardinality: &Cardinality,
) -> (Vec<RecordBatch>, usize) {
    let mut rng = rand::rng();
    let mut batches = Vec::new();
    let mut total_bytes = 0;
    let mut remaining = num_rows;

    while remaining > 0 {
        // Random batch size between 100 and 10000, capped at remaining
        let max_batch = 10_000.min(remaining);
        let min_batch = 100.min(max_batch);
        let batch_size = if min_batch == max_batch {
            min_batch
        } else {
            rng.random_range(min_batch..=max_batch)
        };
        batches.push(generate_batch(
            schema,
            batch_size,
            cardinality,
            &mut rng,
            &mut total_bytes,
        ));
        remaining -= batch_size;
    }

    (batches, total_bytes)
}

async fn bench_parquet_writer(batches: &[RecordBatch], props: &WriterProperties) {
    let schema = batches[0].schema();
    let mut writer =
        ParquetAsyncArrowWriter::try_new(tokio::io::sink(), schema, Some(props.clone())).unwrap();
    for batch in batches {
        writer.write(batch).await.unwrap();
    }
    writer.close().await.unwrap();
}

async fn bench_our_writer(batches: &[RecordBatch], props: &WriterProperties) {
    let schema = batches[0].schema();
    let mut writer =
        OurAsyncArrowWriter::try_new(tokio::io::sink(), schema, Some(props.clone())).unwrap();
    for batch in batches {
        writer.write(batch).await.unwrap();
    }
    writer.close().await.unwrap();
}

struct BenchInput<'a> {
    batches: &'a [RecordBatch],
    props: WriterPropertiesPtr,
}

fn writer_benchmarks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let columns = [10, 30];
    let rows = [(1_000, 1_000), (100_000, 64_000), (1_000_000, 100_000)];

    let compressions = compression_configs();
    println!(
        "Benchmarking with compressions: {:?}",
        compressions
            .iter()
            .map(|(name, _, _)| *name)
            .collect::<Vec<_>>()
    );

    let mut group = c.benchmark_group("parquet_writer");

    for &num_cols in &columns {
        for schema in [SchemaKind::Complex(num_cols), SchemaKind::Simple(num_cols)] {
            let schema_name = schema.as_str();
            let schema = schema.generate_schema();
            for &(num_rows, max_row_group_size) in &rows {
                let (batches, total_bytes) = generate_batches(&schema, num_rows, &Cardinality::Low);

                for (compression_name, compression, n) in &compressions {
                    let param =
                        format!("{compression_name}/{schema_name}_{num_cols}cols_{num_rows}rows");
                    let props = WriterProperties::builder()
                        .set_compression(*compression)
                        .set_max_row_group_size(max_row_group_size)
                        .build();

                    let input = BenchInput {
                        batches: &batches,
                        props: Arc::new(props),
                    };

                    group
                        .throughput(Throughput::Bytes(total_bytes as u64))
                        .sample_size(*n)
                        .bench_with_input(
                            BenchmarkId::new("this_crate", &param),
                            &input,
                            |b, input| {
                                b.to_async(&rt)
                                    .iter(|| bench_our_writer(input.batches, &input.props));
                            },
                        );

                    group
                        .throughput(Throughput::Bytes(total_bytes as u64))
                        .sample_size(*n)
                        .bench_with_input(
                            BenchmarkId::new("parquet_crate", &param),
                            &input,
                            |b, input| {
                                b.to_async(&rt)
                                    .iter(|| bench_parquet_writer(input.batches, &input.props));
                            },
                        );
                }
            }
        }
    }

    group.finish();
}

criterion_group!(benches, writer_benchmarks);
criterion_main!(benches);
