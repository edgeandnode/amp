use datafusion::arrow::array;
use datafusion::arrow::array::Array;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::array::AsArray as _;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::IntervalDayTime;
use datafusion::arrow::datatypes::IntervalMonthDayNano;
use datafusion::arrow::datatypes::IntervalUnit;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::Accumulator;
use datafusion::logical_expr::AggregateUDFImpl;
use datafusion::logical_expr::Signature;
use datafusion::logical_expr::Volatility;
use datafusion::scalar::ScalarValue;
use stable_hash::FieldAddress;
use stable_hash::StableHash as _;
use stable_hash::StableHasher;

#[derive(Debug)]
pub struct AttestationHasherUDF(Signature);

impl AttestationHasherUDF {
    pub fn new() -> Self {
        Self(Signature::variadic_any(Volatility::Immutable))
    }
}

impl AggregateUDFImpl for AttestationHasherUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "attestation_hash"
    }

    fn signature(&self) -> &Signature {
        &self.0
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> datafusion::error::Result<Box<dyn Accumulator>> {
        Ok(Box::new(AttestationHasher::new(args.schema.clone())))
    }
}

type Hasher = stable_hash::fast::FastStableHasher;

pub struct AttestationHasher {
    schema: Schema,
    set_hasher: Hasher,
}

impl AttestationHasher {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            set_hasher: Hasher::new(),
        }
    }
}

impl std::fmt::Debug for AttestationHasher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AttestationHasher(..)")
    }
}

impl Accumulator for AttestationHasher {
    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        let hash = self.set_hasher.finish().to_le_bytes();
        Ok(ScalarValue::Binary(Some(hash.to_vec())))
    }

    fn size(&self) -> usize {
        let hasher_state_bytes = 32;
        assert!(hasher_state_bytes == std::mem::size_of::<<Hasher as StableHasher>::Bytes>());
        hasher_state_bytes
    }

    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        let state = self.set_hasher.to_bytes();
        Ok(vec![ScalarValue::Binary(Some(state.to_vec()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert!(states.len() == 1);
        let states = &states[0];
        for state in states.as_binary_view() {
            let mut bytes = <Hasher as StableHasher>::Bytes::default();
            match state {
                Some(state) => bytes.copy_from_slice(state),
                None => continue,
            };
            self.set_hasher.mixin(&Hasher::from_bytes(bytes));
        }
        Ok(())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        let fields = self.schema.fields();
        if values.is_empty() || fields.is_empty() {
            return Ok(());
        }

        let mut row_hashers: Vec<Hasher> = Default::default();
        for ((field_index, field), column) in fields.into_iter().enumerate().zip(values) {
            hash_column(&mut row_hashers, field, field_index, &column)?;
        }

        for row in &row_hashers {
            let addr = <Hasher as StableHasher>::Addr::root();
            self.set_hasher.write(addr, row.to_bytes().as_ref());
        }
        Ok(())
    }

    // TODO: support retract_batch?
}

fn hash_column(
    row_hashers: &mut Vec<Hasher>,
    field: &Field,
    field_index: usize,
    data: &dyn Array,
) -> datafusion::error::Result<()> {
    fn hash_column_inner<I, V>(row_hashers: &mut Vec<Hasher>, field_index: usize, field_values: I)
    where
        I: IntoIterator<Item = Option<V>>,
        V: AsRef<[u8]>,
    {
        for (i, value) in field_values.into_iter().enumerate() {
            if row_hashers.len() <= i {
                row_hashers.push(Hasher::new());
            }
            let field_address = <Hasher as StableHasher>::Addr::root().child(field_index as u64);
            let v: Option<&[u8]> = value.as_ref().map(|v| v.as_ref());
            v.stable_hash(field_address, &mut row_hashers[i]);
        }
    }
    macro_rules! hash {
        ($t:ty, $f:expr) => {{
            let data = data.as_any().downcast_ref::<$t>().unwrap();
            let field_values = data.into_iter().map(|v| v.map($f));
            hash_column_inner(row_hashers, field_index, field_values);
        }};
    }
    match field.data_type() {
        DataType::Null => (),
        DataType::Boolean => hash!(array::BooleanArray, |v| if v { [1_u8] } else { [0_u8] }),
        DataType::Int8 => hash!(array::Int8Array, |v| v.to_le_bytes()),
        DataType::Int16 => hash!(array::Int16Array, |v| v.to_le_bytes()),
        DataType::Int32 => hash!(array::Int32Array, |v| v.to_le_bytes()),
        DataType::Int64 => hash!(array::Int64Array, |v| v.to_le_bytes()),
        DataType::UInt8 => hash!(array::UInt8Array, |v| v.to_le_bytes()),
        DataType::UInt16 => hash!(array::UInt16Array, |v| v.to_le_bytes()),
        DataType::UInt32 => hash!(array::UInt32Array, |v| v.to_le_bytes()),
        DataType::UInt64 => hash!(array::UInt64Array, |v| v.to_le_bytes()),
        DataType::Decimal128(_, _) => hash!(array::Decimal128Array, |v| v.to_le_bytes()),
        DataType::Decimal256(_, _) => hash!(array::Decimal256Array, |v| v.to_le_bytes()),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => hash!(array::TimestampSecondArray, |v| v.to_le_bytes()),
            TimeUnit::Millisecond => hash!(array::TimestampMillisecondArray, |v| v.to_le_bytes()),
            TimeUnit::Microsecond => hash!(array::TimestampMicrosecondArray, |v| v.to_le_bytes()),
            TimeUnit::Nanosecond => hash!(array::TimestampNanosecondArray, |v| v.to_le_bytes()),
        },
        DataType::Date32 => hash!(array::Date32Array, |v| v.to_le_bytes()),
        DataType::Date64 => hash!(array::Date64Array, |v| v.to_le_bytes()),
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => hash!(array::Time32SecondArray, |v| v.to_le_bytes()),
            TimeUnit::Millisecond => hash!(array::Time32MillisecondArray, |v| v.to_le_bytes()),
            TimeUnit::Microsecond | TimeUnit::Nanosecond => unreachable!(),
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Second | TimeUnit::Millisecond => unreachable!(),
            TimeUnit::Microsecond => hash!(array::Time64MicrosecondArray, |v| v.to_le_bytes()),
            TimeUnit::Nanosecond => hash!(array::Time64NanosecondArray, |v| v.to_le_bytes()),
        },
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => hash!(array::DurationSecondArray, |v| v.to_le_bytes()),
            TimeUnit::Millisecond => hash!(array::DurationMillisecondArray, |v| v.to_le_bytes()),
            TimeUnit::Microsecond => hash!(array::DurationMicrosecondArray, |v| v.to_le_bytes()),
            TimeUnit::Nanosecond => hash!(array::DurationNanosecondArray, |v| v.to_le_bytes()),
        },
        DataType::Interval(unit) => match unit {
            IntervalUnit::YearMonth => hash!(array::IntervalYearMonthArray, |v| v.to_le_bytes()),
            IntervalUnit::DayTime => hash!(
                array::IntervalDayTimeArray,
                |IntervalDayTime { days, milliseconds }| {
                    let mut bytes = [0_u8; 8];
                    bytes[0..4].copy_from_slice(&days.to_le_bytes());
                    bytes[4..8].copy_from_slice(&milliseconds.to_le_bytes());
                    bytes
                }
            ),
            IntervalUnit::MonthDayNano => {
                hash!(
                    array::IntervalMonthDayNanoArray,
                    |IntervalMonthDayNano {
                         months,
                         days,
                         nanoseconds,
                     }| {
                        let mut bytes = [0_u8; 16];
                        bytes[0..4].copy_from_slice(&months.to_le_bytes());
                        bytes[4..8].copy_from_slice(&days.to_le_bytes());
                        bytes[8..16].copy_from_slice(&nanoseconds.to_le_bytes());
                        bytes
                    }
                )
            }
        },
        DataType::FixedSizeBinary(_) => hash!(array::FixedSizeBinaryArray, |v| v),
        DataType::Binary => hash!(array::BinaryArray, |v| v),
        DataType::LargeBinary => hash!(array::LargeBinaryArray, |v| v),
        DataType::BinaryView => hash!(array::BinaryViewArray, |v| v),
        DataType::Utf8 => hash!(array::StringArray, |v| v),
        DataType::LargeUtf8 => hash!(array::LargeStringArray, |v| v),
        DataType::Utf8View => hash!(array::StringViewArray, |v| v),
        t @ DataType::Float16
        | t @ DataType::Float32
        | t @ DataType::Float64
        | t @ DataType::List(_)
        | t @ DataType::ListView(_)
        | t @ DataType::FixedSizeList(_, _)
        | t @ DataType::LargeList(_)
        | t @ DataType::LargeListView(_)
        | t @ DataType::Struct(_)
        | t @ DataType::Union(_, _)
        | t @ DataType::Dictionary(_, _)
        | t @ DataType::Map(_, _)
        | t @ DataType::RunEndEncoded(_, _) => {
            return Err(DataFusionError::NotImplemented(format!(
                "hash column type {t}"
            )))
        }
    };
    Ok(())
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::sync::Arc;

    use alloy::hex::ToHexExt;
    use datafusion::arrow::array::ArrayRef;
    use datafusion::arrow::array::AsArray as _;
    use datafusion::arrow::array::PrimitiveBuilder;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::datatypes::Field;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::datatypes::UInt8Type;
    use datafusion::logical_expr::Accumulator;
    use datafusion::scalar::ScalarValue;
    use rand::prelude::SliceRandom as _;
    use rand::rng;
    use rand::rngs::StdRng;
    use rand::Rng as _;
    use rand::RngCore;
    use rand::SeedableRng;

    #[derive(Debug)]
    struct TestRecords(Vec<TestRow>);

    #[derive(Debug, Hash, PartialEq, Eq)]
    struct TestRow(Vec<Option<u8>>);

    impl PartialEq for TestRecords {
        fn eq(&self, other: &Self) -> bool {
            let a = self.0.iter().collect::<HashSet<&TestRow>>();
            let b = other.0.iter().collect::<HashSet<&TestRow>>();
            a == b
        }
    }

    impl TestRecords {
        fn schema(column_count: usize) -> Schema {
            let fields: Vec<Field> = (0..column_count)
                .map(|i| Field::new(i.to_string(), DataType::UInt8, true))
                .collect();
            Schema::new(fields)
        }

        fn new(rng: &mut impl RngCore, schema: &Schema) -> Self {
            let column_count = schema.fields().len();
            let row_count = rng.random_range(1..=5);
            let mut gen_element = || rng.random_bool(0.8).then(|| rng.random_range(0..=3));
            let mut gen_row = || TestRow((0..column_count).map(|_| gen_element()).collect());
            let rows: HashSet<TestRow> = (0..row_count).map(|_| gen_row()).collect();
            Self(rows.into_iter().collect())
        }

        fn from_record_batch(batch: RecordBatch) -> Self {
            let mut rows: Vec<TestRow> = (0..batch.num_rows())
                .map(|_| TestRow(Vec::with_capacity(batch.num_columns())))
                .collect();
            for column in batch.columns() {
                for (row, value) in column.as_primitive::<UInt8Type>().into_iter().enumerate() {
                    rows[row].0.push(value);
                }
            }
            Self(rows.into_iter().collect())
        }

        fn to_record_batch(&self, schema: &Schema) -> RecordBatch {
            let columns = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(f, _)| {
                    let mut array = PrimitiveBuilder::<UInt8Type>::new();
                    for row in &self.0 {
                        array.append_option(row.0[f]);
                    }
                    Arc::new(array.finish()) as ArrayRef
                })
                .collect();
            RecordBatch::try_new(schema.clone().into(), columns).unwrap()
        }
    }

    fn hash_records(schema: &Schema, records: &TestRecords) -> Vec<u8> {
        let batch = records.to_record_batch(schema);
        let mut udf = super::AttestationHasher::new(schema.clone());
        udf.update_batch(batch.columns()).unwrap();
        match udf.evaluate().unwrap() {
            ScalarValue::Binary(Some(bytes)) => bytes,
            _ => unreachable!(),
        }
    }

    #[test]
    /// forall t in Table: t = TestRecords::from_record_batch(t.to_record_batch())
    fn test_batch_serialize() {
        let seed = rng().next_u64();
        println!("seed: {seed}");
        let mut rng = StdRng::seed_from_u64(seed);
        for _ in 0..1_000 {
            let schema = TestRecords::schema(rng.random_range(1..=3));
            let records = TestRecords::new(&mut rng, &schema);
            assert_eq!(
                records,
                TestRecords::from_record_batch(records.to_record_batch(&schema))
            );
        }
    }

    #[test]
    /// forall t in Table: hash(t) = hash(shuffle(t))
    fn test_hash_shuffle() {
        let seed = rng().next_u64();
        println!("seed: {seed}");
        let mut rng = StdRng::seed_from_u64(seed);
        for _ in 0..1_000 {
            let schema = TestRecords::schema(rng.random_range(1..=3));
            let mut records = TestRecords::new(&mut rng, &schema);
            let hash1 = hash_records(&schema, &records);
            records.0.shuffle(&mut rng);
            let hash2 = hash_records(&schema, &records);
            assert_eq!(hash1.encode_hex(), hash2.encode_hex());
            records.0.pop();
            let hash3 = hash_records(&schema, &records);
            assert_ne!(hash1.encode_hex(), hash3.encode_hex());
        }
    }

    #[test]
    /// forall a, b in Table: (a = b) = (hash(a) = hash(b))
    fn test_hash_eq() {
        let seed = rng().next_u64();
        println!("seed: {seed}");
        let mut rng = StdRng::seed_from_u64(seed);
        for _ in 0..1_000 {
            let schema = TestRecords::schema(rng.random_range(1..=3));
            let a = TestRecords::new(&mut rng, &schema);
            let b = TestRecords::new(&mut rng, &schema);
            assert_eq!(
                a == b,
                hash_records(&schema, &a) == hash_records(&schema, &b)
            )
        }
    }

    #[test]
    fn test_hash_string() {
        let schema = Schema::new(vec![Field::new("1", DataType::Utf8, false)]);
        let mut column = datafusion::arrow::array::StringBuilder::new();
        column.append_value("foo");
        let column = column.finish();
        let mut udf = super::AttestationHasher::new(schema.clone());
        udf.update_batch(&[Arc::new(column)]).unwrap();
        let hash = match udf.evaluate().unwrap() {
            ScalarValue::Binary(Some(bytes)) => bytes,
            _ => unreachable!(),
        };
        assert_eq!(hash.encode_hex(), "d6e778fc6b523cb0cdff0c32b9e7a570");
    }
}
