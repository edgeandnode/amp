use std::sync::Arc;

use anyhow::Context as _;
use prost_reflect::{DynamicMessage, Value};
use prost::Message as _;

use super::tables::{Tables, OutputType};
use crate::proto::sf::substreams::{rpc::v2::BlockScopedData, sink::database::v1::{table_change, DatabaseChanges, TableChange}};
use common::{
    arrow::{array::*, datatypes::{Schema, DataType as ArrowDataType}},
    parquet::data_type::AsBytes as _,
    DatasetRows, TableRows, BLOCK_NUM,
};

pub fn pb_to_rows(block: BlockScopedData, tables: &Tables) -> Result<DatasetRows, anyhow::Error> {
    let mut table_rows = DatasetRows(Vec::new());

    let block_num = block.clock.unwrap().number;
    let value = block
        .output
        .context("no output")?
        .map_output
        .context("module output is empty")?
        .value;

    match tables.output_type {
        OutputType::Proto => {
            let dynamic_message =
                DynamicMessage::decode(tables.message_descriptor.clone(), value.as_bytes())?;
            for (field, value) in dynamic_message.fields() {
                let list = match value {
                    Value::List(list) => list,
                    _ => continue,
                };
                let table = tables
                    .tables
                    .iter()
                    .find(|t| t.name == field.name())
                    .context(format!("table not found: {}", field.name()))?;
                let rows = message_to_rows(list, table.schema.clone(), block_num)?;
                table_rows.0.push(TableRows {
                    rows,
                    table: table.clone(),
                });
            }
        }
        OutputType::DbOut => {
            let changes = DatabaseChanges::decode(value.as_bytes())?;
            for table in &tables.tables {
                let table_changes = changes
                    .table_changes
                    .iter()
                    .filter(|change|
                        change.table == table.name
                        && table_change::Operation::try_from(change.operation).unwrap() == table_change::Operation::Create
                    )
                    .collect::<Vec<_>>();
                if table_changes.is_empty() {
                    continue;
                }
                let rows = table_change_to_rows(&table_changes, table.schema.clone(), block_num)?;
                table_rows.0.push(TableRows {
                    rows,
                    table: table.clone(),
                });
            }
        }
        OutputType::Entities => {
            todo!("Entities output type not implemented")
        }
    }
    Ok(table_rows)
}

fn table_change_to_rows(changes: &[&TableChange], schema: Arc<Schema>, block_num: u64) -> Result<RecordBatch, anyhow::Error> {
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
    let row_count = changes.len();

    for column in schema.fields().iter() {
        let col_name = column.name();
        if col_name == BLOCK_NUM {
            let mut builder = UInt64Builder::with_capacity(row_count);
            builder.append_slice(&vec![block_num; row_count]);
            columns.push(Arc::new(builder.finish()));
            continue;
        }
        let col_iter = changes.iter().map(|change| {
            change
                .fields
                .iter()
                .find(|&field| field.name == *col_name)
                .map(|field| field.new_value.clone())
        });

        match column.data_type() {
            ArrowDataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| Some(field.parse().expect("field type should be an i64"))));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            ArrowDataType::Int32 => {
                let mut builder = Int32Builder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| Some(field.parse().expect("field type should be an i32"))));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            ArrowDataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(row_count, 0);
                let cols = col_iter
                    .map(|field| field.and_then(|field| Some(field.to_string())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            ArrowDataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| Some(field.parse::<bool>().expect("field type should be a boolean"))));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            ArrowDataType::UInt64 => {
                let mut builder = UInt64Builder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| Some(field.parse().expect("field type should be a u64"))));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            ArrowDataType::Timestamp(_, _) => {
                let mut builder = TimestampNanosecondBuilder::with_capacity(row_count).with_data_type(common::timestamp_type());
                let cols = col_iter
                    .map(|field| field
                        .and_then(|field| Some(
                            chrono::DateTime::parse_from_rfc3339(field.as_str())
                                .expect("field type should be a timestamp")
                                .timestamp_nanos_opt()
                                .expect("failed to get nanoseconds")
                            )
                        )
                    );
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            ArrowDataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| Some(field.parse().expect("field type should be a float"))));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            ArrowDataType::Float32 => {
                let mut builder = Float32Builder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| Some(field.parse().expect("field type should be a float"))));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            ArrowDataType::Date32 => {
                let mut builder = Date32Builder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| Some(field.parse().expect("field type should be a date"))));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            ArrowDataType::Binary => {
                let mut builder = BinaryBuilder::with_capacity(row_count, 0);
                let cols = col_iter
                    .map(|field| field.and_then(|field| Some(field.as_bytes().to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            _ => todo!("field type {} not implemented", column.data_type()),
        }
    }

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn message_to_rows(
    list: &[Value],
    schema: Arc<Schema>,
    block_num: u64,
) -> Result<RecordBatch, anyhow::Error> {
    let message = list
        .get(0)
        .context("empty list")?
        .as_message()
        .context("field type should be a message")?;
    let row_count = list.len();
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields().iter() {
        let col_name = field.name();
        if col_name == BLOCK_NUM {
            let mut builder = UInt64Builder::with_capacity(row_count);
            builder.append_slice(&vec![block_num; row_count]);
            columns.push(Arc::new(builder.finish()));
            continue;
        }
        let field = message
            .get_field_by_name(col_name)
            .context(format!("field not found: {col_name}"))?;
        let col_iter = list.iter().map(|row| {
            row.as_message()
                .and_then(|msg| msg.get_field_by_name(col_name))
        });
        match *field {
            Value::String(_) => {
                let mut builder = StringBuilder::with_capacity(row_count, 0);
                let cols = col_iter
                    .map(|field| field.and_then(|field| field.as_str().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            Value::U32(_) => {
                let mut builder = UInt32Builder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| field.as_u32().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            Value::U64(_) => {
                let mut builder = UInt64Builder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| field.as_u64().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            Value::I32(_) => {
                let mut builder = Int32Builder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| field.as_i32().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            Value::I64(_) => {
                let mut builder = Int64Builder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| field.as_i64().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            Value::F32(_) => {
                let mut builder = Float32Builder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| field.as_f32().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            Value::F64(_) => {
                let mut builder = Float64Builder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| field.as_f64().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            Value::Bytes(_) => {
                let mut builder = BinaryBuilder::with_capacity(row_count, 0);
                let cols = col_iter
                    .map(|field| field.and_then(|field| field.as_bytes().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            Value::Bool(_) => {
                let mut builder = BooleanBuilder::with_capacity(row_count);
                let cols = col_iter
                    .map(|field| field.and_then(|field| field.as_bool().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            Value::EnumNumber(_) => {
                let mut builder = Int32Builder::with_capacity(row_count);
                let cols = col_iter.map(|field| {
                    field.and_then(|field| field.as_enum_number().map(|s| s.to_owned()))
                });
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            Value::Message(_) => {
                let mut builder = StringBuilder::with_capacity(row_count, 0);
                let cols = col_iter
                    .map(|field| field.and_then(|field| field.as_message().map(|s| s.to_string())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
            Value::List(_) | Value::Map(_) => {
                let mut builder = StringBuilder::with_capacity(row_count, 0);
                let cols = col_iter.map(|field| field.and_then(|field| Some(field.to_string())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            }
        }
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use common::arrow::datatypes::*;
    use prost_reflect::DescriptorPool;
    use std::sync::Arc;

    #[test]
    fn test_message_to_rows() -> Result<()> {
        let fields = vec![
            (
                "i32",
                vec![Value::I32(123412), Value::I32(345345), Value::I32(1234123)],
                DataType::Int32,
            ),
            (
                "i64",
                vec![
                    Value::I64(5435234),
                    Value::I64(312312342),
                    Value::I64(1234123413),
                ],
                DataType::Int64,
            ),
            (
                "str",
                vec![
                    Value::String("test".to_string()),
                    Value::String("test test".to_string()),
                    Value::String("test test test".to_string()),
                ],
                DataType::Utf8,
            ),
            (
                "boolean",
                vec![Value::Bool(true), Value::Bool(false), Value::Bool(true)],
                DataType::Boolean,
            ),
            (
                BLOCK_NUM,
                vec![Value::U64(42), Value::U64(42), Value::U64(42)],
                DataType::UInt64,
            ),
        ];
        let pool =
            DescriptorPool::decode(include_bytes!("../testdata/descriptors.bin").as_ref()).unwrap();
        let message_descriptor = pool.get_message_by_name("package.MyMessage").unwrap();
        // let mut buff = Vec::new();
        // dynamic_message.encode(&mut buff)?;
        // let dynamic_message = DynamicMessage::decode(message_descriptor, [8, 1, 16, 2, 26, 8, 84, 101, 115, 116, 68, 97, 116, 97, 32, 1].as_ref()).unwrap();

        let messages = fields
            .get(0)
            .unwrap()
            .1
            .iter()
            .enumerate()
            .map(|(i, _)| {
                let mut dynamic_message = DynamicMessage::new(message_descriptor.clone());
                fields.iter().for_each(|field| {
                    if field.0 == BLOCK_NUM {
                        return;
                    }
                    let field_descriptor = message_descriptor.get_field_by_name(field.0).unwrap();
                    dynamic_message.set_field(&field_descriptor, field.1[i].clone());
                });
                Value::Message(dynamic_message)
            })
            .collect::<Vec<_>>();

        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|field| Field::new(field.0, field.2.clone(), false))
                .collect::<Vec<_>>(),
        ));

        let result = message_to_rows(&messages, schema, 42)?;

        assert_eq!(result.num_columns(), fields.len());
        assert_eq!(result.num_rows(), messages.len());

        for (i, (_, values, data_type)) in fields.iter().enumerate() {
            match data_type {
                DataType::Int32 => {
                    let expected = Int32Array::from(
                        values
                            .iter()
                            .filter_map(|v| {
                                if let Value::I32(val) = v {
                                    Some(*val)
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>(),
                    );
                    assert_eq!(
                        result
                            .column(i)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap(),
                        &expected
                    );
                }
                DataType::Int64 => {
                    let expected = Int64Array::from(
                        values
                            .iter()
                            .filter_map(|v| {
                                if let Value::I64(val) = v {
                                    Some(*val)
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>(),
                    );
                    assert_eq!(
                        result
                            .column(i)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap(),
                        &expected
                    );
                }
                DataType::Utf8 => {
                    let expected = StringArray::from(
                        values
                            .iter()
                            .filter_map(|v| {
                                if let Value::String(val) = v {
                                    Some(val.clone())
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>(),
                    );
                    assert_eq!(
                        result
                            .column(i)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap(),
                        &expected
                    );
                }
                DataType::Boolean => {
                    let expected = BooleanArray::from(
                        values
                            .iter()
                            .filter_map(|v| {
                                if let Value::Bool(val) = v {
                                    Some(*val)
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>(),
                    );
                    assert_eq!(
                        result
                            .column(i)
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap(),
                        &expected
                    );
                }
                DataType::UInt64 => {
                    let expected = UInt64Array::from(
                        values
                            .iter()
                            .filter_map(|v| {
                                if let Value::U64(val) = v {
                                    Some(*val)
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>(),
                    );
                    assert_eq!(
                        result
                            .column(i)
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap(),
                        &expected
                    );
                }
                _ => panic!("unsupported DataType"),
            }
        }

        Ok(())
    }
}
