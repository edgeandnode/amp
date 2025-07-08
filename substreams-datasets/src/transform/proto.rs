use std::sync::Arc;

use anyhow::{Context as _, anyhow};
use common::{
    BLOCK_NUM, RawDatasetRows, RawTableRows, Table,
    arrow::{
        array::*,
        datatypes::{DataType as ArrowDataType, Field, Schema},
    },
    metadata::range::BlockRange,
};
use prost::Message as _;
pub use prost_reflect::MessageDescriptor;
use prost_reflect::{Cardinality, DescriptorPool, DynamicMessage, FieldDescriptor, Kind, Value};

use crate::proto::{google::protobuf::FileDescriptorSet, sf::substreams::v1::Package};

/// transform Protobuf message to RecordBatch based on the message descriptor
pub(crate) fn pb_to_rows(
    message_descriptor: &MessageDescriptor,
    value: &[u8],
    schemas: &[Table],
    range: &BlockRange,
) -> Result<RawDatasetRows, anyhow::Error> {
    let dynamic_message = DynamicMessage::decode(message_descriptor.clone(), value)
        .context("failed to decode module output message")?;

    let tables: Result<Vec<_>, anyhow::Error> = dynamic_message
        .fields()
        .filter_map(|(field, value)| {
            let list = match value {
                Value::List(list) => list,
                _ => return None,
            };

            let table = schemas.iter().find(|t| t.name() == field.name());
            if table.is_none() {
                return Some(Err(anyhow::anyhow!("table not found")));
            }

            let table = table.unwrap();
            let block_num = *range.numbers.start();
            let rows = message_to_rows(list, table.schema().clone(), block_num);
            if let Err(err) = rows {
                return Some(Err(err.into()));
            }

            Some(
                RawTableRows::new(
                    table.clone(),
                    range.clone(),
                    rows.unwrap().columns().to_vec(),
                )
                .map_err(|e| anyhow!(e)),
            )
        })
        .collect();

    Ok(RawDatasetRows::new(tables?))
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

    let columns: Vec<Arc<dyn Array>> = schema
        .fields()
        .iter()
        .filter_map(|column| {
            let col_name = column.name();
            if col_name == BLOCK_NUM {
                let mut builder = UInt64Builder::with_capacity(row_count);
                builder.append_slice(&vec![block_num; row_count]);
                return Some(Ok(Arc::new(builder.finish()) as Arc<dyn Array>));
            }
            let field_value = message.get_field_by_name(col_name);
            if field_value.is_none() {
                return Some(Err(anyhow::anyhow!("field not found: {col_name}")));
            }
            let col_iter = list.iter().map(|row| {
                row.as_message()
                    .and_then(|msg| msg.get_field_by_name(col_name))
            });
            let col: Arc<dyn Array> = match *field_value.unwrap() {
                Value::String(_) => {
                    let mut builder = StringBuilder::with_capacity(row_count, 0);
                    let cols = col_iter
                        .map(|field| field.and_then(|field| field.as_str().map(|s| s.to_owned())));
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                Value::U32(_) => {
                    let mut builder = UInt32Builder::with_capacity(row_count);
                    let cols = col_iter
                        .map(|field| field.and_then(|field| field.as_u32().map(|s| s.to_owned())));
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                Value::U64(_) => {
                    let mut builder = UInt64Builder::with_capacity(row_count);
                    let cols = col_iter
                        .map(|field| field.and_then(|field| field.as_u64().map(|s| s.to_owned())));
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                Value::I32(_) => {
                    let mut builder = Int32Builder::with_capacity(row_count);
                    let cols = col_iter
                        .map(|field| field.and_then(|field| field.as_i32().map(|s| s.to_owned())));
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                Value::I64(_) => {
                    let mut builder = Int64Builder::with_capacity(row_count);
                    let cols = col_iter
                        .map(|field| field.and_then(|field| field.as_i64().map(|s| s.to_owned())));
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                Value::F32(_) => {
                    let mut builder = Float32Builder::with_capacity(row_count);
                    let cols = col_iter
                        .map(|field| field.and_then(|field| field.as_f32().map(|s| s.to_owned())));
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                Value::F64(_) => {
                    let mut builder = Float64Builder::with_capacity(row_count);
                    let cols = col_iter
                        .map(|field| field.and_then(|field| field.as_f64().map(|s| s.to_owned())));
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                Value::Bytes(_) => {
                    let mut builder = BinaryBuilder::with_capacity(row_count, 0);
                    let cols = col_iter.map(|field| {
                        field.and_then(|field| field.as_bytes().map(|s| s.to_owned()))
                    });
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                Value::Bool(_) => {
                    let mut builder = BooleanBuilder::with_capacity(row_count);
                    let cols = col_iter
                        .map(|field| field.and_then(|field| field.as_bool().map(|s| s.to_owned())));
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                Value::EnumNumber(_) => {
                    let mut builder = Int32Builder::with_capacity(row_count);
                    let cols = col_iter.map(|field| {
                        field.and_then(|field| field.as_enum_number().map(|s| s.to_owned()))
                    });
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                Value::Message(_) => {
                    let mut builder = StringBuilder::with_capacity(row_count, 0);
                    let cols = col_iter.map(|field| {
                        field.and_then(|field| field.as_message().map(|s| s.to_string()))
                    });
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                Value::List(_) | Value::Map(_) => {
                    let mut builder = StringBuilder::with_capacity(row_count, 0);
                    let cols =
                        col_iter.map(|field| field.and_then(|field| Some(field.to_string())));
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
            };
            Some(Ok(col))
        })
        .collect::<Result<_, _>>()?;

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn field_to_table(field: &FieldDescriptor, pool: &DescriptorPool, network: &str) -> Option<Table> {
    if !field.is_list() {
        return None;
    }
    let typename = field.field_descriptor_proto().type_name();
    let message = pool.get_message_by_name(typename).unwrap();
    let mut fields = Vec::with_capacity(message.fields().len() + 1);
    if message.get_field_by_name(BLOCK_NUM).is_none() {
        fields.push(Field::new(BLOCK_NUM, ArrowDataType::UInt64, false));
    }
    fields.extend(
        message
            .fields()
            .map(|f| {
                let mut datatype = match f.kind() {
                    Kind::String => ArrowDataType::Utf8,
                    Kind::Uint32 => ArrowDataType::UInt32,
                    Kind::Uint64 => ArrowDataType::UInt64,
                    Kind::Int32 => ArrowDataType::Int32,
                    Kind::Int64 => ArrowDataType::Int64,
                    Kind::Float => ArrowDataType::Float32,
                    Kind::Double => ArrowDataType::Float64,
                    Kind::Bytes => ArrowDataType::Binary,
                    Kind::Bool => ArrowDataType::Boolean,
                    Kind::Message(_) => ArrowDataType::Utf8,
                    Kind::Enum(_) => ArrowDataType::Int32,
                    _ => ArrowDataType::Utf8,
                };
                if f.cardinality() == Cardinality::Repeated {
                    // datatype = ArrowDataType::List(Arc::new(Field::new(f.name(), datatype, false)));
                    datatype = ArrowDataType::Utf8;
                }
                // block_num always UInt64
                if f.name() == BLOCK_NUM {
                    datatype = ArrowDataType::UInt64;
                }
                Field::new(f.name(), datatype, false)
            })
            .collect::<Vec<_>>(),
    );

    Some(Table::new(
        field.name().to_string(),
        Arc::new(Schema::new(fields)),
        network.to_string(),
    ))
}

pub(crate) fn package_to_schemas(
    package: &Package,
    output_type: &str,
) -> Result<(Vec<Table>, MessageDescriptor), anyhow::Error> {
    let descr_set = FileDescriptorSet {
        file: package.proto_files.clone(),
    };
    let output_type = parse_message_type(output_type);
    let pool = DescriptorPool::decode(descr_set.encode_to_vec().as_slice())
        .context("failed to construct descriptor pool from package")?;
    let message_descriptor = pool
        .get_message_by_name(output_type.as_str())
        .context(format!("failed to get descriptor for type {output_type}"))?;

    let tables = message_descriptor
        .fields()
        .filter_map(|field| field_to_table(&field, &pool, &package.network))
        .collect::<Vec<_>>();

    if tables.is_empty() {
        return Err(anyhow::anyhow!("no suitable tables in the module"));
    }

    Ok((tables, message_descriptor))
}

// proto:sf.ethereum.token.v1.Transfers -> sf.ethereum.token.v1.Transfers
fn parse_message_type(s: &str) -> String {
    if let Some(index) = s.find(':') {
        s[index + 1..].to_string()
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use common::arrow::datatypes::*;
    use prost_reflect::DescriptorPool;

    use super::*;

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
            DescriptorPool::decode(include_bytes!("../../testdata/descriptors.bin").as_ref())
                .unwrap();
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
