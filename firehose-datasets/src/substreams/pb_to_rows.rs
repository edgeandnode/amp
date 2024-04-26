use std::sync::Arc;

use anyhow::Context as _;
use prost_reflect::{DynamicMessage, Value};

use super::tables::Tables;
use crate::proto::sf::substreams::rpc::v2::BlockScopedData;
use common::{
    arrow::{array::*, datatypes::Schema}, parquet::data_type::AsBytes as _, DatasetRows, TableRows, BLOCK_NUM
};


pub fn pb_to_rows(block: BlockScopedData, tables: &Tables) -> Result<DatasetRows, anyhow::Error> {
    let mut table_rows = DatasetRows(Vec::new());

    let block_num = block.clock.unwrap().number;
    let value = block.output
        .context("no output")?
        .map_output
        .context("map_output is empty")?
        .value;

    let dynamic_message = DynamicMessage::decode(tables.message_descriptor.clone(), value.as_bytes())?;
    for (field, value) in dynamic_message.fields() {
        let list = match value {
            Value::List(list) => list,
            _ => continue,
        };
        let table = tables.tables.iter()
            .find(|t| t.name == field.name())
            .context(format!("table not found: {}", field.name()))?;
        let rows = message_to_rows(list, table.schema.clone(), block_num)?;
        table_rows.0.push(TableRows {
            rows,
            table: table.clone(),
        });
    }
    Ok(table_rows)
}



fn message_to_rows(list: &Vec<Value>, schema: Arc<Schema>, block_num: u64) -> Result<RecordBatch, anyhow::Error> {
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
        let field = message.get_field_by_name(col_name).context(format!("field not found: {col_name}"))?;
        let col_iter = list.iter().map(|row| row.as_message().and_then(|msg| msg.get_field_by_name(col_name)));
        match *field {
            Value::String(_) => {
                let mut builder = StringBuilder::with_capacity(row_count, 0);
                let cols = col_iter.map(|field| field.and_then(|field| field.as_str().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            },
            Value::U32(_) => {
                let mut builder = UInt32Builder::with_capacity(row_count);
                let cols = col_iter.map(|field| field.and_then(|field| field.as_u32().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            },
            Value::U64(_) => {
                let mut builder = UInt64Builder::with_capacity(row_count);
                let cols = col_iter.map(|field| field.and_then(|field| field.as_u64().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            },
            Value::I32(_) => {
                let mut builder = Int32Builder::with_capacity(row_count);
                let cols = col_iter.map(|field| field.and_then(|field| field.as_i32().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            },
            Value::I64(_) => {
                let mut builder = Int64Builder::with_capacity(row_count);
                let cols = col_iter.map(|field| field.and_then(|field| field.as_i64().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            },
            Value::F32(_) => {
                let mut builder = Float32Builder::with_capacity(row_count);
                let cols = col_iter.map(|field| field.and_then(|field| field.as_f32().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            },
            Value::F64(_) => {
                let mut builder = Float64Builder::with_capacity(row_count);
                let cols = col_iter.map(|field| field.and_then(|field| field.as_f64().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            },
            Value::Bytes(_) => {
                let mut builder = BinaryBuilder::with_capacity(row_count, 0);
                let cols = col_iter.map(|field| field.and_then(|field| field.as_bytes().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            },
            Value::Bool(_) => {
                let mut builder = BooleanBuilder::with_capacity(row_count);
                let cols = col_iter.map(|field| field.and_then(|field| field.as_bool().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            },
            Value::EnumNumber(_) => {
                let mut builder = Int32Builder::with_capacity(row_count);
                let cols = col_iter.map(|field| field.and_then(|field| field.as_enum_number().map(|s| s.to_owned())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            },
            Value::Message(_) => {
                let mut builder = StringBuilder::with_capacity(row_count, 0);
                let cols = col_iter.map(|field| field.and_then(|field| field.as_message().map(|s| s.to_string())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            },
            Value::List(_) | Value::Map(_) => {
                let mut builder = StringBuilder::with_capacity(row_count, 0);
                let cols = col_iter.map(|field| field.and_then(|field| Some(field.to_string())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            },
        }
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::arrow::datatypes::*;
    use prost_reflect::DescriptorPool;
    use std::sync::Arc;
    use anyhow::Result;

    #[test]
    fn test_message_to_rows() -> Result<()> {
        let fields = vec![
            ("i32", Value::I32(1), DataType::Int32),
            ("i64", Value::I64(2), DataType::Int64),
            ("str", Value::String("test".to_string()), DataType::Utf8),
            ("boolean", Value::Bool(true), DataType::Boolean),
            (BLOCK_NUM, Value::U64(42), DataType::UInt64),
        ];
        let pool = DescriptorPool::decode(include_bytes!("test/descriptors.bin").as_ref()).unwrap();
        let message_descriptor = pool.get_message_by_name("package.MyMessage").unwrap();

        let mut dynamic_message = DynamicMessage::new(message_descriptor.clone());
        fields.iter().for_each(|field| {
            if field.0 == BLOCK_NUM { return; }
            let field_descriptor = message_descriptor.get_field_by_name(field.0).unwrap();
            dynamic_message.set_field(&field_descriptor, field.1.clone());
        });
        // let mut buff = Vec::new();
        // dynamic_message.encode(&mut buff)?;
        // let dynamic_message = DynamicMessage::decode(message_descriptor, [8, 1, 16, 2, 26, 8, 84, 101, 115, 116, 68, 97, 116, 97, 32, 1].as_ref()).unwrap();
        let message = vec![
            Value::Message(dynamic_message)
        ];

        let schema = Arc::new(Schema::new(fields.iter().map(|field| {
            Field::new(field.0, field.2.clone(), false)
        }).collect::<Vec<_>>()));

        let result = message_to_rows(&message, schema, 42)?;

        assert_eq!(result.num_columns(), 5);
        assert_eq!(result.num_rows(), 1);

        assert_eq!(result.column(0).as_any().downcast_ref::<Int32Array>().unwrap(), &Int32Array::from(vec![1]));
        assert_eq!(result.column(1).as_any().downcast_ref::<Int64Array>().unwrap(), &Int64Array::from(vec![2]));
        assert_eq!(result.column(2).as_any().downcast_ref::<StringArray>().unwrap(), &StringArray::from(vec!["test"]));
        assert_eq!(result.column(3).as_any().downcast_ref::<BooleanArray>().unwrap(), &BooleanArray::from(vec![true]));
        assert_eq!(result.column(4).as_any().downcast_ref::<UInt64Array>().unwrap(), &UInt64Array::from(vec![42]));

        Ok(())
    }
}