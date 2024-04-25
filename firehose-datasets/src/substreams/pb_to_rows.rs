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
            Value::List(_) => {
                let mut builder = StringBuilder::with_capacity(row_count, 0);
                let cols = col_iter.map(|field| field.and_then(|_field| Some("list".to_string())));
                builder.extend(cols);
                columns.push(Arc::new(builder.finish()));
            },
            _ => return Err(anyhow::anyhow!("unsupported data type: {:?}", *field)),
        }
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}
