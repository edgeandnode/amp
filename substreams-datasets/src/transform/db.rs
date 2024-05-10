

use std::sync::Arc;

use prost::Message as _;

use crate::tables::Tables;
use crate::proto::sf::substreams::sink::database::v1::{
    table_change,
    DatabaseChanges,
    TableChange
};

use common::{
    arrow::{
        array::*,
        datatypes::{
            Schema,
            DataType as ArrowDataType
        }
    },
    parquet::data_type::AsBytes as _,
    DatasetRows,
    TableRows,
    BLOCK_NUM,
};


pub(crate) fn pb_to_rows(value: Vec<u8>, tables: &Tables, block_num: u64) -> Result<DatasetRows, anyhow::Error> {

    let changes = DatabaseChanges::decode(value.as_bytes())?;
    let tables: Result<Vec<_>, anyhow::Error> = tables.tables.iter().filter_map(|table| {
        let table_changes = changes
            .table_changes
            .iter()
            .filter(|change|
                change.table == table.name
                && table_change::Operation::try_from(change.operation).unwrap() == table_change::Operation::Create
            )
            .collect::<Vec<_>>();
        if table_changes.is_empty() {
            return None;
        }
        let rows = table_changes_to_rows(&table_changes, table.schema.clone(), block_num);
        if let Err(err) = rows {
            return Some(Err(err.into()));
        }
        Some(Ok(TableRows {
            rows: rows.unwrap(),
            table: table.clone(),
        }))
    })
    .collect();

    Ok(DatasetRows(tables?))
}


fn table_changes_to_rows(changes: &[&TableChange], schema: Arc<Schema>, block_num: u64) -> Result<RecordBatch, anyhow::Error> {
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
    let row_count = changes.len();

    for column in schema.fields() {
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
