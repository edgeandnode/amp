use std::sync::Arc;

use anyhow::{anyhow, Context as _};
use common::{
    arrow::{
        array::*,
        datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit},
    },
    metadata::range::BlockRange,
    timestamp_type, RawDatasetRows, RawTableRows, Table, BLOCK_NUM,
};
use datafusion::sql::sqlparser::{
    ast::{CreateTable, DataType as SqlDataType, Statement},
    dialect,
    parser::Parser,
};
use prost::Message as _;

use crate::proto::sf::substreams::{
    sink::{
        database::v1::{table_change, DatabaseChanges, TableChange},
        sql::v1::{service::Engine as SqlEngine, Service as SqlService},
    },
    v1::Package,
};

/// transform DatabaseChanges proto message to RecordBatch based on the schemas
pub(crate) fn pb_to_rows(
    value: &[u8],
    schemas: &[Table],
    range: &BlockRange,
) -> Result<RawDatasetRows, anyhow::Error> {
    let changes = DatabaseChanges::decode(value).context("failed to decode dbChanges output")?;
    let tables: Result<Vec<_>, anyhow::Error> = schemas
        .iter()
        .filter_map(|table| {
            let table_changes = changes
                .table_changes
                .iter()
                .filter(|change| {
                    change.table == table.name
                        && table_change::Operation::try_from(change.operation).unwrap()
                            == table_change::Operation::Create
                })
                .collect::<Vec<_>>();
            if table_changes.is_empty() {
                return None;
            }
            let block_num = *range.numbers.start();
            let rows = table_changes_to_rows(&table_changes, table.schema.clone(), block_num);
            if let Err(err) = rows {
                return Some(Err(err
                    .context(format!(
                        "Processing table changes for '{}' at block {}",
                        table.name, block_num
                    ))
                    .into()));
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

fn table_changes_to_rows(
    changes: &[&TableChange],
    schema: Arc<Schema>,
    block_num: u64,
) -> Result<RecordBatch, anyhow::Error> {
    let row_count = changes.len();

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

            // iterator over all table changes for this column
            let col_iter = changes.iter().map(|change| {
                if let Some(table_change::PrimaryKey::CompositePk(key)) = &change.primary_key {
                    if let Some(val) = key.keys.get(col_name) {
                        return Some(val);
                    }
                }
                change.fields.iter().find_map(|field| {
                    if field.name == *col_name {
                        return Some(&field.new_value);
                    }
                    None // shouldn't be reachable, will trigger an Arrow batch error
                })
            });

            let col: Arc<dyn Array> = match column.data_type() {
                ArrowDataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(row_count);
                    let cols = col_iter.map(|field| {
                        field.and_then(|field| {
                            Some(field.parse().expect("field type should be an i64"))
                        })
                    });
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                ArrowDataType::Int32 => {
                    let mut builder = Int32Builder::with_capacity(row_count);
                    let cols = col_iter.map(|field| {
                        field.and_then(|field| {
                            Some(field.parse().expect("field type should be an i32"))
                        })
                    });
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                ArrowDataType::Utf8 => {
                    let mut builder = StringBuilder::with_capacity(row_count, 0);
                    let cols =
                        col_iter.map(|field| field.and_then(|field| Some(field.to_string())));
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                ArrowDataType::Boolean => {
                    let mut builder = BooleanBuilder::with_capacity(row_count);
                    let cols = col_iter.map(|field| {
                        field.and_then(|field| {
                            Some(
                                field
                                    .parse::<bool>()
                                    .expect("field type should be a boolean"),
                            )
                        })
                    });
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                ArrowDataType::UInt64 => {
                    let mut builder = UInt64Builder::with_capacity(row_count);
                    let cols = col_iter.map(|field| {
                        field.and_then(|field| {
                            Some(field.parse().expect("field type should be a u64"))
                        })
                    });
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                ArrowDataType::Timestamp(_, _) => {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(row_count)
                        .with_data_type(common::timestamp_type());
                    let cols = col_iter.map(|field| {
                        field.and_then(|field| {
                            Some(
                                chrono::DateTime::parse_from_rfc3339(field.as_str())
                                    .expect("field type should be a timestamp")
                                    .timestamp_nanos_opt()
                                    .expect("failed to get nanoseconds"),
                            )
                        })
                    });
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                ArrowDataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(row_count);
                    let cols = col_iter.map(|field| {
                        field.and_then(|field| {
                            Some(field.parse().expect("field type should be a float"))
                        })
                    });
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                ArrowDataType::Float32 => {
                    let mut builder = Float32Builder::with_capacity(row_count);
                    let cols = col_iter.map(|field| {
                        field.and_then(|field| {
                            Some(field.parse().expect("field type should be a float"))
                        })
                    });
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                ArrowDataType::Date32 => {
                    let mut builder = Date32Builder::with_capacity(row_count);
                    let cols = col_iter.map(|field| {
                        field.and_then(|field| {
                            Some(field.parse().expect("field type should be a date"))
                        })
                    });
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                ArrowDataType::Binary => {
                    let mut builder = BinaryBuilder::with_capacity(row_count, 0);
                    let cols = col_iter
                        .map(|field| field.and_then(|field| Some(field.as_bytes().to_owned())));
                    builder.extend(cols);
                    Arc::new(builder.finish())
                }
                _ => todo!("field type {} not implemented", column.data_type()),
            };
            Some(Ok(col))
        })
        .collect::<Result<_, anyhow::Error>>()?;

    Ok(RecordBatch::try_new(schema, columns)?)
}

pub(crate) fn package_to_schemas(
    package: &Package,
    output_module: &str,
) -> Result<Vec<Table>, anyhow::Error> {
    let sink_config = SqlService::decode(
        package
            .sink_config
            .as_ref()
            .ok_or_else(|| anyhow!("sink config not defined"))?
            .value
            .clone()
            .as_slice(),
    )
    .context("failed to decode sink config")?;

    if sink_config.schema.is_empty() {
        return Err(anyhow!("sink schema is empty"));
    }

    if SqlEngine::try_from(sink_config.engine)? != SqlEngine::Postgres {
        return Err(anyhow!("only postgres sink engine supported"));
    }

    if package.sink_module != output_module {
        return Err(anyhow!(
            "sink module {} does not match output module {}",
            package.sink_module,
            output_module
        ));
    }

    let tables = sql_to_schemas(sink_config.schema, &package.network)?;

    Ok(tables)
}

fn sql_to_schemas(sql: String, network: &str) -> Result<Vec<Table>, anyhow::Error> {
    let dialect = dialect::GenericDialect {};

    let statements = Parser::parse_sql(&dialect, &sql).context("failed to parse SQL")?;

    let tables = statements
        .iter()
        .filter_map(|statement| statement_to_table(statement, network))
        .collect();

    Ok(tables)
}

fn statement_to_table(statement: &Statement, network: &str) -> Option<Table> {
    match statement {
        Statement::CreateTable(CreateTable { name, columns, .. }) => {
            let fields = std::iter::once(Field::new(BLOCK_NUM, ArrowDataType::UInt64, false))
                .chain(
                    columns
                        .iter()
                        .filter(|column| column.name.value != BLOCK_NUM)
                        .map(|column| {
                            let datatype = match column.data_type {
                                SqlDataType::Int(_) => ArrowDataType::Int32,
                                SqlDataType::BigInt(_) => ArrowDataType::Int64,
                                SqlDataType::SmallInt(_) => ArrowDataType::Int16,
                                SqlDataType::TinyInt(_) => ArrowDataType::Int8,
                                SqlDataType::Char(_) => ArrowDataType::Utf8,
                                SqlDataType::Varchar(_) => ArrowDataType::Utf8,
                                SqlDataType::Text => ArrowDataType::Utf8,
                                SqlDataType::Date => ArrowDataType::Date32,
                                SqlDataType::Time(_, _) => {
                                    ArrowDataType::Time64(TimeUnit::Nanosecond)
                                }
                                // SqlDataType::Timestamp(_,_) => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                                SqlDataType::Timestamp(_, _) => timestamp_type(),
                                SqlDataType::Decimal(_) => ArrowDataType::Float64,
                                SqlDataType::Real => ArrowDataType::Float32,
                                SqlDataType::Double(_) => ArrowDataType::Float64,
                                SqlDataType::Boolean => ArrowDataType::Boolean,
                                SqlDataType::Binary(_) => ArrowDataType::Binary,
                                _ => ArrowDataType::Utf8,
                            };
                            Field::new(column.name.value.as_str(), datatype, false)
                        }),
                )
                .collect::<Vec<_>>();

            Some(Table {
                name: name.to_string(),
                schema: Arc::new(Schema::new(fields)),
                network: network.to_string(),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_sql() {
        let sql = "---------- comment ---------
CREATE TABLE example_table (
    -- Integer types
    int_column INT,
    tinyint_column TINYINT,
    smallint_column SMALLINT,
    mediumint_column MEDIUMINT,
    bigint_column BIGINT,

    -- Numeric types
    float_column FLOAT,
    double_column DOUBLE,
    decimal_column DECIMAL(10,2),

    -- Date and time types
    date_column DATE,
    datetime_column DATETIME,
    timestamp_column TIMESTAMP,
    time_column TIME,
    year_column YEAR,

    -- Character types
    char_column CHAR(10),
    varchar_column VARCHAR(255),
    text_column TEXT,

    -- Binary types
    binary_column BINARY(10),
    varbinary_column VARBINARY(255),
    blob_column BLOB,

    -- Other types
    enum_column ENUM('value1', 'value2', 'value3'),
    set_column SET('option1', 'option2', 'option3')
);"
        .to_string();

        let res = sql_to_schemas(sql, "test_network");
        assert!(res.is_ok());
        println!("{:?}", res.unwrap());
    }
}
