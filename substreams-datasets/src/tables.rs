use std::sync::Arc;

use anyhow::{anyhow, Context as _};
use prost::Message as _;
use prost_reflect::{Cardinality, DescriptorPool, FieldDescriptor, Kind, MessageDescriptor};
use substreams_entity_change::tables;

use crate::proto::{google::protobuf::FileDescriptorSet, sf::substreams::{sink::sql::v1::{Service as SqlService, service::Engine as SqlEngine}, v1::Package}};
use common::{
    arrow::datatypes::{DataType as ArrowDataType, TimeUnit, Field, Schema},
    Table, BLOCK_NUM,
};

use sqlparser::dialect;
use sqlparser::parser::Parser;
use sqlparser::ast::{Statement, DataType as SqlDataType};

#[derive(Debug, Clone)]
pub enum OutputType {
    Proto,
    DbOut,
    Entities,
}

#[derive(Debug, Clone)]
pub struct Tables {
    pub tables: Vec<Table>,
    pub output_type: OutputType,
    pub message_descriptor: MessageDescriptor,
}

impl Tables {
    // create table schemas from spkg package
    pub fn from_package(package: &Package, output_module: String) -> Result<Self, anyhow::Error> {
        let descr_set = FileDescriptorSet {
            file: package.proto_files.clone(),
        };
        let module = package
            .modules
            .as_ref()
            .ok_or_else(|| anyhow!("modules not defined"))?
            .modules
            .iter()
            .find(|&m| m.name == output_module)
            .context(format!("module {} not found", output_module))?;
        let output_type = parse_message_type(module.output.as_ref().unwrap().r#type.as_str());
        let pool = DescriptorPool::decode(descr_set.encode_to_vec().as_slice())
            .context("failed to construct descriptor pool from package")?;
        let message_descriptor = pool.get_message_by_name(output_type.as_str())
            .context(format!("failed to get message descriptor for type {}", output_type))?;

        match output_type.as_str() {
            "sf.substreams.sink.database.v1.DatabaseChanges" => {
                let sink_config = SqlService::decode(
                    package
                        .sink_config
                        .as_ref()
                        .ok_or_else(|| anyhow!("sink config not defined"))?
                        .value
                        .clone()
                        .as_slice()
                ).context("failed to decode sink config")?;

                if sink_config.schema.is_empty() {
                    return Err(anyhow!("sink schema is empty"));
                }

                if SqlEngine::try_from(sink_config.engine)?.as_str_name() != "postgres" {
                    return Err(anyhow!("only postgres sink engine supported"));
                }

                if package.sink_module != output_module {
                    return Err(anyhow!(
                        "sink module {} does not match output module {}",
                        package.sink_module,
                        output_module
                    ));
                }

                let tables = parse_sql(&sink_config.schema)?;

                Ok(Self {
                    output_type: OutputType::DbOut,
                    tables,
                    message_descriptor,
                })
            },
            "sf.substreams.sink.entities.v1.Entities" => {
                return Err(anyhow!("Entities output type not supported yet"));
                // Ok(Self {
                //     output_type: OutputType::Entities,
                //     tables: vec![],
                //     message_descriptor,
                // })
            },
            _ => {
                let tables = message_descriptor
                    .fields()
                    .filter_map(|field| table_from_field(&field, &pool))
                    .collect::<Vec<_>>();
                Ok(Self {
                    output_type: OutputType::Proto,
                    tables,
                    message_descriptor,
                })
            }
        }
    }
}

fn parse_sql(sql: &str) -> Result<Vec<Table>, anyhow::Error> {
    let dialect = dialect::GenericDialect {};

    let statements = Parser::parse_sql(&dialect, sql).context("failed to parse SQL")?;

    let tables = statements.iter().filter_map(|statement| {
        match statement {
            Statement::CreateTable { name, columns, .. } => {
                let fields = columns.iter().map(|column| {
                    let datatype = match &column.data_type {
                        SqlDataType::Int(_) => ArrowDataType::Int32,
                        SqlDataType::BigInt(_) => ArrowDataType::Int64,
                        SqlDataType::SmallInt(_) => ArrowDataType::Int16,
                        SqlDataType::TinyInt(_) => ArrowDataType::Int8,
                        SqlDataType::Char(_) => ArrowDataType::Utf8,
                        SqlDataType::Varchar(_) => ArrowDataType::Utf8,
                        SqlDataType::Text => ArrowDataType::Utf8,
                        SqlDataType::Date => ArrowDataType::Date32,
                        SqlDataType::Time(_,_) => ArrowDataType::Time64(TimeUnit::Nanosecond),
                        SqlDataType::Timestamp(_,_) => ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                        SqlDataType::Decimal(_) => ArrowDataType::Float64,   // replace with Decimal?
                        SqlDataType::Real => ArrowDataType::Float32,
                        SqlDataType::Double => ArrowDataType::Float64,
                        SqlDataType::Boolean => ArrowDataType::Boolean,
                        SqlDataType::Binary(_) => ArrowDataType::Binary,
                        _ => ArrowDataType::Utf8,
                    };
                    Field::new(column.name.value.as_str(), datatype, false)
                }).collect::<Vec<_>>();

                Some(Table {
                    name: name.to_string(),
                    schema: Arc::new(Schema::new(fields)),
                })
            }
            _ => { None }
        }
    }).collect();

    Ok(tables)
}

// proto:sf.ethereum.token.v1.Transfers -> sf.ethereum.token.v1.Transfers
fn parse_message_type(s: &str) -> String {
    if let Some(index) = s.find(':') {
        s[index + 1..].to_string()
    } else {
        s.to_string()
    }
}

fn table_from_field(field: &FieldDescriptor, pool: &DescriptorPool) -> Option<Table> {
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
                Field::new(f.name(), datatype, false)
            })
            .collect::<Vec<_>>(),
    );

    Some(Table {
        name: field.name().to_string(),
        schema: Arc::new(Schema::new(fields)),
    })
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
);";

        let res = parse_sql(sql);
        assert!(res.is_ok());
        println!("{:?}", res.unwrap());
    }
}
