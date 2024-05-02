use std::sync::Arc;

use anyhow::Context as _;
use prost::Message as _;
use prost_reflect::{Cardinality, DescriptorPool, Kind, MessageDescriptor};

use crate::proto::{google::protobuf::FileDescriptorSet, sf::substreams::v1::Package};
use common::{
    arrow::datatypes::{DataType, Field, Schema},
    Table, BLOCK_NUM,
};

#[derive(Debug, Clone)]
pub struct Tables {
    pub tables: Vec<Table>,
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
            .unwrap()
            .modules
            .iter()
            .find(|&m| m.name == output_module)
            .context(format!("module {} not found", output_module))?;
        let output_type = Self::parse_message_type(module.output.as_ref().unwrap().r#type.as_str());
        let pool = DescriptorPool::decode(descr_set.encode_to_vec().as_slice())
            .context("failed to decode descriptor pool from package")?;
        let message_descriptor = pool.get_message_by_name(output_type.as_str()).unwrap();
        let tables = message_descriptor
            .fields()
            .filter_map(|field| {
                if !field.is_list() {
                    return None;
                }
                let typename = field.field_descriptor_proto().type_name();
                let message = pool.get_message_by_name(typename).unwrap();
                let mut fields = Vec::with_capacity(message.fields().len() + 1);
                if message.get_field_by_name(BLOCK_NUM).is_none() {
                    fields.push(Field::new(BLOCK_NUM, DataType::UInt64, false));
                }
                fields.extend(
                    message
                        .fields()
                        .map(|f| {
                            let mut datatype = match f.kind() {
                                Kind::String => DataType::Utf8,
                                Kind::Uint32 => DataType::UInt32,
                                Kind::Uint64 => DataType::UInt64,
                                Kind::Int32 => DataType::Int32,
                                Kind::Int64 => DataType::Int64,
                                Kind::Float => DataType::Float32,
                                Kind::Double => DataType::Float64,
                                Kind::Bytes => DataType::Binary,
                                Kind::Bool => DataType::Boolean,
                                Kind::Message(_) => DataType::Utf8,
                                Kind::Enum(_) => DataType::Int32,
                                _ => DataType::Utf8,
                            };
                            if f.cardinality() == Cardinality::Repeated {
                                // datatype = DataType::List(Arc::new(Field::new(f.name(), datatype, false)));
                                datatype = DataType::Utf8;
                            }
                            Field::new(f.name(), datatype, false)
                        })
                        .collect::<Vec<_>>(),
                );

                Some(Table {
                    name: field.name().to_string(),
                    schema: Arc::new(Schema::new(fields)),
                })
            })
            .collect();

        Ok(Self {
            tables,
            message_descriptor,
        })
    }

    // proto:sf.ethereum.token.v1.Transfers -> sf.ethereum.token.v1.Transfers
    fn parse_message_type(s: &str) -> String {
        if let Some(index) = s.find(':') {
            s[index + 1..].to_string()
        } else {
            s.to_string()
        }
    }
}
