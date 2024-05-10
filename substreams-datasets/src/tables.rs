use anyhow::{anyhow, Context as _};
use prost::Message as _;
use prost_reflect::{DescriptorPool, MessageDescriptor};

use crate::{
    proto::{
        google::protobuf::FileDescriptorSet,
        sf::substreams::{
            sink::sql::v1::{service::Engine as SqlEngine, Service as SqlService},
            v1::Package,
        },
    },
    transform,
};
use common::Table;

#[derive(Debug, Clone)]
pub enum OutputType {
    Proto(MessageDescriptor),
    DbOut,
    Entities,
}

#[derive(Debug, Clone)]
pub struct Tables {
    pub tables: Vec<Table>,
    pub output_type: OutputType,
}

impl Tables {
    /// create table schemas from spkg package
    pub fn from_package(package: &Package, output_module: String) -> Result<Self, anyhow::Error> {
        let module = package
            .modules
            .as_ref()
            .ok_or_else(|| anyhow!("modules not defined"))?
            .modules
            .iter()
            .find(|&m| m.name == output_module)
            .context(format!("module {} not found", output_module))?;
        let output_type = parse_message_type(module.output.as_ref().unwrap().r#type.as_str());

        match output_type.as_str() {
            "sf.substreams.sink.database.v1.DatabaseChanges" => {
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

                Ok(Self {
                    output_type: OutputType::DbOut,
                    tables: transform::db::sql_to_schemas(sink_config.schema)?,
                })
            }
            "sf.substreams.sink.entities.v1.Entities" => {
                todo!("Entities output type not supported yet");
            }
            _ => {
                let descr_set = FileDescriptorSet {
                    file: package.proto_files.clone(),
                };
                let pool = DescriptorPool::decode(descr_set.encode_to_vec().as_slice())
                    .context("failed to construct descriptor pool from package")?;
                let message_descriptor = pool
                    .get_message_by_name(output_type.as_str())
                    .context(format!("failed to get descriptor for type {output_type}"))?;

                Ok(Self {
                    output_type: OutputType::Proto(message_descriptor.clone()),
                    tables: transform::proto::descriptor_to_schemas(message_descriptor, pool)?,
                })
            }
        }
    }
}

// proto:sf.ethereum.token.v1.Transfers -> sf.ethereum.token.v1.Transfers
fn parse_message_type(s: &str) -> String {
    if let Some(index) = s.find(':') {
        s[index + 1..].to_string()
    } else {
        s.to_string()
    }
}
