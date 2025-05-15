use anyhow::{anyhow, Context as _};
use common::Table;

use crate::{
    proto::sf::substreams::v1::Package,
    transform::{self, proto::MessageDescriptor},
};

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
    pub fn from_package(package: &Package, output_module: &str) -> Result<Self, anyhow::Error> {
        let module = package
            .modules
            .as_ref()
            .ok_or_else(|| anyhow!("modules not defined"))?
            .modules
            .iter()
            .find(|&m| m.name == output_module)
            .context(format!("module {} not found", output_module))?;
        let output_type = module.output.as_ref().unwrap().r#type.as_str();

        match output_type {
            "proto:sf.substreams.sink.database.v1.DatabaseChanges" => {
                let tables = transform::db::package_to_schemas(package, output_module)?;
                Ok(Self {
                    output_type: OutputType::DbOut,
                    tables,
                })
            }
            "proto:sf.substreams.sink.entity.v1.EntityChanges" => {
                let tables = transform::entities::package_to_schemas(package, output_module)?;
                Ok(Self {
                    output_type: OutputType::Entities,
                    tables,
                })
            }
            _ => {
                let (tables, message_descriptor) =
                    transform::proto::package_to_schemas(package, output_type)?;
                Ok(Self {
                    output_type: OutputType::Proto(message_descriptor),
                    tables,
                })
            }
        }
    }
}
