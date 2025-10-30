use std::{
    collections::HashSet,
    hash::{DefaultHasher, Hash, Hasher},
    iter::ExactSizeIterator,
};

use super::container::AtLeastOne;
use crate::{
    Connection, SupportedVendor,
    config::{
        adbc::DriverOpts,
        container::{HasName, MemoizedHash},
    },
    error::Result,
    schema::TableRef,
};

#[derive(Debug, Clone)]
pub struct Drivers(pub AtLeastOne<DriverConfig>);

impl Drivers {
    pub fn new(vendors: impl ExactSizeIterator<Item = DriverConfig>) -> Result<Self> {
        AtLeastOne::try_new(vendors).map(Drivers)
    }

    pub fn names(&self) -> HashSet<String> {
        self.iter().map(|v| v.name.clone()).collect()
    }

    pub fn table_refs(&self) -> impl Iterator<Item = TableRef> {
        use DriverOpts::*;
        self.iter().map(|config| match &config.opts {
            Postgres {
                database,
                schema,
                table,
                ..
            } => {
                let database = database.to_string();
                let schema = Some(schema.to_string());
                let table = table.to_string();
                TableRef {
                    database,
                    schema,
                    table,
                }
            }
            Snowflake {
                database,
                schema,
                table,
                ..
            } => {
                let database = database.to_string();
                let schema = Some(schema.to_string());
                let table = table.to_string();
                TableRef {
                    database,
                    schema,
                    table,
                }
            }
            _ => unimplemented!(),
        })
    }

    pub fn vendors(&self) -> impl Iterator<Item = SupportedVendor> {
        self.iter().map(DriverConfig::vendor)
    }

    pub fn get<T: Into<<DriverConfig as HasName>::NameType>>(
        &self,
        name: T,
    ) -> Option<&DriverConfig> {
        self.0.get(&name.into())
    }

    pub fn get_mut<T: Into<<DriverConfig as HasName>::NameType>>(
        &mut self,
        name: T,
    ) -> Option<&mut DriverConfig> {
        self.0.get_mut(&name.into())
    }

    pub fn iter(&self) -> impl Iterator<Item = &DriverConfig> {
        self.0.iter()
    }

    pub fn iter_mut_by_vendor(
        &mut self,
        vendor: SupportedVendor,
    ) -> impl Iterator<Item = &mut DriverConfig> {
        self.0.iter_mut().filter(move |config| {
            use SupportedVendor::*;
            match (&config.opts, &vendor) {
                (DriverOpts::BigQuery { .. }, BigQuery) => true,
                (DriverOpts::Postgres { .. }, Postgres) => true,
                (DriverOpts::Snowflake { .. }, Snowflake) => true,
                (DriverOpts::Sqlite { .. }, Sqlite) => true,
                _ => false,
            }
        })
    }

    pub fn iter_mut_by_persona(
        &mut self,
        account: &str,
        username: &str,
        vendor: SupportedVendor,
    ) -> impl Iterator<Item = &mut DriverConfig> {
        self.iter_mut_by_vendor(vendor)
            .filter(move |DriverConfig { opts, .. }| {
                use DriverOpts::*;
                match opts {
                    Postgres { username: u, .. } if u == username => true,
                    Snowflake {
                        username: u,
                        account: a,
                        ..
                    } if u == username && a == account => true,
                    _ => false,
                }
            })
    }

    pub fn set_all_passwords(
        &mut self,
        account: &str,
        username: &str,
        password: String,
        vendor: SupportedVendor,
    ) {
        self.iter_mut_by_persona(account, username, vendor)
            .for_each(DriverConfig::set_password(password));
    }
}
impl TryFrom<&mut Drivers> for Vec<Connection> {
    type Error = crate::Error;
    fn try_from(drivers: &mut Drivers) -> Result<Self> {
        drivers
            .0
            .iter_mut()
            .map(|DriverConfig { opts, .. }| Connection::connect(opts))
            .collect::<Result<Vec<_>>>()
    }
}

impl PartialEq for Drivers {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() == other.0.len() {
            let this = self.0.to_vec();
            let mut other = other.0.to_vec();
            this.into_iter().for_each(|conf| {
                if let Some(index) = other
                    .iter()
                    .enumerate()
                    .find_map(|(idx, other)| (conf == *other).then_some(idx))
                {
                    other.remove(index);
                }
            });

            return other.is_empty();
        }
        false
    }
}

impl Eq for Drivers {}

#[derive(Debug, Clone, Eq)]
pub struct DriverConfig {
    pub name: String,
    pub opts: DriverOpts,
    pub hash: u64,
}

impl Hash for DriverConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.set_hash(state);
    }
}

impl MemoizedHash for DriverConfig {
    fn get_hash(&self) -> u64 {
        self.hash
    }
}

impl HasName for DriverConfig {
    type NameType = String;
    fn name(&self) -> &Self::NameType {
        &self.name
    }
}

impl DriverConfig {
    pub fn new(name: String, config: DriverOpts) -> Self {
        let mut hasher = DefaultHasher::new();
        config.hash(&mut hasher);
        name.hash(&mut hasher);
        let hash = hasher.finish();
        DriverConfig {
            name,
            opts: config,
            hash,
        }
    }

    pub fn set_password(password: String) -> impl FnMut(&mut DriverConfig) {
        use DriverOpts::*;
        move |config| {
            let old_password = match &mut config.opts {
                Postgres { password: p, .. } => std::mem::replace(p, Some(password.clone())),
                Snowflake { password: p, .. } => std::mem::replace(p, Some(password.clone())),
                _ => None,
            };
            drop(old_password);
            config.update_hash();
        }
    }

    pub fn update_hash(&mut self) {
        let mut hasher = DefaultHasher::new();
        self.opts.hash(&mut hasher);
        self.name.hash(&mut hasher);
        self.hash = hasher.finish();
    }

    pub fn vendor(&self) -> SupportedVendor {
        use DriverOpts::*;
        match &self.opts {
            Postgres { .. } => SupportedVendor::Postgres,
            Snowflake { .. } => SupportedVendor::Snowflake,
            BigQuery { .. } => SupportedVendor::BigQuery,
            Sqlite { .. } => SupportedVendor::Sqlite,
        }
    }
}

impl PartialEq for DriverConfig {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

pub(super) mod serde_vendors {
    use serde::{Deserializer, Serializer, de::Visitor, ser::SerializeMap};

    use super::{DriverConfig, DriverOpts, Drivers};

    pub fn serialize<S>(vendors: &Drivers, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(vendors.0.len()))?;
        for vendor in vendors.0.iter() {
            map.serialize_entry(&vendor.name, &vendor.opts)?;
        }
        map.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Drivers, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VendorsVisitor;

        impl<'de> Visitor<'de> for VendorsVisitor {
            type Value = Drivers;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a map of vendor names to driver options")
            }

            fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                let mut vendors = Vec::with_capacity(access.size_hint().unwrap_or(0));
                while let Some((name, opts)) = access.next_entry::<String, DriverOpts>()? {
                    vendors.push(DriverConfig::new(name, opts));
                }

                Ok(Drivers::new(vendors.into_iter()).map_err(serde::de::Error::custom)?)
            }
        }

        deserializer.deserialize_map(VendorsVisitor)
    }
}
