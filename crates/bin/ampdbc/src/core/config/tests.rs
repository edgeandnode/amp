use super::*;
use crate::{SupportedVendor, config::driver::DriverConfig};

impl Drivers {
    fn mock_snowflake_n(n: usize) -> Self {
        let account = "test_account";
        let username = "test_user";
        let password = "test_password";
        let database = "test_database";
        let schema = "test_schema";
        let table = "test_table";
        let warehouse = "test_warehouse";

        let vendors = (0..n).map(|i| {
            let name = format!("snowflake_driver_{}", i);
            let account = account.to_string();
            let username = username.to_string();
            let password = password.to_string().into();
            let database = database.to_string();
            let schema = schema.to_string();
            let table = format!("{}_{}", table, i);
            let warehouse = warehouse.to_string();
            let ingest_writer_concurrency = 4;
            let ingest_upload_concurrency = 4;
            let ingest_copy_concurrency = 4;
            let ingest_target_file_size = 1024;
            let opts = DriverOpts::Snowflake {
                account,
                username,
                password,
                database,
                schema,
                table,
                warehouse,
                role: None,
                ingest_writer_concurrency,
                ingest_upload_concurrency,
                ingest_copy_concurrency,
                ingest_target_file_size,
            };
            DriverConfig::new(name, opts)
        });
        Drivers::new(vendors).unwrap()
    }

    fn mock_postgres_n(n: usize) -> Self {
        let host = "localhost";
        let port = 5432;
        let username = "test_user";
        let password = "test_password";
        let database = "test_database";
        let schema = "public";
        let table = "test_table";
        let application_name = Some("ampdbc_test".to_string());

        let vendors = (0..n).map(|i| {
            let name = format!("postgres_driver_{}", i);
            let host = host.to_string();
            let port = port;
            let username = username.to_string();
            let password = password.to_string().into();
            let database = database.to_string();
            let schema = schema.to_string();
            let table = format!("{}_{}", table, i);
            let opts = DriverOpts::Postgres {
                host,
                port,
                username,
                password,
                database,
                table,
                socket: None,
                application_name: application_name.clone(),
                schema,
                statement_cache_capacity: None,
            };
            DriverConfig::new(name, opts)
        });
        Drivers::new(vendors).unwrap()
    }

    fn mock_bigquery_n(n: usize) -> Self {
        let project_id = "test_project";
        let catalog = "test_catalog";
        let table = "test_table";

        let vendors = (0..n).map(|i| {
            let name = format!("bigquery_driver_{}", i);
            let project_id = project_id.to_string();
            let catalog = catalog.to_string();
            let table = format!("{}_{}", table, i);
            let opts = DriverOpts::BigQuery {
                project_id,
                catalog,
                key_file: None,
                table,
            };
            DriverConfig::new(name, opts)
        });
        Drivers::new(vendors).unwrap()
    }

    pub fn mock_n(n: usize, kind: Option<SupportedVendor>) -> Self {
        use SupportedVendor::*;
        if let Some(vendor) = kind {
            match vendor {
                Snowflake => return Self::mock_snowflake_n(n),
                Postgres => return Self::mock_postgres_n(n),
                BigQuery => return Self::mock_bigquery_n(n),
                _ => unimplemented!("Mock not implemented for vendor {vendor:?}"),
            }
        } else {
            let vendor_sample = vec![Snowflake, Postgres, BigQuery];
            let vendors = (0..n).map(|i| {
                let vendor = &vendor_sample[i % vendor_sample.len()];
                match vendor {
                    Snowflake => {
                        let name = format!("snowflake_driver_{}", i);
                        let account = "test_account".to_string();
                        let username = "test_user".to_string();
                        let password = "test_password".to_string().into();
                        let database = "test_database".to_string();
                        let schema = "test_schema".to_string();
                        let table = "test_table".to_string();
                        let warehouse = "test_warehouse".to_string();
                        let ingest_writer_concurrency = 4;
                        let ingest_upload_concurrency = 4;
                        let ingest_copy_concurrency = 4;
                        let ingest_target_file_size = 1024;
                        let opts = DriverOpts::Snowflake {
                            account,
                            username,
                            password,
                            database,
                            schema,
                            table,
                            warehouse,
                            role: None,
                            ingest_writer_concurrency,
                            ingest_upload_concurrency,
                            ingest_copy_concurrency,
                            ingest_target_file_size,
                        };
                        DriverConfig::new(name, opts)
                    }
                    Postgres => {
                        let name = format!("postgres_driver_{}", i);
                        let host = "localhost".to_string();
                        let port = 5432;
                        let username = "test_user".to_string();
                        let password = "test_password".to_string().into();
                        let database = "test_database".to_string();
                        let schema = "public".to_string();
                        let table = format!("test_table_{}", i);
                        let application_name = Some("ampdbc_test".to_string());
                        let opts = DriverOpts::Postgres {
                            host,
                            port,
                            username,
                            password,
                            database,
                            table,
                            socket: None,
                            application_name,
                            schema,
                            statement_cache_capacity: None,
                        };
                        DriverConfig::new(name, opts)
                    }
                    BigQuery => {
                        let name = format!("bigquery_driver_{}", i);
                        let project_id = "test_project".to_string();
                        let catalog = "test_catalog".to_string();
                        let table = format!("test_table_{}", i);
                        let opts = DriverOpts::BigQuery {
                            project_id,
                            catalog,
                            key_file: None,
                            table,
                        };
                        DriverConfig::new(name, opts)
                    }
                    _ => unimplemented!("Mock not implemented for vendor {vendor:?}"),
                }
            });
            Drivers::new(vendors).unwrap()
        }
    }
}

#[test]
fn test_config_serialization_json() {}

#[test]
fn test_config_deserialization_json() {}

#[test]
fn test_config_serialization_toml() {}

#[test]
fn test_config_deserialization_toml() {}
