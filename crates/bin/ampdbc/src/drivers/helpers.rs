#[macro_export]
macro_rules! impl_adbc_optionable {
    ( $ty:ty, $option_ty:ty ) => {
        impl Optionable for $ty {
            type Option = $option_ty;
            fn get_option_bytes(&self, key: Self::Option) -> AdbcResult<Vec<u8>> {
                self.0.get_option_bytes(key)
            }

            fn get_option_double(&self, key: Self::Option) -> AdbcResult<f64> {
                self.0.get_option_double(key)
            }

            fn get_option_int(&self, key: Self::Option) -> AdbcResult<i64> {
                self.0.get_option_int(key)
            }

            fn get_option_string(&self, key: Self::Option) -> AdbcResult<String> {
                self.0.get_option_string(key)
            }

            fn set_option(&mut self, key: Self::Option, value: OptionValue) -> AdbcResult<()> {
                self.0.set_option(key, value)
            }
        }
    };
}

#[macro_export]
macro_rules! impl_adbc_connection {
    ( $variant:ident ) => {
        pub mod connection {
            use std::collections::HashSet;

            use adbc_driver_manager::ManagedConnection;

            use super::statement::Statement;
            use crate::{
                ConnectionExt,
                adbc::{
                    AdbcConnection, Optionable,
                    error::AdbcResult,
                    options::{InfoCode, ObjectDepth, OptionConnection, OptionValue},
                },
                arrow::{RecordBatchReader, Schema},
            };

            #[derive(Clone)]
            pub struct Connection(pub(crate) ManagedConnection);

            crate::impl_adbc_optionable!(Connection, OptionConnection);

            impl AdbcConnection for Connection {
                type StatementType = Statement;

                fn cancel(&mut self) -> AdbcResult<()> {
                    self.0.cancel()
                }

                fn commit(&mut self) -> AdbcResult<()> {
                    self.0.commit()
                }

                fn get_info(
                    &self,
                    codes: Option<HashSet<InfoCode>>,
                ) -> AdbcResult<impl RecordBatchReader + Send> {
                    self.0.get_info(codes)
                }

                fn get_objects(
                    &self,
                    depth: ObjectDepth,
                    catalog: Option<&str>,
                    db_schema: Option<&str>,
                    table_name: Option<&str>,
                    table_type: Option<Vec<&str>>,
                    column_name: Option<&str>,
                ) -> AdbcResult<impl RecordBatchReader + Send> {
                    self.0.get_objects(
                        depth,
                        catalog,
                        db_schema,
                        table_name,
                        table_type,
                        column_name,
                    )
                }

                fn get_statistic_names(&self) -> AdbcResult<impl RecordBatchReader + Send> {
                    self.0.get_statistic_names()
                }

                fn get_statistics(
                    &self,
                    catalog: Option<&str>,
                    db_schema: Option<&str>,
                    table_name: Option<&str>,
                    approximate: bool,
                ) -> AdbcResult<impl RecordBatchReader + Send> {
                    self.0
                        .get_statistics(catalog, db_schema, table_name, approximate)
                }

                fn get_table_schema(
                    &self,
                    catalog: Option<&str>,
                    db_schema: Option<&str>,
                    table_name: &str,
                ) -> AdbcResult<Schema> {
                    self.0.get_table_schema(catalog, db_schema, table_name)
                }

                fn get_table_types(&self) -> AdbcResult<impl RecordBatchReader + Send> {
                    self.0.get_table_types()
                }

                fn new_statement(&mut self) -> AdbcResult<Self::StatementType> {
                    self.0.new_statement().map(Statement)
                }

                fn read_partition(
                    &self,
                    partition: impl AsRef<[u8]>,
                ) -> AdbcResult<impl RecordBatchReader + Send> {
                    self.0.read_partition(partition)
                }

                fn rollback(&mut self) -> AdbcResult<()> {
                    self.0.rollback()
                }
            }

            impl From<Connection> for crate::connection::Connection {
                fn from(conn: Connection) -> Self {
                    crate::connection::Connection::$variant(conn)
                }
            }

            impl ConnectionExt for Connection {
                type Statement = super::statement::Statement;
                fn create_statement(&mut self) -> Result<Self::Statement, super::error::Error> {
                    Ok(self.new_statement()?)
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_adbc_database {
    ($variant:ident) => {
        pub mod database {
            use adbc_driver_manager::ManagedDatabase;

            use super::connection::Connection;
            use crate::adbc::{
                AdbcDatabase, Optionable,
                error::AdbcResult,
                options::{OptionConnection, OptionDatabase, OptionValue},
            };

            #[derive(Clone)]
            pub struct Database(pub(crate) ManagedDatabase);

            crate::impl_adbc_optionable!(Database, OptionDatabase);

            impl AdbcDatabase for Database {
                type ConnectionType = Connection;
                fn new_connection(&self) -> AdbcResult<Self::ConnectionType> {
                    self.0.new_connection().map(Connection)
                }

                fn new_connection_with_opts(
                    &self,
                    opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
                ) -> AdbcResult<Self::ConnectionType> {
                    self.0.new_connection_with_opts(opts).map(Connection)
                }
            }

            impl From<Database> for crate::database::Database {
                fn from(db: Database) -> Self {
                    crate::database::Database::$variant(db)
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_adbc_statement {
    ( $variant:ident ) => {
        pub mod statement {
            use adbc_driver_manager::ManagedStatement;

            use crate::{
                StatementExt,
                adbc::{
                    AdbcStatement, Optionable,
                    error::AdbcResult,
                    options::{OptionStatement, OptionValue},
                },
                arrow::{RecordBatch, RecordBatchReader, Schema},
            };

            #[derive(Clone)]
            pub struct Statement(pub(crate) ManagedStatement);

            impl StatementExt for Statement {
                type ConnectionType = super::connection::Connection;
                type SchemaType = super::schema::Schema;
            }

            crate::impl_adbc_optionable!(Statement, OptionStatement);

            impl AdbcStatement for Statement {
                fn bind(&mut self, batch: RecordBatch) -> AdbcResult<()> {
                    self.0.bind(batch)
                }

                fn prepare(&mut self) -> AdbcResult<()> {
                    self.0.prepare()
                }

                fn bind_stream(
                    &mut self,
                    reader: Box<dyn RecordBatchReader + Send>,
                ) -> AdbcResult<()> {
                    self.0.bind_stream(reader)
                }

                fn cancel(&mut self) -> AdbcResult<()> {
                    self.0.cancel()
                }

                fn execute(&mut self) -> AdbcResult<impl RecordBatchReader + Send> {
                    self.0.execute()
                }

                fn execute_partitions(&mut self) -> AdbcResult<adbc_core::PartitionedResult> {
                    self.0.execute_partitions()
                }

                fn execute_schema(&mut self) -> AdbcResult<Schema> {
                    self.0.execute_schema()
                }

                fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
                    self.0.execute_update()
                }

                fn get_parameter_schema(&self) -> AdbcResult<Schema> {
                    self.0.get_parameter_schema()
                }

                fn set_sql_query(&mut self, query: impl AsRef<str>) -> AdbcResult<()> {
                    self.0.set_sql_query(query)
                }

                fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> AdbcResult<()> {
                    self.0.set_substrait_plan(plan)
                }
            }

            impl From<Statement> for crate::Statement {
                fn from(stmt: Statement) -> Self {
                    crate::Statement::$variant(stmt)
                }
            }
        }
    };
}

#[macro_export]
macro_rules! impl_adbc_driver {
    ( $supported:expr ) => {
        pub mod driver {
            use adbc_driver_manager::ManagedDriver;

            use super::database::Database;
            use crate::adbc::{
                AdbcDriver,
                error::AdbcResult,
                options::{OptionDatabase, OptionValue},
            };

            pub const SUPPORT_STATUS: bool = $supported;

            #[derive(Clone, Debug)]
            pub struct Driver(pub(crate) ManagedDriver);

            impl AdbcDriver for Driver {
                type DatabaseType = Database;
                fn new_database(&mut self) -> AdbcResult<Self::DatabaseType> {
                    self.0.new_database().map(Database)
                }

                fn new_database_with_opts(
                    &mut self,
                    opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
                ) -> AdbcResult<Self::DatabaseType> {
                    self.0.new_database_with_opts(opts).map(Database)
                }
            }
        }
    };
}

#[macro_export]
macro_rules! variant {
    ( in $( $($vendor:ident$(, $variant:ident)?;)+)* ) => {
        $(
            crate::variant! ( out $vendor $(, $variant)? )
        )+
    };
    ( out $vendor:ident ) => {
        paste! {
            [<$vendor:camel>]
        }
    };
    ( out $vendor:ident, $variant:ident ) => {
        $variant
    };
}

#[macro_export]
macro_rules! vendors {
    ( $(dyn $vendor:ident, $variant:ident$(, $supported:literal)?;)*$(extern $native_vendor:ident, $externa_crate:path, true;)+ ) => {

        paste::paste! {
            #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
            pub enum SupportedVendor {
                $(
                    $variant,
                )*
                $(
                    [<$native_vendor:camel>],
                )*
            }
            impl std::fmt::Display for SupportedVendor {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    match self {
                        $(
                            SupportedVendor::$variant => write!(f, stringify!($variant)),
                        )*
                        $(
                            SupportedVendor::[<$native_vendor:camel>] => write!(f, stringify!($native_vendor)),
                        )*
                    }
                }
            }
        }
        $(
                crate::vendors! ( $vendor, $variant$(, $supported)? );
        )*
        $(
            pub mod $native_vendor {
                pub mod error;
                pub mod schema;

                pub mod connection {
                    pub use adbc_snowflake::connection::{Builder, Connection};

                    use crate::adbc::AdbcConnection;

                    impl crate::ConnectionExt for Connection {
                        type Statement = crate::drivers::snowflake::statement::Statement;

                        fn create_statement(&mut self) -> Result<Self::Statement, super::error::Error> {
                            Ok(self.new_statement()?)
                        }
                    }

                    impl From<Connection> for crate::Connection {
                        fn from(conn: Connection) -> Self {
                            crate::Connection::Snowflake(conn)
                        }
                    }
                }

                pub mod database {
                    #[allow(unused_imports)]
                    pub use adbc_snowflake::database::{AuthType, Builder, Database, LogLevel, Protocol};
                }

                pub mod driver {
                    #[allow(unused_imports)]
                    pub use adbc_snowflake::driver::{Builder, Driver};

                    pub const SUPPORT_STATUS: bool = true;
                }

                pub mod statement {
                    pub use adbc_snowflake::Statement;
                }
            }
        )*
    };
    ( $vendor:ident, $variant:ident ) => {
        crate::vendors! { $vendor, $variant, false }
    };

    ( $vendor:ident, $variant:ident, $supported:expr ) => {
        pub mod $vendor {
            pub mod config;
            pub mod error;
            pub mod schema;

            crate::impl_adbc_statement!($variant);
            crate::impl_adbc_connection!($variant);
            crate::impl_adbc_database!($variant);
            crate::impl_adbc_driver!($supported);
        }
    };
}
