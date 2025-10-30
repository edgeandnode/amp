use crate::{
    adbc::{
        AdbcDatabase, Optionable,
        error::AdbcResult,
        options::{OptionDatabase, OptionValue},
    },
    connection::Connection,
};
pub use crate::{
    bigquery::database as bigquery, postgres::database as postgres,
    snowflake::database as snowflake, sqlite::database as sqlite,
};

#[derive(Clone)]
pub enum Database {
    BigQuery(bigquery::Database),
    Postgres(postgres::Database),
    Snowflake(snowflake::Database),
    Sqlite(sqlite::Database),
}

impl From<snowflake::Database> for Database {
    fn from(db: snowflake::Database) -> Self {
        Database::Snowflake(db)
    }
}

impl Optionable for Database {
    type Option = OptionDatabase;

    fn get_option_bytes(&self, key: Self::Option) -> AdbcResult<Vec<u8>> {
        use Database::*;
        match self {
            BigQuery(db) => db.get_option_bytes(key),
            Postgres(db) => db.get_option_bytes(key),
            Snowflake(db) => db.get_option_bytes(key),
            Sqlite(db) => db.get_option_bytes(key),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> AdbcResult<f64> {
        use Database::*;
        match self {
            BigQuery(db) => db.get_option_double(key),
            Postgres(db) => db.get_option_double(key),
            Snowflake(db) => db.get_option_double(key),
            Sqlite(db) => db.get_option_double(key),
        }
    }

    fn get_option_int(&self, key: Self::Option) -> AdbcResult<i64> {
        use Database::*;
        match self {
            BigQuery(db) => db.get_option_int(key),
            Postgres(db) => db.get_option_int(key),
            Snowflake(db) => db.get_option_int(key),
            Sqlite(db) => db.get_option_int(key),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> AdbcResult<String> {
        use Database::*;
        match self {
            BigQuery(db) => db.get_option_string(key),
            Postgres(db) => db.get_option_string(key),
            Snowflake(db) => db.get_option_string(key),
            Sqlite(db) => db.get_option_string(key),
        }
    }

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> AdbcResult<()> {
        use Database::*;
        match self {
            BigQuery(db) => db.set_option(key, value),
            Postgres(db) => db.set_option(key, value),
            Snowflake(db) => db.set_option(key, value),
            Sqlite(db) => db.set_option(key, value),
        }
    }
}

impl AdbcDatabase for Database {
    type ConnectionType = Connection;
    fn new_connection(&self) -> AdbcResult<Self::ConnectionType> {
        use Database::*;
        match self {
            BigQuery(db) => db.new_connection().map(Connection::BigQuery),
            Postgres(db) => db.new_connection().map(Connection::Postgres),
            Snowflake(db) => db.new_connection().map(Connection::Snowflake),
            Sqlite(db) => db.new_connection().map(Connection::Sqlite),
        }
    }
    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<
            Item = (
                adbc_core::options::OptionConnection,
                adbc_core::options::OptionValue,
            ),
        >,
    ) -> AdbcResult<Self::ConnectionType> {
        use Database::*;
        match self {
            BigQuery(db) => db.new_connection_with_opts(opts).map(Connection::BigQuery),
            Postgres(db) => db.new_connection_with_opts(opts).map(Connection::Postgres),
            Snowflake(db) => db.new_connection_with_opts(opts).map(Connection::Snowflake),
            Sqlite(db) => db.new_connection_with_opts(opts).map(Connection::Sqlite),
        }
    }
}
