use crate::{
    adbc::{
        AdbcDriver,
        error::AdbcResult,
        options::{OptionDatabase, OptionValue},
    },
    database::Database,
};
pub use crate::{
    bigquery::driver as bigquery, postgres::driver as postgres, snowflake::driver as snowflake,
    sqlite::driver as sqlite,
};

#[derive(Clone, Debug)]
pub enum Driver {
    BigQuery(bigquery::Driver),
    Postgres(postgres::Driver),
    Snowflake(snowflake::Driver),
    Sqlite(sqlite::Driver),
}

impl AdbcDriver for Driver {
    type DatabaseType = Database;

    fn new_database(&mut self) -> AdbcResult<Self::DatabaseType> {
        match self {
            Driver::BigQuery(driver) => driver.new_database().map(Into::into),
            Driver::Postgres(driver) => driver.new_database().map(Into::into),
            Driver::Snowflake(driver) => driver.new_database().map(Into::into),
            Driver::Sqlite(driver) => driver.new_database().map(Into::into),
        }
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> AdbcResult<Self::DatabaseType> {
        match self {
            Driver::BigQuery(driver) => driver.new_database_with_opts(opts).map(Into::into),
            Driver::Postgres(driver) => driver.new_database_with_opts(opts).map(Into::into),
            Driver::Snowflake(driver) => driver.new_database_with_opts(opts).map(Into::into),
            Driver::Sqlite(driver) => driver.new_database_with_opts(opts).map(Into::into),
        }
    }
}
