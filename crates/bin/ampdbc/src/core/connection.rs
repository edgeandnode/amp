use std::collections::HashSet;

use adbc_core::{
    Connection as AdbcConnection, Database as AdbcDatabase, Optionable,
    error::Result as AdbcResult,
    options::{InfoCode, IsolationLevel, ObjectDepth, OptionConnection, OptionValue},
};
use common::arrow::{array::RecordBatchReader, datatypes::Schema};
use tracing::info;

use crate::{
    DDLSafety, DriverSupport, SchemaExt, Statement, StatementExt, SupportedVendor,
    config::DriverOpts, error::Result,
};
pub use crate::{
    bigquery::connection as bigquery, postgres::connection as postgres,
    snowflake::connection as snowflake, sqlite::connection as sqlite,
};

pub trait ConnectionExt: AdbcConnection
where
    Self: Sized + 'static,
    crate::Error: From<
        <<<Self as ConnectionExt>::Statement as StatementExt>::SchemaType as SchemaExt>::ErrorType,
    >,
{
    type Statement: StatementExt<ConnectionType = Self>;

    fn create_statement(
        &mut self,
    ) -> std::result::Result<
        Self::Statement,
        <<<Self as ConnectionExt>::Statement as StatementExt>::SchemaType as SchemaExt>::ErrorType,
    >
    where
        Self: Sized;

    fn create_insert_statement(
        &mut self,
        schema: &<Self::Statement as StatementExt>::SchemaType,
    ) -> crate::Result<Self::Statement> {
        let mut stmt: Self::Statement = self.create_statement()?;

        stmt.prepare_insert(&schema)?;
        Ok(stmt)
    }

    fn create_insert_statement_returning_id(
        &mut self,
        schema: &<Self::Statement as StatementExt>::SchemaType,
    ) -> crate::Result<Self::Statement> {
        let mut stmt: Self::Statement = self.create_statement()?;

        stmt.prepare_insert(&schema)?;
        Ok(stmt)
    }

    fn create_table_ddl_statement(
        &mut self,
        schema: <Self::Statement as StatementExt>::SchemaType,
        ddl_safety: DDLSafety,
    ) -> crate::Result<Self::Statement> {
        let mut stmt: Self::Statement = self.create_statement()?;

        stmt.prepare_create_table(schema, ddl_safety)?;
        Ok(stmt)
    }
}

#[derive(Clone)]
pub enum Connection {
    BigQuery(bigquery::Connection),
    Postgres(postgres::Connection),
    Snowflake(snowflake::Connection),
    Sqlite(sqlite::Connection),
}

impl Connection {
    pub fn connect(opts: &mut DriverOpts) -> Result<Self> {
        Self::try_from(opts)
    }
}

impl ConnectionExt for Connection {
    type Statement = Statement;
    fn create_statement(
            &mut self,
        ) -> std::result::Result<
            Self::Statement,
            <<<Self as ConnectionExt>::Statement as crate::StatementExt>::SchemaType as crate::SchemaExt>::ErrorType,
        >
        where
    Self: Sized{
        match self {
            Connection::BigQuery(conn) => Ok(Statement::BigQuery(conn.create_statement()?)),
            Connection::Postgres(conn) => Ok(Statement::Postgres(conn.create_statement()?)),
            Connection::Snowflake(conn) => Ok(Statement::Snowflake(conn.create_statement()?)),
            Connection::Sqlite(conn) => Ok(Statement::Sqlite(conn.create_statement()?)),
        }
    }
}

impl Optionable for Connection {
    type Option = OptionConnection;
    fn get_option_bytes(&self, key: Self::Option) -> AdbcResult<Vec<u8>> {
        use Connection::*;
        match self {
            BigQuery(conn) => conn.get_option_bytes(key),
            Postgres(conn) => conn.get_option_bytes(key),
            Snowflake(conn) => conn.get_option_bytes(key),
            Sqlite(conn) => conn.get_option_bytes(key),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> AdbcResult<f64> {
        use Connection::*;
        match self {
            BigQuery(conn) => conn.get_option_double(key),
            Postgres(conn) => conn.get_option_double(key),
            Snowflake(conn) => conn.get_option_double(key),
            Sqlite(conn) => conn.get_option_double(key),
        }
    }

    fn get_option_int(&self, key: Self::Option) -> AdbcResult<i64> {
        use Connection::*;
        match self {
            BigQuery(conn) => conn.get_option_int(key),
            Postgres(conn) => conn.get_option_int(key),
            Snowflake(conn) => conn.get_option_int(key),
            Sqlite(conn) => conn.get_option_int(key),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> AdbcResult<String> {
        use Connection::*;
        match self {
            BigQuery(conn) => conn.get_option_string(key),
            Postgres(conn) => conn.get_option_string(key),
            Snowflake(conn) => conn.get_option_string(key),
            Sqlite(conn) => conn.get_option_string(key),
        }
    }

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> AdbcResult<()> {
        use Connection::*;
        match self {
            BigQuery(conn) => conn.set_option(key, value),
            Postgres(conn) => conn.set_option(key, value),
            Snowflake(conn) => conn.set_option(key, value),
            Sqlite(conn) => conn.set_option(key, value),
        }
    }
}

impl AdbcConnection for Connection {
    type StatementType = Statement;

    fn cancel(&mut self) -> AdbcResult<()> {
        match self {
            Self::Snowflake(conn) => conn.cancel(),
            _ => unimplemented!(),
        }
    }

    fn commit(&mut self) -> AdbcResult<()> {
        match self {
            Self::Snowflake(conn) => conn.commit(),
            _ => unimplemented!(),
        }
    }

    fn rollback(&mut self) -> AdbcResult<()> {
        match self {
            Self::Snowflake(conn) => conn.rollback(),
            _ => unimplemented!(),
        }
    }

    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> AdbcResult<impl RecordBatchReader + Send> {
        match self {
            Self::Snowflake(conn) => conn.get_info(codes),
            _ => unimplemented!(),
        }
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
        match self {
            Self::Snowflake(conn) => conn.get_objects(
                depth,
                catalog,
                db_schema,
                table_name,
                table_type,
                column_name,
            ),
            _ => unimplemented!(),
        }
    }

    fn get_statistic_names(&self) -> AdbcResult<impl RecordBatchReader + Send> {
        match self {
            Self::Snowflake(conn) => conn.get_statistic_names(),
            _ => unimplemented!(),
        }
    }

    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> AdbcResult<impl RecordBatchReader + Send> {
        match self {
            Self::Snowflake(conn) => {
                conn.get_statistics(catalog, db_schema, table_name, approximate)
            }
            _ => unimplemented!(),
        }
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> AdbcResult<Schema> {
        match self {
            Self::Snowflake(conn) => conn.get_table_schema(catalog, db_schema, table_name),
            _ => unimplemented!(),
        }
    }

    fn get_table_types(&self) -> AdbcResult<impl RecordBatchReader + Send> {
        match self {
            Self::Snowflake(conn) => conn.get_table_types(),
            _ => unimplemented!(),
        }
    }

    fn new_statement(&mut self) -> AdbcResult<Self::StatementType> {
        match self {
            Self::Snowflake(conn) => Ok(Statement::Snowflake(conn.new_statement()?)),
            _ => unimplemented!(),
        }
    }

    fn read_partition(
        &self,
        partition: impl AsRef<[u8]>,
    ) -> AdbcResult<impl RecordBatchReader + Send> {
        match self {
            Self::Snowflake(conn) => conn.read_partition(partition),
            _ => unimplemented!(),
        }
    }
}

impl<'a> TryFrom<&'a mut DriverOpts> for Connection {
    type Error = crate::Error;
    fn try_from(value: &'a mut DriverOpts) -> Result<Self> {
        use DriverOpts::*;
        match value {
            Snowflake {
                account,
                username,
                password,
                database,
                schema,
                warehouse,
                role,
                ..
            } => {
                use crate::error::snowflake::Error;

                info!("Connecting to Snowflake: {database}.{schema} @ {account}");

                let mut driver = adbc_snowflake::Driver::try_load().map_err(Error::from)?;

                let builder = if let Some(password) = password.take() {
                    adbc_snowflake::database::Builder::default()
                        .with_account(account.to_string())
                        .with_username(username.to_string())
                        .with_password(password)
                        .with_database(database.to_string())
                        .with_schema(schema.to_string())
                        .with_warehouse(warehouse.to_string())
                } else {
                    prompt_for_password(password, username, SupportedVendor::Snowflake)?;

                    return Self::try_from(value);
                };

                let database = if let Some(role) = role {
                    builder.with_role(role.to_string()).build(&mut driver)?
                } else {
                    builder.build(&mut driver)?
                };

                let mut conn = database.new_connection().map_err(Error::from)?;
                // conn.set_option(OptionConnection::AutoCommit, "false".into())?;
                conn.set_option(
                    OptionConnection::IsolationLevel,
                    IsolationLevel::Snapshot.into(),
                )?;

                Ok(Self::Snowflake(conn))
            }
            _ => unimplemented!(),
        }
    }
}

impl DriverSupport for Connection {
    fn is_supported(&self) -> bool {
        use Connection::*;

        use crate::driver::*;

        match self {
            BigQuery(_) => bigquery::SUPPORT_STATUS,
            Postgres(_) => postgres::SUPPORT_STATUS,
            Snowflake(_) => snowflake::SUPPORT_STATUS,
            Sqlite(_) => sqlite::SUPPORT_STATUS,
        }
    }
}

fn prompt_for_password(
    password: &mut Option<String>,
    username: &str,
    vendor: SupportedVendor,
) -> Result<()> {
    use std::io::{Write, stdin, stdout};

    use termion::input::TermRead;
    let stdout = stdout();
    let mut stdout = stdout.lock();
    let stdin = stdin();
    let mut stdin = stdin.lock();

    write!(
        stdout,
        "Enter password for user '{}' on {}: ",
        username, vendor
    )?;
    stdout.flush()?;

    let input = stdin.read_passwd(&mut stdout)?.unwrap_or_default();

    if let Some(mut old) = std::mem::replace(password, Some(input.trim().to_string())) {
        let fill = u8::default();
        unsafe {
            old.as_bytes_mut().fill(fill);
        }
        old.clear();
    };
    Ok(())
}
