use adbc_core::{
    Optionable, PartitionedResult, Statement as AdbcStatement,
    error::Result as AdbcResult,
    options::{OptionStatement, OptionValue},
};
use common::arrow::{
    array::{RecordBatch, RecordBatchReader},
    datatypes::Schema as ArrowSchema,
};

use crate::{ConnectionExt, SchemaExt, connection::Connection, schema::Schema};
pub use crate::{
    bigquery::statement as bigquery, postgres::statement as postgres,
    snowflake::statement as snowflake, sqlite::statement as sqlite,
};

pub trait StatementExt: AdbcStatement
where
    Self: Sized + 'static,
    crate::Error: From<<<Self as StatementExt>::SchemaType as SchemaExt>::ErrorType>,
{
    type ConnectionType: ConnectionExt<Statement = Self>;
    type SchemaType: SchemaExt<Statement = Self>;

    fn prepare_create_table(
        &mut self,
        schema: &Self::SchemaType,
    ) -> std::result::Result<(), <Self::SchemaType as SchemaExt>::ErrorType> {
        let query = schema.as_table_ddl()?;
        self.set_sql_query(query)?;
        Ok(())
    }

    fn prepare_insert(
        &mut self,
        schema: &Self::SchemaType,
    ) -> std::result::Result<(), <Self::SchemaType as SchemaExt>::ErrorType> {
        let query = schema.clone().as_insert_stmt()?;
        self.set_sql_query(query)?;

        Ok(())
    }

    fn prepare_insert_returning_id(
        &mut self,
        schema: &Self::SchemaType,
    ) -> std::result::Result<(), <Self::SchemaType as SchemaExt>::ErrorType> {
        let query = format!("{} RETURNING id", schema.clone().as_insert_stmt()?);
        self.set_sql_query(query)?;

        Ok(())
    }
}

pub enum Statement {
    BigQuery(bigquery::Statement),
    Postgres(postgres::Statement),
    Snowflake(snowflake::Statement),
    Sqlite(sqlite::Statement),
}

impl StatementExt for Statement {
    type ConnectionType = Connection;
    type SchemaType = Schema;
}

impl Optionable for Statement {
    type Option = OptionStatement;

    fn get_option_bytes(&self, key: Self::Option) -> AdbcResult<Vec<u8>> {
        match self {
            Self::Postgres(stmt) => stmt.get_option_bytes(key),
            Self::Snowflake(stmt) => stmt.get_option_bytes(key),
            _ => unimplemented!(),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> AdbcResult<f64> {
        match self {
            Self::Postgres(stmt) => stmt.get_option_double(key),
            Self::Snowflake(stmt) => stmt.get_option_double(key),
            _ => unimplemented!(),
        }
    }

    fn get_option_int(&self, key: Self::Option) -> AdbcResult<i64> {
        match self {
            Self::Postgres(stmt) => stmt.get_option_int(key),
            Self::Snowflake(stmt) => stmt.get_option_int(key),
            _ => unimplemented!(),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> AdbcResult<String> {
        match self {
            Self::Postgres(stmt) => stmt.get_option_string(key),
            Self::Snowflake(stmt) => stmt.get_option_string(key),
            _ => unimplemented!(),
        }
    }

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> AdbcResult<()> {
        match self {
            Self::Postgres(stmt) => stmt.set_option(key, value),
            Self::Snowflake(stmt) => stmt.set_option(key, value),
            _ => unimplemented!(),
        }
    }
}

impl AdbcStatement for Statement {
    fn prepare(&mut self) -> AdbcResult<()> {
        use Statement::*;
        match self {
            BigQuery(stmt) => stmt.prepare(),
            Postgres(stmt) => stmt.prepare(),
            Snowflake(stmt) => stmt.prepare(),
            Sqlite(stmt) => stmt.prepare(),
        }
    }

    fn bind(&mut self, batch: RecordBatch) -> AdbcResult<()> {
        use Statement::*;
        match self {
            BigQuery(stmt) => stmt.bind(batch),
            Postgres(stmt) => stmt.bind(batch),
            Snowflake(stmt) => stmt.bind(batch),
            Sqlite(stmt) => stmt.bind(batch),
        }
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
        use Statement::*;
        match self {
            BigQuery(stmt) => stmt.bind_stream(reader),
            Postgres(stmt) => stmt.bind_stream(reader),
            Snowflake(stmt) => stmt.bind_stream(reader),
            Sqlite(stmt) => stmt.bind_stream(reader),
        }
    }

    fn cancel(&mut self) -> AdbcResult<()> {
        use Statement::*;
        match self {
            BigQuery(stmt) => stmt.cancel(),
            Postgres(stmt) => stmt.cancel(),
            Snowflake(stmt) => stmt.cancel(),
            Sqlite(stmt) => stmt.cancel(),
        }
    }

    fn execute(&mut self) -> AdbcResult<impl RecordBatchReader + Send> {
        match self {
            Self::Snowflake(stmt) => stmt.execute(),
            _ => unimplemented!(),
        }
    }

    fn execute_partitions(&mut self) -> AdbcResult<PartitionedResult> {
        use Statement::*;
        match self {
            BigQuery(stmt) => stmt.execute_partitions(),
            Postgres(stmt) => stmt.execute_partitions(),
            Snowflake(stmt) => stmt.execute_partitions(),
            Sqlite(stmt) => stmt.execute_partitions(),
        }
    }

    fn execute_schema(&mut self) -> AdbcResult<ArrowSchema> {
        match self {
            Self::Snowflake(stmt) => stmt.execute_schema(),
            _ => unimplemented!(),
        }
    }

    fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
        match self {
            Self::Snowflake(stmt) => stmt.execute_update(),
            _ => unimplemented!(),
        }
    }

    fn get_parameter_schema(&self) -> AdbcResult<ArrowSchema> {
        match self {
            Self::Snowflake(stmt) => stmt.get_parameter_schema(),
            _ => unimplemented!(),
        }
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> AdbcResult<()> {
        match self {
            Self::Snowflake(stmt) => stmt.set_sql_query(query),
            _ => unimplemented!(),
        }
    }

    fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> AdbcResult<()> {
        match self {
            Self::Snowflake(stmt) => stmt.set_substrait_plan(plan),
            _ => unimplemented!(),
        }
    }
}
