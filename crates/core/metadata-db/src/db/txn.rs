//! Transaction wrapper providing RAII semantics with automatic rollback

use sqlx::Postgres;

use crate::error::Error;

/// Transaction wrapper providing RAII semantics
///
/// This type wraps a [`sqlx::Transaction`] and provides automatic rollback behavior.
/// The transaction will be rolled back automatically when dropped unless `commit()`
/// is called explicitly.
#[derive(Debug)]
pub struct Transaction(sqlx::Transaction<'static, Postgres>);

impl Transaction {
    /// Wraps a `sqlx` transaction with RAII rollback semantics.
    pub(crate) fn new(tx: sqlx::Transaction<'static, Postgres>) -> Self {
        Self(tx)
    }

    /// Commits all changes made within this transaction.
    ///
    /// If not called, the transaction automatically rolls back when dropped.
    pub async fn commit(self) -> Result<(), Error> {
        self.0.commit().await.map_err(Error::Database)
    }

    /// Rolls back all changes made within this transaction.
    ///
    /// Equivalent to dropping the transaction but allows explicit error handling.
    pub async fn rollback(self) -> Result<(), Error> {
        self.0.rollback().await.map_err(Error::Database)
    }
}

// Implement sqlx::Executor for &mut Transaction by delegating to the underlying sqlx::Transaction
impl<'c> sqlx::Executor<'c> for &'c mut Transaction {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::stream::BoxStream<
        'e,
        Result<
            sqlx::Either<
                <Postgres as sqlx::Database>::QueryResult,
                <Postgres as sqlx::Database>::Row,
            >,
            sqlx::Error,
        >,
    >
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Self::Database>,
    {
        (&mut self.0).fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures::future::BoxFuture<
        'e,
        Result<Option<<Postgres as sqlx::Database>::Row>, sqlx::Error>,
    >
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Self::Database>,
    {
        (&mut self.0).fetch_optional(query)
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Postgres as sqlx::Database>::TypeInfo],
    ) -> futures::future::BoxFuture<
        'e,
        Result<<Postgres as sqlx::Database>::Statement<'q>, sqlx::Error>,
    >
    where
        'c: 'e,
    {
        (&mut self.0).prepare_with(sql, parameters)
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures::future::BoxFuture<'e, Result<sqlx::Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        (&mut self.0).describe(sql)
    }
}

impl<'c> super::Executor<'c> for &'c mut Transaction {}

impl crate::_priv::Sealed for &mut Transaction {}
