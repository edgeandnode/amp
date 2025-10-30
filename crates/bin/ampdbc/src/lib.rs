#![feature(generic_const_exprs)]
#![allow(incomplete_features)]

pub mod client;
pub mod error;
pub mod metrics;
pub mod pipeline;
pub mod sql;

mod core {
    pub mod config;
    pub mod connection;
    pub mod database;
    pub mod driver;
    pub mod schema;
    pub mod statement;
}

mod drivers {
    mod helpers;

    crate::vendors!(
        dyn bigquery, BigQuery, false;
        dyn postgres, Postgres, false;
        dyn sqlite, Sqlite, false;
        extern snowflake, adbc_snowflake, true;
    );
}

pub use common::{BYTES32_TYPE, SPECIAL_BLOCK_NUM};

pub(crate) use crate::drivers::*;
use crate::sql::DDLSafety;
pub use crate::{
    core::{
        config::{self, Config, DriverOpts},
        connection::{self, Connection, ConnectionExt},
        database::{self, Database},
        driver::{self, Driver},
        schema::{self, Schema, SchemaExt},
        statement::{self, Statement, StatementExt},
    },
    drivers::SupportedVendor,
    error::{DriverError, Error, Result},
    pipeline::QueryPipeline,
};

pub(crate) mod adbc {
    pub use adbc_core::{
        Connection as AdbcConnection, Database as AdbcDatabase, Driver as AdbcDriver, Optionable,
        Statement as AdbcStatement, options,
    };
    pub mod error {
        pub use adbc_core::error::{
            Error as AdbcError, Result as AdbcResult,
        };
    }
}

pub(crate) mod arrow {
    pub(crate) mod array {
        #[allow(unused_imports)]
        pub use common::arrow::array::{
            Array, ArrayAccessor, ArrayRef, AsArray, Int64Array, PrimitiveArray, StringArray,
            StructArray, UInt64Array,
        };
    }

    pub mod datatypes {
        pub use common::arrow::datatypes::{
            DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit,
        };
    }

    pub mod error {
        pub use common::arrow::error::ArrowError;
    }

    pub mod record_batch {
        pub use common::arrow::record_batch::{RecordBatch, RecordBatchReader};
    }

    #[allow(unused_imports)]
    pub(crate) use array::*;
    pub(crate) use datatypes::*;
    pub(crate) use record_batch::*;
}

pub(crate) mod datafusion {
    pub use common::{DataFusionError, ScalarValue};
}

/// A trait implemented only for `Assert<true>`, enabling compile-time assertions that
/// fail when `CHECK` is false. You can use this trait as a bound on generic parameters to
/// enforce compile-time conditions.
///
/// ## Example
/// ```rust ignore
/// pub struct AtLeastN<const N: usize, T: MemoizedHash>
/// where
///     Assert<{ N > 0 }>: IsTrue,
///     [T; subtract_one::<N>()]: Sized,
/// # {
/// #    /// The first item (which always exists since N > 0)
/// #   first: T,
/// #   /// The next N-1 items (as an array)
/// #   /// If N == 1, this is an empty array.
/// #   next: Box<[T; subtract_one::<N>()]>,
/// #   /// Any additional items (beyond N)
/// #   /// This can be empty.
/// #   last: Vec<T>,
/// # }
/// ```
pub trait IsTrue {}

/// An empty enum used for compile-time assertions. In fact, it
/// cannot be instantiated at all. It is only used as a marker or
/// wrapper around a constant boolean value for the [`IsTrue`]
/// trait implementation.
pub enum Assert<const CHECK: bool> {}

impl IsTrue for Assert<true> {}

pub trait DriverSupport {
    fn is_supported(&self) -> bool;
}
