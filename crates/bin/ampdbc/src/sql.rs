pub mod error;
pub mod validation;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DDLSafety {
    // DropIfExists,
    IfNotExists,
    OrReplace,
    Unsafe,
}

impl DDLSafety {
    pub fn to_sql_clause(&self) -> &'static str {
        use DDLSafety::*;

        match self {
            // DropIfExists => "DROP TABLE IF EXISTS",
            IfNotExists => "CREATE TABLE IF NOT EXISTS",
            OrReplace => "CREATE OR REPLACE TABLE",
            Unsafe => "CREATE TABLE",
        }
    }
}

impl Default for DDLSafety {
    fn default() -> Self {
        DDLSafety::IfNotExists
    }
}
