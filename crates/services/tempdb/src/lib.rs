pub mod service;

use std::sync::LazyLock;

pub use service::TempDbHandle;

/// Whether to keep the temporary directory after the database is dropped
///
/// This is set to `false` by default, but can be overridden by the `KEEP_TEMP_DIRS` environment
/// variable.
pub static KEEP_TEMP_DIRS: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("KEEP_TEMP_DIRS")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
});
