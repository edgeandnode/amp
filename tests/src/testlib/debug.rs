use std::sync::LazyLock;

/// Whether to keep the temporary directory after the Metadata DB is dropped
///
/// This is set to `false` by default, but can be overridden by the `TESTS_KEEP_TEMP_DIRS` environment
/// variable.
pub static TESTS_KEEP_TEMP_DIRS: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("TESTS_KEEP_TEMP_DIRS")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
});

/// Initialize color backtrace for better error messages in tests.
#[ctor::ctor]
fn init_color_backtrace() {
    color_backtrace::install();
}
