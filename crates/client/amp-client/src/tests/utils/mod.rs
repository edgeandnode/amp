//! Test utilities for amp-client streaming tests.
//!
//! This module provides ergonomic helpers for testing streaming scenarios including:
//! - Response builders for creating mock ResponseBatch sequences
//! - Scenario builder for declarative test setup
//! - Assertion helpers for validating events
//! - Mock stores for failure injection
//!
//! # Using the `scenario!` macro
//!
//! The `scenario!` macro provides a concise way to define test scenarios:
//!
//! ```ignore
//! // Simple scenario
//! let events = scenario! {
//!     Step::data(0..=10),
//!     Step::watermark(10),
//! }.await;
//!
//! // With configuration (note semicolon separator)
//! let events = scenario! {
//!     store: my_store,
//!     retention: 64;
//!
//!     Step::data(0..=10),
//!     Step::watermark(10),
//! }.await;
//!
//! // Get stream instead of auto-collecting
//! let mut stream = scenario! {
//!     @stream
//!     Step::data(0..=10),
//!     Step::watermark(10),
//! }.await;
//! ```

pub mod assert;
pub mod cdc;
pub mod response;
pub mod scenario;
pub mod store;

pub use assert::*;
pub use cdc::*;
pub use response::Step;
pub use scenario::{MockResponseStream, Scenario};
pub use store::SharedStore;

/// Macro for creating test scenarios with minimal boilerplate.
///
/// # Basic usage
///
/// ```ignore
/// let events = scenario! {
///     Step::data(0..=10),
///     Step::watermark(10),
/// }.await;
/// ```
///
/// # With configuration (semicolon separator required)
///
/// ```ignore
/// let events = scenario! {
///     store: my_store,
///     retention: 128;
///
///     Step::data(0..=10),
///     Step::watermark(10),
/// }.await;
/// ```
///
/// # Stream mode
///
/// ```ignore
/// let mut stream = scenario! {
///     @stream
///     Step::data(0..=10),
///     Step::watermark(10),
/// }.await;
/// ```
#[macro_export]
macro_rules! scenario {
    // Simple case: just steps, auto-collect
    ($($step:expr),* $(,)?) => {{
        async {
            $crate::tests::utils::Scenario::builder()
                $(.push($step))*
                .build()
                .run()
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
        }
    }};

    // Stream mode: return stream instead of collecting
    (@stream $($step:expr),* $(,)?) => {{
        async {
            $crate::tests::utils::Scenario::builder()
                $(.push($step))*
                .build()
                .run()
                .await
                .unwrap()
                .stream()
        }
    }};

    // With config options, then steps (auto-collect)
    (
        $($key:ident : $value:expr),+ ;
        $($step:expr),* $(,)?
    ) => {{
        async {
            let builder = $crate::tests::utils::Scenario::builder();
            $(
                let builder = scenario!(@config builder, $key, $value);
            )*
            builder
                $(.push($step))*
                .build()
                .run()
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
        }
    }};

    // With config options, stream mode
    (
        @stream
        $($key:ident : $value:expr),+ ;
        $($step:expr),* $(,)?
    ) => {{
        async {
            let builder = $crate::tests::utils::Scenario::builder();
            $(
                let builder = scenario!(@config builder, $key, $value);
            )*
            builder
                $(.push($step))*
                .build()
                .run()
                .await
                .unwrap()
                .stream()
        }
    }};

    // Internal: apply configuration
    (@config $builder:expr, store, $value:expr) => {
        $builder.with_store($value)
    };
    (@config $builder:expr, retention, $value:expr) => {
        $builder.with_retention($value)
    };
}
