//! Version type alias for backward compatibility.
//!
//! This module re-exports [`VersionTag`] as `Version` to support incremental
//! migration of APIs across the codebase.

pub use crate::version_tag::{VersionTag, VersionTagError};

/// Type alias for [`VersionTag`] to support incremental API migration.
///
/// This alias allows existing code to continue using `Version` while gradually
/// migrating to the more descriptive `VersionTag` name.
pub type Version = VersionTag;

/// Type alias for [`VersionTagError`] to support incremental API migration.
pub type VersionError = VersionTagError;
