//! Dependency types and utilities for derived datasets.
//!
//! This module provides types for working with dependency aliases in derived dataset manifests
//! and utilities for dependency graph operations.

mod alias;
mod reference;

pub use self::{
    alias::{
        DepAlias, DepAliasError, DepAliasOrSelfRef, DepAliasOrSelfRefError, SELF_REF_KEYWORD,
        validate_dep_alias,
    },
    reference::{DepReference, DepReferenceParseError, HashOrVersion, HashOrVersionParseError},
};
