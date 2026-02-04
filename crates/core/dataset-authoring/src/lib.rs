//! Dataset authoring support for Amp.
//!
//! This crate provides the authoring workflow for derived datasets, including:
//!
//! - **Configuration**: `amp.yaml` parsing and validation ([`config`])
//! - **Model discovery**: dbt-style model file discovery ([`discovery`])
//! - **Templating**: Jinja2-compatible SQL templating ([`jinja`])
//! - **SQL validation**: SELECT statement validation ([`query`])
//! - **Schema inference**: Arrow schema inference via DataFusion planning ([`schema`])
//! - **Schema files (IPC)**: Arrow schema IPC serialization ([`arrow_ipc`])
//! - **Schema files (JSON)**: Arrow schema JSON serialization ([`arrow_json`])
//! - **File utilities**: File hashing and path normalization ([`files`])
//! - **Manifest generation**: Canonical JSON manifest with content hashing ([`manifest`])
//! - **Packaging**: Deterministic archive creation for deployment ([`package`])
//! - **Caching**: Global cache for resolved dependencies ([`cache`])
//! - **Lockfile**: Dependency lockfile for reproducible builds ([`lockfile`])
//! - **Legacy bridge**: Conversion to legacy inline manifest format ([`bridge`])
//!
//! ## Authoring Workflow
//!
//! 1. Parse `amp.yaml` configuration
//! 2. Discover models from `models/` directory
//! 3. Resolve dependencies from registry
//! 4. Render Jinja SQL templates
//! 5. Validate SELECT statements
//! 6. Infer schemas via DataFusion
//! 7. Write schema files to `tables/<table>.ipc`
//! 8. Generate canonical manifest with file hashes
//! 9. Package for deployment

pub mod arrow_ipc;
pub mod arrow_json;
pub mod bridge;
pub mod cache;
pub mod canonical;
pub mod config;
pub mod dependency_manifest;
pub mod discovery;
pub mod files;
pub mod jinja;
pub mod lockfile;
pub mod manifest;
pub mod package;
pub mod query;
pub mod resolver;
pub mod schema;
pub mod validation;
