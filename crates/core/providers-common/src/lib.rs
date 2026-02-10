//! Shared provider configuration types and utilities.
//!
//! This crate provides:
//! - `config::ProviderConfigRaw`: Raw provider configuration as TOML table with accessor methods
//! - `config::ProviderResolvedConfigRaw`: Post-substitution config with redacted Debug for security
//! - `config::TryIntoConfig`: Trait for converting raw configs to typed configs
//! - `provider_name::ProviderName`: A newtype enforcing kebab-case naming for provider identifiers
//! - `kind::ProviderKindStr`: Type-erased provider kind identifier
//! - `sql_name::sanitize_sql_name()`: SQL-safe identifier normalization for schema/catalog names
//! - `envsub`: Environment variable substitution for TOML values
//! - `redacted::Redacted`: A wrapper type that redacts sensitive data in Debug output

pub mod config;
pub mod envsub;
pub mod kind;
pub mod network_id;
pub mod provider_name;
pub mod redacted;
pub mod sql_name;
