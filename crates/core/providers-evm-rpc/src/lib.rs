//! EVM RPC provider types and configuration.
//!
//! This crate provides the core types for EVM RPC providers, including the
//! provider kind identifier, configuration structure, and the HTTP provider
//! client with rate limiting. These types are used throughout the system to
//! represent JSON-RPC blockchain data sources.

pub mod config;
pub mod kind;
pub mod provider;
