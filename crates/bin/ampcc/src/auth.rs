//! Authentication module for CLI auth state management.
//!
//! This module provides:
//! - [`AuthStorage`] - Credentials stored on disk
//! - [`AuthClient`] - Token refresh functionality
//! - [`PkceDeviceFlowClient`] - PKCE device flow authentication
//! - [`AuthError`] - Error types for auth operations

mod client;
mod domain;
mod error;
mod pkce;

pub use client::AuthClient;
pub use domain::AuthStorage;
pub use error::AuthError;
pub use pkce::PkceDeviceFlowClient;
