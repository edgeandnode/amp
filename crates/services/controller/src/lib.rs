//! Amp Controller Service
//!
//! The controller service provides the admin API for managing Amp operations.

mod scheduler;
pub mod service;

// Re-export build_info to avoid code duplication
pub use admin_api::build_info;
