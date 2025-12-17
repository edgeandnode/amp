//! Temporary PostgreSQL database service
//!
//! This crate provides a service that spawns and manages temporary PostgreSQL instances
//! for testing and development purposes. The database is automatically cleaned up when
//! the service is stopped (unless configured to persist).
//!
//! # Features
//!
//! - Automatic PostgreSQL instance creation and lifecycle management
//! - Async-first design with Tokio integration

pub mod service;
