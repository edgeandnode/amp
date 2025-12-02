//! Distributed worker service for executing scheduled dump jobs.
//!
//! This crate provides the standalone worker component that executes extraction jobs in distributed
//! Amp deployments. Workers coordinate with the server via a shared metadata database, enabling
//! distributed extraction architectures with resource isolation and horizontal scaling.
//!
//! The worker service handles registration and heartbeat management, listens for job notifications
//! via `PostgreSQL` LISTEN/NOTIFY, executes assigned dump jobs, updates job status and progress in
//! the metadata DB, and gracefully recovers jobs after restarts with periodic state reconciliation.

pub mod config;
pub mod info;
pub mod job;
pub mod node_id;
pub mod service;
