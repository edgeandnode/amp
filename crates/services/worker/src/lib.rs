//! Distributed worker service for executing scheduled dump jobs.
//!
//! This crate provides the standalone worker component that executes extraction jobs in distributed
//! Amp deployments. Workers coordinate with the server via a shared metadata database, enabling
//! distributed extraction architectures with resource isolation and horizontal scaling.
//!
//! The worker service handles registration and heartbeat management, listens for job notifications
//! via `PostgreSQL` LISTEN/NOTIFY, executes assigned dump jobs, updates job status and progress in
//! the metadata DB, and gracefully recovers jobs after restarts with periodic state reconciliation.

mod db;
mod error;
mod info;
mod jobs;
mod node_id;
mod worker;

pub use self::{
    error::{
        AbortJobError, BootstrapError, Error, HeartbeatSetupError, HeartbeatTaskError,
        JobCreationError, JobResultError, MainLoopError, NotificationError, NotificationSetupError,
        ReconcileError, RegistrationError, SpawnJobError, StartActionError,
    },
    info::WorkerInfo,
    jobs::{
        Action as JobNotifAction, Descriptor as JobDescriptor, JobId, JobIdFromStrError,
        JobIdI64ConvError, JobIdU64Error, JobStatus, Notification as JobNotification,
    },
    node_id::{InvalidIdError, NodeId},
    worker::Worker,
};
