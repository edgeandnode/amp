pub use amp_worker_core::Ctx;
use amp_worker_core::{jobs::job_id::JobId, node_id::NodeId};
use chrono::{DateTime, Utc};
use metadata_db::jobs::JobDescriptorRawOwned;

mod notif;

use amp_worker_core::jobs::status::JobStatus;

pub use self::notif::{Action as JobAction, Notification as JobNotification};

/// Job data transfer object for the Worker service.
///
/// This DTO decouples the Worker service from the metadata-db `Job` type,
/// providing a stable interface that can evolve independently of the database schema.
#[derive(Clone, Debug)]
pub struct Job {
    /// Unique identifier for the job
    pub id: JobId,
    /// Node ID assigned to execute this job
    pub node_id: NodeId,
    /// Current status of the job
    pub status: JobStatus,
    /// Job descriptor (contains dataset name and other metadata)
    pub desc: JobDescriptorRawOwned,
    /// Job creation timestamp
    pub created_at: DateTime<Utc>,
    /// Job last update timestamp
    pub updated_at: DateTime<Utc>,
}

impl From<metadata_db::jobs::Job> for Job {
    fn from(job_meta: metadata_db::jobs::Job) -> Self {
        Self {
            id: job_meta.id.into(),
            node_id: job_meta.node_id.into(),
            status: job_meta.status.into(),
            desc: job_meta.desc,
            created_at: job_meta.created_at,
            updated_at: job_meta.updated_at,
        }
    }
}

impl From<Job> for metadata_db::jobs::Job {
    fn from(job: Job) -> Self {
        Self {
            id: job.id.into(),
            node_id: job.node_id.into(),
            status: job.status.into(),
            desc: job.desc,
            created_at: job.created_at,
            updated_at: job.updated_at,
        }
    }
}

/// The logical descriptor of a job, as stored in the `descriptor` column of the `jobs`
/// metadata DB table.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum JobDescriptor {
    MaterializeRaw(amp_worker_datasets_raw::job_descriptor::JobDescriptor),
    MaterializeDerived(amp_worker_datasets_derived::job_descriptor::JobDescriptor),
}
