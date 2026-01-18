use amp_dataset_store::dataset_kind::DatasetKind;
use chrono::{DateTime, Utc};
use datasets_common::{hash::Hash, name::Name, namespace::Namespace};
pub use dump::Ctx;
use dump::EndBlock;
use serde_json::Value as JsonValue;

use crate::node_id::NodeId;

mod id;
mod notif;
mod status;

pub use self::{
    id::{JobId, JobIdFromStrError, JobIdI64ConvError, JobIdU64Error},
    notif::{Action as JobAction, Notification as JobNotification},
    status::JobStatus,
};

/// The logical descriptor of a job, as stored in the `descriptor` column of the `jobs`
/// metadata DB table.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum JobDescriptor {
    Dump {
        end_block: EndBlock,
        #[serde(default = "default_max_writers")]
        max_writers: u16,

        dataset_namespace: Namespace,
        dataset_name: Name,
        manifest_hash: Hash,
        dataset_kind: DatasetKind,
    },
}

fn default_max_writers() -> u16 {
    1
}

impl JobDescriptor {
    /// Get the dataset namespace from the job descriptor
    pub fn dataset_namespace(&self) -> &Namespace {
        match self {
            JobDescriptor::Dump {
                dataset_namespace, ..
            } => dataset_namespace,
        }
    }

    /// Get the dataset name from the job descriptor
    pub fn dataset_name(&self) -> &Name {
        match self {
            JobDescriptor::Dump { dataset_name, .. } => dataset_name,
        }
    }

    /// Get the manifest hash from the job descriptor
    pub fn manifest_hash(&self) -> &Hash {
        match self {
            JobDescriptor::Dump { manifest_hash, .. } => manifest_hash,
        }
    }

    /// Get the dataset kind from the job descriptor
    pub fn dataset_kind(&self) -> DatasetKind {
        match self {
            JobDescriptor::Dump { dataset_kind, .. } => *dataset_kind,
        }
    }
}

/// Job data transfer object for the Worker service.
///
/// This DTO decouples the Worker service from the metadata-db `Job` type,
/// providing a stable interface that can evolve independently of the database schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Job {
    /// Unique identifier for the job
    pub id: JobId,
    /// Node ID assigned to execute this job
    pub node_id: NodeId,
    /// Current status of the job
    pub status: JobStatus,
    /// Job descriptor (contains dataset name and other metadata)
    pub desc: JsonValue,
    /// Job creation timestamp
    pub created_at: DateTime<Utc>,
    /// Job last update timestamp
    pub updated_at: DateTime<Utc>,
}

impl From<metadata_db::Job> for Job {
    fn from(job_meta: metadata_db::Job) -> Self {
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

impl From<Job> for metadata_db::Job {
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
