use datasets_common::{end_block::EndBlock, hash::Hash, name::Name, namespace::Namespace};

use crate::job_kind::MaterializeRawJobKind;

/// Job descriptor for raw dataset materialization.
///
/// Contains all the fields needed to execute a raw dataset materialization job.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JobDescriptor {
    /// The block height up to which the dataset should be materialized.
    pub end_block: EndBlock,
    /// The maximum number of concurrent writers for this job. Defaults to `1`.
    #[serde(default = "default_max_writers")]
    pub max_writers: u16,
    /// The namespace of the dataset to materialize.
    pub dataset_namespace: Namespace,
    /// The name of the dataset to materialize.
    pub dataset_name: Name,
    /// The hash of the dataset manifest that defines the extraction.
    pub manifest_hash: Hash,
}

fn default_max_writers() -> u16 {
    1
}

impl From<JobDescriptor> for metadata_db::jobs::JobDescriptorRawOwned {
    fn from(desc: JobDescriptor) -> Self {
        // Internal struct used to serialize the descriptor with the required `kind` tag
        #[derive(serde::Serialize)]
        struct Tagged<'a> {
            kind: MaterializeRawJobKind,
            #[serde(flatten)]
            inner: &'a JobDescriptor,
        }

        // SAFETY: `to_raw_value` only fails on non-string map keys which cannot occur
        // with a flat struct of primitive/string fields.
        let raw = serde_json::value::to_raw_value(&Tagged {
            kind: MaterializeRawJobKind,
            inner: &desc,
        })
        .expect("JobDescriptor serialization is infallible");

        // SAFETY: This is safe because the raw value is guaranteed to be valid JSON
        metadata_db::jobs::JobDescriptorRaw::from_owned_unchecked(raw)
    }
}

impl TryFrom<&metadata_db::jobs::JobDescriptorRaw<'_>> for JobDescriptor {
    type Error = InvalidJobDescriptorError;

    fn try_from(raw: &metadata_db::jobs::JobDescriptorRaw<'_>) -> Result<Self, Self::Error> {
        // Internal struct used to deserialize the descriptor with the expected `kind` tag
        #[derive(serde::Deserialize)]
        struct TaggedOwned {
            #[allow(dead_code)]
            kind: MaterializeRawJobKind,
            #[serde(flatten)]
            inner: JobDescriptor,
        }

        let tagged: TaggedOwned =
            serde_json::from_str(raw.as_str()).map_err(InvalidJobDescriptorError)?;
        Ok(tagged.inner)
    }
}

/// Error returned when a [`metadata_db::jobs::JobDescriptorRaw`] cannot be converted into a [`JobDescriptor`].
///
/// This wraps the underlying deserialization error, which may indicate either a kind
/// mismatch (wrong `kind` tag) or malformed descriptor fields.
#[derive(Debug, thiserror::Error)]
#[error("invalid job descriptor")]
pub struct InvalidJobDescriptorError(#[source] pub serde_json::Error);
