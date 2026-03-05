//! Idempotency key computation for raw dataset materialization jobs
//!
//! This module computes idempotency keys by combining the raw job kind discriminator
//! with a dataset reference, producing a deterministic hash that prevents duplicate
//! job scheduling for the same dataset version.

use datasets_common::{hash::Hash, hash_reference::HashReference};
use metadata_db::jobs::IdempotencyKey;

use crate::job_kind::JOB_KIND;

/// Compute an idempotency key for a raw materialization job.
///
/// The key is derived by hashing `{job_kind}:{namespace}/{name}@{manifest_hash}`,
/// producing a deterministic 64-character hex string.
pub fn idempotency_key(reference: &HashReference) -> IdempotencyKey<'static> {
    let input = format!("{JOB_KIND}:{reference}");
    let hash: Hash = datasets_common::hash::hash(input);
    // SAFETY: The hash is a validated 64-char hex string produced by our hash function.
    IdempotencyKey::from_owned_unchecked(hash.into_inner())
}
