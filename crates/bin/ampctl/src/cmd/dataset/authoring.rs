//! Shared helpers for dataset authoring commands.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use dataset_authoring::{
    lockfile::{Lockfile, LockfileError, RootInfo},
    resolver::{ManifestFetcher, ResolveError},
};
use datasets_common::{hash::Hash, reference::Reference};
use datasets_derived::deps::{DepAlias, DepReference, HashOrVersion};

use crate::args::GlobalArgs;

/// Parse a NAME=VALUE string into a tuple.
pub(super) fn parse_var(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid variable format: expected NAME=VALUE, got '{s}'"))?;
    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}

pub(super) fn dependencies_have_version_refs(
    dependencies: &BTreeMap<DepAlias, DepReference>,
) -> bool {
    dependencies
        .values()
        .any(|reference| matches!(reference.revision(), HashOrVersion::Version(_)))
}

pub(super) fn load_lockfile_for_locked(
    lockfile_path: &Path,
    config: &dataset_authoring::config::AmpYaml,
) -> Result<Lockfile, LockfileLoadError> {
    if !lockfile_path.exists() {
        return Err(LockfileLoadError::LockfileRequired);
    }

    let lockfile = Lockfile::read(lockfile_path).map_err(LockfileLoadError::Lockfile)?;
    let root = RootInfo {
        namespace: config.namespace.clone(),
        name: config.name.clone(),
        version: config.version.clone(),
    };
    lockfile
        .verify_root(&root)
        .map_err(LockfileLoadError::Lockfile)?;
    lockfile
        .verify_dependencies(&config.dependencies)
        .map_err(LockfileLoadError::Lockfile)?;

    Ok(lockfile)
}

pub(super) fn load_lockfile_for_offline(
    lockfile_path: &Path,
) -> Result<Lockfile, LockfileLoadError> {
    if !lockfile_path.exists() {
        return Err(LockfileLoadError::OfflineLockfileRequired(
            lockfile_path.to_path_buf(),
        ));
    }

    Lockfile::read(lockfile_path).map_err(LockfileLoadError::Lockfile)
}

/// Admin API manifest fetcher.
pub(super) struct AdminApiFetcher {
    client: Arc<admin_client::Client>,
}

impl AdminApiFetcher {
    pub(super) fn new(global: &GlobalArgs) -> Result<Self, crate::args::BuildClientError> {
        let client = global.build_client()?;
        Ok(Self {
            client: Arc::new(client),
        })
    }
}

#[async_trait]
impl ManifestFetcher for AdminApiFetcher {
    async fn fetch_by_version(
        &self,
        reference: &DepReference,
    ) -> Result<serde_json::Value, ResolveError> {
        // Convert DepReference to Reference for the datasets client
        let ref_str = reference.to_string();
        let datasets_ref: Reference = ref_str.parse().map_err(|e| ResolveError::FetchError {
            reference: ref_str.clone(),
            message: format!("invalid reference format: {e}"),
        })?;

        // Use the datasets client to fetch manifest by reference
        let result = self
            .client
            .datasets()
            .get_manifest(&datasets_ref)
            .await
            .map_err(|e| ResolveError::FetchError {
                reference: ref_str.clone(),
                message: e.to_string(),
            })?;

        result.ok_or(ResolveError::NotFound { reference: ref_str })
    }

    async fn fetch_by_hash(&self, hash: &Hash) -> Result<Option<serde_json::Value>, ResolveError> {
        self.client
            .manifests()
            .get(hash)
            .await
            .map_err(|e| ResolveError::FetchError {
                reference: hash.to_string(),
                message: e.to_string(),
            })
    }
}

/// Errors for loading/validating amp.lock in authoring commands.
#[derive(Debug, thiserror::Error)]
pub(super) enum LockfileLoadError {
    /// Lockfile required but not found (--locked mode).
    #[error("amp.lock not found (required for --locked mode)")]
    LockfileRequired,

    /// Lockfile required for offline version resolution.
    #[error(
        "offline mode requires amp.lock when version refs are present; add amp.lock, pin dependencies by hash, or run without --offline (expected at {0})"
    )]
    OfflineLockfileRequired(PathBuf),

    /// Lockfile operation failed.
    #[error("lockfile error")]
    Lockfile(#[source] LockfileError),
}
