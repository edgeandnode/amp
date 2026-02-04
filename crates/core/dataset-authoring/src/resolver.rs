//! Dependency resolution for dataset authoring.
//!
//! Resolves dataset dependencies from a registry (admin API) and caches
//! them locally. Handles both version-based and hash-based references,
//! with cycle detection in the dependency graph.
//!
//! # Resolution Process
//!
//! 1. Parse dependency references from `amp.yaml`
//! 2. Check local cache for each dependency
//! 3. Fetch missing dependencies from admin API
//! 4. Compute and verify content hashes
//! 5. Build transitive dependency graph
//! 6. Detect cycles using DFS
//!
//! # Example
//!
//! ```ignore
//! use dataset_authoring::resolver::{Resolver, ResolverConfig};
//! use admin_client::Client;
//!
//! let client = Client::new(base_url);
//! let resolver = Resolver::new(client, ResolverConfig::default());
//!
//! let graph = resolver.resolve_all(&dependencies).await?;
//! ```

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use async_trait::async_trait;
use datasets_common::{
    hash::Hash,
    name::{Name, NameError},
    namespace::{Namespace, NamespaceError},
};
use datasets_derived::deps::{DepAlias, DepReference, HashOrVersion};

use crate::{
    cache::{Cache, CacheError},
    canonical::{CanonicalizeError, hash_canonical},
    dependency_manifest::DependencyManifest,
    lockfile::Lockfile,
};

/// Errors that occur during dependency resolution.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    /// Dependency not found in registry.
    ///
    /// This occurs when a referenced dataset does not exist in the registry
    /// at the specified version or hash.
    #[error("dependency not found: {reference}")]
    NotFound {
        /// The reference that could not be found
        reference: String,
    },

    /// Network or API error during resolution.
    ///
    /// This occurs when communication with the registry fails due to
    /// network issues, authentication problems, or server errors.
    #[error("failed to fetch dependency '{reference}'")]
    FetchError {
        /// The reference being fetched
        reference: String,
        /// The underlying error message
        message: String,
    },

    /// Circular dependency detected in the graph.
    ///
    /// This occurs when the dependency graph contains a cycle, which would
    /// cause infinite resolution or runtime loops.
    #[error("circular dependency detected: {path}")]
    CycleDetected {
        /// A string representation of the cycle path
        path: String,
    },

    /// Hash mismatch between declared and computed hash.
    ///
    /// This occurs when a dependency references a specific hash but the
    /// fetched manifest's computed hash does not match.
    #[error("hash mismatch for '{reference}': expected {expected}, got {actual}")]
    HashMismatch {
        /// The reference being resolved
        reference: String,
        /// The expected hash from the reference
        expected: Hash,
        /// The actual computed hash of the manifest
        actual: Hash,
    },

    /// Failed to parse manifest from registry.
    ///
    /// This occurs when the registry returns invalid JSON or a manifest
    /// that does not conform to the expected schema.
    #[error("failed to parse manifest for '{reference}'")]
    ParseError {
        /// The reference being resolved
        reference: String,
        /// The underlying parse error
        #[source]
        source: serde_json::Error,
    },

    /// Failed to canonicalize manifest for hashing.
    ///
    /// This occurs when the manifest cannot be converted to canonical JSON
    /// for hash computation.
    #[error("failed to canonicalize manifest for '{reference}'")]
    CanonicalizeError {
        /// The reference being resolved
        reference: String,
        /// The underlying canonicalization error
        #[source]
        source: CanonicalizeError,
    },

    /// Cache operation failed.
    ///
    /// This occurs when reading from or writing to the local cache fails.
    #[error("cache error for hash '{hash}'")]
    CacheError {
        /// The hash involved in the cache operation
        hash: Hash,
        /// The underlying cache error
        #[source]
        source: CacheError,
    },

    /// Dependency is missing from cache in offline mode.
    ///
    /// This occurs when offline resolution is requested and the dependency
    /// is not available in the local cache.
    #[error("dependency '{0}' not in cache (offline mode)")]
    OfflineCacheMiss(String),

    /// Version references cannot be resolved in offline mode without a lockfile.
    ///
    /// This occurs when a dependency reference uses a version and offline mode
    /// is enabled, requiring an amp.lock mapping or hash pinning.
    #[error(
        "offline mode requires amp.lock for version reference '{0}'; add amp.lock, pin by hash, or run without --offline"
    )]
    OfflineVersionReference(String),

    /// Lockfile node has an invalid namespace value.
    ///
    /// This occurs when a lockfile node's namespace cannot be parsed into a
    /// valid [`Namespace`], which indicates a corrupted or invalid lockfile.
    #[error("invalid namespace '{namespace}' in lockfile for hash '{hash}'")]
    InvalidLockfileNamespace {
        /// The dependency hash with invalid namespace metadata.
        hash: Hash,
        /// The namespace string from the lockfile.
        namespace: String,
        /// The underlying namespace parsing error.
        #[source]
        source: NamespaceError,
    },

    /// Lockfile node has an invalid name value.
    ///
    /// This occurs when a lockfile node's name cannot be parsed into a
    /// valid [`Name`], which indicates a corrupted or invalid lockfile.
    #[error("invalid name '{name}' in lockfile for hash '{hash}'")]
    InvalidLockfileName {
        /// The dependency hash with invalid name metadata.
        hash: Hash,
        /// The name string from the lockfile.
        name: String,
        /// The underlying name parsing error.
        #[source]
        source: NameError,
    },
}

/// A resolved dependency with its manifest and computed hash.
#[derive(Debug, Clone)]
pub struct ResolvedDependency {
    /// The content hash of the manifest (identity).
    pub hash: Hash,
    /// The resolved manifest.
    pub manifest: DependencyManifest,
    /// The namespace extracted from the dependency reference.
    pub namespace: Namespace,
    /// The name extracted from the dependency reference.
    pub name: Name,
}

/// Node in the dependency graph.
#[derive(Debug, Clone)]
pub struct DependencyNode {
    /// The resolved dependency information.
    pub resolved: ResolvedDependency,
    /// Hashes of direct dependencies (from manifest.dependencies).
    pub deps: Vec<Hash>,
}

/// Complete resolved dependency graph.
///
/// Contains all transitive dependencies keyed by their content hash,
/// plus the mapping from top-level aliases to their resolved hashes.
#[derive(Debug, Clone)]
pub struct DependencyGraph {
    /// Direct dependencies mapped from alias to hash.
    pub direct: BTreeMap<DepAlias, Hash>,
    /// All nodes in the graph (transitive closure) keyed by hash.
    pub nodes: BTreeMap<Hash, DependencyNode>,
}

impl DependencyGraph {
    /// Creates an empty dependency graph.
    pub fn new() -> Self {
        Self {
            direct: BTreeMap::new(),
            nodes: BTreeMap::new(),
        }
    }

    /// Returns the number of nodes in the graph (transitive dependencies).
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Returns true if the graph has no dependencies.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Gets a dependency by its hash.
    pub fn get(&self, hash: &Hash) -> Option<&DependencyNode> {
        self.nodes.get(hash)
    }

    /// Gets a direct dependency by its alias.
    pub fn get_by_alias(&self, alias: &DepAlias) -> Option<&DependencyNode> {
        self.direct.get(alias).and_then(|h| self.nodes.get(h))
    }

    /// Returns an iterator over all dependency hashes in topological order.
    ///
    /// Dependencies are ordered such that each dependency appears after
    /// all of its own dependencies.
    pub fn topological_order(&self) -> Result<Vec<Hash>, ResolveError> {
        let mut ordered = Vec::new();
        let mut visited = BTreeSet::new();
        let mut visiting = BTreeSet::new();

        // Build adjacency list
        let deps_map: BTreeMap<Hash, Vec<Hash>> = self
            .nodes
            .iter()
            .map(|(hash, node)| (hash.clone(), node.deps.clone()))
            .collect();

        for hash in self.nodes.keys() {
            self.dfs_visit(hash, &deps_map, &mut ordered, &mut visited, &mut visiting)?;
        }

        Ok(ordered)
    }

    fn dfs_visit(
        &self,
        node: &Hash,
        deps: &BTreeMap<Hash, Vec<Hash>>,
        ordered: &mut Vec<Hash>,
        visited: &mut BTreeSet<Hash>,
        visiting: &mut BTreeSet<Hash>,
    ) -> Result<(), ResolveError> {
        if visiting.contains(node) {
            return Err(ResolveError::CycleDetected {
                path: node.to_string(),
            });
        }
        if visited.contains(node) {
            return Ok(());
        }

        visiting.insert(node.clone());

        if let Some(node_deps) = deps.get(node) {
            for dep in node_deps {
                self.dfs_visit(dep, deps, ordered, visited, visiting)?;
            }
        }

        visiting.remove(node);
        visited.insert(node.clone());
        ordered.push(node.clone());

        Ok(())
    }
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for fetching manifests from a registry.
///
/// This abstraction allows for different registry implementations
/// (admin API, mock for testing, etc.).
#[async_trait]
pub trait ManifestFetcher: Send + Sync {
    /// Fetches a manifest by version reference.
    ///
    /// The reference format is `namespace/name@version`.
    async fn fetch_by_version(
        &self,
        reference: &DepReference,
    ) -> Result<serde_json::Value, ResolveError>;

    /// Fetches a manifest by content hash.
    async fn fetch_by_hash(&self, hash: &Hash) -> Result<Option<serde_json::Value>, ResolveError>;
}

/// Dependency resolver for dataset authoring.
///
/// Resolves dependencies from a registry and caches them locally.
pub struct Resolver<F> {
    /// The manifest fetcher (registry client).
    fetcher: F,
    /// Local cache for resolved manifests.
    cache: Cache,
    /// Whether network fetches are disabled.
    offline: bool,
}

impl<F: ManifestFetcher> Resolver<F> {
    /// Creates a new resolver with the given fetcher and cache.
    pub fn new(fetcher: F, cache: Cache) -> Self {
        Self {
            fetcher,
            cache,
            offline: false,
        }
    }

    /// Creates a new resolver with the given fetcher and default cache location.
    ///
    /// # Errors
    ///
    /// Returns [`CacheError::NoHomeDir`] if the home directory cannot be determined.
    pub fn with_default_cache(fetcher: F) -> Result<Self, CacheError> {
        let cache = Cache::new()?;
        Ok(Self {
            fetcher,
            cache,
            offline: false,
        })
    }

    /// Sets whether the resolver should operate in offline mode.
    pub fn with_offline(mut self, offline: bool) -> Self {
        self.offline = offline;
        self
    }

    /// Resolves a single dependency reference to its manifest and hash.
    ///
    /// This method handles both version-based and hash-based references:
    /// - Version references are resolved via the registry
    /// - Hash references are checked in cache first, then fetched if needed
    ///
    /// # Errors
    ///
    /// Returns an error if the dependency cannot be found, fetched, or parsed.
    pub async fn resolve_one(
        &self,
        reference: &DepReference,
    ) -> Result<ResolvedDependency, ResolveError> {
        match reference.revision() {
            HashOrVersion::Hash(expected_hash) => {
                let namespace = reference.fqn().namespace().clone();
                let name = reference.fqn().name().clone();
                let reference = reference.to_string();
                self.resolve_hash(expected_hash, namespace, name, reference)
                    .await
            }
            HashOrVersion::Version(_) => {
                if self.offline {
                    return Err(ResolveError::OfflineVersionReference(reference.to_string()));
                }
                // Fetch by version from registry
                let json = self.fetcher.fetch_by_version(reference).await?;

                // Compute hash of fetched manifest
                let hash = hash_canonical(&json).map_err(|e| ResolveError::CanonicalizeError {
                    reference: reference.to_string(),
                    source: e,
                })?;

                // Check cache using computed hash
                if let Some(manifest) =
                    self.cache
                        .get(&hash)
                        .map_err(|e| ResolveError::CacheError {
                            hash: hash.clone(),
                            source: e,
                        })?
                {
                    return Ok(ResolvedDependency {
                        hash,
                        manifest,
                        namespace: reference.fqn().namespace().clone(),
                        name: reference.fqn().name().clone(),
                    });
                }

                // Parse and cache
                let manifest: DependencyManifest =
                    serde_json::from_value(json).map_err(|e| ResolveError::ParseError {
                        reference: reference.to_string(),
                        source: e,
                    })?;

                self.cache
                    .put(&hash, &manifest)
                    .map_err(|e| ResolveError::CacheError {
                        hash: hash.clone(),
                        source: e,
                    })?;

                Ok(ResolvedDependency {
                    hash,
                    manifest,
                    namespace: reference.fqn().namespace().clone(),
                    name: reference.fqn().name().clone(),
                })
            }
        }
    }

    /// Resolves dependencies using hashes from a lockfile.
    ///
    /// This uses the lockfile as the source of truth for dependency hashes and
    /// fetches manifests by hash only (no version resolution).
    ///
    /// # Errors
    ///
    /// Returns an error if any dependency cannot be fetched or parsed.
    pub async fn resolve_all_locked(
        &self,
        lockfile: &Lockfile,
    ) -> Result<DependencyGraph, ResolveError> {
        let mut graph = DependencyGraph::new();
        graph.direct = lockfile.dependencies.clone();

        for (hash, node) in &lockfile.nodes {
            let namespace: Namespace =
                node.namespace
                    .parse()
                    .map_err(|err| ResolveError::InvalidLockfileNamespace {
                        hash: hash.clone(),
                        namespace: node.namespace.clone(),
                        source: err,
                    })?;
            let name: Name =
                node.name
                    .parse()
                    .map_err(|err| ResolveError::InvalidLockfileName {
                        hash: hash.clone(),
                        name: node.name.clone(),
                        source: err,
                    })?;

            let reference = format!("{namespace}/{name}@{hash}");
            let resolved = self.resolve_hash(hash, namespace, name, reference).await?;

            graph.nodes.insert(
                hash.clone(),
                DependencyNode {
                    resolved,
                    deps: node.deps.clone(),
                },
            );
        }

        graph.topological_order()?;

        Ok(graph)
    }

    async fn resolve_hash(
        &self,
        expected_hash: &Hash,
        namespace: Namespace,
        name: Name,
        reference: String,
    ) -> Result<ResolvedDependency, ResolveError> {
        if let Some(manifest) =
            self.cache
                .get(expected_hash)
                .map_err(|err| ResolveError::CacheError {
                    hash: expected_hash.clone(),
                    source: err,
                })?
        {
            return Ok(ResolvedDependency {
                hash: expected_hash.clone(),
                manifest,
                namespace,
                name,
            });
        }

        if self.offline {
            return Err(ResolveError::OfflineCacheMiss(reference));
        }

        let json = self
            .fetcher
            .fetch_by_hash(expected_hash)
            .await?
            .ok_or_else(|| ResolveError::NotFound {
                reference: reference.clone(),
            })?;

        let actual_hash = hash_canonical(&json).map_err(|err| ResolveError::CanonicalizeError {
            reference: reference.clone(),
            source: err,
        })?;

        if &actual_hash != expected_hash {
            return Err(ResolveError::HashMismatch {
                reference,
                expected: expected_hash.clone(),
                actual: actual_hash,
            });
        }

        let manifest: DependencyManifest =
            serde_json::from_value(json).map_err(|err| ResolveError::ParseError {
                reference: reference.clone(),
                source: err,
            })?;

        self.cache
            .put(expected_hash, &manifest)
            .map_err(|err| ResolveError::CacheError {
                hash: expected_hash.clone(),
                source: err,
            })?;

        Ok(ResolvedDependency {
            hash: expected_hash.clone(),
            manifest,
            namespace,
            name,
        })
    }

    /// Resolves all dependencies and their transitive dependencies.
    ///
    /// This method:
    /// 1. Resolves each direct dependency from the input map
    /// 2. Recursively resolves transitive dependencies
    /// 3. Detects cycles in the dependency graph
    /// 4. Returns the complete dependency graph
    ///
    /// # Errors
    ///
    /// Returns an error if any dependency cannot be resolved or if a cycle
    /// is detected.
    pub async fn resolve_all(
        &self,
        dependencies: &BTreeMap<DepAlias, DepReference>,
    ) -> Result<DependencyGraph, ResolveError> {
        let mut graph = DependencyGraph::new();
        let mut pending: VecDeque<DepReference> = dependencies.values().cloned().collect();
        let mut seen_hashes: BTreeSet<Hash> = BTreeSet::new();

        // Resolve direct dependencies first to build alias mapping
        for (alias, reference) in dependencies {
            let resolved = self.resolve_one(reference).await?;
            graph.direct.insert(alias.clone(), resolved.hash.clone());
            pending.push_back(reference.clone());
        }

        // BFS to resolve all transitive dependencies
        while let Some(reference) = pending.pop_front() {
            let resolved = self.resolve_one(&reference).await?;

            if seen_hashes.contains(&resolved.hash) {
                continue;
            }
            seen_hashes.insert(resolved.hash.clone());

            // Collect hashes of this node's dependencies
            let dep_hashes: Vec<Hash> = {
                let mut hashes = Vec::new();
                for dep_ref in resolved.manifest.dependencies.values() {
                    // Resolve each transitive dependency to get its hash
                    let dep_resolved = self.resolve_one(dep_ref).await?;
                    hashes.push(dep_resolved.hash.clone());

                    // Queue for further resolution if not seen
                    if !seen_hashes.contains(&dep_resolved.hash) {
                        pending.push_back(dep_ref.clone());
                    }
                }
                hashes
            };

            graph.nodes.insert(
                resolved.hash.clone(),
                DependencyNode {
                    resolved,
                    deps: dep_hashes,
                },
            );
        }

        // Verify no cycles using topological sort
        graph.topological_order()?;

        Ok(graph)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Mutex};

    use datasets_common::dataset_kind_str::DatasetKindStr;

    use super::*;
    use crate::{
        canonical::hash_canonical,
        lockfile::{LOCKFILE_VERSION, Lockfile, NodeInfo, RootInfo},
    };

    /// Mock fetcher for testing that returns predefined manifests.
    struct MockFetcher {
        manifests: Mutex<BTreeMap<String, serde_json::Value>>,
        by_hash: Mutex<BTreeMap<Hash, serde_json::Value>>,
    }

    impl MockFetcher {
        fn new() -> Self {
            Self {
                manifests: Mutex::new(BTreeMap::new()),
                by_hash: Mutex::new(BTreeMap::new()),
            }
        }

        fn add_manifest(&self, reference: &str, manifest: serde_json::Value) {
            self.manifests
                .lock()
                .unwrap()
                .insert(reference.to_string(), manifest);
        }

        #[allow(dead_code)]
        fn add_by_hash(&self, hash: Hash, manifest: serde_json::Value) {
            self.by_hash.lock().unwrap().insert(hash, manifest);
        }
    }

    #[async_trait]
    impl ManifestFetcher for MockFetcher {
        async fn fetch_by_version(
            &self,
            reference: &DepReference,
        ) -> Result<serde_json::Value, ResolveError> {
            self.manifests
                .lock()
                .unwrap()
                .get(&reference.to_string())
                .cloned()
                .ok_or_else(|| ResolveError::NotFound {
                    reference: reference.to_string(),
                })
        }

        async fn fetch_by_hash(
            &self,
            hash: &Hash,
        ) -> Result<Option<serde_json::Value>, ResolveError> {
            Ok(self.by_hash.lock().unwrap().get(hash).cloned())
        }
    }

    fn create_minimal_manifest() -> serde_json::Value {
        serde_json::json!({
            "kind": "manifest",
            "dependencies": {},
            "tables": {},
            "functions": {}
        })
    }

    /// Helper to create a test resolved dependency with default namespace/name.
    fn create_test_resolved_dependency(
        hash: Hash,
        manifest: DependencyManifest,
    ) -> ResolvedDependency {
        ResolvedDependency {
            hash,
            manifest,
            namespace: "test".parse().expect("valid namespace"),
            name: "dataset".parse().expect("valid name"),
        }
    }

    #[allow(dead_code)]
    fn create_manifest_with_deps(deps: BTreeMap<&str, &str>) -> serde_json::Value {
        serde_json::json!({
            "kind": "manifest",
            "dependencies": deps,
            "tables": {},
            "functions": {}
        })
    }

    #[test]
    fn dependency_graph_new_creates_empty_graph() {
        //* When
        let graph = DependencyGraph::new();

        //* Then
        assert!(graph.is_empty());
        assert_eq!(graph.len(), 0);
        assert!(graph.direct.is_empty());
    }

    #[test]
    fn dependency_graph_default_creates_empty_graph() {
        //* When
        let graph = DependencyGraph::default();

        //* Then
        assert!(graph.is_empty());
    }

    #[tokio::test]
    async fn resolve_one_with_version_fetches_and_caches() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let fetcher = MockFetcher::new();

        let manifest = create_minimal_manifest();
        fetcher.add_manifest("test/dataset@1.0.0", manifest.clone());

        let resolver = Resolver::new(fetcher, cache.clone());
        let reference: DepReference = "test/dataset@1.0.0".parse().expect("valid reference");

        //* When
        let result = resolver.resolve_one(&reference).await;

        //* Then
        assert!(result.is_ok(), "resolution should succeed");
        let resolved = result.expect("should have resolved");

        // Verify the manifest is now cached
        assert!(
            cache.contains(&resolved.hash),
            "manifest should be cached after resolution"
        );
    }

    #[tokio::test]
    async fn resolve_one_with_missing_dependency_returns_not_found() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let fetcher = MockFetcher::new();
        // Don't add any manifests

        let resolver = Resolver::new(fetcher, cache);
        let reference: DepReference = "missing/dataset@1.0.0".parse().expect("valid reference");

        //* When
        let result = resolver.resolve_one(&reference).await;

        //* Then
        assert!(result.is_err(), "resolution should fail");
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, ResolveError::NotFound { .. }),
            "should be NotFound error, got: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn resolve_one_accepts_raw_manifest_kind() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let fetcher = MockFetcher::new();

        let manifest = serde_json::json!({
            "kind": "evm-rpc",
            "network": "mainnet",
            "start_block": 0,
            "finalized_blocks_only": false,
            "tables": {
                "blocks": {
                    "schema": { "arrow": { "fields": [] } },
                    "network": "mainnet"
                }
            }
        });
        fetcher.add_manifest("test/raw@1.0.0", manifest);

        let resolver = Resolver::new(fetcher, cache);
        let reference: DepReference = "test/raw@1.0.0".parse().expect("valid reference");

        //* When
        let result = resolver.resolve_one(&reference).await;

        //* Then
        let resolved = result.expect("resolution should succeed");
        assert_eq!(resolved.manifest.kind.as_str(), "evm-rpc");
        assert!(
            resolved
                .manifest
                .tables
                .contains_key(&"blocks".parse().expect("valid table name"))
        );
    }

    #[tokio::test]
    async fn resolve_one_uses_cache_for_hash_reference() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());

        // Pre-populate cache with a manifest
        let manifest = DependencyManifest {
            kind: DatasetKindStr::new("manifest".to_string()),
            dependencies: BTreeMap::new(),
            tables: BTreeMap::new(),
            functions: BTreeMap::new(),
        };

        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        cache.put(&hash, &manifest).expect("cache put should work");

        let fetcher = MockFetcher::new();
        // Don't add anything to fetcher - should use cache

        let resolver = Resolver::new(fetcher, cache);
        let reference: DepReference = format!("test/dataset@{}", hash)
            .parse()
            .expect("valid reference");

        //* When
        let result = resolver.resolve_one(&reference).await;

        //* Then
        assert!(result.is_ok(), "resolution should succeed from cache");
        let resolved = result.expect("should have resolved");
        assert_eq!(resolved.hash, hash);
    }

    #[tokio::test]
    async fn resolve_one_offline_rejects_version_reference() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let fetcher = MockFetcher::new();
        let resolver = Resolver::new(fetcher, cache).with_offline(true);
        let reference: DepReference = "test/dataset@1.0.0".parse().expect("valid reference");

        //* When
        let result = resolver.resolve_one(&reference).await;

        //* Then
        let err = result.expect_err("should reject version refs in offline mode");
        assert!(
            matches!(err, ResolveError::OfflineVersionReference(_)),
            "expected OfflineVersionReference error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn resolve_one_offline_reports_cache_miss_for_hash_reference() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let fetcher = MockFetcher::new();
        let resolver = Resolver::new(fetcher, cache).with_offline(true);
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let reference: DepReference = format!("test/dataset@{}", hash)
            .parse()
            .expect("valid reference");

        //* When
        let result = resolver.resolve_one(&reference).await;

        //* Then
        let err = result.expect_err("should report cache miss in offline mode");
        assert!(
            matches!(err, ResolveError::OfflineCacheMiss(_)),
            "expected OfflineCacheMiss error, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn resolve_all_with_empty_dependencies_returns_empty_graph() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let fetcher = MockFetcher::new();
        let resolver = Resolver::new(fetcher, cache);

        let dependencies: BTreeMap<DepAlias, DepReference> = BTreeMap::new();

        //* When
        let result = resolver.resolve_all(&dependencies).await;

        //* Then
        assert!(result.is_ok(), "resolution should succeed");
        let graph = result.expect("should have graph");
        assert!(graph.is_empty());
        assert!(graph.direct.is_empty());
    }

    #[tokio::test]
    async fn resolve_all_resolves_single_dependency() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let fetcher = MockFetcher::new();

        let manifest = create_minimal_manifest();
        fetcher.add_manifest("test/dataset@1.0.0", manifest);

        let resolver = Resolver::new(fetcher, cache);

        let alias: DepAlias = "test_dep".parse().expect("valid alias");
        let reference: DepReference = "test/dataset@1.0.0".parse().expect("valid reference");
        let mut dependencies = BTreeMap::new();
        dependencies.insert(alias.clone(), reference);

        //* When
        let result = resolver.resolve_all(&dependencies).await;

        //* Then
        assert!(result.is_ok(), "resolution should succeed");
        let graph = result.expect("should have graph");
        assert_eq!(graph.len(), 1);
        assert!(graph.direct.contains_key(&alias));
        assert!(graph.get_by_alias(&alias).is_some());
    }

    #[tokio::test]
    async fn resolve_all_locked_uses_lockfile_hashes() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let cache = Cache::with_root(temp_dir.path().to_path_buf());
        let fetcher = MockFetcher::new();

        let manifest = create_minimal_manifest();
        let hash = hash_canonical(&manifest).expect("should compute hash");
        fetcher.add_by_hash(hash.clone(), manifest);

        let alias: DepAlias = "eth".parse().expect("valid alias");

        let root = RootInfo {
            namespace: "root".parse().expect("valid namespace"),
            name: "dataset".parse().expect("valid name"),
            version: "1.0.0".parse().expect("valid version"),
        };

        let mut dependencies = BTreeMap::new();
        dependencies.insert(alias.clone(), hash.clone());

        let mut nodes = BTreeMap::new();
        nodes.insert(
            hash.clone(),
            NodeInfo {
                namespace: "test".to_string(),
                name: "dataset".to_string(),
                deps: vec![],
            },
        );

        let lockfile = Lockfile {
            version: LOCKFILE_VERSION,
            root,
            dependencies,
            nodes,
        };

        let resolver = Resolver::new(fetcher, cache);

        //* When
        let result = resolver.resolve_all_locked(&lockfile).await;

        //* Then
        assert!(result.is_ok(), "resolution should succeed");
        let graph = result.expect("should have graph");
        assert_eq!(graph.direct.get(&alias), Some(&hash));
        let node = graph.get(&hash).expect("should have node");
        assert_eq!(node.resolved.hash, hash);
        assert_eq!(node.resolved.namespace.as_str(), "test");
        assert_eq!(node.resolved.name.as_str(), "dataset");
    }

    #[test]
    fn topological_order_with_empty_graph_returns_empty() {
        //* Given
        let graph = DependencyGraph::new();

        //* When
        let result = graph.topological_order();

        //* Then
        assert!(result.is_ok());
        let order = result.expect("should succeed");
        assert!(order.is_empty());
    }

    #[test]
    fn topological_order_with_single_node_returns_that_node() {
        //* Given
        let mut graph = DependencyGraph::new();
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");

        let manifest = DependencyManifest {
            kind: DatasetKindStr::new("manifest".to_string()),
            dependencies: BTreeMap::new(),
            tables: BTreeMap::new(),
            functions: BTreeMap::new(),
        };

        graph.nodes.insert(
            hash.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash.clone(), manifest),
                deps: vec![],
            },
        );

        //* When
        let result = graph.topological_order();

        //* Then
        assert!(result.is_ok());
        let order = result.expect("should succeed");
        assert_eq!(order.len(), 1);
        assert_eq!(order[0], hash);
    }

    #[test]
    fn topological_order_with_linear_deps_returns_correct_order() {
        //* Given
        let mut graph = DependencyGraph::new();

        // Create three nodes: A -> B -> C (A depends on B, B depends on C)
        let hash_a: Hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .expect("valid hash");
        let hash_b: Hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .parse()
            .expect("valid hash");
        let hash_c: Hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            .parse()
            .expect("valid hash");

        let manifest = DependencyManifest {
            kind: DatasetKindStr::new("manifest".to_string()),
            dependencies: BTreeMap::new(),
            tables: BTreeMap::new(),
            functions: BTreeMap::new(),
        };

        graph.nodes.insert(
            hash_a.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash_a.clone(), manifest.clone()),
                deps: vec![hash_b.clone()],
            },
        );

        graph.nodes.insert(
            hash_b.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash_b.clone(), manifest.clone()),
                deps: vec![hash_c.clone()],
            },
        );

        graph.nodes.insert(
            hash_c.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash_c.clone(), manifest),
                deps: vec![],
            },
        );

        //* When
        let result = graph.topological_order();

        //* Then
        assert!(result.is_ok());
        let order = result.expect("should succeed");
        assert_eq!(order.len(), 3);

        // C must come before B, B must come before A
        let pos_a = order.iter().position(|h| h == &hash_a).unwrap();
        let pos_b = order.iter().position(|h| h == &hash_b).unwrap();
        let pos_c = order.iter().position(|h| h == &hash_c).unwrap();

        assert!(pos_c < pos_b, "C should come before B");
        assert!(pos_b < pos_a, "B should come before A");
    }

    #[test]
    fn topological_order_with_cycle_returns_error() {
        //* Given
        let mut graph = DependencyGraph::new();

        // Create cycle: A -> B -> A
        let hash_a: Hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .expect("valid hash");
        let hash_b: Hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .parse()
            .expect("valid hash");

        let manifest = DependencyManifest {
            kind: DatasetKindStr::new("manifest".to_string()),
            dependencies: BTreeMap::new(),
            tables: BTreeMap::new(),
            functions: BTreeMap::new(),
        };

        graph.nodes.insert(
            hash_a.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash_a.clone(), manifest.clone()),
                deps: vec![hash_b.clone()],
            },
        );

        graph.nodes.insert(
            hash_b.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash_b.clone(), manifest),
                deps: vec![hash_a.clone()], // Creates cycle
            },
        );

        //* When
        let result = graph.topological_order();

        //* Then
        assert!(result.is_err(), "should detect cycle");
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, ResolveError::CycleDetected { .. }),
            "should be CycleDetected error, got: {:?}",
            err
        );
    }
}
