//! Dependency lockfile support.
//!
//! Defines the `amp.lock` format for reproducible builds. The lockfile captures
//! the complete resolved dependency graph at a point in time, enabling:
//!
//! - **Reproducible builds**: Same lockfile = same dependency versions
//! - **Offline resolution**: Lockfile contains all information needed for builds
//! - **Change detection**: Diff lockfiles to see dependency changes
//!
//! # Lockfile Format
//!
//! The lockfile is a YAML file with the following structure:
//!
//! ```yaml
//! version: 1
//! root:
//!   namespace: my_namespace
//!   name: my_dataset
//!   version: 1.0.0
//! dependencies:
//!   eth: b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
//!   tokens: a1b2c3d4e5f6...
//! nodes:
//!   b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9:
//!     namespace: graph
//!     name: eth_mainnet
//!     deps:
//!       - cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
//! ```
//!
//! # Usage Modes
//!
//! - **Normal**: Resolve dependencies, update lockfile if changed
//! - **`--locked`**: Fail if lockfile is missing or out of date

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
};

use datasets_common::{hash::Hash, name::Name, namespace::Namespace, version::Version};
use datasets_derived::deps::{DepAlias, DepReference, HashOrVersion};

use crate::resolver::DependencyGraph;

/// Current lockfile format version.
pub const LOCKFILE_VERSION: u32 = 1;

/// Default lockfile filename.
pub const LOCKFILE_NAME: &str = "amp.lock";

/// Root package information in the lockfile.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RootInfo {
    /// Package namespace.
    pub namespace: Namespace,
    /// Package name.
    pub name: Name,
    /// Package version.
    pub version: Version,
}

/// Information about a dependency node in the lockfile.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeInfo {
    /// Dependency namespace.
    pub namespace: String,
    /// Dependency name.
    pub name: String,
    /// Hashes of this node's direct dependencies.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deps: Vec<Hash>,
}

/// The amp.lock lockfile for dependency resolution.
///
/// The lockfile captures the complete resolved dependency graph at a point
/// in time, enabling reproducible builds. It contains:
///
/// - Format version for compatibility checking
/// - Root package information
/// - Direct dependency aliases mapped to their resolved hashes
/// - All transitive dependency nodes keyed by hash
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Lockfile {
    /// Lockfile format version. Always 1 for this implementation.
    pub version: u32,
    /// Information about the root package being built.
    pub root: RootInfo,
    /// Direct dependencies mapped from alias to resolved hash.
    pub dependencies: BTreeMap<DepAlias, Hash>,
    /// All transitive dependency nodes keyed by content hash.
    pub nodes: BTreeMap<Hash, NodeInfo>,
}

impl Lockfile {
    /// Creates a new lockfile from root info and a resolved dependency graph.
    ///
    /// This extracts the necessary information from the dependency graph to
    /// create a lockfile that can reproduce the same resolution.
    pub fn from_graph(root: RootInfo, graph: &DependencyGraph) -> Self {
        let mut nodes = BTreeMap::new();

        for (hash, node) in &graph.nodes {
            // Extract namespace and name from the resolved dependency
            // These values were captured during dependency resolution from the DepReference.
            let namespace = node.resolved.namespace.to_string();
            let name = node.resolved.name.to_string();

            nodes.insert(
                hash.clone(),
                NodeInfo {
                    namespace,
                    name,
                    deps: node.deps.clone(),
                },
            );
        }

        Self {
            version: LOCKFILE_VERSION,
            root,
            dependencies: graph.direct.clone(),
            nodes,
        }
    }

    /// Reads a lockfile from the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    pub fn read(path: &Path) -> Result<Self, LockfileError> {
        let contents =
            std::fs::read_to_string(path).map_err(|err| LockfileError::ReadFile { source: err })?;

        Self::from_yaml(&contents)
    }

    /// Parses a lockfile from a YAML string.
    ///
    /// # Errors
    ///
    /// Returns an error if the YAML is invalid or doesn't match the expected schema.
    pub fn from_yaml(yaml: &str) -> Result<Self, LockfileError> {
        let lockfile: Lockfile =
            serde_yaml::from_str(yaml).map_err(|err| LockfileError::Parse { source: err })?;

        // Validate version
        if lockfile.version != LOCKFILE_VERSION {
            return Err(LockfileError::UnsupportedVersion {
                found: lockfile.version,
                expected: LOCKFILE_VERSION,
            });
        }

        Ok(lockfile)
    }

    /// Serializes the lockfile to a YAML string.
    ///
    /// The output is deterministic: same lockfile contents = same YAML bytes.
    /// This is achieved through BTreeMap's sorted iteration.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_yaml(&self) -> Result<String, LockfileError> {
        serde_yaml::to_string(self).map_err(|err| LockfileError::Serialize { source: err })
    }

    /// Writes the lockfile to the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be written.
    pub fn write(&self, path: &Path) -> Result<(), LockfileError> {
        let yaml = self.to_yaml()?;
        std::fs::write(path, yaml).map_err(|err| LockfileError::WriteFile { source: err })?;
        Ok(())
    }

    /// Checks if this lockfile's root metadata matches the expected root info.
    ///
    /// # Errors
    ///
    /// Returns [`LockfileError::RootMismatch`] if the root metadata does not match.
    pub fn verify_root(&self, root: &RootInfo) -> Result<(), LockfileError> {
        if self.root != *root {
            return Err(LockfileError::RootMismatch {
                lockfile: Box::new(self.root.clone()),
                expected: Box::new(root.clone()),
            });
        }

        Ok(())
    }

    /// Checks if this lockfile is compatible with a resolved dependency graph.
    ///
    /// Returns `Ok(())` if the lockfile matches the graph, or an error describing
    /// the mismatch.
    ///
    /// This is used for `--locked` mode to verify the lockfile is up to date.
    pub fn verify(&self, graph: &DependencyGraph) -> Result<(), LockfileError> {
        self.verify_with_root(None, graph)
    }

    /// Checks if this lockfile is compatible with a resolved dependency graph and root info.
    ///
    /// Returns `Ok(())` if the lockfile matches the graph and root info, or an error describing
    /// the mismatch.
    ///
    /// This is used for `--locked` mode to verify the lockfile is up to date and matches
    /// the current package metadata.
    pub fn verify_with_root(
        &self,
        root: Option<&RootInfo>,
        graph: &DependencyGraph,
    ) -> Result<(), LockfileError> {
        // Check root metadata matches if provided
        if let Some(root) = root {
            self.verify_root(root)?;
        }

        // Check direct dependencies match
        if self.dependencies != graph.direct {
            return Err(LockfileError::DirectDepsMismatch {
                lockfile: self.dependencies.keys().cloned().collect(),
                resolved: graph.direct.keys().cloned().collect(),
            });
        }

        // Check all nodes are present
        for (hash, node) in &graph.nodes {
            if !self.nodes.contains_key(hash) {
                return Err(LockfileError::MissingNode { hash: hash.clone() });
            }

            // Check deps match
            let locked_node = &self.nodes[hash];
            if locked_node.deps != node.deps {
                return Err(LockfileError::NodeDepsMismatch { hash: hash.clone() });
            }
        }

        // Check no extra nodes in lockfile
        for hash in self.nodes.keys() {
            if !graph.nodes.contains_key(hash) {
                return Err(LockfileError::ExtraNode { hash: hash.clone() });
            }
        }

        Ok(())
    }

    /// Checks if this lockfile is compatible with the direct dependencies
    /// declared in `amp.yaml`.
    ///
    /// This validates:
    /// - Direct dependency aliases match between config and lockfile.
    /// - Hash references in config match the lockfile hash.
    /// - Namespace/name in lockfile matches the config reference FQN.
    /// - Lockfile node dependencies reference existing nodes.
    ///
    /// # Errors
    ///
    /// Returns [`LockfileError`] variants describing any mismatch.
    pub fn verify_dependencies(
        &self,
        dependencies: &BTreeMap<DepAlias, DepReference>,
    ) -> Result<(), LockfileError> {
        let lockfile_aliases: BTreeSet<DepAlias> = self.dependencies.keys().cloned().collect();
        let config_aliases: BTreeSet<DepAlias> = dependencies.keys().cloned().collect();

        if lockfile_aliases != config_aliases {
            return Err(LockfileError::DirectDepsMismatch {
                lockfile: lockfile_aliases.into_iter().collect(),
                resolved: config_aliases.into_iter().collect(),
            });
        }

        for (alias, reference) in dependencies {
            let locked_hash =
                self.dependencies
                    .get(alias)
                    .ok_or_else(|| LockfileError::DirectDepsMismatch {
                        lockfile: self.dependencies.keys().cloned().collect(),
                        resolved: dependencies.keys().cloned().collect(),
                    })?;

            if let HashOrVersion::Hash(expected_hash) = reference.revision()
                && expected_hash != locked_hash
            {
                return Err(LockfileError::DirectDepHashMismatch {
                    alias: alias.clone(),
                    lockfile: locked_hash.clone(),
                    config: expected_hash.clone(),
                });
            }

            let Some(node) = self.nodes.get(locked_hash) else {
                return Err(LockfileError::MissingNode {
                    hash: locked_hash.clone(),
                });
            };

            let expected_namespace = reference.fqn().namespace().as_str();
            let expected_name = reference.fqn().name().as_str();

            if node.namespace != expected_namespace || node.name != expected_name {
                return Err(LockfileError::DirectDepFqnMismatch {
                    alias: alias.clone(),
                    lockfile_namespace: node.namespace.clone(),
                    lockfile_name: node.name.clone(),
                    expected_namespace: expected_namespace.to_string(),
                    expected_name: expected_name.to_string(),
                });
            }
        }

        for node in self.nodes.values() {
            for dep in &node.deps {
                if !self.nodes.contains_key(dep) {
                    return Err(LockfileError::MissingNode { hash: dep.clone() });
                }
            }
        }

        let mut reachable: BTreeSet<Hash> = BTreeSet::new();
        let mut pending: Vec<Hash> = self.dependencies.values().cloned().collect();
        while let Some(hash) = pending.pop() {
            if !reachable.insert(hash.clone()) {
                continue;
            }

            let node = self
                .nodes
                .get(&hash)
                .ok_or_else(|| LockfileError::MissingNode { hash: hash.clone() })?;
            for dep in &node.deps {
                pending.push(dep.clone());
            }
        }

        for hash in self.nodes.keys() {
            if !reachable.contains(hash) {
                return Err(LockfileError::ExtraNode { hash: hash.clone() });
            }
        }

        Ok(())
    }

    /// Returns the path to the lockfile in the given directory.
    pub fn path_in(dir: &Path) -> PathBuf {
        dir.join(LOCKFILE_NAME)
    }
}

/// Errors that occur during lockfile operations.
#[derive(Debug, thiserror::Error)]
pub enum LockfileError {
    /// Failed to read lockfile from disk.
    ///
    /// This occurs when the lockfile cannot be read, typically due to
    /// missing file, permission issues, or I/O errors.
    #[error("failed to read lockfile")]
    ReadFile {
        #[source]
        source: io::Error,
    },

    /// Failed to parse lockfile YAML.
    ///
    /// This occurs when the lockfile exists but contains invalid YAML
    /// or doesn't match the expected schema.
    #[error("failed to parse lockfile")]
    Parse {
        #[source]
        source: serde_yaml::Error,
    },

    /// Unsupported lockfile version.
    ///
    /// This occurs when the lockfile was created with a newer format
    /// that this version doesn't support.
    #[error("unsupported lockfile version: found {found}, expected {expected}")]
    UnsupportedVersion {
        /// The version found in the lockfile.
        found: u32,
        /// The version expected by this implementation.
        expected: u32,
    },

    /// Failed to serialize lockfile to YAML.
    ///
    /// This is an internal error that should rarely occur.
    #[error("failed to serialize lockfile")]
    Serialize {
        #[source]
        source: serde_yaml::Error,
    },

    /// Failed to write lockfile to disk.
    ///
    /// This occurs when the lockfile cannot be written, typically due to
    /// permission issues or disk space constraints.
    #[error("failed to write lockfile")]
    WriteFile {
        #[source]
        source: io::Error,
    },

    /// Lockfile is missing (required for --locked mode).
    ///
    /// This occurs in --locked mode when no lockfile exists.
    #[error("lockfile not found (required for --locked mode)")]
    NotFound,

    /// Root package metadata mismatch.
    ///
    /// This occurs in --locked mode when the lockfile's root metadata
    /// (namespace, name, version) doesn't match the current amp.yaml.
    #[error(
        "root mismatch: lockfile has {}/{} v{}, expected {}/{} v{}",
        lockfile.namespace, lockfile.name, lockfile.version,
        expected.namespace, expected.name, expected.version
    )]
    RootMismatch {
        /// Root info in the lockfile.
        lockfile: Box<RootInfo>,
        /// Expected root info from amp.yaml.
        expected: Box<RootInfo>,
    },

    /// Direct dependencies in lockfile don't match resolved dependencies.
    ///
    /// This occurs in --locked mode when the lockfile's direct dependencies
    /// don't match what was resolved from amp.yaml.
    #[error("direct dependencies mismatch: lockfile has {lockfile:?}, resolved has {resolved:?}")]
    DirectDepsMismatch {
        /// Aliases present in the lockfile.
        lockfile: Vec<DepAlias>,
        /// Aliases present in the resolved graph.
        resolved: Vec<DepAlias>,
    },

    /// Direct dependency hash mismatch.
    ///
    /// This occurs in --locked mode when amp.yaml specifies a hash reference
    /// that differs from the hash recorded in amp.lock.
    #[error(
        "direct dependency '{alias}' hash mismatch: lockfile has {lockfile}, config has {config}"
    )]
    DirectDepHashMismatch {
        /// The dependency alias with mismatched hashes.
        alias: DepAlias,
        /// The hash recorded in the lockfile.
        lockfile: Hash,
        /// The hash declared in amp.yaml.
        config: Hash,
    },

    /// Direct dependency namespace/name mismatch.
    ///
    /// This occurs in --locked mode when amp.yaml references a different
    /// dataset than the lockfile records for the alias.
    #[error(
        "direct dependency '{alias}' points to {lockfile_namespace}/{lockfile_name} in lockfile, expected {expected_namespace}/{expected_name}"
    )]
    DirectDepFqnMismatch {
        /// The dependency alias with mismatched target.
        alias: DepAlias,
        /// Namespace recorded in amp.lock for the alias.
        lockfile_namespace: String,
        /// Name recorded in amp.lock for the alias.
        lockfile_name: String,
        /// Namespace declared in amp.yaml.
        expected_namespace: String,
        /// Name declared in amp.yaml.
        expected_name: String,
    },

    /// A resolved dependency is missing from the lockfile.
    ///
    /// This occurs in --locked mode when a dependency was resolved that
    /// doesn't exist in the lockfile.
    #[error("dependency with hash '{hash}' not in lockfile")]
    MissingNode {
        /// The hash of the missing dependency.
        hash: Hash,
    },

    /// A dependency's transitive deps don't match the lockfile.
    ///
    /// This occurs in --locked mode when a dependency's own dependencies
    /// don't match what's recorded in the lockfile.
    #[error("dependency '{hash}' has different deps than lockfile")]
    NodeDepsMismatch {
        /// The hash of the dependency with mismatched deps.
        hash: Hash,
    },

    /// Lockfile contains a dependency not in the resolved graph.
    ///
    /// This occurs in --locked mode when the lockfile contains nodes
    /// that are no longer part of the dependency graph.
    #[error("lockfile contains extra dependency '{hash}' not in resolved graph")]
    ExtraNode {
        /// The hash of the extra dependency.
        hash: Hash,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use datasets_common::dataset_kind_str::DatasetKindStr;

    use super::*;
    use crate::{
        dependency_manifest::DependencyManifest,
        resolver::{DependencyNode, ResolvedDependency},
    };

    fn create_test_manifest() -> DependencyManifest {
        DependencyManifest {
            kind: DatasetKindStr::new("manifest".to_string()),
            dependencies: BTreeMap::new(),
            tables: BTreeMap::new(),
            functions: BTreeMap::new(),
        }
    }

    fn create_root_info() -> RootInfo {
        RootInfo {
            namespace: "test_namespace".parse().expect("valid namespace"),
            name: "test_dataset".parse().expect("valid name"),
            version: "1.0.0".parse().expect("valid version"),
        }
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

    #[test]
    fn lockfile_version_is_one() {
        assert_eq!(LOCKFILE_VERSION, 1);
    }

    #[test]
    fn lockfile_name_is_amp_lock() {
        assert_eq!(LOCKFILE_NAME, "amp.lock");
    }

    #[test]
    fn from_graph_creates_empty_lockfile_for_empty_graph() {
        //* Given
        let root = create_root_info();
        let graph = DependencyGraph::new();

        //* When
        let lockfile = Lockfile::from_graph(root.clone(), &graph);

        //* Then
        assert_eq!(lockfile.version, LOCKFILE_VERSION);
        assert_eq!(lockfile.root, root);
        assert!(lockfile.dependencies.is_empty());
        assert!(lockfile.nodes.is_empty());
    }

    #[test]
    fn from_graph_captures_direct_dependencies() {
        //* Given
        let root = create_root_info();
        let mut graph = DependencyGraph::new();

        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let alias: DepAlias = "eth".parse().expect("valid alias");

        graph.direct.insert(alias.clone(), hash.clone());
        graph.nodes.insert(
            hash.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash.clone(), create_test_manifest()),
                deps: vec![],
            },
        );

        //* When
        let lockfile = Lockfile::from_graph(root, &graph);

        //* Then
        assert_eq!(lockfile.dependencies.len(), 1);
        assert_eq!(lockfile.dependencies.get(&alias), Some(&hash));
        assert_eq!(lockfile.nodes.len(), 1);
        assert!(lockfile.nodes.contains_key(&hash));
    }

    #[test]
    fn from_graph_captures_transitive_dependencies() {
        //* Given
        let root = create_root_info();
        let mut graph = DependencyGraph::new();

        let hash_a: Hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .expect("valid hash");
        let hash_b: Hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .parse()
            .expect("valid hash");
        let alias: DepAlias = "dep_a".parse().expect("valid alias");

        graph.direct.insert(alias.clone(), hash_a.clone());
        graph.nodes.insert(
            hash_a.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash_a.clone(), create_test_manifest()),
                deps: vec![hash_b.clone()],
            },
        );
        graph.nodes.insert(
            hash_b.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash_b.clone(), create_test_manifest()),
                deps: vec![],
            },
        );

        //* When
        let lockfile = Lockfile::from_graph(root, &graph);

        //* Then
        assert_eq!(lockfile.nodes.len(), 2);
        assert!(lockfile.nodes.contains_key(&hash_a));
        assert!(lockfile.nodes.contains_key(&hash_b));

        let node_a = &lockfile.nodes[&hash_a];
        assert_eq!(node_a.deps, vec![hash_b.clone()]);

        let node_b = &lockfile.nodes[&hash_b];
        assert!(node_b.deps.is_empty());
    }

    #[test]
    fn to_yaml_produces_valid_yaml() {
        //* Given
        let root = create_root_info();
        let graph = DependencyGraph::new();
        let lockfile = Lockfile::from_graph(root, &graph);

        //* When
        let yaml = lockfile.to_yaml();

        //* Then
        assert!(yaml.is_ok(), "serialization should succeed");
        let yaml_str = yaml.expect("should have yaml");
        assert!(yaml_str.contains("version: 1"));
        assert!(yaml_str.contains("namespace: test_namespace"));
        assert!(yaml_str.contains("name: test_dataset"));
        assert!(yaml_str.contains("version: 1.0.0"));
    }

    #[test]
    fn yaml_roundtrip_preserves_data() {
        //* Given
        let root = create_root_info();
        let mut graph = DependencyGraph::new();

        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let alias: DepAlias = "eth".parse().expect("valid alias");

        graph.direct.insert(alias, hash.clone());
        graph.nodes.insert(
            hash.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash.clone(), create_test_manifest()),
                deps: vec![],
            },
        );

        let original = Lockfile::from_graph(root, &graph);

        //* When
        let yaml = original.to_yaml().expect("should serialize");
        let parsed = Lockfile::from_yaml(&yaml);

        //* Then
        assert!(parsed.is_ok(), "parsing should succeed");
        let parsed = parsed.expect("should have lockfile");
        assert_eq!(parsed.version, original.version);
        assert_eq!(parsed.root, original.root);
        assert_eq!(parsed.dependencies, original.dependencies);
        assert_eq!(parsed.nodes, original.nodes);
    }

    #[test]
    fn yaml_output_is_deterministic() {
        //* Given
        let root = create_root_info();
        let mut graph = DependencyGraph::new();

        // Add multiple dependencies to test ordering
        let hash_a: Hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .expect("valid hash");
        let hash_b: Hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .parse()
            .expect("valid hash");

        let alias_a: DepAlias = "alpha".parse().expect("valid alias");
        let alias_b: DepAlias = "beta".parse().expect("valid alias");

        graph.direct.insert(alias_a, hash_a.clone());
        graph.direct.insert(alias_b, hash_b.clone());
        graph.nodes.insert(
            hash_a.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash_a.clone(), create_test_manifest()),
                deps: vec![],
            },
        );
        graph.nodes.insert(
            hash_b.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash_b.clone(), create_test_manifest()),
                deps: vec![],
            },
        );

        let lockfile = Lockfile::from_graph(root, &graph);

        //* When
        let yaml1 = lockfile.to_yaml().expect("should serialize");
        let yaml2 = lockfile.to_yaml().expect("should serialize");

        //* Then
        assert_eq!(yaml1, yaml2, "YAML output should be deterministic");
    }

    #[test]
    fn from_yaml_rejects_unsupported_version() {
        //* Given
        let yaml = r#"
version: 999
root:
  namespace: test_namespace
  name: test_dataset
  version: 1.0.0
dependencies: {}
nodes: {}
"#;

        //* When
        let result = Lockfile::from_yaml(yaml);

        //* Then
        assert!(result.is_err(), "should reject unsupported version");
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, LockfileError::UnsupportedVersion { found: 999, .. }),
            "should be UnsupportedVersion error, got: {:?}",
            err
        );
    }

    #[test]
    fn from_yaml_rejects_invalid_yaml() {
        //* Given
        let yaml = "not: valid: yaml: [[[";

        //* When
        let result = Lockfile::from_yaml(yaml);

        //* Then
        assert!(result.is_err(), "should reject invalid YAML");
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, LockfileError::Parse { .. }),
            "should be Parse error"
        );
    }

    #[test]
    fn verify_succeeds_for_matching_graph() {
        //* Given
        let root = create_root_info();
        let mut graph = DependencyGraph::new();

        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let alias: DepAlias = "eth".parse().expect("valid alias");

        graph.direct.insert(alias, hash.clone());
        graph.nodes.insert(
            hash.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash.clone(), create_test_manifest()),
                deps: vec![],
            },
        );

        let lockfile = Lockfile::from_graph(root, &graph);

        //* When
        let result = lockfile.verify(&graph);

        //* Then
        assert!(
            result.is_ok(),
            "verification should succeed for matching graph"
        );
    }

    #[test]
    fn verify_fails_for_missing_direct_dep() {
        //* Given
        let root = create_root_info();
        let graph1 = DependencyGraph::new();
        let lockfile = Lockfile::from_graph(root.clone(), &graph1);

        // Create a new graph with a dependency
        let mut graph2 = DependencyGraph::new();
        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let alias: DepAlias = "eth".parse().expect("valid alias");

        graph2.direct.insert(alias, hash.clone());
        graph2.nodes.insert(
            hash.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash.clone(), create_test_manifest()),
                deps: vec![],
            },
        );

        //* When
        let result = lockfile.verify(&graph2);

        //* Then
        assert!(result.is_err(), "verification should fail");
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, LockfileError::DirectDepsMismatch { .. }),
            "should be DirectDepsMismatch error, got: {:?}",
            err
        );
    }

    #[test]
    fn verify_fails_for_extra_node_in_lockfile() {
        //* Given
        let root = create_root_info();
        let mut graph = DependencyGraph::new();

        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let alias: DepAlias = "eth".parse().expect("valid alias");

        graph.direct.insert(alias, hash.clone());
        graph.nodes.insert(
            hash.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash.clone(), create_test_manifest()),
                deps: vec![],
            },
        );

        let lockfile = Lockfile::from_graph(root, &graph);

        // Remove the node from the graph
        graph.nodes.clear();
        graph.direct.clear();

        //* When
        let result = lockfile.verify(&graph);

        //* Then
        assert!(result.is_err(), "verification should fail");
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, LockfileError::DirectDepsMismatch { .. }),
            "should be DirectDepsMismatch error for direct deps mismatch, got: {:?}",
            err
        );
    }

    #[test]
    fn file_roundtrip_succeeds() {
        //* Given
        let temp_dir = tempfile::tempdir().expect("should create temp dir");
        let lockfile_path = temp_dir.path().join("amp.lock");

        let root = create_root_info();
        let mut graph = DependencyGraph::new();

        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let alias: DepAlias = "eth".parse().expect("valid alias");

        graph.direct.insert(alias, hash.clone());
        graph.nodes.insert(
            hash.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash.clone(), create_test_manifest()),
                deps: vec![],
            },
        );

        let original = Lockfile::from_graph(root, &graph);

        //* When
        original
            .write(&lockfile_path)
            .expect("write should succeed");
        let loaded = Lockfile::read(&lockfile_path);

        //* Then
        assert!(loaded.is_ok(), "read should succeed");
        let loaded = loaded.expect("should have lockfile");
        assert_eq!(loaded, original);
    }

    #[test]
    fn read_nonexistent_file_returns_error() {
        //* Given
        let path = Path::new("/nonexistent/path/amp.lock");

        //* When
        let result = Lockfile::read(path);

        //* Then
        assert!(result.is_err(), "read should fail for nonexistent file");
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, LockfileError::ReadFile { .. }),
            "should be ReadFile error"
        );
    }

    #[test]
    fn path_in_returns_correct_path() {
        //* Given
        let dir = Path::new("/some/project");

        //* When
        let path = Lockfile::path_in(dir);

        //* Then
        assert_eq!(path, PathBuf::from("/some/project/amp.lock"));
    }

    #[test]
    fn lockfile_nodes_have_namespace_name() {
        //* Given
        let root = create_root_info();
        let mut graph = DependencyGraph::new();

        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let alias: DepAlias = "eth".parse().expect("valid alias");

        // Create resolved dependency with specific namespace/name
        let resolved = ResolvedDependency {
            hash: hash.clone(),
            manifest: create_test_manifest(),
            namespace: "graph".parse().expect("valid namespace"),
            name: "eth_mainnet".parse().expect("valid name"),
        };

        graph.direct.insert(alias, hash.clone());
        graph.nodes.insert(
            hash.clone(),
            DependencyNode {
                resolved,
                deps: vec![],
            },
        );

        //* When
        let lockfile = Lockfile::from_graph(root, &graph);

        //* Then
        let node = &lockfile.nodes[&hash];
        assert_eq!(node.namespace, "graph", "namespace should be captured");
        assert_eq!(node.name, "eth_mainnet", "name should be captured");
    }

    #[test]
    fn verify_with_root_succeeds_for_matching_root() {
        //* Given
        let root = create_root_info();
        let graph = DependencyGraph::new();
        let lockfile = Lockfile::from_graph(root.clone(), &graph);

        //* When
        let result = lockfile.verify_with_root(Some(&root), &graph);

        //* Then
        assert!(
            result.is_ok(),
            "verification should succeed for matching root"
        );
    }

    #[test]
    fn verify_with_root_fails_for_mismatched_namespace() {
        //* Given
        let root = create_root_info();
        let graph = DependencyGraph::new();
        let lockfile = Lockfile::from_graph(root, &graph);

        let different_root = RootInfo {
            namespace: "different_namespace".parse().expect("valid namespace"),
            name: "test_dataset".parse().expect("valid name"),
            version: "1.0.0".parse().expect("valid version"),
        };

        //* When
        let result = lockfile.verify_with_root(Some(&different_root), &graph);

        //* Then
        assert!(
            result.is_err(),
            "verification should fail for mismatched namespace"
        );
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, LockfileError::RootMismatch { .. }),
            "should be RootMismatch error, got: {:?}",
            err
        );
    }

    #[test]
    fn verify_with_root_fails_for_mismatched_version() {
        //* Given
        let root = create_root_info();
        let graph = DependencyGraph::new();
        let lockfile = Lockfile::from_graph(root, &graph);

        let different_root = RootInfo {
            namespace: "test_namespace".parse().expect("valid namespace"),
            name: "test_dataset".parse().expect("valid name"),
            version: "2.0.0".parse().expect("valid version"),
        };

        //* When
        let result = lockfile.verify_with_root(Some(&different_root), &graph);

        //* Then
        assert!(
            result.is_err(),
            "verification should fail for mismatched version"
        );
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, LockfileError::RootMismatch { .. }),
            "should be RootMismatch error, got: {:?}",
            err
        );
    }

    #[test]
    fn verify_dependencies_succeeds_for_matching_config() {
        //* Given
        let root = create_root_info();
        let mut graph = DependencyGraph::new();

        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let alias: DepAlias = "eth".parse().expect("valid alias");

        graph.direct.insert(alias.clone(), hash.clone());
        graph.nodes.insert(
            hash.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash.clone(), create_test_manifest()),
                deps: vec![],
            },
        );

        let lockfile = Lockfile::from_graph(root, &graph);

        let reference: DepReference = "test/dataset@1.0.0".parse().expect("valid reference");
        let mut dependencies = BTreeMap::new();
        dependencies.insert(alias, reference);

        //* When
        let result = lockfile.verify_dependencies(&dependencies);

        //* Then
        assert!(result.is_ok(), "verification should succeed");
    }

    #[test]
    fn verify_dependencies_fails_for_hash_mismatch() {
        //* Given
        let root = create_root_info();
        let mut graph = DependencyGraph::new();

        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let alias: DepAlias = "eth".parse().expect("valid alias");

        graph.direct.insert(alias.clone(), hash.clone());
        graph.nodes.insert(
            hash.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash.clone(), create_test_manifest()),
                deps: vec![],
            },
        );

        let lockfile = Lockfile::from_graph(root, &graph);

        let other_hash: Hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .expect("valid hash");
        let reference: DepReference = format!("test/dataset@{other_hash}")
            .parse()
            .expect("valid reference");
        let mut dependencies = BTreeMap::new();
        dependencies.insert(alias, reference);

        //* When
        let result = lockfile.verify_dependencies(&dependencies);

        //* Then
        assert!(result.is_err(), "verification should fail");
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, LockfileError::DirectDepHashMismatch { .. }),
            "should be DirectDepHashMismatch error, got: {:?}",
            err
        );
    }

    #[test]
    fn verify_dependencies_fails_for_fqn_mismatch() {
        //* Given
        let root = create_root_info();
        let mut graph = DependencyGraph::new();

        let hash: Hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
            .parse()
            .expect("valid hash");
        let alias: DepAlias = "eth".parse().expect("valid alias");

        graph.direct.insert(alias.clone(), hash.clone());
        graph.nodes.insert(
            hash.clone(),
            DependencyNode {
                resolved: create_test_resolved_dependency(hash.clone(), create_test_manifest()),
                deps: vec![],
            },
        );

        let lockfile = Lockfile::from_graph(root, &graph);

        let reference: DepReference = "other/dataset@1.0.0".parse().expect("valid reference");
        let mut dependencies = BTreeMap::new();
        dependencies.insert(alias, reference);

        //* When
        let result = lockfile.verify_dependencies(&dependencies);

        //* Then
        assert!(result.is_err(), "verification should fail");
        let err = result.expect_err("should have error");
        assert!(
            matches!(err, LockfileError::DirectDepFqnMismatch { .. }),
            "should be DirectDepFqnMismatch error, got: {:?}",
            err
        );
    }
}
