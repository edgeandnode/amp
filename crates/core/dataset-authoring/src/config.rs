//! amp.yaml configuration parsing.
//!
//! Defines the [`AmpYaml`] struct for parsing and validating the `amp.yaml` authoring
//! configuration file. This is the entrypoint for dataset authoring.
//!
//! # amp.yaml Structure
//!
//! ```yaml
//! amp: 1.0.0
//! namespace: my_namespace
//! name: my_dataset
//! version: 1.0.0
//! dependencies:
//!   eth: my_namespace/eth_mainnet@1.0.0
//! tables: tables  # optional; defaults to "tables"
//! functions:  # optional
//!   myFunc:
//!     input_types: [Int64, Utf8]
//!     output_type: Utf8
//!     source: functions/my_func.js
//! vars:  # optional
//!   my_var: default_value
//! ```

use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
};

use datasets_common::{name::Name, namespace::Namespace, version::Version};
use datasets_derived::{
    deps::{DepAlias, DepReference},
    func_name::FuncName,
};
use semver::Version as SemverVersion;

const SUPPORTED_AMP_MAJOR: u64 = 1;
const AMP_YAML_FILE: &str = "amp.yaml";
const AMP_YML_FILE: &str = "amp.yml";

/// Authoring API contract version declared in `amp.yaml`.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct AmpVersion(SemverVersion);

impl AmpVersion {
    fn major(&self) -> u64 {
        self.0.major
    }
}

impl std::fmt::Display for AmpVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for AmpVersion {
    type Err = semver::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        SemverVersion::parse(s).map(Self)
    }
}

/// User-defined function definition in the authoring config.
///
/// Specifies the function signature and source file location.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionDef {
    /// Arrow data types for function input parameters.
    pub input_types: Vec<String>,
    /// Arrow data type for function return value.
    pub output_type: String,
    /// Path to the function source file (relative to amp.yaml).
    pub source: PathBuf,
}

/// The amp.yaml configuration for dataset authoring.
///
/// This struct represents the parsed and validated amp.yaml file,
/// which is the entrypoint for the dataset authoring workflow.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AmpYaml {
    /// Authoring API contract version.
    pub amp: AmpVersion,
    /// Dataset namespace.
    pub namespace: Namespace,
    /// Dataset name.
    pub name: Name,
    /// Dataset version (semver).
    pub version: Version,
    /// External dataset dependencies with version requirements.
    ///
    /// Maps dependency aliases to their references (namespace/name@version or hash).
    #[serde(default)]
    pub dependencies: BTreeMap<DepAlias, DepReference>,
    /// Directory containing table SQL files (`**/*.sql`).
    ///
    /// Each `.sql` file in this directory (recursively) defines a table.
    /// The table name is derived from the file stem (must be unique across all files).
    /// Defaults to `tables` when omitted.
    #[serde(default = "default_tables_dir")]
    pub tables: PathBuf,
    /// Optional user-defined function definitions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub functions: Option<BTreeMap<FuncName, FunctionDef>>,
    /// Optional variables with default values.
    ///
    /// These can be overridden via CLI `--var` arguments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vars: Option<BTreeMap<String, String>>,
}

/// Versioned amp.yaml configuration (v1 schema).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AmpYamlV1 {
    /// Dataset namespace.
    pub namespace: Namespace,
    /// Dataset name.
    pub name: Name,
    /// Dataset version (semver).
    pub version: Version,
    /// External dataset dependencies with version requirements.
    ///
    /// Maps dependency aliases to their references (namespace/name@version or hash).
    #[serde(default)]
    pub dependencies: BTreeMap<DepAlias, DepReference>,
    /// Directory containing table SQL files (`**/*.sql`).
    ///
    /// Each `.sql` file in this directory (recursively) defines a table.
    /// The table name is derived from the file stem (must be unique across all files).
    /// Defaults to `tables` when omitted.
    #[serde(default = "default_tables_dir")]
    pub tables: PathBuf,
    /// Optional user-defined function definitions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub functions: Option<BTreeMap<FuncName, FunctionDef>>,
    /// Optional variables with default values.
    ///
    /// These can be overridden via CLI `--var` arguments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vars: Option<BTreeMap<String, String>>,
}

enum AmpYamlVersioned {
    V1(AmpYamlV1),
}

fn default_tables_dir() -> PathBuf {
    PathBuf::from("tables")
}

impl AmpYaml {
    /// Parse an `AmpYaml` configuration from a YAML string.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Parse`] if the YAML is invalid or doesn't match
    /// the expected schema.
    ///
    /// Returns [`ConfigError::MissingAmpVersion`], [`ConfigError::InvalidAmpVersion`],
    /// or [`ConfigError::UnsupportedAmpVersion`] if the authoring contract
    /// version is missing, invalid, or unsupported.
    ///
    /// Returns [`ConfigError::Validation`] if the configuration fails validation
    /// (e.g., no tables defined).
    pub fn from_yaml(yaml: &str) -> Result<Self, ConfigError> {
        let mut raw: serde_yaml::Value =
            serde_yaml::from_str(yaml).map_err(|err| ConfigError::Parse { source: err })?;

        let amp_version = parse_amp_version(&raw)?;

        let versioned = match amp_version.major() {
            SUPPORTED_AMP_MAJOR => {
                remove_amp_field(&mut raw)?;
                let v1: AmpYamlV1 = serde_yaml::from_value(raw)
                    .map_err(|err| ConfigError::Parse { source: err })?;
                AmpYamlVersioned::V1(v1)
            }
            _ => {
                return Err(ConfigError::UnsupportedAmpVersion {
                    version: amp_version.to_string(),
                    supported_major: SUPPORTED_AMP_MAJOR,
                });
            }
        };

        let config = Self::from_versioned(amp_version, versioned)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Validation`] if the configuration is invalid.
    /// Returns [`ConfigError::InvalidPath`] if any path is absolute or uses `..` traversal.
    fn validate(&self) -> Result<(), ConfigError> {
        // Validate tables directory path
        validate_path(&self.tables, "tables directory")?;

        // Validate function source paths
        if let Some(functions) = &self.functions {
            for (func_name, func_def) in functions {
                validate_path(&func_def.source, &format!("function '{func_name}'"))?;
            }
        }

        Ok(())
    }

    fn from_versioned(amp: AmpVersion, versioned: AmpYamlVersioned) -> Result<Self, ConfigError> {
        match (amp.major(), versioned) {
            (SUPPORTED_AMP_MAJOR, AmpYamlVersioned::V1(v1)) => Ok(Self {
                amp,
                namespace: v1.namespace,
                name: v1.name,
                version: v1.version,
                dependencies: v1.dependencies,
                tables: v1.tables,
                functions: v1.functions,
                vars: v1.vars,
            }),
            _ => Err(ConfigError::UnsupportedAmpVersion {
                version: amp.to_string(),
                supported_major: SUPPORTED_AMP_MAJOR,
            }),
        }
    }
}

fn parse_amp_version(raw: &serde_yaml::Value) -> Result<AmpVersion, ConfigError> {
    let mapping = raw
        .as_mapping()
        .ok_or_else(|| ConfigError::InvalidEnvelope {
            message: "amp.yaml must be a mapping".to_string(),
        })?;
    let amp_value = mapping
        .get(serde_yaml::Value::String("amp".to_string()))
        .ok_or(ConfigError::MissingAmpVersion)?;
    let amp_str = amp_value
        .as_str()
        .ok_or_else(|| ConfigError::InvalidAmpVersionType {
            found: yaml_value_type(amp_value),
        })?;
    amp_str
        .parse()
        .map_err(|source| ConfigError::InvalidAmpVersion {
            value: amp_str.to_string(),
            source,
        })
}

fn remove_amp_field(raw: &mut serde_yaml::Value) -> Result<(), ConfigError> {
    let mapping = raw
        .as_mapping_mut()
        .ok_or_else(|| ConfigError::InvalidEnvelope {
            message: "amp.yaml must be a mapping".to_string(),
        })?;
    mapping.remove(serde_yaml::Value::String("amp".to_string()));
    Ok(())
}

fn yaml_value_type(value: &serde_yaml::Value) -> &'static str {
    match value {
        serde_yaml::Value::Null => "null",
        serde_yaml::Value::Bool(_) => "bool",
        serde_yaml::Value::Number(_) => "number",
        serde_yaml::Value::String(_) => "string",
        serde_yaml::Value::Sequence(_) => "sequence",
        serde_yaml::Value::Mapping(_) => "mapping",
        serde_yaml::Value::Tagged(_) => "tagged",
    }
}

/// Errors that occur during amp.yaml configuration parsing and validation.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// YAML parsing failed.
    ///
    /// This occurs when the YAML syntax is invalid or the structure
    /// doesn't match the expected schema.
    #[error("failed to parse amp.yaml")]
    Parse {
        /// The underlying YAML parsing error.
        #[source]
        source: serde_yaml::Error,
    },

    /// Required amp.yaml envelope data is missing.
    #[error("missing required 'amp' version in amp.yaml")]
    MissingAmpVersion,

    /// The amp version value is not a string.
    #[error("invalid 'amp' version: expected semver string, got {found}")]
    InvalidAmpVersionType {
        /// The YAML value type that was provided.
        found: &'static str,
    },

    /// The amp version string is not valid semver.
    #[error("invalid 'amp' version '{value}': {source}")]
    InvalidAmpVersion {
        /// The invalid version string.
        value: String,
        /// The underlying semver parse error.
        #[source]
        source: semver::Error,
    },

    /// The amp version major is not supported.
    #[error("unsupported 'amp' version '{version}'; supported major versions: {supported_major}")]
    UnsupportedAmpVersion {
        /// The provided amp version.
        version: String,
        /// The supported major version.
        supported_major: u64,
    },

    /// The amp.yaml envelope is malformed.
    #[error("invalid amp.yaml: {message}")]
    InvalidEnvelope {
        /// Description of the envelope error.
        message: String,
    },

    /// Configuration validation failed.
    ///
    /// This occurs when the parsed configuration doesn't meet
    /// the required constraints (e.g., no tables defined).
    #[error("invalid amp.yaml configuration: {message}")]
    Validation {
        /// Description of the validation failure.
        message: String,
    },

    /// Invalid path in configuration.
    ///
    /// This occurs when a path in the configuration is absolute,
    /// uses parent directory traversal (`..`), or is otherwise invalid.
    #[error("invalid path '{path}': {reason}")]
    InvalidPath {
        /// The invalid path.
        path: PathBuf,
        /// Reason why the path is invalid.
        reason: String,
    },
}

/// Errors that occur while locating or loading amp.yaml/amp.yml configuration files.
#[derive(Debug, thiserror::Error)]
pub enum ConfigLoadError {
    /// Both amp.yaml and amp.yml were found, creating ambiguity.
    #[error(
        "both amp.yaml and amp.yml found in {}; remove one to continue",
        dir.display()
    )]
    Ambiguous {
        /// Directory containing the configs.
        dir: PathBuf,
        /// Path to amp.yaml.
        yaml: PathBuf,
        /// Path to amp.yml.
        yml: PathBuf,
    },

    /// No amp.yaml or amp.yml was found.
    #[error(
        "no amp.yaml or amp.yml found in {}; expected one configuration file",
        dir.display()
    )]
    Missing {
        /// Directory searched for the config.
        dir: PathBuf,
    },

    /// Failed to read the config file.
    #[error("failed to read config at {}", path.display())]
    Read {
        /// Path to the config file.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Failed to parse the config file.
    #[error("failed to parse config at {}", path.display())]
    Parse {
        /// Path to the config file.
        path: PathBuf,
        /// The underlying config error.
        #[source]
        source: ConfigError,
    },
}

/// Validate that a path is relative and doesn't use parent directory traversal.
///
/// # Arguments
///
/// * `path` - The path to validate.
/// * `context` - Description of where this path is used (for error messages).
///
/// # Errors
///
/// Returns [`ConfigError::InvalidPath`] if:
/// - The path is absolute
/// - The path contains parent directory traversal (`..`)
fn validate_path(path: &Path, context: &str) -> Result<(), ConfigError> {
    // Check for absolute paths
    if path.is_absolute() {
        return Err(ConfigError::InvalidPath {
            path: path.to_path_buf(),
            reason: format!("{context} path must be relative"),
        });
    }

    // Check for Windows drive letters (e.g., C:\path) which may not register as absolute on Unix
    if let Some(s) = path.to_str() {
        let mut chars = s.chars();
        if let (Some(first), Some(second)) = (chars.next(), chars.next())
            && first.is_ascii_alphabetic()
            && second == ':'
        {
            return Err(ConfigError::InvalidPath {
                path: path.to_path_buf(),
                reason: format!("{context} path must be relative"),
            });
        }
    }

    // Check for parent directory traversal
    for component in path.components() {
        if component == Component::ParentDir {
            return Err(ConfigError::InvalidPath {
                path: path.to_path_buf(),
                reason: format!("{context} path must not use parent directory traversal (..)"),
            });
        }
    }

    Ok(())
}

/// Locate the authoring config file (`amp.yaml` or `amp.yml`) in a directory.
///
/// # Errors
///
/// Returns [`ConfigLoadError::Ambiguous`] if both files exist.
/// Returns [`ConfigLoadError::Missing`] if neither file exists.
pub fn find_config_path(dir: &Path) -> Result<PathBuf, ConfigLoadError> {
    let yaml_path = dir.join(AMP_YAML_FILE);
    let yml_path = dir.join(AMP_YML_FILE);
    let yaml_exists = yaml_path.is_file();
    let yml_exists = yml_path.is_file();

    match (yaml_exists, yml_exists) {
        (true, true) => Err(ConfigLoadError::Ambiguous {
            dir: dir.to_path_buf(),
            yaml: yaml_path,
            yml: yml_path,
        }),
        (true, false) => Ok(yaml_path),
        (false, true) => Ok(yml_path),
        (false, false) => Err(ConfigLoadError::Missing {
            dir: dir.to_path_buf(),
        }),
    }
}

/// Load and parse the authoring config from a directory.
///
/// Returns the resolved config path and parsed [`AmpYaml`] configuration.
///
/// # Errors
///
/// Returns [`ConfigLoadError`] if the file cannot be located, read, or parsed.
pub fn load_from_dir(dir: &Path) -> Result<(PathBuf, AmpYaml), ConfigLoadError> {
    let path = find_config_path(dir)?;
    let contents = fs::read_to_string(&path).map_err(|source| ConfigLoadError::Read {
        path: path.clone(),
        source,
    })?;
    let config = AmpYaml::from_yaml(&contents).map_err(|source| ConfigLoadError::Parse {
        path: path.clone(),
        source,
    })?;
    Ok((path, config))
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::*;

    #[test]
    fn parse_minimal_valid_config() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let config = result.expect("should parse valid config");
        assert_eq!(config.amp.to_string(), "1.0.0");
        assert_eq!(config.namespace, "my_namespace");
        assert_eq!(config.name, "my_dataset");
        assert_eq!(config.version.to_string(), "1.0.0");
        assert!(config.dependencies.is_empty());
        assert_eq!(config.tables, PathBuf::from("tables"));
        assert!(config.functions.is_none());
        assert!(config.vars.is_none());
    }

    #[test]
    fn parse_full_config_with_all_fields() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.2.3
dependencies:
  eth: my_namespace/eth_mainnet@1.0.0
  other: other_ns/other_dataset@0.5.0
tables: sql/tables
functions:
  myFunc:
    input_types: [Int64, Utf8]
    output_type: Utf8
    source: functions/my_func.js
vars:
  network: mainnet
  api_key: default_key
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let config = result.expect("should parse valid config");
        assert_eq!(config.amp.to_string(), "1.0.0");
        assert_eq!(config.namespace, "my_namespace");
        assert_eq!(config.name, "my_dataset");
        assert_eq!(config.version.to_string(), "1.2.3");
        assert_eq!(config.dependencies.len(), 2);
        assert_eq!(config.tables, PathBuf::from("sql/tables"));
        assert!(config.functions.is_some());
        let funcs = config.functions.as_ref().expect("should have functions");
        assert_eq!(funcs.len(), 1);
        assert!(config.vars.is_some());
        let vars = config.vars.as_ref().expect("should have vars");
        assert_eq!(vars.len(), 2);
        assert_eq!(vars.get("network"), Some(&"mainnet".to_string()));
    }

    #[test]
    fn reject_config_without_amp_version() {
        //* Given
        let yaml = r#"
namespace: my_namespace
name: my_dataset
version: 1.0.0
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject config without amp version");
        assert!(matches!(err, ConfigError::MissingAmpVersion));
    }

    #[test]
    fn reject_config_with_invalid_amp_version() {
        //* Given
        let yaml = r#"
amp: not-a-version
namespace: my_namespace
name: my_dataset
version: 1.0.0
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject invalid amp version");
        assert!(matches!(err, ConfigError::InvalidAmpVersion { .. }));
    }

    #[test]
    fn reject_config_with_start_block() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
start_block: 12345678
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        assert!(result.is_err(), "start_block is no longer supported");
    }

    #[test]
    fn reject_config_with_non_string_amp_version() {
        //* Given
        let yaml = r#"
amp: 123
namespace: my_namespace
name: my_dataset
version: 1.0.0
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject non-string amp version");
        assert!(matches!(err, ConfigError::InvalidAmpVersionType { .. }));
    }

    #[test]
    fn reject_config_with_unsupported_amp_major() {
        //* Given
        let yaml = r#"
amp: 2.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject unsupported amp major");
        assert!(matches!(err, ConfigError::UnsupportedAmpVersion { .. }));
    }

    #[test]
    fn parse_config_with_hash_dependency() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
dependencies:
  eth: my_namespace/eth_mainnet@b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let config = result.expect("should parse config with hash dependency");
        assert_eq!(config.dependencies.len(), 1);
    }

    #[test]
    fn default_tables_when_missing() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let config = result.expect("should default tables field");
        assert_eq!(config.tables, PathBuf::from("tables"));
    }

    #[test]
    fn reject_config_with_unknown_fields() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
unknown_field: some_value
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject config with unknown fields");
        assert!(matches!(err, ConfigError::Parse { .. }));
    }

    #[test]
    fn reject_config_with_invalid_namespace() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: Invalid-Namespace
name: my_dataset
version: 1.0.0
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject config with invalid namespace");
        assert!(matches!(err, ConfigError::Parse { .. }));
    }

    #[test]
    fn reject_config_with_invalid_name() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: Invalid-Name
version: 1.0.0
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject config with invalid name");
        assert!(matches!(err, ConfigError::Parse { .. }));
    }

    #[test]
    fn reject_config_with_invalid_version() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: not-a-version
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject config with invalid version");
        assert!(matches!(err, ConfigError::Parse { .. }));
    }

    #[test]
    fn reject_config_with_kind_field() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
kind: derived
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject config with kind field");
        let source = match err {
            ConfigError::Parse { source } => source,
            other => panic!("expected parse error, got: {other:?}"),
        };
        let message = source.to_string();
        assert!(
            message.contains("unknown field `kind`"),
            "error should mention unknown 'kind' field: {message}"
        );
    }

    #[test]
    fn reject_config_with_invalid_dep_alias() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
dependencies:
  123invalid: my_namespace/eth_mainnet@1.0.0
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject config with invalid dep alias");
        assert!(matches!(err, ConfigError::Parse { .. }));
    }

    #[test]
    fn reject_config_with_symbolic_dep_reference() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
dependencies:
  eth: my_namespace/eth_mainnet@latest
tables: tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject symbolic dep reference like 'latest'");
        assert!(matches!(err, ConfigError::Parse { .. }));
    }

    #[test]
    fn reject_function_def_with_unknown_fields() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
tables: tables
functions:
  myFunc:
    input_types: [Int64]
    output_type: Utf8
    source: functions/my_func.js
    unknown_field: value
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject function with unknown fields");
        assert!(matches!(err, ConfigError::Parse { .. }));
    }

    #[test]
    fn validate_relative_path_succeeds() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
tables: sql/tables
functions:
  helper:
    input_types: [Int64]
    output_type: Utf8
    source: lib/funcs/helper.js
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        result.expect("should accept relative paths");
    }

    #[test]
    fn validate_absolute_path_fails_for_tables() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
tables: /abs/path/tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject absolute path in tables");
        assert!(
            matches!(err, ConfigError::InvalidPath { .. }),
            "expected InvalidPath error, got: {err:?}"
        );
        let message = err.to_string();
        assert!(
            message.contains("must be relative"),
            "error should mention 'must be relative': {message}"
        );
    }

    #[test]
    fn validate_absolute_path_fails_for_function() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
tables: tables
functions:
  helper:
    input_types: [Int64]
    output_type: Utf8
    source: /abs/path/helper.js
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject absolute path in function");
        assert!(
            matches!(err, ConfigError::InvalidPath { .. }),
            "expected InvalidPath error, got: {err:?}"
        );
        let message = err.to_string();
        assert!(
            message.contains("must be relative"),
            "error should mention 'must be relative': {message}"
        );
    }

    #[test]
    fn validate_windows_drive_path_fails() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
tables: "C:\\path\\tables"
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject Windows drive path");
        assert!(
            matches!(err, ConfigError::InvalidPath { .. }),
            "expected InvalidPath error, got: {err:?}"
        );
    }

    #[test]
    fn validate_parent_traversal_fails_for_tables() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
tables: sql/../tables
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject parent traversal in tables path");
        assert!(
            matches!(err, ConfigError::InvalidPath { .. }),
            "expected InvalidPath error, got: {err:?}"
        );
        let message = err.to_string();
        assert!(
            message.contains(".."),
            "error should mention '..': {message}"
        );
    }

    #[test]
    fn validate_parent_traversal_fails_for_function() {
        //* Given
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
tables: tables
functions:
  helper:
    input_types: [Int64]
    output_type: Utf8
    source: ../outside/helper.js
"#;

        //* When
        let result = AmpYaml::from_yaml(yaml);

        //* Then
        let err = result.expect_err("should reject parent traversal in function path");
        assert!(
            matches!(err, ConfigError::InvalidPath { .. }),
            "expected InvalidPath error, got: {err:?}"
        );
        let message = err.to_string();
        assert!(
            message.contains(".."),
            "error should mention '..': {message}"
        );
    }

    fn write_minimal_config(path: &Path) {
        let yaml = r#"
amp: 1.0.0
namespace: my_namespace
name: my_dataset
version: 1.0.0
"#;
        fs::write(path, yaml).expect("should write config file");
    }

    #[test]
    fn load_from_dir_prefers_amp_yaml_when_present() {
        //* Given
        let temp = tempfile::tempdir().expect("should create temp dir");
        let yaml_path = temp.path().join("amp.yaml");
        write_minimal_config(&yaml_path);

        //* When
        let (path, config) = load_from_dir(temp.path()).expect("should load amp.yaml");

        //* Then
        assert_eq!(path, yaml_path);
        assert_eq!(config.name, "my_dataset");
    }

    #[test]
    fn load_from_dir_supports_amp_yml() {
        //* Given
        let temp = tempfile::tempdir().expect("should create temp dir");
        let yml_path = temp.path().join("amp.yml");
        write_minimal_config(&yml_path);

        //* When
        let (path, config) = load_from_dir(temp.path()).expect("should load amp.yml");

        //* Then
        assert_eq!(path, yml_path);
        assert_eq!(config.namespace, "my_namespace");
    }

    #[test]
    fn load_from_dir_rejects_ambiguous_configs() {
        //* Given
        let temp = tempfile::tempdir().expect("should create temp dir");
        let yaml_path = temp.path().join("amp.yaml");
        let yml_path = temp.path().join("amp.yml");
        write_minimal_config(&yaml_path);
        write_minimal_config(&yml_path);

        //* When
        let err = load_from_dir(temp.path()).expect_err("should reject ambiguity");

        //* Then
        assert!(
            matches!(err, ConfigLoadError::Ambiguous { .. }),
            "expected ambiguous error, got: {err:?}"
        );
    }

    #[test]
    fn load_from_dir_errors_when_missing_config() {
        //* Given
        let temp = tempfile::tempdir().expect("should create temp dir");

        //* When
        let err = load_from_dir(temp.path()).expect_err("should reject missing config");

        //* Then
        assert!(
            matches!(err, ConfigLoadError::Missing { .. }),
            "expected missing error, got: {err:?}"
        );
    }
}
