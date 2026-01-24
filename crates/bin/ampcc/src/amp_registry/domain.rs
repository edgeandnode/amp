//! AMP Registry domain types.
//!
//! These types mirror the AMP Registry API responses. Some fields may not be
//! directly accessed in application code but are required for deserialization.

use datasets_common::{
    hash::Hash, name::Name, namespace::Namespace, reference::Reference, revision::Revision,
};

/// Query parameters for fetching datasets.
#[derive(Debug, Default, serde::Serialize, schemars::JsonSchema)]
pub struct FetchDatasetsParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction: Option<SortDirection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexing_chains: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keywords: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated: Option<LastUpdatedBucket>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    Asc,
    Desc,
}

// Allow prefixed variants - naming follows API's "1 day", "1 week" pattern
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub enum LastUpdatedBucket {
    #[serde(rename = "1 day")]
    OneDay,
    #[serde(rename = "1 week")]
    OneWeek,
    #[serde(rename = "1 month")]
    OneMonth,
    #[serde(rename = "1 year")]
    OneYear,
}

/// Response from GET /api/v1/datasets
#[derive(Debug, Clone, serde::Deserialize, schemars::JsonSchema)]
pub struct FetchDatasetsResponse {
    pub datasets: Vec<DatasetDto>,
    pub has_next_page: bool,
    pub total_count: i64,
}

/// Query parameters for searching datasets
#[derive(Debug, Default)]
pub struct SearchDatasetsParams {
    pub search: String,
    pub limit: Option<i64>,
    pub page: Option<i64>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum DerivedManifest {
    Manifest(amp_datasets_derived::Manifest),
    EvmRpc(amp_datasets_evmrpc::Manifest),
    Firehose(amp_datasets_firehose::dataset::Manifest),
}

#[derive(
    Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
pub enum DatasetVersionStatus {
    /// The dataset version is in a draft, non-deployed, status.
    /// This is the default status
    #[serde(rename = "draft")]
    Draft,
    /// The dataset version has been deployed to the dataset registry
    #[serde(rename = "published")]
    Published,
    /// The dataset version has been deprecated
    #[serde(rename = "deprecated")]
    Deprecated,
    /// The dataset version was archived by the user.
    #[serde(rename = "archived")]
    Archived,
}

impl std::fmt::Display for DatasetVersionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatasetVersionStatus::Draft => write!(f, "draft"),
            DatasetVersionStatus::Published => write!(f, "published"),
            DatasetVersionStatus::Deprecated => write!(f, "deprecated"),
            DatasetVersionStatus::Archived => write!(f, "archived"),
        }
    }
}
#[derive(
    Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
pub enum DatasetVisibility {
    /// The dataset is not publicly visible. It will not showup in queries/MCP.
    /// It is not installable, unless installing user is owner of dataset.
    #[serde(rename = "private")]
    Private,
    /// The dataset is publicly visible.
    #[serde(rename = "public")]
    Public,
}

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
pub enum ManifestKind {
    #[serde(rename = "manifest")]
    Manifest,
    #[serde(rename = "evm-rpc")]
    EvmRpc,
    #[serde(rename = "eth-beacon")]
    EthBeacon,
    #[serde(rename = "firehose")]
    Firehose,
}
impl std::fmt::Display for ManifestKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestKind::Manifest => write!(f, "manifest"),
            ManifestKind::EvmRpc => write!(f, "evm-rpc"),
            ManifestKind::EthBeacon => write!(f, "eth-beacon"),
            ManifestKind::Firehose => write!(f, "firehose"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
pub struct ManifestDto {
    pub manifest_hash: Hash,
    pub kind: ManifestKind,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
pub struct ManifestTagDto {
    pub namespace: Namespace,
    pub name: Name,
    pub version_tag: Revision,
    pub dataset_reference: Reference,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<ManifestDto>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
pub struct DatasetVersionAncestryDto {
    /// Dataset reference in the format: {namespace}/{name}@{version_tag}
    /// Points to the DatasetVersion.dataset_reference
    pub dataset_reference: Reference,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
pub struct DatasetVersionDto {
    pub status: DatasetVersionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changelog: Option<String>,
    pub created_at: String,
    pub version_tag: Revision,
    pub dataset_reference: Reference,

    // Version-pinned dependencies
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ancestors: Option<Vec<DatasetVersionAncestryDto>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub descendants: Option<Vec<DatasetVersionAncestryDto>>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize, schemars::JsonSchema)]
pub struct DatasetDto {
    // Core fields
    pub namespace: Namespace,
    pub name: Name,
    pub created_at: String,
    pub updated_at: String,

    // Discovery fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keywords: Option<Vec<String>>,
    pub indexing_chains: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readme: Option<String>,
    pub visibility: DatasetVisibility,

    // Metadata fields
    pub owner: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,

    // Computed and linked fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_version: Option<DatasetVersionDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_reference: Option<Reference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub versions: Option<Vec<DatasetVersionDto>>,
}
