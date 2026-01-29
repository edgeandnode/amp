//! AMP Registry domain types.
//!
//! These types mirror the AMP Registry API responses. Some fields may not be
//! directly accessed in application code but are required for deserialization.

use datasets_common::{name::Name, namespace::Namespace, reference::Reference, revision::Revision};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Query parameters for fetching datasets.
#[derive(Debug, Default, JsonSchema)]
pub struct FetchDatasetsParams {
    /// The number of Datasets to return.
    ///
    /// # Default
    /// If not specified, the default limit is 50 Datasets per page
    ///
    /// # Maximum
    /// The limit cannot exceed 1000
    pub limit: Option<i64>,
    /// The page of Datasets to return.
    /// Acts as an offset with the limit to enable pagination.
    ///
    /// # Example
    /// - limit: 10
    /// - page: 2
    ///   returns the second page of 10 Datasets
    ///
    /// # Default
    /// If not specified, the default page is 1.
    ///
    /// # Minimum
    /// The page cannot be less than 1 (0, or a negative)
    pub page: Option<i64>,
    /// The field to sort the Datasets by
    ///
    /// # Default
    /// If not specified, the default sort_by is created_at
    pub sort_by: Option<DatasetSortBy>,
    /// The direction to sort the Datasets at the sort_by field
    ///
    /// # Default
    /// If not specified, the default direction is descending.
    /// Resulting in the default Dataset list sort to be: created_at descending
    pub direction: Option<SortDirection>,
    /// Filters the Datasets list to only return Datasets that are indexing these given chains
    pub indexing_chains: Option<Vec<String>>,
    /// Filters the Datasets list to only return Datasets that have keywords matching these values.
    /// Uses OR for filtering: {keyword_1} OR {keyword_2}.
    pub keywords: Option<Vec<String>>,
    /// Filters the Datasets list to return Datasets that were last updated in this bucket of time.
    pub last_updated: Option<LastUpdatedBucket>,
}

/// Enum of the available Dataset sort by field options
// Allowing dead_code so it is apparent that if the sort_by option is used, what values are available
#[allow(dead_code)]
#[derive(Debug, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DatasetSortBy {
    /// Sort the Datasets by their created_at timestamp; when the Dataset was created.
    CreatedAt,
    /// Sort the Datasets by their updated_at timestamp; when the Dataset was last updated.
    UpdatedAt,
    /// Sort the Datasets by the namespace value
    Namespace,
    /// Sort the Datasets by their name value
    Name,
    /// Sort the Datasets by the Dataset owner, putting all Datasets owned by a user together
    Owner,
}

impl std::fmt::Display for DatasetSortBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatasetSortBy::CreatedAt => write!(f, "created_at"),
            DatasetSortBy::UpdatedAt => write!(f, "updated_at"),
            DatasetSortBy::Namespace => write!(f, "namespace"),
            DatasetSortBy::Name => write!(f, "name"),
            DatasetSortBy::Owner => write!(f, "owner"),
        }
    }
}

/// Enum of the available Dataset sort directions to apply with the sort_by field.
// Allowing dead_code so it is apparent that if the direction option is used, what values are available
#[allow(dead_code)]
#[derive(Debug, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    /// Sort the Datasets by the sort_by field in ascending order
    Asc,
    /// Sort the Datasets by the sort_by field in descending order
    Desc,
}

impl std::fmt::Display for SortDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortDirection::Asc => write!(f, "asc"),
            SortDirection::Desc => write!(f, "desc"),
        }
    }
}

/// Enum of the available buckets of when the Dataset was last updated to filter Datasets by.
///
/// Allow prefixed variants - naming follows API's "1 day", "1 week" pattern
// Allowing dead_code so it is apparent that if the last_updated option is used, what values are available
#[allow(dead_code, clippy::enum_variant_names)]
#[derive(Debug, JsonSchema)]
pub enum LastUpdatedBucket {
    /// Returns Datasets that were last updated in the last 24hr period
    OneDay,
    /// Returns Datasets that were last updated in the last 1-week period
    OneWeek,
    /// Returns Datasets that were last updated in the last 1-month period
    OneMonth,
    /// Returns Datasets that were last updated in the last 1-year period
    OneYear,
}

impl std::fmt::Display for LastUpdatedBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LastUpdatedBucket::OneDay => write!(f, "1 day"),
            LastUpdatedBucket::OneWeek => write!(f, "1 week"),
            LastUpdatedBucket::OneMonth => write!(f, "1 month"),
            LastUpdatedBucket::OneYear => write!(f, "1 year"),
        }
    }
}

/// Response from GET /api/v1/datasets
#[derive(Debug, Deserialize, JsonSchema)]
pub struct FetchDatasetsResponse {
    /// The array of Datasets returned in the page
    pub datasets: Vec<DatasetDto>,
    /// Enables pagination.
    /// If true, there is a next page of Dataset records that can be fetched
    pub has_next_page: bool,
    /// The total count of Datasets available.
    /// This is not the total count in the current page, but rather the total count of matching Datasets.
    pub total_count: i64,
}

/// Query parameters for searching datasets
#[derive(Debug)]
pub struct SearchDatasetsParams {
    /// The search term to apply to the full-text weighted search to filter Datasets by
    pub search: String,
    /// The number of Datasets to return.
    ///
    /// # Default
    /// If not specified, the default limit is 50 Datasets per page
    ///
    /// # Maximum
    /// The limit cannot exceed 1000
    pub limit: Option<i64>,
    /// The page of Datasets to return.
    /// Acts as an offset with the limit to enable pagination.
    ///
    /// # Example
    /// - limit: 10
    /// - page: 2
    ///   returns the second page of 10 Datasets
    ///
    /// # Default
    /// If not specified, the default page is 1.
    ///
    /// # Minimum
    /// The page cannot be less than 1 (0, or a negative)
    pub page: Option<i64>,
}

/// The Dataset Manifest based off the Manifest kind
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum DerivedManifest {
    /// The Dataset is a derived Dataset kind
    Manifest(amp_datasets_derived::Manifest),
    /// The Dataset is an evm-rpc Dataset kind
    EvmRpc(amp_datasets_evmrpc::Manifest),
    /// The Dataset is a firehose Dataset kind
    Firehose(amp_datasets_firehose::dataset::Manifest),
}

/// Status of the DatasetVersion
#[derive(Debug, Eq, PartialEq, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DatasetVersionStatus {
    /// The dataset version is in a draft, non-deployed, status.
    /// This is the default status
    Draft,
    /// The dataset version has been deployed to the dataset registry
    Published,
    /// The dataset version has been deprecated
    Deprecated,
    /// The dataset version was archived by the user.
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

/// The Dataset visibility.
/// Allows users to mark Datasets as private, meaning they will not be returned in the public list.
#[derive(Debug, Eq, PartialEq, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DatasetVisibility {
    /// The dataset is not publicly visible. It will not showup in queries/MCP.
    /// It is not installable, unless installing user is owner of dataset.
    Private,
    /// The dataset is publicly visible.
    Public,
}

/// A minified version of the Dataset Version.
/// If a DatasetVersion is a descendent of, or ancestor to, a DatasetVersion;
/// this is that ancestry Dataset Version.
/// Allows for building Dataset lineage; or a connection between Datasets.
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
pub struct DatasetVersionAncestryDto {
    /// Dataset reference in the format: {namespace}/{name}@{version_tag}
    /// Points to the DatasetVersion.dataset_reference
    pub dataset_reference: Reference,
}

/// Dataset Version info.
/// A Dataset can have 1 -> many Versions as the owner makes changes and redeploys.
/// The Version holds the reference to the Manifest that represents the Dataset at the given revision.
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
pub struct DatasetVersionDto {
    /// The Dataset Version Status.
    pub status: DatasetVersionStatus,
    /// A changelog between the previous version and this version.
    /// Helps users querying this Dataset to see what changes were made and how to adjust their queries.
    pub changelog: Option<String>,
    /// UTC timestamp of when the Dataset Version was created
    pub created_at: String,
    /// The revision:
    /// - in semantic versioning: {major}.{minor}.{patch}
    /// - a hash code (like a github commit hash of the change)
    /// - latest
    /// - dev
    pub version_tag: Revision,
    /// The Dataset Version PURL. A globally unique reference to this Dataset Version
    ///
    /// # Format
    /// {namespace}/{name}@{revision}
    pub dataset_reference: Reference,
    /// A list of ancestors that this Dataset Version descends from.
    /// These are Datasets that this Dataset has marked as a dependency.
    /// Shows Dataset lineage
    pub ancestors: Option<Vec<DatasetVersionAncestryDto>>,
    /// A list of descendants of this Dataset Version.
    /// These are Datasets that have marked this Dataset as a dependency.
    /// Shows Dataset lineage
    pub descendants: Option<Vec<DatasetVersionAncestryDto>>,
}

/// Dataset info.
/// Contains the ownership and metadata of the Dataset with a list of Versions.
#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
pub struct DatasetDto {
    /// Dataset namespace.
    /// This is the logical grouping mechanism of Datasets.
    ///
    /// # Examples
    /// - edgeandnode
    /// - graphprotocol
    pub namespace: Namespace,
    /// Dataset name.
    pub name: Name,
    /// UTC timestamp of when the Dataset record was created.
    pub created_at: String,
    /// UTC timestamp of when the Dataset record was last updated
    pub updated_at: String,
    /// A high-level description of the Dataset, what data it is indexing, its purpose, etc.
    pub description: Option<String>,
    /// A list of keywords, like tags, to attach to the Dataset.
    /// Provides metadata for searching/filtering Datasets for specific uses.
    ///
    /// # Examples
    /// - DeFi
    /// - DePIN
    pub keywords: Option<Vec<String>>,
    /// A list of chains where the Dataset data is indexing from.
    ///
    /// # Examples
    /// - ethereum-mainnet
    /// - base-mainnet
    /// - arbitrum-one
    pub indexing_chains: Vec<String>,
    /// The Dataset source of data.
    /// This is open-ended but is really useful for listing the smart contracts 0x addresses that
    /// this Dataset is indexing data/events from.
    /// Provides search/filtering to find Datasets indexing from specific smart contracts.
    pub source: Option<Vec<String>>,
    /// A README providing in-depth details about the Dataset, with usage, examples, etc.
    pub readme: Option<String>,
    /// The Dataset visibility.
    /// If private, then the Dataset will only show up if the authenticated user is the Dataset owner.
    pub visibility: DatasetVisibility,
    /// The Dataset owner
    pub owner: String,
    /// An optional link to a repository (like github repo) that contains the Datasets manifest.
    pub repository_url: Option<String>,
    /// If there is a license covering the Dataset usage
    pub license: Option<String>,
    /// The latest, non-archived version of the Dataset
    pub latest_version: Option<DatasetVersionDto>,
    /// The latest Dataset Version PURL. A globally unique reference to this latest, non-archived, Dataset Version.
    ///
    /// # Format
    /// {namespace}/{name}@{revision}
    pub dataset_reference: Option<Reference>,
    /// List of Dataset Versions belonging to this Dataset
    pub versions: Option<Vec<DatasetVersionDto>>,
}
