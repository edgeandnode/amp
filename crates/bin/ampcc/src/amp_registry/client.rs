//! AMP Registry API client.

use datasets_common::{name::Name, namespace::Namespace, reference::Reference, revision::Revision};
use reqwest::{Client, StatusCode};
use url::Url;

use super::{
    domain::{DerivedManifest, FetchDatasetsParams, FetchDatasetsResponse, SearchDatasetsParams},
    error::AmpRegistryError,
};

/// Build URL path for listing all datasets from the amp-registry-api.
///
/// GET `/api/v1/datasets`
fn dataset_list() -> &'static str {
    "api/v1/datasets"
}

/// Build URL path for searching datasets from the amp-registry-api.
///
/// GET `/api/v1/datasets/search`
fn dataset_search() -> &'static str {
    "api/v1/datasets/search"
}

/// Build URL path for listing datasets owned by the authenticated user from the amp-registry-api.
///
/// GET `/api/v1/owners/@me/datasets`
fn owned_datasets_list() -> &'static str {
    "api/v1/owners/@me/datasets"
}

/// Build URL path for searching datasets owned by the authenticated user from the amp-registry-api.
///
/// GET `/api/v1/owners/@me/datasets/search`
fn owned_datasets_search() -> &'static str {
    "api/v1/owners/@me/datasets/search"
}

/// Build URL path for fetching the Dataset Manifest from the amp-registry-api.
///
/// GET `/api/v1/datasets/{namespace}/{name}/versions/{revision}/manifest`
fn dataset_revision_manifest(reference: &Reference) -> String {
    format!(
        "api/v1/datasets/{}/{}/versions/{}/manifest",
        reference.namespace(),
        reference.name(),
        reference.revision()
    )
}

pub struct AmpRegistryClient {
    client: Client,
    base_url: Url,
}

impl AmpRegistryClient {
    pub fn new(client: Client, base_url: Url) -> AmpRegistryClient {
        AmpRegistryClient { client, base_url }
    }

    /// Build query params from FetchDatasetsParams, taking ownership to avoid cloning.
    fn build_fetch_query_params(params: FetchDatasetsParams) -> Vec<(&'static str, String)> {
        let mut query_params: Vec<(&str, String)> = Vec::new();

        if let Some(limit) = params.limit {
            query_params.push(("limit", limit.to_string()));
        }
        if let Some(page) = params.page {
            query_params.push(("page", page.to_string()));
        }
        if let Some(sort_by) = params.sort_by {
            query_params.push(("sort_by", sort_by.to_string()));
        }
        if let Some(direction) = params.direction {
            query_params.push(("direction", direction.to_string()));
        }
        if let Some(keywords) = params.keywords {
            for keyword in keywords {
                query_params.push(("keywords", keyword));
            }
        }
        if let Some(chains) = params.indexing_chains {
            for chain in chains {
                query_params.push(("indexing_chains", chain));
            }
        }
        if let Some(last_updated) = params.last_updated {
            query_params.push(("last_updated", last_updated.to_string()));
        }

        query_params
    }

    /// Build search query params from SearchDatasetsParams, taking ownership to avoid cloning.
    fn build_search_query_params(params: SearchDatasetsParams) -> Vec<(&'static str, String)> {
        let mut query_params: Vec<(&str, String)> = vec![("search", params.search)];

        if let Some(limit) = params.limit {
            query_params.push(("limit", limit.to_string()));
        }
        if let Some(page) = params.page {
            query_params.push(("page", page.to_string()));
        }

        query_params
    }

    /// Fetch datasets from the registry with optional query parameters.
    pub async fn fetch_datasets(
        &self,
        params: Option<FetchDatasetsParams>,
    ) -> Result<FetchDatasetsResponse, AmpRegistryError> {
        let url = self.base_url.join(dataset_list()).expect("valid URL");
        let mut request = self.client.get(url);

        if let Some(p) = params {
            let query_params = Self::build_fetch_query_params(p);
            request = request.query(&query_params);
        }

        let response = request
            .send()
            .await
            .map_err(AmpRegistryError::FetchDatasets)?
            .error_for_status()
            .map_err(AmpRegistryError::FetchDatasets)?;
        response
            .json()
            .await
            .map_err(AmpRegistryError::FetchDatasets)
    }

    /// Search datasets using the search endpoint.
    pub async fn search_datasets(
        &self,
        params: SearchDatasetsParams,
    ) -> Result<FetchDatasetsResponse, AmpRegistryError> {
        let search_term = params.search.clone();
        let url = self.base_url.join(dataset_search()).expect("valid URL");
        let query_params = Self::build_search_query_params(params);

        let make_err = |err| AmpRegistryError::SearchDatasets(search_term.clone(), err);

        let response = self
            .client
            .get(url)
            .query(&query_params)
            .send()
            .await
            .map_err(&make_err)?
            .error_for_status()
            .map_err(&make_err)?;
        response.json().await.map_err(make_err)
    }

    /// Fetch owned datasets for the authenticated user.
    ///
    /// Requires authentication via bearer token.
    ///
    /// # Errors
    ///
    /// Returns [`AmpRegistryError::Unauthorized`] if the access token is invalid or expired (401).
    pub async fn fetch_owned_datasets(
        &self,
        params: Option<FetchDatasetsParams>,
        access_token: &str,
    ) -> Result<FetchDatasetsResponse, AmpRegistryError> {
        let url = self
            .base_url
            .join(owned_datasets_list())
            .expect("valid URL");
        let mut request = self.client.get(url).bearer_auth(access_token);

        if let Some(p) = params {
            let query_params = Self::build_fetch_query_params(p);
            request = request.query(&query_params);
        }

        let response = request
            .send()
            .await
            .map_err(AmpRegistryError::FetchOwnedDatasets)?;

        if response.status() == StatusCode::UNAUTHORIZED {
            return Err(AmpRegistryError::Unauthorized);
        }

        let response = response
            .error_for_status()
            .map_err(AmpRegistryError::FetchOwnedDatasets)?;
        response
            .json()
            .await
            .map_err(AmpRegistryError::FetchOwnedDatasets)
    }

    /// Search owned datasets for the authenticated user.
    ///
    /// Requires authentication via bearer token.
    ///
    /// # Errors
    ///
    /// Returns [`AmpRegistryError::Unauthorized`] if the access token is invalid or expired (401).
    pub async fn search_owned_datasets(
        &self,
        params: SearchDatasetsParams,
        access_token: &str,
    ) -> Result<FetchDatasetsResponse, AmpRegistryError> {
        let search_term = params.search.clone();
        let url = self
            .base_url
            .join(owned_datasets_search())
            .expect("valid URL");
        let query_params = Self::build_search_query_params(params);

        let make_err = |err| AmpRegistryError::SearchOwnedDatasets(search_term.clone(), err);

        let response = self
            .client
            .get(url)
            .bearer_auth(access_token)
            .query(&query_params)
            .send()
            .await
            .map_err(&make_err)?;

        if response.status() == StatusCode::UNAUTHORIZED {
            return Err(AmpRegistryError::Unauthorized);
        }

        let response = response.error_for_status().map_err(&make_err)?;
        response.json().await.map_err(make_err)
    }

    /// Fetches the dataset version manifest containing the Dataset schema
    pub async fn fetch_dataset_version_manifest(
        &self,
        namespace: Namespace,
        name: Name,
        revision: Revision,
    ) -> Result<DerivedManifest, AmpRegistryError> {
        let reference = Reference::new(namespace, name, revision);
        let url = self
            .base_url
            .join(&dataset_revision_manifest(&reference))
            .expect("valid URL");

        let make_err = |err| AmpRegistryError::FetchDatasetRevisionManifest(reference.clone(), err);

        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(&make_err)?
            .error_for_status()
            .map_err(&make_err)?;
        response.json().await.map_err(make_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::amp_registry::domain::{DatasetSortBy, LastUpdatedBucket, SortDirection};

    // ==========================================================================
    // Query param building tests
    // ==========================================================================

    #[test]
    fn build_fetch_query_params_with_empty_params_returns_empty_vec() {
        //* Given
        let params = FetchDatasetsParams::default();

        //* When
        let query = AmpRegistryClient::build_fetch_query_params(params);

        //* Then
        assert!(
            query.is_empty(),
            "query params should be empty for default params"
        );
    }

    #[test]
    fn build_fetch_query_params_with_pagination_includes_limit_and_page() {
        //* Given
        let params = FetchDatasetsParams {
            limit: Some(25),
            page: Some(2),
            ..Default::default()
        };

        //* When
        let query = AmpRegistryClient::build_fetch_query_params(params);

        //* Then
        assert!(
            query.contains(&("limit", "25".to_string())),
            "query should contain limit param"
        );
        assert!(
            query.contains(&("page", "2".to_string())),
            "query should contain page param"
        );
    }

    #[test]
    fn build_fetch_query_params_with_sort_includes_sort_by_and_direction() {
        //* Given
        let params = FetchDatasetsParams {
            sort_by: Some(DatasetSortBy::UpdatedAt),
            direction: Some(SortDirection::Desc),
            ..Default::default()
        };

        //* When
        let query = AmpRegistryClient::build_fetch_query_params(params);

        //* Then
        assert!(
            query.contains(&("sort_by", "updated_at".to_string())),
            "query should contain sort_by param"
        );
        assert!(
            query.contains(&("direction", "desc".to_string())),
            "query should contain direction param"
        );
    }

    #[test]
    fn build_fetch_query_params_with_asc_direction_includes_asc_value() {
        //* Given
        let params = FetchDatasetsParams {
            direction: Some(SortDirection::Asc),
            ..Default::default()
        };

        //* When
        let query = AmpRegistryClient::build_fetch_query_params(params);

        //* Then
        assert!(
            query.contains(&("direction", "asc".to_string())),
            "query should contain direction=asc"
        );
    }

    #[test]
    fn build_fetch_query_params_with_keywords_includes_multiple_keyword_entries() {
        //* Given
        let params = FetchDatasetsParams {
            keywords: Some(vec!["defi".to_string(), "nft".to_string()]),
            ..Default::default()
        };

        //* When
        let query = AmpRegistryClient::build_fetch_query_params(params);

        //* Then
        let keyword_entries: Vec<_> = query.iter().filter(|(k, _)| *k == "keywords").collect();
        assert_eq!(keyword_entries.len(), 2, "should have two keyword entries");
        assert!(
            query.contains(&("keywords", "defi".to_string())),
            "query should contain defi keyword"
        );
        assert!(
            query.contains(&("keywords", "nft".to_string())),
            "query should contain nft keyword"
        );
    }

    #[test]
    fn build_fetch_query_params_with_chains_includes_multiple_chain_entries() {
        //* Given
        let params = FetchDatasetsParams {
            indexing_chains: Some(vec!["eip155:1".to_string(), "eip155:137".to_string()]),
            ..Default::default()
        };

        //* When
        let query = AmpRegistryClient::build_fetch_query_params(params);

        //* Then
        let chain_entries: Vec<_> = query
            .iter()
            .filter(|(k, _)| *k == "indexing_chains")
            .collect();
        assert_eq!(chain_entries.len(), 2, "should have two chain entries");
    }

    #[test]
    fn build_fetch_query_params_with_last_updated_bucket_includes_correct_value() {
        //* Given
        let test_cases = [
            (LastUpdatedBucket::OneDay, "1 day"),
            (LastUpdatedBucket::OneWeek, "1 week"),
            (LastUpdatedBucket::OneMonth, "1 month"),
            (LastUpdatedBucket::OneYear, "1 year"),
        ];

        for (bucket, expected) in test_cases {
            //* When
            let params = FetchDatasetsParams {
                last_updated: Some(bucket),
                ..Default::default()
            };
            let query = AmpRegistryClient::build_fetch_query_params(params);

            //* Then
            assert!(
                query.contains(&("last_updated", expected.to_string())),
                "query should contain last_updated={expected}"
            );
        }
    }

    #[test]
    fn build_search_query_params_with_all_params_includes_search_limit_and_page() {
        //* Given
        let params = SearchDatasetsParams {
            search: "ethereum".to_string(),
            limit: Some(10),
            page: Some(1),
        };

        //* When
        let query = AmpRegistryClient::build_search_query_params(params);

        //* Then
        assert!(
            query.contains(&("search", "ethereum".to_string())),
            "query should contain search param"
        );
        assert!(
            query.contains(&("limit", "10".to_string())),
            "query should contain limit param"
        );
        assert!(
            query.contains(&("page", "1".to_string())),
            "query should contain page param"
        );
    }

    #[test]
    fn build_search_query_params_with_only_search_returns_single_entry() {
        //* Given
        let params = SearchDatasetsParams {
            search: "test".to_string(),
            limit: None,
            page: None,
        };

        //* When
        let query = AmpRegistryClient::build_search_query_params(params);

        //* Then
        assert_eq!(query.len(), 1, "query should have exactly one entry");
        assert!(
            query.contains(&("search", "test".to_string())),
            "query should contain search param"
        );
    }

    // ==========================================================================
    // Client construction tests
    // ==========================================================================

    #[test]
    fn new_with_valid_url_stores_base_url() {
        //* Given
        let http_client = Client::new();
        let base_url = Url::parse("https://api.example.com").expect("valid test URL");

        //* When
        let client = AmpRegistryClient::new(http_client, base_url);

        //* Then
        assert_eq!(
            client.base_url.as_str(),
            "https://api.example.com/",
            "base_url should match provided URL"
        );
    }
}
