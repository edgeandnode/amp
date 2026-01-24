//! AMP Registry API client.

use datasets_common::{name::Name, namespace::Namespace, revision::Revision};
use reqwest::Client;

use super::domain::{
    DatasetDto, DerivedManifest, FetchDatasetsParams, FetchDatasetsResponse, LastUpdatedBucket,
    SearchDatasetsParams, SortDirection,
};

pub struct AmpRegistryClient {
    client: Client,
    base_url: String,
}

impl AmpRegistryClient {
    pub fn new(client: Client, base_url: impl Into<String>) -> AmpRegistryClient {
        AmpRegistryClient {
            client,
            base_url: base_url.into(),
        }
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
            query_params.push(("sort_by", sort_by));
        }
        if let Some(direction) = params.direction {
            query_params.push((
                "direction",
                match direction {
                    SortDirection::Asc => "asc".to_string(),
                    SortDirection::Desc => "desc".to_string(),
                },
            ));
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
            query_params.push((
                "last_updated",
                match last_updated {
                    LastUpdatedBucket::OneDay => "1 day".to_string(),
                    LastUpdatedBucket::OneWeek => "1 week".to_string(),
                    LastUpdatedBucket::OneMonth => "1 month".to_string(),
                    LastUpdatedBucket::OneYear => "1 year".to_string(),
                },
            ));
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
    ) -> Result<FetchDatasetsResponse, reqwest::Error> {
        let url = format!("{}/api/v1/datasets", self.base_url);
        let mut request = self.client.get(&url);

        if let Some(p) = params {
            let query_params = Self::build_fetch_query_params(p);
            request = request.query(&query_params);
        }

        let response = request.send().await?.error_for_status()?;
        response.json().await
    }

    /// Search datasets using the search endpoint.
    pub async fn search_datasets(
        &self,
        params: SearchDatasetsParams,
    ) -> Result<FetchDatasetsResponse, reqwest::Error> {
        let url = format!("{}/api/v1/datasets/search", self.base_url);
        let query_params = Self::build_search_query_params(params);

        let request = self.client.get(&url).query(&query_params);
        let response = request.send().await?.error_for_status()?;
        response.json().await
    }

    /// Fetch owned datasets for the authenticated user.
    ///
    /// Requires authentication via bearer token.
    pub async fn fetch_owned_datasets(
        &self,
        params: Option<FetchDatasetsParams>,
        access_token: &str,
    ) -> Result<FetchDatasetsResponse, reqwest::Error> {
        let url = format!("{}/api/v1/owners/@me/datasets", self.base_url);
        let mut request = self.client.get(&url).bearer_auth(access_token);

        if let Some(p) = params {
            let query_params = Self::build_fetch_query_params(p);
            request = request.query(&query_params);
        }

        let response = request.send().await?.error_for_status()?;
        response.json().await
    }

    /// Search owned datasets for the authenticated user.
    ///
    /// Requires authentication via bearer token.
    pub async fn search_owned_datasets(
        &self,
        params: SearchDatasetsParams,
        access_token: &str,
    ) -> Result<FetchDatasetsResponse, reqwest::Error> {
        let url = format!("{}/api/v1/owners/@me/datasets/search", self.base_url);
        let query_params = Self::build_search_query_params(params);

        let request = self
            .client
            .get(&url)
            .bearer_auth(access_token)
            .query(&query_params);
        let response = request.send().await?.error_for_status()?;
        response.json().await
    }

    /// Fetches a Dataset by its fully qualified name
    pub async fn fetch_dataset(
        &self,
        namespace: Namespace,
        name: Name,
    ) -> Result<DatasetDto, reqwest::Error> {
        let url = format!("{}/api/v1/datasets/{}/{}", self.base_url, namespace, name);

        let request = self.client.get(&url);

        let response = request.send().await?.error_for_status()?;
        response.json().await
    }

    /// Fetches the dataset version manifest containing the Dataset schema
    pub async fn fetch_dataset_version_manifest(
        &self,
        namespace: Namespace,
        name: Name,
        revision: Revision,
    ) -> Result<DerivedManifest, reqwest::Error> {
        let url = format!(
            "{}/api/v1/datasets/{}/{}/versions/{}/manifest",
            self.base_url, namespace, name, revision
        );

        let request = self.client.get(&url);

        let response = request.send().await?.error_for_status()?;
        response.json().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==========================================================================
    // Query param building tests
    // ==========================================================================

    #[test]
    fn test_build_fetch_query_params_empty() {
        let params = FetchDatasetsParams::default();
        let query = AmpRegistryClient::build_fetch_query_params(params);
        assert!(query.is_empty());
    }

    #[test]
    fn test_build_fetch_query_params_pagination() {
        let params = FetchDatasetsParams {
            limit: Some(25),
            page: Some(2),
            ..Default::default()
        };
        let query = AmpRegistryClient::build_fetch_query_params(params);

        assert!(query.contains(&("limit", "25".to_string())));
        assert!(query.contains(&("page", "2".to_string())));
    }

    #[test]
    fn test_build_fetch_query_params_sort() {
        let params = FetchDatasetsParams {
            sort_by: Some("updated_at".to_string()),
            direction: Some(SortDirection::Desc),
            ..Default::default()
        };
        let query = AmpRegistryClient::build_fetch_query_params(params);

        assert!(query.contains(&("sort_by", "updated_at".to_string())));
        assert!(query.contains(&("direction", "desc".to_string())));
    }

    #[test]
    fn test_build_fetch_query_params_direction_asc() {
        let params = FetchDatasetsParams {
            direction: Some(SortDirection::Asc),
            ..Default::default()
        };
        let query = AmpRegistryClient::build_fetch_query_params(params);

        assert!(query.contains(&("direction", "asc".to_string())));
    }

    #[test]
    fn test_build_fetch_query_params_keywords() {
        let params = FetchDatasetsParams {
            keywords: Some(vec!["defi".to_string(), "nft".to_string()]),
            ..Default::default()
        };
        let query = AmpRegistryClient::build_fetch_query_params(params);

        // Should have two keyword entries
        let keyword_entries: Vec<_> = query.iter().filter(|(k, _)| *k == "keywords").collect();
        assert_eq!(keyword_entries.len(), 2);
        assert!(query.contains(&("keywords", "defi".to_string())));
        assert!(query.contains(&("keywords", "nft".to_string())));
    }

    #[test]
    fn test_build_fetch_query_params_chains() {
        let params = FetchDatasetsParams {
            indexing_chains: Some(vec!["eip155:1".to_string(), "eip155:137".to_string()]),
            ..Default::default()
        };
        let query = AmpRegistryClient::build_fetch_query_params(params);

        let chain_entries: Vec<_> = query
            .iter()
            .filter(|(k, _)| *k == "indexing_chains")
            .collect();
        assert_eq!(chain_entries.len(), 2);
    }

    #[test]
    fn test_build_fetch_query_params_last_updated_buckets() {
        // Test all bucket variants
        let test_cases = [
            (LastUpdatedBucket::OneDay, "1 day"),
            (LastUpdatedBucket::OneWeek, "1 week"),
            (LastUpdatedBucket::OneMonth, "1 month"),
            (LastUpdatedBucket::OneYear, "1 year"),
        ];

        for (bucket, expected) in test_cases {
            let params = FetchDatasetsParams {
                last_updated: Some(bucket),
                ..Default::default()
            };
            let query = AmpRegistryClient::build_fetch_query_params(params);
            assert!(query.contains(&("last_updated", expected.to_string())));
        }
    }

    #[test]
    fn test_build_search_query_params() {
        let params = SearchDatasetsParams {
            search: "ethereum".to_string(),
            limit: Some(10),
            page: Some(1),
        };
        let query = AmpRegistryClient::build_search_query_params(params);

        assert!(query.contains(&("search", "ethereum".to_string())));
        assert!(query.contains(&("limit", "10".to_string())));
        assert!(query.contains(&("page", "1".to_string())));
    }

    #[test]
    fn test_build_search_query_params_minimal() {
        let params = SearchDatasetsParams {
            search: "test".to_string(),
            limit: None,
            page: None,
        };
        let query = AmpRegistryClient::build_search_query_params(params);

        assert_eq!(query.len(), 1);
        assert!(query.contains(&("search", "test".to_string())));
    }

    // ==========================================================================
    // Client construction tests
    // ==========================================================================

    #[test]
    fn test_client_new() {
        let client = AmpRegistryClient::new(Client::new(), "https://api.example.com");
        assert_eq!(client.base_url, "https://api.example.com");
    }
}
