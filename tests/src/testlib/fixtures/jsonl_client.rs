//! JSONL client fixture for querying Amp's HTTP JSON Lines server.
//!
//! This fixture provides a convenient interface for sending SQL queries to the Amp
//! JSON Lines server and parsing the responses. It handles the HTTP communication and
//! JSON deserialization, making it easy to write tests that verify query results.

use anyhow::{Result, anyhow};
use serde::de::DeserializeOwned;

/// JSONL client fixture for querying the Amp JSON Lines server.
///
/// This fixture wraps an HTTP client and provides convenient methods for
/// sending SQL queries to the JSON Lines server and parsing the responses.
/// It automatically handles the HTTP communication and JSON deserialization.
#[derive(Clone)]
pub struct JsonlClient {
    http_client: reqwest::Client,
    server_url: String,
}

impl JsonlClient {
    /// Create a new JSONL client fixture.
    ///
    /// Takes the base URL of the JSON Lines server (without trailing slash).
    /// The client will automatically append the appropriate path for queries.
    pub fn new(url: impl AsRef<str>) -> Self {
        Self {
            http_client: reqwest::Client::new(),
            server_url: format!("{}/", url.as_ref()),
        }
    }

    /// Get the server URL this client is connected to.
    pub fn server_url(&self) -> &str {
        &self.server_url
    }

    /// Execute a SQL query and return the results as a vector of the specified type.
    ///
    /// The query is sent as a POST request to the JSON Lines server, and the response
    /// is parsed as newline-delimited JSON. Each line in the response is deserialized
    /// into the specified type T.
    pub async fn query<T>(&self, sql: &str) -> Result<Vec<T>>
    where
        T: DeserializeOwned + std::fmt::Debug,
    {
        tracing::debug!("Executing SQL query: {}", sql);

        let response = self
            .http_client
            .post(&self.server_url)
            .body(sql.to_string())
            .send()
            .await
            .map_err(|err| anyhow!("Failed to send HTTP request: {}", err))?;

        let status = response.status();
        if !status.is_success() {
            return Err(anyhow!(
                "HTTP request failed with status {}: {}",
                status,
                response.text().await.unwrap_or_default()
            ));
        }

        let buffer = response
            .text()
            .await
            .map_err(|err| anyhow!("Failed to read response body: {}", err))?;

        let mut rows = Vec::new();
        for (line_num, line) in buffer.lines().enumerate() {
            if line.trim().is_empty() {
                continue; // Skip empty lines
            }

            let row: T = serde_json::from_str(line).map_err(|err| {
                anyhow!(
                    "Failed to parse JSON on line {}: {} (line content: '{}')",
                    line_num + 1,
                    err,
                    line
                )
            })?;
            rows.push(row);
        }

        tracing::debug!("Query returned {} rows", rows.len());
        Ok(rows)
    }
}

impl std::fmt::Debug for JsonlClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonlClient")
            .field("server_url", &self.server_url)
            .finish()
    }
}
