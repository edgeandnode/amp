//! Canton Scan API client implementing BlockStreamer trait.
//!
//! Connects to a Canton Scan API endpoint and streams ledger updates as AMP Rows.

use std::{collections::HashMap, sync::Arc, time::Duration};

use alloy::primitives::B256;
use async_stream::stream;
use datasets_common::{block_num::BlockNum, block_range::BlockRange};
use datasets_raw::{
    client::{BlockStreamError, BlockStreamer, CleanupError, LatestBlockError},
    rows::Rows,
};
use futures::Stream;
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::{
    ProviderConfig, tables,
    tables::{
        choices_exercised::{ChoiceExercised, ChoiceExercisedRowsBuilder},
        contracts_created::{ContractCreated, ContractCreatedRowsBuilder},
        transactions::{Transaction as TransactionRecord, TransactionRowsBuilder},
    },
};

/// Canton Scan API client for streaming ledger data.
#[derive(Clone)]
pub struct Client {
    http_client: Arc<HttpClient>,
    scan_url: String,
    provider_name: String,
    network: String,
    page_size: u32,
}

impl Client {
    /// Create a new Canton Scan API client from provider configuration.
    pub async fn new(config: ProviderConfig) -> Result<Self, Error> {
        info!("Connecting to Canton Scan API at {}", config.scan_url);

        let http_client = HttpClient::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| Error::Connection(e.to_string()))?;

        Ok(Self {
            http_client: Arc::new(http_client),
            scan_url: config
                .scan_url
                .to_string()
                .trim_end_matches('/')
                .to_string(),
            provider_name: config.name,
            network: config.network.to_string(),
            page_size: config.page_size,
        })
    }

    /// Get the network name.
    pub fn network(&self) -> &str {
        &self.network
    }

    /// Get tables for the network
    fn tables(&self) -> Vec<datasets_common::dataset::Table> {
        tables::all(&self.network.parse().expect("valid network"))
    }

    /// Fetch updates from the Scan API
    async fn fetch_updates(
        &self,
        page_size: u32,
        after: Option<PaginationAfter>,
    ) -> Result<UpdatesResponse, Error> {
        let url = format!("{}/api/scan/v2/updates", self.scan_url);

        let request = UpdatesRequest { page_size, after };

        let response = self
            .http_client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Request(e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(Error::Api(format!("HTTP {status}: {body}")));
        }

        response
            .json()
            .await
            .map_err(|e| Error::Parse(e.to_string()))
    }
}

impl BlockStreamer for Client {
    /// Stream Canton Scan API data as AMP Rows.
    ///
    /// Note: Canton uses offset for ordering. The start/end parameters
    /// are interpreted as offsets for compatibility with AMP's BlockNum.
    async fn block_stream(
        self,
        start_block: BlockNum,
        end_block: BlockNum,
    ) -> impl Stream<Item = Result<Rows, BlockStreamError>> + Send {
        let network = self.network.clone();
        let tables = self.tables();
        let page_size = self.page_size;

        debug!(
            start_offset = start_block,
            end_offset = end_block,
            "Starting Canton Scan API block stream"
        );

        stream! {
            // Find our tables
            let transactions_table = tables.iter()
                .find(|t| t.name().as_str() == "transactions")
                .cloned();
            let contracts_created_table = tables.iter()
                .find(|t| t.name().as_str() == "contracts_created")
                .cloned();
            let choices_exercised_table = tables.iter()
                .find(|t| t.name().as_str() == "choices_exercised")
                .cloned();

            let Some(_transactions_table) = transactions_table else {
                yield Err(Box::new(std::io::Error::other(
                    "Transactions table not found"
                )) as BlockStreamError);
                return;
            };
            let Some(_contracts_created_table) = contracts_created_table else {
                yield Err(Box::new(std::io::Error::other(
                    "Contracts created table not found"
                )) as BlockStreamError);
                return;
            };
            let Some(_choices_exercised_table) = choices_exercised_table else {
                yield Err(Box::new(std::io::Error::other(
                    "Choices exercised table not found"
                )) as BlockStreamError);
                return;
            };

            // Track pagination
            let mut after: Option<PaginationAfter> = None;
            // Track block_num for contracts and choices (they don't have offset in CSV)
            let mut contract_block_num = start_block;
            let mut choice_block_num = start_block;

            loop {
                // Fetch a page of updates
                let response = match self.fetch_updates(page_size, after.clone()).await {
                    Ok(r) => r,
                    Err(e) => {
                        yield Err(Box::new(std::io::Error::other(format!(
                            "Failed to fetch updates: {e}"
                        ))) as BlockStreamError);
                        return;
                    }
                };

                if response.transactions.is_empty() {
                    debug!("No more updates from Scan API");
                    break;
                }

                // Process each transaction/update
                for tx in &response.transactions {
                    let offset = tx.migration_id;

                    // Skip if before our start range
                    if offset < start_block as i64 {
                        continue;
                    }

                    // Stop if past our end range
                    if offset > end_block as i64 {
                        debug!(offset, end_block, "Reached end of requested range");
                        return;
                    }

                    // Create transaction record matching CSV schema
                    let tx_record = TransactionRecord {
                        offset,
                        update_id: tx.update_id.clone(),
                        record_time: tx.record_time.clone(),
                        effective_at: tx.effective_at.clone(),
                        synchronizer_id: tx.synchronizer_id.clone(),
                        event_count: tx.events_by_id.len() as u32,
                    };

                    // Collect events
                    let mut created_records = Vec::new();
                    let mut exercised_records = Vec::new();

                    for (event_id, event) in &tx.events_by_id {
                        if let Some(created) = &event.created {
                            created_records.push(ContractCreated {
                                block_num: contract_block_num,
                                contract_id: created.contract_id.clone(),
                                template_id: created.template_id.clone(),
                                package_name: extract_package_name(&created.template_id),
                                created_at: created.created_at.clone().unwrap_or_default(),
                                signatories: created.signatories.join(";"),
                                observers: if created.observers.is_empty() {
                                    None
                                } else {
                                    Some(created.observers.join(";"))
                                },
                            });
                            contract_block_num += 1;
                        }

                        if let Some(exercised) = &event.exercised {
                            exercised_records.push(ChoiceExercised {
                                block_num: choice_block_num,
                                event_id: event_id.clone(),
                                contract_id: exercised.contract_id.clone(),
                                template_id: exercised.template_id.clone(),
                                choice: exercised.choice.clone(),
                                consuming: exercised.consuming,
                                acting_parties: exercised.acting_parties.join(";"),
                                effective_at: tx.effective_at.clone().unwrap_or_default(),
                            });
                            choice_block_num += 1;
                        }
                    }

                    // Build Arrow arrays for each table
                    let range = BlockRange {
                        numbers: offset as u64..=offset as u64,
                        network: network.parse().expect("valid network"),
                        // Canton doesn't have block hashes, use zero hash
                        hash: B256::ZERO,
                        prev_hash: B256::ZERO,
                    };

                    let mut table_rows_vec = Vec::new();

                    // Transactions table
                    {
                        let mut builder = TransactionRowsBuilder::with_capacity(1);
                        builder.append(&tx_record);

                        match builder.build(range.clone()) {
                            Ok(rows) => table_rows_vec.push(rows),
                            Err(e) => {
                                warn!("Failed to create transactions TableRows: {e}");
                            }
                        }
                    }

                    // Contracts created table (if any)
                    if !created_records.is_empty() {
                        // Need a different range for contracts
                        let contract_range = BlockRange {
                            numbers: (contract_block_num - created_records.len() as u64)..=(contract_block_num - 1),
                            network: network.parse().expect("valid network"),
                            hash: B256::ZERO,
                            prev_hash: B256::ZERO,
                        };

                        let mut builder = ContractCreatedRowsBuilder::with_capacity(created_records.len());
                        for record in &created_records {
                            builder.append(record);
                        }

                        match builder.build(contract_range) {
                            Ok(rows) => table_rows_vec.push(rows),
                            Err(e) => {
                                warn!("Failed to create contracts_created TableRows: {e}");
                            }
                        }
                    }

                    // Choices exercised table (if any)
                    if !exercised_records.is_empty() {
                        // Need a different range for choices
                        let choice_range = BlockRange {
                            numbers: (choice_block_num - exercised_records.len() as u64)..=(choice_block_num - 1),
                            network: network.parse().expect("valid network"),
                            hash: B256::ZERO,
                            prev_hash: B256::ZERO,
                        };

                        let mut builder = ChoiceExercisedRowsBuilder::with_capacity(exercised_records.len());
                        for record in &exercised_records {
                            builder.append(record);
                        }

                        match builder.build(choice_range) {
                            Ok(rows) => table_rows_vec.push(rows),
                            Err(e) => {
                                warn!("Failed to create choices_exercised TableRows: {e}");
                            }
                        }
                    }

                    if !table_rows_vec.is_empty() {
                        yield Ok(Rows::new(table_rows_vec));
                    }
                }

                // Check if we've reached the end
                if let Some(last_tx) = response.transactions.last() {
                    if last_tx.migration_id >= end_block as i64 {
                        break;
                    }

                    // Set up pagination for next request
                    after = Some(PaginationAfter {
                        after_migration_id: last_tx.migration_id,
                        after_record_time: last_tx.record_time.clone(),
                    });
                } else {
                    break;
                }

                // If we got fewer results than requested, we're done
                if response.transactions.len() < page_size as usize {
                    break;
                }
            }

            debug!("Canton Scan API block stream completed");
        }
    }

    async fn latest_block(
        &mut self,
        _finalized: bool,
    ) -> Result<Option<BlockNum>, LatestBlockError> {
        // Fetch latest update to determine current offset
        // Canton has finality, so finalized flag is ignored

        match self.fetch_updates(1, None).await {
            Ok(response) => {
                if let Some(tx) = response.transactions.first() {
                    Ok(Some(tx.migration_id as BlockNum))
                } else {
                    Ok(Some(0))
                }
            }
            Err(e) => Err(Box::new(std::io::Error::other(format!(
                "Failed to get latest block: {e}"
            ))) as LatestBlockError),
        }
    }

    async fn wait_for_cleanup(self) -> Result<(), CleanupError> {
        // HTTP client doesn't require explicit cleanup
        Ok(())
    }

    fn provider_name(&self) -> &str {
        &self.provider_name
    }
}

/// Extract package name from template_id (e.g., "pkg_id:Module:Entity" -> get from API or default)
fn extract_package_name(template_id: &str) -> String {
    // The package_name is not in the template_id - it comes from a separate field
    // For now, return an empty string or extract from first segment
    template_id
        .split(':')
        .next()
        .unwrap_or("unknown")
        .to_string()
}

// ============================================================================
// Scan API Request/Response Types
// ============================================================================

#[derive(Debug, Serialize)]
struct UpdatesRequest {
    page_size: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<PaginationAfter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaginationAfter {
    after_migration_id: i64,
    after_record_time: String,
}

#[derive(Debug, Deserialize)]
struct UpdatesResponse {
    transactions: Vec<ApiTransaction>,
}

#[derive(Debug, Deserialize)]
struct ApiTransaction {
    update_id: String,
    migration_id: i64,
    record_time: String,
    synchronizer_id: String,
    #[serde(default)]
    workflow_id: String,
    #[serde(default)]
    effective_at: Option<String>,
    #[serde(default)]
    root_event_ids: Vec<String>,
    #[serde(default)]
    events_by_id: HashMap<String, Event>,
}

#[derive(Debug, Deserialize)]
struct Event {
    #[serde(default)]
    created: Option<CreatedEvent>,
    #[serde(default)]
    exercised: Option<ExercisedEvent>,
}

#[derive(Debug, Deserialize)]
struct CreatedEvent {
    contract_id: String,
    template_id: String,
    #[serde(default)]
    signatories: Vec<String>,
    #[serde(default)]
    observers: Vec<String>,
    #[serde(default)]
    create_arguments: serde_json::Value,
    #[serde(default)]
    created_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExercisedEvent {
    contract_id: String,
    template_id: String,
    choice: String,
    #[serde(default)]
    consuming: bool,
    #[serde(default)]
    acting_parties: Vec<String>,
    #[serde(default)]
    choice_argument: serde_json::Value,
    #[serde(default)]
    exercise_result: serde_json::Value,
    #[serde(default)]
    child_event_ids: Vec<String>,
}

/// Errors that can occur with the Canton Scan API client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Connection error
    #[error("Failed to connect to Scan API: {0}")]
    Connection(String),
    /// HTTP request error
    #[error("HTTP request failed: {0}")]
    Request(String),
    /// API error response
    #[error("Scan API error: {0}")]
    Api(String),
    /// JSON parsing error
    #[error("Failed to parse response: {0}")]
    Parse(String),
}
