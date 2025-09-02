use async_stream::stream;
use common::{BlockNum, BlockStreamer, BoxError, RawDatasetRows};
use futures::Stream;
use reqwest::Url;
use serde::Deserialize;
use serde_with::serde_as;

use crate::blocks;

#[derive(Debug, Deserialize)]
pub struct Eth1Data {
    pub deposit_root: String,
    pub deposit_count: String,
    pub block_hash: String,
}

#[derive(Clone)]
pub struct BeaconClient {
    network: String,
    url: Url,
    final_blocks_only: bool,
    client: reqwest::Client,
}

impl BeaconClient {
    pub fn new(network: String, url: Url, final_blocks_only: bool) -> Self {
        Self {
            network,
            url,
            final_blocks_only,
            client: reqwest::Client::new(),
        }
    }

    pub async fn fetch_block(&self, number: BlockNum) -> Result<RawDatasetRows, BoxError> {
        let url = format!("{}/eth/v2/beacon/blocks/{}", self.url, number);
        let response = self
            .client
            .get(&url)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await?
            .error_for_status()?;
        let record = response.json().await?;
        let row = blocks::json_to_row(&self.network, record)?;
        Ok(RawDatasetRows::new(vec![row]))
    }
}

impl BlockStreamer for BeaconClient {
    async fn block_stream(
        self,
        start: common::BlockNum,
        end: common::BlockNum,
    ) -> impl Stream<Item = Result<RawDatasetRows, BoxError>> + Send {
        stream! {
            for number in start..=end {
                match self.fetch_block(number).await {
                    Ok(block) => {
                        yield Ok(block);
                    }
                    Err(err) => {
                        yield Err(err);
                        continue;
                    }
                };
            }
        }
    }

    async fn latest_block(&mut self) -> Result<BlockNum, BoxError> {
        let block_id = match self.final_blocks_only {
            true => "finalized",
            false => "head",
        };
        let url = format!("{}/eth/v2/beacon/blocks/{}", self.url, block_id);
        let response = self
            .client
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await?
            .error_for_status()?;

        #[derive(serde::Deserialize)]
        pub struct Response {
            pub data: Data,
        }
        #[derive(serde::Deserialize)]
        pub struct Data {
            pub message: Message,
        }
        #[serde_as]
        #[derive(serde::Deserialize)]
        pub struct Message {
            #[serde_as(as = "serde_with::DisplayFromStr")]
            pub slot: u64,
        }
        let response: Response = response.json().await?;

        Ok(response.data.message.slot)
    }
}
