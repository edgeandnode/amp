use std::{num::NonZeroU32, sync::Arc};

use async_stream::stream;
use common::{BlockNum, BoxError, RawDatasetRows};
use futures::Stream;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use reqwest::{StatusCode, Url};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct BeaconClient {
    url: Url,
    network: String,
    provider_name: String,
    client: reqwest::Client,
    concurrency_limiter: Arc<tokio::sync::Semaphore>,
    rate_limiter: Option<Arc<DefaultDirectRateLimiter>>,
}

impl BeaconClient {
    pub fn new(
        url: Url,
        network: String,
        provider_name: String,
        concurrent_request_limit: u16,
        rate_limit_per_minute: Option<NonZeroU32>,
    ) -> Self {
        let rate_limiter = rate_limit_per_minute.map(|limit| {
            let quota = Quota::per_minute(limit);
            Arc::new(RateLimiter::direct(quota))
        });
        Self {
            url,
            network,
            provider_name,
            client: reqwest::Client::new(),
            concurrency_limiter: Semaphore::new(concurrent_request_limit as usize).into(),
            rate_limiter,
        }
    }

    pub async fn fetch_block(&self, number: BlockNum) -> Result<RawDatasetRows, BoxError> {
        let _permit = self.concurrency_limiter.acquire().await;
        if let Some(rate_limiter) = &self.rate_limiter {
            rate_limiter.until_ready().await;
        }

        let url = format!("{}/eth/v2/beacon/blocks/{}", self.url, number);
        let response = self
            .client
            .get(&url)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await?;
        if !response.status().is_success() && (response.status() != StatusCode::NOT_FOUND) {
            return Err(format!("Response status: {}", response.status().as_u16()).into());
        }
        let record = response.json().await?;
        let row = crate::block::json_to_row(&self.network, record)?;
        Ok(RawDatasetRows::new(vec![row]))
    }
}

impl common::BlockStreamer for BeaconClient {
    fn provider_name(&self) -> &str {
        &self.provider_name
    }

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

    async fn latest_block(&mut self, finalized: bool) -> Result<Option<BlockNum>, BoxError> {
        let _permit = self.concurrency_limiter.acquire().await;
        if let Some(rate_limiter) = &self.rate_limiter {
            rate_limiter.until_ready().await;
        }

        let block_id = match finalized {
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
        #[serde_with::serde_as]
        #[derive(serde::Deserialize)]
        pub struct Message {
            #[serde_as(as = "serde_with::DisplayFromStr")]
            pub slot: u64,
        }
        let response: Response = response.json().await?;

        Ok(Some(response.data.message.slot))
    }

    async fn wait_for_cleanup(self) -> Result<(), BoxError> {
        Ok(())
    }
}
