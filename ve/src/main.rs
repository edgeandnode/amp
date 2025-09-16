use anyhow::{Result, anyhow};
use serde::Deserialize;
use serde_json;

#[derive(Deserialize, Debug)]
struct Block {
    block_num: u64,
    hash: String,
}

async fn fetch_blocks_ndjson(base_url: &str, query: &str) -> Result<Vec<Block>> {
    let client = reqwest::Client::new();
    let resp = client.post(base_url)
        .body(query.to_string())
        .header("Content-Type", "text/plain")
        .send()
        .await
        .map_err(|e| anyhow!(e.to_string()))?;

    if !resp.status().is_success() {
        return Err(anyhow!("HTTP request failed with status {}", resp.status()));
    }

    let text = resp.text().await.map_err(|e| anyhow!(e.to_string()))?;
    let mut blocks = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        let b: Block = serde_json::from_str(line).map_err(|e| anyhow!(e.to_string()))?;
        blocks.push(b);
    }
    Ok(blocks)
}

mod dep {
    use super::Block;
    pub async fn process(block: &Block) -> anyhow::Result<()> {
        println!("Processing block height: {}", block.block_num);
        println!("Processing block hash: {}", block.hash);


        // insert real in-block logic here
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let base_url = std::env::var("NOZZLE_BLOCKS_API").unwrap_or_else(|_| "http://localhost:1603".to_string());
    let query = "select * from eth_firehose.blocks limit 8192";

    let blocks = fetch_blocks_ndjson(&base_url, query).await?;
    for b in blocks.iter() {
        dep::process(b).await?;
    }

    Ok(())
}
