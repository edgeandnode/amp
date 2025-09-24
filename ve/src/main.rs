use anyhow::Result;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Block {
    height: u64,
    hash: String,
    // include other fields you need (e.g., parent hash, timestamp, transactions)
    // payload or other chain data can be added here as needed
}

// Simple placeholder for your real dependency logic
mod dep {
    use super::Block;
    pub async fn process(block: &Block) -> anyhow::Result<()> 
        println!("Processing block height: {}", block.height);
        // insert real in-block logic here
        Ok(())
    }
}

#[derive(Deserialize, Debug)]
struct BlockRow {
    height: u64,
    hash: String,
    // adapt to your DB schema if needed
}

#[tokio::main]
async fn main() -> Result<()> {
    // Configure nozzle's API or dataset endpoint
    let nozzle_endpoint = std::env::var("NOZZLE_BLOCKS_API").unwrap_or_else(|_| "http://localhost:8000/api/blocks".to_string());

    // Query first 8192 blocks (0..8191)
    // Adapt the query parameters to your nozzle API
    let url = format!("{}?start=0&limit=8192", nozzle_endpoint);
    let blocks: Vec<Block> = reqwest::get(&url).await?.json().await?;

    for b in blocks.iter() {
        dep::process(b).await?;
    }

    Ok(())
}
