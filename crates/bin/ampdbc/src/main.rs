use ampdbc::{Config, QueryPipeline, Result};

#[tokio::main]
async fn main() -> Result<()> {
    main_inner().await
}

async fn main_inner() -> Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .expect("must provide --config <path>");

    let ampdbc_config_path = std::env::args()
        .nth(2)
        .expect("must provide --ampdbc-config <path>");

    let query = std::env::args().nth(3).expect("must provide <query>");

    let mut config = Config::load(config_path, ampdbc_config_path).await?;

    let pipeline = QueryPipeline::try_new(&mut config, &query).await?;

    pipeline.run().await?;

    Ok(())
}
