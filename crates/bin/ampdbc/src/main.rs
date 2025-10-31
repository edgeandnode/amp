use ampdbc::{AmpdbcConfig, QueryPipeline, Result, cli::EntryPoint};
use clap::Parser;
use monitoring::logging;

#[tokio::main]
async fn main() {
    std::process::exit(match main_inner().await {
        Ok(()) => {
            eprintln!("✓ Pipeline completed successfully");
            0
        }
        Err(err) => {
            eprintln!("Error: {err}\n");

            // Provide helpful context based on error type
            if err.to_string().contains("config") || err.to_string().contains("Config") {
                eprintln!("   • Ensure --config points to a valid Amp configuration file");
                eprintln!("   • Ensure --ampdbc-config points to a valid ADBC configuration file");
                eprintln!("   • Check that all required fields are present in the configs");
                eprintln!("   • Verify file paths are absolute or relative to working directory\n");
            } else if err.to_string().contains("query") || err.to_string().contains("Query") {
                eprintln!("   • Verify SQL syntax matches DataFusion SQL:");
                eprintln!("     https://datafusion.apache.org/user-guide/sql/select.html");
                eprintln!("   • Ensure dataset references exist in your Amp config");
                eprintln!("   • Check table names use the format: dataset.table");
                eprintln!("   • Try running the query via `ampd server` first for debugging\n");
            } else if err.to_string().contains("connection")
                || err.to_string().contains("Connection")
            {
                eprintln!("   • Verify destination database is reachable");
                eprintln!("   • Check credentials in ADBC config are correct");
                eprintln!("   • Ensure network/firewall allows outbound connections");
                eprintln!("   • For cloud databases, verify account/project permissions\n");
            }

            eprintln!("For more help, run: ampdbc --help");
            1
        }
    });
}

async fn main_inner() -> Result<()> {
    logging::init();
    let EntryPoint {
        query,
        table,
        config,
        ampdbc_config,
        create_mode,
        write_mode,
        auth_type,
    } = EntryPoint::parse();

    // TODO: Use create_mode and write_mode when configuring the pipeline
    // These will be passed to the ADBC driver to control table creation and write behavior
    tracing::debug!(
        ?create_mode,
        ?write_mode,
        %table,
        "Pipeline configuration"
    );

    let mut config = AmpdbcConfig::load(config, ampdbc_config, auth_type, create_mode).await?;

    let pipeline = QueryPipeline::try_new(&mut config, &query).await?;
    pipeline.run().await?;
    return Ok(());
}
