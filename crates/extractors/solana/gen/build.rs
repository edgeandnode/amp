fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(gen_schema_manifest)]
    {
        println!(
            "cargo:warning=Config 'gen_schema_manifest' enabled: Running JSON schema generation"
        );
        let out_dir = std::env::var("OUT_DIR")?;
        let manifest_schema = schemars::schema_for!(solana_datasets::Manifest);
        let manifest_schema_json = serde_json::to_string_pretty(&manifest_schema)?;
        let manifest_schema_path = format!("{out_dir}/schema.json");
        std::fs::write(&manifest_schema_path, manifest_schema_json)?;
        println!(
            "cargo:warning=Generated Solana dataset manifest schema file: {}",
            manifest_schema_path
        );
    }
    #[cfg(not(gen_schema_manifest))]
    {
        println!(
            "cargo:debug=Config 'gen_schema_manifest' not enabled: Skipping JSON schema generation"
        );
    }

    #[cfg(gen_schema_tables)]
    {
        println!(
            "cargo:warning=Config 'gen_schema_tables' enabled: Running table schema markdown generation"
        );
        let rt = tokio::runtime::Runtime::new()?;
        let markdown = rt.block_on(async {
            datasets_raw::schema::to_markdown(solana_datasets::tables::all("test_network")).await
        });
        let out_dir = std::env::var("OUT_DIR")?;
        let tables_schema_path = format!("{out_dir}/tables.md");
        std::fs::write(&tables_schema_path, markdown)?;
        println!(
            "cargo:warning=Generated Solana table schema markdown file: {}",
            tables_schema_path
        );
    }
    #[cfg(not(gen_schema_tables))]
    {
        println!(
            "cargo:debug=Config 'gen_schema_tables' not enabled: Skipping table schema markdown generation"
        );
    }

    Ok(())
}
