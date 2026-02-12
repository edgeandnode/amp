fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(gen_schema_config)]
    {
        println!(
            "cargo:warning=Config 'gen_schema_config' enabled: Running JSON schema generation"
        );
        let out_dir = std::env::var("OUT_DIR")?;
        let schema = schemars::schema_for!(amp_config::ConfigFile);
        let schema_json = serde_json::to_string_pretty(&schema)?;
        let schema_path = format!("{out_dir}/schema.json");
        std::fs::write(&schema_path, schema_json)?;
        println!(
            "cargo:warning=Generated config schema file: {}",
            schema_path
        );
    }
    #[cfg(not(gen_schema_config))]
    {
        println!(
            "cargo:debug=Config 'gen_schema_config' not enabled: Skipping JSON schema generation"
        );
    }

    Ok(())
}
