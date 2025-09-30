fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(gen_schema)]
    {
        println!("cargo:warning=Config 'gen_schema' enabled: Running JSON schema generation");
        let out_dir = std::env::var("OUT_DIR")?;
        let manifest_schema = schemars::schema_for!(firehose_datasets::dataset::Manifest);
        let manifest_schema_json = serde_json::to_string_pretty(&manifest_schema)?;
        let manifest_schema_path = format!("{out_dir}/schema.json");
        std::fs::write(&manifest_schema_path, manifest_schema_json)?;
        println!(
            "cargo:warning=Generated Firehose dataset manifest schema file: {}",
            manifest_schema_path
        );
    }
    #[cfg(not(gen_schema))]
    {
        println!("cargo:debug=Config 'gen_schema' not enabled: Skipping JSON schema generation");
    }
    Ok(())
}
