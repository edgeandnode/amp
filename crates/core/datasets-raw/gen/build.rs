fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(gen_schema_manifest)]
    {
        println!(
            "cargo:warning=Config 'gen_schema_manifest' enabled: Running JSON schema generation"
        );
        let out_dir = std::env::var("OUT_DIR")?;
        let manifest_schema =
            schemars::schema_for!(datasets_raw::manifest::schema::RawManifestSchema);
        let manifest_schema_json = serde_json::to_string_pretty(&manifest_schema)?;
        let manifest_schema_path = format!("{out_dir}/raw.schema.json");
        std::fs::write(&manifest_schema_path, &manifest_schema_json)?;
        println!(
            "cargo:warning=Generated unified raw dataset manifest schema file: {}",
            manifest_schema_path
        );
    }
    #[cfg(not(gen_schema_manifest))]
    {
        println!(
            "cargo:debug=Config 'gen_schema_manifest' not enabled: Skipping JSON schema generation"
        );
    }

    Ok(())
}
