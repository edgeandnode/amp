fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(gen_schema_manifest)]
    {
        println!(
            "cargo:warning=Config 'gen_schema_manifest' enabled: Running JSON schema generation"
        );

        let out_dir = std::env::var("OUT_DIR")?;

        // Generate JSON schema for static dataset manifest struct
        let static_manifest_schema = schemars::schema_for!(amp_datasets_static::manifest::Manifest);
        let static_manifest_schema_json = serde_json::to_string_pretty(&static_manifest_schema)?;
        let static_manifest_schema_path = format!("{out_dir}/schema.json");
        std::fs::write(&static_manifest_schema_path, static_manifest_schema_json)?;

        println!(
            "cargo:warning=Generated static dataset manifest schema file: {}",
            static_manifest_schema_path
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
