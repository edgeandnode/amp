fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(gen_schema_manifest)]
    {
        println!(
            "cargo:warning=Config 'gen_schema_manifest' enabled: Running JSON schema generation"
        );

        let out_dir = std::env::var("OUT_DIR")?;

        // Generate JSON schema for common derived dataset manifest struct
        let derived_manifest_schema = schemars::schema_for!(datasets_derived::Manifest);
        let derived_manifest_schema_json = serde_json::to_string_pretty(&derived_manifest_schema)?;
        let derived_manifest_schema_path = format!("{out_dir}/schema.json");
        std::fs::write(&derived_manifest_schema_path, derived_manifest_schema_json)?;

        println!(
            "cargo:warning=Generated derived dataset manifest schema file: {}",
            derived_manifest_schema_path
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
