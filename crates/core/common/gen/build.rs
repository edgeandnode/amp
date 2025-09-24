fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(gen_schema)]
    {
        println!("cargo:warning=Config 'gen_schema' enabled: Running JSON schema generation");

        let out_dir = std::env::var("OUT_DIR")?;

        // Generate JSON schema for common derived dataset manifest struct
        let derived_manifest_schema = schemars::schema_for!(common::manifest::derived::Manifest);
        let derived_manifest_schema_json = serde_json::to_string_pretty(&derived_manifest_schema)?;
        let derived_manifest_schema_path = format!("{out_dir}/schema.json");
        std::fs::write(&derived_manifest_schema_path, derived_manifest_schema_json)?;

        println!(
            "cargo:warning=Generated common derived dataset manifest schema file: {}",
            derived_manifest_schema_path
        );

        // Generate JSON schema for SQL dataset manifest struct
        let sql_manifest_schema = schemars::schema_for!(common::manifest::sql_datasets::Manifest);
        let sql_manifest_schema_json = serde_json::to_string_pretty(&sql_manifest_schema)?;
        let sql_manifest_schema_path = format!("{out_dir}/sql_schema.json");
        std::fs::write(&sql_manifest_schema_path, sql_manifest_schema_json)?;

        println!(
            "cargo:warning=Generated SQL dataset manifest schema file: {}",
            sql_manifest_schema_path
        );
    }
    #[cfg(not(gen_schema))]
    {
        println!("cargo:debug=Config 'gen_schema' not enabled: Skipping JSON schema generation");
    }

    Ok(())
}
