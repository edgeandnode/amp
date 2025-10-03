fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(gen_openapi_spec)]
    {
        println!(
            "cargo:warning=Config 'gen_openapi_spec' enabled: Running OpenAPI schema generation"
        );

        let out_dir = std::env::var("OUT_DIR")?;
        let spec = admin_api::generate_openapi_spec();
        let spec_json = serde_json::to_string_pretty(&spec)?;
        let spec_path = format!("{out_dir}/openapi.spec.json");
        std::fs::write(&spec_path, spec_json)?;

        println!(
            "cargo:warning=Generated admin API OpenAPI spec file: {}",
            spec_path
        );
    }
    #[cfg(not(gen_openapi_spec))]
    {
        println!(
            "cargo:debug=Config 'gen_openapi_spec' not enabled: Skipping OpenAPI schema generation"
        );
    }
    Ok(())
}
