fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(gen_schema)]
    {
        println!("cargo:warning=Config 'gen_schema' enabled: Running JSON schema generation");
        let out_dir = std::env::var("OUT_DIR")?;
        let dataset_def_schema = schemars::schema_for!(evm_rpc_datasets::Manifest);
        let dataset_def_schema_json = serde_json::to_string_pretty(&dataset_def_schema)?;
        let dataset_def_schema_path = format!("{out_dir}/schema.json");
        std::fs::write(&dataset_def_schema_path, dataset_def_schema_json)?;
        println!(
            "cargo:warning=Generated EVM RPC dataset definition schema file: {}",
            dataset_def_schema_path
        );
    }
    #[cfg(not(gen_schema))]
    {
        println!("cargo:debug=Config 'gen_schema' not enabled: Skipping JSON schema generation");
    }
    Ok(())
}
