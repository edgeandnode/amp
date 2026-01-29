fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(gen_proto)]
    {
        println!("cargo:warning=Config 'gen_proto' enabled: Running protobuf codegen");

        let mut config = prost_build::Config::new();
        config.out_dir("src/proto");

        config.compile_protos(&["proto/worker_events.proto"], &["proto/"])?;

        // Instruct cargo to rerun this build script if any of the proto files change
        println!("cargo:rerun-if-changed=proto");
    }
    #[cfg(not(gen_proto))]
    {
        println!("cargo:debug=Config 'gen_proto' not enabled: Skipping protobuf codegen");
    }

    Ok(())
}
