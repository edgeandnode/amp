fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(gen_proto)]
    {
        println!("cargo:warning=Config 'gen_proto' enabled: Running protobuf codegen");

        let config = tonic_build::configure()
            .build_client(true)
            .build_server(false)
            .out_dir("src/proto")
            .include_file("mod.rs")
            .type_attribute(
                "TransactionErrorType",
                "#[cfg_attr(test, derive(enum_iterator::Sequence))]",
            )
            .type_attribute(
                "InstructionErrorType",
                "#[cfg_attr(test, derive(enum_iterator::Sequence))]",
            )
            .emit_rerun_if_changed(false); // See https://github.com/hyperium/tonic/issues/1070

        config.compile_protos(
            &[
                "proto/confirmed_block.proto",
                "proto/entries.proto",
                "proto/transaction_by_addr.proto",
            ],
            &["proto"],
        )?;

        println!("cargo:rerun-if-changed=proto");
    }
    #[cfg(not(gen_proto))]
    {
        println!("cargo:debug=Config 'gen_proto' not enabled: Skipping protobuf codegen");
    }

    Ok(())
}
