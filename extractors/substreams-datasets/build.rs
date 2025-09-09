fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "gen-proto")]
    {
        println!("cargo:warning=Feature 'gen-proto' enabled: Running protobuf codegen");

        let mut prost_config = prost_build::Config::new();

        // These comments break doc tests, so we disable them.
        prost_config.disable_comments(["google.protobuf.Timestamp", "google.protobuf.Any"]);

        let config = tonic_build::configure()
            .build_server(false)
            .out_dir("src/proto");

        // Prost does not have support for the Protobuf JSON format, which we use for pretty output,
        // for debug and test purposes. These are hacks so we can get something close just with
        // `serde_json`.
        let config = config
            .protoc_arg("--experimental_allow_proto3_optional")
            .compile_well_known_types(true)
            .include_file("mod.rs")
            .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
            .emit_rerun_if_changed(false); // See https://github.com/hyperium/tonic/issues/1070

        config.compile_protos_with_config(
            prost_config,
            &[
                "sf/substreams/v1/modules.proto",
                "sf/substreams/v1/package.proto",
                "sf/substreams/rpc/v2/substreams.proto",
                "sf/substreams/sink/sql/v1/services.proto",
                "sf/substreams/sink/database/v1/database.proto",
            ],
            &["proto", "proto/sf/substreams/v1"],
        )?;

        // Instruct cargo to rerun this build script if any of the proto files change
        println!("cargo:rerun-if-changed=proto");
    }
    Ok(())
}
