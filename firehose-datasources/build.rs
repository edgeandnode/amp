fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut prost_config = prost_build::Config::new();

    // These comments break doc tests, so we disable them.
    prost_config.disable_comments(&["google.protobuf.Timestamp", "google.protobuf.Any"]);

    let config = tonic_build::configure()
        .build_server(false)
        .out_dir("src/proto");

    // Prost does not have support for the Protobuf JSON format, which we use for pretty output, for
    // debug and test purposes. These are hacks so we can get something close just with `serde_json`.
    let config = config
        .compile_well_known_types(true)
        .include_file("mod.rs")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");

    config.compile_with_config(
        prost_config,
        &["proto/firehose.proto", "proto/ethereum.proto"],
        &[""],
    )?;

    Ok(())
}
