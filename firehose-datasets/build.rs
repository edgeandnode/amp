fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .out_dir("src/proto")
        .build_server(false)
        .compile(&["proto/firehose.proto", "proto/ethereum.proto"], &[""])?;
    Ok(())
}
