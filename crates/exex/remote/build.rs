fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        // Build client for tests, server for production
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/exex.proto"], &["proto"])?;
    Ok(())
}
