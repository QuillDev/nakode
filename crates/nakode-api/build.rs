fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: Cargo runs each build script in its own process. Setting PROTOC
    // here only configures this crate's child code generator.
    unsafe { std::env::set_var("PROTOC", protoc) };

    let proto = "../../proto/nakode/v1/nakode.proto";
    println!("cargo:rerun-if-changed={proto}");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("nakode-v1-descriptor.bin"),
        )
        .compile_protos(&[proto], &["../../proto"])?;
    Ok(())
}
