#[cfg(feature = "provider-logbroker")]
fn main() -> Result<(), Box<dyn core::error::Error>> {
    println!("cargo:rerun-if-changed=../../proto");
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(false)
        .compile_protos(
            &[
                "../../proto/ydb/ydb_persqueue_v1.proto",
                "../../proto/ydb/ydb_discovery.proto",
            ],
            &["../../proto/"],
        )?;
    Ok(())
}

#[cfg(not(feature = "provider-logbroker"))]
fn main() {}
