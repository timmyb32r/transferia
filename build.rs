fn main() -> Result<(), Box<dyn core::error::Error>> {
    tonic_prost_build::configure()
        // Runtime uses raw tonic paths so generated client/server wrappers only
        // increase compile time and expose an API surface the binary never uses.
        .build_client(false)
        .build_server(false)
        .compile_protos(
            &[
                "proto/ydb/ydb_persqueue_v1.proto",
                "proto/ydb/ydb_discovery.proto",
            ],
            &["proto/"],
        )?;
    Ok(())
}
