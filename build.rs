fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(
            &[
                "proto/ydb/ydb_persqueue_v1.proto",
                "proto/ydb/ydb_persqueue_v1_service.proto",
                "proto/ydb/ydb_discovery.proto",
                "proto/ydb/ydb_discovery_v1.proto",
            ],
            &["proto/"],
        )?;
    Ok(())
}
