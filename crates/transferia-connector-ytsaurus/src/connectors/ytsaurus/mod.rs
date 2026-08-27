mod client;
mod columnar_chunk;
mod config;
mod discard;
mod direct_data_node;
mod native_rpc;
mod schema;
mod sink;
mod yt_wire;
pub mod src_batch;

pub use config::{
    YTsaurusAuthConfig, YTsaurusBenchmarkDiscardConfig, YTsaurusConnectionConfig,
    YTsaurusBenchmarkTransport, YTsaurusReadFormat, YTsaurusReadOrdering, YTsaurusSinkConfig,
    YTsaurusSourceConfig, YTsaurusTableReaderConfig, YTsaurusWriteFormat,
};
pub use sink::YTsaurusSinkConnector;
pub use src_batch::YTsaurusSourceConnector;

pub async fn check_connection(config: &YTsaurusConnectionConfig) -> anyhow::Result<()> {
    let client = client::YTsaurusClient::new(config)?;
    client
        .get_json("//sys/@id")
        .await
        .map_err(|error| anyhow::anyhow!("YTsaurus connection check failed: {error}"))?;
    Ok(())
}

#[cfg(test)]
mod tests;
