mod client;
mod config;
mod discard;
mod schema;
mod sink;
pub mod src_batch;

pub use config::{
    YTsaurusAuthConfig, YTsaurusBenchmarkDiscardConfig, YTsaurusConnectionConfig,
    YTsaurusReadFormat, YTsaurusSinkConfig, YTsaurusSourceConfig, YTsaurusTableReaderConfig,
    YTsaurusWriteFormat,
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
