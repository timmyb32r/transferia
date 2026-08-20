mod client;
mod config;
mod schema;
mod sink;
pub mod src_batch;

pub use config::{
    YTsaurusAuthConfig, YTsaurusConnectionConfig, YTsaurusSinkConfig, YTsaurusSourceConfig,
    YTsaurusWriteFormat,
};
pub use sink::YTsaurusSinkProvider;
pub use src_batch::YTsaurusSourceProvider;

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
