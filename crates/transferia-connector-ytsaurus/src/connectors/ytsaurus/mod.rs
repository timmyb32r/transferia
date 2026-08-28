mod client;
mod columnar_chunk;
mod config;
mod direct_data_node;
mod discard;
mod native_rpc;
mod schema;
mod sink;
pub mod src_batch;
mod yt_wire;

pub use config::{
    YTsaurusAuthConfig, YTsaurusBenchmarkDiscardConfig, YTsaurusBenchmarkTransport,
    YTsaurusConnectionConfig, YTsaurusReadFormat, YTsaurusReadOrdering, YTsaurusSinkConfig,
    YTsaurusSourceConfig, YTsaurusTableReaderConfig, YTsaurusWriteFormat,
};
pub use sink::YTsaurusSinkConnector;
pub use src_batch::YTsaurusSourceConnector;

pub async fn list_table_path_suggestions(
    config: &YTsaurusConnectionConfig,
    query: &str,
) -> anyhow::Result<Vec<String>> {
    let client = client::YTsaurusClient::new(config)?;
    client.list_table_paths(query).await
}

pub async fn check_connection(config: &YTsaurusConnectionConfig) -> anyhow::Result<()> {
    checked_client(config).await?;
    Ok(())
}

pub(crate) async fn check_source_tables(
    config: &YTsaurusConnectionConfig,
    paths: &[String],
) -> anyhow::Result<()> {
    let client = checked_client(config).await?;
    for path in paths {
        let node_type = node_type(&client, path).await?;
        anyhow::ensure!(
            node_type == "table",
            "configured YTsaurus source path '{path}' is a {node_type}, not a table"
        );
    }
    Ok(())
}

pub(crate) async fn check_sink_directory(
    config: &YTsaurusConnectionConfig,
    path: &str,
) -> anyhow::Result<()> {
    let client = checked_client(config).await?;
    let node_type = node_type(&client, path).await?;
    anyhow::ensure!(
        matches!(
            node_type.as_str(),
            "map_node" | "portal_entrance" | "rootstock"
        ),
        "configured YTsaurus destination path '{path}' is a {node_type}, not a directory"
    );
    Ok(())
}

async fn checked_client(
    config: &YTsaurusConnectionConfig,
) -> anyhow::Result<client::YTsaurusClient> {
    let client = client::YTsaurusClient::new(config)?;
    client
        .get_json("//sys/@id")
        .await
        .map_err(|error| anyhow::anyhow!("YTsaurus connection check failed: {error}"))?;
    Ok(client)
}

async fn node_type(client: &client::YTsaurusClient, path: &str) -> anyhow::Result<String> {
    client
        .get_json(&attribute_path(path, "type"))
        .await
        .map_err(|error| anyhow::anyhow!("cannot access YTsaurus entity '{path}': {error}"))?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("YTsaurus entity '{path}' returned a non-string type"))
}

fn attribute_path(path: &str, attribute: &str) -> String {
    let separator = if path.ends_with('/') { "" } else { "/" };
    format!("{path}{separator}@{attribute}")
}

#[cfg(test)]
mod tests;
