mod config;
mod sink;
mod source;
mod src_batch_and_stream;
mod src_stream;
mod transport;
mod types;

pub(crate) use types::source_type_mapping;

pub(crate) fn sink_type_mapping() -> transferia_registry::type_mapping::TypeMapping {
    transferia_registry::type_mapping::destination_mapping("Evaluated by YDB's YQL type resolver. Non-null columns without extensions. YDB extensions, optionality and primary-key constraints are validated separately.", sink::yql_type)
}

pub use config::{
    YdbAuth, YdbConnectionCheckConfig, YdbConnectionConfig, YdbSinkConfig, YdbSourceConfig,
    YdbTableConfig,
};
pub use sink::YdbSinkConnector;
pub use source::YdbSourceConnector;
pub use src_stream::YdbReplicationConfig;

pub async fn check_connection(config: &YdbConnectionConfig) -> anyhow::Result<()> {
    let mut client = transport::YdbClient::connect(config).await?;
    let session_id = client.create_session().await?;
    client.delete_session(session_id).await
}

pub async fn check_network_connection(config: &YdbConnectionCheckConfig) -> anyhow::Result<()> {
    let endpoint = config.connection().tonic_endpoint()?;
    let uri = endpoint.parse::<http::Uri>()?;
    let host = uri
        .host()
        .ok_or_else(|| anyhow::anyhow!("ydb.endpoint has no host"))?;
    let port = uri.port_u16().unwrap_or_else(|| {
        if uri.scheme_str() == Some("https") {
            443
        } else {
            80
        }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::net::TcpStream::connect((host, port)),
    )
    .await
    .map_err(|_| anyhow::anyhow!("YDB network connection timed out after 3 seconds"))??;
    Ok(())
}

#[cfg(test)]
mod tests;
