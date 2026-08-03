//! PQv1 smoke test: connect (proxy discovery is internal) and read one batch.
use ydb_ch_replicator::config::yaml::{build_credentials_with_token, Config};
use ydb_ch_replicator::pipeline::source::Source;
use ydb_ch_replicator::source::pq_v1::{parse_endpoint, PqV1Client, PqV1Source};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_file("config_bench.yaml")?;
    let (_creds, token) = build_credentials_with_token(&config.source.auth)?;
    let token = token.expect("access_token auth required");
    let (scheme, host, _) = parse_endpoint(&config.source.connection_string)?;
    let endpoint = format!("{}://{}", scheme, host);

    println!("--- Connecting to {endpoint} topic={} ---", config.source.topic_path);
    let (client, queues) = PqV1Client::connect(
        &endpoint,
        &config.source.topic_path,
        &config.source.consumer_name,
        &token,
        &[0],
    )
    .await?;
    println!("✅ HANDSHAKE OK");

    let rx = queues.into_values().next().expect("one partition queue");
    let mut src = PqV1Source::new(client, rx, 0);

    println!("Waiting for DataBatch...");
    let batch = src.read_batch().await?;
    println!("=== {} MESSAGES ===", batch.messages.len());
    for (i, m) in batch.messages.iter().enumerate().take(3) {
        println!("  [{}] {}...", i, String::from_utf8_lossy(&m.value[..m.value.len().min(120)]));
    }
    Ok(())
}
