//! PQv1 smoke test: connect (proxy discovery is internal) and read one batch.
use ydb_ch_replicator::config::yaml::{build_credentials_with_token, Config, SourceConfig};
use ydb_ch_replicator::pipeline::source::Source;
use ydb_ch_replicator::source::pq_v1::{parse_endpoint, PqV1Client, PqV1Source};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_file("config_bench.yaml")?;
    let p = match &config.source {
        SourceConfig::Pqv1(p) => p,
        other => anyhow::bail!("Expected pqv1 source, got {:?}", std::mem::discriminant(other)),
    };
    let (_creds, token) = build_credentials_with_token(&p.auth)?;
    let token = token.expect("access_token auth required");
    let (scheme, host, _) = parse_endpoint(&p.connection_string)?;
    let endpoint = format!("{}://{}", scheme, host);

    println!("--- Connecting to {endpoint} topic={} ---", p.topic_path);
    let (client, queues) = PqV1Client::connect(
        &endpoint,
        &p.topic_path,
        &p.consumer_name,
        &token,
        &[0],
    )
    .await?;
    println!("✅ HANDSHAKE OK");

    let rx = queues.into_values().next().expect("one partition queue");
    let mut src = PqV1Source::new(client, rx, 0);

    println!("Waiting for DataBatch...");
    let result = src.read_batch().await?;
    let batch = match result {
        ydb_ch_replicator::pipeline::source::ReadResult::Batch(b) => b,
        _ => anyhow::bail!("Expected Batch, got Exhausted/Failed"),
    };
    println!("=== {} MESSAGES ===", batch.messages.len());
    for (i, m) in batch.messages.iter().enumerate().take(3) {
        println!("  [{}] {}...", i, String::from_utf8_lossy(&m.value[..m.value.len().min(120)]));
    }
    Ok(())
}
