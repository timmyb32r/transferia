use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_yaml::Value;
use tokio_util::sync::CancellationToken;

use crate::config::yaml::{validate_parser, AuthConfig, ParserConfig};
use crate::pipeline::source::Source;
use crate::providers::traits::SourceProvider;
use crate::providers::yds::credentials::{build_credentials, build_credentials_with_token};
use crate::providers::yds::pq_v1::{parse_endpoint, partition_to_group, PqV1Client, PqV1Source};
use crate::providers::yds::ydb_topic::YdbTopicSource;

#[derive(Debug, Clone, Deserialize)]
pub struct YdsSourceConfig {
    pub connection_string: String,
    pub topic_path: String,
    pub consumer_name: String,
    #[serde(default)]
    pub auth: AuthConfig,
    pub parser: ParserConfig,
    #[serde(default)]
    pub discovery_endpoint: Option<String>,
    #[serde(default)]
    pub partition_ids: Option<Vec<i64>>,
}

pub struct YdsSourceProvider {
    cfg: YdsSourceConfig,
    kind: String, // "topic" or "pqv1"
}

impl YdsSourceProvider {
    pub fn from_config(value: Value, kind: &str) -> anyhow::Result<Self> {
        let cfg: YdsSourceConfig = serde_yaml::from_value(value)
            .map_err(|e| anyhow::anyhow!("Failed to parse YDS source config: {}", e))?;
        if cfg.connection_string.is_empty() {
            anyhow::bail!("{}.connection_string must not be empty", kind);
        }
        if cfg.topic_path.is_empty() {
            anyhow::bail!("{}.topic_path must not be empty", kind);
        }
        if cfg.consumer_name.is_empty() {
            anyhow::bail!("{}.consumer_name must not be empty", kind);
        }
        let allowed_parsers: std::collections::HashSet<&str> = ["json_parser"].into();
        validate_parser(&cfg.parser, &[], &allowed_parsers)?;
        Ok(Self { cfg, kind: kind.to_string() })
    }
}

impl SourceProvider for YdsSourceProvider {
    fn build_source<'a>(
        &'a self,
        partition_id: i64,
        _cancel_token: CancellationToken,
    ) -> BoxFuture<'a, anyhow::Result<Box<dyn Source>>> {
        let conn = self.cfg.connection_string.clone();
        let tpath = self.cfg.topic_path.clone();
        let consumer = self.cfg.consumer_name.clone();
        let auth = self.cfg.auth.clone();
        let disc_ep = self.cfg.discovery_endpoint.clone();
        let kind = self.kind.clone();

        Box::pin(async move {
            match kind.as_str() {
                "topic" => {
                    let creds = build_credentials(&auth)?;
                    let src = YdbTopicSource::new(&conn, &tpath, &consumer, partition_id, creds, disc_ep.as_deref()).await?;
                    Ok(Box::new(src) as Box<dyn Source>)
                }
                "pqv1" => {
                    let (_, raw_token) = build_credentials_with_token(&auth)?;
                    let token = raw_token.ok_or_else(|| anyhow::anyhow!("PQv1 requires access_token auth"))?;
                    let (scheme, host, _) = parse_endpoint(&conn)?;
                    let endpoint = format!("{}://{}", scheme, host);
                    let pg_id = partition_to_group(partition_id);
                    let (client, mut queues) = PqV1Client::connect(&endpoint, &tpath, &consumer, &token, &[pg_id]).await?;
                    let rx = queues.remove(&partition_id)
                        .ok_or_else(|| anyhow::anyhow!("No queue for partition {}", partition_id))?;
                    Ok(Box::new(PqV1Source::new(client, rx, partition_id)) as Box<dyn Source>)
                }
                _ => anyhow::bail!("Unknown YDS kind: {}", kind),
            }
        })
    }

    fn discover_partitions<'a>(
        &'a self,
        total_workers: u32,
        worker_index: u32,
    ) -> BoxFuture<'a, anyhow::Result<Vec<i64>>> {
        let cfg = self.cfg.clone();
        let kind = self.kind.clone();

        Box::pin(async move {
            match kind.as_str() {
                "pqv1" => {
                    let (_, raw_token) = build_credentials_with_token(&cfg.auth)?;
                    let token = raw_token.ok_or_else(|| anyhow::anyhow!("PQv1 requires access_token auth"))?;
                    let parts = if let Some(ref static_ids) = cfg.partition_ids {
                        static_ids.iter()
                            .filter(|id| id.unsigned_abs() as u32 % total_workers == worker_index)
                            .copied()
                            .collect()
                    } else {
                        let (scheme, host, _) = parse_endpoint(&cfg.connection_string)?;
                        let endpoint = format!("{}://{}", scheme, host);
                        PqV1Client::discover_partitions(&endpoint, &cfg.topic_path, &cfg.consumer_name, &token)
                            .await?
                            .into_iter()
                            .filter(|id| id.unsigned_abs() as u32 % total_workers == worker_index)
                            .collect()
                    };
                    Ok(parts)
                }
                "topic" => {
                    let creds = build_credentials(&cfg.auth)?;
                    let mut builder = ydb::ClientBuilder::new_from_connection_string(&cfg.connection_string)?
                        .with_credentials(creds);
                    if let Some(ref ep) = cfg.discovery_endpoint {
                        let discovery = ydb::StaticDiscovery::new_from_str(ep.as_str())
                            .map_err(|e| anyhow::anyhow!("StaticDiscovery: {}", e))?;
                        builder = builder.with_discovery(discovery);
                    }
                    let client = builder.client()?;
                    let mut topic_client = client.topic_client();
                    let parts = crate::partition::discover_my_partitions(
                        &mut topic_client, &cfg.topic_path, total_workers, worker_index,
                    ).await?;
                    Ok(parts)
                }
                _ => anyhow::bail!("Unknown YDS kind: {}", kind),
            }
        })
    }

    fn resolve_table_name(&self) -> anyhow::Result<String> {
        self.cfg.parser.resolve_table_name(&self.cfg.topic_path)
    }

    fn parser_config(&self) -> &ParserConfig {
        &self.cfg.parser
    }
}
