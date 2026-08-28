use std::sync::Arc;
use std::time::Instant;

use anyhow::Context as _;
use arrow::datatypes::{DataType, Field, Schema};
use bytes::{Bytes, BytesMut};
use futures_util::stream::{self, FuturesOrdered};
use futures_util::StreamExt as _;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use transferia_connector_support::outbound_http::{NetworkPolicy, OutboundHttpClient};

use super::config::{ClickHouseParquetCompression, ClickHouseSourceConfig};
use super::connector::DiscoveredTable;
use super::reader::SnapshotStream;
use crate::connectors::address::host_port;
use crate::connectors::clickhouse::sink::client::quote_identifier;
use crate::metrics::SourceCounters;
use transferia_core::data::system_columns::SystemColumnKind;

#[derive(Clone, Copy)]
pub(super) struct ParquetReadSettings {
    pub compression: ClickHouseParquetCompression,
    pub max_threads: usize,
    pub row_group_rows: usize,
    pub decode_threads: usize,
    pub max_response_bytes: u64,
}

#[derive(Clone)]
pub(super) struct ParquetTransport {
    hosts: Arc<[String]>,
    http_port: u16,
    trusted_plaintext: bool,
    username: Arc<str>,
    password: Arc<str>,
    client: OutboundHttpClient,
    settings: ParquetReadSettings,
}

impl ParquetTransport {
    pub(super) fn new(
        config: &ClickHouseSourceConfig,
        settings: ParquetReadSettings,
    ) -> anyhow::Result<Self> {
        let roots = match &config.tls_ca_file {
            Some(path) => {
                let pem = std::fs::read(path)
                    .with_context(|| format!("cannot read ClickHouse TLS CA bundle {path:?}"))?;
                reqwest::Certificate::from_pem_bundle(&pem)
                    .context("cannot parse ClickHouse TLS CA bundle")?
            }
            None => Vec::new(),
        };
        Ok(Self {
            hosts: Arc::from(config.hosts.clone()),
            http_port: config.http_port,
            trusted_plaintext: config.trusted_plaintext,
            username: Arc::from(config.username.as_str()),
            password: Arc::from(config.password.as_str()),
            client: OutboundHttpClient::new(
                config.request_timeout(),
                roots,
                NetworkPolicy::AllowPrivateNetworks,
            )?,
            settings,
        })
    }

    pub(super) fn snapshot_stream(
        &self,
        table: DiscoveredTable,
        batch_rows: usize,
        counters: Arc<SourceCounters>,
        cancellation: CancellationToken,
    ) -> SnapshotStream {
        let capacity = self.settings.decode_threads.saturating_mul(2).max(1);
        let (sender, mut receiver) = mpsc::channel(capacity);
        let transport = self.clone();
        tokio::spawn(async move {
            if let Err(error) = transport
                .download_and_decode(
                    &table,
                    batch_rows,
                    Arc::clone(&counters),
                    cancellation,
                    &sender,
                )
                .await
            {
                drop(sender.send(Err(error)).await);
            }
        });
        Box::pin(stream::poll_fn(move |context| receiver.poll_recv(context)))
    }

    async fn download_and_decode(
        &self,
        table: &DiscoveredTable,
        batch_rows: usize,
        counters: Arc<SourceCounters>,
        cancellation: CancellationToken,
        sender: &mpsc::Sender<anyhow::Result<arrow::record_batch::RecordBatch>>,
    ) -> anyhow::Result<()> {
        let bytes = self
            .download(table, Arc::clone(&counters), &cancellation)
            .await?;
        let expected_schema = parquet_input_schema(table);
        let metadata = tokio::task::spawn_blocking({
            let bytes = bytes.clone();
            move || {
                ArrowReaderMetadata::load(
                    &bytes,
                    ArrowReaderOptions::new().with_schema(expected_schema),
                )
                .context("cannot decode ClickHouse Parquet metadata")
            }
        })
        .await
        .context("ClickHouse Parquet metadata task failed")??;
        let row_groups = metadata.metadata().num_row_groups();
        let concurrency = self.settings.decode_threads.min(row_groups.max(1));
        let mut next_row_group = 0;
        let mut decoders = FuturesOrdered::new();
        while next_row_group < concurrency {
            decoders.push_back(decode_row_group(
                bytes.clone(),
                metadata.clone(),
                next_row_group,
                batch_rows,
                Arc::clone(&counters),
            ));
            next_row_group += 1;
        }
        while let Some(decoded) = decoders.next().await {
            for batch in decoded.context("ClickHouse Parquet decoder task failed")?? {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => anyhow::bail!("ClickHouse Parquet read cancelled"),
                    result = sender.send(Ok(batch)) => {
                        result.map_err(|_| anyhow::anyhow!("ClickHouse Parquet consumer stopped"))?;
                    }
                }
            }
            if next_row_group < row_groups {
                decoders.push_back(decode_row_group(
                    bytes.clone(),
                    metadata.clone(),
                    next_row_group,
                    batch_rows,
                    Arc::clone(&counters),
                ));
                next_row_group += 1;
            }
        }
        Ok(())
    }

    async fn download(
        &self,
        table: &DiscoveredTable,
        counters: Arc<SourceCounters>,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<Bytes> {
        let mut failures = Vec::new();
        for host in self.hosts.iter() {
            match self
                .download_from_host(host, table, Arc::clone(&counters), cancellation)
                .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(error) if cancellation.is_cancelled() => return Err(error),
                Err(error) => failures.push(format!("{host}: {error:#}")),
            }
        }
        anyhow::bail!(
            "ClickHouse Parquet snapshot failed on every configured host: {}",
            failures.join("; ")
        )
    }

    async fn download_from_host(
        &self,
        host: &str,
        table: &DiscoveredTable,
        counters: Arc<SourceCounters>,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<Bytes> {
        let scheme = if self.trusted_plaintext {
            "http"
        } else {
            "https"
        };
        let mut url =
            reqwest::Url::parse(&format!("{scheme}://{}/", host_port(host, self.http_port)))?;
        let max_threads = self.settings.max_threads.to_string();
        let row_group_rows = self.settings.row_group_rows.to_string();
        url.query_pairs_mut()
            .append_pair("max_threads", &max_threads)
            .append_pair(
                "output_format_parquet_compression_method",
                self.settings.compression.clickhouse_name(),
            )
            .append_pair("output_format_parquet_row_group_size", &row_group_rows)
            .append_pair("output_format_parquet_parallel_encoding", "1")
            .append_pair("output_format_parquet_string_as_string", "0")
            .append_pair("output_format_parquet_write_page_index", "0")
            .append_pair("output_format_parquet_write_bloom_filter", "0");
        let query = format!(
            "SELECT * FROM {}.{} FORMAT Parquet",
            quote_identifier(&table.config.database),
            quote_identifier(&table.config.name)
        );
        let wait_started = Instant::now();
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => anyhow::bail!("ClickHouse Parquet read cancelled"),
            response = self.client
                .request(reqwest::Method::POST, url)
                .configure(|request| request
                    .basic_auth(self.username.as_ref(), Some(self.password.as_ref()))
                    .header(reqwest::header::CONTENT_TYPE, "text/plain")
                    .body(query))
                .send() => response.context("ClickHouse Parquet request failed")?,
        };
        counters.add_response_wait(wait_started.elapsed());
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let message = body
                .lines()
                .next()
                .unwrap_or("the server rejected the request");
            anyhow::bail!("ClickHouse HTTP {status}: {message}");
        }
        if let Some(length) = response.content_length() {
            anyhow::ensure!(
                length <= self.settings.max_response_bytes,
                "ClickHouse Parquet response declares {length} bytes, exceeding configured max_response_bytes {}",
                self.settings.max_response_bytes
            );
        }
        let initial_capacity = response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or(0);
        let mut body = BytesMut::with_capacity(initial_capacity);
        let mut stream = response.bytes_stream();
        while let Some(chunk) = {
            let wait_started = Instant::now();
            let next = tokio::select! {
                biased;
                () = cancellation.cancelled() => anyhow::bail!("ClickHouse Parquet read cancelled"),
                next = stream.next() => next,
            };
            counters.add_response_wait(wait_started.elapsed());
            next
        } {
            let chunk = chunk.context("ClickHouse Parquet response body failed")?;
            let next_len = body
                .len()
                .checked_add(chunk.len())
                .ok_or_else(|| anyhow::anyhow!("ClickHouse Parquet response size overflow"))?;
            anyhow::ensure!(
                u64::try_from(next_len).unwrap_or(u64::MAX) <= self.settings.max_response_bytes,
                "ClickHouse Parquet response exceeded configured max_response_bytes {}",
                self.settings.max_response_bytes
            );
            counters.add_network_raw_bytes(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            body.extend_from_slice(&chunk);
        }
        anyhow::ensure!(
            !body.is_empty(),
            "ClickHouse returned an empty Parquet response"
        );
        Ok(body.freeze())
    }
}

fn decode_row_group(
    bytes: Bytes,
    metadata: ArrowReaderMetadata,
    row_group: usize,
    batch_rows: usize,
    counters: Arc<SourceCounters>,
) -> tokio::task::JoinHandle<anyhow::Result<Vec<arrow::record_batch::RecordBatch>>> {
    tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let result = (|| {
            let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(bytes, metadata)
                .with_row_groups(vec![row_group])
                .with_batch_size(batch_rows)
                .build()
                .context("cannot construct ClickHouse Parquet row-group decoder")?;
            reader
                .map(|batch| batch.context("cannot decode ClickHouse Parquet record batch"))
                .collect::<anyhow::Result<Vec<_>>>()
        })();
        counters.add_network_decode_busy(started.elapsed());
        result
    })
}

fn parquet_input_schema(table: &DiscoveredTable) -> Arc<Schema> {
    Arc::new(Schema::new(
        table
            .schema
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let data_type = table
                    .physical_system_columns
                    .iter()
                    .find(|system| system.index == index && system.kind == SystemColumnKind::Topic)
                    .map_or_else(|| column.data_type.clone(), |_| DataType::Binary);
                Field::new(&column.name, data_type, column.nullable)
            })
            .collect::<Vec<_>>(),
    ))
}
