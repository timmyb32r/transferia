use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression as ParquetCompression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use transferia_connector_support::outbound_http::{
    NetworkPolicy, OutboundHttpClient, OutboundHttpError,
};

use super::client::quote_identifier;
use super::config::{
    ClickHouseCompression, ClickHouseInsertFormat, ClickHouseSinkConfig,
};
use super::transport::{InsertError, InsertTransport};
use crate::connectors::address::host_port;

pub(super) struct HttpInsertTransport {
    context: HttpInsertContext,
    next_host: AtomicUsize,
}

#[derive(Clone)]
struct HttpInsertContext {
    hosts: Arc<[String]>,
    http_port: u16,
    trusted_plaintext: bool,
    database: Arc<str>,
    username: Arc<str>,
    password: Arc<str>,
    client: OutboundHttpClient,
    format: ClickHouseInsertFormat,
    compression: ClickHouseCompression,
    format_threads: usize,
    parquet_row_group_rows: usize,
    async_insert: bool,
}

impl HttpInsertTransport {
    pub(super) fn new(config: &ClickHouseSinkConfig) -> anyhow::Result<Self> {
        let roots = match &config.tls_ca_file {
            Some(path) => {
                let pem = std::fs::read(path).map_err(|error| {
                    anyhow::anyhow!("cannot read ClickHouse TLS CA bundle {path:?}: {error}")
                })?;
                reqwest::Certificate::from_pem_bundle(&pem)
                    .map_err(|error| anyhow::anyhow!("cannot parse ClickHouse TLS CA bundle: {error}"))?
            }
            None => Vec::new(),
        };
        Ok(Self {
            context: HttpInsertContext {
                hosts: Arc::from(config.hosts.clone()),
                http_port: config.http_port,
                trusted_plaintext: config.trusted_plaintext,
                database: Arc::from(config.database.as_str()),
                username: Arc::from(config.username.as_str()),
                password: Arc::from(config.password.as_str()),
                client: OutboundHttpClient::new(
                    config.request_timeout(),
                    roots,
                    NetworkPolicy::AllowPrivateNetworks,
                )?,
                format: config.insert_format,
                compression: config.compression,
                format_threads: config.format_threads,
                parquet_row_group_rows: config.parquet_row_group_rows,
                async_insert: config.async_insert,
            },
            next_host: AtomicUsize::new(0),
        })
    }
}

impl HttpInsertContext {
    async fn insert_encoded(
        self,
        host_index: usize,
        table: Arc<str>,
        batches: Vec<RecordBatch>,
    ) -> Result<(), InsertError> {
        let schema = batches
            .first()
            .ok_or_else(|| InsertError::Permanent(anyhow::anyhow!("empty INSERT batch list")))?
            .schema();
        if batches.iter().any(|batch| batch.schema() != schema) {
            return Err(InsertError::Permanent(anyhow::anyhow!(
                "all INSERT batches must have the same Arrow schema"
            )));
        }
        let format = self.format;
        let compression = self.compression;
        let row_group_rows = self.parquet_row_group_rows;
        let body = tokio::task::spawn_blocking(move || {
            encode(format, compression, row_group_rows, batches)
        })
        .await
        .map_err(|error| {
            InsertError::Permanent(anyhow::anyhow!(
                "ClickHouse {format:?} encoder task failed: {error}"
            ))
        })?
        .map_err(InsertError::Permanent)?;

        let host = &self.hosts[host_index];
        let scheme = if self.trusted_plaintext {
            "http"
        } else {
            "https"
        };
        let mut url = reqwest::Url::parse(&format!(
            "{scheme}://{}/",
            host_port(host, self.http_port)
        ))
        .map_err(|error| InsertError::Permanent(error.into()))?;
        let columns = schema
            .fields()
            .iter()
            .map(|field| quote_identifier(field.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "INSERT INTO {}.{} ({columns}) FORMAT {}",
            quote_identifier(&self.database),
            quote_identifier(&table),
            format.clickhouse_name(),
        );
        let format_threads = self.format_threads.to_string();
        url.query_pairs_mut()
            .append_pair("query", &query)
            .append_pair("max_threads", &format_threads)
            .append_pair("max_parsing_threads", &format_threads)
            .append_pair("input_format_parallel_parsing", "1")
            .append_pair("input_format_parquet_allow_missing_columns", "0")
            .append_pair("input_format_parquet_case_insensitive_column_matching", "0")
            .append_pair("input_format_arrow_allow_missing_columns", "0")
            .append_pair("input_format_arrow_case_insensitive_column_matching", "0")
            .append_pair("async_insert", if self.async_insert { "1" } else { "0" })
            .append_pair("wait_for_async_insert", "1")
            .append_pair("insert_deduplicate", "0");
        let response = self
            .client
            .request(reqwest::Method::POST, url)
            .configure(|request| {
                request
                    .basic_auth(self.username.as_ref(), Some(self.password.as_ref()))
                    .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                    .body(body)
            })
            .send()
            .await
            .map_err(classify_http_error)?;
        let status = response.status();
        let response_body = response.bytes().await.map_err(|error| {
            InsertError::Transient(anyhow::anyhow!(
                "ClickHouse HTTP INSERT response failed after an ambiguous result: {error}"
            ))
        })?;
        if status.is_success() {
            return Ok(());
        }
        let message = String::from_utf8_lossy(&response_body);
        let message = message
            .lines()
            .next()
            .unwrap_or("the server rejected the request");
        let error = anyhow::anyhow!("ClickHouse HTTP {status}: {message}");
        if status.is_server_error()
            || status == reqwest::StatusCode::REQUEST_TIMEOUT
            || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        {
            Err(InsertError::Transient(error))
        } else {
            Err(InsertError::Permanent(error))
        }
    }
}

impl InsertTransport for HttpInsertTransport {
    fn insert(
        &self,
        table: Arc<str>,
        batches: Vec<RecordBatch>,
    ) -> BoxFuture<'static, Result<(), InsertError>> {
        let context = self.context.clone();
        let host_index = self.next_host.fetch_add(1, Ordering::Relaxed) % context.hosts.len();
        Box::pin(async move { context.insert_encoded(host_index, table, batches).await })
    }
}

impl ClickHouseInsertFormat {
    const fn clickhouse_name(self) -> &'static str {
        match self {
            Self::Native => "Native",
            Self::Parquet => "Parquet",
            Self::ArrowStream => "ArrowStream",
        }
    }
}

fn encode(
    format: ClickHouseInsertFormat,
    compression: ClickHouseCompression,
    parquet_row_group_rows: usize,
    batches: Vec<RecordBatch>,
) -> anyhow::Result<Vec<u8>> {
    match format {
        ClickHouseInsertFormat::Native => {
            anyhow::bail!("native INSERTs must use the native protocol transport")
        }
        ClickHouseInsertFormat::Parquet => {
            encode_parquet(compression, parquet_row_group_rows, &batches)
        }
        ClickHouseInsertFormat::ArrowStream => encode_arrow_stream(compression, &batches),
    }
}

fn encode_parquet(
    compression: ClickHouseCompression,
    row_group_rows: usize,
    batches: &[RecordBatch],
) -> anyhow::Result<Vec<u8>> {
    let properties = WriterProperties::builder()
        .set_compression(match compression {
            ClickHouseCompression::None => ParquetCompression::UNCOMPRESSED,
            ClickHouseCompression::Lz4 => ParquetCompression::LZ4_RAW,
            ClickHouseCompression::Zstd => ParquetCompression::ZSTD(ZstdLevel::default()),
        })
        .set_max_row_group_size(row_group_rows)
        .build();
    let mut output = Vec::new();
    let mut writer = ArrowWriter::try_new(
        &mut output,
        batches
            .first()
            .ok_or_else(|| anyhow::anyhow!("empty Parquet INSERT batch list"))?
            .schema(),
        Some(properties),
    )?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(output)
}

fn encode_arrow_stream(
    compression: ClickHouseCompression,
    batches: &[RecordBatch],
) -> anyhow::Result<Vec<u8>> {
    let compression = match compression {
        ClickHouseCompression::None => None,
        ClickHouseCompression::Lz4 => Some(arrow::ipc::CompressionType::LZ4_FRAME),
        ClickHouseCompression::Zstd => Some(arrow::ipc::CompressionType::ZSTD),
    };
    let options = IpcWriteOptions::default().try_with_compression(compression)?;
    let mut output = Vec::new();
    let schema = batches
        .first()
        .ok_or_else(|| anyhow::anyhow!("empty ArrowStream INSERT batch list"))?
        .schema();
    let mut writer =
        StreamWriter::try_new_with_options(&mut output, schema.as_ref(), options)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(output)
}

fn classify_http_error(error: OutboundHttpError) -> InsertError {
    match error {
        OutboundHttpError::Request(request)
            if request.is_timeout()
                || request.is_connect()
                || request.is_body()
                || request.is_request() =>
        {
            InsertError::Transient(anyhow::Error::new(request))
        }
        error => InsertError::Permanent(anyhow::Error::new(error)),
    }
}

#[cfg(test)]
#[path = "tests/http.rs"]
mod tests;
