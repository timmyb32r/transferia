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
use super::reader::{EnumTransport, SnapshotStream};
use super::types::is_string_conversion;
use crate::connectors::address::host_port;
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
        let expected_schema = parquet_input_schema(table)?;
        let table_name = format!("{}.{}", table.config.database, table.config.name);
        let metadata = tokio::task::spawn_blocking({
            let bytes = bytes.clone();
            move || {
                let inferred = ArrowReaderMetadata::load(
                    &bytes,
                    ArrowReaderOptions::new().with_skip_arrow_metadata(true),
                )
                .context("cannot decode ClickHouse Parquet metadata")?;
                // A supplied Arrow schema is a conversion hint: it can reinterpret
                // plain integers as timestamps. Validate the unhinted wire schema
                // first, then preserve the requested dictionary representation.
                let schema = validate_parquet_input_schema(inferred.schema(), &expected_schema)
                    .map_err(|error| anyhow::anyhow!(
                        "ClickHouse Parquet table '{table_name}' schema drifted: {error:#}",
                    ))?;
                ArrowReaderMetadata::try_new(
                    Arc::clone(inferred.metadata()),
                    ArrowReaderOptions::new().with_schema(schema),
                ).context("cannot construct ClickHouse Parquet schema decoder")
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
        let query = format!("{} FORMAT Parquet", super::connector::snapshot_query(table));
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

fn parquet_input_schema(table: &DiscoveredTable) -> anyhow::Result<Arc<Schema>> {
    Ok(Arc::new(Schema::new(
        table
            .schema
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let data_type = if is_string_conversion(column) || table.physical_system_columns
                    .iter().any(|system| system.index == index && system.kind == SystemColumnKind::Topic)
                {
                    DataType::Binary
                } else {
                    EnumTransport::for_column(column)
                        .and_then(|plan| plan.parquet_type(&column.data_type))
                        .with_context(|| format!(
                            "ClickHouse Parquet source table '{}.{}' column '{}' has an invalid transport schema",
                            table.config.database, table.config.name, column.name,
                        ))?
                };
                Ok(Field::new(&column.name, parquet_input_type(&data_type), column.nullable)
                    .with_metadata(column.arrow_metadata()))
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
    )))
}

pub(super) fn parquet_input_type(data_type: &DataType) -> DataType {
    let field = |field: &Arc<Field>| Arc::new(
        field.as_ref().clone().with_data_type(parquet_input_type(field.data_type())),
    );
    match data_type {
        DataType::Timestamp(unit, _) => DataType::Timestamp(
            match unit {
                arrow::datatypes::TimeUnit::Second => arrow::datatypes::TimeUnit::Millisecond,
                unit => *unit,
            },
            Some(Arc::from("UTC")),
        ),
        DataType::List(item) => DataType::List(field(item)),
        DataType::LargeList(item) => DataType::LargeList(field(item)),
        DataType::FixedSizeList(item, length) => DataType::FixedSizeList(field(item), *length),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        DataType::Map(entries, sorted) => DataType::Map(field(entries), *sorted),
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(parquet_input_type(value)))
        }
        data_type => data_type.clone(),
    }
}

pub(super) fn validate_parquet_input_schema(
    actual: &Schema,
    expected: &Schema,
) -> anyhow::Result<Arc<Schema>> {
    anyhow::ensure!(actual.fields().len() == expected.fields().len(),
        "ClickHouse Parquet schema drifted: discovered {} columns, query returned {}",
        expected.fields().len(), actual.fields().len(),
    );
    let fields = actual.fields().iter().zip(expected.fields()).map(|(actual, expected)| {
        parquet_input_field(actual, expected, expected.name(), false)
    }).collect::<anyhow::Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn parquet_input_field(
    actual: &Field,
    expected: &Field,
    path: &str,
    structural_name: bool,
) -> anyhow::Result<Arc<Field>> {
    anyhow::ensure!(
        (structural_name || actual.name() == expected.name())
            && actual.is_nullable() == expected.is_nullable(),
        "ClickHouse Parquet column '{path}' schema drifted: discovered '{} nullable={}', query returned '{} nullable={}'",
        expected.name(), expected.is_nullable(), actual.name(), actual.is_nullable(),
    );
    let data_type = parquet_input_hint(actual.data_type(), expected.data_type(), path)?;
    Ok(Arc::new(expected.clone().with_name(actual.name()).with_data_type(data_type)))
}

fn parquet_input_hint(actual: &DataType, expected: &DataType, path: &str) -> anyhow::Result<DataType> {
    if actual == expected {
        return Ok(expected.clone());
    }
    if let DataType::Dictionary(key, value) = expected {
        let actual = match actual {
            DataType::Dictionary(_, value) => value.as_ref(),
            actual => actual,
        };
        return Ok(DataType::Dictionary(key.clone(), Box::new(parquet_input_hint(actual, value, path)?)));
    }
    Ok(match (actual, expected) {
        (DataType::List(actual), DataType::List(expected)) => {
            DataType::List(parquet_input_field(actual, expected, &format!("{path}.{}", expected.name()), true)?)
        }
        (DataType::LargeList(actual), DataType::LargeList(expected)) => {
            DataType::LargeList(parquet_input_field(actual, expected, &format!("{path}.{}", expected.name()), true)?)
        }
        (DataType::FixedSizeList(actual, actual_length), DataType::FixedSizeList(expected, expected_length))
            if actual_length == expected_length => {
                DataType::FixedSizeList(parquet_input_field(actual, expected, &format!("{path}.{}", expected.name()), true)?, *expected_length)
            }
        (DataType::Struct(actual), DataType::Struct(expected)) if actual.len() == expected.len() => {
            DataType::Struct(actual.iter().zip(expected).map(|(actual, expected)| {
                parquet_input_field(actual, expected, &format!("{path}.{}", expected.name()), false)
            }).collect::<anyhow::Result<Vec<_>>>()?.into())
        }
        (DataType::Map(actual, actual_sorted), DataType::Map(expected, expected_sorted))
            if actual_sorted == expected_sorted => {
                DataType::Map(parquet_input_field(actual, expected, &format!("{path}.{}", expected.name()), true)?, *expected_sorted)
            }
        _ => anyhow::bail!(
            "ClickHouse Parquet column '{path}' schema drifted: discovered {expected:?}, query returned {actual:?}",
        ),
    })
}
