//! S3 snapshot source — reads JSON-line files from S3-compatible object stores.
//!
//! Streaming architecture:
//!   list objects → open file → read chunk (16 MiB) → `safe_split_at` →
//!   `split_into_records` → `Vec<Message>` → `ReadResult::Batch`.
//!
//! Chunk boundaries never split records. Remainder after the last delimiter
//! is carried to the next chunk. EOF remainder (no trailing `\n`) is emitted
//! as a final record when the file ends.
//!
//! Zero-copy: `Bytes::slice` from the chunk buffer — no per-record memcpy.

use alloc::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use futures_util::{StreamExt as _, TryStreamExt as _};
use object_store::{GetResult, ObjectStore};

use crate::parsers::json_parser::{ChunkSplitter, JsonParserConfig};
use crate::pipeline::source::{CommitMarker, ReadResult, Source};
use crate::providers::s3::config::S3SourceConfig;
use crate::types::message::{Message, MessageBatch};

// ---------------------------------------------------------------------------
// Error classification
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum S3ReadError {
    /// Network, S3 API, stream interruption — retry with backoff.
    Transport {
        op: &'static str,
        file: Arc<str>,
        source: anyhow::Error,
    },
    /// Bad / corrupt / too-large record — non-retryable, abort snapshot.
    Data { file: Arc<str>, reason: String },
    /// File vanished between list and read — abort (incomplete snapshot).
    NoSuchFile { file: Arc<str> },
}

impl core::fmt::Display for S3ReadError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match *self {
            Self::Transport {
                op,
                ref file,
                ref source,
            } => {
                write!(f, "S3 transport {op} {file}: {source}")
            }
            Self::Data {
                ref file,
                ref reason,
            } => write!(f, "S3 data error {file}: {reason}"),
            Self::NoSuchFile { ref file } => write!(f, "S3 NoSuchFile: {file}"),
        }
    }
}

#[expect(
    clippy::missing_trait_methods,
    reason = "`provide` and `type_id` require unstable features (`error_generic_member_access`, `error_type_id`) and cannot be implemented on stable Rust"
)]
impl core::error::Error for S3ReadError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match *self {
            Self::Transport { ref source, .. } => Some(source.as_ref()),
            Self::Data { .. } | Self::NoSuchFile { .. } => None,
        }
    }

    fn description(&self) -> &str {
        match *self {
            Self::Transport { .. } => "S3 transport error",
            Self::Data { .. } => "S3 data error",
            Self::NoSuchFile { .. } => "S3 NoSuchFile error",
        }
    }

    fn cause(&self) -> Option<&dyn core::error::Error> {
        self.source()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3Retry {
    Retry,
    Return,
}

/// Single classification point — maps every `S3ReadError` to a retry decision.
#[must_use]
pub const fn classify(err: &S3ReadError) -> S3Retry {
    match *err {
        S3ReadError::Transport { .. } => S3Retry::Retry,
        S3ReadError::Data { .. } | S3ReadError::NoSuchFile { .. } => S3Retry::Return,
    }
}

#[expect(
    clippy::unseparated_literal_suffix,
    reason = "crate style: no underscore between digits and type suffix"
)]
fn backoff_ms(attempt: u32) -> core::time::Duration {
    core::time::Duration::from_millis(
        100u64
            .saturating_mul(2u64.pow(attempt.saturating_sub(1)))
            .min(5000),
    )
}

// ---------------------------------------------------------------------------
// ChunkedReader — streaming file reader with resume-offset
// ---------------------------------------------------------------------------

struct ChunkedReader {
    stream: futures_util::stream::BoxStream<'static, Result<Bytes, object_store::Error>>,
    eof: bool,
}

impl ChunkedReader {
    fn new(result: GetResult) -> Self {
        Self {
            stream: result.into_stream(),
            eof: false,
        }
    }

    /// Read enough bytes from the stream so that `buf` grows by approximately
    /// `target` bytes, or until EOF. The actual growth may exceed `target`
    /// because the underlying S3 chunks are appended whole.
    ///
    /// Returns the number of bytes added. 0 means EOF.
    async fn read_more(&mut self, buf: &mut BytesMut, target: usize) -> anyhow::Result<usize> {
        if self.eof {
            return Ok(0);
        }
        buf.reserve(target);
        let needed = target.saturating_sub(buf.len());
        let before = buf.len();
        while buf.len() - before < needed {
            match self.stream.next().await {
                Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                Some(Err(e)) => return Err(anyhow::anyhow!("S3 read: {e}")),
                None => {
                    self.eof = true;
                    break;
                }
            }
        }
        Ok(buf.len() - before)
    }
}

// ---------------------------------------------------------------------------
// S3Source
// ---------------------------------------------------------------------------

const READ_MORE_GRANULARITY: usize = 1024 * 1024; // 1 MiB — progress for giant records
const LOG_EVERY_ROWS: u64 = 10_000;

pub struct S3Source {
    store: Arc<dyn ObjectStore>,
    files: Vec<object_store::ObjectMeta>,
    current_idx: usize,
    current_reader: Option<ChunkedReader>,
    framer: ChunkSplitter,
    chunk_size: usize,
    max_retries: u32,
    partition_id: i64,
    /// Un-framed bytes: read-cursor via `BytesMut` (O(1) `split_to`).
    pending: BytesMut,
    /// Prefix of `pending` already known to contain no `\n`.
    /// `safe_split_at` only scans `pending[pending_scanned..]`.
    pending_scanned: usize,
    /// Bytes already framed into messages (for resume offset).
    framed: usize,
    /// Files fully ingested.
    files_done: usize,
    /// Total rows produced.
    rows_produced: u64,
    _config: S3SourceConfig,
}

impl S3Source {
    pub async fn new(
        config: S3SourceConfig,
        store: Arc<dyn ObjectStore>,
        partition_id: i64,
    ) -> anyhow::Result<Self> {
        let prefix = config.prefix.clone();
        let chunk_size = config.chunk_size_bytes;
        let max_retries = config.max_retries;

        let parser_cfg: JsonParserConfig =
            serde_yaml::from_value(config.parser.parser.raw()?.clone())?;
        let framer = parser_cfg.chunk_splitter;

        let mut files: Vec<object_store::ObjectMeta> = store
            .list(Some(&object_store::path::Path::from(prefix.as_str())))
            .try_collect()
            .await
            .map_err(|e| anyhow::anyhow!("S3 list '{prefix}' failed: {e}"))?;
        files.sort_by(|a, b| a.location.cmp(&b.location));
        if files.is_empty() {
            anyhow::bail!(
                "S3 source: no objects found under prefix '{prefix}' — refusing empty snapshot"
            );
        }
        tracing::info!(
            "S3 source: {} objects under '{}' (chunk={}MiB retries={})",
            files.len(),
            prefix,
            chunk_size / (1024 * 1024),
            max_retries,
        );
        Ok(Self {
            store,
            files,
            current_idx: 0,
            current_reader: None,
            framer,
            chunk_size,
            max_retries,
            partition_id,
            pending: BytesMut::new(),
            pending_scanned: 0,
            framed: 0,
            files_done: 0,
            rows_produced: 0,
            _config: config,
        })
    }

    /// Open the current file. On retry (resume > 0), uses `GetOptions.range`
    /// to avoid re-reading already-framed bytes.
    async fn open_current(&mut self) -> Result<(), S3ReadError> {
        let meta = &self.files[self.current_idx];
        let path = meta.location.clone();
        let resume = self.framed + self.pending.len();

        let get = if resume > 0 {
            tracing::debug!("S3: resuming {} at offset {}", path, resume,);
            let opts = object_store::GetOptions {
                range: Some(object_store::GetRange::from(resume as u64..)),
                ..Default::default()
            };
            self.store.get_opts(&path, opts).await
        } else {
            self.store.get(&path).await
        };

        match get {
            Ok(r) => {
                self.current_reader = Some(ChunkedReader::new(r));
                Ok(())
            }
            Err(object_store::Error::NotFound { .. }) => Err(S3ReadError::NoSuchFile {
                file: path.to_string().into(),
            }),
            Err(e) => Err(S3ReadError::Transport {
                op: "get",
                file: path.to_string().into(),
                source: anyhow::anyhow!(e),
            }),
        }
    }

    /// Helper: maps the outcome of `try_read_records` into a `ReadResult`.
    fn outcome_to_result(messages: Option<Vec<Message>>, partition_id: i64) -> ReadResult {
        messages.map_or_else(
            || ReadResult::Exhausted,
            |msgs| {
                ReadResult::Batch(MessageBatch {
                    messages: msgs,
                    partition_id,
                    commit_marker: None,
                    memory: Vec::new(),
                })
            },
        )
    }

    /// Core loop: read chunks until we have ≥1 framed record, or EOF.
    /// Returns `None` when all files are exhausted.
    async fn try_read_records(&mut self) -> Result<Option<Vec<Message>>, S3ReadError> {
        loop {
            if self.current_reader.is_none() {
                if self.current_idx >= self.files.len() {
                    return Ok(None);
                }
                self.framed = 0;
                self.pending.clear();
                self.pending_scanned = 0;
                self.open_current().await?;
            }

            let file: Arc<str> = self.files[self.current_idx].location.to_string().into();
            let reader = self
                .current_reader
                .as_mut()
                .ok_or_else(|| S3ReadError::Data {
                    file,
                    reason: "reader missing after open".into(),
                })?;
            let boundary = if reader.eof {
                self.pending.len() // EOF: emit remainder as final record
            } else if self.pending_scanned < self.pending.len() {
                // Incremental scan: only check the new tail since last scan.
                let tail = &self.pending[self.pending_scanned..];
                if let Some(i) = tail.iter().rposition(|&b| b == b'\n') {
                    self.pending_scanned + i + 1 // boundary in full pending coords
                } else {
                    self.pending_scanned = self.pending.len();
                    0
                }
            } else {
                0 // already scanned, no boundary found
            };

            if boundary == 0 && !reader.eof {
                // No delimiter yet — need more bytes.
                let max = 2 * self.chunk_size + READ_MORE_GRANULARITY;
                if self.pending.len() > max {
                    return Err(S3ReadError::Data {
                        file: self.files[self.current_idx].location.to_string().into(),
                        reason: format!(
                            "pending buffer exceeded {} MiB without record boundary — \
                             corrupt file or single record > chunk_size",
                            max / (1024 * 1024),
                        ),
                    });
                }
                let target = if self.pending.len() < self.chunk_size {
                    self.chunk_size - self.pending.len()
                } else {
                    READ_MORE_GRANULARITY
                };
                reader
                    .read_more(&mut self.pending, target)
                    .await
                    .map_err(|e| S3ReadError::Transport {
                        op: "read",
                        file: self.files[self.current_idx].location.to_string().into(),
                        source: e,
                    })?;
                continue;
            }

            if boundary == 0 {
                // EOF + nothing pending → file done.
                self.current_reader = None;
                self.current_idx += 1;
                self.files_done += 1;
                tracing::info!(
                    "S3: file {}/{} done ({} rows so far)",
                    self.files_done,
                    self.files.len(),
                    self.rows_produced,
                );
                continue;
            }

            // Split framed bytes into records — zero-copy via Bytes::slice.
            let chunk = Bytes::from(self.pending.split_to(boundary));
            self.pending_scanned = self.pending_scanned.saturating_sub(boundary);
            let base_ptr = chunk.as_ptr() as usize;
            self.framed += boundary;

            let messages: Vec<Message> = self
                .framer
                .split_into_records(&chunk)
                .into_iter()
                .map(|rec| {
                    let offset = rec.as_ptr() as usize - base_ptr;
                    Message::new(chunk.slice(offset..offset + rec.len()))
                })
                .collect();

            // Progress logging — milestone-based.
            let n = messages.len() as u64;
            let prev_stone = self.rows_produced / LOG_EVERY_ROWS;
            self.rows_produced += n;
            let new_stone = self.rows_produced / LOG_EVERY_ROWS;
            if new_stone > prev_stone || self.rows_produced == n {
                tracing::info!(
                    "S3: {} rows produced (file {}/{})",
                    self.rows_produced,
                    self.files_done + 1,
                    self.files.len(),
                );
            }

            return Ok(Some(messages));
        }
    }
}

impl Source for S3Source {
    fn read_batch(&mut self) -> BoxFuture<'_, anyhow::Result<ReadResult>> {
        Box::pin(async move {
            let outcome = self.try_read_records().await;
            match outcome {
                Ok(messages) => Ok(Self::outcome_to_result(messages, self.partition_id)),
                Err(ref e @ S3ReadError::NoSuchFile { .. }) => {
                    tracing::error!("{e} \u{2014} file vanished, aborting snapshot");
                    Ok(ReadResult::Failed(anyhow::anyhow!("{e}")))
                }
                Err(ref first_err) => match classify(first_err) {
                    S3Retry::Retry => {
                        let mut attempt: u32 = 0;
                        let mut last_msg = format!("{first_err}");
                        loop {
                            attempt += 1;
                            if attempt > self.max_retries {
                                tracing::error!(
                                    "{} retries exhausted (last: {last_msg}); aborting snapshot",
                                    self.max_retries,
                                );
                                return Ok(ReadResult::Failed(anyhow::anyhow!("{last_msg}")));
                            }
                            tracing::warn!(
                                "attempt {}/{} \u{2014} {last_msg}",
                                attempt,
                                self.max_retries
                            );
                            tokio::time::sleep(backoff_ms(attempt)).await;
                            self.current_reader = None; // force re-open with resume offset
                            match self.try_read_records().await {
                                Ok(messages) => {
                                    return Ok(Self::outcome_to_result(
                                        messages,
                                        self.partition_id,
                                    ));
                                }
                                Err(e2) => {
                                    last_msg = format!("{e2}");
                                    match classify(&e2) {
                                        S3Retry::Retry => {}
                                        S3Retry::Return => {
                                            tracing::error!(
                                                "{e2} \u{2014} non-retryable, aborting"
                                            );
                                            return Ok(ReadResult::Failed(anyhow::anyhow!("{e2}")));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    S3Retry::Return => {
                        tracing::error!("{first_err} \u{2014} non-retryable, aborting snapshot");
                        Ok(ReadResult::Failed(anyhow::anyhow!("{first_err}")))
                    }
                },
            }
        })
    }

    fn commit_offsets<'ctx>(
        &'ctx mut self,
        _marker: &'ctx CommitMarker,
    ) -> BoxFuture<'ctx, anyhow::Result<()>> {
        Box::pin(async move { Ok(()) })
    }
}
