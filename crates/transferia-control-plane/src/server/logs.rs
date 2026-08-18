use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

use super::service::ServiceError;

pub const DEFAULT_LOG_READ_BYTES: usize = 128 * 1024;
pub const MAX_LOG_READ_BYTES: usize = 256 * 1024;

#[derive(Clone)]
pub struct WorkerLogReader {
    runs_dir: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerLogEntry {
    pub worker_id: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerLogChunk {
    pub text: String,
    pub start_offset: u64,
    pub next_offset: u64,
    pub end_offset: u64,
    pub truncated_before: bool,
}

impl WorkerLogReader {
    pub fn new(state_dir: &Path) -> Self {
        Self {
            runs_dir: state_dir.join("runs"),
        }
    }

    pub async fn list(&self, delivery_id: &str) -> Result<Vec<WorkerLogEntry>, ServiceError> {
        validate_identifier("delivery id", delivery_id)?;
        let delivery_dir = self.runs_dir.join(delivery_id);
        let mut directory = match tokio::fs::read_dir(delivery_dir).await {
            Ok(directory) => directory,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(ServiceError::Internal(error.into())),
        };
        let mut logs = Vec::new();
        while let Some(entry) = directory.next_entry().await.map_err(anyhow::Error::from)? {
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(worker_id) = name.strip_suffix(".log") else {
                continue;
            };
            if validate_identifier("worker id", worker_id).is_err() {
                continue;
            }
            let metadata = entry.metadata().await.map_err(anyhow::Error::from)?;
            if !metadata.is_file()
                || entry
                    .file_type()
                    .await
                    .map_err(anyhow::Error::from)?
                    .is_symlink()
            {
                continue;
            }
            logs.push(WorkerLogEntry {
                worker_id: worker_id.to_owned(),
                size_bytes: metadata.len(),
            });
        }
        logs.sort_by(|left, right| right.worker_id.cmp(&left.worker_id));
        Ok(logs)
    }

    pub async fn read(
        &self,
        delivery_id: &str,
        worker_id: &str,
        cursor: Option<u64>,
        requested_bytes: Option<usize>,
    ) -> Result<WorkerLogChunk, ServiceError> {
        validate_identifier("delivery id", delivery_id)?;
        validate_identifier("worker id", worker_id)?;
        let entry = self
            .list(delivery_id)
            .await?
            .into_iter()
            .find(|entry| entry.worker_id == worker_id)
            .ok_or_else(|| {
                ServiceError::NotFound(format!(
                    "worker log '{worker_id}' does not exist for delivery '{delivery_id}'"
                ))
            })?;
        let limit = requested_bytes
            .unwrap_or(DEFAULT_LOG_READ_BYTES)
            .clamp(1, MAX_LOG_READ_BYTES);
        let tail_start = entry.size_bytes.saturating_sub(limit as u64);
        let (start_offset, truncated_before) = match cursor {
            Some(cursor) if cursor <= entry.size_bytes => (cursor, false),
            Some(_) | None => (tail_start, tail_start > 0),
        };
        let bytes_to_read = (entry.size_bytes - start_offset).min(limit as u64) as usize;
        let path = self
            .runs_dir
            .join(delivery_id)
            .join(format!("{worker_id}.log"));
        let mut file = tokio::fs::File::open(path)
            .await
            .map_err(anyhow::Error::from)?;
        file.seek(std::io::SeekFrom::Start(start_offset))
            .await
            .map_err(anyhow::Error::from)?;
        let mut bytes = Vec::with_capacity(bytes_to_read);
        file.take(bytes_to_read as u64)
            .read_to_end(&mut bytes)
            .await
            .map_err(anyhow::Error::from)?;
        let next_offset = start_offset + bytes.len() as u64;
        Ok(WorkerLogChunk {
            text: redact_log_text(&String::from_utf8_lossy(&bytes)),
            start_offset,
            next_offset,
            end_offset: entry.size_bytes,
            truncated_before,
        })
    }
}

fn validate_identifier(kind: &str, value: &str) -> Result<(), ServiceError> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Ok(());
    }
    Err(ServiceError::InvalidInput(format!("invalid {kind}")))
}

fn redact_log_text(text: &str) -> String {
    text.lines()
        .map(redact_log_line)
        .collect::<Vec<_>>()
        .join("\n")
        + if text.ends_with('\n') { "\n" } else { "" }
}

fn redact_log_line(line: &str) -> String {
    const SENSITIVE_KEYS: [&str; 5] =
        ["password", "token", "secret", "authorization", "credential"];
    let lower = line.to_ascii_lowercase();
    let sensitive_value_start = SENSITIVE_KEYS
        .iter()
        .filter_map(|key| {
            let key_start = lower.find(key)?;
            let after_key = key_start + key.len();
            let separator = line[after_key..].find(['=', ':'])?;
            (separator <= 2).then_some(after_key + separator + 1)
        })
        .min();
    sensitive_value_start.map_or_else(
        || line.to_owned(),
        |value_start| format!("{}[REDACTED]", &line[..value_start]),
    )
}

#[cfg(test)]
#[path = "tests/logs.rs"]
mod tests;
