use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_util::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::sync::Mutex;

const MAGIC: &[u8; 8] = b"TRFDUR01";
const CHECKSUM_BYTES: usize = 32;
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableValue {
    pub revision: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompareExchangeResult {
    Applied(DurableValue),
    Conflict(Option<DurableValue>),
}

/// Revisioned durable key/value storage used by providers for crash-recovery protocols.
pub trait DurableStorage: Send + Sync {
    fn read<'a>(&'a self, key: &'a str) -> BoxFuture<'a, anyhow::Result<Option<DurableValue>>>;

    fn compare_exchange<'a>(
        &'a self,
        key: &'a str,
        expected_revision: Option<u64>,
        payload: &'a [u8],
    ) -> BoxFuture<'a, anyhow::Result<CompareExchangeResult>>;
}

#[derive(Clone)]
pub struct DurableContext {
    pub delivery_id: Arc<str>,
    pub storage: Arc<dyn DurableStorage>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableStorageConfig {
    LocalFile { path: PathBuf },
}

impl DurableStorageConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::LocalFile { path } => anyhow::ensure!(
                !path.as_os_str().is_empty(),
                "durable_storage.path must not be empty"
            ),
        }
        Ok(())
    }

    pub fn build(&self, delivery_id: &str) -> anyhow::Result<DurableContext> {
        self.validate()?;
        validate_component("delivery_id", delivery_id)?;
        let storage: Arc<dyn DurableStorage> = match self {
            Self::LocalFile { path } => {
                Arc::new(LocalFileDurableStorage::new(path.join(delivery_id)))
            }
        };
        Ok(DurableContext {
            delivery_id: Arc::from(delivery_id),
            storage,
        })
    }
}

#[cfg(test)]
#[doc(hidden)]
pub mod test_support;

/// Crash-safe local implementation. File locking serializes competing processes.
pub struct LocalFileDurableStorage {
    root: PathBuf,
    operation: Mutex<()>,
}

impl LocalFileDurableStorage {
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            operation: Mutex::new(()),
        }
    }

    fn path(&self, key: &str) -> anyhow::Result<PathBuf> {
        let mut path = self.root.clone();
        for component in key.split('/') {
            validate_component("durable-storage key component", component)?;
            path.push(component);
        }
        Ok(path.with_extension("state"))
    }
}

impl DurableStorage for LocalFileDurableStorage {
    fn read<'a>(&'a self, key: &'a str) -> BoxFuture<'a, anyhow::Result<Option<DurableValue>>> {
        Box::pin(async move {
            let _operation = self.operation.lock().await;
            let path = self.path(key)?;
            let root = self.root.clone();
            tokio::task::spawn_blocking(move || with_file_lock(&root, || read_file(&path))).await?
        })
    }

    fn compare_exchange<'a>(
        &'a self,
        key: &'a str,
        expected_revision: Option<u64>,
        payload: &'a [u8],
    ) -> BoxFuture<'a, anyhow::Result<CompareExchangeResult>> {
        Box::pin(async move {
            let _operation = self.operation.lock().await;
            let path = self.path(key)?;
            let root = self.root.clone();
            let payload = payload.to_vec();
            tokio::task::spawn_blocking(move || {
                with_file_lock(&root, || {
                    let current = read_file(&path)?;
                    if current.as_ref().map(|value| value.revision) != expected_revision {
                        return Ok(CompareExchangeResult::Conflict(current));
                    }
                    let revision = expected_revision.map_or(0, |value| value.saturating_add(1));
                    let value = DurableValue { revision, payload };
                    write_file(&path, &value)?;
                    Ok(CompareExchangeResult::Applied(value))
                })
            })
            .await?
        })
    }
}

fn with_file_lock<T>(
    root: &Path,
    operation: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    std::fs::create_dir_all(root)?;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(root.join(".lock"))?;
    lock.lock()?;
    let result = operation();
    lock.unlock()?;
    result
}

fn validate_component(label: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty() && value.len() <= 128,
        "{label} must contain 1..=128 ASCII bytes"
    );
    anyhow::ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "{label} must contain only ASCII letters, digits, '-', '_' or '.'"
    );
    Ok(())
}

fn read_file(path: &Path) -> anyhow::Result<Option<DurableValue>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        bytes.len() >= MAGIC.len() + 16 + CHECKSUM_BYTES && bytes.starts_with(MAGIC),
        "durable state '{}' has an invalid header",
        path.display()
    );
    let revision = u64::from_le_bytes(bytes[8..16].try_into()?);
    let payload_len = usize::try_from(u64::from_le_bytes(bytes[16..24].try_into()?))?;
    let payload_end = 24_usize
        .checked_add(payload_len)
        .ok_or_else(|| anyhow::anyhow!("durable state payload length overflow"))?;
    anyhow::ensure!(
        payload_end + CHECKSUM_BYTES == bytes.len(),
        "durable state '{}' has an invalid payload length",
        path.display()
    );
    let checksum = Sha256::digest(&bytes[..payload_end]);
    anyhow::ensure!(
        checksum.as_slice() == &bytes[payload_end..],
        "durable state '{}' checksum mismatch",
        path.display()
    );
    Ok(Some(DurableValue {
        revision,
        payload: bytes[24..payload_end].to_vec(),
    }))
}

fn write_file(path: &Path, value: &DurableValue) -> anyhow::Result<()> {
    use std::io::Write as _;

    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("durable state path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = parent.join(format!(
        ".{}.{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("state"),
        std::process::id(),
        sequence
    ));
    let mut bytes = Vec::with_capacity(24 + value.payload.len() + CHECKSUM_BYTES);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&value.revision.to_le_bytes());
    bytes.extend_from_slice(&u64::try_from(value.payload.len())?.to_le_bytes());
    bytes.extend_from_slice(&value.payload);
    let checksum = Sha256::digest(&bytes);
    bytes.extend_from_slice(&checksum);

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
