use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_util::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

const MAGIC: &[u8; 8] = b"TRFDUR02";
const CHECKSUM_BYTES: usize = 16;
const RESOURCE_NAMESPACE_DIRECTORY: &str = "@resources";
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

/// An exclusive, crash-released lease owned by a running connector execution.
///
/// The concrete guard is storage-specific. Dropping this value must release the
/// lease before another execution can acquire the same key.
pub struct DurableLease {
    _guard: Box<dyn Send + Sync>,
}

impl DurableLease {
    #[must_use]
    pub fn new(guard: impl Send + Sync + 'static) -> Self {
        Self {
            _guard: Box::new(guard),
        }
    }
}

/// Revisioned durable key/value storage used by connectors for crash-recovery protocols.
pub trait DurableStorage: Send + Sync {
    fn read<'a>(&'a self, key: &'a str) -> BoxFuture<'a, anyhow::Result<Option<DurableValue>>>;

    fn compare_exchange<'a>(
        &'a self,
        key: &'a str,
        expected_revision: Option<u64>,
        payload: &'a [u8],
    ) -> BoxFuture<'a, anyhow::Result<CompareExchangeResult>>;

    /// Acquire an execution-scoped exclusive lease.
    ///
    /// Implementations must release the lease when the returned guard is
    /// dropped, including after process termination. Connectors use this to
    /// fence concurrent executions before either can produce sink-visible data.
    fn acquire_execution_lease<'a>(
        &'a self,
        _key: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<DurableLease>> {
        Box::pin(async {
            anyhow::bail!("configured durable storage does not support execution leases")
        })
    }
}

#[derive(Clone)]
pub struct DurableContext {
    pub delivery_id: Arc<str>,

    /// Delivery-local offsets, phases, and connector state.
    pub storage: Arc<dyn DurableStorage>,

    /// Resource ownership shared by every delivery using the configured durable root.
    pub resource_storage: Arc<dyn DurableStorage>,
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
        let resource_storage: Arc<dyn DurableStorage> = match self {
            Self::LocalFile { path } => Arc::new(LocalFileDurableStorage::new(
                path.join(RESOURCE_NAMESPACE_DIRECTORY),
            )),
        };
        Ok(DurableContext {
            delivery_id: Arc::from(delivery_id),
            storage,
            resource_storage,
        })
    }
}

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
                    let revision = match expected_revision {
                        Some(revision) => revision
                            .checked_add(1)
                            .ok_or_else(|| anyhow::anyhow!("durable revision overflow"))?,
                        None => 0,
                    };
                    let value = DurableValue { revision, payload };
                    write_file(&path, &value)?;
                    Ok(CompareExchangeResult::Applied(value))
                })
            })
            .await?
        })
    }

    fn acquire_execution_lease<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<DurableLease>> {
        Box::pin(async move {
            validate_component("durable execution lease", key)?;
            let root = self.root.clone();
            let key = key.to_owned();
            tokio::task::spawn_blocking(move || {
                std::fs::create_dir_all(&root)?;
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(root.join(format!(".{key}.lease")))?;
                file.try_lock().map_err(|error| {
                    anyhow::anyhow!("another execution already owns durable lease '{key}': {error}")
                })?;
                Ok(DurableLease::new(LocalFileLease { _file: file }))
            })
            .await?
        })
    }
}

struct LocalFileLease {
    _file: std::fs::File,
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
    let checksum = durable_checksum(&bytes[..payload_end])?;
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
    bytes.extend_from_slice(&durable_checksum(&bytes)?);

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

fn durable_checksum(bytes: &[u8]) -> anyhow::Result<[u8; CHECKSUM_BYTES]> {
    Ok(murmur3::murmur3_x64_128(&mut Cursor::new(bytes), 0)?.to_le_bytes())
}

#[cfg(test)]
#[path = "durable_tests.rs"]
mod tests;
