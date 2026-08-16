use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::model::{DeliveryRecord, StoredState, STATE_VERSION};

const STATE_FILE: &str = "deliveries.json";
const LOCK_FILE: &str = ".control-plane.lock";
const STATE_GITIGNORE: &str = "*\n!.gitignore\n";
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
static FAIL_DIRECTORY_SYNC_ROOT: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);
#[cfg(test)]
static FAIL_BEFORE_RENAME_ROOT: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("delivery '{0}' does not exist")]
    NotFound(String),
    #[error("delivery '{0}' already exists")]
    AlreadyExists(String),
    #[error(
        "delivery '{id}' changed: expected record version {expected}, current record version {actual}"
    )]
    RecordVersionConflict {
        id: String,
        expected: u64,
        actual: u64,
    },
    #[error("invalid record version for delivery '{id}': expected {expected}, got {actual}")]
    InvalidRecordVersion {
        id: String,
        expected: u64,
        actual: u64,
    },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[async_trait]
pub trait DeliveryStore: Send + Sync {
    async fn list(&self) -> Result<Vec<DeliveryRecord>, StoreError>;

    async fn get(&self, id: &str) -> Result<DeliveryRecord, StoreError>;

    async fn insert(&self, delivery: DeliveryRecord) -> Result<(), StoreError>;

    async fn replace(
        &self,
        delivery: DeliveryRecord,
        expected_record_version: u64,
    ) -> Result<(), StoreError>;
}

pub struct JsonDeliveryStore {
    root: PathBuf,
    state: Mutex<StoredState>,
    _directory_lock: std::fs::File,
}

impl JsonDeliveryStore {
    pub async fn open(root: PathBuf) -> anyhow::Result<Self> {
        prepare_state_directory(&root).await?;
        let lock_root = root.clone();
        let directory_lock =
            tokio::task::spawn_blocking(move || acquire_lock(&lock_root)).await??;
        prepare_gitignore(&root).await?;
        let mut state = load_state(&root).await?;
        anyhow::ensure!(
            state.version == STATE_VERSION,
            "unsupported control-plane state version {}; expected {STATE_VERSION}",
            state.version
        );
        let mut normalized = false;
        for delivery in state.deliveries.values_mut() {
            normalized |= delivery.normalize_after_server_restart()?;
        }
        if normalized {
            persist_state(root.clone(), state.clone()).await?;
        }
        Ok(Self {
            root,
            state: Mutex::new(state),
            _directory_lock: directory_lock,
        })
    }

    async fn persist_candidate(&self, candidate: StoredState) -> Result<(), PersistError> {
        persist_state(self.root.clone(), candidate).await
    }
}

#[async_trait]
impl DeliveryStore for JsonDeliveryStore {
    async fn list(&self) -> Result<Vec<DeliveryRecord>, StoreError> {
        Ok(self
            .state
            .lock()
            .await
            .deliveries
            .values()
            .cloned()
            .collect())
    }

    async fn get(&self, id: &str) -> Result<DeliveryRecord, StoreError> {
        self.state
            .lock()
            .await
            .deliveries
            .get(id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(id.to_owned()))
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the state lock is the transaction lock and must serialize persistence"
    )]
    async fn insert(&self, delivery: DeliveryRecord) -> Result<(), StoreError> {
        let mut state = self.state.lock().await;
        if state.deliveries.contains_key(&delivery.id) {
            return Err(StoreError::AlreadyExists(delivery.id));
        }
        if delivery.record_version != 1 {
            return Err(StoreError::InvalidRecordVersion {
                id: delivery.id,
                expected: 1,
                actual: delivery.record_version,
            });
        }
        let mut candidate = state.clone();
        candidate.deliveries.insert(delivery.id.clone(), delivery);
        apply_persist_result(
            &mut state,
            candidate.clone(),
            self.persist_candidate(candidate).await,
        )
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "the state lock is the transaction lock and must serialize CAS persistence"
    )]
    async fn replace(
        &self,
        delivery: DeliveryRecord,
        expected_record_version: u64,
    ) -> Result<(), StoreError> {
        let mut state = self.state.lock().await;
        let current = state
            .deliveries
            .get(&delivery.id)
            .ok_or_else(|| StoreError::NotFound(delivery.id.clone()))?;
        if current.record_version != expected_record_version {
            return Err(StoreError::RecordVersionConflict {
                id: delivery.id,
                expected: expected_record_version,
                actual: current.record_version,
            });
        }
        let next_record_version = expected_record_version
            .checked_add(1)
            .ok_or_else(|| StoreError::Internal(anyhow::anyhow!("record version overflow")))?;
        if delivery.record_version != next_record_version {
            return Err(StoreError::InvalidRecordVersion {
                id: delivery.id,
                expected: next_record_version,
                actual: delivery.record_version,
            });
        }
        let mut candidate = state.clone();
        candidate.deliveries.insert(delivery.id.clone(), delivery);
        apply_persist_result(
            &mut state,
            candidate.clone(),
            self.persist_candidate(candidate).await,
        )
    }
}

fn apply_persist_result(
    state: &mut StoredState,
    candidate: StoredState,
    result: Result<(), PersistError>,
) -> Result<(), StoreError> {
    match result {
        Ok(()) => {
            *state = candidate;
            Ok(())
        }
        Err(error) if error.committed => {
            *state = candidate;
            tracing::error!(
                error = ?error.source,
                "state was committed, but crash-durability could not be confirmed"
            );
            Ok(())
        }
        Err(error) => Err(StoreError::Internal(error.source)),
    }
}

async fn prepare_state_directory(root: &Path) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(root).await?;
    set_directory_permissions(root).await
}

async fn prepare_gitignore(root: &Path) -> anyhow::Result<()> {
    let ignore_path = root.join(".gitignore");
    match tokio::fs::write(&ignore_path, STATE_GITIGNORE).await {
        Ok(()) => set_file_permissions(&ignore_path).await,
        Err(error) => Err(error.into()),
    }
}

fn acquire_lock(root: &Path) -> anyhow::Result<std::fs::File> {
    let path = root.join(LOCK_FILE);
    let lock = secure_open(&path)?;
    lock.try_lock().map_err(|error| {
        anyhow::anyhow!(
            "control-plane state directory '{}' is already in use: {error}",
            root.display()
        )
    })?;
    Ok(lock)
}

async fn load_state(root: &Path) -> anyhow::Result<StoredState> {
    let path = root.join(STATE_FILE);
    match tokio::fs::read(&path).await {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(anyhow::Error::from)
            .map_err(|error| error.context("invalid control-plane state")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(StoredState::default()),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{source}")]
struct PersistError {
    #[source]
    source: anyhow::Error,
    committed: bool,
}

impl PersistError {
    fn before_commit(source: impl Into<anyhow::Error>) -> Self {
        Self {
            source: source.into(),
            committed: false,
        }
    }

    fn after_commit(source: impl Into<anyhow::Error>) -> Self {
        Self {
            source: source.into(),
            committed: true,
        }
    }
}

async fn persist_state(root: PathBuf, state: StoredState) -> Result<(), PersistError> {
    tokio::task::spawn_blocking(move || persist_state_blocking(&root, &state))
        .await
        .map_err(PersistError::before_commit)?
}

fn persist_state_blocking(root: &Path, state: &StoredState) -> Result<(), PersistError> {
    let bytes = serde_json::to_vec_pretty(state).map_err(PersistError::before_commit)?;
    let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = root.join(format!(
        ".{STATE_FILE}.{}.{}.tmp",
        std::process::id(),
        sequence
    ));
    let destination = root.join(STATE_FILE);
    let result = (|| {
        let directory = std::fs::File::open(root).map_err(PersistError::before_commit)?;
        let mut file = secure_file(&temporary).map_err(PersistError::before_commit)?;
        file.write_all(&bytes)
            .map_err(PersistError::before_commit)?;
        file.sync_all().map_err(PersistError::before_commit)?;

        #[cfg(test)]
        if take_failure_target(&FAIL_BEFORE_RENAME_ROOT, root) {
            return Err(PersistError::before_commit(anyhow::anyhow!(
                "injected pre-rename failure"
            )));
        }

        std::fs::rename(&temporary, &destination).map_err(PersistError::before_commit)?;

        #[cfg(test)]
        if take_failure_target(&FAIL_DIRECTORY_SYNC_ROOT, root) {
            return Err(PersistError::after_commit(anyhow::anyhow!(
                "injected directory sync failure"
            )));
        }

        directory.sync_all().map_err(PersistError::after_commit)
    })();
    if result.as_ref().is_err_and(|error| !error.committed) {
        let _ignored = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
fn take_failure_target(target: &std::sync::Mutex<Option<PathBuf>>, root: &Path) -> bool {
    let mut target = target.lock().expect("store fault-injection lock poisoned");
    if target.as_deref() == Some(root) {
        target.take();
        drop(target);
        return true;
    }
    drop(target);
    false
}

fn secure_file(path: &Path) -> anyhow::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}

fn secure_open(path: &Path) -> anyhow::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}

async fn set_directory_permissions(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).await?;
    }
    Ok(())
}

async fn set_file_permissions(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/store.rs"]
mod tests;
