use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use tokio::sync::Mutex;

use super::model::{DeliveryRecord, StoredState, STATE_VERSION};

const STATE_FILE: &str = "deliveries.json";
const STATE_GITIGNORE: &str = "*\n!.gitignore\n";
static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("delivery '{0}' does not exist")]
    NotFound(String),
    #[error("delivery '{0}' already exists")]
    AlreadyExists(String),
    #[error("delivery '{id}' changed: expected revision {expected}, current revision {actual}")]
    RevisionConflict {
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
        expected_revision: u64,
    ) -> Result<(), StoreError>;
}

pub struct JsonDeliveryStore {
    root: PathBuf,
    state: Mutex<StoredState>,
}

impl JsonDeliveryStore {
    pub async fn open(root: PathBuf) -> anyhow::Result<Self> {
        prepare_state_directory(&root).await?;
        let mut state = load_state(&root).await?;
        anyhow::ensure!(
            state.version == STATE_VERSION,
            "unsupported control-plane state version {}; expected {STATE_VERSION}",
            state.version
        );
        let mut normalized = false;
        for delivery in state.deliveries.values_mut() {
            normalized |= delivery.normalize_after_server_restart();
        }
        if normalized {
            persist_state(root.clone(), state.clone()).await?;
        }
        Ok(Self {
            root,
            state: Mutex::new(state),
        })
    }

    async fn persist_candidate(&self, candidate: StoredState) -> anyhow::Result<()> {
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

    async fn insert(&self, delivery: DeliveryRecord) -> Result<(), StoreError> {
        let mut state = self.state.lock().await;
        if state.deliveries.contains_key(&delivery.id) {
            return Err(StoreError::AlreadyExists(delivery.id));
        }
        let mut candidate = state.clone();
        candidate.deliveries.insert(delivery.id.clone(), delivery);
        self.persist_candidate(candidate.clone()).await?;
        *state = candidate;
        drop(state);
        Ok(())
    }

    async fn replace(
        &self,
        delivery: DeliveryRecord,
        expected_revision: u64,
    ) -> Result<(), StoreError> {
        let mut state = self.state.lock().await;
        let current = state
            .deliveries
            .get(&delivery.id)
            .ok_or_else(|| StoreError::NotFound(delivery.id.clone()))?;
        if current.revision != expected_revision {
            return Err(StoreError::RevisionConflict {
                id: delivery.id,
                expected: expected_revision,
                actual: current.revision,
            });
        }
        let mut candidate = state.clone();
        candidate.deliveries.insert(delivery.id.clone(), delivery);
        self.persist_candidate(candidate.clone()).await?;
        *state = candidate;
        drop(state);
        Ok(())
    }
}

async fn prepare_state_directory(root: &Path) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(root).await?;
    set_directory_permissions(root).await?;
    let ignore_path = root.join(".gitignore");
    match tokio::fs::write(&ignore_path, STATE_GITIGNORE).await {
        Ok(()) => set_file_permissions(&ignore_path).await,
        Err(error) => Err(error.into()),
    }
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

async fn persist_state(root: PathBuf, state: StoredState) -> anyhow::Result<()> {
    tokio::task::spawn_blocking(move || persist_state_blocking(&root, &state)).await?
}

fn persist_state_blocking(root: &Path, state: &StoredState) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec_pretty(state)?;
    let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = root.join(format!(
        ".{STATE_FILE}.{}.{}.tmp",
        std::process::id(),
        sequence
    ));
    let destination = root.join(STATE_FILE);
    let mut file = secure_file(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(&temporary, &destination)?;
    secure_existing_file(&destination)?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
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

fn secure_existing_file(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
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
