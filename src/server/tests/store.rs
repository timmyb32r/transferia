use super::*;
use crate::server::model::{RuntimeState, ValidationState};

fn record(id: &str) -> DeliveryRecord {
    DeliveryRecord {
        id: id.to_owned(),
        name: "test".to_owned(),
        description: String::new(),
        config: serde_json::json!({}),
        revision: 1,
        validation: ValidationState::Draft,
        runtime: RuntimeState::Stopped,
        created_at_ms: 1,
        updated_at_ms: 1,
    }
}

#[tokio::test]
async fn failed_revision_does_not_change_memory_or_disk() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!(
        "transferia-store-test-{}-{}",
        std::process::id(),
        TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let store = JsonDeliveryStore::open(root.clone()).await?;
    store.insert(record("one")).await?;
    let mut changed = record("one");
    changed.name = "changed".to_owned();

    assert!(matches!(
        store.replace(changed, 99).await,
        Err(StoreError::RevisionConflict { .. })
    ));
    assert_eq!(store.get("one").await?.name, "test");
    let reopened = JsonDeliveryStore::open(root.clone()).await?;
    assert_eq!(reopened.get("one").await?.name, "test");
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn state_directory_and_file_are_private() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let root = std::env::temp_dir().join(format!(
        "transferia-store-permissions-{}-{}",
        std::process::id(),
        TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let store = JsonDeliveryStore::open(root.clone()).await?;
    store.insert(record("one")).await?;

    assert_eq!(
        tokio::fs::metadata(&root).await?.permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        tokio::fs::metadata(root.join(STATE_FILE))
            .await?
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        tokio::fs::read_to_string(root.join(".gitignore")).await?,
        STATE_GITIGNORE
    );
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn reopening_normalizes_running_workers_to_stopped() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!(
        "transferia-store-restart-{}-{}",
        std::process::id(),
        TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let store = JsonDeliveryStore::open(root.clone()).await?;
    let mut running = record("one");
    running.runtime = RuntimeState::Running { pid: 42 };
    store.insert(running).await?;
    drop(store);

    let reopened = JsonDeliveryStore::open(root.clone()).await?;
    assert_eq!(reopened.get("one").await?.runtime, RuntimeState::Stopped);
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn corrupt_state_fails_closed() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!(
        "transferia-store-corrupt-{}-{}",
        std::process::id(),
        TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    tokio::fs::create_dir_all(&root).await?;
    tokio::fs::write(root.join(STATE_FILE), b"not json").await?;
    assert!(JsonDeliveryStore::open(root.clone()).await.is_err());
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}
