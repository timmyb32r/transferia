use super::*;
use crate::server::model::{RunId, RuntimeState, ValidationState};

fn record(id: &str) -> DeliveryRecord {
    DeliveryRecord {
        id: id.to_owned(),
        name: "test".to_owned(),
        description: String::new(),
        config: serde_json::json!({}),
        revision: 1,
        record_version: 1,
        validation: ValidationState::Draft,
        runtime: RuntimeState::Stopped,
        created_at_ms: 1,
        updated_at_ms: 1,
    }
}

#[tokio::test]
async fn failed_record_version_does_not_change_memory_or_disk() -> anyhow::Result<()> {
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
        Err(StoreError::RecordVersionConflict { .. })
    ));
    assert_eq!(store.get("one").await?.name, "test");
    drop(store);
    let reopened = JsonDeliveryStore::open(root.clone()).await?;
    assert_eq!(reopened.get("one").await?.name, "test");
    drop(reopened);
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
        tokio::fs::metadata(root.join(LOCK_FILE))
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
    running.runtime = RuntimeState::Running {
        run_id: RunId("run".to_owned()),
        pid: 42,
    };
    store.insert(running).await?;
    drop(store);

    let reopened = JsonDeliveryStore::open(root.clone()).await?;
    let normalized = reopened.get("one").await?;
    assert_eq!(normalized.runtime, RuntimeState::Stopped);
    assert_eq!(normalized.record_version, 2);
    drop(reopened);
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

#[tokio::test]
async fn state_directory_has_a_single_live_owner() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!(
        "transferia-store-lock-{}-{}",
        std::process::id(),
        TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let first = JsonDeliveryStore::open(root.clone()).await?;
    let error = JsonDeliveryStore::open(root.clone())
        .await
        .err()
        .expect("a second store must not acquire the state directory");
    assert!(format!("{error:#}").contains("already in use"));

    drop(first);
    let reopened = JsonDeliveryStore::open(root.clone()).await?;
    drop(reopened);
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn runtime_mutations_use_record_version_not_config_revision() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!(
        "transferia-store-record-version-{}-{}",
        std::process::id(),
        TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let store = JsonDeliveryStore::open(root.clone()).await?;
    store.insert(record("one")).await?;
    let mut changed = store.get("one").await?;
    changed.record_version = 2;
    changed.runtime = RuntimeState::Starting {
        run_id: RunId("run-1".to_owned()),
    };
    store.replace(changed, 1).await?;

    let current = store.get("one").await?;
    assert_eq!(current.revision, 1);
    assert_eq!(current.record_version, 2);
    drop(store);
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn post_commit_durability_failure_continues_from_committed_state() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!(
        "transferia-store-commit-point-{}-{}",
        std::process::id(),
        TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let store = JsonDeliveryStore::open(root.clone()).await?;
    store.insert(record("one")).await?;
    let mut changed = store.get("one").await?;
    changed.name = "committed".to_owned();
    changed.record_version = 2;
    *FAIL_DIRECTORY_SYNC_ROOT
        .lock()
        .expect("store fault-injection lock poisoned") = Some(root.clone());

    store.replace(changed, 1).await?;
    assert_eq!(store.get("one").await?.name, "committed");
    drop(store);
    let reopened = JsonDeliveryStore::open(root.clone()).await?;
    assert_eq!(reopened.get("one").await?.name, "committed");
    drop(reopened);
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn pre_commit_failure_changes_nothing_and_cleans_temporary_file() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!(
        "transferia-store-before-commit-{}-{}",
        std::process::id(),
        TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let store = JsonDeliveryStore::open(root.clone()).await?;
    store.insert(record("one")).await?;
    let mut changed = store.get("one").await?;
    changed.name = "must-not-commit".to_owned();
    changed.record_version = 2;
    *FAIL_BEFORE_RENAME_ROOT
        .lock()
        .expect("store fault-injection lock poisoned") = Some(root.clone());

    assert!(matches!(
        store.replace(changed, 1).await,
        Err(StoreError::Internal(_))
    ));
    assert_eq!(store.get("one").await?.name, "test");
    let mut entries = tokio::fs::read_dir(&root).await?;
    while let Some(entry) = entries.next_entry().await? {
        assert!(!entry.file_name().to_string_lossy().ends_with(".tmp"));
    }
    drop(store);
    let reopened = JsonDeliveryStore::open(root.clone()).await?;
    assert_eq!(reopened.get("one").await?.name, "test");
    drop(reopened);
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}
