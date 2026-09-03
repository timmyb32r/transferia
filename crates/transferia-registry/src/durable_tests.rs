use super::*;

fn temporary_root() -> PathBuf {
    std::env::temp_dir().join(format!(
        "transferia-durable-{}-{}",
        std::process::id(),
        TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

#[tokio::test]
async fn local_file_storage_survives_reopen_and_enforces_revisions() -> anyhow::Result<()> {
    let root = temporary_root();
    let first = LocalFileDurableStorage::new(root.clone());
    assert_eq!(first.read("s3/partition-0").await?, None);
    assert_eq!(
        first
            .compare_exchange("s3/partition-0", None, b"open")
            .await?,
        CompareExchangeResult::Applied(DurableValue {
            revision: 0,
            payload: b"open".to_vec()
        })
    );
    assert!(matches!(
        first
            .compare_exchange("s3/partition-0", None, b"wrong")
            .await?,
        CompareExchangeResult::Conflict(Some(DurableValue { revision: 0, .. }))
    ));
    drop(first);

    let reopened = LocalFileDurableStorage::new(root.clone());
    assert_eq!(
        reopened.read("s3/partition-0").await?,
        Some(DurableValue {
            revision: 0,
            payload: b"open".to_vec()
        })
    );
    assert!(matches!(
        reopened
            .compare_exchange("s3/partition-0", Some(0), b"closed")
            .await?,
        CompareExchangeResult::Applied(DurableValue { revision: 1, .. })
    ));
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn corrupted_state_fails_instead_of_being_ignored() -> anyhow::Result<()> {
    let root = temporary_root();
    let storage = LocalFileDurableStorage::new(root.clone());
    storage
        .compare_exchange("scope/key", None, b"value")
        .await?;
    let path = storage.path("scope/key")?;
    let mut bytes = std::fs::read(&path)?;
    *bytes.last_mut().expect("state has checksum") ^= 1;
    std::fs::write(path, bytes)?;
    let error = storage.read("scope/key").await.unwrap_err();
    assert!(error.to_string().contains("checksum mismatch"));
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn revision_overflow_fails_without_rewriting_durable_state() -> anyhow::Result<()> {
    let root = temporary_root();
    let storage = LocalFileDurableStorage::new(root.clone());
    let path = storage.path("scope/key")?;
    write_file(
        &path,
        &DurableValue {
            revision: u64::MAX,
            payload: b"original".to_vec(),
        },
    )?;

    let message = storage
        .compare_exchange("scope/key", Some(u64::MAX), b"replacement")
        .await
        .err()
        .expect("durable revision overflow must fail")
        .to_string();
    assert!(message.contains("revision overflow"), "{message}");
    assert_eq!(
        storage.read("scope/key").await?,
        Some(DurableValue {
            revision: u64::MAX,
            payload: b"original".to_vec(),
        })
    );
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn independent_instances_serialize_compare_exchange_with_a_file_lock() -> anyhow::Result<()> {
    let root = temporary_root();
    let left = LocalFileDurableStorage::new(root.clone());
    let right = LocalFileDurableStorage::new(root.clone());
    let (left, right) = tokio::join!(
        left.compare_exchange("scope/key", None, b"left"),
        right.compare_exchange("scope/key", None, b"right")
    );
    let applied = [left?, right?]
        .into_iter()
        .filter(|result| matches!(result, CompareExchangeResult::Applied(_)))
        .count();
    assert_eq!(applied, 1, "only one compare-exchange may win");
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn execution_lease_fences_independent_instances_until_guard_drop() -> anyhow::Result<()> {
    let root = temporary_root();
    let left = LocalFileDurableStorage::new(root.clone());
    let right = LocalFileDurableStorage::new(root.clone());

    let lease = left.acquire_execution_lease("postgres-source").await?;
    let error = right
        .acquire_execution_lease("postgres-source")
        .await
        .err()
        .expect("a live execution must fence a second process");
    assert!(error.to_string().contains("already owns"));

    drop(lease);
    let reacquired = right.acquire_execution_lease("postgres-source").await?;
    drop(reacquired);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn durable_contexts_isolate_delivery_state_and_share_resource_state() -> anyhow::Result<()> {
    let root = temporary_root();
    let config = DurableStorageConfig::LocalFile { path: root.clone() };
    let first = config.build("delivery-a")?;
    let second = config.build("delivery-b")?;

    first
        .storage
        .compare_exchange("postgres-offset", None, b"delivery-a")
        .await?;
    assert_eq!(second.storage.read("postgres-offset").await?, None);

    first
        .resource_storage
        .compare_exchange("postgres-resource", None, b"shared-owner")
        .await?;
    assert_eq!(
        second.resource_storage.read("postgres-resource").await?,
        Some(DurableValue {
            revision: 0,
            payload: b"shared-owner".to_vec(),
        })
    );

    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn resource_lease_fences_different_delivery_contexts_until_guard_drop() -> anyhow::Result<()>
{
    let root = temporary_root();
    let config = DurableStorageConfig::LocalFile { path: root.clone() };
    let first = config.build("delivery-a")?;
    let second = config.build("delivery-b")?;

    let lease = first
        .resource_storage
        .acquire_execution_lease("postgres-7412345678901234567-16384-shared_slot")
        .await?;
    let error = second
        .resource_storage
        .acquire_execution_lease("postgres-7412345678901234567-16384-shared_slot")
        .await
        .err()
        .expect("a resource lease must be global across delivery IDs");
    assert!(error.to_string().contains("already owns"));

    drop(lease);
    let reacquired = second
        .resource_storage
        .acquire_execution_lease("postgres-7412345678901234567-16384-shared_slot")
        .await?;
    drop(reacquired);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[test]
fn delivery_and_key_components_are_explicit_ascii_identifiers() {
    assert!(validate_component("delivery_id", "orders.eu-1").is_ok());
    assert!(validate_component("delivery_id", RESOURCE_NAMESPACE_DIRECTORY).is_err());
    assert!(validate_component("delivery_id", "orders/eu").is_err());
    assert!(validate_component("delivery_id", "заказы").is_err());
}
