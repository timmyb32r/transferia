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

#[test]
fn delivery_and_key_components_are_explicit_ascii_identifiers() {
    assert!(validate_component("delivery_id", "orders.eu-1").is_ok());
    assert!(validate_component("delivery_id", "orders/eu").is_err());
    assert!(validate_component("delivery_id", "заказы").is_err());
}
