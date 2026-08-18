use super::*;

fn test_root() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "transferia-log-reader-{}-{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ))
}

#[tokio::test]
async fn lists_only_delivery_owned_regular_log_files() -> anyhow::Result<()> {
    let root = test_root();
    let runs = root.join("runs");
    tokio::fs::create_dir_all(runs.join("delivery-1")).await?;
    tokio::fs::create_dir_all(runs.join("delivery-2")).await?;
    tokio::fs::write(runs.join("delivery-1/worker-a.log"), "ok").await?;
    tokio::fs::write(runs.join("delivery-2/worker-b.log"), "other").await?;
    tokio::fs::write(runs.join("delivery-1/worker-a.yaml"), "config").await?;

    let logs = WorkerLogReader::new(&root).list("delivery-1").await?;
    assert_eq!(
        logs,
        vec![WorkerLogEntry {
            worker_id: "worker-a".to_owned(),
            size_bytes: 2,
        }]
    );
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn tails_bounded_logs_and_redacts_credentials() -> anyhow::Result<()> {
    let root = test_root();
    let runs = root.join("runs");
    tokio::fs::create_dir_all(runs.join("delivery-1")).await?;
    tokio::fs::write(
        runs.join("delivery-1/worker-a.log"),
        "old line\npassword=hunter2 diagnostic\ntoken:abc123\nlast line\n",
    )
    .await?;

    let chunk = WorkerLogReader::new(&root)
        .read("delivery-1", "worker-a", Some(0), Some(1024))
        .await?;
    assert!(chunk.text.contains("password=[REDACTED]"));
    assert!(chunk.text.contains("token:[REDACTED]"));
    assert!(!chunk.text.contains("hunter2"));
    assert!(!chunk.text.contains("abc123"));
    assert_eq!(chunk.next_offset, chunk.end_offset);

    let tail = WorkerLogReader::new(&root)
        .read("delivery-1", "worker-a", None, Some(9))
        .await?;
    assert!(tail.truncated_before);
    assert!(tail.text.ends_with("ast line\n"));
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}

#[tokio::test]
async fn rejects_path_traversal_identifiers() {
    let error = WorkerLogReader::new(std::path::Path::new("unused"))
        .read("delivery-1", "../state", None, None)
        .await
        .expect_err("path traversal must be rejected");
    assert!(matches!(error, ServiceError::InvalidInput(_)));
}

#[tokio::test]
async fn clamps_untrusted_read_limits() -> anyhow::Result<()> {
    let root = test_root();
    let runs = root.join("runs/delivery-1");
    tokio::fs::create_dir_all(&runs).await?;
    tokio::fs::write(
        runs.join("worker-a.log"),
        vec![b'x'; MAX_LOG_READ_BYTES + 1024],
    )
    .await?;

    let chunk = WorkerLogReader::new(&root)
        .read("delivery-1", "worker-a", Some(0), Some(usize::MAX))
        .await?;
    assert_eq!(chunk.text.len(), MAX_LOG_READ_BYTES);
    assert_eq!(chunk.next_offset, MAX_LOG_READ_BYTES as u64);
    tokio::fs::remove_dir_all(root).await?;
    Ok(())
}
