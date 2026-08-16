use super::*;

#[test]
fn parent_tokens_are_random_and_not_empty() -> anyhow::Result<()> {
    let first = random_token()?;
    let second = random_token()?;
    assert_eq!(first.len(), 64);
    assert_ne!(first, second);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn resolved_worker_config_is_private_and_removed_by_its_guard() -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let path = std::env::temp_dir().join(format!(
        "transferia-worker-config-{}-{}",
        std::process::id(),
        random_token()?
    ));
    let guard = TemporaryConfig::new(path.clone());
    secure_write(&path, b"secret: value\n").await?;
    assert_eq!(
        tokio::fs::metadata(&path).await?.permissions().mode() & 0o777,
        0o600
    );
    drop(guard);
    assert!(!path.exists());
    Ok(())
}

#[test]
fn startup_removes_only_stale_resolved_configs() -> anyhow::Result<()> {
    let root = std::env::temp_dir().join(format!(
        "transferia-worker-cleanup-{}-{}",
        std::process::id(),
        random_token()?
    ));
    let runs = root.join("runs");
    std::fs::create_dir_all(&runs)?;
    std::fs::write(runs.join("stale.yaml"), "secret")?;
    std::fs::write(runs.join("worker.log"), "diagnostics")?;

    cleanup_stale_worker_configs(&root)?;

    assert!(!runs.join("stale.yaml").exists());
    assert!(runs.join("worker.log").exists());
    std::fs::remove_dir_all(root)?;
    Ok(())
}

fn test_supervisor() -> anyhow::Result<LocalWorkerSupervisor> {
    let state_dir = std::env::temp_dir().join(format!(
        "transferia-supervisor-test-{}-{}",
        std::process::id(),
        random_token()?
    ));
    Ok(LocalWorkerSupervisor::new(
        PathBuf::from("unused-worker"),
        state_dir,
    ))
}

fn test_handle(
    run_id: &RunId,
) -> (
    WorkerHandle,
    CancellationToken,
    watch::Sender<Option<Result<(), String>>>,
) {
    let cancellation = CancellationToken::new();
    let (completion, receiver) = watch::channel(None);
    (
        WorkerHandle {
            run_id: run_id.clone(),
            cancellation: cancellation.clone(),
            completion: receiver,
        },
        cancellation,
        completion,
    )
}

#[tokio::test]
async fn shutdown_cancels_workers_that_are_still_starting() -> anyhow::Result<()> {
    let supervisor = test_supervisor()?;
    let run_id = RunId("starting-run".to_owned());
    let (handle, cancellation, completion) = test_handle(&run_id);
    supervisor
        .workers
        .lock()
        .await
        .insert("delivery".to_owned(), handle);
    let observed = cancellation.clone();
    tokio::spawn(async move {
        observed.cancelled().await;
        completion.send_replace(Some(Ok(())));
    });

    supervisor.shutdown_all().await?;

    assert!(cancellation.is_cancelled());
    assert!(matches!(
        supervisor
            .start("other", &run_id, "config", "composition")
            .await,
        Err(SupervisorError::ShuttingDown)
    ));
    Ok(())
}

#[tokio::test]
async fn shutdown_reports_workers_that_could_not_be_stopped() -> anyhow::Result<()> {
    let supervisor = test_supervisor()?;
    let run_id = RunId("failed-shutdown".to_owned());
    let (handle, cancellation, completion) = test_handle(&run_id);
    supervisor
        .workers
        .lock()
        .await
        .insert("delivery".to_owned(), handle);
    tokio::spawn(async move {
        cancellation.cancelled().await;
        completion.send_replace(Some(Err("kill failed".to_owned())));
    });

    let error = supervisor
        .shutdown_all()
        .await
        .err()
        .context("shutdown failure must be returned")?;

    assert!(matches!(error, SupervisorError::Stop(message) if message.contains("kill failed")));
    Ok(())
}

#[tokio::test]
async fn dropping_the_start_wait_cancels_startup() -> anyhow::Result<()> {
    let cancellation = CancellationToken::new();
    let (_sender, receiver) = oneshot::channel();
    let wait = StartupWait {
        receiver,
        cancellation: cancellation.clone(),
        completed: false,
    };
    let task = tokio::spawn(wait.wait());
    tokio::task::yield_now().await;
    task.abort();
    let _ignored = task.await;

    assert!(cancellation.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn stop_failure_is_reported_deterministically() -> anyhow::Result<()> {
    let supervisor = test_supervisor()?;
    let run_id = RunId("failed-stop".to_owned());
    let (handle, cancellation, completion) = test_handle(&run_id);
    supervisor
        .workers
        .lock()
        .await
        .insert("delivery".to_owned(), handle);
    tokio::spawn(async move {
        cancellation.cancelled().await;
        completion.send_replace(Some(Err("kill failed".to_owned())));
    });

    let error = supervisor
        .stop("delivery", &run_id)
        .await
        .err()
        .context("stop failure must be returned")?;

    assert!(matches!(error, SupervisorError::Stop(message) if message == "kill failed"));
    Ok(())
}

#[tokio::test]
async fn stop_rejects_a_stale_run_id_without_cancelling_the_worker() -> anyhow::Result<()> {
    let supervisor = test_supervisor()?;
    let current_run = RunId("current".to_owned());
    let (handle, cancellation, _completion) = test_handle(&current_run);
    supervisor
        .workers
        .lock()
        .await
        .insert("delivery".to_owned(), handle);

    assert!(matches!(
        supervisor
            .stop("delivery", &RunId("stale".to_owned()))
            .await,
        Err(SupervisorError::RunMismatch { .. })
    ));
    assert!(!cancellation.is_cancelled());
    Ok(())
}
