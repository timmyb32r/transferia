use super::*;

#[tokio::test]
async fn startup_barrier_requires_every_partition_and_reports_early_exit() {
    let cancellation = CancellationToken::new();
    let (first, first_rx) = oneshot::channel();
    let (second, second_rx) = oneshot::channel::<()>();
    first.send(()).expect("startup receiver must be alive");
    drop(second);

    let error = wait_for_partition_startup(vec![(7, first_rx), (11, second_rx)], &cancellation)
        .await
        .expect_err("all assigned partitions must cross the construction barrier");
    assert!(error.to_string().contains("partition 11"));
    assert!(error.to_string().contains("source and sink"));
}

#[tokio::test]
async fn startup_barrier_is_cancellable() {
    let cancellation = CancellationToken::new();
    let (_sender, receiver) = oneshot::channel();
    cancellation.cancel();

    let error = wait_for_partition_startup(vec![(3, receiver)], &cancellation)
        .await
        .expect_err("cancelled startup must stop waiting");
    assert!(error.to_string().contains("cancelled"));
}

#[test]
fn retryable_partition_failures_use_capped_backoff_without_exhaustion() {
    let mut policy = PartitionRestartPolicy::new();

    for expected_failure in 1..=100 {
        let (failure, delay) = policy.record_failure(false);
        assert_eq!(failure, expected_failure);
        assert!(delay <= MAX_PARTITION_RESTART_DELAY);
    }
    assert_eq!(policy.next_delay, MAX_PARTITION_RESTART_DELAY);
}

#[test]
fn durable_progress_resets_failure_streak_and_backoff() {
    let mut policy = PartitionRestartPolicy::new();
    for _ in 0..10 {
        policy.record_failure(false);
    }

    let (failure, delay) = policy.record_failure(true);

    assert_eq!(failure, 1);
    assert_eq!(delay, INITIAL_PARTITION_RESTART_DELAY);
    for expected_failure in 2..5 {
        let (failure, _) = policy.record_failure(false);
        assert_eq!(failure, expected_failure);
    }
}

#[test]
fn finite_source_completion_is_not_restarted() {
    assert!(classify_partition_completion(Ok(()), false, true).is_none());
    assert!(classify_partition_completion(Ok(()), false, false).is_some());
}
