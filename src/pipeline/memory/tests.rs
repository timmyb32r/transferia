use super::*;

#[tokio::test]
async fn reservation_is_released_by_last_clone() {
    let memory = PipelineMemory::new(100);
    let lease = memory.reserve(60).await;
    let clone = lease.clone();
    assert_eq!(memory.used(), 60);
    assert_eq!(memory.source_used(), 60);
    assert_eq!(memory.transform_used(), 0);
    drop(lease);
    assert_eq!(memory.used(), 60);
    drop(clone);
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn oversized_allocation_is_admitted_alone() {
    let memory = PipelineMemory::new(10);
    let lease = memory.reserve(20).await;
    assert_eq!(memory.used(), 20);
    drop(lease);
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn transform_pressure_starts_at_the_limit_and_clears_on_release() {
    let memory = PipelineMemory::new(10);
    let lease = memory.reserve_transform(10);
    assert!(memory.is_transform_pressured());
    drop(lease);
    assert!(!memory.is_transform_pressured());
}

#[tokio::test]
async fn active_transform_is_admitted_once_and_converts_to_exact_output() {
    let memory = PipelineMemory::new(10);
    let lease = memory.admit_active_transform(20).await;
    assert_eq!(memory.used(), 20);
    assert_eq!(memory.transform_used(), 0);
    assert!(!memory.is_transform_pressured());
    let mut second = Box::pin(memory.admit_active_transform(1));
    assert!(
        tokio::time::timeout(core::time::Duration::from_millis(20), &mut second)
            .await
            .is_err()
    );
    let output = lease.finish(7);
    assert_eq!(memory.used(), 7);
    assert_eq!(memory.transform_used(), 7);
    drop(output);
    let second = second.await;
    drop(second);
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn active_transform_waits_at_limit_but_ignores_source_bytes() {
    let memory = PipelineMemory::new(10);
    let source = memory.reserve_progress_source(20).await;
    let retained = memory.reserve_transform(10);
    let mut active = Box::pin(memory.admit_active_transform(5));
    assert!(
        tokio::time::timeout(core::time::Duration::from_millis(20), &mut active)
            .await
            .is_err()
    );
    drop(retained);
    let active = tokio::time::timeout(core::time::Duration::from_millis(50), active)
        .await
        .expect("source accounting must not deadlock parser admission");
    drop(active);
    drop(source);
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn dropping_active_transform_releases_accounting_and_wakes_waiter() {
    let memory = PipelineMemory::new(10);
    let first = memory.admit_active_transform(7).await;
    let mut second = Box::pin(memory.admit_active_transform(3));
    assert!(
        tokio::time::timeout(core::time::Duration::from_millis(20), &mut second)
            .await
            .is_err()
    );
    drop(first);
    let second = tokio::time::timeout(core::time::Duration::from_millis(50), second)
        .await
        .expect("dropping an unfinished active reservation must wake its waiter");
    assert_eq!(memory.used(), 3);
    drop(second);
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn transform_admission_ignores_queued_source_bytes() {
    let memory = PipelineMemory::new(10);
    let source = memory.reserve(10).await;
    tokio::time::timeout(
        core::time::Duration::from_millis(50),
        memory.wait_transform_below_limit(),
    )
    .await
    .expect("source bytes must not block the parser");
    assert_eq!(memory.source_used(), 10);
    assert_eq!(memory.transform_used(), 0);
    drop(source);
}

#[tokio::test]
async fn shrinking_peak_reservation_updates_stage_and_total_usage() {
    let memory = PipelineMemory::new(100);
    let lease = memory.reserve(80).await;
    let clone = lease.clone();
    assert!(clone.shrink_to(30));
    assert_eq!(lease.bytes(), 30);
    assert_eq!(memory.used(), 30);
    assert_eq!(memory.source_used(), 30);
    assert!(!lease.shrink_to(40));
    drop(lease);
    assert_eq!(memory.used(), 30);
    drop(clone);
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn transform_usage_is_accounted_separately() {
    let memory = PipelineMemory::new(10);
    let lease = memory.reserve_transform(12);
    assert_eq!(memory.used(), 12);
    assert_eq!(memory.source_used(), 0);
    assert_eq!(memory.transform_used(), 12);
    assert!(tokio::time::timeout(
        core::time::Duration::from_millis(20),
        memory.wait_transform_below_limit(),
    )
    .await
    .is_err());
    drop(lease);
    memory.wait_transform_below_limit().await;
}

#[tokio::test]
async fn progress_source_can_cross_retained_transform_pressure() {
    let memory = PipelineMemory::new(10);
    let transform = memory.reserve_transform(10);
    let source = memory.reserve_progress_source(20).await;

    assert_eq!(memory.used(), 30);
    assert_eq!(memory.source_used(), 20);
    assert_eq!(memory.transform_used(), 10);
    drop(source);
    assert_eq!(memory.used(), 10);
    drop(transform);
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn progress_source_is_singleton_and_grows_for_overlap() {
    let memory = PipelineMemory::new(10);
    let first = memory.reserve_progress_source(20).await;
    first.grow_progress_source_to(35).unwrap();
    assert_eq!(first.bytes(), 35);
    assert_eq!(memory.source_used(), 35);

    let mut second = Box::pin(memory.reserve_progress_source(30));
    assert!(
        tokio::time::timeout(core::time::Duration::from_millis(20), &mut second)
            .await
            .is_err()
    );
    assert!(first.shrink_to(12));
    assert_eq!(memory.source_used(), 12);
    drop(first);
    let second = second.await;
    assert_eq!(second.bytes(), 30);
}
