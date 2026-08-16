use super::*;

#[test]
fn commits_only_the_contiguous_completed_prefix() -> anyhow::Result<()> {
    let mut tracker = DeliveryTracker::new();
    tracker.accept(DeliveryId::new(1), 2, 3)?;
    tracker.accept(DeliveryId::new(2), 1, 5)?;
    tracker.complete(DeliveryId::new(2), 1)?;
    assert!(tracker.take_committed().is_none());

    tracker.complete(DeliveryId::new(1), 2)?;
    let committed = tracker.take_committed().expect("completed prefix");
    assert_eq!(committed.through, DeliveryId::new(2));
    assert_eq!(committed.source_messages, 8);
    assert!(tracker.is_empty());
    Ok(())
}
