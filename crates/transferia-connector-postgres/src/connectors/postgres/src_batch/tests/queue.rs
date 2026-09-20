use std::num::NonZeroU32;
use std::time::Duration;

use super::*;
use super::super::planner::{
    chunks, decide_for_evaluation, Constraints, CostRates, OutputDistribution, PhysicalAccess, RawTableFacts,
    Strategy, TableFacts,
};

fn descriptors(parts: u32) -> Vec<Chunk> {
    let facts = TableFacts::try_from(RawTableFacts {
        physical_access: PhysicalAccess::GuardedHeap,
        native_tid_ranges: true,
        heap_pages: 1_024,
        block_bytes: 8_192,
        estimated_rows: Some(10_000.0),
        estimated_output_bytes: Some(1_000_000.0),
        metadata_seconds: 0.001,
        request_seconds: 0.001,
        indexed_boundary_seconds: 0.001,
        indexed_cuts: 0,
        index_correlation: None,
        output_distribution: OutputDistribution::UniformPrior,
        rates: CostRates::priors(),
    }).unwrap();
    let decision = decide_for_evaluation(facts, Constraints::default(), Strategy::Ctid, NonZeroU32::new(parts).unwrap()).unwrap();
    chunks(&decision).unwrap()
}

fn queue(parts: u32, lanes: u32, cap: Option<u32>) -> Arc<SnapshotQueue> {
    SnapshotQueue::new(descriptors(parts), lanes, cap.map(|value| NonZeroU32::new(value).unwrap())).unwrap()
}

fn finish(lease: &LaneLease, task: &ClaimedChunk) -> CommitMarker {
    lease.finish_read(task, Duration::from_millis(1), Duration::from_secs(1), 0).unwrap()
}

#[test]
fn construction_rejects_invalid_lane_and_part_contracts() {
    assert!(SnapshotQueue::new(vec![], 1, None).is_err());
    assert!(SnapshotQueue::new(descriptors(1), 0, None).is_err());
    assert!(SnapshotQueue::new(descriptors(1), 2, None).is_err());
    assert!(SnapshotQueue::new(descriptors(2), 1, NonZeroU32::new(1)).is_err());
}

#[test]
fn construction_rejects_cloned_omitted_or_reordered_chunks() {
    let original = descriptors(3);
    let mut repeated = original.clone();
    repeated[1] = repeated[0].clone();
    assert!(SnapshotQueue::new(repeated, 1, None).is_err());
    let mut omitted = original.clone();
    omitted.remove(1);
    assert!(SnapshotQueue::new(omitted, 1, None).is_err());
    let mut reversed = original;
    reversed.reverse();
    assert!(SnapshotQueue::new(reversed, 1, None).is_err());
}

#[test]
fn ownership_is_exclusive_and_claim_is_lazy() {
    let queue = queue(2, 2, Some(2));
    let first = queue.open_lane(0).unwrap();
    let second = queue.open_lane(1).unwrap();
    assert_eq!(queue.lock().unwrap().pending.len(), 2);
    assert!(queue.open_lane(0).is_err());
    assert!(queue.open_lane(2).is_err());
    let a = first.claim().unwrap().unwrap();
    let b = second.claim().unwrap().unwrap();
    assert_ne!(a.id, b.id);
    assert!(first.claim().is_err());
    assert!(second.claim().is_err());
}

#[test]
fn empty_task_requires_its_destination_durability_marker() {
    let queue = queue(1, 1, Some(1));
    let mut lane = queue.open_lane(0).unwrap();
    let task = lane.claim().unwrap().unwrap();
    let marker = finish(&lane, &task);
    assert!(lane.claim().unwrap().is_none());
    assert!(queue.ensure_complete().is_err());
    lane.acknowledge(&[marker]).unwrap();
    queue.ensure_complete().unwrap();
    lane.close().unwrap();
    lane.close().unwrap();
}

#[test]
fn lane_can_read_next_task_while_previous_task_waits_for_ack() {
    let queue = queue(2, 1, Some(2));
    let lane = queue.open_lane(0).unwrap();
    let first = lane.claim().unwrap().unwrap();
    let (_, rows) = lane.emit_rows(&first, 0, 3).unwrap();
    let end_first = finish(&lane, &first);
    let second = lane.claim().unwrap().unwrap();
    assert_ne!(first.id, second.id);
    assert_eq!(lane.offset().unwrap(), 3);
    let (_, more_rows) = lane.emit_rows(&second, 3, 7).unwrap();
    let end_second = finish(&lane, &second);
    lane.acknowledge(&[rows, end_first, more_rows, end_second]).unwrap();
    assert_eq!(lane.offset().unwrap(), 10);
    queue.ensure_complete().unwrap();
}

#[test]
fn acknowledgement_group_is_atomic_and_rejects_wrong_types_and_order() {
    let queue = queue(1, 1, Some(1));
    let lane = queue.open_lane(0).unwrap();
    let task = lane.claim().unwrap().unwrap();
    let (_, first) = lane.emit_rows(&task, 0, 2).unwrap();
    let (_, second) = lane.emit_rows(&task, 2, 2).unwrap();
    let end = finish(&lane, &task);
    assert!(lane.acknowledge(&[first.clone(), CommitMarker::new(123_i64)]).is_err());
    assert!(lane.acknowledge(&[first.clone(), end.clone()]).is_err());
    assert_eq!(queue.lock().unwrap().lanes[0].outstanding.len(), 3);
    lane.acknowledge(&[first.clone(), second, end]).unwrap();
    assert!(lane.acknowledge(&[first]).is_err());
    queue.ensure_complete().unwrap();
}

#[test]
fn foreign_epochs_lanes_attempts_and_forged_offsets_are_rejected() {
    let queue = queue(2, 2, Some(2));
    let lane = queue.open_lane(0).unwrap();
    let task = lane.claim().unwrap().unwrap();
    let (_, marker) = lane.emit_rows(&task, 0, 1).unwrap();
    let actual = marker.value::<SnapshotMarker>().unwrap();
    let mut invalid = actual.clone();
    invalid.epoch = Arc::new(());
    assert!(lane.acknowledge(&[CommitMarker::new(invalid)]).is_err());
    let mut invalid = actual.clone();
    invalid.lane = 1;
    assert!(lane.acknowledge(&[CommitMarker::new(invalid)]).is_err());
    let mut invalid = actual.clone();
    invalid.attempt += 1;
    assert!(lane.acknowledge(&[CommitMarker::new(invalid)]).is_err());
    let mut invalid = actual.clone();
    invalid.record.offset += 1;
    assert!(lane.acknowledge(&[CommitMarker::new(invalid)]).is_err());
    lane.acknowledge(&[marker]).unwrap();
}

#[test]
fn invalid_row_progress_does_not_mutate_publication_state() {
    let queue = queue(1, 1, Some(1));
    let lane = queue.open_lane(0).unwrap();
    let task = lane.claim().unwrap().unwrap();
    assert!(lane.emit_rows(&task, 1, 2).is_err());
    assert!(lane.emit_rows(&task, 0, 0).is_err());
    assert!(lane.emit_rows(&task, 0, u64::MAX).is_err());
    assert_eq!(lane.offset().unwrap(), 0);
    assert!(lane.retry_allowed());
}

#[test]
fn dropping_an_unpublished_task_preserves_exact_descriptor_for_retry() {
    let queue = queue(1, 1, Some(1));
    let lane = queue.open_lane(0).unwrap();
    let task = lane.claim().unwrap().unwrap();
    drop(lane);
    let retry = queue.open_lane(0).unwrap();
    let replay = retry.claim().unwrap().unwrap();
    assert_eq!(replay.id, task.id);
    assert_eq!(replay.chunk, task.chunk);
    assert_eq!(retry.offset().unwrap(), 0);
    assert!(retry.retry_allowed());
}

#[test]
fn an_incomplete_published_task_aborts_epoch_even_after_row_ack() {
    let queue = queue(2, 2, Some(2));
    let mut lane = queue.open_lane(0).unwrap();
    let task = lane.claim().unwrap().unwrap();
    let (_, marker) = lane.emit_rows(&task, 0, 1).unwrap();
    lane.acknowledge(&[marker]).unwrap();
    assert!(!lane.retry_allowed());
    assert!(lane.close().is_err());
    assert!(queue.open_lane(0).is_err());
    assert!(queue.open_lane(1).is_err());
    assert!(queue.ensure_complete().is_err());
}

#[test]
fn unacknowledged_task_end_cannot_be_mistaken_for_completion() {
    let queue = queue(1, 1, Some(1));
    let lane = queue.open_lane(0).unwrap();
    let task = lane.claim().unwrap().unwrap();
    let _marker = finish(&lane, &task);
    drop(lane);
    assert!(queue.open_lane(0).is_err());
    assert!(queue.ensure_complete().is_err());
}

#[test]
fn completed_tasks_and_offsets_survive_a_later_unpublished_retry() {
    let queue = queue(2, 1, Some(2));
    let lane = queue.open_lane(0).unwrap();
    let first = lane.claim().unwrap().unwrap();
    let (_, rows) = lane.emit_rows(&first, 0, 5).unwrap();
    let end = finish(&lane, &first);
    lane.acknowledge(&[rows, end]).unwrap();
    let second = lane.claim().unwrap().unwrap();
    drop(lane);
    let retry = queue.open_lane(0).unwrap();
    assert_eq!(retry.offset().unwrap(), 5);
    assert_eq!(retry.claim().unwrap().unwrap().id, second.id);
    assert_eq!(queue.lock().unwrap().tasks[first.id].state, TaskState::DurableComplete);
}

#[test]
fn adaptation_only_splits_pending_work_and_counts_completed_leaves() {
    let queue = queue(3, 2, Some(4));
    let first_lane = queue.open_lane(0).unwrap();
    let second_lane = queue.open_lane(1).unwrap();
    let first = first_lane.claim().unwrap().unwrap();
    let active = second_lane.claim().unwrap().unwrap();
    let before = queue.lock().unwrap().tasks[2].chunk.clone();
    let end = first_lane.finish_read(&first, Duration::from_secs(1), Duration::from_millis(1), 32).unwrap();
    first_lane.acknowledge(&[end]).unwrap();
    {
        let state = queue.lock().unwrap();
        assert_eq!(state.leaves, 4);
        assert_eq!(state.tasks[first.id].state, TaskState::DurableComplete);
        assert_eq!(state.tasks[active.id].chunk, active.chunk);
        assert_eq!(state.tasks[active.id].state, TaskState::Owned);
        assert_eq!(state.tasks[2].state, TaskState::Retired);
        let (left, right) = before.split_pending().unwrap();
        assert_eq!(state.tasks[3].chunk, left);
        assert_eq!(state.tasks[4].chunk, right);
    }
    let child = first_lane.claim().unwrap().unwrap();
    let end = first_lane.finish_read(&child, Duration::from_secs(1), Duration::ZERO, 32).unwrap();
    first_lane.acknowledge(&[end]).unwrap();
    assert_eq!(queue.lock().unwrap().leaves, 4, "completed leaves still consume the cap");
}

#[test]
fn extra_query_cost_can_reject_adaptation_without_changing_coverage() {
    let queue = queue(3, 2, None);
    let first = queue.open_lane(0).unwrap();
    let second = queue.open_lane(1).unwrap();
    let completed = first.claim().unwrap().unwrap();
    let _owned = second.claim().unwrap().unwrap();
    let end = first.finish_read(&completed, Duration::from_millis(2), Duration::from_secs(1), 0).unwrap();
    first.acknowledge(&[end]).unwrap();
    assert_eq!(queue.lock().unwrap().leaves, 3);
}

#[test]
fn empty_pending_queue_does_not_wait_for_other_lanes_acknowledgements() {
    let queue = queue(2, 2, Some(2));
    let first = queue.open_lane(0).unwrap();
    let second = queue.open_lane(1).unwrap();
    let completed = first.claim().unwrap().unwrap();
    let _owned = second.claim().unwrap().unwrap();
    let end = finish(&first, &completed);
    assert!(first.claim().unwrap().is_none());
    first.acknowledge(&[end]).unwrap();
    assert!(queue.ensure_complete().is_err());
}
