//! In-process ownership and durability for one table's snapshot tasks.
//!
//! A task is pending, exclusively owned by one lane attempt, read-complete, or
//! durably complete. Only pending tasks may split. The part ceiling counts all
//! live plan leaves, including completed work; retired parents do not count.
//! Rows never live in this queue. The existing pipeline owns their memory and
//! calls `acknowledge` only after destination durability.
//!
//! A lane may read another task before its previous task is acknowledged. It
//! must never wait for its own acknowledgement inside `Source::read_batch`.
//! Dropping an attempt with published but unacknowledged work invalidates the
//! epoch: unordered COPY cannot safely replay a partially published task.

use std::collections::{BTreeSet, VecDeque};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use super::planner::{Chunk, ChunkKind};
use transferia_core::source::CommitMarker;

pub(crate) struct SnapshotQueue {
    identity: Arc<()>,
    state: Mutex<QueueState>,
}

struct QueueState {
    tasks: Vec<Task>,
    pending: VecDeque<usize>,
    lanes: Vec<LaneState>,
    leaves: usize,
    max_parts: Option<usize>,
    interrupted: bool,
}

struct Task {
    chunk: Chunk,
    state: TaskState,
    owner: Option<(usize, u64)>,
    published: bool,
    rows: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TaskState {
    Pending,
    Owned,
    ReadComplete,
    DurableComplete,
    Retired,
}

#[derive(Default)]
struct LaneState {
    attempt: u64,
    active: bool,
    offset: i64,
    next_sequence: u64,
    outstanding: VecDeque<MarkerRecord>,
    active_task: Option<usize>,
    unfinished: BTreeSet<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MarkerRecord {
    task: usize,
    sequence: u64,
    offset: i64,
    task_end: bool,
}

#[derive(Clone)]
struct SnapshotMarker {
    epoch: Arc<()>,
    lane: usize,
    attempt: u64,
    record: MarkerRecord,
}

struct Adaptation {
    parent: usize,
    left: usize,
    right: usize,
    leaves: usize,
    predicted_seconds: f64,
    query_seconds: f64,
}

pub(super) struct ClaimedChunk {
    pub(super) id: usize,
    pub(super) chunk: Chunk,
}

/// Exclusive attempt token. Drop releases unstarted work and fails an epoch
/// which would otherwise silently replay an incomplete published task.
pub(super) struct LaneLease {
    queue: Arc<SnapshotQueue>,
    lane: usize,
    attempt: u64,
    closed: bool,
}

impl SnapshotQueue {
    pub(crate) fn new(
        chunks: Vec<Chunk>,
        lanes: u32,
        max_parts: Option<NonZeroU32>,
    ) -> anyhow::Result<Arc<Self>> {
        let lanes = usize::try_from(lanes)?;
        let max_parts = max_parts.map(|value| usize::try_from(value.get())).transpose()?;
        anyhow::ensure!(!chunks.is_empty(), "snapshot plan has no chunks");
        anyhow::ensure!(lanes > 0 && lanes <= chunks.len(), "snapshot lane count must be positive and no greater than the chunk count");
        anyhow::ensure!(max_parts.is_none_or(|limit| chunks.len() <= limit), "snapshot plan exceeds maximum parts");
        validate_initial_cover(&chunks)?;
        let leaves = chunks.len();
        Ok(Arc::new(Self {
            identity: Arc::new(()),
            state: Mutex::new(QueueState {
                tasks: chunks.into_iter().map(|chunk| Task {
                    chunk, state: TaskState::Pending, owner: None, published: false, rows: 0,
                }).collect(),
                pending: (0..leaves).collect(),
                lanes: (0..lanes).map(|_| LaneState::default()).collect(),
                leaves,
                max_parts,
                interrupted: false,
            }),
        }))
    }

    fn lock(&self) -> anyhow::Result<MutexGuard<'_, QueueState>> {
        let state = self.state.lock().map_err(|_| anyhow::anyhow!("snapshot queue ownership state is poisoned"))?;
        anyhow::ensure!(!state.interrupted, "PostgreSQL snapshot epoch was interrupted after publishing incomplete work; partial COPY cannot be replayed safely");
        Ok(state)
    }

    pub(super) fn open_lane(self: &Arc<Self>, lane: usize) -> anyhow::Result<LaneLease> {
        let mut state = self.lock()?;
        let slot = state.lanes.get_mut(lane).ok_or_else(|| anyhow::anyhow!("snapshot lane does not exist"))?;
        anyhow::ensure!(!slot.active, "snapshot lane already has an active attempt");
        let attempt = slot.attempt.checked_add(1).ok_or_else(|| anyhow::anyhow!("snapshot lane attempt overflow"))?;
        slot.attempt = attempt;
        slot.active = true;
        Ok(LaneLease { queue: Arc::clone(self), lane, attempt, closed: false })
    }

    pub(crate) fn ensure_complete(&self) -> anyhow::Result<()> {
        let state = self.lock()?;
        anyhow::ensure!(state.tasks.iter().all(|task| matches!(task.state, TaskState::DurableComplete | TaskState::Retired)), "PostgreSQL snapshot still has tasks without destination durability acknowledgement");
        anyhow::ensure!(state.lanes.iter().all(|lane| lane.outstanding.is_empty()), "PostgreSQL snapshot has outstanding durability markers");
        Ok(())
    }
}

impl LaneLease {
    fn check(&self, state: &QueueState) -> anyhow::Result<()> {
        anyhow::ensure!(!self.closed, "snapshot lane attempt is closed");
        let lane = state.lanes.get(self.lane).ok_or_else(|| anyhow::anyhow!("snapshot lane does not exist"))?;
        anyhow::ensure!(lane.active && lane.attempt == self.attempt, "stale snapshot lane attempt");
        Ok(())
    }

    pub(super) fn offset(&self) -> anyhow::Result<i64> {
        let state = self.queue.lock()?;
        self.check(&state)?;
        Ok(state.lanes[self.lane].offset)
    }

    pub(super) fn retry_allowed(&self) -> bool {
        self.queue.lock().is_ok_and(|state| {
            self.check(&state).is_ok()
                && state.lanes[self.lane].unfinished.iter().all(|id| !state.tasks[*id].published)
        })
    }

    pub(super) fn claim(&self) -> anyhow::Result<Option<ClaimedChunk>> {
        let mut state = self.queue.lock()?;
        self.check(&state)?;
        anyhow::ensure!(state.lanes[self.lane].active_task.is_none(), "snapshot lane already owns an unread task");
        let Some(id) = state.pending.front().copied() else { return Ok(None); };
        let task = state.tasks.get_mut(id).ok_or_else(|| anyhow::anyhow!("snapshot pending task is missing"))?;
        anyhow::ensure!(task.state == TaskState::Pending, "snapshot pending task is already owned");
        task.state = TaskState::Owned;
        task.owner = Some((self.lane, self.attempt));
        let chunk = task.chunk.clone();
        state.pending.pop_front();
        state.lanes[self.lane].active_task = Some(id);
        state.lanes[self.lane].unfinished.insert(id);
        tracing::debug!(target: "transferia.postgres.snapshot", lane = self.lane, attempt = self.attempt, task = id, "snapshot task claimed");
        Ok(Some(ClaimedChunk { id, chunk }))
    }

    pub(super) fn emit_rows(
        &self,
        task: &ClaimedChunk,
        start_offset: i64,
        rows: u64,
    ) -> anyhow::Result<(i64, CommitMarker)> {
        anyhow::ensure!(rows > 0, "snapshot row marker must describe a nonempty batch");
        let mut state = self.queue.lock()?;
        self.check(&state)?;
        validate_owned(&state, task.id, self.lane, self.attempt)?;
        let lane = &state.lanes[self.lane];
        anyhow::ensure!(lane.offset == start_offset, "snapshot batch starts at an unexpected offset");
        let offset = start_offset.checked_add(i64::try_from(rows)?).ok_or_else(|| anyhow::anyhow!("PostgreSQL source offset overflow"))?;
        let total_rows = state.tasks[task.id].rows.checked_add(rows).ok_or_else(|| anyhow::anyhow!("snapshot task row count overflow"))?;
        let sequence = lane.next_sequence.checked_add(1).ok_or_else(|| anyhow::anyhow!("snapshot marker sequence overflow"))?;
        state.tasks[task.id].published = true;
        state.tasks[task.id].rows = total_rows;
        let record = MarkerRecord { task: task.id, sequence, offset, task_end: false };
        let lane = &mut state.lanes[self.lane];
        lane.offset = offset;
        lane.next_sequence = sequence;
        lane.outstanding.push_back(record);
        Ok((offset, self.marker(record)))
    }

    pub(super) fn finish_read(
        &self,
        task: &ClaimedChunk,
        elapsed: Duration,
        query_start: Duration,
        bytes: u64,
    ) -> anyhow::Result<CommitMarker> {
        let mut state = self.queue.lock()?;
        self.check(&state)?;
        validate_owned(&state, task.id, self.lane, self.attempt)?;
        let lane = &state.lanes[self.lane];
        let sequence = lane.next_sequence.checked_add(1).ok_or_else(|| anyhow::anyhow!("snapshot marker sequence overflow"))?;
        let record = MarkerRecord { task: task.id, sequence, offset: lane.offset, task_end: true };
        state.tasks[task.id].state = TaskState::ReadComplete;
        state.tasks[task.id].published = true;
        state.lanes[self.lane].next_sequence = sequence;
        state.lanes[self.lane].outstanding.push_back(record);
        state.lanes[self.lane].active_task = None;
        let rows = state.tasks[task.id].rows;
        let adaptation = adapt_pending(&mut state, &task.chunk, elapsed, query_start);
        drop(state);
        tracing::info!(target: "transferia.postgres.snapshot", lane = self.lane, task = task.id, rows, copy_payload_bytes = bytes, elapsed_ms = elapsed.as_secs_f64() * 1000.0, query_start_ms = query_start.as_secs_f64() * 1000.0, "snapshot task read completed; awaiting destination durability");
        if let Some(change) = adaptation {
            tracing::info!(target: "transferia.postgres.snapshot", parent_task = change.parent, left_task = change.left, right_task = change.right, parts = change.leaves, estimated_saved_ms = change.predicted_seconds * 500.0, extra_query_ms = change.query_seconds * 1000.0, reason = "pending_tail_saving_exceeds_query_startup", "snapshot pending task split");
        }
        Ok(self.marker(record))
    }

    fn marker(&self, record: MarkerRecord) -> CommitMarker {
        CommitMarker::new(SnapshotMarker {
            epoch: Arc::clone(&self.queue.identity), lane: self.lane, attempt: self.attempt, record,
        })
    }

    pub(super) fn acknowledge(&self, markers: &[CommitMarker]) -> anyhow::Result<()> {
        let markers = markers.iter().map(CommitMarker::value::<SnapshotMarker>).collect::<Result<Vec<_>, _>>()?;
        let mut state = self.queue.lock()?;
        self.check(&state)?;
        let outstanding = &state.lanes[self.lane].outstanding;
        anyhow::ensure!(markers.len() <= outstanding.len(), "snapshot acknowledgement repeats or exceeds published work");
        // Validate the complete durability group before changing any task.
        for (marker, expected) in markers.iter().zip(outstanding) {
            anyhow::ensure!(Arc::ptr_eq(&marker.epoch, &self.queue.identity) && marker.lane == self.lane && marker.attempt == self.attempt, "snapshot acknowledgement belongs to a different epoch or lane attempt");
            anyhow::ensure!(marker.record == *expected, "snapshot acknowledgement is duplicated, out of order, or has invalid progress");
            let task = state.tasks.get(expected.task).ok_or_else(|| anyhow::anyhow!("snapshot acknowledgement references a missing task"))?;
            anyhow::ensure!(task.owner == Some((self.lane, self.attempt)), "snapshot acknowledgement references a foreign task");
            anyhow::ensure!(if expected.task_end { task.state == TaskState::ReadComplete } else { matches!(task.state, TaskState::Owned | TaskState::ReadComplete) }, "snapshot acknowledgement has an invalid task lifecycle");
        }
        let committed = state.lanes[self.lane].outstanding.drain(..markers.len()).collect::<Vec<_>>();
        for record in committed {
            if record.task_end {
                state.tasks[record.task].state = TaskState::DurableComplete;
                state.lanes[self.lane].unfinished.remove(&record.task);
                tracing::debug!(target: "transferia.postgres.snapshot", lane = self.lane, task = record.task, "snapshot task durably completed");
            }
        }
        Ok(())
    }

    pub(super) fn close(&mut self) -> anyhow::Result<()> {
        if self.closed { return Ok(()); }
        self.closed = true;
        let mut state = self.queue.state.lock().map_err(|_| anyhow::anyhow!("snapshot queue ownership state is poisoned"))?;
        let mut returned = Vec::new();
        let mut ambiguous = false;
        let unfinished = state.lanes[self.lane].unfinished.iter().copied().collect::<Vec<_>>();
        for id in unfinished {
            let task = &mut state.tasks[id];
            if task.published {
                ambiguous = true;
            } else {
                task.state = TaskState::Pending;
                task.owner = None;
                returned.push(id);
            }
        }
        state.pending.extend(returned);
        state.lanes[self.lane].active = false;
        state.lanes[self.lane].active_task = None;
        if !ambiguous { state.lanes[self.lane].unfinished.clear(); }
        state.interrupted |= ambiguous;
        anyhow::ensure!(!ambiguous, "PostgreSQL snapshot epoch aborted after publishing incomplete work; partial COPY cannot be replayed safely");
        Ok(())
    }
}

impl Drop for LaneLease {
    fn drop(&mut self) { drop(self.close()); }
}

fn validate_owned(state: &QueueState, id: usize, lane: usize, attempt: u64) -> anyhow::Result<()> {
    let task = state.tasks.get(id).ok_or_else(|| anyhow::anyhow!("snapshot task does not exist"))?;
    anyhow::ensure!(task.state == TaskState::Owned && task.owner == Some((lane, attempt)), "snapshot task is not owned by this lane attempt");
    Ok(())
}

/// Construction requires one ordered, complete strategy domain. Opaque chunks
/// can still be cloned or accidentally omitted by a caller; detect that before
/// any task is published rather than trusting a Vec to prove coverage.
fn validate_initial_cover(chunks: &[Chunk]) -> anyhow::Result<()> {
    let mut previous_upper = None;
    for (index, chunk) in chunks.iter().enumerate() {
        let first = index == 0;
        let last = index + 1 == chunks.len();
        match chunk.kind() {
            ChunkKind::Whole => anyhow::ensure!(chunks.len() == 1, "complete-query snapshot descriptor cannot be combined with other chunks"),
            ChunkKind::Ctid { lower_page, upper_page, estimated_end_page } => {
                anyhow::ensure!(matches!(chunks[0].kind(), ChunkKind::Ctid { .. }), "snapshot plan mixes partitioning strategies");
                anyhow::ensure!(*lower_page == previous_upper && (!first || lower_page.is_none()) && (upper_page.is_none() == last), "CTID snapshot chunks have a gap, overlap, or closed outer tail");
                let lower = u64::from(lower_page.unwrap_or(0));
                let upper = upper_page.map_or(*estimated_end_page, u64::from);
                anyhow::ensure!(upper >= lower && (last || upper > lower), "CTID snapshot chunk has invalid bounds");
                previous_upper = *upper_page;
            }
            ChunkKind::Indexed { lower_cut, upper_cut } => {
                anyhow::ensure!(matches!(chunks[0].kind(), ChunkKind::Indexed { .. }), "snapshot plan mixes partitioning strategies");
                anyhow::ensure!(*lower_cut == previous_upper && (!first || lower_cut.is_none()) && (upper_cut.is_none() == last), "indexed snapshot chunks have a gap, overlap, or closed outer tail");
                anyhow::ensure!(lower_cut.zip(*upper_cut).is_none_or(|(lower, upper)| lower < upper), "indexed snapshot chunk has reversed or duplicate cuts");
                previous_upper = *upper_cut;
            }
            ChunkKind::HashCtid { bucket, modulus } => {
                anyhow::ensure!(matches!(chunks[0].kind(), ChunkKind::HashCtid { .. }), "snapshot plan mixes partitioning strategies");
                anyhow::ensure!(usize::try_from(*bucket)? == index && usize::try_from(modulus.get())? == chunks.len(), "snapshot hash buckets do not exactly cover their modulus");
            }
        }
    }
    Ok(())
}

fn adapt_pending(state: &mut QueueState, completed: &Chunk, elapsed: Duration, query_start: Duration) -> Option<Adaptation> {
    let observed_pages = completed.page_span().filter(|pages| *pages > 0)?;
    if state.pending.len() >= state.lanes.len() || state.max_parts.is_some_and(|limit| state.leaves >= limit) { return None; }
    let candidate = state.pending.iter().filter_map(|id| state.tasks[*id].chunk.page_span().map(|pages| (*id, pages))).max_by_key(|(_, pages)| *pages);
    let (id, pages) = candidate?;
    let predicted_seconds = elapsed.as_secs_f64() * pages as f64 / observed_pages as f64;
    // Two equally sized page ranges can at most halve the predicted tail. Split
    // only when that saving covers another measured COPY startup, and there are
    // fewer pending pieces than lanes. This is a scheduling hint, never coverage.
    if predicted_seconds / 2.0 <= query_start.as_secs_f64() { return None; }
    let (left, right) = state.tasks[id].chunk.split_pending()?;
    let leaves = state.leaves.checked_add(1)?;
    let first = state.tasks.len();
    let second = first.checked_add(1)?;
    state.tasks[id].state = TaskState::Retired;
    state.pending.retain(|pending| *pending != id);
    for chunk in [left, right] {
        state.tasks.push(Task { chunk, state: TaskState::Pending, owner: None, published: false, rows: 0 });
    }
    state.pending.extend([first, second]);
    state.leaves = leaves;
    Some(Adaptation { parent: id, left: first, right: second, leaves, predicted_seconds, query_seconds: query_start.as_secs_f64() })
}

#[cfg(test)]
#[path = "tests/queue.rs"]
mod tests;
