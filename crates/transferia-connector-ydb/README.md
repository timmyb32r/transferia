# YDB source bootstrap

## Batch and stream: snapshot with overlap

Batch + stream uses a separate table snapshot with an
already-running CDC changefeed. There is no strategy selector: native initial
scan is deferred.

Before reading the snapshot, the connector durably captures the start
offset of every CDC topic under its execution fence. After the destination has
committed the entire snapshot, it durably records the phase transition and
consumes CDC from those captured offsets. It validates retained offsets and
resource identity again before replay; an expired offset is an error, never a
reason to skip ahead.

This is an overlapping handoff, not an exactly-once snapshot boundary. The
snapshot can already contain a change that CDC subsequently replays. Older
values can temporarily be reapplied before newer changes restore the current
state. In this explicitly selected mode, complete UPDATE images are applied as
upserts (the internal `c` operation), retaining before-images and preserving
DELETE operations. An incomplete update is rejected before emission. Ordinary
stream delivery preserves its original UPDATE operations. This reconciliation
is necessary when a row was updated and deleted before the snapshot read it:
the earlier update must be allowed to recreate the row before its delete.

`source.version` is separate UInt64 control metadata: snapshot rows have version
zero and overlap CDC rows have version `topic offset + 1` (ordinary stream keeps
version equal to its offset). The real Topic offset remains
unchanged in `_system_offset`; snapshot offsets are local row ordinals, not Topic
positions. Unknown snapshot transaction IDs and timestamps are null, not invented
values. The resource ownership format is versioned to reject resuming older CDC
executions with a different row-version contract.

A crash during the snapshot requires manual recovery. Repeating
a fresh snapshot into a partially populated destination is not automatically
safe: a row can disappear between attempts, and duplicate primary keys must
not be silently resolved. An incomplete durable snapshot state blocks restart.
Use a new delivery with a clean destination and a dedicated CDC consumer after
checking the failed delivery. The connector never cleans the destination or
resets durable state automatically. A completed durable snapshot resumes only
the stream phase.

Changefeeds, important consumers and the Coordination node must already exist,
as for ordinary stream delivery. Do not share a consumer with an external
reader. Exactly one fixed Topic partition per changefeed is currently required.
All snapshot tables run sequentially in one co-located worker, followed by the
stream in that worker. This is not an atomic cross-table database snapshot.

## Deferred: native CDC initial scan

YDB initial scan places existing rows and subsequent changes into the same CDC
topic. Snapshot records and changes for different keys can interleave, while
the initial state of each key precedes its later changes. This resembles DBLog
in its combination of snapshotting with a live change stream, but it is not an
implementation of the DBLog watermark protocol.

Keep this distinction in documentation and module names: a future initial-scan
implementation belongs in `src_batch_and_stream`, not `src_dblog`. Merely
combining a snapshot with CDC does not provide end-to-end exactly-once delivery;
destination commit and replay behavior remain part of that guarantee.
