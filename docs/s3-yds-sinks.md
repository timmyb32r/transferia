# S3 sink runtime contract

The registered S3 sink serializes Arrow rows as compact NDJSON and groups them
into deterministic commit epochs. Object keys contain the source topic,
partition, and first offset. Main and DLQ objects from one delivery are all
durable before the source delivery can be acknowledged.

Important boundaries are configuration-derived and stable across restarts:

- `rotation.max_rows` and `rotation.max_bytes` bound each object;
- `buffering.max_epoch_bytes` and `max_open_objects` bound a deterministic
  epoch and are semantic state across replay;
- `buffering.max_pending_objects` and `max_buffered_bytes` bound live state;
- `upload.max_in_flight_objects` and `parallel_parts` bound upload concurrency;
- `retry.max_attempts` prevents a permanently failing object from retrying
forever.

`max_epoch_bytes` defaults to a fixed 128MiB and is semantic state. Pending
object count and upload completion timing affect admission only, never object
rotation. The epoch limit must not exceed either `max_buffered_bytes` or the
global `pipeline_memory_limit_bytes`.

The runtime attempts to abort multipart uploads after part/complete failures
and during graceful cancellation. Configure an S3
`AbortIncompleteMultipartUpload` lifecycle rule as a hard-crash safety net.
Successful deterministic overwrite is exactly-once only while
parser/projection settings (including `keep_system_columns_in_sink`), S3
prefix, partitioning/rotation, `max_open_objects`, and `max_epoch_bytes` remain
unchanged across replay. Wall-clock rotation is reported as at-least-once
because restart timing changes object boundaries.

The YDS sink sources under `providers/yds/sink/` are not registered by the
current executable and are outside this runtime contract.
