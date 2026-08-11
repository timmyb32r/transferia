# S3 sink runtime contract

The registered S3 sink serializes Arrow rows as compact NDJSON and groups them
into deterministic commit epochs. Object keys contain the source topic,
partition, and first offset. Main and DLQ objects from one delivery are all
durable before the source delivery can be acknowledged. All PQv1 cookies in a
newly durable contiguous prefix are submitted together in one commit request
and acknowledged before the local progress ledger advances.

Important thresholds are configuration-derived and stable across restarts.
Rotation and admission thresholds are checked only after accepting an atomic
source message or delivery, so that unit may temporarily exceed a row, byte,
open-object, pending-object, or buffered-byte threshold. The next delivery is
backpressured until the sink is below its admission thresholds:

- `rotation.max_rows` and `rotation.max_bytes` are soft object thresholds;
- `buffering.max_epoch_bytes` (serialized payload, routing-string UTF-8 lengths,
  and a fixed 128-byte logical overhead per row) and `max_open_objects` are
  soft deterministic epoch thresholds and semantic state across replay;
- `buffering.max_pending_upload_objects` and `max_buffered_bytes` are soft
  admission thresholds for live state;
- `upload.max_in_flight_objects` and `parallel_parts` are hard upload-concurrency
  limits;
- `upload.operation_timeout` bounds each object-store request;
- `retry.max_attempts` prevents a permanently failing object from retrying
  forever.

`max_epoch_bytes` defaults to a fixed 128MiB and is semantic state. Pending
object count and upload completion timing affect admission only, never object
rotation. The epoch limit must not exceed either `max_buffered_bytes` or the
global `pipeline_memory_limit_bytes`.

The runtime attempts to abort multipart uploads after part/complete failures
and during graceful cancellation; each abort attempt has a separate
four-second bound. Configure an S3
`AbortIncompleteMultipartUpload` lifecycle rule as a hard-crash safety net.
Successful deterministic overwrite is exactly-once only while
parser, middleware and projection settings (including
`keep_system_columns_in_sink`), destination identity (bucket/endpoint/region),
S3 prefix, partitioning/rotation, `max_open_objects`, and `max_epoch_bytes`
remain unchanged across replay. Wall-clock rotation is reported as
at-least-once because restart timing changes object boundaries.
