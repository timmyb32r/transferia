# Benchmark discard sink

`sink.discard` is a registered, deliberately non-durable sink for isolating
source, decompression, and parser throughput. It is used only by configurations
under `benchmarks/`.

For each ordered delivery it accounts rows, bytes, and source-message counters;
drops the output batches; and immediately emits `CommittedThrough(delivery_id)`.
Startup semantics report `no_durability`: acknowledgement means measured and
discarded, not stored.
