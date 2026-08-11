# Empty benchmark sink

`sink.empty` is a registered, deliberately non-durable sink for isolating
source, decompression, and parser throughput. It is used by the configurations
under `benchmarks/` and should not be used for a real transfer.

The sink is a normal long-lived pipeline actor. For each ordered delivery it:

1. accounts rows, Arrow bytes, and source-message counters;
2. drops the owned output batches, releasing their memory reservations;
3. immediately emits `CommittedThrough(delivery_id)`.

`prepare()` is a no-op and each partition gets its own sink/counters. Startup
semantics explicitly report `no_durability`; acknowledging a delivery means it
was measured and discarded, not stored.
