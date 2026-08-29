# Iceberg sink throughput benchmark

`../windowed_sink_throughput.py` measures the production Iceberg sink with the
built-in transfer-log generator. Use a dedicated benchmark bucket or a pinned
local S3-compatible service: the runner writes large immutable Parquet objects
and must never target user data.

1. Build the actual data-plane binary in release mode.
2. Copy `config.example.yaml` outside the repository.
3. Create a private delivery template containing the Iceberg REST catalog,
   storage, namespace, and credentials. The runner replaces `delivery_id`,
   durable state, source, and metrics.
4. Run:

```sh
python3 benchmarks/windowed_sink_throughput.py /private/iceberg-write.yaml
```

The result directory contains SHA-256 provenance, private generated configs,
raw logs, per-repetition JSON, and a generated Markdown report. Each repetition
uses a unique table. The runner does not delete tables or object versions;
perform cleanup only in the explicitly dedicated benchmark storage.

