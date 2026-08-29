# ClickHouse sink throughput benchmark

`../windowed_sink_throughput.py` measures the production ClickHouse sink with
the built-in transfer-log generator. Each run uses a unique delivery state and
table, waits for the configured warm-up, records exactly one stats sample per
second for the fixed measurement window, and stops the process without mixing
shutdown latency into the result.

1. Build the actual data-plane binary in release mode.
2. Copy `config.example.yaml` outside the repository.
3. Create a private delivery template containing the ClickHouse sink and its
   credentials. The runner replaces `delivery_id`, durable state, source, and
   metrics; it never copies the template into a report.
4. Point `delivery_template_file` and `rust_binary` at those files and run:

```sh
python3 benchmarks/windowed_sink_throughput.py /private/clickhouse-write.yaml
```

The result directory contains binary/config/template SHA-256 provenance,
private generated delivery YAML, raw logs, one JSON document per repetition,
and a generated Markdown summary. The runner deliberately does not delete
destination tables; clean up only its `sink_window_*` tables after verifying
the run.

