# Iceberg source throughput benchmark

The runner measures the production Iceberg source and production Arrow decoder
against the discard sink. It repeatedly reads one immutable snapshot for at
least the configured measurement window, using one logical Transferia worker.
Every scan is complete; partial scans and failed processes invalidate the run.

The private YAML contains S3 access-key metadata and a path to the secret. Keep
it outside Git. Raw logs, per-scan CPU/RSS, binary Murmur3 x64 128, snapshot identity,
and aggregate throughput are written beneath `results/<timestamp>/`.

```sh
python3 benchmarks/iceberg_read_throughput/runner.py /private/config.yaml
```

Use short windows only to eliminate clearly dominated candidates. Winner
selection requires 120-second windows and two or three interleaved repetitions.
