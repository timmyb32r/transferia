# YTsaurus static-table write benchmark

This benchmark runs complete finite transfers from the built-in transfer-log
generator through the production Arrow pipeline into unique static YTsaurus
tables. A repetition consists of complete scans until at least the configured
measurement duration has elapsed. Only successful process completion counts;
the runner verifies the exact destination row count, then removes only its own
unique destination directory after each scan. It records process CPU, peak RSS,
rows/core-second, and hashes of the release binary and private input config.

The production distributed-write candidate uses the YTsaurus v4 distributed
write session: contiguous Arrow batch ranges are uploaded concurrently under
signed cookies, and `finish_distributed_write_session` attaches their chunk
lists to the destination in source order. The destination is not advanced when
a fragment fails. This is preferable to client-managed temporary tables because
the server owns the upload transaction and final metadata commit.

The measured production defaults are four fragment writers, a 512 MiB flush
target, and a 2 GiB desired chunk size. See [REPORT.md](REPORT.md) for the
screening matrix, five-repetition result, and resource-aware selection rationale.

Copy `config.example.yaml` outside the repository, select a test-only YTsaurus
root, and point it at credential files and a release binary. Run:

```sh
python3 runner.py --config /private/path/ytsaurus-write.yaml
```

The raw logs and machine-readable and Markdown summaries are stored below the
configured `result_root`. Candidate order is shuffled reproducibly.
