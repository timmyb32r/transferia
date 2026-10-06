# Probe provenance

`probe-v1.rs.txt` is a frozen source artifact of the first-pass transport probe and its independently repeated baseline. It is not a second production implementation. The V2 source in `crates/transferia-connector-clickhouse/examples/` adds controlled settings, parallel IPC and representative value verification. Baseline and tuning series identify their source version in the protocol/result provenance.

Server checkout is a copied source tree without `.git`; no checkout revision is invented. The initial CLI checks used the pre-existing September 29 release binary. It is retained remotely as `bin/transferia-before-native-fix`; setup and protocol failures are archived separately. Final production confirmations use a fresh release build after the documented LZ4 negotiation fix and record size/mtime. The probe release build uses Rust 1.96.0, repository-pinned dependencies, fat LTO, one codegen unit and target-cpu=native. Builds, transfers and dataset preparation are outside all measured windows.

The historical transfer-log report is retained as a source artifact. Its original relative links refer to the original server result directory, whose surrounding files are not copied here. It is not the ClickBench result of this experiment.

Probe examples use the system allocator; the CLI composition declares global MiMalloc. Final production CLI is a direct single-process `--config` delivery, not the server worker supervisor. Its kernel maxrss is therefore the measured process peak. Production Parquet disables page-index and Bloom-filter output, whereas default probe profiles retain server defaults; reader-setting controls isolate those writer flags.

`../file-timestamps.json` freezes collected result timestamps/sizes, so V1 temporal query correlation remains reproducible after Git checkout changes filesystem mtimes. V2/C++/curl use exact query IDs; production additionally validates terminal pipeline counts.
