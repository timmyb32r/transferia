# Measurement protocol

The requested budget is eight hours from invocation. This is a dedicated test cluster, not a production load experiment.

## Dataset and environment

Use Petya Gurinov's published ten-part `cb_w105_10m` fixture (10,000,000 rows, 105 columns), referenced by the PDF's reproducibility manifest. Preserve the exact column names and values; record the explicit UTC timestamp declarations used by his loader. Managed ClickHouse requires ReplicatedMergeTree in its replicated database, whereas his loader uses MergeTree; record this difference. The table is immutable during timed runs. Narrow selection uses his ten named columns; medium selection uses the first fifty DDL columns.

Record server version, local client/library/compiler versions, CPU/RAM, availability-zone evidence and network path. The historical benchmark discovered in the internal checkout used 50 million transfer-log rows, not ClickBench: it is evidence about a different workload, not a directly comparable baseline.

## Independent metrics

- Raw HTTP response: serialize, transfer and discard response chunks without client format decoding. A successfully completed finite query supplies the row denominator. Report actual bytes and Content-Encoding separately; bytes/s does not imply equal logical throughput.
- Decoded HTTP response: consume every Arrow record batch and verify the exact row count. ArrowStream decodes concurrently with HTTP input using a bounded channel. Parquet receives the complete file before decoding independent row groups concurrently. Do not substitute download throughput for decoded throughput.
- Native TCP: use the production clickhouse-arrow dependency with verified TLS and explicit network compression settings. Count all decoded rows. Native decoded Arrow buffer sizes are not wire bytes.
- Production source-to-discard: confirm relevant winners in Transferia proper, including discovery, schema checks, metadata and pipeline overhead; keep measurement boundaries explicit.

Do not retain a pandas-like whole output object: a data plane needs bounded consumption, not a complete in-memory dataframe. These results therefore answer a different client contract from Petya's pandas timing.

## Matrix

Primary formats: ArrowStream, Parquet, Native TCP. Wire controls: RowBinaryWithNamesAndTypes, CSVWithNames, JSONEachRow; investigate other stable formats if supported and practical. Primary format codecs: none, LZ4 (Arrow's lz4_frame), ZSTD. HTTP transport controls: none, LZ4, ZSTD, gzip. Format and transport compression are independent layers; primary comparisons enable only one layer. Unsupported cells are explicit outcomes, not zero throughput.

Start with 16 server threads, 8 Parquet decoder threads and 250,000-row groups. Tune finalists and compare like-for-like thread budgets before drawing conclusions. Preserve server settings and actual Content-Encoding. LZ4 framing differs across formats, so a matching family is not identical compressed representation.

## Timing and resources

The primary first-pass matrix warms each cell with a separate complete query, then measures complete repeated queries for at least 120 seconds. C++ controls also have separate per-cell warmups. Tuning, finalist, curl and production phases reuse the warmed immutable dataset but include their first completed query in the measured run; they do not claim a separate per-cell warmup. Keep per-query observations and inspect stability; extend unstable finalists. Completion can exceed 120 seconds: a cancelled Parquet file cannot be a decoded sample. Complete the exhaustive first pass in seeded shuffled order, then independently repeat wide primary profiles and narrow/medium finalists or near ties. Dominated diagnostic cells may retain one stable long run; every chart/table must expose its actual repetition count. Use additional repeats when material variance warrants it. Full finite-query throughput includes all serialization and footer/decode work. A steady interior transfer window, where separately instrumented, is an additional metric and must not replace complete-query throughput silently.

All primary timed clients are release Rust binaries on the dev server. Official C++ Native and C/libcurl ceilings are separate controls with explicit different contracts. Python only orchestrates, loads data, samples process RSS, analyzes or plots. CPU user+system time comes from child getrusage; Baseline RSS is sampled every 0.5 seconds; later controllers additionally record the kernel per-child maxrss. Report rows per aggregate client CPU-second, not throughput divided by allocated core count. Server CPU is separate and must not be presented as measured unless captured.

## Validity

Verify file byte sizes against the published manifest; verify loaded total count, temporal ranges and string byte totals. Decode errors, transport errors and row-count mismatches fail the cell. For winner validation inspect representative/full dataset fingerprints and schema/value preservation rather than trusting row counts alone. Preserve raw logs and every failure. Never expose credentials in artifacts.

## Compression-level controls

Primary defaults differ: Arrow IPC ZSTD level 1, Native ZSTD level 1, Parquet ZSTD level 3. Add Parquet level-1 comparisons. HTTP LZ4 default level 3 selects the HC implementation; add explicit HTTP level 0 (accepted by this server). The format-level LZ4 writers use fast compression. State codec implementation/framing and level alongside each result, not only the family name.
