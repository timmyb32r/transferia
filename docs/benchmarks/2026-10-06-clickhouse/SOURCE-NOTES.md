# Source evidence and comparison boundaries

This file records methodology findings, not a final ranking.

## Petya's experiment

[Reproduction repository](https://github.com/petergurinov/between-sql-and-data), inspected HEAD `e63793f0448d8abdeed17687fbb29ba5413bd0ff`. Final methodology commit `d965b352d3708d6d712b0e4142fc0ea7c2b79552`. Published measurements contain five measured repetitions after one warmup. Server 26.7.5.10, 16 CPU / 64 GB; client 32 CPU / 128 GB, same zone and VPC. Slide 73 compares materialized pandas output for 10M × 105: ArrowStream 35.613246 seconds, Parquet 44.407238 seconds, codec none. Other output contracts have separate measurements: raw bytes 24.480968 vs 18.363248; Arrow Table 11.128677 vs 21.843824. Those are independent runs; subtracting their timings does not reconstruct stage costs.

His ArrowStream path decodes incrementally before constructing a retained whole table/dataframe. His Parquet path buffers a complete response and then decodes it. Our data-plane probe consumes and drops checked batches, never constructs a pandas dataframe. A streaming data plane's memory and timing contract differs from a whole-output materialization benchmark.

Input Parquet files use ZSTD, but that does not imply compressed output in every measurement. Output format and transport settings must be checked separately. ADBC's ClickHouse HTTP path requesting ArrowStream does not constitute a different wire format.

## Historical Transferia experiments

Internal `benchmarks/clickhouse_read_throughput/results/FINAL_REPORT.md`, dated 2026-08-28, uses 50M transfer-log rows. Its Parquet/ZSTD production plane is 11.13M rows/s and complete-run throughput 9.68M; Native/ZSTD wire result ~0.883M. This is not the canonical 105-column ClickBench fixture used now.

A separate [2026-09-03 local ClickBench tournament](../../../benchmarks/clickbench_throughput/REPORT.md) used 2M rows from hits.csv, local ClickHouse 25.8.2.29, and Speedtest's second pass. Its source ceiling was 650,048 rows/s. Fixture, network, table layout and measurement boundaries differ. Neither older result supports a controlled assertion about the effect of changing only availability zone or ClickHouse version.

## Compression implementations

Exact upstream tag [`v26.9.5.2-stable`](https://github.com/ClickHouse/ClickHouse/tree/v26.9.5.2-stable), commit `f9447d2dc036af37cca30b056598ebbadde4cbe4`, is the source reference. Managed server reports 26.9.5.2-yc.1; YC patches have not been audited.

- Arrow IPC compresses individual buffers, ZSTD level 1 and LZ4 frame level 0. Its encoding work uses the shared format thread pool; query max_threads is not necessarily a ceiling on every format task.
- Parquet has page encodings, dictionaries and independent row groups. ZSTD defaults to output_format_compression_level=3; LZ4 uses LZ4_compress_default. The primary sweep and the matched ZSTD level-1 control are separate series.
- Native uses block-level network compression. The Rust dependency explicitly requests ZSTD level 1. The C++ control must pin both compression enablement and network_compression_method; selecting a decoder codec alone does not prove which codec the server used.
- HTTP compression wraps the complete serialized response. HTTP's default level 3 is also passed into LZ4 frame preferences, selecting HC rather than the fast path. Controls explicitly request level 0. [LZ4 frame implementation](https://github.com/lz4/lz4/blob/dev/lib/lz4frame.c) selects HC at its minimum HC level.

Format-family labels alone do not imply equal CPU cost, block size, framing, dictionary effectiveness or compression level.

## Client implementation controls

Rust Native wraps the connection in a buffered reader. However, its binary string reader allocates a temporary Vec for each value before appending/copying into Arrow builders. This is a plausible allocation/copy cost, not a demonstrated causal explanation without a control. Compare the official version-matched C++ client before attributing Native throughput to the protocol itself.

Sequential Arrow StreamReader performs checked batch decoding on one blocking worker. A bounded independent-record-batch utility tests parallel IPC decoding on this dictionary-free fixture. It fails on dictionary state/schema changes and does not promise ordered production delivery. Its performance cannot be presented as an already-integrated Transferia Arrow source.

Production Parquet uses binary string annotations, ordered row-group handling, schema and value checks, metadata and pipeline accounting. The transport probe initially uses UTF-8 annotations and eight decoder workers. Binary-string and decoder-budget controls plus full production source-to-discard runs distinguish these costs.

A critical C++ control pitfall: on exact upstream 26.9.5.2, `clickhouse client --format Null` mirrors the option to the server's output_format setting. executeQuery resolves Null and TCPHandler intentionally skips data blocks. It is therefore NOT a Native extraction benchmark, even if read_rows is 10M. The prepared control uses `--format Native` and discards local output: data is transported, decoded and then reserialized natively to /dev/null. This extra local serialization is included and disclosed. Final validation must also check nonzero network_send_bytes, not only row counts. The standalone clickhouse-benchmark reads blocks without reserialization but hardcodes compression enabled, so it is not automatically equivalent to a compression-disabled control.

HTTP profile defaults are max_block_size=65409 and preferred_block_size_bytes=1000000; the Rust Native primary pins 65409 and preferred_block_size_bytes=0. V2 keeps baseline defaults unchanged and adds explicit equal-block controls for HTTP Arrow/Parquet. Batch counts are recorded in V2 to expose actual block geometry. These settings affect preferred block shape, not a universal row-count guarantee for every encoder chunk.

Parquet's codec none is still a page-encoded representation: dictionary/RLE and logical-type encodings remain. First wide raw response sizes were ~4.214 GB for Parquet/none versus ~6.962 GB for ArrowStream/none; this is not evidence of hidden transport compression. A separate dictionary-limit-zero control tests its contribution. Plain Parquet small integers also use INT32 physical storage with logical annotations, unlike Arrow's INT16 arrays; comparing only codec names ignores these representations.

The vendored Rust Native client advertises revision 54451 deliberately: parallel-replica packets (54453) and custom/sparse serialization (54454) are not fully implemented. It must not advertise capabilities merely to obtain a faster result. The current C++ client negotiates newer capabilities. NativeWriter's getSerializationAndColumn materializes custom columns for old revisions but can send sparse representations to newer clients. Therefore the C++ comparison includes negotiated representation differences; check query-log client_revision and network bytes and do not attribute all gains solely to per-string allocation or Rust/C++ language choice. No nonexistent enable_sparse_serialization query setting is used.

Query-log CPU accounts shared Arrow compression jobs through ThreadPoolCallbackRunnerLocal/getCurrentThreadGroup/ThreadGroupSwitcher in exact-tag src/Common/threadPoolCallbackRunner.h; Parquet encoder jobs also attach to the query group. It is still per-query process CPU, not a host-wide managed-server CPU/RAM metric.

Both exact 26.7.5.10 and 26.9.5.2 tags already use parallel Arrow buffer compression and ZSTD level 1. The large intervening Arrow reader changes do not establish a server-output performance cause for our Python-versus-Rust comparison.

A concrete recent Native change exists: exact 26.9.5.2 advertises TCP revision 54492 and adds String serialization with a separate cumulative-byte-offset stream. Exact 26.7.5.10 advertises 54488 and lacks that feature. NativeWriter gates the new string layout on the peer revision without a separate per-column marker. Our Rust client at 54451 cannot receive it. Prepare an older official C++ 26.7 client control against the same new server if compatibility permits; this compares client versions/negotiation, not a controlled server upgrade. New offset-stream layout is not guaranteed to be smaller uncompressed; actual bytes and CPU must decide.

26.9 also adds output_format_arrow_record_batch_size / _bytes (both default 0), combining smaller input chunks into IPC batches. Controls test 65,536 and 250,000 rows, sequential and bounded parallel Rust decoding, while preserving the baseline profile. Upstream 26.7's registration selected its custom/native writer when output_format_arrow_use_native_writer=true (default true); 26.9 removes the old alternative and always registers it. Thus the mere presence of custom-writer code in an older tag is not sufficient evidence, but its default and the reproduction repository's absence of an override support the comparison caveat above.

The reference driver's contracts differ in a further, causal way. In exact [clickhouse-connect 0.8.9 httpclient.py](https://github.com/ClickHouse/clickhouse-connect/blob/v0.8.9/clickhouse_connect/driver/httpclient.py), raw_query calls _raw_request with server_wait default true, which sends wait_end_of_query=1; raw_stream explicitly disables it. Petya's uncompressed raw cells call raw_query, whereas ArrowStream table cells use a streaming call. Raw retains the complete bytes object, while our raw utility discards chunks. Selected measurements, actual schemas and their dates are in reference-cells.json. Timings cannot be subtracted as pipeline stages. Rust controls compare streaming versus server-buffered response delivery.

Schema fidelity also matters: current native Arrow exporter represents ClickHouse DateTime as UInt32, without its timezone (SchemaConverter.cpp, TypeIndex::DateTime). The reference CSV explicitly records EventTime:uint32 for its ArrowStream table and type_restoration=false. A checked Arrow record batch alone does not prove a restored source timestamp contract. Primary probes measure wire decoding. Final wide Arrow confirmations explicitly restore this immutable fixture's discovered non-null DateTime('UTC') columns from UInt32 seconds to TimestampSecond/UTC, and include the widening cost. The utility rejects unexpected temporal types and never infers a timezone for arbitrary input. Full production results already include their source-schema handling.

Exact-tag HTTPHandler.cpp buffers the first http_response_buffer_size bytes in memory and, with wait_end_of_query=true, the remainder through TemporaryDataBuffer; pushDelayedResults rereads those bytes before sending. TemporaryDataOnDisk.cpp wraps its temporary output in CompressedWriteBuffer (fallback LZ4 when the inherited codec is empty). This is extra server buffering/compression/file I/O even when the public output codec is none. Record file/external-processing counters and header latency; do not infer the managed temporary codec without evidence. Plain response finalization runs before QueryFinish accounting in the exact upstream implementation, so its flush send counters are included.
