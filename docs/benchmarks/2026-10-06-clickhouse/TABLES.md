# Measured tables

## Primary transport decoding

| Profile | Mode | Columns | Codec / HTTP | M rows/s | M rows/client CPU-s | Client cores | RSS MiB | M rows/server CPU-s | n | Seconds/run |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| ArrowStream · t16/d8 · default wire profile | decode | 10 | lz4 / none | 1.467 | 1.044 | 1.41 | 24 | 0.813 | 1 | 123–123 |
| ArrowStream · t16/d8 · default wire profile | decode | 10 | none / none | 0.798 | 0.445 | 1.79 | 18 | 1.545 | 1 | 125–125 |
| ArrowStream · t16/d8 · default wire profile | decode | 10 | zstd / none | 1.572 | 1.119 | 1.40 | 31 | 0.721 | 2 | 120–122 |
| NativeTCP · t16/d8 · default wire profile | decode | 10 | lz4 / none | 1.500 | 2.166 | 0.69 | 96 | 1.007 | 2 | 121–126 |
| NativeTCP · t16/d8 · default wire profile | decode | 10 | none / none | 1.113 | 1.781 | 0.62 | 89 | 1.734 | 1 | 126–126 |
| NativeTCP · t16/d8 · default wire profile | decode | 10 | zstd / none | 1.282 | 1.953 | 0.66 | 92 | 0.871 | 1 | 125–125 |
| Parquet · t16/d8 · default wire profile | decode | 10 | lz4 / none | 2.344 | 1.235 | 1.90 | 1316 | 0.385 | 1 | 124–124 |
| Parquet · t16/d8 · default wire profile | decode | 10 | none / none | 0.990 | 0.579 | 1.71 | 3322 | 0.409 | 1 | 121–121 |
| Parquet · t16/d8 · default wire profile | decode | 10 | zstd / none | 2.813 | 1.285 | 2.19 | 1040 | 0.314 | 2 | 122–124 |
| ArrowStream · t16/d8 · default wire profile | decode | 50 | lz4 / none | 0.790 | 0.593 | 1.33 | 27 | 0.559 | 1 | 127–127 |
| ArrowStream · t16/d8 · default wire profile | decode | 50 | none / none | 0.604 | 0.334 | 1.81 | 17 | 1.065 | 1 | 132–132 |
| ArrowStream · t16/d8 · default wire profile | decode | 50 | zstd / none | 1.230 | 0.881 | 1.40 | 31 | 0.480 | 2 | 121–123 |
| NativeTCP · t16/d8 · default wire profile | decode | 50 | lz4 / none | 0.937 | 1.234 | 0.76 | 123 | 0.743 | 2 | 120–125 |
| NativeTCP · t16/d8 · default wire profile | decode | 50 | none / none | 0.711 | 1.083 | 0.66 | 124 | 1.236 | 1 | 127–127 |
| NativeTCP · t16/d8 · default wire profile | decode | 50 | zstd / none | 0.825 | 1.214 | 0.68 | 119 | 0.640 | 1 | 121–121 |
| Parquet · t16/d8 · default wire profile | decode | 50 | lz4 / none | 1.886 | 1.004 | 1.88 | 1630 | 0.254 | 1 | 122–122 |
| Parquet · t16/d8 · default wire profile | decode | 50 | none / none | 0.810 | 0.479 | 1.69 | 3736 | 0.270 | 1 | 123–123 |
| Parquet · t16/d8 · default wire profile | decode | 50 | zstd / none | 2.267 | 1.032 | 2.20 | 1303 | 0.213 | 2 | 121–122 |
| ArrowStream · t16/d8 · default wire profile | decode | 105 | lz4 / none | 0.475 | 0.370 | 1.29 | 31 | 0.382 | 2 | 126–127 |
| ArrowStream · t16/d8 · default wire profile | decode | 105 | none / none | 0.401 | 0.224 | 1.79 | 31 | 0.690 | 2 | 122–128 |
| ArrowStream · t16/d8 · default wire profile | decode | 105 | zstd / none | 0.922 | 0.673 | 1.37 | 31 | 0.333 | 2 | 121–128 |
| NativeTCP · t16/d8 · default wire profile | decode | 105 | lz4 / none | 0.490 | 0.577 | 0.85 | 203 | 0.556 | 2 | 120–125 |
| NativeTCP · t16/d8 · default wire profile | decode | 105 | none / none | 0.383 | 0.520 | 0.74 | 187 | 0.814 | 2 | 128–133 |
| NativeTCP · t16/d8 · default wire profile | decode | 105 | zstd / none | 0.448 | 0.573 | 0.78 | 241 | 0.467 | 2 | 133–135 |
| Parquet · t16/d8 · default wire profile | decode | 105 | lz4 / none | 1.310 | 0.708 | 1.85 | 2291 | 0.158 | 2 | 120–124 |
| Parquet · t16/d8 · default wire profile | decode | 105 | none / none | 0.652 | 0.387 | 1.68 | 4654 | 0.167 | 2 | 130–131 |
| Parquet · t16/d8 · default wire profile | decode | 105 | zstd / none | 1.692 | 0.721 | 2.35 | 1897 | 0.137 | 2 | 120–122 |

## Confirmed wide decoded finalists

| Profile | Mode | Columns | Codec / HTTP | M rows/s | M rows/client CPU-s | Client cores | RSS MiB | M rows/server CPU-s | n | Seconds/run |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| Parquet · t16/d8 · binary, group=62500 | decode | 105 | zstd / none | 1.897 | 0.723 | 2.62 | 1800 | 0.135 | 2 | 63–123 |
| ArrowStream · t16/d8 · binary, parallel IPC, UTC restored, block=65409, preferred bytes=0 | decode | 105 | zstd / none | 1.202 | 0.811 | 1.48 | 460 | 0.368 | 2 | 124–125 |
| Arrow · t16/d8 · binary, UTC restored | decode | 105 | zstd / none | 0.716 | 0.809 | 0.89 | 1192 | 0.328 | 2 | 126–126 |
| NativeTCP · t16/d8 · default wire profile | decode | 105 | lz4 / none | 0.490 | 0.577 | 0.85 | 203 | 0.556 | 2 | 120–125 |

## Official C++ / C transport controls

| Profile | Mode | Columns | Codec / HTTP | M rows/s | M rows/client CPU-s | Client cores | RSS MiB | M rows/server CPU-s | n | Seconds/run |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| NativeTCP-C++-26.7.5.10 · t16/d1 · default wire profile | decode | 105 | none / none | 1.201 | 1.249 | 0.96 | 281 | 0.942 | 1 | 125–125 |
| NativeTCP-C++-26.9.5.2 · t16/d1 · default wire profile | decode | 105 | none / none | 1.052 | 1.130 | 0.93 | 307 | 0.988 | 1 | 124–124 |
| NativeTCP-C++-26.7.5.10 · t16/d1 · default wire profile | decode | 105 | lz4 / none | 1.042 | 1.326 | 0.79 | 276 | 0.604 | 1 | 125–125 |
| NativeTCP-C++-26.9.5.2 · t16/d1 · default wire profile | decode | 105 | lz4 / none | 0.954 | 1.150 | 0.83 | 310 | 0.574 | 1 | 126–126 |
| NativeTCP-C++-26.7.5.10 · t16/d1 · default wire profile | decode | 105 | zstd / none | 0.781 | 1.079 | 0.72 | 279 | 0.503 | 1 | 128–128 |
| NativeTCP-C++-26.9.5.2 · t16/d1 · default wire profile | decode | 105 | zstd / none | 0.706 | 0.964 | 0.73 | 308 | 0.471 | 1 | 127–127 |
| Parquet-curl · t16/d1 · default wire profile | raw | 105 | none / none | 1.440 | 2.599 | 0.55 | 21 | 0.170 | 1 | 62–62 |
| ArrowStream-curl · t16/d1 · default wire profile | raw | 105 | none / none | 1.154 | 1.620 | 0.71 | 21 | 0.699 | 1 | 61–61 |

## Full production source → discard

| Profile | Mode | Columns | Codec / HTTP | M rows/s | M rows/client CPU-s | Client cores | RSS MiB | M rows/server CPU-s | n | Seconds/run |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| Production-parquet · t16/d8 · binary, group=62500 | decode | 105 | zstd / none | 1.762 | 0.817 | 2.16 | 3318 | 0.118 | 1 | 125–125 |
| Production-parquet · t16/d8 · binary | decode | 105 | lz4 / none | 1.709 | 0.873 | 1.96 | 4225 | 0.138 | 1 | 123–123 |
| Production-parquet · t16/d8 · binary | decode | 105 | zstd / none | 1.650 | 0.787 | 2.10 | 4380 | 0.119 | 1 | 121–121 |
| Production-native · t16/d8 · default wire profile | decode | 105 | lz4 / none | 0.440 | 0.501 | 0.88 | 442 | 0.510 | 1 | 136–136 |
| Production-native · t16/d8 · default wire profile | decode | 105 | zstd / none | 0.419 | 0.498 | 0.84 | 458 | 0.430 | 1 | 143–143 |
| Production-native · t16/d8 · default wire profile | decode | 105 | none / none | 0.341 | 0.451 | 0.76 | 438 | 0.727 | 1 | 147–147 |

## Every completed profile

| Profile | Mode | Columns | Codec / HTTP | M rows/s | M rows/client CPU-s | Client cores | RSS MiB | M rows/server CPU-s | n | Seconds/run |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| Parquet · t16/d8 · level=1 | decode | 10 | zstd / none | 2.905 | 1.286 | 2.26 | 1089 | 0.345 | 2 | 60–120 |
| Parquet · t16/d8 · default wire profile | decode | 10 | zstd / none | 2.813 | 1.285 | 2.19 | 1040 | 0.314 | 2 | 122–124 |
| Parquet · t16/d8 · default wire profile | decode | 10 | lz4 / none | 2.344 | 1.235 | 1.90 | 1316 | 0.385 | 1 | 124–124 |
| ArrowStream · t16/d8 · default wire profile | decode | 10 | zstd / none | 1.572 | 1.119 | 1.40 | 31 | 0.721 | 2 | 120–122 |
| NativeTCP · t16/d8 · default wire profile | decode | 10 | lz4 / none | 1.500 | 2.166 | 0.69 | 96 | 1.007 | 2 | 121–126 |
| ArrowStream · t16/d8 · default wire profile | decode | 10 | lz4 / none | 1.467 | 1.044 | 1.41 | 24 | 0.813 | 1 | 123–123 |
| NativeTCP · t16/d8 · default wire profile | decode | 10 | zstd / none | 1.282 | 1.953 | 0.66 | 92 | 0.871 | 1 | 125–125 |
| NativeTCP · t16/d8 · default wire profile | decode | 10 | none / none | 1.113 | 1.781 | 0.62 | 89 | 1.734 | 1 | 126–126 |
| Parquet · t16/d8 · default wire profile | decode | 10 | none / none | 0.990 | 0.579 | 1.71 | 3322 | 0.409 | 1 | 121–121 |
| ArrowStream · t16/d8 · default wire profile | decode | 10 | none / none | 0.798 | 0.445 | 1.79 | 18 | 1.545 | 1 | 125–125 |
| Parquet · t16/d8 · level=1 | raw | 10 | zstd / none | 4.286 | 3.725 | 1.15 | 22 | 0.347 | 1 | 61–61 |
| Parquet · t16/d8 · default wire profile | raw | 10 | zstd / none | 3.702 | 3.284 | 1.13 | 7 | 0.316 | 1 | 122–122 |
| Parquet · t16/d8 · default wire profile | raw | 10 | lz4 / none | 2.877 | 2.291 | 1.26 | 8 | 0.388 | 1 | 122–122 |
| ArrowStream · t16/d8 · default wire profile | raw | 10 | lz4 / none | 1.908 | 2.120 | 0.90 | 7 | 0.804 | 1 | 121–121 |
| ArrowStream · t16/d8 · default wire profile | raw | 10 | zstd / none | 1.805 | 2.874 | 0.63 | 8 | 0.712 | 1 | 122–122 |
| RowBinaryWithNamesAndTypes · t16/d8 · default wire profile | raw | 10 | none / none | 1.221 | 0.803 | 1.52 | 8 | 1.417 | 1 | 123–123 |
| ArrowStream · t16/d8 · default wire profile | raw | 10 | none / none | 1.130 | 0.746 | 1.51 | 8 | 1.549 | 1 | 124–124 |
| Parquet · t16/d8 · default wire profile | raw | 10 | none / none | 1.082 | 0.765 | 1.41 | 8 | 0.413 | 1 | 120–120 |
| CSVWithNames · t16/d8 · default wire profile | raw | 10 | none / none | 0.966 | 0.632 | 1.53 | 7 | 1.141 | 1 | 124–124 |
| Parquet · t16/d8 · default wire profile | raw | 10 | none / zstd | 0.763 | 3.409 | 0.22 | 7 | 0.280 | 1 | 131–131 |
| JSONEachRow · t16/d8 · default wire profile | raw | 10 | none / none | 0.739 | 0.486 | 1.52 | 7 | 0.687 | 1 | 122–122 |
| RowBinaryWithNamesAndTypes · t16/d8 · default wire profile | raw | 10 | none / zstd | 0.722 | 3.104 | 0.23 | 8 | 0.512 | 1 | 125–125 |
| ArrowStream · t16/d8 · default wire profile | raw | 10 | none / zstd | 0.632 | 3.110 | 0.20 | 7 | 0.517 | 1 | 127–127 |
| CSVWithNames · t16/d8 · default wire profile | raw | 10 | none / zstd | 0.574 | 2.961 | 0.19 | 7 | 0.409 | 1 | 122–122 |
| JSONEachRow · t16/d8 · default wire profile | raw | 10 | none / zstd | 0.525 | 2.913 | 0.18 | 8 | 0.315 | 1 | 133–133 |
| RowBinaryWithNamesAndTypes · t16/d8 · default wire profile | raw | 10 | none / gzip | 0.404 | 2.773 | 0.15 | 8 | 0.328 | 1 | 124–124 |
| CSVWithNames · t16/d8 · default wire profile | raw | 10 | none / gzip | 0.353 | 2.528 | 0.14 | 7 | 0.281 | 1 | 142–142 |
| RowBinaryWithNamesAndTypes · t16/d8 · default wire profile | raw | 10 | none / lz4 | 0.316 | 2.510 | 0.13 | 7 | 0.266 | 1 | 127–127 |
| Parquet · t16/d8 · default wire profile | raw | 10 | none / lz4 | 0.309 | 3.071 | 0.10 | 8 | 0.179 | 1 | 130–130 |
| ArrowStream · t16/d8 · default wire profile | raw | 10 | none / lz4 | 0.300 | 2.605 | 0.12 | 7 | 0.271 | 1 | 133–133 |
| JSONEachRow · t16/d8 · default wire profile | raw | 10 | none / gzip | 0.294 | 2.278 | 0.13 | 8 | 0.214 | 1 | 136–136 |
| CSVWithNames · t16/d8 · default wire profile | raw | 10 | none / lz4 | 0.270 | 2.233 | 0.12 | 7 | 0.226 | 1 | 148–148 |
| JSONEachRow · t16/d8 · default wire profile | raw | 10 | none / lz4 | 0.230 | 1.965 | 0.12 | 8 | 0.177 | 1 | 130–130 |
| Parquet · t16/d8 · level=1 | decode | 50 | zstd / none | 2.480 | 1.052 | 2.36 | 1370 | 0.231 | 2 | 61–121 |
| Parquet · t16/d8 · default wire profile | decode | 50 | zstd / none | 2.267 | 1.032 | 2.20 | 1303 | 0.213 | 2 | 121–122 |
| Parquet · t16/d8 · default wire profile | decode | 50 | lz4 / none | 1.886 | 1.004 | 1.88 | 1630 | 0.254 | 1 | 122–122 |
| ArrowStream · t16/d8 · default wire profile | decode | 50 | zstd / none | 1.230 | 0.881 | 1.40 | 31 | 0.480 | 2 | 121–123 |
| NativeTCP · t16/d8 · default wire profile | decode | 50 | lz4 / none | 0.937 | 1.234 | 0.76 | 123 | 0.743 | 2 | 120–125 |
| NativeTCP · t16/d8 · default wire profile | decode | 50 | zstd / none | 0.825 | 1.214 | 0.68 | 119 | 0.640 | 1 | 121–121 |
| Parquet · t16/d8 · default wire profile | decode | 50 | none / none | 0.810 | 0.479 | 1.69 | 3736 | 0.270 | 1 | 123–123 |
| ArrowStream · t16/d8 · default wire profile | decode | 50 | lz4 / none | 0.790 | 0.593 | 1.33 | 27 | 0.559 | 1 | 127–127 |
| NativeTCP · t16/d8 · default wire profile | decode | 50 | none / none | 0.711 | 1.083 | 0.66 | 124 | 1.236 | 1 | 127–127 |
| ArrowStream · t16/d8 · default wire profile | decode | 50 | none / none | 0.604 | 0.334 | 1.81 | 17 | 1.065 | 1 | 132–132 |
| Parquet · t16/d8 · default wire profile | raw | 50 | zstd / none | 2.929 | 2.732 | 1.07 | 7 | 0.212 | 1 | 123–123 |
| Parquet · t16/d8 · level=1 | raw | 50 | zstd / none | 2.572 | 2.513 | 1.02 | 22 | 0.234 | 1 | 62–62 |
| Parquet · t16/d8 · default wire profile | raw | 50 | lz4 / none | 2.212 | 1.949 | 1.13 | 7 | 0.256 | 1 | 122–122 |
| ArrowStream · t16/d8 · default wire profile | raw | 50 | lz4 / none | 1.584 | 1.684 | 0.94 | 7 | 0.561 | 1 | 126–126 |
| ArrowStream · t16/d8 · default wire profile | raw | 50 | zstd / none | 1.539 | 2.334 | 0.66 | 7 | 0.477 | 1 | 123–123 |
| Parquet · t16/d8 · default wire profile | raw | 50 | none / none | 0.945 | 0.688 | 1.37 | 7 | 0.273 | 1 | 127–127 |
| RowBinaryWithNamesAndTypes · t16/d8 · default wire profile | raw | 50 | none / none | 0.874 | 0.584 | 1.50 | 7 | 0.616 | 1 | 126–126 |
| ArrowStream · t16/d8 · default wire profile | raw | 50 | none / none | 0.816 | 0.544 | 1.50 | 7 | 1.068 | 1 | 123–123 |
| CSVWithNames · t16/d8 · default wire profile | raw | 50 | none / none | 0.666 | 0.441 | 1.51 | 8 | 0.568 | 1 | 120–120 |
| Parquet · t16/d8 · default wire profile | raw | 50 | none / zstd | 0.623 | 3.059 | 0.20 | 7 | 0.197 | 1 | 128–128 |
| RowBinaryWithNamesAndTypes · t16/d8 · default wire profile | raw | 50 | none / zstd | 0.560 | 2.737 | 0.20 | 7 | 0.312 | 1 | 125–125 |
| ArrowStream · t16/d8 · default wire profile | raw | 50 | none / zstd | 0.495 | 2.417 | 0.20 | 7 | 0.392 | 1 | 121–121 |
| CSVWithNames · t16/d8 · default wire profile | raw | 50 | none / zstd | 0.451 | 2.301 | 0.20 | 7 | 0.267 | 1 | 133–133 |
| JSONEachRow · t16/d8 · default wire profile | raw | 50 | none / zstd | 0.335 | 2.223 | 0.15 | 7 | 0.159 | 1 | 149–149 |
| JSONEachRow · t16/d8 · default wire profile | raw | 50 | none / none | 0.335 | 0.224 | 1.49 | 7 | 0.253 | 1 | 149–149 |
| RowBinaryWithNamesAndTypes · t16/d8 · default wire profile | raw | 50 | none / gzip | 0.330 | 2.123 | 0.16 | 7 | 0.224 | 1 | 121–121 |
| Parquet · t16/d8 · default wire profile | raw | 50 | none / lz4 | 0.258 | 2.307 | 0.11 | 7 | 0.132 | 1 | 155–155 |
| CSVWithNames · t16/d8 · default wire profile | raw | 50 | none / gzip | 0.256 | 2.224 | 0.12 | 7 | 0.184 | 1 | 156–156 |
| RowBinaryWithNamesAndTypes · t16/d8 · default wire profile | raw | 50 | none / lz4 | 0.240 | 2.036 | 0.12 | 7 | 0.179 | 1 | 125–125 |
| ArrowStream · t16/d8 · default wire profile | raw | 50 | none / lz4 | 0.237 | 1.935 | 0.12 | 7 | 0.210 | 1 | 127–127 |
| CSVWithNames · t16/d8 · default wire profile | raw | 50 | none / lz4 | 0.214 | 1.933 | 0.11 | 7 | 0.161 | 1 | 140–140 |
| JSONEachRow · t16/d8 · default wire profile | raw | 50 | none / lz4 | 0.157 | 1.445 | 0.11 | 7 | 0.103 | 1 | 127–127 |
| JSONEachRow · t16/d8 · default wire profile | raw | 50 | none / gzip | 0.155 | 1.639 | 0.09 | 7 | 0.103 | 1 | 129–129 |
| Parquet · t16/d8 · binary, group=62500 | decode | 105 | zstd / none | 1.897 | 0.723 | 2.62 | 1800 | 0.135 | 2 | 63–123 |
| Production-parquet · t16/d8 · binary, group=62500 | decode | 105 | zstd / none | 1.762 | 0.817 | 2.16 | 3318 | 0.118 | 1 | 125–125 |
| Production-parquet · t16/d8 · binary | decode | 105 | lz4 / none | 1.709 | 0.873 | 1.96 | 4225 | 0.138 | 1 | 123–123 |
| Parquet · t16/d8 · default wire profile | decode | 105 | zstd / none | 1.692 | 0.721 | 2.35 | 1897 | 0.137 | 2 | 120–122 |
| Production-parquet · t16/d8 · binary | decode | 105 | zstd / none | 1.650 | 0.787 | 2.10 | 4380 | 0.119 | 1 | 121–121 |
| Parquet · t16/d8 · binary, SETTINGS output_format_parquet_write_page_index=0, output_format_parquet_write_bloom_filter=0 | decode | 105 | zstd / none | 1.636 | 0.769 | 2.13 | 1765 | 0.164 | 1 | 122–122 |
| Parquet · t16/d8 · binary | decode | 105 | zstd / none | 1.579 | 0.752 | 2.10 | 1898 | 0.137 | 3 | 64–125 |
| Parquet · t16/d8 · binary, block=65409, preferred bytes=0 | decode | 105 | zstd / none | 1.568 | 0.737 | 2.13 | 1926 | 0.147 | 1 | 64–64 |
| Parquet · t16/d8 · binary, SETTINGS output_format_parquet_max_dictionary_size=0 | decode | 105 | zstd / none | 1.556 | 0.603 | 2.58 | 2548 | 0.162 | 4 | 62–124 |
| Parquet · t16/d16 · binary | decode | 105 | zstd / none | 1.533 | 0.635 | 2.42 | 2548 | 0.137 | 1 | 65–65 |
| Parquet · t32/d8 · binary | decode | 105 | zstd / none | 1.526 | 0.752 | 2.03 | 1887 | 0.099 | 1 | 66–66 |
| Parquet · t16/d8 · binary, SETTINGS output_format_parquet_write_page_index=0, output_format_parquet_write_bloom_filter=0 | decode | 105 | lz4 / none | 1.477 | 0.759 | 1.95 | 2171 | 0.193 | 1 | 122–122 |
| Parquet · t16/d8 · binary, group=1000000 | decode | 105 | zstd / none | 1.417 | 0.755 | 1.88 | 1881 | 0.137 | 1 | 63–63 |
| Parquet · t16/d8 · level=1 | decode | 105 | zstd / none | 1.402 | 0.690 | 2.03 | 1953 | 0.147 | 1 | 64–64 |
| Parquet · t16/d4 · binary | decode | 105 | zstd / none | 1.402 | 0.753 | 1.86 | 1516 | 0.137 | 1 | 64–64 |
| Parquet · t16/d8 · default wire profile | decode | 105 | gzip / none | 1.366 | 0.622 | 2.19 | 1974 | 0.113 | 1 | 124–124 |
| Parquet · t16/d8 · binary, block=65409, preferred bytes=0 | decode | 105 | lz4 / none | 1.354 | 0.719 | 1.88 | 2300 | 0.167 | 1 | 66–66 |
| Parquet · t16/d8 · default wire profile | decode | 105 | lz4 / none | 1.310 | 0.708 | 1.85 | 2291 | 0.158 | 2 | 120–124 |
| Parquet · t16/d8 · binary | decode | 105 | lz4 / none | 1.296 | 0.719 | 1.80 | 2275 | 0.158 | 1 | 62–62 |
| ArrowStream · t16/d8 · binary, parallel IPC, block=65409, preferred bytes=0 | decode | 105 | zstd / none | 1.207 | 0.782 | 1.54 | 430 | 0.368 | 1 | 66–66 |
| ArrowStream · t16/d8 · binary, parallel IPC, UTC restored, block=65409, preferred bytes=0 | decode | 105 | zstd / none | 1.202 | 0.811 | 1.48 | 460 | 0.368 | 2 | 124–125 |
| NativeTCP-C++-26.7.5.10 · t16/d1 · default wire profile | decode | 105 | none / none | 1.201 | 1.249 | 0.96 | 281 | 0.942 | 1 | 125–125 |
| ArrowStream · t16/d8 · binary, parallel IPC, block=65409, preferred bytes=0 | decode | 105 | lz4 / none | 1.200 | 0.610 | 1.97 | 498 | 0.433 | 1 | 67–67 |
| Parquet · t8/d8 · binary | decode | 105 | zstd / none | 1.197 | 0.699 | 1.71 | 1875 | 0.174 | 1 | 67–67 |
| Parquet · t16/d8 · default wire profile | decode | 105 | snappy / none | 1.180 | 0.638 | 1.85 | 2390 | 0.161 | 1 | 127–127 |
| ArrowStream · t8/d8 · binary, parallel IPC | decode | 105 | zstd / none | 1.149 | 0.723 | 1.59 | 68 | 0.333 | 1 | 61–61 |
| Parquet · t16/d8 · default wire profile | decode | 105 | brotli / none | 1.142 | 0.377 | 3.03 | 2033 | 0.115 | 1 | 123–123 |
| ArrowStream · t16/d8 · binary, parallel IPC | decode | 105 | zstd / none | 1.141 | 0.732 | 1.56 | 78 | 0.331 | 1 | 61–61 |
| ArrowStream · t16/d16 · binary, parallel IPC | decode | 105 | zstd / none | 1.139 | 0.723 | 1.58 | 130 | 0.330 | 1 | 61–61 |
| ArrowStream · t16/d1 · binary, parallel IPC | decode | 105 | zstd / none | 1.139 | 0.762 | 1.49 | 22 | 0.330 | 1 | 61–61 |
| ArrowStream · t16/d4 · binary, parallel IPC | decode | 105 | zstd / none | 1.138 | 0.726 | 1.57 | 48 | 0.330 | 1 | 62–62 |
| ArrowStream · t16/d8 · parallel IPC | decode | 105 | zstd / none | 1.136 | 0.617 | 1.84 | 76 | 0.329 | 1 | 62–62 |
| ArrowStream · t16/d8 · binary | decode | 105 | zstd / none | 1.136 | 0.802 | 1.42 | 22 | 0.330 | 1 | 62–62 |
| ArrowStream · t16/d8 · binary, parallel IPC | decode | 105 | lz4 / none | 1.128 | 0.377 | 3.00 | 153 | 0.390 | 1 | 62–62 |
| ArrowStream · t16/d8 · parallel IPC | decode | 105 | lz4 / none | 1.127 | 0.350 | 3.22 | 155 | 0.389 | 1 | 62–62 |
| ArrowStream · t32/d8 · binary, parallel IPC | decode | 105 | zstd / none | 1.124 | 0.686 | 1.64 | 79 | 0.321 | 1 | 62–62 |
| NativeTCP-C++-26.9.5.2 · t16/d1 · default wire profile | decode | 105 | none / none | 1.052 | 1.130 | 0.93 | 307 | 0.988 | 1 | 124–124 |
| NativeTCP-C++-26.7.5.10 · t16/d1 · default wire profile | decode | 105 | lz4 / none | 1.042 | 1.326 | 0.79 | 276 | 0.604 | 1 | 125–125 |
| NativeTCP-C++-26.9.5.2 · t16/d1 · default wire profile | decode | 105 | lz4 / none | 0.954 | 1.150 | 0.83 | 310 | 0.574 | 1 | 126–126 |
| ArrowStream · t16/d8 · default wire profile | decode | 105 | zstd / none | 0.922 | 0.673 | 1.37 | 31 | 0.333 | 2 | 121–128 |
| ArrowStream · t16/d8 · binary, parallel IPC, SETTINGS output_format_arrow_record_batch_size=65536 | decode | 105 | zstd / none | 0.821 | 0.712 | 1.15 | 428 | 0.309 | 1 | 61–61 |
| Parquet · t16/d1 · binary | decode | 105 | zstd / none | 0.820 | 0.773 | 1.06 | 1222 | 0.138 | 1 | 61–61 |
| ArrowStream · t16/d8 · binary, SETTINGS output_format_arrow_record_batch_size=65536 | decode | 105 | zstd / none | 0.814 | 0.698 | 1.17 | 72 | 0.308 | 1 | 61–61 |
| NativeTCP-C++-26.7.5.10 · t16/d1 · default wire profile | decode | 105 | zstd / none | 0.781 | 1.079 | 0.72 | 279 | 0.503 | 1 | 128–128 |
| Arrow · t16/d8 · binary, UTC restored | decode | 105 | zstd / none | 0.716 | 0.809 | 0.89 | 1192 | 0.328 | 2 | 126–126 |
| NativeTCP-C++-26.9.5.2 · t16/d1 · default wire profile | decode | 105 | zstd / none | 0.706 | 0.964 | 0.73 | 308 | 0.471 | 1 | 127–127 |
| ArrowStream · t16/d8 · binary, SETTINGS output_format_arrow_record_batch_size=250000 | decode | 105 | zstd / none | 0.702 | 0.775 | 0.91 | 240 | 0.306 | 1 | 71–71 |
| Parquet · t16/d8 · default wire profile | decode | 105 | none / none | 0.652 | 0.387 | 1.68 | 4654 | 0.167 | 2 | 130–131 |
| ArrowStream · t16/d8 · binary, parallel IPC, SETTINGS output_format_arrow_record_batch_size=250000 | decode | 105 | zstd / none | 0.645 | 0.737 | 0.88 | 1347 | 0.304 | 1 | 62–62 |
| Parquet · t16/d8 · binary | decode | 105 | none / none | 0.632 | 0.383 | 1.65 | 4633 | 0.167 | 1 | 63–63 |
| Arrow · t16/d8 · default wire profile | decode | 105 | zstd / none | 0.612 | 0.691 | 0.89 | 1192 | 0.329 | 1 | 65–65 |
| ArrowStream · t16/d8 · binary | decode | 105 | none / none | 0.548 | 0.293 | 1.87 | 29 | 0.702 | 1 | 73–73 |
| ArrowStream · t16/d8 · binary | decode | 105 | lz4 / none | 0.544 | 0.428 | 1.27 | 30 | 0.386 | 1 | 73–73 |
| ArrowStream · t16/d8 · binary, parallel IPC | decode | 105 | none / none | 0.541 | 0.286 | 1.89 | 31 | 0.682 | 1 | 74–74 |
| ArrowStream · t16/d8 · parallel IPC | decode | 105 | none / none | 0.493 | 0.243 | 2.03 | 34 | 0.696 | 1 | 61–61 |
| NativeTCP · t16/d8 · default wire profile | decode | 105 | lz4 / none | 0.490 | 0.577 | 0.85 | 203 | 0.556 | 2 | 120–125 |
| ArrowStream · t16/d8 · default wire profile | decode | 105 | lz4 / none | 0.475 | 0.370 | 1.29 | 31 | 0.382 | 2 | 126–127 |
| NativeTCP · t16/d8 · default wire profile | decode | 105 | zstd / none | 0.448 | 0.573 | 0.78 | 241 | 0.467 | 2 | 133–135 |
| Production-native · t16/d8 · default wire profile | decode | 105 | lz4 / none | 0.440 | 0.501 | 0.88 | 442 | 0.510 | 1 | 136–136 |
| Arrow · t16/d8 · default wire profile | decode | 105 | none / none | 0.420 | 0.272 | 1.55 | 6666 | 0.709 | 1 | 71–71 |
| Production-native · t16/d8 · default wire profile | decode | 105 | zstd / none | 0.419 | 0.498 | 0.84 | 458 | 0.430 | 1 | 143–143 |
| Arrow · t16/d8 · default wire profile | decode | 105 | lz4 / none | 0.415 | 0.392 | 1.06 | 1735 | 0.387 | 1 | 72–72 |
| ArrowStream · t16/d8 · default wire profile | decode | 105 | none / none | 0.401 | 0.224 | 1.79 | 31 | 0.690 | 2 | 122–128 |
| NativeTCP · t16/d8 · default wire profile | decode | 105 | none / none | 0.383 | 0.520 | 0.74 | 187 | 0.814 | 2 | 128–133 |
| Production-native · t16/d8 · default wire profile | decode | 105 | none / none | 0.341 | 0.451 | 0.76 | 438 | 0.727 | 1 | 147–147 |
| Parquet · t16/d8 · level=1 | raw | 105 | zstd / none | 2.408 | 1.995 | 1.21 | 22 | 0.148 | 1 | 62–62 |
| Parquet · t16/d8 · default wire profile | raw | 105 | zstd / none | 2.007 | 1.950 | 1.03 | 31 | 0.137 | 2 | 122–122 |
| Parquet · t16/d8 · default wire profile | raw | 105 | brotli / none | 1.715 | 1.883 | 0.91 | 8 | 0.115 | 1 | 122–122 |
| Parquet · t16/d8 · default wire profile | raw | 105 | gzip / none | 1.642 | 1.815 | 0.90 | 7 | 0.112 | 1 | 122–122 |
| Parquet · t16/d8 · default wire profile | raw | 105 | lz4 / none | 1.598 | 1.459 | 1.10 | 7 | 0.158 | 1 | 125–125 |
| Parquet-curl · t16/d1 · default wire profile | raw | 105 | none / none | 1.440 | 2.599 | 0.55 | 21 | 0.170 | 1 | 62–62 |
| Parquet · t16/d8 · default wire profile | raw | 105 | snappy / none | 1.411 | 1.322 | 1.07 | 7 | 0.162 | 1 | 121–121 |
| ArrowStream-curl · t16/d1 · default wire profile | raw | 105 | none / none | 1.154 | 1.620 | 0.71 | 21 | 0.699 | 1 | 61–61 |
| ArrowStream · t16/d8 · default wire profile | raw | 105 | zstd / none | 1.142 | 1.678 | 0.68 | 31 | 0.330 | 2 | 123–123 |
| Arrow · t16/d8 · default wire profile | raw | 105 | zstd / none | 1.139 | 1.718 | 0.66 | 22 | 0.330 | 1 | 61–61 |
| Arrow · t16/d8 · default wire profile | raw | 105 | lz4 / none | 1.126 | 1.249 | 0.90 | 22 | 0.389 | 1 | 62–62 |
| ArrowStream · t16/d8 · default wire profile | raw | 105 | lz4 / none | 1.123 | 1.206 | 0.93 | 7 | 0.388 | 1 | 125–125 |
| Parquet · t16/d8 · HTTP level=0 | raw | 105 | none / lz4 | 0.892 | 1.473 | 0.61 | 22 | 0.152 | 1 | 67–67 |
| RowBinaryWithNamesAndTypes · t16/d8 · HTTP level=0 | raw | 105 | none / lz4 | 0.847 | 1.249 | 0.68 | 22 | 0.256 | 1 | 71–71 |
| Native · t16/d8 · HTTP level=0 | raw | 105 | none / lz4 | 0.821 | 1.704 | 0.48 | 22 | 0.510 | 1 | 61–61 |
| Parquet · t16/d8 · default wire profile | raw | 105 | none / none | 0.716 | 0.544 | 1.32 | 22 | 0.167 | 2 | 68–128 |
| ArrowStream · t16/d8 · HTTP level=0 | raw | 105 | none / lz4 | 0.703 | 1.336 | 0.53 | 22 | 0.464 | 1 | 71–71 |
| Native · t16/d8 · HTTP level=1 | raw | 105 | none / none | 0.657 | 0.444 | 1.48 | 22 | 0.771 | 1 | 61–61 |
| CSVWithNames · t16/d8 · HTTP level=0 | raw | 105 | none / lz4 | 0.649 | 1.032 | 0.63 | 22 | 0.227 | 1 | 62–62 |
| Native · t16/d8 · HTTP level=1 | raw | 105 | none / zstd | 0.591 | 2.057 | 0.29 | 22 | 0.409 | 1 | 68–68 |
| RowBinaryWithNamesAndTypes · t16/d8 · default wire profile | raw | 105 | none / none | 0.584 | 0.393 | 1.49 | 7 | 0.331 | 1 | 137–137 |
| Arrow · t16/d8 · default wire profile | raw | 105 | none / none | 0.515 | 0.341 | 1.51 | 22 | 0.697 | 1 | 78–78 |
| ArrowStream · t16/d8 · default wire profile | raw | 105 | none / none | 0.510 | 0.340 | 1.50 | 22 | 0.692 | 2 | 76–122 |
| Parquet · t16/d8 · default wire profile | raw | 105 | none / zstd | 0.496 | 1.821 | 0.27 | 7 | 0.131 | 1 | 121–121 |
| CSVWithNames · t16/d8 · default wire profile | raw | 105 | none / none | 0.453 | 0.299 | 1.52 | 7 | 0.305 | 1 | 132–132 |
| Parquet · t16/d8 · wait=1 | raw | 105 | none / none | 0.449 | 0.565 | 0.79 | 22 | 0.144 | 1 | 67–67 |
| ORC · t16/d8 · default wire profile | raw | 105 | none / none | 0.427 | 0.585 | 0.73 | 22 | 0.326 | 1 | 70–70 |
| RowBinaryWithNamesAndTypes · t16/d8 · default wire profile | raw | 105 | none / zstd | 0.419 | 1.939 | 0.22 | 7 | 0.196 | 1 | 143–143 |
| Native · t16/d8 · HTTP level=1 | raw | 105 | none / gzip | 0.409 | 2.097 | 0.20 | 22 | 0.313 | 1 | 73–73 |
| Parquet · t16/d8 · binary, SETTINGS output_format_parquet_max_dictionary_size=0 | raw | 105 | none / none | 0.399 | 0.287 | 1.39 | 22 | 0.192 | 1 | 75–75 |
| JSONEachRow · t16/d8 · HTTP level=0 | raw | 105 | none / lz4 | 0.393 | 0.846 | 0.46 | 22 | 0.109 | 1 | 76–76 |
| ArrowStream · t16/d8 · default wire profile | raw | 105 | none / zstd | 0.352 | 1.822 | 0.19 | 7 | 0.279 | 1 | 142–142 |
| ArrowStream · t16/d8 · wait=1 | raw | 105 | none / none | 0.331 | 0.407 | 0.81 | 22 | 0.379 | 1 | 60–60 |
| ORC · t16/d8 · default wire profile | raw | 105 | snappy / none | 0.326 | 1.677 | 0.19 | 22 | 0.279 | 1 | 61–61 |
| ORC · t16/d8 · default wire profile | raw | 105 | lz4 / none | 0.308 | 0.589 | 0.52 | 22 | 0.308 | 1 | 65–65 |
| ORC · t16/d8 · default wire profile | raw | 105 | zstd / none | 0.306 | 2.148 | 0.14 | 22 | 0.261 | 1 | 65–65 |
| CSVWithNames · t16/d8 · default wire profile | raw | 105 | none / zstd | 0.303 | 1.692 | 0.18 | 7 | 0.161 | 1 | 132–132 |
| RowBinaryWithNamesAndTypes · t16/d8 · default wire profile | raw | 105 | none / gzip | 0.249 | 1.551 | 0.16 | 7 | 0.147 | 1 | 121–121 |
| JSONEachRow · t16/d8 · default wire profile | raw | 105 | none / zstd | 0.209 | 1.369 | 0.15 | 7 | 0.087 | 1 | 144–144 |
| Parquet · t16/d8 · default wire profile | raw | 105 | none / lz4 | 0.192 | 1.554 | 0.12 | 7 | 0.088 | 1 | 156–156 |
| CSVWithNames · t16/d8 · default wire profile | raw | 105 | none / gzip | 0.192 | 1.321 | 0.15 | 7 | 0.123 | 1 | 157–157 |
| RowBinaryWithNamesAndTypes · t16/d8 · default wire profile | raw | 105 | none / lz4 | 0.186 | 1.500 | 0.12 | 7 | 0.123 | 1 | 162–162 |
| ORC · t16/d8 · default wire profile | raw | 105 | zlib / none | 0.186 | 2.114 | 0.09 | 22 | 0.164 | 1 | 108–108 |
| ArrowStream · t16/d8 · default wire profile | raw | 105 | none / lz4 | 0.176 | 1.351 | 0.13 | 7 | 0.156 | 1 | 170–170 |
| JSONEachRow · t16/d8 · default wire profile | raw | 105 | none / none | 0.161 | 0.107 | 1.51 | 7 | 0.129 | 1 | 124–124 |
| CSVWithNames · t16/d8 · default wire profile | raw | 105 | none / lz4 | 0.137 | 1.405 | 0.10 | 7 | 0.097 | 1 | 146–146 |
| JSONEachRow · t16/d8 · default wire profile | raw | 105 | none / lz4 | 0.100 | 1.070 | 0.09 | 7 | 0.061 | 1 | 199–199 |
| JSONEachRow · t16/d8 · default wire profile | raw | 105 | none / gzip | 0.100 | 1.084 | 0.09 | 7 | 0.059 | 1 | 201–201 |
