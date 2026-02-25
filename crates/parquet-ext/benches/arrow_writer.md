# Arrow Benchmark: Encoding Performance

## A/B comparison of row group column encoding: Arrow (sequential) vs parquet-ext (parallel)

- **Batch size**: 4,096 rows
- **Writer**: `Empty` sink (no I/O -- pure encoding)
- **Arrow**: Sequential column-by-column encoding via `ArrowRowGroupWriterFactory`
- **parquet-ext**: Parallel column encoding via `RowGroupEncoder::encode()` with thread-per-column work stealing

## Summary

Speedup = Arrow time / parquet-ext time. Values **> 1.0x** mean parquet-ext is faster.

| Data Type | Default | Bloom Filter | Parquet 2.0 | ZSTD | ZSTD + Pq2 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| primitive | **1.84x** | 0.99x | **1.29x** | 0.72x | 0.55x |
| primitive non null | 0.77x | 0.98x | 0.59x | 0.79x | 0.76x |
| bool | 0.55x | 0.65x | 0.62x | 0.61x | 0.64x |
| bool non null | 0.33x | 0.60x | 0.47x | 0.42x | 0.51x |
| string | **1.18x** | **1.74x** | **1.27x** | **1.45x** | **1.20x** |
| string non null | **1.49x** | **1.50x** | **1.44x** | **1.71x** | **1.78x** |
| string and binary view | **1.10x** | **1.11x** | **1.08x** | **1.36x** | **1.30x** |
| string dictionary | 0.86x | 0.92x | 0.97x | 0.95x | 0.94x |
| float with nans | **1.11x** | **1.03x** | 1.00x | **1.41x** | **1.19x** |
| list primitive | **1.21x** | **1.26x** | **1.39x** | **1.05x** | **1.18x** |
| list primitive non null | **1.34x** | **1.33x** | **1.22x** | **1.17x** | **1.15x** |

## Detailed Results

### Primitive

| Writer Property | Arrow Time | parquet-ext Time | Speedup | Arrow Thrpt | parquet-ext Thrpt |
| :--- | ---: | ---: | :---: | ---: | ---: |
| Default | 322.23 µs | 175.16 µs | **1.84x** | 545.99 MiB/s | 1004.4 MiB/s |
| Bloom Filter | 651.87 µs | 659.12 µs | 0.99x | 269.89 MiB/s | 266.92 MiB/s |
| Parquet 2.0 | 376.44 µs | 290.95 µs | **1.29x** | 467.37 MiB/s | 604.69 MiB/s |
| ZSTD | 554.52 µs | 769.43 µs | 0.72x | 317.27 MiB/s | 228.66 MiB/s |
| ZSTD + Pq2 | 735.95 µs | 1.3308 ms | 0.55x | 239.06 MiB/s | 132.20 MiB/s |

### Primitive Non Null

| Writer Property | Arrow Time | parquet-ext Time | Speedup | Arrow Thrpt | parquet-ext Thrpt |
| :--- | ---: | ---: | :---: | ---: | ---: |
| Default | 255.33 µs | 332.22 µs | 0.77x | 675.66 MiB/s | 519.27 MiB/s |
| Bloom Filter | 734.30 µs | 746.35 µs | 0.98x | 234.94 MiB/s | 231.15 MiB/s |
| Parquet 2.0 | 290.08 µs | 490.18 µs | 0.59x | 594.71 MiB/s | 351.94 MiB/s |
| ZSTD | 645.49 µs | 816.08 µs | 0.79x | 267.26 MiB/s | 211.40 MiB/s |
| ZSTD + Pq2 | 592.23 µs | 779.41 µs | 0.76x | 291.30 MiB/s | 221.34 MiB/s |

### Bool

| Writer Property | Arrow Time | parquet-ext Time | Speedup | Arrow Thrpt | parquet-ext Thrpt |
| :--- | ---: | ---: | :---: | ---: | ---: |
| Default | 27.763 µs | 50.144 µs | 0.55x | 38.198 MiB/s | 21.149 MiB/s |
| Bloom Filter | 46.233 µs | 70.883 µs | 0.65x | 22.938 MiB/s | 14.961 MiB/s |
| Parquet 2.0 | 33.121 µs | 53.217 µs | 0.62x | 32.019 MiB/s | 19.928 MiB/s |
| ZSTD | 32.192 µs | 52.496 µs | 0.61x | 32.943 MiB/s | 20.201 MiB/s |
| ZSTD + Pq2 | 36.506 µs | 57.114 µs | 0.64x | 29.050 MiB/s | 18.568 MiB/s |

### Bool Non Null

| Writer Property | Arrow Time | parquet-ext Time | Speedup | Arrow Thrpt | parquet-ext Thrpt |
| :--- | ---: | ---: | :---: | ---: | ---: |
| Default | 9.4085 µs | 28.164 µs | 0.33x | 60.818 MiB/s | 20.317 MiB/s |
| Bloom Filter | 30.214 µs | 50.474 µs | 0.60x | 18.938 MiB/s | 11.337 MiB/s |
| Parquet 2.0 | 16.282 µs | 34.956 µs | 0.47x | 35.144 MiB/s | 16.369 MiB/s |
| ZSTD | 13.287 µs | 31.624 µs | 0.42x | 43.066 MiB/s | 18.094 MiB/s |
| ZSTD + Pq2 | 20.109 µs | 39.699 µs | 0.51x | 28.455 MiB/s | 14.414 MiB/s |

### String

| Writer Property | Arrow Time | parquet-ext Time | Speedup | Arrow Thrpt | parquet-ext Thrpt |
| :--- | ---: | ---: | :---: | ---: | ---: |
| Default | 552.17 µs | 469.77 µs | **1.18x** | 3.6222 GiB/s | 4.2576 GiB/s |
| Bloom Filter | 934.63 µs | 538.10 µs | **1.74x** | 2.1400 GiB/s | 3.7170 GiB/s |
| Parquet 2.0 | 529.12 µs | 415.78 µs | **1.27x** | 3.7800 GiB/s | 4.8105 GiB/s |
| ZSTD | 1.5094 ms | 1.0415 ms | **1.45x** | 1.3251 GiB/s | 1.9204 GiB/s |
| ZSTD + Pq2 | 1.3461 ms | 1.1246 ms | **1.20x** | 1.4858 GiB/s | 1.7785 GiB/s |

### String Non Null

| Writer Property | Arrow Time | parquet-ext Time | Speedup | Arrow Thrpt | parquet-ext Thrpt |
| :--- | ---: | ---: | :---: | ---: | ---: |
| Default | 667.14 µs | 447.43 µs | **1.49x** | 2.9966 GiB/s | 4.4681 GiB/s |
| Bloom Filter | 944.04 µs | 630.33 µs | **1.50x** | 2.1176 GiB/s | 3.1716 GiB/s |
| Parquet 2.0 | 663.55 µs | 460.11 µs | **1.44x** | 3.0128 GiB/s | 4.3449 GiB/s |
| ZSTD | 1.8295 ms | 1.0698 ms | **1.71x** | 1.0927 GiB/s | 1.8687 GiB/s |
| ZSTD + Pq2 | 1.7526 ms | 985.74 µs | **1.78x** | 1.1407 GiB/s | 2.0281 GiB/s |

### String And Binary View

| Writer Property | Arrow Time | parquet-ext Time | Speedup | Arrow Thrpt | parquet-ext Thrpt |
| :--- | ---: | ---: | :---: | ---: | ---: |
| Default | 223.64 µs | 204.02 µs | **1.10x** | 564.27 MiB/s | 618.52 MiB/s |
| Bloom Filter | 348.98 µs | 313.11 µs | **1.11x** | 361.60 MiB/s | 403.02 MiB/s |
| Parquet 2.0 | 249.41 µs | 231.77 µs | **1.08x** | 505.96 MiB/s | 544.47 MiB/s |
| ZSTD | 447.43 µs | 328.81 µs | **1.36x** | 282.03 MiB/s | 383.78 MiB/s |
| ZSTD + Pq2 | 419.45 µs | 321.44 µs | **1.30x** | 300.85 MiB/s | 392.57 MiB/s |

### String Dictionary

| Writer Property | Arrow Time | parquet-ext Time | Speedup | Arrow Thrpt | parquet-ext Thrpt |
| :--- | ---: | ---: | :---: | ---: | ---: |
| Default | 248.46 µs | 287.86 µs | 0.86x | 4.0562 GiB/s | 3.5010 GiB/s |
| Bloom Filter | 429.93 µs | 465.79 µs | 0.92x | 2.3442 GiB/s | 2.1637 GiB/s |
| Parquet 2.0 | 251.90 µs | 258.72 µs | 0.97x | 4.0009 GiB/s | 3.8953 GiB/s |
| ZSTD | 720.15 µs | 756.77 µs | 0.95x | 1.3994 GiB/s | 1.3317 GiB/s |
| ZSTD + Pq2 | 694.32 µs | 736.75 µs | 0.94x | 1.4515 GiB/s | 1.3679 GiB/s |

### Float With Nans

| Writer Property | Arrow Time | parquet-ext Time | Speedup | Arrow Thrpt | parquet-ext Thrpt |
| :--- | ---: | ---: | :---: | ---: | ---: |
| Default | 251.14 µs | 226.41 µs | **1.11x** | 218.85 MiB/s | 242.75 MiB/s |
| Bloom Filter | 303.02 µs | 292.82 µs | **1.03x** | 181.38 MiB/s | 187.70 MiB/s |
| Parquet 2.0 | 387.83 µs | 387.84 µs | 1.00x | 141.72 MiB/s | 141.71 MiB/s |
| ZSTD | 356.92 µs | 253.61 µs | **1.41x** | 153.99 MiB/s | 216.72 MiB/s |
| ZSTD + Pq2 | 471.65 µs | 395.83 µs | **1.19x** | 116.53 MiB/s | 138.85 MiB/s |

### List Primitive

| Writer Property | Arrow Time | parquet-ext Time | Speedup | Arrow Thrpt | parquet-ext Thrpt |
| :--- | ---: | ---: | :---: | ---: | ---: |
| Default | 878.04 µs | 725.16 µs | **1.21x** | 2.3711 GiB/s | 2.8710 GiB/s |
| Bloom Filter | 1.0946 ms | 867.24 µs | **1.26x** | 1.9020 GiB/s | 2.4007 GiB/s |
| Parquet 2.0 | 919.72 µs | 661.81 µs | **1.39x** | 2.2637 GiB/s | 3.1458 GiB/s |
| ZSTD | 1.4311 ms | 1.3580 ms | **1.05x** | 1.4548 GiB/s | 1.5331 GiB/s |
| ZSTD + Pq2 | 1.5269 ms | 1.2940 ms | **1.18x** | 1.3635 GiB/s | 1.6090 GiB/s |

### List Primitive Non Null

| Writer Property | Arrow Time | parquet-ext Time | Speedup | Arrow Thrpt | parquet-ext Thrpt |
| :--- | ---: | ---: | :---: | ---: | ---: |
| Default | 949.77 µs | 709.77 µs | **1.34x** | 2.1873 GiB/s | 2.9270 GiB/s |
| Bloom Filter | 1.2179 ms | 917.86 µs | **1.33x** | 1.7058 GiB/s | 2.2634 GiB/s |
| Parquet 2.0 | 998.70 µs | 821.79 µs | **1.22x** | 2.0802 GiB/s | 2.5280 GiB/s |
| ZSTD | 2.0607 ms | 1.7650 ms | **1.17x** | 1.0081 GiB/s | 1.1771 GiB/s |
| ZSTD + Pq2 | 2.0530 ms | 1.7802 ms | **1.15x** | 1.0119 GiB/s | 1.1670 GiB/s |

## Analysis

- **parquet-ext wins 31/55** benchmark configurations (56%)
- **Best speedup**: 1.84x at `primitive/default`
- **Worst regression**: 0.33x at `bool_non_null/default`

### Where parallel encoding helps most

- **Multi-column schemas** (string, list_primitive) -- more columns to parallelize across
- **Compression-heavy configs** (ZSTD) -- CPU-bound work benefits from threading
- **Large string data** -- encoding + compression of string columns is expensive and parallelizable

### Where sequential Arrow is faster

- **Single-column schemas** (bool, bool_non_null) -- thread spawn overhead dominates tiny workloads
- **Dictionary-encoded data** -- already compact, little parallel work to distribute
- **Very small batches** -- 4,096 rows may not provide enough work to amortize threading overhead
