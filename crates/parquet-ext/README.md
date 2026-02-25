# parquet-ext

A high-performance, pipelined Parquet writer that parallelizes column encoding across threads. Drop-in async replacement for `parquet::arrow::AsyncArrowWriter` with up to **8x speedup** on multi-column schemas.

## Motivation

The standard Arrow Parquet writer encodes columns sequentially within each row group. For wide schemas or CPU-intensive compression (e.g., ZSTD), this leaves significant parallelism on the table. `parquet-ext` introduces a two-stage pipeline that encodes columns in parallel on a dedicated thread pool while an async writer thread streams completed row groups to the output.

## Architecture

``` text
                    ┌──────────────────────────────┐
  RecordBatch ──►   │     Encoder Thread Pool      │
                    │  (parallel column encoding)  │
                    └──────────────┬───────────────┘
                                   │ encoded row groups
                                   ▼
                    ┌──────────────────────────────┐
                    │     Async Writer Thread      │
                    │  (sequential row group I/O)  │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                              Parquet File
```

1. **Encoder stage** -- Receives batches via bounded channel, encodes leaf columns in parallel using work-stealing across a thread scope, and sends completed row groups to the writer.
2. **Writer stage** -- Receives encoded row groups, reorders them to preserve sequential ordering, and writes them to the async output.

## Usage

### Drop-in `AsyncArrowWriter` replacement

```rust
use parquet_ext::arrow::async_writer::AsyncArrowWriter;

let mut writer = AsyncArrowWriter::try_new(
    async_output,       // impl AsyncFileWriter + Send
    schema.clone(),     // Arrow SchemaRef
    Some(writer_props), // Optional WriterProperties
)?;

writer.write(&batch).await?;
writer.flush().await?;
let metadata = writer.close().await?;
```

### Pipeline builder (advanced)

For fine-grained control over progress tracking, parallelism, and channel sizing:

```rust
use parquet_ext::writer::Pipeline;

let pipeline = Pipeline::<Vec<u8>, _>::builder()
    .build_properties(&schema, Some(props))?
    .build_writer(Vec::new())?
    .with_progress()          // enable atomic progress counters
    .into_factory()
    .spawn_encoder()          // start encoder thread
    .spawn_writer(async_out)  // start writer thread
    .build();

pipeline.add_batch_async(batch).await?;
let metadata = pipeline.close_async().await?;
```

## Features

| Feature | Default | Description |
| ------- | ------- | ----------- |
| `default-parquet` | yes | Enables parquet's default feature set |
| `tokio` | yes | Async runtime support via tokio |
| `async` | via `tokio` | Core async support (`parquet/async`) |
| `object_store` | via `tokio` | `object_store` integration |
| `zstd` | no | ZSTD compression codec |
| `snap` | no | Snappy compression codec |
| `brotli` | no | Brotli compression codec |
| `lz4` / `lz4_flex` | no | LZ4 compression codecs |
| `flate2` | no | Deflate/gzip compression |

## Benchmarks

Two benchmark suites compare parquet-ext against Arrow's built-in writer. Both write to `tokio::io::sink()` to isolate encode/pipeline overhead from I/O.

```bash
cargo bench -p parquet-ext
```

### [arrow_writer](benches/arrow_writer.rs) - Encoding microbenchmark

Copied from the [parquet crate's benchmarks](https://github.com/apache/arrow-rs/blob/main/parquet/benches/arrow_writer.rs) Isolates the **row group encoding** step only (no async pipeline, no file framing). Compares `RowGroupEncoder::encode()` (parallel, work-stealing across threads) against Arrow's sequential `ArrowRowGroupWriterFactory` column-by-column encoding.

- **Batch size**: Fixed at 4,096 rows
- **Schemas**: 11 data type variants -- primitive, primitive non-null, bool, bool non-null, string, string non-null, string+binary view, string dictionary, float with NaNs, list primitive, list primitive non-null
- **Writer properties**: default, bloom filter, Parquet 2.0, ZSTD, ZSTD + Parquet 2.0

Wins 31/55 configurations (56%). Best speedup: **1.84x** (primitive/default). See [arrow_writer.md](benches/arrow_writer.md) for full results.

### [end_to_end_writer](benches/end_to_end_writer.rs) - Full pipeline benchmark

Measures the **complete async write path** end-to-end: batch submission, encoding, row group assembly, and async file writing. Compares `parquet_ext::AsyncArrowWriter` against `parquet::arrow::AsyncArrowWriter`.

- **Row counts**: 1K, 100K, 1M
- **Column counts**: 10, 30
- **Schemas**: Simple (Int64, Utf8) and Complex (Int64, Utf8, List\<Int32>, List\<Utf8>, Struct{Int64,Utf8,Float64}, Boolean)
- **Compression**: uncompressed, ZSTD level 1, ZSTD level 3

Wins 31/36 configurations (86%). Best speedup: **8.1x** (simple, 30 columns, 1M rows, ZSTD1). See [end_to_end_writer.md](benches/end_to_end_writer.md) for full results.

### End-to-end summary (speedup = baseline / parquet-ext)

**Complex schema**:

| Rows | 10 columns | 30 columns |
|-----:|-----------:|-----------:|
| 1K   | 1.5x       | 1.6x       |
| 100K | 3.2x       | 6.5x       |
| 1M   | 3.6x       | 7.1x       |

**Simple schema**:

| Rows | 10 columns | 30 columns |
|-----:|-----------:|-----------:|
| 1K   | 0.48x      | 0.83x      |
| 100K | 4.1x       | 7.2x       |
| 1M   | 5.1x       | 7.9x       |

Peak throughput: **6.1 GiB/s** (simple, 30 columns, 1M rows, uncompressed) vs parquet crate's baseline's 795 MiB/s.

### When parquet-ext wins

- Many columns (more parallel work to distribute)
- Large batches (100K+ rows amortize thread overhead)
- CPU-intensive compression (ZSTD, brotli)
- Complex/nested types (lists, structs)

### When parquet is faster*

- Very small batches (< 1K rows) where thread coordination overhead dominates
- Single-column or very narrow schemas

\* Although technically faster, times for both writers are in the hundreds of microseconds.

## Configuration

| Environment Variable             | Description                      | Default                      |
|----------------------------------|----------------------------------|------------------------------|
| `AMP_PARQUET_WRITER_PARALLELISM` | Number of encoder worker threads | System available parallelism |
