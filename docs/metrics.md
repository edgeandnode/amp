# Amp Telemetry Metrics

This document enumerates all metrics collected by Amp for observability and performance monitoring. Metrics are exported via OpenTelemetry and designed for Prometheus-compatible backends.

## Metric Types

Amp uses four OpenTelemetry instrument types:

- **Counter**: Monotonically increasing value (e.g., total requests). Always named with `_total` suffix per Prometheus conventions.
- **Gauge**: Value that can increase or decrease, sampled from external source (e.g., queue depth from DB).
- **UpDownCounter**: Value that can increase or decrease, event-driven (e.g., active connections). Resets to 0 on process restart.
- **Histogram**: Distribution of values with configurable buckets (e.g., request duration). Enables percentile calculations.

## Query Metrics

**Location:** `crates/services/server/src/metrics.rs`

**Note:** HTTP-level metrics (request duration, bytes sent/received, active connections) are provided by `opentelemetry-instrumentation-tower` following OpenTelemetry HTTP semantic conventions. These are not listed here as they're automatically instrumented.

### query_count_total

**Type:** Counter
**Labels:** None

Tracks total queries executed since service start. Provides the foundation for understanding query workload patterns and system utilization. Use to track overall query load, detect anomalous traffic patterns, and understand system usage trends. Essential for capacity planning and SLA monitoring.

### query_duration_milliseconds

**Type:** Histogram
**Unit:** milliseconds
**Labels:** None

Measures end-to-end query execution time from submission to result completion. Primary SLA metric for query performance. Histogram enables percentile calculations (p50, p95, p99) which are more meaningful than averages for understanding user experience. Use to track latency against SLOs, detect performance regressions after deployments, and identify optimization targets. Critical for ensuring interactive queries remain fast.

### query_rows_returned_total

**Type:** Counter
**Labels:** None

Cumulative count of rows returned to clients across all queries. Helps understand data access patterns and query selectivity. High counts indicate full table scans or large result sets, low counts suggest well-filtered queries. Use to analyze query patterns, estimate egress volume, calculate query efficiency (rows/second), and evaluate pagination effectiveness. Essential for network bandwidth planning and understanding query behavior.

### query_bytes_egress_total

**Type:** Counter
**Unit:** bytes
**Labels:** None

Total bytes sent to clients across all queries. Direct measure of network egress and primary driver of cloud provider costs. Use for cloud cost attribution, network capacity planning, compression strategy evaluation, and bandwidth monitoring. Critical for cost management in cloud environments where egress is expensive.

### query_errors_total

**Type:** Counter
**Labels:** `error_code`

Count of failed queries by error code. Essential for reliability monitoring and understanding failure modes. Use to calculate error rates against SLOs, prioritize bug fixes based on frequency, detect systemic issues vs random failures, and understand error patterns. Key reliability metric for on-call dashboards.

## Streaming Query Metrics

**Location:** `crates/services/server/src/metrics.rs`

### streaming_queries_started_total

**Type:** Counter
**Labels:** None

Total number of streaming queries started. Streaming queries continuously deliver data as new blocks are produced. Use to track streaming query adoption, understand demand for real-time data, and validate streaming infrastructure is being utilized.

### streaming_queries_completed_total

**Type:** Counter
**Labels:** None

Total number of streaming queries that completed (stream fully consumed or client disconnected). Use with `started_total` to calculate completion rate and identify abandoned streams.

### streaming_queries_active

**Type:** UpDownCounter
**Labels:** None

Current number of active streaming queries in this process. Increments when stream starts, decrements when completed or errored. Resets to 0 on process restart (correct behavior - streams don't survive restarts). This is the **correct** way to track active streams, not `started_total - completed_total` which breaks on process restarts. Use to monitor concurrent streaming load, detect resource leaks, and plan streaming capacity.

### streaming_query_lifetime_milliseconds

**Type:** Histogram
**Unit:** milliseconds
**Labels:** None

Duration that streaming queries remain active from start to completion. Use to understand typical stream lifetimes, identify long-running streams, detect abandoned connections, and plan connection timeout policies.

### streaming_microbatch_rows

**Type:** Histogram
**Unit:** rows
**Labels:** None

Distribution of rows per microbatch in streaming queries. Use to understand streaming throughput patterns, tune microbatch configuration, and identify queries with inefficient batching.

### streaming_microbatch_duration_milliseconds

**Type:** Histogram
**Unit:** milliseconds
**Labels:** None

Duration of each streaming microbatch from start to completion. Measures the time taken to process and deliver each batch of results to clients. Use to understand microbatch processing performance, identify slow batches that may indicate complex queries or large result sets, correlate with batch size metrics to optimize microbatch configuration, and detect performance regressions in streaming query execution.

### streaming_rows_sent_total

**Type:** Counter
**Labels:** None

Total rows sent incrementally via streaming queries. Updated in real-time as each microbatch is sent. Use to calculate real-time streaming throughput (rows/sec), compare streaming vs non-streaming query volumes, and validate streaming data delivery.

### streaming_bytes_sent_total

**Type:** Counter
**Unit:** bytes
**Labels:** None

Total bytes sent incrementally via streaming queries. Updated in real-time as each microbatch is sent. Use to calculate streaming bandwidth utilization, estimate streaming egress costs, and compare streaming vs non-streaming network usage.

## Dump & Extraction Metrics

**Location:** `crates/core/dump/src/metrics.rs`

### rows_ingested_total

**Type:** Counter
**Labels:** `dataset`, `version`, `table`, `location_id`

Total rows ingested from all sources and written to storage. Measures data ingestion throughput. Use to track extraction progress, calculate ingestion rates (rows/sec), identify slow extractors, validate data completeness, and monitor dataset generation. Essential for monitoring ETL pipeline health.

### files_written_total

**Type:** Counter
**Labels:** `dataset`, `version`, `table`, `location_id`

Total number of Parquet files written to storage (both raw and SQL datasets). Use to track file creation rate, estimate storage object counts, plan for filesystem/object storage limits, detect file creation issues, and validate dataset completeness.

### bytes_written_total

**Type:** Counter
**Unit:** bytes
**Labels:** `dataset`, `version`, `table`, `location_id`

Total bytes written to storage for all datasets (both raw and SQL). Primary metric for storage capacity planning. Use to calculate storage costs, predict disk/object storage growth, compare compression effectiveness across sources, identify datasets with high storage costs, and measure storage impact of SQL transformations.

### latest_block_number

**Type:** Gauge
**Unit:** block_number
**Labels:** `dataset`, `version`, `table`, `location_id`

Latest block successfully dumped for a dataset. Critical for monitoring extraction progress and data freshness. Use to calculate lag behind chain tip, detect stalled extractors, validate that dumps are progressing, and alert on extraction failures. Essential for ensuring data remains current.

### dump_duration_milliseconds

**Type:** Histogram
**Unit:** milliseconds
**Labels:** `dataset`, `version`, `table`, `job_id`

Duration of complete dump operations. Measures time from job start to completion. Use to estimate future job durations, detect performance degradations, identify slow datasets needing optimization, plan job scheduling, and set realistic SLOs.

### dump_errors_total

**Type:** Counter
**Labels:** `dataset`, `version`, `table`

Count of dump failures. Use to identify unreliable data sources, detect configuration issues, prioritize reliability improvements, and distinguish transient vs persistent errors.

### compaction_files_read_total

**Type:** Counter
**Labels:** `dataset`, `table`, `location_id`

Number of files read during compaction operations. Use to understand compaction workload, estimate I/O costs, and tune compaction triggers.

### compaction_bytes_read_total

**Type:** Counter
**Unit:** bytes
**Labels:** `dataset`, `table`, `location_id`

Total bytes read during compaction. Use to calculate compaction I/O costs and understand data movement volumes.

### compaction_bytes_written_total

**Type:** Counter
**Unit:** bytes
**Labels:** `dataset`, `table`, `location_id`

Bytes written after compaction. Use with read bytes to calculate compression ratio and validate compaction effectiveness.

### compaction_duration_milliseconds

**Type:** Histogram
**Unit:** milliseconds
**Labels:** `dataset`, `table`, `location_id`

Time spent in compaction operations. Use to estimate compaction windows, identify slow compactions, and tune compaction scheduling to avoid interference with queries.

**Note:** Compaction ratio can be calculated in Prometheus as `compaction_bytes_written_total / compaction_bytes_read_total`. The dedicated `compaction_ratio` histogram metric was removed to reduce cardinality.

### successful_compactions

**Type:** Counter
**Labels:** `dataset`, `table`, `range_start`

Count of successful compaction operations. Tracks each successful compaction job by dataset, table, and the starting block number of the compacted range. Use to monitor compaction success rate, verify compaction jobs are completing successfully, identify which block ranges have been compacted, and calculate compaction success/failure ratios. Essential for understanding compaction reliability and troubleshooting compaction issues.

### failed_compactions

**Type:** Counter
**Labels:** `dataset`, `table`

Count of failed compaction operations. Incremented when compaction jobs encounter errors or fail to complete. Use with `successful_compactions` to calculate compaction reliability metrics, identify datasets with compaction issues, prioritize compaction bug fixes, and alert on compaction failures that could lead to storage inefficiency or query performance degradation.

### files_deleted

**Type:** Counter
**Labels:** `dataset`, `table`

Number of files successfully deleted by the garbage collector. Tracks physical file deletions from storage (local filesystem or object storage). Use to monitor garbage collection effectiveness, verify that obsolete files are being cleaned up, estimate storage reclamation, and validate that compaction cleanup is working correctly.

### files_not_found

**Type:** Counter
**Labels:** `dataset`, `table`

Number of files that were not found during garbage collection attempts. Incremented when the garbage collector tries to delete a file that doesn't exist in storage. High counts may indicate metadata inconsistencies or external file deletion. Use to detect synchronization issues between metadata and storage, identify potential data loss scenarios, and troubleshoot garbage collection problems.

### expired_files_found

**Type:** Counter
**Labels:** `dataset`, `table`

Number of expired files discovered during garbage collection scans. These are files marked for deletion but not yet physically removed from storage. Use to understand the backlog of files waiting for cleanup, estimate pending storage reclamation, tune garbage collection frequency, and monitor the lag between marking files as expired and actual deletion.

### expired_entries_deleted

**Type:** Counter
**Labels:** `dataset`, `table`

Number of expired metadata entries successfully removed from the metadata database. Tracks cleanup of database records for files that have been marked as expired. Use to monitor metadata cleanup effectiveness, ensure metadata doesn't grow unbounded, verify garbage collection is progressing through all phases (metadata and storage), and detect metadata cleanup bottlenecks.

### successful_collections

**Type:** Counter
**Labels:** `dataset`, `table`

Count of successful garbage collection cycles. Incremented when a complete garbage collection pass finishes without errors. Use to monitor garbage collection reliability, calculate collection frequency, verify that cleanup jobs are running as scheduled, and establish baseline metrics for garbage collection health.

### failed_collections

**Type:** Counter
**Labels:** `dataset`, `table`

Count of failed garbage collection cycles. Incremented when garbage collection encounters errors that prevent completion. Use with `successful_collections` to calculate garbage collection reliability, identify datasets with cleanup issues, prioritize garbage collection bug fixes, and alert on failures that could lead to storage bloat or metadata corruption.

## EVM RPC Metrics

**Location:** `crates/extractors/evm-rpc/src/metrics.rs`

EVM-RPC is the primary extraction path (Firehose is being deprecated).

### evm_rpc_requests_total

**Type:** Counter
**Labels:** `provider`, `network`, `method` (single requests) or `provider`, `network` (batch requests)

Total RPC requests to EVM-compatible endpoints. Use to track RPC usage by provider and method, calculate costs (many providers charge per request), identify rate limiting, compare provider reliability, and optimize batching strategies.

### evm_rpc_request_duration

**Type:** Histogram
**Unit:** milliseconds
**Labels:** `provider`, `network`, `method` (single requests) or `provider`, `network` (batch requests)

RPC request latency. Use to compare provider performance, identify slow RPC methods, detect network issues, and choose optimal providers for specific networks.

### evm_rpc_errors_total

**Type:** Counter
**Labels:** `provider`, `network`

Total RPC errors encountered. Use to identify unreliable providers, detect network issues, and prioritize error handling improvements.

### evm_rpc_batch_size_requests

**Type:** Histogram
**Unit:** requests
**Labels:** `provider`, `network`

Distribution of batch sizes (number of requests per batch). Use to understand actual batching behavior, optimize batch size configuration, identify whether batching is being utilized effectively, and correlate batch sizes with costs and performance.

### evm_rpc_request_bytes

**Type:** Histogram
**Unit:** bytes
**Labels:** `provider`, `network`, `method`

Size of RPC request payloads. Use to understand bandwidth requirements, identify methods with large request payloads, correlate payload sizes with provider costs, and optimize request serialization.

### evm_rpc_response_bytes

**Type:** Histogram
**Unit:** bytes
**Labels:** `provider`, `network`, `method`

Size of RPC response payloads. Use to understand egress bandwidth utilization, identify methods returning large responses, correlate with provider costs (some charge per GB), and optimize response handling.

## Firehose Metrics

**Location:** `crates/extractors/firehose/src/metrics.rs`

### firehose_blocks_received_total

**Type:** Counter
**Labels:** `provider`, `network`

Blocks received via Firehose streaming protocol. Use to track ingestion throughput, validate stream health, compare against expected block production rate, and detect stream interruptions.

### firehose_stream_duration_milliseconds

**Type:** Histogram
**Unit:** milliseconds
**Labels:** `provider`, `network`

Duration of streaming sessions. Use to understand stream stability, detect frequent disconnects, validate reconnection logic, and measure time-to-recovery after failures.

### firehose_stream_errors_total

**Type:** Counter
**Labels:** `provider`, `network`, `error_type`

Stream error count by category. Use to identify unreliable Firehose endpoints, distinguish network vs server errors, prioritize reliability improvements, and validate retry logic.

## Admin API Metrics

**Location:** HTTP metrics provided by `opentelemetry-instrumentation-tower`

Admin API HTTP metrics (request count, duration, bytes sent/received, status codes) are automatically instrumented by the `opentelemetry-instrumentation-tower` crate, following OpenTelemetry HTTP semantic conventions. These metrics are labeled with `http.route`, `http.method`, and `http.status_code` following standard conventions.

## Prometheus Query Examples

### Query Metrics

```promql
# Query rate
rate(query_count_total[5m])

# P95 query latency (convert milliseconds to seconds for display)
histogram_quantile(0.95, rate(query_duration_milliseconds_bucket[5m])) / 1000

# Query error rate
rate(query_errors_total[5m]) / rate(query_count_total[5m])

# Query throughput (rows/sec)
rate(query_rows_returned_total[5m])

# Egress bandwidth (bytes/sec)
rate(query_bytes_egress_total[5m])
```

### Streaming Query Metrics

```promql
# Current active streaming queries
streaming_queries_active

# Streaming throughput (rows/sec)
rate(streaming_rows_sent_total[5m])

# Streaming bandwidth (bytes/sec)
rate(streaming_bytes_sent_total[5m])

# P95 streaming query lifetime (convert milliseconds to seconds for display)
histogram_quantile(0.95, rate(streaming_query_lifetime_milliseconds_bucket[5m])) / 1000

# Average microbatch size
rate(streaming_rows_sent_total[5m]) / rate(streaming_microbatch_rows_count[5m])

# P95 microbatch duration (convert milliseconds to seconds for display)
histogram_quantile(0.95, rate(streaming_microbatch_duration_milliseconds_bucket[5m])) / 1000

# Streaming completion rate
rate(streaming_queries_completed_total[5m])
```

### Dump & Extraction Metrics

```promql
# Ingestion rate by dataset (rows/sec)
rate(rows_ingested_total[5m]) by (dataset, table)

# Storage growth rate (bytes/sec)
rate(bytes_written_total[5m]) by (dataset, table)

# Files written per second
rate(files_written_total[5m]) by (dataset, table)

# Data lag (blocks behind chain tip) - requires external chain tip metric
max(ethereum_latest_block) - max(latest_block_number) by (dataset)

# Compaction efficiency ratio
rate(compaction_bytes_written_total[5m]) / rate(compaction_bytes_read_total[5m])

# Compaction success rate
rate(successful_compactions[5m]) / (rate(successful_compactions[5m]) + rate(failed_compactions[5m]))

# Total compaction operations per second
rate(successful_compactions[5m]) + rate(failed_compactions[5m])

# Garbage collection file deletion rate (files/sec)
rate(files_deleted[5m]) by (dataset, table)

# Files not found rate (potential metadata issues)
rate(files_not_found[5m]) by (dataset, table)

# Expired files backlog
sum(expired_files_found) by (dataset, table)

# Garbage collection success rate
rate(successful_collections[5m]) / (rate(successful_collections[5m]) + rate(failed_collections[5m]))

# Storage reclamation rate (bytes/sec) - requires tracking file sizes
rate(files_deleted[5m]) * avg(bytes_written_total / files_written_total)
```

### EVM-RPC Metrics

```promql
# RPC request rate by provider
rate(evm_rpc_requests_total[5m]) by (provider, network)

# P95 RPC latency by provider (convert milliseconds to seconds for display)
histogram_quantile(0.95, rate(evm_rpc_request_duration_bucket[5m])) by (provider) / 1000

# RPC error rate
rate(evm_rpc_errors_total[5m]) / rate(evm_rpc_requests_total[5m])

# Average batch size
rate(evm_rpc_batch_size_requests_sum[5m]) / rate(evm_rpc_batch_size_requests_count[5m])

# P95 response payload size
histogram_quantile(0.95, rate(evm_rpc_response_bytes_bucket[5m])) by (method)

# Total bandwidth utilization
rate(evm_rpc_response_bytes_sum[5m])
```

