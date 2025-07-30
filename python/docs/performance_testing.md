# Performance Testing Guide

This guide covers how to use performance tests to ensure the data loaders remain fast, memory-efficient, and regression-free.

## Overview

Our performance testing framework automatically tracks key metrics and detects regressions:
- **Throughput**: Rows loaded per second
- **Memory**: Peak memory usage during loading
- **Duration**: Total time to complete operations

## Regression Thresholds

The system flags performance regressions when metrics exceed these thresholds:
- **Throughput**: 5% slower than baseline
- **Memory**: 10% more memory than baseline  
- **Duration**: 10% longer than baseline

## Running Performance Tests

### Basic Usage

```bash
# Run all performance tests
make test-performance

# Run specific loader performance tests
uv run pytest tests/performance/ -m "postgresql" -v
uv run pytest tests/performance/ -m "redis" -v
uv run pytest tests/performance/ -m "delta_lake" -v
```

### Environment Configuration

Performance tests use the same database configuration as integration tests (`.env.test`):

```bash
# Required for PostgreSQL performance tests
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=test_nozzle
POSTGRES_USER=ford
POSTGRES_PASSWORD=word

# Required for Redis performance tests
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=1
REDIS_PASSWORD=mypassword
```

### Test Data Size

Control the size of performance test datasets:

```bash
# Default: 50,000 rows
make test-performance

# Large dataset: 100,000 rows
PERF_TEST_SIZE=100000 make test-performance

# Small dataset for quick checks: 10,000 rows
PERF_TEST_SIZE=10000 make test-performance
```

### Important Notes

- **Database connections required**: Performance tests need database connections from `.env.test`
- **Automatic benchmark recording**: Every test run automatically records benchmarks and checks for regressions
- **Regression warnings**: If performance degrades beyond thresholds, you'll see warnings in the test output

### Understanding Regression Warnings

When a regression is detected, you'll see output like:

```
⚠️  PERFORMANCE REGRESSION DETECTED:
   Test: throughput_comparison
   Loader: postgresql
   Metric: throughput
   Regression: 13.2%
   Current: 73626.78
   Baseline: 84786.86
```

This means:
- The test's current performance is 13.2% worse than the baseline
- You should investigate what changed to cause this regression
- Consider reverting changes or optimizing the code
- If the regression is expected (e.g., added security checks), update the baseline

## Development Workflow

### 1. Before Making Changes

Establish a baseline before making performance-related changes:

```bash
# Record current performance
make test-performance
# Benchmarks are automatically saved to performance_benchmarks.json
git add performance_benchmarks.json
git commit -m "perf: establish baseline before optimization"
```

### 2. After Making Changes

Verify your changes don't introduce regressions:

```bash
# Test performance - will automatically detect regressions
make test-performance

# If regression detected, you'll see:
# ⚠️  PERFORMANCE REGRESSION DETECTED:
#    Test: large_table_loading_performance
#    Loader: postgresql
#    Metric: throughput
#    Regression: 7.2%
#    Current: 4650.0
#    Baseline: 5012.0
```

### 3. Optimizing Performance

Use performance tests to guide optimization efforts:

```bash
# Compare loader performance
make test-performance | grep "Throughput comparison"

# Focus on the slowest loaders
# Make targeted improvements
# Re-run tests to measure gains
```

## Benchmark Management

### Baseline Storage

Performance baselines are stored in `performance_benchmarks.json`:
- **Automatic tracking**: Every test run updates baselines
- **Git integration**: Commit hash linked to each benchmark
- **Historical data**: Track performance over time

### Manual Baseline Management

```bash
# View current baselines
cat performance_benchmarks.json

# Reset baselines (use carefully!)
rm performance_benchmarks.json
make test-performance  # Creates new baselines

# View performance summary
python -c "
import json
with open('performance_benchmarks.json') as f:
    data = json.load(f)
    for key, bench in data.items():
        print(f'{bench[\"loader_type\"]}: {bench[\"throughput_rows_per_sec\"]:,.0f} rows/sec')
"
```

### Understanding Benchmark Data

Each benchmark includes:
```json
{
  "postgresql_large_table_loading_performance": {
    "test_name": "large_table_loading_performance",
    "loader_type": "postgresql", 
    "throughput_rows_per_sec": 33825.85,
    "memory_mb": 115.1,
    "duration_seconds": 0.029,
    "dataset_size": 1000,
    "timestamp": "2025-07-29T07:18:04.091203",
    "git_commit": "c3e98d42",
    "environment": "local"
  }
}
```

## Performance Test Types

### Loader-Specific Tests

**PostgreSQL:**
- Large table loading (configurable via PERF_TEST_SIZE, default 50k rows)
- Batch size scaling (1k, 5k, 10k)
- Connection pool efficiency

**Redis:**
- Pipeline optimization comparison
- Data structure performance (hash, string, sorted_set)
- Memory efficiency validation

**Delta Lake:**
- Large file write performance
- Partitioned write optimization

### Cross-Loader Comparisons

- **Throughput benchmark**: Compare rows/sec across all loaders
- **Memory usage**: Identify most memory-efficient loaders
- **Scaling characteristics**: How performance changes with data size

## Troubleshooting

### Test Failures

**"PostgreSQL throughput too low"**
```bash
# Check database configuration
psql -h localhost -U ford -d test_nozzle -c "SELECT version();"

# Verify connection pooling
grep "max_connections" .env.test
```

**"Redis pipeline optimization not effective"**
```bash
# Check Redis connection
redis-cli -h localhost -p 6379 -a mypassword ping

# Verify pipeline settings in test
```

**"Delta Lake loader not available"**
```bash
# Install Delta Lake dependencies
uv sync --group delta_lake
```

### Memory Issues

If memory usage is consistently high:

```bash
# Monitor system memory during tests
htop &
make test-performance

# Check for memory leaks in specific loaders
uv run pytest tests/performance/ -k "memory" -s
```

## Performance Optimization Tips

### Database Tuning

**PostgreSQL:**
- Increase `batch_size` for bulk operations
- Tune `max_connections` based on workload
- Use `COPY` instead of individual `INSERT`s

**Redis:**
- Increase `pipeline_size` for bulk operations
- Choose appropriate data structure for use case
- Use batching for large datasets

**Delta Lake:**
- Enable `optimize_after_write` for read performance
- Use appropriate partitioning strategy
- Consider file size optimization

### Code Patterns

**Efficient data processing:**
```python
# Good: Use PyArrow for zero-copy operations
table = pa.Table.from_pandas(df)
result = loader.load_table(table, 'my_table')

# Avoid: Converting between formats unnecessarily
# df -> table -> df -> table (multiple conversions)
```

**Memory management:**
```python
# Good: Process in batches
for batch in table.to_batches(max_chunksize=10000):
    loader.load_batch(batch, 'my_table', mode=LoadMode.APPEND)

# Avoid: Loading entire dataset into memory
# large_df = pd.read_csv('huge_file.csv')  # May cause OOM
```

## Monitoring Production Performance

### Metrics to Track

- **Throughput trends**: Is performance degrading over time?
- **Memory usage**: Are we approaching system limits?
- **Error rates**: Do performance issues correlate with failures?

### Regular Maintenance

```bash
# Weekly performance review
make test-performance > reports/weekly_$(date +%Y%m%d).log

# Monthly baseline updates
git add performance_benchmarks.json
git commit -m "perf: monthly baseline update $(date +%Y-%m)"

# Quarterly optimization sprints
# 1. Identify bottlenecks from performance tests
# 2. Profile and optimize worst performers
# 3. Validate improvements with performance tests
```