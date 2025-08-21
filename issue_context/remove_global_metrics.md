# Plan to Remove Global METRICS

## Problem
Currently, there are global `METRICS` static variables using `LazyLock` in:
- `dump/src/metrics.rs` - static METRICS for dump operations
- `dump-check/src/metrics.rs` - static METRICS for dump-check operations

This makes testing difficult as tests can't have isolated metrics instances and all tests share the same global state.

## Current Usage

### dump/src/metrics.rs
- Static `METRICS: LazyLock<MetricsRegistry>` containing counters for:
  - raw_dataset_rows
  - sql_dataset_rows  
  - raw_dataset_files_written
  - sql_dataset_files_written
  - raw_dataset_bytes_written
  - sql_dataset_bytes_written
- Public functions `raw::*` and `sql::*` that access the global METRICS

### dump-check/src/metrics.rs
- Static `METRICS: LazyLock<MetricsRegistry>` containing:
  - bytes_read: ReadableCounter
  - blocks_read: ReadableCounter
- Used by:
  - `dump-check/src/job.rs` - increments counters during processing
  - `dump-check/src/ui.rs` - reads counters for progress display

## Solution - Remove Global METRICS

### 1. Convert dump/src/metrics.rs
- Remove `static METRICS: LazyLock<MetricsRegistry>`
- Make `MetricsRegistry` public
- Keep `MetricsRegistry::new()` public
- Change module functions to methods on MetricsRegistry:
  - `raw::inc_rows(rows, dataset)` → `registry.inc_raw_rows(rows, dataset)`
  - `raw::inc_files(dataset)` → `registry.inc_raw_files(dataset)`
  - `raw::inc_bytes(bytes, dataset)` → `registry.inc_raw_bytes(bytes, dataset)`
  - `sql::inc_rows(rows, dataset)` → `registry.inc_sql_rows(rows, dataset)`
  - `sql::inc_files(dataset)` → `registry.inc_sql_files(dataset)`
  - `sql::inc_bytes(bytes, dataset)` → `registry.inc_sql_bytes(bytes, dataset)`

### 2. Convert dump-check/src/metrics.rs
- Remove `pub static METRICS: LazyLock<MetricsRegistry>`
- Keep `MetricsRegistry` struct public
- Add a `MetricsRegistry::new()` constructor

### 3. Update dump-check usage
- `dump-check/src/main.rs`:
  - Create `MetricsRegistry` instance in main
  - Pass it to job and ui functions
- `dump-check/src/job.rs`:
  - Accept `metrics: &MetricsRegistry` parameter in relevant functions
  - Use `metrics.blocks_read.inc()` instead of `METRICS.blocks_read.inc()`
- `dump-check/src/ui.rs`:
  - Accept `metrics: &MetricsRegistry` parameter in `start_ui` function
  - Use passed metrics instead of global METRICS

### 4. Update dump usage
- Find where dump metrics are used (likely in dump command implementation)
- Create `MetricsRegistry` instance where needed
- Pass it through to functions that need metrics
- Update calls from `raw::inc_*` to `metrics.inc_raw_*`

## Implementation Steps

1. **Update dump/src/metrics.rs**
   - Remove static METRICS
   - Convert module functions to MetricsRegistry methods
   - Make MetricsRegistry public

2. **Update dump-check/src/metrics.rs**
   - Remove static METRICS  
   - Add MetricsRegistry::new() constructor

3. **Update dump-check/src/job.rs**
   - Add metrics parameter to functions
   - Use passed metrics instance

4. **Update dump-check/src/ui.rs**
   - Add metrics parameter to start_ui
   - Use passed metrics instance

5. **Update dump-check/src/main.rs**
   - Create MetricsRegistry in main
   - Pass to job and ui

6. **Find and update dump command usage**
   - Search for where dump metrics are used
   - Create and pass MetricsRegistry instance

## Benefits

- Tests can create isolated MetricsRegistry instances
- No global state interference between tests
- More explicit dependencies (metrics passed as parameters)
- Easier to mock/stub metrics in tests
- Can have multiple metric registries if needed (e.g., for different subsystems)

## Example Changes

### Before (dump/src/metrics.rs):
```rust
static METRICS: LazyLock<MetricsRegistry> = LazyLock::new(|| MetricsRegistry::new());

pub(crate) mod raw {
    pub(crate) fn inc_rows(rows: u64, dataset: String) {
        METRICS.raw_dataset_rows.inc_by_with_kvs(rows, &kv_pairs);
    }
}
```

### After:
```rust
pub struct MetricsRegistry { ... }

impl MetricsRegistry {
    pub fn new() -> Self { ... }
    
    pub fn inc_raw_rows(&self, rows: u64, dataset: String) {
        self.raw_dataset_rows.inc_by_with_kvs(rows, &kv_pairs);
    }
}
```

### Usage Before:
```rust
metrics::raw::inc_rows(100, dataset_name);
```

### Usage After:
```rust
let metrics = MetricsRegistry::new();
metrics.inc_raw_rows(100, dataset_name);
```