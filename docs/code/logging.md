---
name: "logging"
description: "Structured logging with tracing, log levels, field formatting. Load when adding logs or configuring logging"
type: core
scope: "global"
---

# Logging Patterns

**MANDATORY for ALL logging in the Amp project**

## PURPOSE

This document establishes consistent, production-grade logging patterns across the entire Amp codebase. These patterns ensure:

- **Observability** - Clear visibility into system behavior
- **Structured data** - Machine-parseable logs for aggregation and analysis
- **Operational clarity** - Consistent log format across all services and crates

## TABLE OF CONTENTS

1. [Logger Configuration](#logger-configuration)
2. [Core Principles](#core-principles)
   - [1. Use `tracing` Crate Exclusively](#1-use-tracing-crate-exclusively)
   - [2. Structured Logging is Mandatory](#2-structured-logging-is-mandatory)
   - [3. Line Length and Multiline Formatting](#3-line-length-and-multiline-formatting)
   - [4. Consistent Log Levels](#4-consistent-log-levels)
   - [5. Field Naming Conventions](#5-field-naming-conventions)
3. [Field Formatting](#field-formatting)
   - [1. Display Formatting (`%`)](#1-display-formatting-)
   - [2. Debug Formatting (`?`)](#2-debug-formatting-)
   - [3. Avoid Redundant Context](#3-avoid-redundant-context)
4. [Log Level Guidelines](#log-level-guidelines)
   - [1. Error Level](#1-error-level)
   - [2. Warn Level](#2-warn-level)
   - [3. Info Level](#3-info-level)
   - [4. Debug Level](#4-debug-level)
   - [5. Trace Level](#5-trace-level)
5. [Message Formatting](#message-formatting)
   - [1. Descriptive Messages, Not snake_case](#1-descriptive-messages-not-snake_case)
   - [2. Brief and Clear](#2-brief-and-clear)
   - [3. Action-Oriented Past Tense](#3-action-oriented-past-tense)
   - [4. No Punctuation](#4-no-punctuation)
6. [Complete Examples](#complete-examples)
7. [Checklist](#checklist)
8. [References](#references)

## LOGGER CONFIGURATION

### Default Log Level

The Amp logging system uses a **two-tier configuration** for log levels:

**Default Levels:**

- **Amp workspace crates**: `info` level (configurable via `AMP_LOG` environment variable)
- **External dependencies**: `error` level (configurable via `RUST_LOG` environment variable)

**Environment Variables:**

```bash
# AMP_LOG: Controls log level for all Amp workspace crates
# Default: info
# Values: error, warn, info, debug, trace
export AMP_LOG=info

# RUST_LOG: Controls log level for specific crates (overrides AMP_LOG)
# Use for fine-grained control or external dependencies
export RUST_LOG="metadata_db=debug,sqlx=warn"
```

**How It Works:**

1. **`AMP_LOG`** sets the baseline level for all Amp crates (metadata_db, worker, server, etc.)
2. **`RUST_LOG`** can override specific crates or enable logging for external dependencies
3. External crates default to `error` level to reduce noise
4. Directives in `RUST_LOG` take precedence over `AMP_LOG`

**Best Practices:**

- Use `AMP_LOG=info` for production (default)
- Use `AMP_LOG=debug` for local development
- Use `RUST_LOG` for targeted debugging of specific modules
- Never use `trace` level in production (performance impact)

## CORE PRINCIPLES

### 1. Use `tracing` Crate Exclusively

**ALWAYS** use the fully qualified form `tracing::<macro>!()` for all logging operations and `#[tracing::instrument]` for the instrument attribute. **NEVER** use `println!`, `eprintln!`, `log` crate, or import tracing macros.

```rust
// ✅ CORRECT - Fully qualified tracing macros
tracing::info!(job_id = %id, "job started");
tracing::error!(error = %err, error_source = logging::error_source(&err), "job failed");

// ✅ CORRECT - Fully qualified instrument attribute
#[tracing::instrument(skip_all, fields(job_id = %job_id))]
pub async fn process_job(job_id: JobId) -> Result<(), Error> {
    // ...
}

// ❌ WRONG - Importing macros creates ambiguity
use tracing::info;
info!(job_id = %id, "job started");

// ❌ WRONG - Importing instrument attribute
use tracing::instrument;
#[instrument(skip_all)]
pub async fn process_job() -> Result<(), Error> {
    // ...
}

// ❌ WRONG - Using println/eprintln
println!("Job {} started", id);
eprintln!("Error: {}", err);

// ❌ WRONG - Using log crate
log::info!("Job started");
```

### 2. Structured Logging is Mandatory

**ALWAYS** use field-based structured logging. **AVOID** using string interpolation or formatting in log messages.

```rust
// ✅ CORRECT - Structured fields
tracing::info!(
    job_id = %job_id,
    dataset_name = %dataset_name,
    duration_ms = elapsed.as_millis(),
    "job completed"
);

// ❌ WRONG - String interpolation
tracing::info!("Job {} for dataset {} completed in {}ms", job_id, dataset_name, elapsed.as_millis());

// ❌ WRONG - format! macro in messages
tracing::info!(format!("Job {} completed", job_id));

// ❌ WRONG - Mixing string interpolation with fields
tracing::info!(job_id = %job_id, "Job {} completed", job_id);
```

### 3. Line Length and Multiline Formatting

**ALWAYS** split tracing macro calls into multiline format if they exceed 100 characters. **NEVER** write long single-line logging statements.

```rust
// ✅ CORRECT - Multiline format for calls exceeding 100 chars
tracing::info!(
    job_id = %job_id,
    dataset_name = %dataset_name,
    duration_ms = elapsed.as_millis(),
    "job completed"
);

tracing::error!(
    job_id = %job_id,
    error = %err,
    error_source = logging::error_source(&err),
    "failed to mark job as running"
);

// ✅ CORRECT - Single line acceptable for short calls (< 100 chars)
tracing::info!(worker_id = %id, "worker registered");
tracing::debug!(query = %sql, "executing database query");

// ❌ WRONG - Long single-line format (exceeds 100 chars)
tracing::error!(job_id = %job_id, error = %err, error_source = logging::error_source(&err), "failed to mark job as running");

// ❌ WRONG - Long single-line format with many fields
tracing::info!(job_id = %job_id, dataset_name = %dataset_name, duration_ms = elapsed.as_millis(), rows = count, "job completed");
```

**Formatting Rules:**

- Opening parenthesis on same line as macro name: `tracing::info!(`
- Each field on its own line with consistent indentation (4 spaces)
- **Message string MUST be the last parameter** (after all fields)
- Closing parenthesis and semicolon on same line: `);`
- Use multiline format consistently for all calls with 3+ fields or exceeding 100 chars
- Single-line format acceptable ONLY for simple calls under 100 characters

### 4. Consistent Log Levels

**ALWAYS** use appropriate log levels based on operational significance. See [Log Level Guidelines](#-log-level-guidelines) for detailed rules.

```rust
// ✅ CORRECT - Appropriate log levels
tracing::error!(error = %err, error_source = logging::error_source(&err), "database connection failed");
tracing::warn!(retry_attempt = 3, "connection retry scheduled after backoff");
tracing::info!(worker_id = %id, "worker registered");
tracing::debug!(query = %sql, "executing database query");
tracing::trace!(batch_size = rows.len(), "processing batch");

// ❌ WRONG - Misused log levels
tracing::error!("worker registered"); // Not an error
tracing::info!(error = %err, "database connection failed"); // Should be error level
tracing::debug!(worker_id = %id, "worker registered"); // Important event, should be info
```

### 5. Field Naming Conventions

**ALWAYS** use `snake_case` for field names. **ALWAYS** use consistent field names across the entire codebase.

```rust
// ✅ CORRECT - snake_case field names
tracing::info!(
    job_id = %job_id,
    worker_node_id = %worker_id,
    dataset_name = %dataset_name,
    block_number = block_num,
    duration_ms = elapsed.as_millis(),
    "operation completed"
);

// ❌ WRONG - camelCase field names
tracing::info!(
    jobId = %job_id,
    workerNodeId = %worker_id,
    datasetName = %dataset_name,
    "operation completed"
);

// ❌ WRONG - Inconsistent naming
tracing::info!(job = %job_id, "job started");
tracing::info!(job_id = %job_id, "job completed"); // Use job_id consistently

// ❌ WRONG - Abbreviated names
tracing::info!(ds = %dataset_name, wrk = %worker_id, "processing");
```

**Standard Field Names:**

| Resource          | Field Name          | Example                                                  |
| ----------------- | ------------------- | -------------------------------------------------------- |
| Job ID            | `job_id`            | `job_id = %job_id`                                       |
| Worker Node ID    | `node_id`           | `node_id = %node_id`                                     |
| Dataset Namespace | `dataset_namespace` | `dataset_namespace = %namespace`                         |
| Dataset Name      | `dataset_name`      | `dataset_name = %dataset_name`                           |
| Dataset Revision  | `dataset_revision`  | `dataset_revision = %revision`                           |
| Dataset Reference | `dataset_reference` | `dataset_reference = %reference`                         |
| Block Number      | `block_number`      | `block_number = block_num`                               |
| Duration          | `duration_ms`       | `duration_ms = elapsed.as_millis()`                      |
| Retry Attempt     | `retry_attempt`     | `retry_attempt = 3`                                      |
| Error             | `error`             | `error = %err` (MANDATORY format)                        |
| Error Source      | `error_source`      | `error_source = logging::error_source(&err)` (MANDATORY) |

## FIELD FORMATTING

### 1. Display Formatting (`%`)

**USE** `%` prefix for human-readable string representation (implements `Display` trait).

```rust
// ✅ CORRECT - Display formatting for readable values
tracing::info!(
    job_id = %job_id,              // JobId implements Display
    dataset_name = %dataset_name,   // String-like types
    error = %err,                   // Top-level error message
    status = %job_status,           // Enum with Display
    "job state changed"
);

// ❌ WRONG - Using Debug when Display is available
tracing::info!(job_id = ?job_id, "job started");
```

### 2. Debug Formatting (`?`)

**USE** `?` prefix for Debug representation (implements `Debug` trait). Useful for complex types, collections, and error source chains.

```rust
// ✅ CORRECT - Debug formatting for complex types
tracing::debug!(
    config = ?config,                           // Complex struct
    error_source = logging::error_source(&err), // Returns DebugValue<Vec<String>>
    headers = ?request_headers,                 // HashMap
    "request processed"
);

// ✅ CORRECT - No prefix for primitive types
tracing::info!(
    retry_attempt = 3,           // i32/u32/usize - no prefix
    duration_ms = elapsed.as_millis(), // u128 - no prefix
    row_count = rows.len(),      // usize - no prefix
    "operation completed"
);
```

### 3. Avoid Redundant Context

**DO NOT** log the same field multiple times in nested spans or repeated log statements.

```rust
// ✅ CORRECT - Set context once in span
#[tracing::instrument(skip_all, fields(job_id = %job_id))]
pub async fn process_job(job_id: JobId) -> Result<(), Error> {
    tracing::info!("job started");  // job_id already in span

    match execute_job().await {
        Ok(_) => tracing::info!("job completed"),  // job_id already in span
        Err(err) => {
            tracing::error!(
                error = %err,
                error_source = logging::error_source(&err),
                "job execution failed"
            );
        }
    }

    Ok(())
}

// ❌ WRONG - Repeating job_id in every log
pub async fn process_job(job_id: JobId) -> Result<(), Error> {
    tracing::info!(job_id = %job_id, "job started");

    match execute_job().await {
        Ok(_) => tracing::info!(job_id = %job_id, "job completed"),
        Err(err) => {
            tracing::error!(
                job_id = %job_id,  // Redundant if in span
                error = %err,
                error_source = logging::error_source(&err),
                "job execution failed"
            );
        }
    }

    Ok(())
}
```

## LOG LEVEL GUIDELINES

### 1. Error Level

**USE** `tracing::error!` for unrecoverable failures, data loss risks, and critical system issues.

**When to use:**

- Database connection failures (after retries exhausted)
- Data corruption detected
- Critical resource unavailable
- Unexpected errors that require immediate attention
- System integrity compromised

```rust
// ✅ CORRECT - Error level usage
tracing::error!(
    error = %err,
    error_source = logging::error_source(&err),
    "database connection failed after retries"
);

tracing::error!(
    job_id = %job_id,
    error = %err,
    error_source = logging::error_source(&err),
    "data corruption detected"
);

tracing::error!(
    manifest_hash = %hash,
    "manifest validation failed"
);

// ❌ WRONG - Not error-level events
tracing::error!("worker_started");  // Should be info
tracing::error!(retry_attempt = 1, "retrying connection");  // Should be warn
```

### 2. Warn Level

**USE** `tracing::warn!` for recoverable failures, degraded performance, and retry attempts.

**When to use:**

- Transient failures that will be retried
- Performance degradation detected
- Resource limits approaching
- Expected errors during retries
- Deprecated functionality usage

```rust
// ✅ CORRECT - Warn level usage
tracing::warn!(
    node_id = %node_id,
    retry_attempt = 3,
    error = %err,
    error_source = logging::error_source(&err),
    "connection retry scheduled after backoff"
);

tracing::warn!(
    memory_usage_percent = 85,
    "memory usage approaching limit"
);

tracing::warn!(
    job_id = %job_id,
    duration_ms = elapsed.as_millis(),
    "job execution time exceeded threshold"
);

// ❌ WRONG - Not warning-level events
tracing::warn!("job completed");  // Should be info
tracing::warn!("starting_database_query");  // Should be debug
```

### 3. Info Level

**USE** `tracing::info!` for important state changes, successful operations, and lifecycle events.

**When to use:**

- Service startup/shutdown
- Worker registration/deregistration
- Job lifecycle events (started, completed)
- Dataset operations (registered, deployed)
- Important configuration changes

```rust
// ✅ CORRECT - Info level usage
tracing::info!(
    node_id = %node_id,
    worker_type = "dump",
    "worker registered"
);

tracing::info!(
    job_id = %job_id,
    dataset_name = %dataset_name,
    duration_ms = elapsed.as_millis(),
    "job completed"
);

tracing::info!(
    dataset_name = %dataset_name,
    version = %version,
    "dataset deployed"
);

// ❌ WRONG - Too verbose for info level
tracing::info!(batch_size = 100, "processing batch");  // Should be debug
tracing::info!("checking database connection");  // Should be debug
```

### 4. Debug Level

**USE** `tracing::debug!` for detailed execution flow, expected errors, and diagnostic information.

**When to use:**

- Detailed operational flow
- Database query execution
- Expected error conditions during normal operation
- Intermediate processing steps
- Resource allocation/deallocation

```rust
// ✅ CORRECT - Debug level usage
tracing::debug!(
    query = %sql,
    params = ?query_params,
    "executing database query"
);

tracing::debug!(
    job_id = %job_id,
    status = %current_status,
    "checking job status"
);

tracing::debug!(
    batch_size = rows.len(),
    block_range = ?(start_block, end_block),
    "processing batch"
);

// ❌ WRONG - Too important for debug level
tracing::debug!(node_id = %node_id, "worker registered");  // Should be info
tracing::debug!(error = %err, error_source = logging::error_source(&err), "critical_failure");  // Should be error
```

### 5. Trace Level

**USE** `tracing::trace!` for extremely verbose debugging. Disabled by default in production.

**When to use:**

- Function entry/exit (when not using `#[tracing::instrument]`)
- Every iteration in loops
- Low-level protocol details
- Memory allocation details
- Performance profiling data points

```rust
// ✅ CORRECT - Trace level usage
tracing::trace!("entering process batch function");

tracing::trace!(
    row_index = i,
    row_data = ?row,
    "processing individual row"
);

tracing::trace!(
    buffer_size = buffer.len(),
    capacity = buffer.capacity(),
    "buffer allocation"
);

// ❌ WRONG - Too important for trace level
tracing::trace!(job_id = %job_id, "job completed");  // Should be info
tracing::trace!(error = %err, error_source = logging::error_source(&err), "database_error");  // Should be error
```

## MESSAGE FORMATTING

### 1. Descriptive Messages, Not snake_case

**ALWAYS** use descriptive, human-readable messages. **AVOID** using snake_case, camelCase, or interpolation.
**Data belongs in fields**, not in the message string.

```rust
// ✅ CORRECT - Descriptive messages with data in fields
tracing::info!(worker_id = %id, "worker registered");
tracing::info!(job_id = %job_id, dataset = %name, "job started for dataset");
tracing::error!(
    job_id = %job_id,
    error = %err,
    error_source = logging::error_source(&err),
    "failed to mark job as running"
);

// ❌ WRONG - snake_case messages
tracing::info!("worker registered");  // Not descriptive
tracing::info!("job started");  // Not descriptive

// ❌ WRONG - Data interpolation in message
tracing::info!("worker {} registered", id);  // Data should be in fields
tracing::info!(job_id = %job_id, "job {} started", job_id);  // Redundant
```

### 2. Brief and Clear

**USE** concise, descriptive messages that explain what happened. **AVOID** verbose sentences.

```rust
// ✅ CORRECT - Brief and clear
tracing::info!(worker_id = %id, "worker registered");
tracing::info!(job_id = %job_id, rows = count, "job completed");
tracing::error!(
    error = %err,
    error_source = logging::error_source(&err),
    "database connection failed"
);

// ❌ WRONG - Too verbose
tracing::info!(worker_id = %id, "The worker has been successfully registered in the system");
tracing::info!(job_id = %job_id, "Job processing has now completed");
```

### 3. Action-Oriented Past Tense

**USE** past tense verbs describing what happened. **AVOID** present progressive or editorial comments.

```rust
// ✅ CORRECT - Past tense actions
tracing::info!(worker_id = %id, "worker registered");
tracing::info!(job_id = %job_id, "job completed");
tracing::info!(dataset = %name, "dataset deployed");
tracing::warn!(retry = attempt, "connection retry scheduled");

// ❌ WRONG - Present progressive
tracing::info!(worker_id = %id, "registering worker");
tracing::info!(job_id = %job_id, "completing job");

// ❌ WRONG - Editorial comments
tracing::info!(dataset = %name, "successfully deployed dataset");
tracing::error!(error = %err, error_source = logging::error_source(&err), "oh no connection problem");
```

### 4. No Punctuation

**NEVER** include punctuation (periods, exclamation marks, question marks) in log messages.

```rust
// ✅ CORRECT - No punctuation
tracing::info!(worker_id = %id, "worker registered");
tracing::error!(
    error = %err,
    error_source = logging::error_source(&err),
    "database connection failed"
);

// ❌ WRONG - Includes punctuation
tracing::info!(worker_id = %id, "worker registered.");
tracing::error!(error = %err, error_source = logging::error_source(&err), "connection failed!");
tracing::warn!(retry = 3, "retrying connection?");
```

## COMPLETE EXAMPLES

### Example 1: Info Level for Lifecycle Events

**Context**: Logging successful operations and state changes

```rust
// ✅ CORRECT - Info for important events
tracing::info!(
    job_id = %job_id,
    rows_processed = result.row_count,
    duration_ms = result.duration.as_millis(),
    "job completed successfully"
);
```

### Example 2: Debug Level for Operational Details

**Context**: Detailed execution flow logging

```rust
// ✅ CORRECT - Debug for detailed flow
tracing::debug!(
    table_name = %table_name,
    batch_count,
    row_count,
    total_rows,
    "processing batch"
);
```

### Example 3: Instrumentation with Span Context

**Context**: Using tracing::instrument to avoid field repetition

```rust
// ✅ CORRECT - Context set in span, not repeated in logs
#[tracing::instrument(skip(self), fields(node_id = %self.node_id, job_id = %job_id))]
pub async fn process_job(&self, job_id: JobId) -> Result<(), Error> {
    tracing::info!("job processing started");  // job_id already in span
    // ... execute job ...
    tracing::info!("job completed");  // job_id already in span
    Ok(())
}
```

## CHECKLIST

Before committing code with logging, verify:

### Core Principles

- [ ] All logging uses fully qualified `tracing::<macro>!()` form
- [ ] Instrument attribute uses fully qualified `#[tracing::instrument]` form
- [ ] No use of `println!`, `eprintln!`, or `log` crate
- [ ] All logs use structured field-based logging
- [ ] Avoid string interpolation in log messages
- [ ] Appropriate log level used (error/warn/info/debug/trace)
- [ ] Multiline format used for calls exceeding 100 characters or with 3+ fields
- [ ] Message string is the last parameter (after all fields)

### Field Formatting

- [ ] Display formatting (`%`) used for human-readable values
- [ ] Debug formatting (`?`) used for complex types and collections
- [ ] No prefix for primitive numeric types
- [ ] `logging::error_source()` returns `DebugValue<Vec<String>>`

### Field Naming

- [ ] All field names use `snake_case`
- [ ] Consistent field names used (e.g., `job_id`, not `job` or `id`)
- [ ] Standard field names followed (see table in Core Principles #5)
- [ ] No abbreviated field names

### Message Formatting

- [ ] Messages are descriptive and human-readable (not snake_case)
- [ ] Data is in fields, not interpolated in messages
- [ ] Messages are brief and action-oriented
- [ ] Past tense verbs used
- [ ] No punctuation in messages
- [ ] No editorial comments or vague descriptions

### Context and Spans

- [ ] Relevant context included in all error logs
- [ ] `#[tracing::instrument]` used for important functions
- [ ] Redundant context avoided in nested spans
- [ ] Resource identifiers included where relevant

## References

- [logging-errors](logging-errors.md) - Related: Error-specific logging patterns
