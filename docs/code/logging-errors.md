---
name: "logging-errors"
description: "Mandatory error logging patterns: error+error_source fields, error chain preservation, retry logging with backon. Load when logging errors or handling Result types"
type: core
scope: "global"
---

# Error Logging Patterns

**MANDATORY for ALL error logging in the Amp project**

## PURPOSE

This document establishes mandatory error logging patterns for the Amp codebase, ensuring:

- **Error traceability** - Complete error chains for debugging distributed issues
- **Consistent error visibility** across all services
- **Machine-parseable** error logs for monitoring and alerting

## TABLE OF CONTENTS

1. [Error Logging Homogeneity](#1-error-logging-homogeneity-mandatory)
2. [Error Formatting Pattern](#2-error-formatting-pattern-mandatory)
3. [Error Logging Patterns](#error-logging-patterns-mandatory)
   - [1. Mandatory Error and Error Source Fields](#1-mandatory-error-and-error-source-fields)
   - [2. Never Log Errors Without Context](#2-never-log-errors-without-context)
   - [3. Closure Parameter Naming](#3-closure-parameter-naming)
   - [4. Error Chain Preservation](#4-error-chain-preservation)
   - [5. Retry Logging with Backon](#5-retry-logging-with-backon)
4. [Complete Examples](#complete-examples)
5. [Checklist](#checklist)

## CORE PRINCIPLES

### 1. Error Logging Homogeneity (MANDATORY)

**ABSOLUTELY MANDATORY**: All error logs that include `std::error::Error` objects **MUST** use this exact pattern:

```rust
error = %err, error_source = logging::error_source(&err)
```

**Field Ordering Requirement**: The `error` and `error_source` fields **MUST be the last fields before the message string**. Context fields (job_id, node_id, etc.) come first, then error fields, then the message.

This ensures:

- **Consistent error visibility** across all services
- **Complete error chain** for debugging
- **Machine-parseable** error logs
- **Uniform monitoring** and alerting

```rust
// ✅ CORRECT - Mandatory error logging pattern (error fields last before message)
tracing::error!(
    job_id = %job_id,
    error = %err,
    error_source = logging::error_source(&err),
    "job execution failed"
);

// ✅ CORRECT - With additional context (context first, then error fields, then message)
tracing::warn!(
    node_id = %node_id,
    retry_attempt = 3,
    error = %err,
    error_source = logging::error_source(&err),
    "connection retry failed"
);

// ❌ WRONG - Missing error_source
tracing::error!(error = %err, "job execution failed");

// ❌ WRONG - Using Debug format for top-level error
tracing::error!(error = ?err, error_source = logging::error_source(&err), "job execution failed");

// ❌ WRONG - Not using logging::error_source utility
tracing::error!(error = %err, source = %err.source().unwrap(), "job execution failed");

// ❌ WRONG - Different field names
tracing::error!(err = %err, error_chain = logging::error_source(&err), "job execution failed");

// ❌ WRONG - Error fields not last before message
tracing::error!(
    error = %err,
    error_source = logging::error_source(&err),
    job_id = %job_id,  // Context field should come before error fields
    "job execution failed"
);
```

### 2. Error Formatting Pattern (MANDATORY)

**ABSOLUTELY MANDATORY**: Use `%` for top-level error, and `logging::error_source()` for the source chain.

```rust
// ✅ CORRECT - Standard error pattern
match metadata_db::jobs::mark_running(&db, job_id).await {
    Ok(_) => {
        tracing::info!(job_id = %job_id, "job marked as running");
    }
    Err(err) => {
        tracing::error!(
            job_id = %job_id,
            error = %err,  // Display format - top-level error message
            error_source = logging::error_source(&err),  // Debug format - source chain
            "failed to mark job as running"
        );
    }
}

// ❌ WRONG - Using Debug for top-level error
tracing::error!(
    job_id = %job_id,
    error = ?err,  // WRONG - should be %err
    error_source = logging::error_source(&err),
    "failed to mark job as running"
);

// ❌ WRONG - Missing error_source
tracing::error!(
    job_id = %job_id,
    error = %err,
    "failed to mark job as running"
);
```

**Why This Pattern?**

- `error = %err` shows the immediate error message (Display)
- `error_source = logging::error_source(&err)` shows the complete chain (Debug of Vec<String>)
- Error fields come last before the message for consistency
- Consistent format enables automated log parsing and alerting
- Preserves full error context for debugging

## ERROR LOGGING PATTERNS (MANDATORY)

### 1. Mandatory Error and Error Source Fields

**ABSOLUTELY MANDATORY**: All error logs with `std::error::Error` objects **MUST** include both fields:

```rust
error = %err, error_source = logging::error_source(&err)
```

**Complete Pattern:**

```rust
// ✅ CORRECT - Complete error logging pattern
pub async fn execute_job(job_id: JobId) -> Result<(), Error> {
    match metadata_db::jobs::mark_running(&db, job_id).await {
        Ok(_) => {
            tracing::info!(job_id = %job_id, "job marked as running");
        }
        Err(err) => {
            tracing::error!(
                job_id = %job_id,
                error = %err,
                error_source = logging::error_source(&err),
                "failed to mark job as running"
            );
            return Err(Error::JobStateTransition(err));
        }
    }

    // Execute job logic
    match process_job_data(&job_id).await {
        Ok(result) => {
            tracing::info!(
                job_id = %job_id,
                rows_processed = result.row_count,
                duration_ms = result.duration.as_millis(),
                "job_processing_completed"
            );
            Ok(())
        }
        Err(err) => {
            tracing::error!(
                job_id = %job_id,
                error = %err,
                error_source = logging::error_source(&err),
                "job_processing_failed"
            );
            Err(Error::JobProcessing(err))
        }
    }
}

// ❌ WRONG - Missing error_source
tracing::error!(
    job_id = %job_id,
    error = %err,
    "failed to mark job as running"
);

// ❌ WRONG - Using Debug format for top-level error
tracing::error!(
    job_id = %job_id,
    error = ?err,
    error_source = logging::error_source(&err),
    "failed to mark job as running"
);

// ❌ WRONG - Different field names
tracing::error!(
    job_id = %job_id,
    err = %err,
    source = logging::error_source(&err),
    "failed to mark job as running"
);
```

### 2. Never Log Errors Without Context

**ALWAYS** include relevant operational context when logging errors.

```rust
// ✅ CORRECT - Error with context
tracing::error!(
    job_id = %job_id,
    dataset_name = %dataset_name,
    block_range = ?(start_block, end_block),
    error = %err,
    error_source = logging::error_source(&err),
    "failed_to_process_block_range"
);

// ✅ CORRECT - Retry context
tracing::warn!(
    node_id = %node_id,
    retry_attempt = attempt_num,
    max_retries = max_attempts,
    backoff_ms = delay.as_millis(),
    error = %err,
    error_source = logging::error_source(&err),
    "connection_retry_scheduled"
);

// ❌ WRONG - No context
tracing::error!(
    error = %err,
    error_source = logging::error_source(&err),
    "operation failed"
);

// ❌ WRONG - Generic message
tracing::error!(
    error = %err,
    error_source = logging::error_source(&err),
    "error"
);
```

### 3. Closure Parameter Naming

**ALWAYS** name error variables as `err` in error handling contexts. **NEVER** use `e`.

```rust
// ✅ CORRECT - Using 'err' parameter name
match metadata_db::jobs::get_by_id(&db, job_id).await {
    Ok(job) => process_job(job).await,
    Err(err) => {
        tracing::error!(
            job_id = %job_id,
            error = %err,
            error_source = logging::error_source(&err),
            "failed to fetch job"
        );
        return Err(Error::JobFetch(err));
    }
}

// ✅ CORRECT - Using 'err' in map_err
let result = some_operation()
    .await
    .map_err(|err| {
        tracing::error!(
            error = %err,
            error_source = logging::error_source(&err),
            "operation failed"
        );
        Error::OperationFailed(err)
    })?;

// ❌ WRONG - Using 'e' instead of 'err'
match metadata_db::jobs::get_by_id(&db, job_id).await {
    Ok(job) => process_job(job).await,
    Err(e) => {  // WRONG - should be 'err'
        tracing::error!(
            job_id = %job_id,
            error = %e,
            error_source = logging::error_source(&e),
            "failed to fetch job"
        );
        return Err(Error::JobFetch(e));
    }
}
```

### 4. Error Chain Preservation

**ALWAYS** use `monitoring::logging::error_source()` utility function to preserve the complete error chain.

```rust
// ✅ CORRECT - Using logging::error_source() utility
use monitoring::logging;

tracing::error!(
    job_id = %job_id,
    error = %err,
    error_source = logging::error_source(&err),  // Preserves full chain
    "job execution failed"
);

// ❌ WRONG - Manually accessing source
tracing::error!(
    job_id = %job_id,
    error = %err,
    source = ?err.source(),  // Only shows immediate source
    "job execution failed"
);

// ❌ WRONG - Not including error chain at all
tracing::error!(
    job_id = %job_id,
    error = %err,
    "job execution failed"
);

// ❌ WRONG - Using Debug format for entire error
tracing::error!(
    job_id = %job_id,
    error = ?err,  // WRONG - should be %err with separate error_source
    "job execution failed"
);
```

**Understanding `logging::error_source()`:**

```rust
/// Example error chain
#[derive(Debug, thiserror::Error)]
#[error("failed to fetch user data")]
struct FetchUserDataError(#[source] QueryExecutionError);

#[derive(Debug, thiserror::Error)]
#[error("failed to execute query")]
struct QueryExecutionError(#[source] DatabaseConnectionError);

#[derive(Debug, thiserror::Error)]
#[error("database connection refused")]
struct DatabaseConnectionError;

let err = FetchUserDataError(
    QueryExecutionError(
        DatabaseConnectionError
    )
);

// Logging output:
// error = "failed to fetch user data"
// error_source = ["failed to execute query", "database connection refused"]
tracing::error!(
    error = %err,  // Top-level: "failed to fetch user data"
    error_source = logging::error_source(&err),  // Chain: ["failed to execute query", "database connection refused"]
    "operation failed"
);
```

### 5. Retry Logging with Backon

**ALWAYS** use consistent logging patterns when using the `backon` crate for automatic retries with exponential backoff.

**Standard Backon Retry Pattern:**

```rust
use backon::{ExponentialBuilder, Retryable};
use monitoring::logging;

(|| async_operation())
    .retry(ExponentialBuilder::default())
    .when(|err| should_retry(err))
    .notify(|err, dur| {
        tracing::warn!(
            context_field = %context_value,
            error = %err,
            error_source = logging::error_source(&err),
            "Descriptive message explaining operation. Retrying in {:.1}s",
            dur.as_secs_f32()
        );
    })
    .await
```

**Key Requirements:**

- ✅ **Use `warn` level** - Retries are expected recoverable failures, not errors
- ✅ **Include context fields first** - job_id, node_id, etc. before error fields
- ✅ **Use standard error pattern** - `error = %err, error_source = logging::error_source(&err)`
- ✅ **Include retry delay** - Format as `"Retrying in {:.1}s"` with `dur.as_secs_f32()`
- ✅ **Error fields last before message** - Maintain consistent field ordering
- ❌ **Don't use `error` level** - Retries are not critical failures
- ❌ **Don't omit retry delay** - Users need to know when retry will occur

**Complete Examples:**

```rust
// ✅ CORRECT - Database connection retry with context
fn notify_retry(err: &sqlx::Error, dur: Duration) {
    tracing::warn!(
        error = %err,
        error_source = logging::error_source(&err),
        "Database still starting up during connection. Retrying in {:.1}s",
        dur.as_secs_f32()
    );
}

(|| PgConnection::connect(url))
    .retry(retry_policy)
    .when(is_db_starting_up)
    .notify(notify_retry)
    .await

// ✅ CORRECT - Job queue operation retry with context
(|| metadata_db::jobs::mark_running(&self.metadata_db, job_id))
    .retry(with_policy())
    .when(MetadataDbError::is_connection_error)
    .notify(|err, dur| {
        tracing::warn!(
            job_id = %job_id,
            error = %err,
            error_source = logging::error_source(&err),
            "Connection error while marking job as running. Retrying in {:.1}s",
            dur.as_secs_f32()
        );
    })
    .await

// ✅ CORRECT - Multiple context fields with retry
(|| metadata_db::jobs::get_active(&self.metadata_db, node_id))
    .retry(with_policy())
    .when(MetadataDbError::is_connection_error)
    .notify(|err, dur| {
        tracing::warn!(
            node_id = %node_id,
            error = %err,
            error_source = logging::error_source(&err),
            "Connection error while getting active jobs. Retrying in {:.1}s",
            dur.as_secs_f32()
        );
    })
    .await

// ❌ WRONG - Using error level for expected retries
.notify(|err, dur| {
    tracing::error!(
        error = %err,
        error_source = logging::error_source(&err),
        "Retry scheduled. Retrying in {:.1}s",
        dur.as_secs_f32()
    );
})

// ❌ WRONG - Missing retry delay information
.notify(|err, dur| {
    tracing::warn!(
        job_id = %job_id,
        error = %err,
        error_source = logging::error_source(&err),
        "Connection error, retrying"
    );
})

// ❌ WRONG - Context fields after error fields
.notify(|err, dur| {
    tracing::warn!(
        error = %err,
        error_source = logging::error_source(&err),
        job_id = %job_id,  // Should come before error fields
        "Connection error. Retrying in {:.1}s",
        dur.as_secs_f32()
    );
})

// ❌ WRONG - Missing error_source
.notify(|err, dur| {
    tracing::warn!(
        job_id = %job_id,
        error = %err,
        "Connection error. Retrying in {:.1}s",
        dur.as_secs_f32()
    );
})
```

**Why Use `warn` Level for Retries?**

- Retries are **expected** during normal operations (transient network issues, database restarts)
- `error` level should be reserved for **unrecoverable** failures requiring immediate attention
- `warn` level indicates degraded service that is being automatically remediated
- Consistent with industry best practices for retry logging

## COMPLETE EXAMPLES

### Example 1: Error Logging with Mandatory Pattern

**Context**: Handling database errors in worker service

```rust
use monitoring::logging;

// ✅ CORRECT - Mandatory error pattern
match metadata_db::jobs::mark_running(&self.metadata_db, job_id).await {
    Ok(_) => tracing::info!(job_id = %job_id, "job marked as running"),
    Err(err) => {
        tracing::error!(
            job_id = %job_id,
            error = %err,
            error_source = logging::error_source(&err),
            "failed to mark job as running"
        );
        return Err(Error::JobStateTransition(err));
    }
}
```

### Example 2: Retry Logic with Warn Level

**Context**: Retrying failed operations with exponential backoff

```rust
// ✅ CORRECT - Warn for retryable errors
Err(err) if retry_attempt < max_retries => {
    retry_attempt += 1;
    let backoff = Duration::from_secs(2_u64.pow(retry_attempt));

    tracing::warn!(
        retry_attempt,
        max_retries,
        backoff_secs = backoff.as_secs(),
        error = %err,
        error_source = logging::error_source(&err),
        "job execution failed, retrying with backoff"
    );

    tokio::time::sleep(backoff).await;
}
```

## CHECKLIST

Before committing code with error logging, verify:

### Error Logging (MANDATORY)

- [ ] All error logs include `error = %err` (Display format)
- [ ] All error logs include `error_source = logging::error_source(&err)`
- [ ] Error and error_source fields are the last fields before the message
- [ ] Context fields (job_id, node_id, etc.) come before error fields
- [ ] No use of `error = ?err` (Debug format for top-level error)
- [ ] Error variable named `err` (not `e`)
- [ ] `monitoring::logging` module imported where errors are logged

### Retry Logging (Backon)

- [ ] Retry logs use `warn` level (not `error`)
- [ ] Context fields come before error fields
- [ ] Standard error pattern used: `error = %err, error_source = logging::error_source(&err)`
- [ ] Retry delay included in message: `"Retrying in {:.1}s"` with `dur.as_secs_f32()`
- [ ] Descriptive message explains what operation is retrying

## References

- [logging](logging.md) - Related: General structured logging patterns
