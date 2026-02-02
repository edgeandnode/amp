---
name: "test-files"
description: "Test file placement, cfg(test) modules, it_* naming, in-tree vs tests/ directory. Load when creating test files or organizing test modules"
type: core
scope: "global"
---

# Test Files

## PURPOSE

This document defines where test files and modules go in the directory structure. It covers the three locations for tests (co-located, in-tree directory, `tests/` directory), the critical `it_` naming convention, and how to structure test modules using `#[cfg(test)]`.

For test function authoring (naming, Given-When-Then structure, assertions), see [test-functions.md](test-functions.md).

For the three-tier testing strategy overview, see [test-strategy.md](test-strategy.md).

## Table of Contents

1. [Quick Reference](#quick-reference)
2. [Unit Test Placement](#unit-test-placement)
3. [In-tree Integration Test Placement](#in-tree-integration-test-placement)
4. [Public API Integration Test Placement](#public-api-integration-test-placement)
5. [The it_ Naming Convention](#the-it_-naming-convention)
6. [Module Structure Within cfg(test)](#module-structure-within-cfgtest)
7. [File Naming Rules](#file-naming-rules)

---

## Quick Reference

**Directory Tree - Canonical Layout:**

```
<crate-root>/
  src/
    module.rs              # Source + #[cfg(test)] mod tests { ... }
    module/
      tests/
        validation.rs      # Unit tests (NO it_ prefix!)
        it_database.rs     # In-tree integration tests (it_ prefix)
  tests/
    it_api_workers.rs      # Public API integration tests (it_ prefix)
```

**Critical Rule**: The `it_` prefix distinguishes integration tests (testing cross-component interaction, which may or may not require external dependencies) from unit tests (testing a single function in isolation). For nextest filtering of external-dependency tests, name/regex patterns are used — the `it_` prefix alone does not determine whether a test needs external services.

---

## Unit Test Placement

Unit tests have **no external dependencies** and execute in **milliseconds**. They validate pure business logic, data transformations, and error handling.

### Option 1: Co-located Tests (Recommended for Simple Cases)

Tests live in the same file as the code, within a `#[cfg(test)]` module:

```rust
// <crate-root>/src/workers/node_id.rs
fn validate_worker_id(id: &str) -> Result<String, ValidationError> {
    if id.is_empty() {
        return Err(ValidationError::EmptyId);
    }
    if id.len() > 64 {
        return Err(ValidationError::TooLong);
    }
    Ok(id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    mod validation {
        use super::*;

        #[test]
        fn validate_worker_id_with_valid_input_succeeds() {
            //* Given
            let valid_id = "worker-123";

            //* When
            let result = validate_worker_id(valid_id);

            //* Then
            assert!(result.is_ok(), "validation should succeed with valid input");
            assert_eq!(result.expect("should return valid value"), valid_id);
        }

        #[test]
        fn validate_worker_id_with_empty_input_fails() {
            //* Given
            let empty_id = "";

            //* When
            let result = validate_worker_id(empty_id);

            //* Then
            assert!(result.is_err(), "validation should fail with empty input");
            let error = result.expect_err("should return validation error");
            assert!(matches!(error, ValidationError::EmptyId),
                "Expected EmptyId error, got {:?}", error);
        }
    }
}
```

**When to use**:
- Small number of tests per module (< 50 lines)
- Tests are simple and focused
- No complex test setup or fixtures needed

### Option 2: In-tree Tests Directory (For Complex Unit Test Suites)

For larger test suites, extract tests to `src/<module>/tests/` directory.

**CRITICAL**: Unit test modules in the `tests/` directory **MUST NOT** start with `it_`.

```rust
// <crate-root>/src/workers/tests/validation.rs  ← ✅ CORRECT - NO 'it_' prefix
use crate::workers::*;

mod unit_validation {
    use super::*;

    #[test]
    fn validate_worker_id_with_valid_input_succeeds() {
        //* Given
        let valid_id = "worker-123";

        //* When
        let result = validate_worker_id(valid_id);

        //* Then
        assert!(result.is_ok(), "validation should succeed with valid input");
        assert_eq!(result.expect("should return valid value"), valid_id);
    }

    #[test]
    fn validate_worker_id_with_empty_input_fails() {
        //* Given
        let empty_id = "";

        //* When
        let result = validate_worker_id(empty_id);

        //* Then
        assert!(result.is_err(), "validation should fail with empty input");
        let error = result.expect_err("should return validation error");
        assert!(matches!(error, ValidationError::EmptyId),
            "Expected EmptyId error, got {:?}", error);
    }
}

mod parsing_functions {  // ✅ CORRECT - Unit tests, no external dependencies
    use super::*;

    #[test]
    fn parse_worker_config_with_valid_json_succeeds() {
        //* Given
        let json_input = r#"{"name": "worker-1", "max_tasks": 10}"#;

        //* When
        let result = parse_worker_config(json_input);

        //* Then
        assert!(result.is_ok(), "parsing should succeed with valid JSON");
        let config = result.expect("should return valid config");
        assert_eq!(config.name, "worker-1");
        assert_eq!(config.max_tasks, 10);
    }
}
```

**When to use**:
- Large test suites (> 50 lines)
- Complex test fixtures or setup code
- Multiple test files for the same module
- Tests benefit from being separated from implementation

---

## In-tree Integration Test Placement

In-tree integration tests cover **internal functionality** not exposed through the crate's public API. These tests have **external dependencies** (database, network, filesystem).

**CRITICAL**: Integration test modules **MUST** start with `it_` for test filtering.

### Option 1: Inline Integration Test Submodule

```rust
// <crate-root>/src/workers.rs
pub async fn update_heartbeat_timestamp<'c, E>(
    executor: E,
    worker_id: &WorkerId,
    timestamp: chrono::DateTime<chrono::Utc>,
) -> Result<(), WorkerError>
where
    E: sqlx::Executor<'c, Database = sqlx::Postgres>,
{
    let query = indoc! {r#"
        UPDATE workers
        SET last_heartbeat = $1, updated_at = NOW()
        WHERE id = $2
    "#};

    sqlx::query(query)
        .bind(timestamp)
        .bind(worker_id.as_i64())
        .execute(executor)
        .await
        .map_err(WorkerError::Database)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Unit tests for pure functions here...

    mod it_heartbeat {  // ✅ CORRECT - 'it_' prefix for integration tests
        use super::*;
        use crate::temp::temp_metadata_db;

        #[tokio::test]
        async fn update_heartbeat_timestamp_updates_worker_record() {
            //* Given
            let db = temp_metadata_db().await;
            let worker_id = WorkerId::new(1);
            let new_timestamp = chrono::Utc::now();

            // Insert a test worker first
            let insert_result = insert(&db.pool, "test-worker").await;
            assert!(insert_result.is_ok(), "worker insertion should succeed");

            //* When
            let result = update_heartbeat_timestamp(&db.pool, &worker_id, new_timestamp).await;

            //* Then
            assert!(result.is_ok(), "heartbeat update should succeed");

            // Verify the timestamp was actually updated
            let updated_worker = get_by_id(&db.pool, &worker_id).await
                .expect("should retrieve updated worker")
                .expect("worker should exist");
            assert!(
                updated_worker.last_heartbeat.is_some(),
                "heartbeat timestamp should be set"
            );
        }

        #[tokio::test]
        async fn update_heartbeat_timestamp_with_nonexistent_worker_succeeds_silently() {
            //* Given
            let db = temp_metadata_db().await;
            let nonexistent_id = WorkerId::new(999);
            let timestamp = chrono::Utc::now();

            //* When
            let result = update_heartbeat_timestamp(&db.pool, &nonexistent_id, timestamp).await;

            //* Then
            assert!(result.is_ok(), "update should succeed even if worker doesn't exist");
        }
    }
}
```

**When to use**:
- Tests are closely related to implementation
- Small number of integration tests
- Tests benefit from proximity to source code

### Option 2: External Integration Test File

```rust
// <crate-root>/src/workers/tests/it_workers.rs  ← ✅ CORRECT - 'it_' prefix
use crate::workers::*;
use crate::temp::temp_metadata_db;

#[tokio::test]
async fn update_heartbeat_timestamp_updates_worker_record() {
    //* Given
    let db = temp_metadata_db().await;
    let worker_id = WorkerId::new(1);
    let new_timestamp = chrono::Utc::now();

    //* When
    let result = update_heartbeat_timestamp(&db.pool, &worker_id, new_timestamp).await;

    //* Then
    assert!(result.is_ok(), "heartbeat update should succeed");
}
```

**When to use**:
- Large integration test suites
- Complex test setup requiring dedicated files
- Multiple integration test files for same module

---

## Public API Integration Test Placement

Public API integration tests verify **end-to-end functionality** through the **crate's public API only**. This is Rust's standard integration testing mechanism.

**Location**: `<crate-root>/tests/` directory (outside `src/`)

**CRITICAL**: Files **MUST** be named `it_*` for test filtering.

```rust
// <crate-root>/tests/it_api_workers.rs  ← ✅ CORRECT - 'it_' prefix
use metadata_db::{MetadataDb, WorkerNodeId, JobStatus, Error};
use metadata_db::temp::temp_metadata_db;

#[tokio::test]
async fn register_worker_and_schedule_job_workflow_succeeds() {
    //* Given
    let db = temp_metadata_db().await;
    let node_id = WorkerNodeId::new("test-worker".to_string())
        .expect("should create valid worker node ID");

    // Register worker first
    let register_result = db.register_worker(&node_id).await;
    assert!(register_result.is_ok(), "worker registration should succeed");

    //* When
    let job_result = db.schedule_job(&node_id, "test job", JobStatus::Scheduled).await;

    //* Then
    assert!(job_result.is_ok(), "job scheduling should succeed");
    let job_id = job_result.expect("should return valid job ID");
    let retrieved_job = db.get_job(job_id).await
        .expect("should retrieve scheduled job")
        .expect("job should exist");

    assert_eq!(retrieved_job.node_id, node_id);
    assert_eq!(retrieved_job.status, JobStatus::Scheduled);
}

#[tokio::test]
async fn worker_lifecycle_complete_workflow_succeeds() {
    //* Given
    let db = temp_metadata_db().await;
    let node_id = WorkerNodeId::new("lifecycle-worker".to_string())
        .expect("should create valid worker node ID");

    // Register worker and set up initial state
    let register_result = db.register_worker(&node_id).await;
    assert!(register_result.is_ok(), "worker registration should succeed");

    let heartbeat_result = db.update_worker_heartbeat(&node_id).await;
    assert!(heartbeat_result.is_ok(), "heartbeat update should succeed");

    let mut job_ids = vec![];
    for i in 0..3 {
        let job_result = db.schedule_job(&node_id, &format!("job-{}", i), JobStatus::Scheduled).await;
        assert!(job_result.is_ok(), "job scheduling should succeed");
        job_ids.push(job_result.expect("should return job ID"));
    }

    //* When
    for job_id in job_ids.clone() {
        let complete_result = db.mark_job_completed(job_id).await;
        assert!(complete_result.is_ok(), "job completion should succeed");
    }

    //* Then
    let final_jobs = db.list_worker_jobs(&node_id).await
        .expect("should list final worker jobs");
    assert_eq!(final_jobs.len(), 3, "worker should have 3 completed jobs");
    assert!(
        final_jobs.iter().all(|job| job.status == JobStatus::Completed),
        "all jobs should be completed"
    );
}
```

**Characteristics**:
- **Public API only**: No access to internal crate APIs
- **Separate crate**: Each file in `tests/` is compiled as a separate crate
- **End-to-end testing**: Test complete user workflows
- **External dependencies**: May use database, network, etc.

---

## The it_ Naming Convention

The `it_` prefix is **CRITICAL** for distinguishing integration tests from unit tests. Test selection and filtering depend on this convention.

### Rules

| Test Type | Location | Naming Rule | Example |
|-----------|----------|-------------|---------|
| **Unit** (no external deps) | `#[cfg(test)] mod tests` | **NO** `it_` prefix | `mod validation` |
| **Unit** (no external deps) | `src/*/tests/*.rs` | **NO** `it_` prefix | `tests/validation.rs` |
| **In-tree Integration** | `#[cfg(test)] mod tests` | **YES** `it_` prefix | `mod it_heartbeat` |
| **In-tree Integration** | `src/*/tests/*.rs` | **YES** `it_` prefix | `tests/it_database.rs` |
| **Public API Integration** | `tests/*.rs` | **YES** `it_` prefix | `tests/it_api_workers.rs` |

### Why This Matters

The `it_` prefix identifies integration tests (cross-component interaction) broadly, enabling:
- Separation of unit tests (fast, isolated) from integration tests (multi-component)
- Targeted test execution via `cargo test 'tests::it_'` or `-- --skip 'tests::it_'`
- Organized test output with clear module path distinction

For filtering tests that require **external dependencies** specifically (database, network), nextest profiles use name/regex patterns beyond just the `it_` prefix. The prefix provides the first level of categorization; nextest configuration provides fine-grained control.

**Violating this convention breaks test filtering and causes local test failures.**

---

## Module Structure Within cfg(test)

**Recommended when a test module grows to 10+ tests.** For smaller modules, a flat list of test functions within `#[cfg(test)] mod tests` is sufficient. When the test count warrants grouping, use nested `mod` blocks to organize by concern:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    mod constructors {
        use super::*;
        // Tests for creation functions
        #[test]
        fn new_with_valid_config_succeeds() { /* ... */ }

        #[test]
        fn new_with_invalid_config_fails() { /* ... */ }
    }

    mod validation {
        use super::*;
        // Tests for validation logic
        #[test]
        fn validate_input_with_valid_data_succeeds() { /* ... */ }

        #[test]
        fn validate_input_with_invalid_data_fails() { /* ... */ }
    }

    mod it_database_operations {
        use super::*;
        use crate::temp::temp_metadata_db;
        // In-tree integration tests with database

        #[tokio::test]
        async fn database_operations_work_end_to_end() { /* ... */ }
    }
}
```

**Benefits**:
- Groups related tests together
- Reduces namespace pollution
- Makes test output more organized
- Allows shared test utilities per concern

---

## File Naming Rules

| Test Type | File Location | Filename Pattern | Example |
|-----------|---------------|------------------|---------|
| **Co-located unit** | Same as source | `*.rs` with `#[cfg(test)]` | `src/workers.rs` |
| **In-tree unit** | `src/*/tests/` | No `it_` prefix | `src/workers/tests/validation.rs` |
| **In-tree integration** | `src/*/tests/` | `it_*.rs` prefix | `src/workers/tests/it_database.rs` |
| **Public API integration** | `tests/` (crate root) | `it_*.rs` prefix | `tests/it_api_workers.rs` |

**Critical**: The `it_` prefix on filenames in `src/*/tests/` and `tests/` directories is **MANDATORY** for integration tests. This ensures correct test selection and filtering.

---

## CHECKLIST

Before creating or moving test files, verify:

- [ ] Unit tests (no external deps) are co-located or in `src/*/tests/` without `it_` prefix
- [ ] In-tree integration tests (with external deps) use `it_` prefix in module or filename
- [ ] Public API integration tests are in `tests/` directory with `it_*.rs` naming
- [ ] All tests use `#[cfg(test)]` module structure when co-located
- [ ] Module names accurately reflect whether tests have external dependencies
- [ ] Test file location matches test type (unit vs integration)

## RATIONALE

### Why co-locate unit tests?

Co-locating unit tests with source code makes it trivial to find tests for any function. When reading `src/workers.rs`, scroll down to see its `#[cfg(test)] mod tests`. This proximity encourages developers to keep tests updated as code evolves.

The `#[cfg(test)]` annotation ensures test code is never compiled into production binaries, so there's no performance or binary size penalty.

### Why the it_ prefix matters

Test filtering is essential for development velocity. Developers need fast feedback loops with unit tests (milliseconds), while integration tests requiring external dependencies (seconds to minutes) run separately.

The `it_` prefix enables test selection:
- `cargo test tests::` skips all `it_*` tests (unit tests only)
- `cargo test tests::it_` runs only `it_*` tests (integration only)
- `cargo nextest run --profile local` excludes external-dependency tests

Without consistent naming, test filtering fails, causing:
- Slow local development (running all integration tests)
- CI failures (missing required credentials)
- Developer frustration (unexplained test failures)

### Why separate tests/ directory for public API tests?

Rust's `tests/` directory compiles each file as a separate crate, ensuring tests only access the crate's public API. This prevents tests from depending on internal implementation details, making refactoring safer.

Public API tests serve as:
- **Integration smoke tests** - Verify complete workflows work
- **API contract validation** - Ensure exported interfaces don't break
- **User documentation** - Show real-world usage patterns

### Why nested modules within #[cfg(test)]?

For modules with 10+ tests, grouping by concern (constructors, validation, database_operations) makes test suites easier to navigate. When a test fails, the module path in the error message (e.g., `tests::validation::validate_input_with_empty_string_fails`) immediately indicates which area of functionality broke. For smaller test suites, this overhead is unnecessary — a flat list within `mod tests` is clearer.

Nested modules also enable:
- Scoped test utilities (`validation::create_test_input()`)
- Logical test organization (happy path, error cases, edge cases)
- Progressive test complexity (simple → complex within each module)

---
