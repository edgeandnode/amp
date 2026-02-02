---
name: "test-strategy"
description: "Three-tier testing strategy: unit, in-tree integration, public API integration. Load when deciding which test type to write or understanding the test architecture"
type: core
scope: "global"
---

# Test Strategy

## PURPOSE

This document defines the three-tier testing strategy used in Amp: unit tests, in-tree integration tests, and public API integration tests. Each tier serves a specific purpose with different dependency requirements, performance characteristics, and organizational patterns.

For test function authoring (naming, Given-When-Then structure, assertions), see [test-functions.md](test-functions.md).

For test file organization (where tests go in the directory structure), see [test-files.md](test-files.md).

---

## Table of Contents

1. [Overview](#overview)
2. [Unit Tests](#unit-tests)
3. [In-tree Integration Tests](#in-tree-integration-tests)
4. [Public API Integration Tests](#public-api-integration-tests)
5. [Running Tests by Tier](#running-tests-by-tier)
6. [Progressive Test Complexity](#progressive-test-complexity)

---

## Overview

The three-tier testing strategy provides comprehensive coverage across different levels of abstraction, ensuring reliability and correctness from individual functions to complete workflows. Each test type serves a specific purpose and has different requirements, performance characteristics, and organizational patterns.

### Test Type Comparison

| Test Type | Dependencies | Speed | Purpose | Location |
|-----------|--------------|--------|---------|----------|
| **Unit Tests** | None | Milliseconds | Pure business logic | Same file (`#[cfg(test)]`) |
| **In-tree Integration** | External (DB, Network) | Slower | Internal APIs with dependencies | `tests::it_*` submodules |
| **Public API Integration** | External (DB, Network) | Slower | End-to-end workflows | `tests/` directory |

**Key principle**: Start with unit tests for pure logic, use in-tree integration tests for internal APIs with external dependencies, and use public API integration tests for end-to-end user workflows.

---

## Unit Tests

Unit tests must have **no external dependencies** and execute in **milliseconds**. These tests validate pure business logic, data transformations, and error handling without requiring database connections or external services.

### Purpose

Unit tests verify the correctness of individual functions and modules in isolation. They are the foundation of test coverage and should be fast, reliable, and comprehensive.

### Characteristics

- **NO EXTERNAL DEPENDENCIES**: No PostgreSQL database instance, no network calls, no filesystem operations (except temp dirs)
- **Performance**: Must complete execution in milliseconds
- **Co-location**: Tests live within the same file as the code being tested
- **Module structure**: Use `#[cfg(test)]` annotated `tests` submodule
- **Reliability**: 100% deterministic, no flakiness

### What to Test with Unit Tests

Unit tests should cover:

- **Data validation logic** — ID validation, input sanitization, format checking
- **Business rule enforcement** — Status transitions, constraint checking, invariant validation
- **Data transformation functions** — Parsing, formatting, conversion between types
- **Error condition handling** — Boundary cases, invalid inputs, edge conditions
- **Pure computational functions** — Calculations, algorithms, data structure operations

### Example

```rust
#[cfg(test)]
mod tests {
    mod validation {
        #[test]
        fn validate_worker_id_with_valid_input_succeeds() {
            //* Given — valid ID string
            //* When  — call validate_worker_id
            //* Then  — returns Ok with matching value
        }
    }
}
```

See [test-functions.md](test-functions.md#-complete-examples) for full unit test examples with Given-When-Then structure.

**For file placement and module structure details**, see [test-files.md](test-files.md).

---

## In-tree Integration Tests

In-tree integration tests cover **internal functionality** not exposed through the crate's public API. These tests are named "integration" because they test functionality with **external dependencies** and can suffer from slowness and flakiness.

### Purpose

In-tree integration tests verify that internal components work correctly with external dependencies like databases, network services, or the filesystem. They test the integration between internal modules and external systems.

### Characteristics

- **External dependencies**: Use actual database connections or external services (e.g., `pgtemp` for PostgreSQL, Anvil for blockchain)
- **Mandatory nesting**: Must live in `tests::it_*` submodules for test selection and filtering
- **File structure**: Either separate files in `src/<module>/tests/it_*.rs` or inline submodules named `mod it_*`
- **Flakiness risk**: May fail due to external dependency issues (network, database constraints, etc.)
- **Performance**: Slower execution due to external dependencies (seconds, not milliseconds)

### What to Test with In-tree Integration Tests

In-tree integration tests should cover:

- **Database operations** — CRUD operations, complex queries, transaction behavior
- **Transaction behavior** — Rollback on failure, atomicity, isolation guarantees
- **Error handling with external systems** — Network failures, database constraints, timeout handling
- **Resource management** — Connection pooling, cleanup, lifecycle management
- **Migration and schema changes** — Forward and backward compatibility

### Example

```rust
#[cfg(test)]
mod tests {
    mod it_heartbeat {
        #[tokio::test]
        async fn update_heartbeat_timestamp_updates_worker_record() {
            //* Given — temp database + inserted worker
            //* When  — call update_heartbeat_timestamp
            //* Then  — heartbeat timestamp is set
        }
    }
}
```

See [test-files.md](test-files.md#in-tree-integration-test-placement) for full in-tree integration test examples.

**For file placement and the `it_*` naming convention**, see [test-files.md](test-files.md).

---

## Public API Integration Tests

Public API tests are Rust's standard integration testing mechanism as defined in the [Rust Book](https://doc.rust-lang.org/book/ch11-03-test-organization.html#integration-tests). These tests verify **end-to-end functionality** by testing the integration between different code parts through the **crate's public API only**.

### Purpose

Public API integration tests verify that the crate's exported interface works correctly for end users. They test complete workflows and user scenarios using only the public API, ensuring that the crate delivers its promised functionality.

### Characteristics

- **Public API only**: No access to internal crate APIs unless explicitly made public
- **External location**: Located in `tests/` directory (separate from source code)
- **End-to-end testing**: Test complete user workflows and integration scenarios
- **External dependencies**: These ARE integration tests and MAY use external dependencies
- **Cargo integration**: Each file in `tests/` is compiled as a separate crate
- **File naming**: Must be named `it_*` for test filtering purposes

### What to Test with Public API Integration Tests

Public API integration tests should cover:

- **Complete user workflows** — Registration → job assignment → completion
- **Cross-resource interactions** — Workers + jobs + locations interacting through public API
- **Error propagation** — How errors bubble up through the public API
- **Concurrent operations** — Thread safety, consistency guarantees under concurrent load
- **Resource cleanup and lifecycle management** — Proper teardown, no resource leaks

### Example

```rust
// tests/it_api_workers.rs
use metadata_db::{MetadataDb, WorkerNodeId, JobStatus};

#[tokio::test]
async fn register_worker_and_schedule_job_workflow_succeeds() {
    //* Given — temp database + registered worker
    //* When  — schedule a job via public API
    //* Then  — job exists with correct status
}
```

See [test-files.md](test-files.md#public-api-integration-test-placement) for full public API integration test examples.

**For file placement rules**, see [test-files.md](test-files.md).

---

## Running Tests by Tier

The three-tier testing strategy allows for **selective test execution** based on performance and dependency requirements. Run tests from fastest (unit) to slowest (integration) to catch issues early.

### Unit Tests (fast, no external dependencies)

Run only unit tests, skipping all integration tests:

```bash
# Run only unit tests for a crate, skip in-tree integration tests
cargo test -p metadata-db 'tests::' -- --skip 'tests::it_'

# Run specific module's unit tests
cargo test -p metadata-db 'workers::tests::' -- --skip 'tests::it_'
```

### In-tree Integration Tests (slower, requires external dependencies)

Run only in-tree integration tests (requires database, network, etc.):

```bash
# Run only in-tree integration tests for a crate
cargo test -p metadata-db 'tests::it_'

# Run specific in-tree integration test suite
cargo test -p metadata-db 'tests::it_workers'

# Run in-tree integration tests for specific module
cargo test -p metadata-db 'workers::tests::it_'
```

### Public API Integration Tests (slower, requires external dependencies)

Run only public API integration tests:

```bash
# Run all public API integration tests for a crate
cargo test -p metadata-db --test '*'

# Run specific public API integration test file
cargo test -p metadata-db --test it_api_workers

# Run specific test within integration test file
cargo test -p metadata-db --test it_api_workers register_worker_and_schedule_job_workflow_succeeds
```

### Complete Test Suite (run all tiers in order of speed)

```bash
# Run all tests in order of speed (fastest first)
cargo test -p metadata-db 'tests::' -- --skip 'tests::it_'  # Unit tests first
cargo test -p metadata-db 'tests::it_'                       # In-tree integration second
cargo test -p metadata-db --test '*'                         # Public API integration last
```

**Rationale**: Running tests from fastest to slowest provides quick feedback on simple failures before investing time in slower integration tests.

---

## Progressive Test Complexity

Structure tests from simple to complex within each category. This pattern helps maintain clarity and makes test failures easier to debug.

### Progression Pattern

Within a test module, organize tests in order of increasing complexity:

1. **Basic functionality** — Happy path with minimal setup
2. **With configuration** — Custom options and parameters
3. **Error scenarios** — Invalid inputs, boundary cases
4. **External dependencies** — Database, network, filesystem
5. **Full integration** — Complete workflows, multiple resources

### Example

```rust
mod feature_progression {
    use super::*;

    // 1. Basic functionality
    #[test]
    fn validate_input_with_defaults_succeeds() {
        //* Given
        let input = create_basic_input();

        //* When
        let result = validate_input(input);

        //* Then
        assert!(result.is_ok(), "validation with default input should succeed");
    }

    // 2. With configuration
    #[test]
    fn validate_input_with_custom_config_succeeds() {
        //* Given
        let config = CustomConfig { option: true };

        //* When
        let result = validate_input_with_config(config);

        //* Then
        assert_eq!(result, expected_configured_value);
    }

    // 3. Error scenarios
    #[test]
    fn validate_input_with_empty_string_fails() {
        //* Given
        let invalid_input = create_invalid_input();

        //* When
        let result = validate_input(invalid_input);

        //* Then
        assert!(result.is_err(), "validation with invalid input should fail");
    }

    // 4. External dependencies
    #[tokio::test]
    async fn insert_record_with_valid_data_succeeds() {
        //* Given
        let db = temp_metadata_db().await;
        let test_data = create_test_data();

        //* When
        let result = insert_record(&db.pool, test_data).await;

        //* Then
        assert!(result.is_ok(), "inserting valid record should succeed");
    }

    // 5. Full integration
    #[tokio::test]
    async fn register_and_schedule_workflow_succeeds() {
        //* Given
        let db = temp_metadata_db().await;
        let workflow_data = create_workflow_data();

        //* When
        let result = complete_workflow(&db, workflow_data).await;

        //* Then
        assert!(result.is_ok(), "complete workflow should succeed");
        let completed = get_workflow_status(&db).await
            .expect("should retrieve workflow status");
        assert!(completed.is_finished, "workflow should be marked as finished");
    }
}
```

**Benefits**: This progression makes it easy to locate the right test when debugging failures, and it guides developers to write simple tests before complex ones.

---

## CHECKLIST

When deciding which test tier to use:

- [ ] Does the function have zero external dependencies? → Unit test
- [ ] Does the function need database/network but is internal API? → In-tree integration test with `it_*` prefix
- [ ] Does the test verify end-to-end workflows through public API? → Public API integration test in `tests/` directory
- [ ] Are you testing pure logic transformations? → Unit test
- [ ] Are you testing database transactions or queries? → In-tree integration test
- [ ] Are you testing complete user workflows? → Public API integration test
- [ ] Is the test fast (milliseconds)? → Unit test
- [ ] Is the test slow (seconds) due to external dependencies? → Integration test (in-tree or public API)

---

## RATIONALE

### Why Three Tiers?

The three-tier strategy balances comprehensive coverage with maintainability and performance:

1. **Unit tests** catch logic bugs quickly without external setup (milliseconds)
2. **In-tree integration tests** verify internal components work with real dependencies (seconds)
3. **Public API integration tests** ensure the crate delivers on its promises to users (seconds)

Each tier has a specific role and cannot replace the others. Unit tests cannot verify database behavior. Integration tests cannot verify that the public API is usable. All three tiers are necessary for complete confidence.

### Why `it_*` Prefix for Integration Tests?

The `it_*` prefix enables filtering:

- `cargo test 'tests::' -- --skip 'tests::it_'` runs only unit tests (fast feedback)
- `cargo test 'tests::it_'` runs only in-tree integration tests (targeted testing)

This naming convention is a pragmatic solution for selective test execution without restructuring the codebase.

### Why In-tree vs Public API?

**In-tree integration tests** can test internal APIs that aren't part of the public interface. This is essential for:

- Testing database query functions that don't need to be public
- Testing internal helper functions that support the public API
- Testing error paths in internal components

**Public API integration tests** verify the external contract. They ensure that:

- The crate's public API is ergonomic and correct
- Complete workflows work as advertised
- Error handling propagates correctly through the public API

Both are necessary for comprehensive test coverage.
