---
name: "test-organization"
description: "Unit, integration, and e2e test tiers with it_* convention and nextest profiles. Load when deciding test type or placement."
type: core
scope: "global"
---

# Test Organization

## PURPOSE

This document defines the three-tier testing strategy used in Amp: **unit**, **integration**, and **e2e** tests. The unit and integration tiers each have two variants — **in-tree** (inside `src/`) and **public API** (in `<crate>/tests/`) — distinguished by the `it_*` naming convention.

For how to **run** tests (justfile tasks, nextest profiles, per-crate commands), see the `/code-test` skill.

For test function authoring (naming, Given-When-Then structure, assertions), see [test-functions.md](test-functions.md).

For test file organization (where tests go in the directory structure), see [test-files.md](test-files.md).

---

## Table of Contents

1. [Overview](#overview)
2. [Unit Tests](#unit-tests)
3. [Integration Tests](#integration-tests)
4. [E2E Tests](#e2e-tests)

---

## Overview

The three-tier testing strategy provides comprehensive coverage across different levels of abstraction, ensuring reliability and correctness from individual functions to complete workflows.

### Three Tiers

| Tier | Dependencies | Speed | Purpose | Nextest Profile |
|------|-------------|-------|---------|-----------------|
| **Unit** | None | Milliseconds | Pure business logic | `unit` |
| **Integration** | External (DB, Network) | Seconds | Components with real dependencies | `integration` |
| **E2E** | External (DB, Network) | Seconds | Cross-crate end-to-end workflows | `e2e` |

### In-tree vs Public API Variants

The unit and integration tiers each split into two variants based on **where** the test lives and **what** it can access:

| Variant | Location | API Access | Distinguishing Convention |
|---------|----------|------------|--------------------------|
| **In-tree** | `src/` (`#[cfg(test)]` modules) | Internal + public APIs | Unit: `tests::` (no `it_*`), Integration: `tests::it_*` |
| **Public API** | `<crate>/tests/` directory | Public API only | Unit: no `it_*` prefix, Integration: `it_*` prefix |

The `it_*` prefix is the **sole mechanism** that distinguishes integration tests from unit tests in both locations. Tests without `it_*` are unit tests; tests with `it_*` are integration tests.

**Key principle**: Start with unit tests for pure logic, use integration tests for components with external dependencies, and use e2e tests for cross-crate end-to-end workflows.

---

## Unit Tests

Unit tests must have **no external dependencies** and execute in **milliseconds**. These tests validate pure business logic, data transformations, and error handling without requiring database connections or external services.

### Purpose

Unit tests verify the correctness of individual functions and modules in isolation. They are the foundation of test coverage and should be fast, reliable, and comprehensive.

### Characteristics

- **NO EXTERNAL DEPENDENCIES**: No PostgreSQL database instance, no network calls, no filesystem operations (except temp dirs)
- **Performance**: Must complete execution in milliseconds
- **Reliability**: 100% deterministic, no flakiness
- **No `it_*` prefix**: Test functions and modules must NOT use the `it_*` naming convention

### Variants

#### In-tree Unit Tests

- **Location**: `src/` files, inside `#[cfg(test)] mod tests { ... }`
- **API access**: Internal and public APIs
- **Nextest filter**: `kind(lib)` excluding `test(/::it_/)`

#### Public API Unit Tests

- **Location**: `<crate>/tests/` directory, files without `it_*` prefix
- **API access**: Public API only (compiled as separate crate)
- **Nextest filter**: `kind(test)` excluding `test(/::it_/)`

### What to Test with Unit Tests

- **Data validation logic** — ID validation, input sanitization, format checking
- **Business rule enforcement** — Status transitions, constraint checking, invariant validation
- **Data transformation functions** — Parsing, formatting, conversion between types
- **Error condition handling** — Boundary cases, invalid inputs, edge conditions
- **Pure computational functions** — Calculations, algorithms, data structure operations

### Examples

**In-tree unit test:**

```rust
#[cfg(test)]
mod tests {
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
}
```

**Public API unit test:**

```rust
// tests/api_validation.rs  (no it_* prefix — this is a unit test)
use metadata_db::WorkerNodeId;

#[test]
fn parse_worker_id_from_string_succeeds() {
    //* Given
    let input = "worker-123";

    //* When
    let result = WorkerNodeId::new(input.to_string());

    //* Then
    let node_id = result.expect("should parse valid worker ID");
    assert_eq!(node_id.as_str(), input);
}
```

See [test-functions.md](test-functions.md#-complete-examples) for full examples with Given-When-Then structure.

**For file placement and module structure details**, see [test-files.md](test-files.md).

---

## Integration Tests

Integration tests verify that components work correctly with **external dependencies** like databases, network services, or the filesystem. They are distinguished from unit tests by the mandatory `it_*` naming convention.

### Purpose

Integration tests verify that code works correctly when interacting with real external systems. They test the integration between modules and external dependencies.

### Characteristics

- **External dependencies**: Use actual database connections or external services (e.g., `pgtemp` for PostgreSQL, Anvil for blockchain)
- **Mandatory `it_*` prefix on parent module**: Integration tests must live inside an `it_*`-prefixed module (or file) for filtering
- **Flakiness risk**: May fail due to external dependency issues (network, database constraints, etc.)
- **Performance**: Slower execution due to external dependencies (seconds, not milliseconds)

### Variants

#### In-tree Integration Tests

- **Location**: `src/` files, inside `tests::it_*` submodules
- **API access**: Internal and public APIs
- **File structure**: Either separate files in `src/<module>/tests/it_*.rs` or inline submodules named `mod it_*`
- **Nextest filter**: `kind(lib)` with `test(/::it_/)`

#### Public API Integration Tests

- **Location**: `<crate>/tests/` directory, files with `it_*` prefix
- **API access**: Public API only (compiled as separate crate)
- **Nextest filter**: `kind(test)` with `test(/::it_/)`

### What to Test with Integration Tests

- **Database operations** — CRUD operations, complex queries, transaction behavior
- **Transaction behavior** — Rollback on failure, atomicity, isolation guarantees
- **Error handling with external systems** — Network failures, database constraints, timeout handling
- **Resource management** — Connection pooling, cleanup, lifecycle management
- **Migration and schema changes** — Forward and backward compatibility

### Examples

**In-tree integration test:**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    mod it_heartbeat {
        use super::*;
        use crate::temp::temp_metadata_db;

        #[tokio::test]
        async fn update_heartbeat_timestamp_with_existing_worker_succeeds() {
            //* Given
            let db = temp_metadata_db().await;
            let worker_id = WorkerId::new(1);
            let new_timestamp = chrono::Utc::now();

            //* When
            let result = update_heartbeat_timestamp(&db.pool, &worker_id, new_timestamp).await;

            //* Then
            assert!(result.is_ok(), "heartbeat update should succeed");
            let updated_worker = get_by_id(&db.pool, &worker_id).await
                .expect("should retrieve updated worker")
                .expect("worker should exist");
            assert!(updated_worker.last_heartbeat.is_some(), "heartbeat timestamp should be set");
        }
    }
}
```

**Public API integration test:**

```rust
// tests/it_api_workers.rs  (it_* prefix — this is an integration test)
use metadata_db::{MetadataDb, WorkerNodeId, JobStatus};
use metadata_db::temp::temp_metadata_db;

#[tokio::test]
async fn register_worker_and_schedule_job_workflow_succeeds() {
    //* Given
    let db = temp_metadata_db().await;
    let node_id = WorkerNodeId::new("test-worker".to_string())
        .expect("should create valid worker node ID");
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
    assert_eq!(retrieved_job.status, JobStatus::Scheduled);
}
```

See [test-files.md](test-files.md) for full placement examples.

---

## E2E Tests

E2E tests live in the top-level `tests/` **workspace package** (not individual crate `tests/` directories). They test cross-crate, end-to-end workflows that span multiple components.

### Purpose

E2E tests verify that the system works correctly as a whole, testing complete workflows that cross crate boundaries.

### Characteristics

- **Top-level package**: Located in the workspace-level `tests/` package
- **Cross-crate scope**: Test interactions between multiple crates
- **External dependencies**: Typically require full environment (database, network, etc.)
- **Nextest filter**: `package(tests)`

### What to Test with E2E Tests

- **Cross-crate workflows** — Data flowing through multiple crates end-to-end
- **System integration** — Multiple services working together
- **Complete user scenarios** — Full request lifecycle from entry to completion

---

## CHECKLIST

When deciding which test tier and variant to use:

- [ ] Does the function have zero external dependencies? → **Unit test**
- [ ] Does the function need database/network/external services? → **Integration test** (use `it_*` prefix)
- [ ] Does the test span multiple crates end-to-end? → **E2E test** (top-level `tests/` package)
- [ ] Does the test need access to internal APIs? → **In-tree** variant (in `src/`)
- [ ] Should the test only use the public API? → **Public API** variant (in `<crate>/tests/`)
- [ ] Is the test fast (milliseconds)? → Unit test
- [ ] Is the test slow (seconds) due to external dependencies? → Integration test with `it_*` prefix

---

## RATIONALE

### Why Three Tiers?

The three-tier strategy balances comprehensive coverage with maintainability and performance:

1. **Unit tests** catch logic bugs quickly without external setup (milliseconds)
2. **Integration tests** verify components work with real dependencies (seconds)
3. **E2E tests** ensure the system works correctly across crate boundaries (seconds)

Each tier has a specific role and cannot replace the others. Unit tests cannot verify database behavior. Integration tests cannot verify cross-crate workflows. E2E tests are too slow and broad for isolated logic. All three tiers are necessary for complete confidence.

### Why `it_*` Prefix?

The `it_*` prefix is the sole mechanism that distinguishes integration tests from unit tests:

- Tests **without** `it_*` → unit tests (no external deps, fast)
- Tests **with** `it_*` → integration tests (external deps, slower)

This applies in both locations (`src/` and `<crate>/tests/`), enabling nextest profiles to filter precisely:

- `unit` profile: excludes `test(/::it_/)` → runs only unit tests
- `integration` profile: includes `test(/::it_/)` → runs only integration tests

### Why In-tree vs Public API?

**In-tree tests** (in `src/`) can access internal APIs not part of the public interface. This is essential for:

- Testing database query functions that don't need to be public
- Testing internal helper functions that support the public API
- Testing error paths in internal components

**Public API tests** (in `<crate>/tests/`) verify the external contract. They ensure that:

- The crate's public API is ergonomic and correct
- Workflows work as advertised through the public interface
- Error handling propagates correctly through the public API

Both variants exist within unit and integration tiers. The location determines API access; the `it_*` prefix determines whether external dependencies are involved.
