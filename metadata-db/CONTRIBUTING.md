# Contributing

This document serves as both human and AI agent context for understanding and contributing to the `metadata-db` crate.
The goal is safe, coordinated distributed processing with strong consistency guarantees.

## Overview

The `metadata-db` crate is a PostgreSQL-based metadata management system for Project Nozzle.
It serves as the central coordination layer for distributed data processing jobs,
location tracking, and worker node management in the Nozzle ETL pipeline.

## Context and Purpose

The `metadata-db` crate serves as the coordination backbone for Nozzle's distributed ETL architecture,
managing critical metadata for blockchain data processing workflows.
It enables safe coordination between multiple worker nodes processing terabytes of blockchain data
while maintaining strong consistency guarantees.

**Core Responsibilities:**

The metadata database handles multiple coordination concerns, including but not limited to:

- **Job Coordination**: Manages distributed job queues with state tracking (Scheduled → Running → Terminal)
- **Location Management**: Tracks dataset storage locations across different storage backends (local, S3, GCS, Azure)
- **Worker Orchestration**: Coordinates worker node registration, heartbeats, and job assignments
- **File Lifecycle**: Manages Parquet file metadata including size, compression, and schema information
- **Dataset Versioning**: Maintains dataset manifests and version history for reproducible data processing
- **Garbage Collection**: Schedules cleanup of expired files and inactive locations

**Distributed Architecture Context:**

The metadata database operates in a multi-worker environment where:
- Workers can join and leave dynamically
- Jobs must be resumable across worker failures
- Data consistency is critical for blockchain accuracy
- Storage locations span multiple cloud providers and regions
- Processing scales from gigabytes to terabytes of data

**Key Design Principles:**

- **Atomicity**: All state changes use database transactions to prevent inconsistent states
- **Idempotency**: Operations can be safely retried without side effects
- **Event-driven**: Uses PostgreSQL LISTEN/NOTIFY for real-time coordination
- **Resource isolation**: Clear boundaries between different metadata domains
- **Observability**: Comprehensive tracing and error reporting for debugging distributed issues

This crate is foundational to Nozzle's reliability,
ensuring that distributed blockchain data processing can scale safely
while maintaining the consistency guarantees required for financial and analytical workloads.

## Contributing Checklist

When contributing to `metadata-db`, ensure you follow these requirements:

**Code Requirements:**

- [ ] Follow all architectural patterns documented in this guide
- [ ] Use proper error handling with domain-specific error types (not generic `sqlx::Error`)
- [ ] Implement comprehensive rustdoc documentation for all public functions
- [ ] Follow function naming conventions (`insert_`, `get_`, `list_`, `mark_`, etc.)
- [ ] Use `indoc!` macro for all SQL statements
- [ ] Ensure proper parameter binding to prevent SQL injection
- [ ] Add `#[tracing::instrument]` for complex database operations

**Database Changes:**

- [ ] Add timestamped migration files for any schema changes
- [ ] Test migrations both forward and backward (if reversible)
- [ ] Use proper constraint naming conventions
- [ ] Implement cascading deletes where appropriate

**Testing Requirements:**

- [ ] Write unit tests for pure business logic (no external dependencies)
- [ ] Add in-tree integration tests for internal functionality with external dependencies
- [ ] Create public API integration tests for end-to-end workflows
- [ ] Follow GIVEN-WHEN-THEN structure with exactly one function under test
- [ ] Use descriptive test names: `function_scenario_expected_outcome`
- [ ] Never use `unwrap()` in tests - always use `expect()` with descriptive messages
- [ ] Include descriptive assertion messages for easier debugging

**Before Submitting:**

- [ ] Run unit tests: `cargo test -p metadata-db 'tests::' -- --skip 'tests::it_'`
- [ ] Run in-tree integration tests: `cargo test -p metadata-db 'tests::it_'`
- [ ] Run integration tests: `cargo test -p metadata-db 'tests::it_' --test '*'`
- [ ] Run formatter: `cargo fmt`
- [ ] Ensure no secrets or credentials are committed to the repository
- [ ] Verify proper transaction boundaries for multistep operations

## Crate structure and design patternReREs

### Database schemas and migration scripts (`migrations/`)

Migration files define the evolving structure of the PostgreSQL metadata database.
These SQL scripts are automatically discovered and executed by `sqlx` to maintain schema consistency across deployments.

**Execution Context:**

- Migrations are executed by `sqlx::migrate!()` macro at application startup
- `sqlx` tracks applied migrations in `_sqlx_migrations` table automatically
- Migration checksums prevent accidental modification of applied migrations
- Failed migrations must be manually resolved before restart

**File Naming:**

- Use timestamped format: `YYYYMMDDHHMMSS_descriptive_name.sql`
- Choose descriptive names that explain the purpose: `add_job_status`, `create_gc_manifest`
- One logical change per migration file
- Files must be in `migrations/` directory to be discovered by `sqlx`

**SQL Scripts Content Patterns:**

- Use `CREATE TABLE IF NOT EXISTS` for new tables to avoid conflicts
- Include proper foreign key constraints with cascade behavior
- Add indexes immediately after table creation in same migration
- Use consistent column naming: `created_at`, `updated_at` for timestamps

**Schema Design Guidelines:**

- Prefer BIGINT for all ID columns (PostgreSQL IDENTITY)
- Use TEXT over VARCHAR for string fields (PostgreSQL optimization)
- Store JSON metadata in JSONB columns for query performance
- Reference foreign keys by semantic names (`node_id`, `location_id`)

**Constraint Patterns:**

- Use partial unique indexes for conditional uniqueness (active locations)
- Name constraints descriptively: `unique_active_per_dataset_version_table`
- Implement cascading deletes where parent-child relationships exist

### Crate public API (`src/lib.rs`)

The `lib.rs` file serves as the unified public interface for the metadata database,
implementing a resource-oriented API design with clear separation of concerns.

**Implementation Block Organization:**

- One `impl MetadataDb` block per resource type (workers, jobs, locations, etc.)
- Each block groups related functionality for a specific domain
- Clear separation between resource concerns prevents method proliferation
- Logical grouping improves code navigation and maintainability

**Public API Structure:**

- `MetadataDb` struct provides connection pooling and serves as the main entry point
- All public methods return `Result<T, Error>` for consistent error handling
- Public methods act as transaction-coordinating wrappers around resource modules

**Re-export Pattern:**

- All types used in public APIs are re-exported from `lib.rs`
- Resource-specific types (`LocationId`, `JobId`, `WorkerNodeId`, etc.) exposed at crate root
- Complex types like `JobWithDetails`, `LocationWithDetails` for rich queries
- Error types consolidated into single `Error` enum covering all failure modes

**Transaction Coordination:**

- Multistep operations (job scheduling, location switching) handled in public methods
- Resource modules provide single-operation functions without transaction management
- No raw SQL statements in `lib.rs` - delegated to resource modules
- Atomic operations ensured through proper transaction boundaries

**Method Naming Conventions:**

- Resource views: `get_job()` vs `get_job_with_details()`
- CRUD operations: `register_`, `get_`, `list_`, `delete_` prefixes
- State changes: `mark_job_running()`, `set_active_location()`
- Bulk operations: `list_jobs()`, `delete_all_terminal_jobs()`

**Documentation Standards:**

- Every public method must have comprehensive rustdoc documentation
- Include purpose, parameters, return values, and error conditions
- Document transaction boundaries and state change implications
- Link to related methods within the same resource block

### Resource management modules (`src/jobs.rs`, `src/locations.rs`, `src/workers.rs`, etc.)

Resource management modules implement the core database operations for each metadata resource,
following a consistent single-responsibility design pattern aligned with database entities.

**Module Structure:**

- One module per metadata resource (`jobs`, `workers`, `locations`, etc.)
- Module names directly correspond to primary database table names
- Each module encapsulates all database operations for its specific resource
- Clear boundaries prevent cross-resource dependencies and maintain separation of concerns

**Module Organization Patterns:**

- **Simple resources**: Single file modules (`src/workers.rs`)
- **Complex resources**: When modules grow large, split into subdirectories (`src/locations/`)
- Main module file (`src/locations.rs`) serves as barrel export of internal submodules
- Submodules for related concerns: `locations/pagination.rs`, `locations/location_id.rs`
- Internal types and utilities kept within module boundaries

**Function Design:**

- All functions must be generic over `sqlx::Executor` trait for transaction flexibility
- Function names omit resource prefixes: `insert()`, `get_by_id()`, `list_first_page()`
- Called as `jobs::insert()`, `locations::get_by_id()` from public API
- Return domain-specific error types, not generic `sqlx::Error`
- Implement single-purpose operations without transaction management

**Function Naming Patterns:**

- **insert**: Add new records (`insert()`, `insert_with_default_status()`)
- **get**: Retrieve single items (`get_by_id()`, `get_by_node_id()`, `get_active_by_table_id()`)
- **list**: Retrieve multiple items (`list_first_page()`, `list_by_status()`, `list_by_node_id_and_statuses()`)
- **stream**: Streaming queries (`stream_file_metadata()`, `stream_expired_files()`)
- **update**: Modify multiple fields (`update_status()`, `update_heartbeat()`)
- **mark**: Change resource status (`mark_active_by_url()`, `mark_inactive_by_table_id()`)
- **delete**: Remove records (`delete_by_id()`, `delete_by_statuses()`, `delete_by_id_and_statuses()`)

**Function Naming Modifiers:**

- **by_id**: Primary key selection (`get_by_id()`, `delete_by_id()`)
- **by_<field>**: Single field conditionals (`get_by_node_id()`, `list_by_status()`)
- **\_and\_**: Multiple AND conditions (`get_by_node_id_and_statuses()`, `delete_by_id_and_statuses()`)
- **\_if\_**: Conditional operations (`update_status_if_any_state()`)
- **\_with_details**: Rich queries with joins (`get_by_id_with_details()`, `list_first_page_with_details()`)

**SQL Query Requirements:**

- All SQL statements must be written within `indoc!` macro blocks
- Use consistent indentation and formatting for SQL readability
- Use `sqlx::query_as` for type-safe result mapping with proper binding
- No raw string literals for multi-line queries
- **SECURITY:** Proper parameter binding to prevent SQL injection

**Cursor-Based Pagination Pattern:**

- Implement `list_first_page(limit)` and `list_next_page(limit, cursor)` function pairs
- Use primary key or timestamp fields as cursor values for consistent ordering
- `list_first_page()` starts from beginning without cursor parameter
- `list_next_page()` continues from given cursor using `WHERE id > $cursor`
- Consistent `ORDER BY` clauses ensure stable pagination across calls
- Limit parameter controls page size for memory management

**Type Definitions:**

- Resource structs defined within their respective modules
- Resource structs must NOT implement serialization/deserialization logic.
  Exception: ID _new-type_ wrappers (`JobId`, `LocationId`) may implement serde traits
- Domain-specific enums (`JobStatus`) with database representation
- Export types through module `pub use` for consumption by `lib.rs`

**Function Documentation Requirements:**

- Every public function must have comprehensive rustdoc documentation
- Include clear description of purpose and behavior
- Document all parameters with their types and expected values
- Specify return values and their meaning
- Document error conditions and error types returned
- Add `#[tracing::instrument]` attribute for complex database operations
- Use consistent documentation style across all resource modules

### DB connection and pooling (`src/conn.rs`)

The `conn` module is an internal module that manages database connectivity and connection pooling for the metadata
database.
This module provides the foundational database connection infrastructure,
including connection pool management, automatic schema migrations, and connection retry logic.
It encapsulates `sqlx` connection handling and provides the `DbConnPool` type
that serves as the underlying connection layer for the `MetadataDb` public API.

### Temporary database support for tests (`src/temp.rs`)

The `temp` module provides isolated PostgreSQL database instances for testing,
enabling safe parallel test execution without shared state conflicts.
This module creates ephemeral databases for each test suite,
automatically handling database lifecycle including creation, migration, and cleanup.
Available only when the `temp-db` feature is enabled,
it provides the `temp_metadata_db()` function that returns a configured `MetadataDb` instance
ready for testing with fresh schema state.

## Testing Strategy

The `metadata-db` crate employs a comprehensive three-tier testing approach
to ensure reliability and correctness across different levels of abstraction.

### Writing tests

All tests follow consistent naming conventions and structure patterns
to ensure clarity and maintainability across the codebase.

**Function Naming Convention:**
Test functions use descriptive names that explain the scenario being tested: `<function_name>_<scenario>_<expected_outcome>()`

- `insert_job_with_valid_data_succeeds()`
- `get_by_id_with_nonexistent_id_returns_none()`
- `update_status_with_invalid_transition_fails()`

**GIVEN-WHEN-THEN Structure:**
Every test follows the GIVEN-WHEN-THEN pattern for clear organization:

- **GIVEN**: Set up preconditions and data
- **WHEN**: Execute **EXACTLY ONE** function under test
- **THEN**: Assert expected outcomes and side effects

**Single Function Under Test:**
The WHEN section must test exactly one function call.
Multiple function calls indicate the test scope is too broad
and should be split into separate functions.

**Test Error Handling:**
Never use `unwrap()` in tests. Always use `expect()` with descriptive error messages
to provide clear context when tests fail.
Assertions should include descriptive error messages for easier debugging
when test failures occur.

**Example:**

```rust
#[tokio::test]
async fn insert_job_with_valid_data_succeeds() {
    //* Given
    let db = temp_metadata_db().await;
    let node_id = WorkerNodeId::new("worker-1".to_string())
        .expect("should create valid worker node ID");
    let job_desc = "job description";

    //* When
    let result = jobs::insert(&db.pool, &node_id, job_desc, JobStatus::Scheduled).await;

    //* TheN
    assert!(result.is_ok(), "job insertion should succeed with valid data");
    let job_id = result.expect("should return valid job ID");
    assert!(job_id.as_i64() > 0, "job ID should be positive");
}
```

### Test organization

Each test type serves a specific purpose:
**unit tests** validate individual function behavior,
**in-tree integration tests** verify resource module operations with real databases,
and **public API integration tests** ensure end-to-end workflow correctness
through the complete `MetadataDb` interface.

#### Unit Tests

Unit tests must have no external dependencies and execute in milliseconds.
These tests validate pure business logic, data transformations, and error handling
without requiring database connections or external services.

**Requirements:**

- **NO EXTERNAL DEPENDENCIES**: No PostgreSQL database instance required
- **Performance**: Must complete execution in milliseconds
- **Co-location**: Tests live within the same file as the code being tested
- **Module structure**: Use `#[cfg(test)]` annotated `tests` submodule

**Example:**

```rust
// src/workers/node_id.rs
fn validate_worker_id(id: &str) -> Result<String, Box<dyn std::error::Error>> {
    // [...]
}

// [...]

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_worker_id_with_valid_input_succeeds() {
        // [...]
    }

    // [...]
}
```

#### In-tree integration tests

In-tree integration tests cover internal functionality, that is, functionality not exposed through the crate's API.
These tests are named "integration" because they test functionality with external dependencies
and as a consequence they can suffer from slowness and flakiness.

**Purpose:**

- Test internal module functions that aren't part of the crate's public API
- Cover internal functionality that requires external dependencies (database, network, filesystem)
- Verify integration between internal components
- Test edge cases and error conditions that involve external systems

**Characteristics:**

- **External dependencies**: Use actual database connections or external services
- **Mandatory nesting**: Must live in `tests::it_*` submodules for test selection
- **File structure**: Either separate files (`tests/it_jobs.rs`) or inline submodules
- **Flakiness risk**: May fail due to external dependency issues
- **Performance**: Slower execution due to external dependencies

**File Structure Options:**

**Option 1: Separate integration test file**

```rust
// src/workers.rs
async fn update_heartbeat_timestamp<'c, E>(/* ... */) -> Result<(), sqlx::Error> {
    // [...]
}

#[cfg(test)]
mod tests {
    use super::*;

    // Unit tests here...

    mod it_heartbeat { // <-- Inline in-tree integration test submodule
        use super::*;

        #[tokio::test]
        async fn update_heartbeat_timestamp_updates_worker_record() {
            // [...]
        }
    }
}
```

**Option 2: External integration test file**

```rust
// src/workers/tests/it_workers.rs
use crate::workers::*;

#[tokio::test]
async fn update_heartbeat_timestamp_updates_worker_record() {
    // [...]
}
```

#### Public API integration tests

Public API tests are Rust's standard integration testing mechanism as defined in
the [Rust Book](https://doc.rust-lang.org/book/ch11-03-test-organization.html#integration-tests).
These tests verify end-to-end functionality by testing the integration between different code parts
through the crate's public API only.

**Purpose:**

- Test the public crate interface and all exported functionality
- Verify integration between different components of the crate
- Ensure complete workflows work correctly from a user perspective
- Test the crate as external users would use it

**Characteristics:**

- **Public API only**: No access to internal crate APIs unless explicitly made public
- **External location**: Located in `tests/` directory (separate from source code)
- **End-to-end testing**: Test complete user workflows and integration scenarios
- **External dependencies**: These ARE integration tests and MAY use external dependencies like databases
- **Cargo integration**: Each file in `tests/` is compiled as a separate crate
- **File naming**: Must be named `it_*` for test filtering purposes

**File Structure:**

```rust
// tests/it_api_workers.rs
use metadata_db::{MetadataDb, WorkerNodeId, Error};
use metadata_db::temp::temp_metadata_db;

#[tokio::test]
async fn register_worker_and_schedule_job_workflow_succeeds() {
    //* Given
    let db = temp_metadata_db().await;
    let node_id = WorkerNodeId::new("test-worker".to_string())
        .expect("should create valid worker node ID");

    //* When
    let result = db.register_worker(&node_id).await;

    //* Then
    assert!(result.is_ok(), "worker registration should succeed");
}

// [...]
```

### Running Tests

The three-tier testing strategy allows for selective test execution based on performance and dependency requirements.

**Unit Tests (fast, no external dependencies):**

```bash
# Run only unit tests, skip all in-tree integration tests
cargo test -p metadata-db 'tests::' -- --skip 'tests::it_'
```

**In-tree Integration Tests (slower, requires external dependencies):**

```bash
# Run only in-tree integration tests
cargo test -p metadata-db 'tests::it_'

# Run specific in-tree integration test suite
cargo test -p metadata-db 'tests::it_workers'
```

**Public API Integration Tests (slower, requires external dependencies):**

```bash
# Run all public API integration tests
cargo test -p metadata-db --test '*'

# Run specific public API integration test file
cargo test -p metadata-db --test it_api_workers
```
