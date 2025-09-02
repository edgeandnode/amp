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
- [ ] Include a description of changes being made in the migration filename (like `timestamp_add_[stuff].sql` or `timestamp_update_[stuff].sql`)
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

**For detailed testing patterns and examples, see [.patterns/testing-patterns.md](../.patterns/testing-patterns.md).**

### Test Organization Overview

- **Unit Tests**: Pure business logic without external dependencies (milliseconds)
- **In-tree Integration Tests**: Internal functionality with external dependencies (`tests::it_*` submodules)
- **Public API Integration Tests**: End-to-end workflows through public API (`tests/` directory)

### Running Tests

**Unit Tests (fast, no external dependencies):**
```bash
cargo test -p metadata-db 'tests::' -- --skip 'tests::it_'
```

**In-tree Integration Tests (slower, requires external dependencies):**
```bash
cargo test -p metadata-db 'tests::it_'
```

**Public API Integration Tests (slower, requires external dependencies):**
```bash
cargo test -p metadata-db --test '*'
```
