# Contributing

This document serves as both human and AI agent context for understanding and contributing to the `metadata-db` crate.
The goal is safe, coordinated distributed processing with strong consistency guarantees.

## Overview

The `metadata-db` crate is a PostgreSQL-based metadata management system for Project Nozzle.
It serves as the central coordination layer for distributed data processing jobs,
location tracking, and worker node management in the Nozzle ETL pipeline.

## Context and Purpose

**🎯 PRIMARY PURPOSE: Pure Data Access Layer**

The `metadata-db` crate has ONE responsibility: **provide safe, transactional data access to PostgreSQL-stored metadata**. This crate is strictly a data access layer that offers CRUD operations, queries, and transaction management - nothing more.

**✅ What `metadata-db` IS:**
- A PostgreSQL connection pool manager with automatic migrations
- A type-safe interface for database operations (insert, get, list, update, delete)  
- A transaction coordinator ensuring ACID properties
- A database schema manager with versioned migrations
- A connection abstraction that handles retries and error mapping

**❌ What `metadata-db` is NOT:**
- A business logic container (validation, workflow rules, domain constraints belong in consuming crates)
- A coordination service (consuming crates implement coordination using metadata-db data)
- An application layer (application-specific logic belongs in dump, admin-api, server, etc.)
- A decision-making component (consuming crates make decisions based on data retrieved from metadata-db)

**🎯 Single Responsibility Principle:**

This crate exists solely to **safely persist and retrieve metadata**. All higher-level concerns - including but not limited to business rules, workflow coordination, validation logic, and application-specific constraints - are the responsibility of consuming crates that use metadata-db as their data persistence layer.


**🏗️ Architectural Context:**

In Nozzle's distributed architecture, metadata-db serves as the **shared data persistence layer** that consuming crates use to:
- Store and retrieve job states (consuming crates implement job scheduling)
- Persist location information (consuming crates implement location selection)
- Track worker registrations (consuming crates implement worker orchestration)
- Record file metadata (consuming crates implement file lifecycle management)

This clear separation ensures that business logic remains in appropriate application layers while data persistence concerns are isolated in this single, focused crate.

**Key Design Principles:**

- **Data Access Layer Only**: The metadata-db crate provides ONLY data access operations (CRUD, queries, transactions). All business logic, validation, and domain-specific rules MUST be implemented in consuming crates. This maintains clear architectural boundaries and prevents the metadata layer from becoming a monolithic business logic container.
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
- [ ] **🚫 NO BUSINESS LOGIC**: The `metadata-db` crate must contain ONLY data access operations. Business logic validation, domain-specific rules, and application-specific constraints belong in consuming crates (dump, admin-api, etc.)
- [ ] Use proper error handling with domain-specific error types (not generic `sqlx::Error`)
- [ ] Implement comprehensive rustdoc documentation for all public functions
- [ ] Follow function naming conventions (`insert_`, `get_`, `list_`, `mark_`, etc.)
- [ ] Use `indoc::indoc!` macro for multiline SQL statements (see SQL Query Requirements below)
- [ ] Ensure proper parameter binding to prevent SQL injection
- [ ] Add `#[tracing::instrument]` for complex database operations

**Database Changes:**

- [ ] Add timestamped migration files for any schema changes
- [ ] Include a description of changes being made in the migration filename (like `YYYYMMDDHHMMSS_create_<table_name>_table.sql`, `YYYYMMDDHHMMSS_add_<column_name>_to_<table_name>.sql`, or `YYYYMMDDHHMMSS_add_index_<table_name>_<column_names>.sql`)
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

- [ ] **🔐 SECURITY REVIEW**: Complete the comprehensive security checklist in [SECURITY.md](./SECURITY.md) - this is mandatory for all changes
- [ ] Run formatter: `just fmt`
- [ ] Run unit tests: `cargo test -p metadata-db 'tests::' -- --skip 'tests::it_'`
- [ ] Run in-tree integration tests: `cargo test -p metadata-db 'tests::it_'`
- [ ] Run integration tests: `cargo test -p metadata-db 'tests::it_' --test '*'`
- [ ] Ensure no secrets or credentials are committed to the repository
- [ ] Verify proper transaction boundaries for multistep operations

## 🔐 Security Requirements

**🚨 CRITICAL: Before contributing to `metadata-db`, you MUST review and follow the comprehensive security guidelines.**

Security is fundamental to the metadata-db crate due to its role in managing sensitive blockchain data and coordination metadata. The crate handles:

- **Database connections** with potentially sensitive configuration data
- **SQL query execution** that must be protected against injection attacks
- **Transaction management** requiring proper isolation and consistency
- **Audit logging** for compliance with financial and regulatory standards

**🚨 MANDATORY: Review [SECURITY.md](./SECURITY.md) before making any changes to this crate.**

The security guidelines cover:
- SQL injection prevention and parameterized query requirements
- Database connection security and encryption requirements
- Access control and permission management
- OWASP Top 10 mitigation strategies
- SOC2 compliance requirements
- Secure coding patterns and anti-patterns

**Security violations may result in:**
- Rejection of pull requests
- Security audit requirements
- Compliance violations
- Data breach risks

All contributors MUST complete the security checklist in [SECURITY.md](./SECURITY.md) as part of their development process.

## Crate Structure

The `metadata-db` crate is organized into these key components:

- **Database schemas and migration scripts (`migrations/`)**
- **Crate public API (`src/lib.rs`)**
- **Resource management modules (`src/jobs.rs`, `src/locations.rs`, `src/workers.rs`, etc.)**
- **DB connection and pooling (`src/conn.rs`)**
- **Temporary database support for tests (`src/temp.rs`)**

## Design Patterns and Requirements

### Database Schemas and Migration Scripts (`migrations/`)

Migration files define the evolving structure of the PostgreSQL metadata database.
These SQL scripts are automatically discovered and executed by `sqlx` to maintain schema consistency across deployments.

**Execution Context:**

- Migrations are executed by `sqlx::migrate!()` macro at application startup
- `sqlx` tracks applied migrations in `_sqlx_migrations` table automatically
- Migration checksums prevent accidental modification of applied migrations
- Failed migrations must be manually resolved before restart

**File Naming Requirements:**

- Use timestamped format: `YYYYMMDDHHMMSS_descriptive_name.sql`
- Choose descriptive names that explain the purpose
- One logical change per migration file
- Files must be in `migrations/` directory to be discovered by `sqlx`

**File Naming Examples:**
```
✅ CORRECT Examples:
20240315143022_create_jobs_table.sql
20240315143100_add_status_column_to_jobs.sql
20240315143200_create_locations_table.sql
20240315143300_add_unique_constraint_active_locations.sql
20240315143400_create_workers_table.sql
20240315143500_add_foreign_key_jobs_to_workers.sql
20240315143600_create_file_metadata_table.sql
20240315143700_add_index_jobs_status_created_at.sql
20240315143800_create_gc_manifest_table.sql
20240315143900_add_jsonb_column_locations_metadata.sql

❌ WRONG Examples:
migration_1.sql                   (no timestamp, unclear purpose)
jobs.sql                          (no timestamp, too vague)
20240315_jobs.sql                 (timestamp format incomplete)
create_jobs_20240315.sql          (timestamp in wrong position)
20240315143022_update_stuff.sql   (vague description)
```

**SQL Scripts Content Patterns:**

- Use `CREATE TABLE IF NOT EXISTS` for new tables to avoid conflicts
- Include proper foreign key constraints with cascade behavior
- Add indexes immediately after table creation in same migration
- Use consistent column naming: `created_at`, `updated_at` for timestamps

**SQL Migration Content Examples:**
```sql
-- ✅ CORRECT: Table creation with proper constraints
CREATE TABLE IF NOT EXISTS jobs (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    node_id TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'scheduled',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_jobs_node_id FOREIGN KEY (node_id) 
        REFERENCES workers(node_id) ON DELETE CASCADE
);

-- Add indexes immediately after table creation
CREATE INDEX IF NOT EXISTS idx_jobs_status_created_at 
    ON jobs(status, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_node_id 
    ON jobs(node_id);

-- ✅ CORRECT: Adding column with proper constraints
ALTER TABLE locations 
ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}';

-- ✅ CORRECT: Creating partial unique index
CREATE UNIQUE INDEX IF NOT EXISTS unique_active_per_dataset_version_table
    ON locations(dataset_name, dataset_version, table_name) 
    WHERE is_active = true;
```

**Schema Design Requirements:**

- Prefer BIGINT for all ID columns (PostgreSQL IDENTITY)
- Use TEXT over VARCHAR for string fields (PostgreSQL optimization)
- Store JSON metadata in JSONB columns for query performance
- Reference foreign keys by semantic names (`node_id`, `location_id`)

**Constraint Patterns:**

- Use partial unique indexes for conditional uniqueness (active locations)
- Name constraints descriptively: `unique_active_per_dataset_version_table`
- Implement cascading deletes where parent-child relationships exist

### Public API Design Patterns (`src/lib.rs`)

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

The public API follows consistent naming patterns that make function purposes immediately clear. All method names use `snake_case` and follow semantic patterns based on their operation type.

**Core CRUD Operation Patterns:**
- **`get_<resource>()`**: Retrieve single resource by ID
  - Examples: `get_job()`, `get_location()`, `get_worker()`
  - Always takes resource ID as parameter, returns `Option<Resource>`

- **`get_<resource>_with_details()`**: Retrieve resource with related data via joins
  - Examples: `get_job_with_details()`, `get_location_with_details()`
  - Returns enriched resource struct with related information

- **`list_<resources>()`**: Retrieve multiple resources (paginated)
  - Examples: `list_jobs()`, `list_locations()`, `list_workers()`
  - Returns paginated results, typically first page

- **`register_<resource>()`**: Create/insert new resource (preferred over "create")
  - Examples: `register_job()`, `register_worker()`, `register_location()`
  - Takes creation parameters, returns resource ID

- **`delete_<resource>()`**: Remove single resource by ID
  - Examples: `delete_job()`, `delete_location()`, `delete_worker()`
  - Returns boolean indicating if resource was found and deleted

**State Management Patterns:**

- **`mark_<resource>_<state>()`**: Change resource to specific state
  - Examples: `mark_job_running()`, `mark_job_completed()`, `mark_location_active()`
  - Atomic state transitions with validation

- **`set_<resource>_<property>()`**: Update specific resource property
  - Examples: `set_active_location()`, `set_job_description()`
  - For simple property updates

**Bulk Operation Patterns:**

- **`list_<resources>_by_<criteria>()`**: Filter resources by specific conditions
  - Examples: `list_jobs_by_status()`, `list_locations_by_dataset()`
  - Filtered paginated queries

- **`delete_<resources>_by_<criteria>()`**: Bulk deletion with conditions  
  - Examples: `delete_jobs_by_status()`, `delete_expired_files()`
  - Mass operations with safety constraints

**Specialized Query Patterns:**

- **`count_<resources>()`**: Get resource count without fetching data
  - Examples: `count_active_jobs()`, `count_locations_by_dataset()`
  - Efficient counting queries

- **`exists_<resource>()`**: Check resource existence without fetching
  - Examples: `exists_job()`, `exists_active_location()`
  - Boolean existence checks

**Method Naming Examples by Resource:**

```rust
// ✅ Jobs Resource Methods
impl MetadataDb {
    pub async fn register_job(&self, node_id: &str, description: &str) -> Result<JobId, Error> {}
    pub async fn get_job(&self, job_id: &JobId) -> Result<Option<Job>, Error> {}
    pub async fn get_job_with_details(&self, job_id: &JobId) -> Result<Option<JobWithDetails>, Error> {}
    pub async fn list_jobs(&self, limit: i32) -> Result<Vec<Job>, Error> {}
    pub async fn list_jobs_by_status(&self, status: &JobStatus, limit: i32) -> Result<Vec<Job>, Error> {}
    pub async fn mark_job_running(&self, job_id: &JobId) -> Result<(), Error> {}
    pub async fn mark_job_completed(&self, job_id: &JobId) -> Result<(), Error> {}
    pub async fn delete_job(&self, job_id: &JobId) -> Result<bool, Error> {}
    pub async fn delete_jobs_by_status(&self, status: &JobStatus) -> Result<u64, Error> {}

    // ✅ Locations Resource Methods  
    pub async fn register_location(&self, params: &LocationParams) -> Result<LocationId, Error> {}
    pub async fn get_location(&self, location_id: &LocationId) -> Result<Option<Location>, Error> {}
    pub async fn get_active_location(&self, table_id: &TableId) -> Result<Option<Location>, Error> {}
    pub async fn list_locations(&self, limit: i32) -> Result<Vec<Location>, Error> {}
    pub async fn set_active_location(&self, location_id: &LocationId) -> Result<(), Error> {}
    pub async fn mark_location_inactive(&self, location_id: &LocationId) -> Result<(), Error> {}
    pub async fn delete_location(&self, location_id: &LocationId) -> Result<bool, Error> {}

    // ✅ Workers Resource Methods
    pub async fn register_worker(&self, node_id: &str) -> Result<(), Error> {}
    pub async fn get_worker(&self, node_id: &str) -> Result<Option<Worker>, Error> {}
    pub async fn list_workers(&self, limit: i32) -> Result<Vec<Worker>, Error> {}
    pub async fn update_worker_heartbeat(&self, node_id: &str) -> Result<(), Error> {}
    pub async fn delete_worker(&self, node_id: &str) -> Result<bool, Error> {}
}
```

**❌ Wrong Method Naming Examples:**

```rust
// ❌ WRONG: Inconsistent naming patterns
impl MetadataDb {
    pub async fn create_job(&self) -> Result<JobId, Error> {}           // Use register_job()
    pub async fn fetch_job(&self) -> Result<Option<Job>, Error> {}      // Use get_job()  
    pub async fn find_jobs(&self) -> Result<Vec<Job>, Error> {}         // Use list_jobs()
    pub async fn remove_job(&self) -> Result<bool, Error> {}            // Use delete_job()
    pub async fn update_job_to_running(&self) -> Result<(), Error> {}   // Use mark_job_running()
    pub async fn get_jobs(&self) -> Result<Vec<Job>, Error> {}          // Use list_jobs()
    pub async fn job_exists(&self) -> Result<bool, Error> {}            // Use exists_job()
}
```

**Documentation Standards:**

- Every public method must have comprehensive rustdoc documentation
- Include purpose, parameters, return values, and error conditions
- Document transaction boundaries and state change implications
- Link to related methods within the same resource block

### Resource Management Design Patterns

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

**Example Function Signatures:**
```rust
// ✅ CORRECT: Generic over Executor, returns domain error
pub async fn insert<'c, E>(
    exe: E, 
    node_id: &WorkerNodeId, 
    description: &str,
) -> Result<JobId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{}

// ✅ CORRECT: Simple get operation
pub async fn get_by_id<'c, E>(
    exe: E, 
    job_id: &JobId,
) -> Result<Option<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{}
```

**❌ WRONG Examples:**
```rust
// ❌ WRONG: Not generic over Executor
pub async fn insert(pool: &PgPool, node_id: &WorkerNodeId) -> Result<JobId, sqlx::Error> {}

// ❌ WRONG: Includes resource prefix
pub async fn insert_job<'c, E>(exe: E, node_id: &WorkerNodeId) -> Result<JobId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{}
```

**Function Naming Patterns:**

Resource module functions follow consistent naming patterns that make their purpose immediately clear. All function names omit resource prefixes since they are called via module namespacing (`jobs::insert()` not `insert_job()`).

**Core Operation Patterns:**

- **`insert()`**: Add single new record with required fields
  - Pattern: `insert(exe, required_params...) -> Result<ResourceId, sqlx::Error>`
  - Examples: `jobs::insert(exe, node_id, description)`, `locations::insert(exe, url, dataset_name)`
  - Always returns the newly created resource ID

- **`insert_with_<defaults>()`**: Add record with some fields having default values
  - Pattern: `insert_with_<default_type>(exe, params...) -> Result<ResourceId, sqlx::Error>`
  - Examples: `jobs::insert_with_default_status(exe, node_id)`, `locations::insert_with_inactive_status(exe, params)`
  - Used when some fields have sensible defaults

**Single Record Retrieval Patterns:**

- **`get_by_id()`**: Retrieve single record by primary key
  - Pattern: `get_by_id(exe, id) -> Result<Option<Resource>, sqlx::Error>`
  - Examples: `jobs::get_by_id(exe, job_id)`, `locations::get_by_id(exe, location_id)`
  - Always returns `Option<Resource>` - None if not found

- **`get_by_<field>()`**: Retrieve single record by unique field
  - Pattern: `get_by_<field>(exe, field_value) -> Result<Option<Resource>, sqlx::Error>`
  - Examples: `workers::get_by_node_id(exe, node_id)`, `locations::get_by_url(exe, url)`
  - Used for unique non-primary key lookups

- **`get_<specific>_by_<criteria>()`**: Retrieve specific record matching criteria
  - Pattern: `get_<specific>_by_<criteria>(exe, criteria...) -> Result<Option<Resource>, sqlx::Error>`
  - Examples: `locations::get_active_by_table_id(exe, table_id)`, `jobs::get_running_by_node_id(exe, node_id)`
  - Combines state filtering with field matching

**Multiple Record Retrieval Patterns:**

- **`list_first_page()`**: Get first page of all records (paginated)
  - Pattern: `list_first_page(exe, limit) -> Result<Vec<Resource>, sqlx::Error>`
  - Examples: `jobs::list_first_page(exe, 50)`, `locations::list_first_page(exe, 100)`
  - Always use cursor-based pagination

- **`list_next_page()`**: Get next page using cursor (paginated)
  - Pattern: `list_next_page(exe, limit, cursor) -> Result<Vec<Resource>, sqlx::Error>`
  - Examples: `jobs::list_next_page(exe, 50, last_job_id)`, `locations::list_next_page(exe, 100, last_location_id)`
  - Cursor must be primary key of last item from previous page

- **`list_by_<criteria>()`**: Get filtered records (paginated first page)
  - Pattern: `list_by_<criteria>(exe, criteria_value, limit) -> Result<Vec<Resource>, sqlx::Error>`
  - Examples: `jobs::list_by_status(exe, JobStatus::Running, 50)`, `locations::list_by_dataset(exe, dataset_name, 100)`
  - Always paginated, returns first page

- **`list_by_<criteria>_next_page()`**: Get next page of filtered records
  - Pattern: `list_by_<criteria>_next_page(exe, criteria_value, limit, cursor) -> Result<Vec<Resource>, sqlx::Error>`
  - Examples: `jobs::list_by_status_next_page(exe, JobStatus::Running, 50, cursor)`, `locations::list_by_dataset_next_page(exe, dataset_name, 100, cursor)`
  - Filtered cursor-based pagination

**State Management Patterns:**

- **`mark_<state>()`**: Change record to specific state (simple)
  - Pattern: `mark_<state>(exe, id) -> Result<bool, sqlx::Error>`
  - Examples: `jobs::mark_running(exe, job_id)`, `locations::mark_active(exe, location_id)`
  - Returns `bool` indicating if record was found and updated

- **`mark_<state>_by_<criteria>()`**: Change multiple records to specific state
  - Pattern: `mark_<state>_by_<criteria>(exe, criteria_value) -> Result<u64, sqlx::Error>`
  - Examples: `locations::mark_inactive_by_table_id(exe, table_id)`, `jobs::mark_failed_by_node_id(exe, node_id)`
  - Returns count of records updated

**Field Update Patterns:**

- **`update_<field>()`**: Update single field by ID
  - Pattern: `update_<field>(exe, id, new_value) -> Result<bool, sqlx::Error>`
  - Examples: `workers::update_heartbeat(exe, node_id, timestamp)`, `jobs::update_description(exe, job_id, new_desc)`
  - Returns `bool` indicating if record was found and updated

- **`update_<fields>()`**: Update multiple fields by ID
  - Pattern: `update_<fields>(exe, id, field1, field2, ...) -> Result<bool, sqlx::Error>`
  - Examples: `jobs::update_status_and_progress(exe, job_id, status, progress)`, `locations::update_path_and_metadata(exe, location_id, path, metadata)`
  - Use when updating related fields together

**Deletion Patterns:**

- **`delete_by_id()`**: Delete single record by primary key
  - Pattern: `delete_by_id(exe, id) -> Result<bool, sqlx::Error>`
  - Examples: `jobs::delete_by_id(exe, job_id)`, `locations::delete_by_id(exe, location_id)`
  - Returns `bool` indicating if record was found and deleted

- **`delete_by_<criteria>()`**: Delete multiple records matching criteria
  - Pattern: `delete_by_<criteria>(exe, criteria_value) -> Result<u64, sqlx::Error>`
  - Examples: `jobs::delete_by_status(exe, JobStatus::Completed)`, `locations::delete_by_dataset(exe, dataset_name)`
  - Returns count of records deleted

- **`delete_by_<field1>_and_<field2>()`**: Delete records matching multiple criteria
  - Pattern: `delete_by_<field1>_and_<field2>(exe, value1, value2) -> Result<u64, sqlx::Error>`
  - Examples: `jobs::delete_by_node_id_and_status(exe, node_id, JobStatus::Failed)`, `locations::delete_by_dataset_and_status(exe, dataset_name, false)`
  - Use AND logic for multiple conditions

**Specialized Query Patterns:**

- **`stream_<resource>()`**: Stream large result sets
  - Pattern: `stream_<resource>(exe, criteria...) -> impl Stream<Item = Result<Resource, sqlx::Error>>`
  - Examples: `file_metadata::stream_expired_files(exe, cutoff_date)`, `jobs::stream_by_status(exe, status)`
  - Use for very large datasets that shouldn't be loaded into memory

- **`count_<resource>()`**: Get count without fetching records
  - Pattern: `count_<resource>(exe, criteria...) -> Result<i64, sqlx::Error>`
  - Examples: `jobs::count_by_status(exe, JobStatus::Running)`, `locations::count_active(exe)`
  - Efficient counting queries

- **`exists_<resource>()`**: Check existence without fetching
  - Pattern: `exists_<resource>(exe, criteria...) -> Result<bool, sqlx::Error>`
  - Examples: `jobs::exists_by_id(exe, job_id)`, `workers::exists_by_node_id(exe, node_id)`
  - Boolean existence checks

**Function Naming Modifiers:**

- **`by_id`**: Primary key selection (`get_by_id()`, `delete_by_id()`, `update_status_by_id()`)
- **`by_<field>`**: Single field conditionals (`get_by_node_id()`, `list_by_status()`, `delete_by_dataset()`)
- **`_and_`**: Multiple AND conditions (`get_by_node_id_and_status()`, `delete_by_dataset_and_inactive()`)
- **`_or_`**: Multiple OR conditions (`list_by_status_or_priority()`) - use sparingly
- **`_if_<condition>`**: Conditional operations (`update_status_if_running()`, `delete_if_expired()`)
- **`_with_details`**: Rich queries with JOINs (`get_by_id_with_details()`, `list_with_worker_info()`)
- **`_first_page`**: Initial pagination (`list_first_page()`, `list_by_status_first_page()`)
- **`_next_page`**: Cursor pagination (`list_next_page()`, `list_by_status_next_page()`)

**SQL Query Requirements:**

- **Single-line queries**: Use raw string literals for simple queries that fit within line length limits
- **Multi-line queries**: Use `indoc::indoc!` macro blocks for complex queries requiring multiple lines
- Use consistent indentation and formatting for SQL readability
- Use `sqlx::query_as` for type-safe result mapping with proper binding
- **SECURITY:** Proper parameter binding to prevent SQL injection

**SQL Query Examples:**
```rust
// ✅ CORRECT: Simple single-line query
pub async fn get_count<'c, E>(exe: E) -> Result<i64, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "SELECT COUNT(*) FROM jobs";
    sqlx::query_scalar(query).fetch_one(exe).await
}

// ✅ CORRECT: Multi-line query using `indoc::indoc! {...}` macro 
pub async fn get_by_id<'c, E>(exe: E, job_id: &JobId) -> Result<Option<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT id, node_id, description, status, created_at, updated_at
        FROM jobs 
        WHERE id = $1
    "#};

    sqlx::query_as(query)
        .bind(job_id)
        .fetch_optional(exe)
        .await
}

// ✅ CORRECT: Complex query with joins
pub async fn get_by_id_with_details<'c, E>(
    exe: E, 
    job_id: &JobId,
) -> Result<Option<JobWithDetails>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT j.id, j.node_id, j.description, j.status, 
               j.created_at, j.updated_at,
               w.node_id as worker_node_id, w.last_heartbeat
        FROM jobs j
        LEFT JOIN workers w ON j.node_id = w.node_id
        WHERE j.id = $1
    "#};

    sqlx::query_as(query)
        .bind(job_id)
        .fetch_optional(exe)
        .await
}
```

**❌ WRONG SQL Examples:**
```rust
// ❌ WRONG: Raw string literal for multi-line query (should use indoc!)
pub async fn bad_multiline_query<'c, E>(exe: E, job_id: &JobId) -> Result<Option<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = "SELECT id, node_id, description, status, created_at, updated_at
        FROM jobs j
        LEFT JOIN workers w ON j.node_id = w.node_id
        WHERE j.id = $1";
    sqlx::query_as(query).bind(job_id).fetch_optional(exe).await
}

// ❌ WRONG: Using `indoc!` instead of `indoc::indoc!` fully-qualified form
pub async fn wrong_macro<'c, E>(exe: E, job_id: &JobId) -> Result<Option<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc! {r#"
        SELECT * FROM jobs WHERE id = $1
    "#};
    sqlx::query_as(query).bind(job_id).fetch_optional(exe).await
}
```

**Cursor-Based Pagination Pattern:**

- Implement `list_first_page(limit)` and `list_next_page(limit, cursor)` function pairs
- Use primary key fields as cursor values for consistent ordering
- `list_first_page()` starts from beginning without cursor parameter
- `list_next_page()` continues from given cursor using `WHERE id > $cursor`
- Consistent `ORDER BY` clauses ensure stable pagination across calls
- Limit parameter controls page size for memory management

**Cursor-Based Pagination Examples:**
```rust
// ✅ CORRECT: First page pagination
pub async fn list_first_page<'c, E>(
    exe: E, 
    limit: i32,
) -> Result<Vec<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT id, node_id, description, status, created_at, updated_at
        FROM jobs
        ORDER BY id ASC
        LIMIT $1
    "#};

    sqlx::query_as(query)
        .bind(limit)
        .fetch_all(exe)
        .await
}

// ✅ CORRECT: Next page pagination with cursor
pub async fn list_next_page<'c, E>(
    exe: E,
    limit: i32,
    cursor: &JobId,
) -> Result<Vec<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT id, node_id, description, status, created_at, updated_at
        FROM jobs
        WHERE id > $1
        ORDER BY id ASC
        LIMIT $2
    "#};

    sqlx::query_as(query)
        .bind(cursor)
        .bind(limit)
        .fetch_all(exe)
        .await
}

// ✅ CORRECT: Conditional pagination with cursor
pub async fn list_by_status_next_page<'c, E>(
    exe: E,
    status: &JobStatus,
    limit: i32,
    cursor: &JobId,
) -> Result<Vec<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT id, node_id, description, status, created_at, updated_at
        FROM jobs
        WHERE status = $1 AND id > $2
        ORDER BY id ASC
        LIMIT $3
    "#};

    sqlx::query_as(query)
        .bind(status)
        .bind(cursor)
        .bind(limit)
        .fetch_all(exe)
        .await
}
```

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
