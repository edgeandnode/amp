🔧 `metadata-db` guidelines
=========================

This document serves as both human and AI agent context for understanding and contributing to the `metadata-db` crate.
The goal is safe, coordinated distributed processing with strong consistency guarantees.

## Overview

The `metadata-db` crate is a PostgreSQL-based metadata management system for Amp.
It serves as the central coordination layer for distributed data processing jobs,
location tracking, and worker node management in the Amp ETL pipeline.

## Table of Contents

**Quick Navigation:**
- [Context and Purpose](#context-and-purpose) - Understanding metadata-db's role and responsibilities
- [Parse, Don't Validate Approach](#parse-dont-validate-approach) - Core validation philosophy
- [Contributing Checklist](#contributing-checklist) - Requirements for all contributions
- [Security Requirements](#-security-requirements) - Mandatory security review process
- [Crate Structure](#crate-structure) - High-level organization overview

**Design Patterns (Implementation Guidelines):**
- [Database Schemas and Migration Scripts](#database-schemas-and-migration-scripts-migrations) - SQL migration patterns
- [Public API Design Patterns](#public-api-design-patterns) - Function-based API architecture
- [Resource Management Design Patterns](#resource-management-design-patterns) - Two-layer architecture
- [Invariant-Preserving Transport Types](#invariant-preserving-transport-types) - Type-safe data wrappers
- [DB Connection and Pooling](#db-connection-and-pooling-srcdb) - Connection management

**Key Concepts:**
- Function naming conventions - See [Public API Design Patterns](#public-api-design-patterns)
- Error handling patterns - See [Public API Design Patterns](#public-api-design-patterns)
- Module export pattern - See [Resource Management Design Patterns](#resource-management-design-patterns)
- SQL query requirements - See [Resource Management Design Patterns](#resource-management-design-patterns)

## Context and Purpose

**🎯 PRIMARY PURPOSE: Pure Data Access Layer**

The `metadata-db` crate has ONE responsibility: **provide safe, transactional data access to PostgreSQL-stored metadata**. This crate is strictly a data access layer that offers CRUD operations, queries, and transaction management - nothing more.

**✅ What `metadata-db` IS:**
- A PostgreSQL connection pool manager with automatic migrations
- A function-based API providing type-safe database operations (insert, get, list, update, delete)
- A transaction coordinator ensuring ACID properties with executor-generic functions
- A database schema manager with versioned migrations
- A connection abstraction that handles retries and error mapping
- A provider of invariant-preserving transport types for type-safe data exchange

**❌ What `metadata-db` is NOT:**
- A business logic container (validation, workflow rules, domain constraints belong in consuming crates)
- A coordination service (consuming crates implement coordination using metadata-db data)
- An application layer (application-specific logic belongs in dump, admin-api, server, etc.)
- A decision-making component (consuming crates make decisions based on data retrieved from metadata-db)
- A validation layer (all data in the database is trusted as valid; validation happens at system boundaries)

**🎯 Single Responsibility Principle:**

This crate exists solely to **safely persist and retrieve metadata**. All higher-level concerns - including but not limited to business rules, workflow coordination, validation logic, and application-specific constraints - are the responsibility of consuming crates that use metadata-db as their data persistence layer.


**🏗️ Architectural Context:**

In Amp's distributed architecture, metadata-db serves as the **shared data persistence layer** that consuming crates use to:
- Store and retrieve job states (consuming crates implement job scheduling)
- Track worker registrations (consuming crates implement worker orchestration)
- Record file metadata (consuming crates implement file lifecycle management)
- Manage dataset and manifest metadata (consuming crates implement dataset lifecycle)

This clear separation ensures that business logic remains in appropriate application layers while data persistence concerns are isolated in this single, focused crate.

**Key Design Principles:**

- **Data Access Layer Only**: The metadata-db crate provides ONLY data access operations (CRUD, queries, transactions). All business logic, validation, and domain-specific rules MUST be implemented in consuming crates. This maintains clear architectural boundaries and prevents the metadata layer from becoming a monolithic business logic container.
- **Parse, Don't Validate**: The crate follows the "parse, don't validate" approach. All data in the database is trusted as valid. Validation occurs at system boundaries (in consuming crates) before data enters the database. The metadata-db types provide invariant-preserving wrappers that maintain type safety without re-validating database contents.
- **Atomicity**: All state changes use database transactions to prevent inconsistent states
- **Idempotency**: Operations can be safely retried without side effects
- **Event-driven**: Uses PostgreSQL LISTEN/NOTIFY for real-time coordination
- **Resource isolation**: Clear boundaries between different metadata domains
- **Observability**: Comprehensive tracing and error reporting for debugging distributed issues
- **Executor-generic functions**: Two executor traits enable flexibility:
  - Public API functions use custom `crate::db::Executor<'c>` trait for connection pools and transactions
  - SQL wrapper functions use `sqlx::Executor<'c, Database = Postgres>` for direct database operations
  - This separation allows proper error handling and abstraction at each layer
- **Layered error handling**: SQL wrapper functions return `sqlx::Error`; public API functions convert to `metadata_db::Error`

This crate is foundational to Amp's reliability,
ensuring that distributed blockchain data processing can scale safely
while maintaining the consistency guarantees required for financial and analytical workloads.

## Parse, Don't Validate Approach

The metadata-db crate follows the "parse, don't validate" philosophy: validation happens once at system boundaries, and all database data is trusted as already valid. This approach reduces redundant validation, improves performance, and leverages Rust's type system for correctness.

**Core Principle:**

**Validation happens at boundaries → Database stores valid data → metadata-db trusts database data**

**Where Validation Happens:**

1. **System Boundaries** (Domain Crates like `datasets-common`, `admin-api`):
   - Parse user input strings into validated domain types
   - Use `FromStr`, `TryFrom`, or custom parsing functions
   - Return errors for invalid input
   - Example: `"my-dataset".parse::<datasets_common::Name>()?`

2. **Database Schema** (PostgreSQL):
   - Constraints enforce structural invariants (foreign keys, unique indexes, NOT NULL)
   - Check constraints enforce value invariants where appropriate
   - Migrations maintain schema integrity

**Where Validation Does NOT Happen:**

1. **metadata-db Public API**: No validation of input parameters (trusts caller)
2. **metadata-db SQL Layer**: No validation of database results (trusts database)
3. **Invariant-Preserving Types**: No validation in `_unchecked` constructors (caller responsibility) or in the `sqlx::Encode`/`Decode` implementations.

**Validation vs Error Handling:**
- ❌ **Validation**: Checking if data meets domain rules (e.g., regex checks, format validation, business logic constraints)
- ✅ **Error Handling**: Responding to operation failures (e.g., database connection errors, SQL errors, constraint violations)
- metadata-db does error handling but NOT validation

**Benefits:**

- **Performance**: Avoid redundant validation on trusted data
- **Simplicity**: Database layer focuses solely on data access, not business logic
- **Type Safety**: Rust's type system encodes invariants without runtime checks
- **Clear Boundaries**: Validation responsibility is explicit and localized

**Example Flow:**

```rust
// ========================================
// 1. BOUNDARY: User input validation (in admin-api or similar)
// ========================================

use datasets_common::Name as DomainName;

// User provides string input
let user_input = "my_dataset";

// Parse and validate at boundary
let domain_name: DomainName = user_input.parse()
    .map_err(|e| HttpError::BadRequest(format!("Invalid dataset name: {}", e)))?;

// ========================================
// 2. CONVERSION: Domain → metadata-db (infallible)
// ========================================

// Convert validated domain type to metadata-db type
// This is safe because domain type is already validated
let db_name: metadata_db::DatasetNameOwned = domain_name.into();

// ========================================
// 3. DATABASE: Store in database (no validation)
// ========================================

// metadata-db trusts the input and stores it
metadata_db::datasets::register(&db, db_name, namespace, version).await?;

// ========================================
// 4. RETRIEVAL: Read from database (no validation)
// ========================================

// Database returns data, metadata-db trusts it
let dataset: Option<metadata_db::Dataset> =
    metadata_db::datasets::get_by_name(&db, dataset_name).await?;

// ========================================
// 5. CONVERSION: metadata-db → Domain (may validate)
// ========================================

if let Some(dataset) = dataset {
    // Convert back to domain type if needed
    // This may validate if domain has stricter constraints
    let domain_dataset: DomainDataset = dataset.try_into()?;
}
```

**Data Corruption Handling:**

If database data becomes corrupted (bypassing normal metadata-db operations):

- **DO NOT** add validation to metadata-db functions
- **DO** write migration scripts to repair data
- **DO** add database constraints to prevent future corruption
- **DO** investigate how corruption occurred and fix the root cause

**Invariant Enforcement Layers:**

1. **Type System** (Compile-time):
   - Rust's type system prevents mixing incompatible types
   - Lifetime checking prevents dangling references
   - Invariant-preserving types encode domain constraints

2. **Database Schema** (Database-time):
   - Foreign key constraints
   - Unique constraints
   - Check constraints
   - NOT NULL constraints

3. **Boundary Validation** (Runtime - Domain Layer):
   - FromStr/TryFrom implementations in domain crates
   - HTTP request validation in API handlers
   - CLI argument parsing and validation

4. **NO Validation in metadata-db** (Runtime - Database Layer):
   - Assumes all inputs meet invariants
   - Trusts database contents
   - Focuses on correct data access patterns

**Comparison with Traditional Approach:**

```rust
// ❌ WRONG: Traditional "validate everywhere" approach
pub async fn register<'c, E>(
    exe: E,
    name: impl Into<DatasetName<'_>>,
) -> Result<(), Error> {
    let name = name.into();

    // ❌ Don't do this - validates already-validated data
    if !name.as_str().starts_with(char::is_lowercase) {
        return Err(Error::InvalidDatasetName);
    }
    if name.as_str().is_empty() {
        return Err(Error::EmptyDatasetName);
    }

    sql::insert(exe, name).await?;
    Ok(())
}

// ✅ CORRECT: "Parse, don't validate" approach
pub async fn register<'c, E>(
    exe: E,
    name: impl Into<DatasetName<'_>>,
) -> Result<(), Error> {
    // No validation - trusts the invariant-preserving type
    sql::insert(exe, name.into()).await.map_err(Into::into)
}
```

**Key Takeaways:**

1. **Validate once** at system boundaries (domain layer)
2. **Trust database** contents without re-validation
3. **Use types** to encode and maintain invariants
4. **Fix corruption** with migrations, not validation code
5. **Keep metadata-db simple** - data access only, no business logic

## Contributing Checklist

When contributing to `metadata-db`, ensure you follow these requirements:

**Code Requirements:**

- [ ] Follow all architectural patterns documented in this guide
- [ ] **🚫 NO BUSINESS LOGIC**: The `metadata-db` crate must contain ONLY data access operations. Business logic, validation, domain-specific rules, and application-specific constraints belong in consuming crates (dump, admin-api, etc.)
- [ ] **🚫 NO VALIDATION**: Follow "parse, don't validate" - trust database data and invariant-preserving types without re-validation
- [ ] Implement two-layer architecture:
  - [ ] Public API functions in main module file (e.g., `jobs.rs`) that are generic over `Executor<'c>` and return `Result<T, metadata_db::Error>`
  - [ ] SQL wrapper functions in `sql.rs` submodule (module marked `pub(crate)`, functions marked `pub`) that are generic over `sqlx::Executor` and return `Result<T, sqlx::Error>`
- [ ] Use proper error handling:
  - [ ] Public API functions return `metadata_db::Error`
  - [ ] SQL wrapper functions return `sqlx::Error` or domain-specific errors
  - [ ] Convert errors at public API layer using `.map_err(Into::into)`
- [ ] Follow function naming conventions:
  - [ ] Functions do NOT include resource name (e.g., `register()` not `register_job()`)
  - [ ] Use patterns: `insert()`, `get_by_id()`, `list()`, `mark_<state>()`, `update_<field>()`
- [ ] Use `impl Into<T>` for invariant-preserving type parameters in public API
- [ ] Implement comprehensive rustdoc documentation for all public functions
- [ ] Use `indoc::indoc!` macro for multiline SQL statements in SQL layer
- [ ] Ensure proper parameter binding to prevent SQL injection
- [ ] Add `#[tracing::instrument]` for public API functions

**Invariant-Preserving Types:**

- [ ] Place invariant-preserving types in resource submodules (e.g., `jobs/job_id.rs`)
- [ ] Implement both borrowed (`Type<'a>`) and owned (`TypeOwned`) variants
- [ ] Provide ONLY `*_unchecked` constructors (never `From<String>` or `From<&str>`)
- [ ] Document `/// SAFETY:` when calling `_unchecked` constructors (except in tests)
- [ ] Implement type conversions in DOMAIN crates, not in metadata-db

**Database Changes:**

- [ ] Add timestamped migration files for any schema changes
- [ ] Include a description of changes being made in the migration filename (like `YYYYMMDDHHMMSS_create_<table_name>_table.sql`, `YYYYMMDDHHMMSS_add_<column_name>_to_<table_name>.sql`, or `YYYYMMDDHHMMSS_add_index_<table_name>_<column_names>.sql`)
- [ ] Test migrations both forward and backward (if reversible)
- [ ] Use proper constraint naming conventions
- [ ] Implement cascading deletes where appropriate

**Before Submitting:**

- [ ] **🔐 SECURITY REVIEW**: Complete the security checklist in [security.md](./security.md)
- [ ] Format: `just fmt-file <file>` (or `just fmt-rs` for all Rust files)
- [ ] Check: `just check-crate metadata-db` and `just check-rs`
- [ ] Lint: `just clippy-crate metadata-db` and `just clippy` (fix ALL warnings)
- [ ] Test: `just test-local`
- [ ] Verify no secrets committed
- [ ] Verify proper transaction boundaries
- [ ] Confirm SQL wrapper module is `pub(crate)` with `pub` functions

## 🔐 Security Requirements

**🚨 CRITICAL: Before contributing to `metadata-db`, you MUST review and follow the comprehensive security guidelines.**

Security is fundamental to the metadata-db crate due to its role in managing sensitive blockchain data and coordination metadata. The crate handles:

- **Database connections** with potentially sensitive configuration data
- **SQL query execution** that must be protected against injection attacks
- **Transaction management** requiring proper isolation and consistency
- **Audit logging** for compliance with financial and regulatory standards

**🚨 MANDATORY: Review [security.md](./security.md) before making any changes to this crate.**

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

All contributors MUST complete the security checklist in [security.md](./security.md) as part of their development process.

## Crate Structure

The `metadata-db` crate is organized into these key components:

- **Database schemas and migration scripts (`migrations/`)** - SQL migration files for schema evolution
- **Resource management modules** - Each resource (jobs, workers, manifests, datasets, files) organized with:
  - Main module file (`src/jobs.rs`) - Public API functions and module exports (re-exports from submodules via `pub use`)
  - SQL submodule (`src/jobs/sql.rs`) - Internal SQL wrapper functions (pub(crate))
  - Invariant-preserving types (`src/jobs/job_id.rs`, `src/jobs/job_status.rs`) - Type-safe wrappers
- **Database layer (`src/db/`)** - Connection management, executor trait, transaction support
- **Error types (`src/error.rs`)** - Unified error handling for all operations
- **Library exports (`src/lib.rs`)** - Re-exports of public types and modules

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
- One logical change per migration file (see "One Logical Change" definition below)
- Files must be in `migrations/` directory to be discovered by `sqlx`

**One Logical Change Per Migration:**
- ✅ One table creation WITH its indexes and constraints (atomic unit)
- ✅ Adding multiple related columns that serve one feature
- ✅ Creating a foreign key constraint between existing tables
- ❌ Creating unrelated tables in one migration
- ❌ Mixing table creation with data backfill operations
- ❌ Combining schema changes with unrelated index additions

**File Naming Examples:**
```
✅ CORRECT Examples:
20240315143022_create_jobs_table.sql
20240315143100_add_status_column_to_jobs.sql
20240315143200_create_workers_table.sql
20240315143300_add_unique_constraint_active_manifests.sql
20240315143400_add_foreign_key_jobs_to_workers.sql
20240315143500_create_file_metadata_table.sql
20240315143600_add_index_jobs_status_created_at.sql
20240315143700_create_datasets_table.sql
20240315143800_add_jsonb_column_manifests_metadata.sql

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
ALTER TABLE manifests
ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}';

-- ✅ CORRECT: Creating partial unique index
CREATE UNIQUE INDEX IF NOT EXISTS unique_active_per_dataset_version
    ON manifests(dataset_name, dataset_version)
    WHERE is_active = true;
```

**Schema Design Requirements:**

- Prefer BIGINT for all ID columns (PostgreSQL IDENTITY)
- Use TEXT over VARCHAR for string fields (PostgreSQL optimization)
- Store JSON metadata in JSONB columns for query performance
- Reference foreign keys by semantic names (`node_id`, `job_id`, `manifest_hash`)

**Constraint Patterns:**

- Use partial unique indexes for conditional uniqueness (active manifests, active datasets)
- Name constraints descriptively: `unique_active_per_dataset_version`
- Implement cascading deletes where parent-child relationships exist

### Public API Design Patterns

The public API provides a function-based interface where all operations are exposed as standalone functions within resource modules. This design enables flexible transaction handling and supports both connection pool and transaction contexts.

**API Organization:**

- **Resource-based modules**: Each resource type (jobs, workers, manifests, datasets, files, etc.) has its own module
- **Module export pattern**: Main module file (e.g., `jobs.rs`) contains public API functions and re-exports from submodules via `pub use`
- **Public functions in resource modules**: All public API functions live in the main resource module file
- **SQL isolation**: SQL wrapper functions are isolated in `<resource>/sql.rs` submodules marked `pub(crate)`
- **Function-based design**: No methods on `MetadataDb`; all operations are standalone functions
- **Executor-generic**: Functions accept any type implementing the custom `Executor<'c>` trait

**Module Structure Pattern:**

```
src/
  jobs.rs          # Main module file: public API functions + re-exports
  jobs/
    sql.rs         # SQL wrapper functions (pub(crate), not pub)
    job_id.rs      # JobId invariant-preserving type
    job_status.rs  # JobStatus enum
  workers.rs       # Main module file: public API functions + re-exports
  workers/
    sql.rs         # SQL wrapper functions (pub(crate))
    node_id.rs     # WorkerNodeId invariant-preserving type
  manifests.rs     # Main module file: public API functions + re-exports
  manifests/
    hash.rs        # ManifestHash invariant-preserving type
    path.rs        # ManifestPath invariant-preserving type
    sql.rs         # SQL wrapper functions (pub(crate))
```

**Function Design Requirements:**

1. **Public API functions** (in main module file like `jobs.rs`):
   - Generic over `Executor<'c>` trait (always - see note below)
   - Return `Result<T, metadata_db::Error>`
   - Use `impl Into<T>` for invariant-preserving type parameters
   - Call SQL wrapper functions and convert errors with `.map_err(Into::into)`
   - May include `#[tracing::instrument]` for observability
   - **Function names do NOT include the resource name** (module namespace provides context)

**Executor Generic Pattern:**

✅ **Always generic over `Executor<'c>`** (no exceptions in current design):
```rust
pub async fn get_by_id<'c, E>(exe: E, ...) -> Result<T, Error>
where
    E: Executor<'c>,
```

**Why**: This pattern allows functions to accept both connection pools and transactions, enabling flexible transaction boundaries in consuming code.

2. **SQL wrapper functions** (in `<resource>/sql.rs`):
   - Module marked `pub(crate)`, functions marked `pub`
   - Generic over `sqlx::Executor<'c, Database = Postgres>`
   - Return `Result<T, sqlx::Error>` (never `metadata_db::Error`)
   - Contain all SQL queries using `indoc::indoc!` for multi-line queries
   - NO business logic, only database operations

**Example Function Patterns:**

```rust
// In jobs.rs (Public API)
// Note: Function names don't include "job" - the module namespace provides context

/// Register a job in the queue with the default status (Scheduled)
#[tracing::instrument(skip(exe), err)]
pub async fn register<'c, E>(
    exe: E,
    node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,
    job_desc: &str,
) -> Result<JobId, Error>
where
    E: Executor<'c>,
{
    sql::insert_with_default_status(exe, node_id.into(), job_desc)
        .await
        .map_err(Into::into)
}

/// Get a job by ID
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_id<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<Option<Job>, Error>
where
    E: Executor<'c>,
{
    sql::get_by_id(exe, id.into()).await.map_err(Into::into)
}

/// List jobs with cursor-based pagination support
#[tracing::instrument(skip(exe), err)]
pub async fn list<'c, E>(
    exe: E,
    limit: i64,
    last_job_id: Option<impl Into<JobId> + std::fmt::Debug>,
) -> Result<Vec<Job>, Error>
where
    E: Executor<'c>,
{
    match last_job_id {
        None => sql::list_first_page(exe, limit).await,
        Some(id) => sql::list_next_page(exe, limit, id.into()).await,
    }
    .map_err(Into::into)
}

/// Conditionally marks a job as RUNNING only if it's currently SCHEDULED
#[tracing::instrument(skip(exe), err)]
pub async fn mark_running<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_status_if_any_state(exe, id.into(), &[JobStatus::Scheduled], JobStatus::Running)
        .await
        .map_err(Into::into)
}
```

```rust
// In jobs/sql.rs (SQL wrapper functions - pub(crate) module)

/// Insert a new job into the queue with the default status
pub async fn insert_with_default_status<'c, E>(
    exe: E,
    node_id: WorkerNodeId<'_>,
    descriptor: &str,
) -> Result<JobId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO jobs (node_id, descriptor, status, created_at, updated_at)
        VALUES ($1, $2::jsonb, $3, (timezone('UTC', now())), (timezone('UTC', now())))
        RETURNING id
    "#};
    sqlx::query_scalar(query)
        .bind(&node_id)
        .bind(descriptor)
        .bind(JobStatus::default())
        .fetch_one(exe)
        .await
}

/// Get a job by its ID
pub async fn get_by_id<'c, E>(exe: E, id: JobId) -> Result<Option<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT id, node_id, status, descriptor, created_at, updated_at
        FROM jobs
        WHERE id = $1
    "#};
    sqlx::query_as(query).bind(id).fetch_optional(exe).await
}

/// List the first page of jobs
pub async fn list_first_page<'c, E>(exe: E, limit: i64) -> Result<Vec<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT id, node_id, status, descriptor, created_at, updated_at
        FROM jobs
        ORDER BY id DESC
        LIMIT $1
    "#};
    sqlx::query_as(query).bind(limit).fetch_all(exe).await
}
```

**Function Naming Conventions:**

The public API follows consistent naming patterns that make function purposes immediately clear. All function names use `snake_case` and follow semantic patterns based on their operation type.

**IMPORTANT: Function names do NOT include the resource name.** The module namespace (e.g., `jobs::`, `workers::`, `manifests::`) provides the resource context, so function names should be concise.

**Function Naming Terminology:**
- **Resource name** = The database table/module name (jobs, workers, manifests, datasets, files)
- **Operation** = The action being performed (get, insert, list, mark, update, delete)
- **Field name** = The specific field for filtered operations (by_id, by_status, by_node_id)

**Correct patterns:**
- ✅ `jobs::get_by_id()` - "job" is in module, not function name
- ✅ `workers::register()` - "worker" is in module, not function name
- ✅ `manifests::list()` - "manifest" is in module, not function name

**Wrong patterns:**
- ❌ `jobs::get_job_by_id()` - redundant "job" in function name
- ❌ `workers::register_worker()` - redundant "worker" in function name

**Core CRUD Operation Patterns:**
- **`get_by_id()`**: Retrieve single resource by ID (not `get_job_by_id()`)
  - Examples: `jobs::get_by_id()`, `workers::get_by_node_id()`, `manifests::get_by_hash()`
  - Always takes resource ID as parameter, returns `Option<Resource>`

- **`list()`**: Retrieve multiple resources (paginated) (not `list_jobs()`)
  - Examples: `jobs::list()`, `workers::list()`, `manifests::list()`
  - Returns paginated results using cursor-based pagination

- **`register()`**: Create/insert new resource (not `register_job()`)
  - Examples: `jobs::register()`, `workers::register()`, `manifests::register()`
  - Takes creation parameters, returns resource ID or `()`

- **`delete_by_id()`**: Remove single resource by ID (not `delete_job()`)
  - Examples: `jobs::delete_if_terminal()`, `workers::delete_by_node_id()`
  - Returns boolean indicating if resource was found and deleted

**State Management Patterns:**

- **`mark_<state>()`**: Change resource to specific state (not `mark_job_running()`)
  - Examples: `jobs::mark_running()`, `jobs::mark_completed()`, `jobs::mark_stopped()`
  - Atomic state transitions with validation

- **`request_<action>()`**: Request a state change (not `request_job_stop()`)
  - Examples: `jobs::request_stop()`
  - Initiates workflow that may involve multiple state transitions

**Specialized Query Patterns:**

- **`get_<specific>()`**: Get resources with specific criteria (not `get_scheduled_jobs()`)
  - Examples: `jobs::get_scheduled()`, `jobs::get_active()`, `jobs::get_by_dataset()`
  - Filtered queries with domain-specific semantics

- **`delete_all_by_<criteria>()`**: Delete multiple resources (not `delete_all_jobs_by_status()`)
  - Examples: `jobs::delete_all_by_status()`
  - Returns count of deleted resources

**✅ Correct Function Naming Examples:**

```rust
// Called as: jobs::register(), jobs::get_by_id(), jobs::list()
pub async fn register<'c, E>(...) -> Result<JobId, Error> {}
pub async fn get_by_id<'c, E>(...) -> Result<Option<Job>, Error> {}
pub async fn list<'c, E>(...) -> Result<Vec<Job>, Error> {}
pub async fn mark_running<'c, E>(...) -> Result<(), Error> {}
pub async fn mark_completed<'c, E>(...) -> Result<(), Error> {}
pub async fn delete_if_terminal<'c, E>(...) -> Result<bool, Error> {}

// Called as: workers::register(), workers::get_by_node_id()
pub async fn register<'c, E>(...) -> Result<(), Error> {}
pub async fn get_by_node_id<'c, E>(...) -> Result<Option<Worker>, Error> {}

// Called as: manifests::register(), manifests::get_by_hash()
pub async fn register<'c, E>(...) -> Result<(), Error> {}
pub async fn get_by_hash<'c, E>(...) -> Result<Option<Manifest>, Error> {}
```

**❌ Wrong Function Naming Examples:**

```rust
// ❌ WRONG: Including resource name in function name creates redundancy
pub async fn register_job<'c, E>(...) -> Result<JobId, Error> {}        // Use register()
pub async fn get_job_by_id<'c, E>(...) -> Result<Option<Job>, Error> {} // Use get_by_id()
pub async fn list_jobs<'c, E>(...) -> Result<Vec<Job>, Error> {}        // Use list()
pub async fn mark_job_running<'c, E>(...) -> Result<(), Error> {}       // Use mark_running()
pub async fn delete_job<'c, E>(...) -> Result<bool, Error> {}           // Use delete_by_id()
```

**Error Handling:**

- Public API functions return `Result<T, metadata_db::Error>`
- SQL wrapper functions return `Result<T, sqlx::Error>` OR `Result<T, DomainError>` for complex operations
- Error conversion happens at the public API layer: `.map_err(Into::into)`
- Domain-specific errors (e.g., `JobStatusUpdateError`) are wrapped in `metadata_db::Error`

**Error Type Guidelines:**

| Layer | Return Type | Example | Use Case |
|-------|-------------|---------|----------|
| Public API | `Result<T, metadata_db::Error>` | Always | All public functions |
| SQL wrapper (simple) | `Result<T, sqlx::Error>` | Basic CRUD | Simple insert, get, list, delete |
| SQL wrapper (complex) | `Result<T, DomainError>` | State machines | Conditional updates with multiple failure modes |
| Error conversion | Public API wraps domain errors | `.map_err(Into::into)` or `map_err(Error::from)` | Converting at API boundary |

**When to create domain-specific errors:**
- Complex state transitions with multiple failure modes (e.g., job status updates)
- Operations requiring detailed error context beyond "SQL failed"
- Example: `JobStatusUpdateError` distinguishes NotFound vs StateConflict vs Database errors

**Documentation Standards:**

- Every public function must have comprehensive rustdoc documentation
- Include purpose, parameters, return values, and error conditions
- Document transaction boundaries and state change implications
- Use `#[tracing::instrument]` for complex operations

### Resource Management Design Patterns

Resource management modules implement a two-layer architecture: a public API layer providing executor-generic functions with domain error types, and an internal SQL layer isolating database operations.

**Two-Layer Architecture:**

1. **Public API Layer** (in main module file like `jobs.rs`):
   - Contains public functions that application code calls
   - Generic over custom `Executor<'c>` trait
   - Returns `Result<T, metadata_db::Error>`
   - Uses `impl Into<T>` for ergonomic type conversions
   - Calls SQL wrapper functions and converts errors

2. **SQL Wrapper Layer** (in `<resource>/sql.rs` submodule):
   - Module marked `pub(crate)`, functions marked `pub`
   - Generic over `sqlx::Executor<'c, Database = Postgres>`
   - Returns `Result<T, sqlx::Error>`
   - Contains all SQL query strings
   - No business logic, only database operations

**Module Structure:**

- One module per metadata resource (`jobs`, `workers`, `manifests`, `datasets`, `files`, etc.)
- Module names directly correspond to primary database table names
- Main module file (e.g., `jobs.rs`) contains public API and module exports
- SQL submodule (e.g., `jobs/sql.rs`) contains SQL wrapper functions
- Invariant-preserving types in separate submodule files (e.g., `jobs/job_id.rs`)
- Clear boundaries prevent cross-resource dependencies

**Module Organization Patterns:**

- **Simple resources**: Main file + sql submodule (`src/workers.rs` + `src/workers/sql.rs`)
- **Complex resources**: Main file + multiple submodules (`src/jobs.rs` + `src/jobs/sql.rs` + `src/jobs/job_id.rs` + `src/jobs/job_status.rs`)
- Main module file serves as module export of all public types and functions
- Internal SQL operations isolated in `sql.rs` marked `pub(crate)`

**Module Export Pattern Explained:**

A "module export" re-exports types from submodules for convenient access.

**Example structure:**
```rust
// In jobs.rs (main module file)
mod sql;  // Private - submodule not exported
pub mod job_id;  // Public - submodule exported for direct access
pub mod job_status;  // Public - submodule exported

// Module exports - convenient access without submodule path
pub use job_id::{JobId, JobIdOwned};
pub use job_status::JobStatus;

// Public API functions
pub async fn register(...) -> Result<JobId, Error> { ... }
```

**Usage:**
```rust
use metadata_db::jobs::{JobId, JobStatus, register};  // ✅ Direct access
// vs
use metadata_db::jobs::job_id::JobId;  // ❌ Verbose, not needed
```

**Function Design Requirements:**

**Public API Functions (in main module file):**
- Generic over custom `Executor<'c>` trait
- Return `Result<T, metadata_db::Error>`
- Use `impl Into<T>` for invariant-preserving type parameters
- Function names omit resource prefix (module provides context)
- Call SQL wrapper functions and convert errors with `.map_err(Into::into)`
- May include `#[tracing::instrument]` for observability

**SQL Wrapper Functions (in `sql.rs` submodule):**
- Module visibility: `pub(crate) mod sql;`
- Function visibility: `pub`
- Generic over `sqlx::Executor<'c, Database = Postgres>`
- Return `Result<T, sqlx::Error>` (never `metadata_db::Error`)
- Function names omit resource prefix (same naming as public API)
- Contain all SQL query strings using `indoc::indoc!` for multi-line queries
- No business logic, validation, or error conversion

**Module Visibility Pattern Explained:**
- **Module**: `pub(crate) mod sql;` - visible only within metadata-db crate
- **Functions**: `pub` - visible to any code that can access the module
- **Effect**: SQL functions are public within the crate but private to external crates
- **Rationale**: Public API layer (same crate) can call them, but external crates cannot directly access SQL layer

**Example Two-Layer Pattern:**

```rust
// ========================================
// PUBLIC API LAYER (in jobs.rs)
// ========================================

use crate::{db::Executor, error::Error};

/// Register a job in the queue with the default status (Scheduled)
///
/// This is a public API function that application code calls.
#[tracing::instrument(skip(exe), err)]
pub async fn register<'c, E>(
    exe: E,
    node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,
    job_desc: &str,
) -> Result<JobId, Error>
where
    E: Executor<'c>,
{
    // Call SQL wrapper and convert error
    sql::insert_with_default_status(exe, node_id.into(), job_desc)
        .await
        .map_err(Into::into)
}

/// Get a job by ID
#[tracing::instrument(skip(exe), err)]
pub async fn get_by_id<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<Option<Job>, Error>
where
    E: Executor<'c>,
{
    sql::get_by_id(exe, id.into()).await.map_err(Into::into)
}

/// Conditionally marks a job as RUNNING only if currently SCHEDULED
#[tracing::instrument(skip(exe), err)]
pub async fn mark_running<'c, E>(
    exe: E,
    id: impl Into<JobId> + std::fmt::Debug,
) -> Result<(), Error>
where
    E: Executor<'c>,
{
    sql::update_status_if_any_state(
        exe,
        id.into(),
        &[JobStatus::Scheduled],
        JobStatus::Running
    )
    .await
    .map_err(Into::into)
}
```

```rust
// ========================================
// SQL WRAPPER LAYER (in jobs/sql.rs)
// ========================================

use sqlx::{Executor, Postgres};

/// Insert a new job with the default status
///
/// This is an internal SQL wrapper function.
pub async fn insert_with_default_status<'c, E>(
    exe: E,
    node_id: WorkerNodeId<'_>,
    descriptor: &str,
) -> Result<JobId, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        INSERT INTO jobs (node_id, descriptor, status, created_at, updated_at)
        VALUES ($1, $2::jsonb, $3, (timezone('UTC', now())), (timezone('UTC', now())))
        RETURNING id
    "#};

    sqlx::query_scalar(query)
        .bind(&node_id)
        .bind(descriptor)
        .bind(JobStatus::default())
        .fetch_one(exe)
        .await
}

/// Get a job by its ID
pub async fn get_by_id<'c, E>(exe: E, id: JobId) -> Result<Option<Job>, sqlx::Error>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        SELECT id, node_id, status, descriptor, created_at, updated_at
        FROM jobs
        WHERE id = $1
    "#};

    sqlx::query_as(query).bind(id).fetch_optional(exe).await
}

/// Update job status if it matches any of the expected statuses
pub async fn update_status_if_any_state<'c, E>(
    exe: E,
    id: JobId,
    expected_statuses: &[JobStatus],
    new_status: JobStatus,
) -> Result<(), JobStatusUpdateError>
where
    E: Executor<'c, Database = Postgres>,
{
    let query = indoc::indoc! {r#"
        WITH target_job AS (
            SELECT id, status FROM jobs WHERE id = $1
        ),
        target_job_update AS (
            UPDATE jobs
            SET status = $3, updated_at = timezone('UTC', now())
            WHERE id = $1 AND status = ANY($2)
            RETURNING id
        )
        SELECT
            target_job_update.id AS updated_id,
            target_job.status AS original_status
        FROM target_job
        LEFT JOIN target_job_update ON target_job.id = target_job_update.id
    "#};

    let result: Option<UpdateResult> = sqlx::query_as(query)
        .bind(id)
        .bind(expected_statuses)
        .bind(new_status)
        .fetch_optional(exe)
        .await
        .map_err(JobStatusUpdateError::Database)?;

    match result {
        Some(UpdateResult { updated_id: Some(_), .. }) => Ok(()),
        Some(UpdateResult { updated_id: None, original_status: Some(status) }) => {
            Err(JobStatusUpdateError::StateConflict {
                expected: expected_statuses.to_vec(),
                actual: status,
            })
        }
        _ => Err(JobStatusUpdateError::NotFound),
    }
}
```

**Function Naming Patterns:**

Both layers follow the same naming patterns - the key difference is in their signatures and return types, not their names. Function names omit resource prefixes since they are called via module namespacing (`jobs::insert()` not `insert_job()`).

**Core Operation Patterns (Both Layers):**

- **`insert()`** / **`insert_with_<defaults>()`**: Add new record
  - Public API: `Result<ResourceId, metadata_db::Error>`
  - SQL layer: `Result<ResourceId, sqlx::Error>`
  - Examples: `insert()`, `insert_with_default_status()`

- **`get_by_id()`** / **`get_by_<field>()`**: Retrieve single record
  - Public API: `Result<Option<Resource>, metadata_db::Error>`
  - SQL layer: `Result<Option<Resource>, sqlx::Error>`
  - Examples: `get_by_id()`, `get_by_node_id()`, `get_by_hash()`

- **`list_first_page()`** / **`list_next_page()`**: Paginated retrieval
  - Public API: `Result<Vec<Resource>, metadata_db::Error>`
  - SQL layer: `Result<Vec<Resource>, sqlx::Error>`
  - Examples: `list_first_page()`, `list_next_page()`

**State Management Patterns (Both Layers):**

- **`mark_<state>()`**: Change record to specific state
  - Public API: `Result<(), metadata_db::Error>` or `Result<bool, metadata_db::Error>`
  - SQL layer: `Result<bool, sqlx::Error>` or domain-specific error
  - Examples: `mark_running()`, `mark_completed()`, `mark_active()`

- **`update_<field>()`** / **`update_<fields>()`**: Field updates
  - Public API: `Result<bool, metadata_db::Error>`
  - SQL layer: `Result<bool, sqlx::Error>`
  - Examples: `update_heartbeat()`, `update_status_and_progress()`

**Deletion Patterns (Both Layers):**

- **`delete_by_id()`** / **`delete_by_<criteria>()`**: Remove records
  - Public API: `Result<bool, metadata_db::Error>` or `Result<usize, metadata_db::Error>`
  - SQL layer: `Result<bool, sqlx::Error>` or `Result<usize, sqlx::Error>`
  - Examples: `delete_by_id()`, `delete_by_status()`

**Function Naming Modifiers:**

- **`by_id`**: Primary key selection (`get_by_id()`, `delete_by_id()`)
- **`by_<field>`**: Single field conditionals (`get_by_node_id()`, `list_by_status()`)
- **`_and_`**: Multiple AND conditions (`get_by_node_id_and_status()`)
- **`_if_<condition>`**: Conditional operations (`update_status_if_any_state()`)
- **`_with_<details>`**: Variants with additional options (`insert_with_default_status()`)
- **`_first_page`**: Initial pagination (`list_first_page()`)
- **`_next_page`**: Cursor pagination (`list_next_page()`)

**SQL Query Requirements (SQL Layer Only):**

- **Single-line queries**: Use raw string literals for simple queries ≤100 characters
- **Multi-line queries**: Use `indoc::indoc!` macro blocks for queries >100 characters or complex queries requiring multiple lines
- Use consistent indentation and formatting for SQL readability
- Use `sqlx::query_as` for type-safe result mapping with proper binding
- **SECURITY:** Proper parameter binding to prevent SQL injection

**Cursor-Based Pagination Pattern (SQL Layer):**

- Implement `list_first_page(limit)` and `list_next_page(limit, cursor)` function pairs
- Use primary key fields as cursor values for consistent ordering
- `list_first_page()` starts from beginning without cursor parameter
- `list_next_page()` continues from given cursor using `WHERE id > $cursor`
- Consistent `ORDER BY` clauses ensure stable pagination across calls
- Limit parameter controls page size for memory management

**Type Definitions:**

- Resource structs defined within their respective modules
- Resource structs must NOT implement serialization/deserialization logic
  Exception: ID _new-type_ wrappers (`JobId`, `FileId`) may implement serde traits
- Domain-specific enums (`JobStatus`) with database representation
- Export types through module `pub use` for consumption by application code

**Documentation Requirements:**

- **Public API functions**: Comprehensive rustdoc with purpose, parameters, return values, errors, and transaction boundaries
- **SQL wrapper functions**: Brief rustdoc describing the database operation
- Add `#[tracing::instrument]` to public API functions for observability
- Use consistent documentation style across all resource modules

### Invariant-Preserving Transport Types

Invariant-preserving transport types are specialized wrapper types around `Cow<'a, str>` or primitive types that maintain type-level guarantees about the data they contain. These types serve as the communication interface between the metadata-db crate and consuming application code.

**Purpose:**

- Provide type-safe data exchange between metadata-db and domain layers
- Encode invariants at the type level (e.g., dataset names, manifest hashes, worker node IDs)
- Trust database contents without re-validation ("parse, don't validate" approach)
- Enable ergonomic APIs through `impl Into<T>` parameters

**Location:**

- Invariant-preserving types live in resource submodules (e.g., `datasets/name.rs`, `manifests/hash.rs`, `workers/node_id.rs`)
- Each type has both borrowed (`Type<'a>`) and owned (`TypeOwned = Type<'static>`) variants
- Types use `Cow<'a, str>` internally for efficient borrowed/owned semantics

**Core Pattern:**

```rust
// In datasets/name.rs

use std::borrow::Cow;

/// An owned dataset name type for database return values
pub type NameOwned = Name<'static>;

/// A dataset name wrapper for database values
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Name<'a>(Cow<'a, str>);

impl<'a> Name<'a> {
    /// Create a new Name wrapper from a reference to str (borrowed)
    ///
    /// # Safety
    /// The caller must ensure the provided name upholds the dataset name invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_ref_unchecked(name: &'a str) -> Self {
        Self(Cow::Borrowed(name))
    }

    /// Create a new Name wrapper from an owned String
    ///
    /// # Safety
    /// The caller must ensure the provided name upholds the dataset name invariants.
    /// This method does not perform validation. Failure to uphold the invariants may
    /// cause undefined behavior.
    pub fn from_owned_unchecked(name: String) -> Name<'static> {
        Name(Cow::Owned(name))
    }

    /// Get a reference to the inner str
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume and return the inner String (owned)
    pub fn into_inner(self) -> String {
        match self {
            Name(Cow::Owned(name)) => name,
            Name(Cow::Borrowed(name)) => name.to_owned(),
        }
    }
}
```

**🚨 CRITICAL: Safety Requirements**

**The invariant-preserving transport types in metadata-db MUST NEVER implement `From` traits that allow direct construction from primitives:**

```rust
// ❌ FORBIDDEN: These implementations MUST NOT exist in metadata-db types
impl From<String> for DatasetName<'static> { ... }  // ❌ NEVER
impl From<&str> for DatasetName<'_> { ... }          // ❌ NEVER
impl<'a> From<&'a str> for DatasetName<'a> { ... }  // ❌ NEVER
```

**Why?** These implementations would bypass the `_unchecked` constructors and their required `SAFETY` documentation, allowing unsafe construction without explicit safety acknowledgment.

**✅ ONLY the `*_unchecked` constructors may create these types.**

Every use of `_unchecked` constructors MUST be accompanied by a `/// SAFETY:` comment explaining why the invariants are upheld (except in test code).

**Type Conversion Pattern:**

Conversions between domain types and metadata-db types follow a specific pattern:

1. **Domain → metadata-db**: Always infallible (`impl From`)
   - Domain types have already validated the data
   - Conversion is safe by construction
   - Implemented in the DOMAIN crate (e.g., `datasets-common`), not in `metadata-db`

2. **metadata-db → Domain**: May be fallible (`impl TryFrom`) or infallible (`impl From`)
   - Database data is trusted as valid
   - Some domain types may have additional constraints requiring `TryFrom`
   - Implemented in the DOMAIN crate (e.g., `datasets-common`), not in `metadata-db`

**Conversion Implementation Location:**

**🚨 CRITICAL: Conversion traits (`From`, `TryFrom`) are implemented in the DOMAIN TYPE module, NEVER in metadata-db.**

```rust
// ========================================
// FILE LOCATION: datasets-common/src/name.rs (DOMAIN CRATE, NOT metadata-db)
// ========================================

/// Domain type with validation
pub struct Name(String);

impl std::str::FromStr for Name {
    type Err = NameValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Validate invariants
        validate_dataset_name(s)?;
        Ok(Name(s.to_owned()))
    }
}

#[cfg(feature = "metadata-db")]
impl From<Name> for metadata_db::DatasetNameOwned {
    fn from(value: Name) -> Self {
        // SAFETY: Name is validated at construction via FromStr, ensuring invariants are upheld.
        metadata_db::DatasetName::from_owned_unchecked(value.0)
    }
}

#[cfg(feature = "metadata-db")]
impl<'a> From<&'a Name> for metadata_db::DatasetName<'a> {
    fn from(value: &'a Name) -> Self {
        // SAFETY: Name is validated at construction via FromStr, ensuring invariants are upheld.
        metadata_db::DatasetName::from_ref_unchecked(&value.0)
    }
}

#[cfg(feature = "metadata-db")]
impl TryFrom<metadata_db::DatasetNameOwned> for Name {
    type Error = NameValidationError;

    fn try_from(value: metadata_db::DatasetNameOwned) -> Result<Self, Self::Error> {
        // Database data is trusted, but validate for domain-specific constraints if needed
        let s = value.into_inner();
        s.parse()
    }
}
```

**API Ergonomics with `impl Into<T>`:**

Public API functions use `impl Into<T>` parameters to accept any type that can be converted into the invariant-preserving type:

```rust
// In jobs.rs (Public API)

pub async fn register<'c, E>(
    exe: E,
    node_id: impl Into<WorkerNodeId<'_>> + std::fmt::Debug,  // ← Accepts &WorkerNodeId, WorkerNodeId, domain types
    job_desc: &str,
) -> Result<JobId, Error>
where
    E: Executor<'c>,
{
    sql::insert_with_default_status(exe, node_id.into(), job_desc)
        .await
        .map_err(Into::into)
}
```

**Parameter Pattern Explanation:**
- `impl Into<T>`: Ergonomic conversion from domain types or direct metadata-db types
- `+ std::fmt::Debug`: **REQUIRED when using `#[tracing::instrument]`** for parameter logging in trace spans
- Omit `Debug` bound only if function is not instrumented with tracing

This allows callers to pass:
- The metadata-db type directly: `jobs::register(&db, worker_node_id, desc).await?`
- A domain type that implements `Into`: `jobs::register(&db, &domain_worker, desc).await?`

**Required Trait Implementations:**

All invariant-preserving transport types must implement:

- `Clone`, `PartialEq`, `Eq`, `PartialOrd`, `Ord`, `Hash` for general usage
- `Deref<Target = str>` and `AsRef<str>` for string-like types
- `Display` and `Debug` for diagnostics
- `sqlx::Type<sqlx::Postgres>` for PostgreSQL type mapping
- `sqlx::Encode<'_, sqlx::Postgres>` for encoding values
- `sqlx::Decode<'r, sqlx::Postgres>` for decoding values (owned variant only)

**Database Decode Implementation:**

```rust
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for NameOwned {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        // SAFETY: Database values are trusted to uphold invariants; validation occurs at boundaries before insertion.
        Ok(Name::from_owned_unchecked(s))
    }
}
```

**Examples from the Codebase:**

- `datasets/name.rs` - DatasetName with `from_ref_unchecked()` and `from_owned_unchecked()`
- `datasets/namespace.rs` - DatasetNamespace wrapper
- `datasets/version.rs` - DatasetVersion wrapper
- `manifests/hash.rs` - ManifestHash wrapper
- `manifests/path.rs` - ManifestPath wrapper
- `workers/node_id.rs` - WorkerNodeId with lifetime parameters
- `jobs/job_id.rs` - JobId using `#[repr(transparent)]` over i64
- `files/file_id.rs` - FileId wrapper

**Key Takeaways:**

1. **Only `_unchecked` constructors** can create invariant-preserving types
2. **Never implement `From<String>` or `From<&str>`** on these types
3. **Conversions live in domain crates**, not in metadata-db
4. **Always document `SAFETY`** when calling `_unchecked` constructors (except tests)
5. **Database data is trusted** - no re-validation needed
6. **Use `impl Into<T>`** in public API for ergonomic calls

### DB connection and pooling (`src/db/`)

The `db` module manages database connectivity, connection pooling, and the executor abstraction. It provides:
- Connection pool management with automatic migrations
- Connection retry logic
- Custom `Executor<'c>` trait for generic connection handling
- Transaction support for atomic operations
