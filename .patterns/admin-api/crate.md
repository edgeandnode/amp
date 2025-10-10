🔧 `admin-api` guidelines
=========================

This document serves as both human and AI agent context for understanding and contributing to the `admin-api` crate.
The goal is a focused administrative HTTP API for Project Amp management operations.

## Context and Purpose

**🎯 PRIMARY PURPOSE: Administrative HTTP API Layer**

The `admin-api` crate has ONE responsibility: **provide HTTP REST API endpoints for administrative operations in Project Amp**. This crate is strictly an HTTP presentation layer that exposes administrative functionality through REST endpoints - nothing more.

**✅ What `admin-api` IS:**
- An HTTP server providing REST API endpoints for admin operations
- A request/response handler system with proper error mapping
- An API presentation layer that formats data for HTTP consumption
- A validation layer for HTTP request parameters and bodies
- An authentication/authorization enforcement point for admin operations

**❌ What `admin-api` is NOT:**
- A business logic container (domain logic belongs in consuming crates like dataset-store, metadata-db)
- A data storage system (data persistence is handled by dataset-store, metadata-db, etc.)
- A computation engine (data processing belongs in amp, worker crates)
- A general-purpose web service (only administrative operations, not user-facing features)

**🎯 Single Responsibility Principle:**

This crate exists solely to **expose administrative operations via HTTP REST API**. All domain logic, business rules, data persistence, and computation are the responsibility of other crates that admin-api delegates to through well-defined interfaces.

**🏗️ Architectural Context:**

In Amp's distributed architecture, admin-api serves as the **HTTP presentation layer** for administrative operations:
- Exposes dataset management endpoints (delegates to dataset-store)
- Provides system monitoring endpoints (delegates to metadata-db)
- Offers worker management endpoints (delegates to metadata-db)
- Serves configuration endpoints (delegates to configuration layers)

This clear separation ensures that HTTP concerns remain isolated while business logic resides in appropriate domain crates.

## HTTP Framework

**🚀 Axum Web Framework**

The `admin-api` crate is built on the [Axum](https://github.com/tokio-rs/axum) web framework, which provides:

- **Type-safe extractors** for request components (path parameters, query strings, JSON bodies)
- **Async-first design** built on Tokio for high-performance concurrent request handling
- **Modular middleware system** for cross-cutting concerns like logging and error handling
- **Composable routing** with compile-time route validation
- **Built-in JSON support** with automatic serialization/deserialization via serde

**Key Axum Concepts:**
- **Handlers**: Async functions that process HTTP requests and return responses
- **Extractors**: Type-safe way to extract data from requests (`Path`, `Query`, `Json`, `State`)
- **Responses**: Type-safe response builders and automatic JSON serialization
- **State**: Shared application state accessible across all handlers
- **Middleware**: Composable request/response processing layers

All handlers in this crate MUST follow Axum's patterns and leverage its type-safe extractors for request processing.

## Implementation Guidelines

This guide documents the **MANDATORY** patterns and conventions for implementing HTTP handlers in the `admin-api` crate.
🚨 **EVERY HANDLER MUST FOLLOW THESE PATTERNS** - they ensure consistency, maintainability, and proper error handling
across all API endpoints.

## 📁 Crate Structure

The `admin-api` crate follows a modular handler organization pattern:

```
`admin-api`/src/
├── handlers/
│   ├── <resource>/
│   │   ├── delete.rs          # DELETE /<resource> (bulk operations)
│   │   ├── delete_by_id.rs    # DELETE /<resource>/{id}
│   │   ├── get_all.rs         # GET /<resource> (collection list)
│   │   ├── get_by_id.rs       # GET /<resource>/{id}
│   │   ├── <action>.rs        # Resource-specific action handlers (e.g., POST /<resource>/{id}/<action>)
│   │   └── <resource>_info.rs # Shared response types
│   └── <resource>.rs          # Module declaration
├── handlers.rs                # Handler modules registration
├── ctx.rs                     # Shared context
└── lib.rs                     # Route registration
```

## 🛠️ Handler Implementation Patterns

### 1. 📄 File Structure Pattern

🚨 **MANDATORY**: Each handler module MUST follow this exact organization:

- **Module file** (`<resource>.rs`): Contains only module declarations
- **Operation files** (`get_all.rs`, `get_by_id.rs`, `delete.rs`, `delete_by_id.rs`): Individual handler implementations
- **Action files** (`<action>.rs`): Resource-specific endpoint handlers (e.g., `stop.rs`, `deploy.rs`, `dump.rs`)
- **Info types file** (`<resource>_info.rs`): Shared response data structures

### 2. ✍️ Handler Function Signature

#### ⚠️ Standard Handler Pattern (Fallible)

🚨 **MANDATORY**: Most handlers MUST follow this signature pattern for operations that can fail:

```rust
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    // Additional extractors (Path, Query, etc.)
) -> Result<ResponseType, BoxRequestError> {
    // Implementation
}
```

#### ✅ Infallible Handler Pattern

🚨 **MANDATORY**: For operations that cannot fail (e.g., accessing cached data), you MUST use direct return types:

```rust
#[tracing::instrument(skip_all)]
pub async fn handler(
    State(ctx): State<Ctx>,  // State extractor MUST be the first parameter
    // Additional extractors (Path, Query, etc.)
) -> ResponseType {
    // Implementation
}
```

🚨 **NON-NEGOTIABLE REQUIREMENTS:**

- `#[tracing::instrument(skip_all, err)]` for fallible handlers or `#[tracing::instrument(skip_all)]` for infallible
  ones
- `State(ctx): State<Ctx>` as first parameter
- Return type `Result<ResponseType, BoxRequestError>` for fallible or `ResponseType` for infallible operations
- `pub async fn` signature

✅ **Use infallible pattern ONLY when:**

- Accessing cached/preloaded data that cannot fail
- Operations where all errors are handled at a lower level and logged
- Data transformations that are guaranteed to succeed

### 3. 🔍 Parameter Extraction Patterns

#### 🛣️ Axum Routing Behavior

🚨 **CRITICAL**: When implementing handlers with path parameters, you MUST understand these Axum routing behaviors:

- **Empty path segments**: Routes like `/resource/{id}` will return 404 at the routing level for `/resource/` (trailing
  slash with empty parameter)
- **No route conflicts**: `/resource` and `/resource/{id}` do not conflict - Axum handles routing disambiguation
  automatically
- **Path validation**: Axum's Path extractor only passes non-empty, properly decoded strings to handlers

📌 **This means:**

- The `InvalidId`/`InvalidName` error variants in handlers are primarily for malformed but non-empty parameters
- Empty parameters never reach the handler code
- No special handling is needed for empty path segments

#### 🛤️ Path Parameters
🚨 **MANDATORY**: Handle path extraction with validation:

```rust
// Path parameter extraction pattern
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<IdType>, PathRejection>,
) -> Result<Json<ResponseType>, BoxRequestError> {
    let id = match path {
        Ok(Path(path)) => path,
        Err(err) => {
            tracing::debug!(error=?err, "invalid ID in path");
            return Err(Error::InvalidId { err }.into());
        }
    };
    
    // Handler implementation continues...
}
```

#### ❓ Query Parameters

```rust
// Query parameter type example
fn handler(query: Result<Query<QueryParamsStruct>, QueryRejection>) {}
```

🚨 **MANDATORY**: Define query parameter structs with validation using the exact name `QueryParams`:

```rust
// Query parameter structure with defaults
#[derive(Debug, serde::Deserialize)]
pub struct QueryParams {
    #[serde(default = "default_limit")]
    limit: usize,
    // Additional parameters
}

fn default_limit() -> usize {
    50
}

// Usage in handler
pub async fn handler(query: Result<Query<QueryParams>, QueryRejection>) -> Result<(), Error> {
    let params = match query {
        Ok(Query(params)) => params,
        Err(err) => return Err(Error::InvalidQueryParams { err }),
    };
    
    // Use params.limit, etc.
}
```

### 4. 📤 Response Type Patterns

#### ✅ Success Responses

- **GET single resource**: `Json<ResourceInfo>` or `Result<Json<ResourceInfo>, BoxRequestError>`
- **GET collection**: `Json<ResourcesResponse>` or `Result<Json<ResourcesResponse>, BoxRequestError>`
- **DELETE operations**: `StatusCode::NO_CONTENT`
- **PUT operations**: `StatusCode::OK`

⚠️ **IMPORTANT**: Use direct return types (without `Result`) ONLY for infallible operations that cannot fail, such as
handlers accessing cached data.

#### 📋 Collection Response Structure

```rust
// Collection response structure pattern
#[derive(Debug, serde::Serialize)]
pub struct ResourcesResponse {
    pub items: Vec<ResourceInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<ResourceId>,
}
```

### 5. 📊 Collection Response Patterns

#### 📄 Paginated Collections (Default)

🚨 **MANDATORY**: Implement cursor-based pagination for large datasets:

```rust
// Paginated collection handler pattern
const DEFAULT_PAGE_LIMIT: usize = 50;
const MAX_PAGE_LIMIT: usize = 1000;

pub async fn handler() -> Result<(), Error> {
    // Validate limit
    let limit = if query.limit > MAX_PAGE_LIMIT {
        return Err(Error::LimitTooLarge {
            limit: query.limit,
            max: MAX_PAGE_LIMIT,
        });
    } else if query.limit == 0 {
        return Err(Error::LimitInvalid);
    } else {
        query.limit
    };

    // Fetch data with pagination
    let items = ctx.metadata_db
        .list_resources(limit as i64, query.last_id)
        .await?;

    // Determine next cursor
    let next_cursor = items.last().map(|item| item.id);
    
    Ok(())
}
```

#### 📝 Simple Collections (Non-Paginated)

✅ **ONLY FOR**: Smaller, bounded datasets that can be returned in full (e.g., providers):

```rust
// Simple collection handler pattern (non-paginated)
pub async fn handler() -> Result<Json<ResourcesResponse>, Error> {
    // Fetch all items from the data source
    let items = ctx.store
        .resource_store()
        .get_all()
        .await
        .iter()
        .map(|(name, config)| ResourceInfo::new(name.clone(), config.clone()))
        .collect();

    Ok(Json(ResourcesResponse { items }))
}
```

✅ **Use simple collections ONLY when:**

- The total number of items is small and bounded (typically < 100)
- The data source doesn't support efficient pagination
- The cost of returning all items is acceptable

### 6. 💾 Data Source Patterns

#### 🗄️ Database-Backed Resources (`metadata_db`)

🚨 **MANDATORY PATTERN**: For resources stored in the PostgreSQL `metadata_db` (jobs, locations):

```rust
// Database-backed resource handler patterns
pub async fn handler() -> Result<(), Error> {
    // List resources with pagination
    let resources = ctx.metadata_db
        .list_resources(limit as i64, query.last_id)
        .await
        .map_err(Error::MetadataDbError)?;

    // Get single resource
    let resource = ctx.metadata_db
        .get_resource_by_id(id)
        .await
        .map_err(Error::MetadataDbError)?
        .ok_or_else(|| Error::NotFound { id })?;
        
    Ok(())
}
```

#### 🏪 Store-Backed Resources (`DatasetStore`)

🚨 **MANDATORY PATTERN**: For resources managed through specialized stores (providers):

```rust
// Store-backed resource handler pattern
pub async fn handler() -> Result<Json<ResourceInfo>, Error> {
    // Access through store getter method
    let resource = ctx.store
        .providers()  // Get store reference
        .get_by_name(&name)
        .await;

    // Convert and handle not found
    match resource {
        Some(config) => Ok(Json(ResourceInfo::new(name, config))),
        None => Err(Error::NotFound { name }),
    }
}
```

📌 **Key differences:**

- **Database resources**: Use direct async database queries, support pagination, have typed IDs
- **Store resources**: Use store abstraction methods, may not support pagination, often use string identifiers

## 🚨 Error Handling Patterns

### 1. 💥 Error Enum Definition

🔥 **ABSOLUTELY CRITICAL REQUIREMENT**: Each fallible handler MUST define its own comprehensive error enum with detailed
explanations for each variant using the exact name `Error`. This is ONLY applicable to handlers that return `Result<ResponseType, BoxRequestError>`.
✅ Infallible handlers do NOT need error enums.

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Path parameter validation errors
    #[error("invalid resource ID: {err}")]
    InvalidId {
        err: PathRejection,
    },

    /// Resource not found
    #[error("resource '{id}' not found")]
    NotFound {
        id: ResourceId,
    },

    /// Query parameter validation errors
    #[error("invalid query parameters: {err}")]
    InvalidQueryParams {
        err: QueryRejection,
    },

    /// Business logic conflicts
    #[error("resource conflict: {message}")]
    Conflict {
        message: String,
    },

    /// Database errors
    #[error("metadata db error: {0}")]
    MetadataDbError(#[from] metadata_db::Error),
}
```

### 2. 🔄 `RequestError` Implementation

🚨 **MANDATORY**: Implement the `RequestError` trait for proper HTTP responses:

```rust
impl RequestError for Error {
    fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidId { .. } => "INVALID_RESOURCE_ID",
            Error::NotFound { .. } => "RESOURCE_NOT_FOUND",
            Error::InvalidQueryParams { .. } => "INVALID_QUERY_PARAMETERS",
            Error::Conflict { .. } => "RESOURCE_CONFLICT",
            Error::MetadataDbError(_) => "METADATA_DB_ERROR",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidId { .. } => StatusCode::BAD_REQUEST,
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::InvalidQueryParams { .. } => StatusCode::BAD_REQUEST,
            Error::Conflict { .. } => StatusCode::CONFLICT,
            Error::MetadataDbError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
```

### 3. 🏷️ Error Code Conventions

| Error Type               | HTTP Status                               | Error Code Pattern         |
|--------------------------|-------------------------------------------|----------------------------|
| Invalid path parameters  | 400 (`StatusCode::BAD_REQUEST`)           | `INVALID_<RESOURCE>_ID`    |
| Invalid query parameters | 400 (`StatusCode::BAD_REQUEST`)           | `INVALID_QUERY_PARAMETERS` |
| Resource not found       | 404 (`StatusCode::NOT_FOUND`)             | `<RESOURCE>_NOT_FOUND`     |
| Business logic conflicts | 409 (`StatusCode::CONFLICT`)              | `<RESOURCE>_CONFLICT`      |
| Database errors          | 500 (`StatusCode::INTERNAL_SERVER_ERROR`) | `METADATA_DB_ERROR`        |

## 📚 Documentation Standards

### 1. 📝 Handler Documentation

🚨 **MANDATORY**: Each handler MUST include comprehensive documentation following this EXACT pattern:

```rust
/// Handler for the `GET /resources/{id}` endpoint
///
/// Retrieves and returns a specific resource by its ID.
///
/// ## Path Parameters
/// - `id`: The unique identifier (must be a positive integer)
///
/// ## Response
/// - **200 OK**: Returns the resource information as JSON
/// - **400 Bad Request**: Invalid resource ID format
/// - **404 Not Found**: Resource not found
/// - **500 Internal Server Error**: Database error
///
/// ## Error Codes
/// - `INVALID_RESOURCE_ID`: Invalid ID format
/// - `RESOURCE_NOT_FOUND`: Resource doesn't exist
/// - `METADATA_DB_ERROR`: Internal database error
///
/// This handler:
/// - Validates and extracts the resource ID from the URL path
/// - Queries the metadata database for the resource
/// - Returns appropriate HTTP status codes and error messages
#[tracing::instrument(skip_all, err)]
pub async fn handler(/* ... */) {
    // Implementation
}
```

#### 📋 Handler Documentation Sections Explained

🚨 **Each handler documentation MUST contain these sections in this exact order:**

1. **📌 Title Line**: `Handler for the \`<METHOD> /<endpoint>\` endpoint`
    - Must specify the exact HTTP method and endpoint path
    - Use backticks around the method and path

2. **📄 Brief Description**: One-sentence description of what the handler does
    - Must be clear and concise
    - Focus on the primary action performed

3. **🛤️ Path Parameters Section** (if applicable): `## Path Parameters`
    - List each path parameter with its type and validation requirements
    - Format: `- \`param_name\`: Description (constraints)`
    - Only include if handler has path parameters

4. **❓ Query Parameters Section** (if applicable): `## Query Parameters`
    - List each query parameter with its type, default value, and constraints
    - Format: `- \`param_name\`: Description (default: value, constraints)`
    - Only include if handler accepts query parameters

5. **📤 Response Section**: `## Response`
    - List ALL possible HTTP status codes and their meanings
    - Format: `- **<STATUS CODE> <STATUS NAME>**: Description`
    - Must include success responses AND error responses

6. **🏷️ Error Codes Section** (fallible handlers only): `## Error Codes`
    - List ALL custom error codes that can be returned
    - Format: `- \`ERROR_CODE\`: Brief description`
    - Only include for handlers that return `Result<_, BoxRequestError>`

7. **⚙️ Implementation Summary**: `This handler:`
    - Bullet-point list of key operations performed
    - Must describe the main flow and logic
    - Include validation, data access, and response generation steps

### 2. 💥 Error Documentation

🔥 **ABSOLUTELY MANDATORY**: Each error variant MUST include comprehensive documentation explaining exactly when and why
this error is returned. This documentation is CRITICAL for API consumers and maintainers.

```rust
/// Errors that can occur during resource retrieval
///
/// This enum represents all possible error conditions when handling
/// a request, from path parsing to database operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The resource ID in the URL path is invalid
    ///
    /// This occurs when:
    /// - The ID is not a valid number (e.g., "abc", "1.5")
    /// - The ID is zero or negative (e.g., "0", "-5")
    /// - The ID is too large to fit in the expected type
    #[error("invalid resource ID: {err}")]
    InvalidId {
        /// The rejection details from Axum's path extractor
        err: PathRejection,
    },
    // Additional variants...
}
```

## 📋 Response Type Patterns

### 1. 📊 Response Type Location Rules

🚨 **CRITICAL**: Response types MUST be placed according to these rules:

#### 📁 **Handler-Specific Types** (DEFAULT)

🚨 **MANDATORY**: Response types used by only ONE handler MUST be defined within the handler file itself:

```rust
// In: src/handlers/jobs/get_by_id.rs

/// Job information returned by the get_by_id endpoint
#[derive(Debug, serde::Serialize)]
pub struct JobResponse {
    pub id: JobId,
    pub name: String,
    pub status: JobStatus,
    // Handler-specific fields
}

#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<JobId>, PathRejection>,
) -> Result<Json<JobResponse>, BoxRequestError> {
    // Implementation uses JobResponse
}
```

#### 📦 **Shared Types** (`<resource>_info.rs`)

🚨 **MANDATORY**: Response types used by MULTIPLE handlers within the same resource MUST be defined in
`<resource>_info.rs`:

```rust
// In: src/handlers/jobs/job_info.rs

/// Job information shared across multiple job endpoints
#[derive(Debug, serde::Serialize)]
pub struct JobInfo {
    pub id: JobId,
    pub name: String,
    pub created_at: String,  // ISO 8601 format
    pub updated_at: String,  // ISO 8601 format
    pub status: JobStatus,
    // Shared fields
}

impl From<metadata_db::Job> for JobInfo {
    fn from(value: metadata_db::Job) -> Self {
        Self {
            id: value.id,
            name: value.name,
            created_at: value.created_at.to_rfc3339(),
            updated_at: value.updated_at.to_rfc3339(),
            status: value.status,
        }
    }
}

/// Collection response for job listings
#[derive(Debug, serde::Serialize)]
pub struct JobsResponse {
    pub items: Vec<JobInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<JobId>,
}
```

### 2. 🎯 Response Type Guidelines

#### ✅ **When to Use Handler-Specific Types**

- Response used by only ONE endpoint
- Contains handler-specific business logic
- Has unique validation or transformation requirements
- Unlikely to be reused by other handlers

#### ✅ **When to Use Shared Types (`<resource>_info.rs`)**

- Response used by MULTIPLE handlers in the same resource
- Contains common resource representation
- Used in collection responses (e.g., lists, search results)
- Represents the canonical resource format

#### 🚫 **Never Do This**

- DON'T put handler-specific types in `<resource>_info.rs`
- DON'T duplicate shared types across multiple handlers
- DON'T create cross-resource shared types (each resource owns its types)

### 3. 🔒 Response Type Independence Requirements

🚨 **CRITICAL REQUIREMENT**: Response types MUST be completely independent of internal API types (database models, store
types, etc.). You MUST implement proper conversion traits to transform internal types into response types.

#### ✅ **Correct Pattern - Independent Types with Conversion**

```rust
// ❌ NEVER expose internal database types directly
// pub type JobInfo = metadata_db::Job;  // 🚫 FORBIDDEN

// ✅ ALWAYS create independent response types
/// Job information returned by the API
#[derive(Debug, serde::Serialize)]
pub struct JobInfo {
    pub id: JobId,
    pub name: String,
    pub created_at: String,  // ISO 8601 format - different from DB DateTime
    pub updated_at: String,  // ISO 8601 format - different from DB DateTime  
    pub status: String,      // String representation - different from DB enum
}

// ✅ MANDATORY: Implement conversion traits
impl From<metadata_db::Job> for JobInfo {
    fn from(db_job: metadata_db::Job) -> Self {
        Self {
            id: db_job.id,
            name: db_job.name,
            created_at: db_job.created_at.to_rfc3339(),  // Convert DateTime to string
            updated_at: db_job.updated_at.to_rfc3339(),  // Convert DateTime to string
            status: db_job.status.to_string(),           // Convert enum to string
        }
    }
}

// ✅ MANDATORY: Use conversion in handlers
#[tracing::instrument(skip_all, err)]
pub async fn handler(
    State(ctx): State<Ctx>,
    path: Result<Path<JobId>, PathRejection>,
) -> Result<Json<JobInfo>, BoxRequestError> {
    let id = extract_path_id(path)?;

    let db_job = ctx.metadata_db
        .get_job_by_id(id)
        .await
        .map_err(Error::MetadataDbError)?
        .ok_or_else(|| Error::NotFound { id })?;

    // ✅ Convert internal type to response type
    let job_info = JobInfo::from(db_job);
    Ok(Json(job_info))
}
```

#### 🔥 **Why This Is Critical**

1. **API Stability**: Internal types can change without breaking the API
2. **Data Control**: Response types control exactly what data is exposed
3. **Format Independence**: Response types can use different formats (strings vs enums, ISO dates vs timestamps)
4. **Future Flexibility**: Easy to modify internal structures without API changes
5. **Security**: Prevents accidental exposure of internal fields

#### 🚫 **Forbidden Patterns**

```rust
// ❌ NEVER directly expose internal types
pub type JobInfo = metadata_db::Job;
pub type ProviderInfo = dataset_store::ProviderConfig;

// ❌ NEVER return internal types directly  
pub async fn handler() -> Result<Json<metadata_db::Job>, BoxRequestError> {
    // FORBIDDEN - exposes internal structure
}

// ❌ NEVER use internal types in response structs
#[derive(Debug, serde::Serialize)]
pub struct JobsResponse {
    pub items: Vec<metadata_db::Job>,  // FORBIDDEN
    pub next_cursor: Option<JobId>,
}
```

#### 🚨 **Conversion Trait Requirements**

🔥 **MANDATORY**: Every response type MUST implement conversion from its corresponding internal type:

- **For database types**: `impl From<metadata_db::Resource> for ResourceInfo`
- **For store types**: `impl From<dataset_store::Config> for ConfigInfo`
- **For complex conversions**: Use `TryFrom` when conversion can fail
- **For collections**: Implement `From<Vec<Internal>>` or use `map()` with individual conversions

## 📋 Logging Patterns

### 1. 🏗️ Structured Logging

🚨 **MANDATORY**: Use structured logging throughout ALL handlers:

```rust
// Debug level for expected errors
tracing::debug!(
    error=?err,
    resource_id=?id, 
    "failed to get resource"
);

// Info level for successful operations
tracing::info!(
    resource_id=?id,
    deleted_count=count,
    "successfully completed operation"
);

// Warn level for partial failures
tracing::warn!(
    expected=expected_count,
    actual=actual_count,
    "operation completed with warnings"
);

// Error level for unexpected failures
tracing::error!(
    error=?err,
    resource_id=?id,
    "unexpected error during operation"
);
```

### 2. 🔍 Context Information

🚨 **MANDATORY**: Include relevant context in ALL log messages:

- Resource IDs
- Operation parameters
- Error details
- Performance metrics (when relevant)

## 🛣️ Route Registration

🚨 **MANDATORY**: Register routes in `lib.rs` following REST conventions:

```rust
// Route registration pattern
fn handler() -> Router {
    let app = Router::new()
        .route("/resources", get(get_all::handler).post(create::handler))
        .route(
            "/resources/{id}",
            get(get_by_id::handler).delete(delete_by_id::handler),
        )
        .route("/resources/{id}/action", put(action::handler))
        .with_state(ctx);
    app
}
```

## 🧪 Testing Considerations

🚨 **MANDATORY**: When implementing handlers, you MUST consider these testing aspects:

1. **Path parameter validation**: Test invalid, missing, and edge-case IDs
2. **Query parameter validation**: Test limits, invalid formats, and edge cases
3. **Database error handling**: Ensure proper error propagation
4. **Business logic validation**: Test conflict scenarios and state transitions
5. **Response serialization**: Verify JSON structure matches expectations

## 🔒 Security Considerations

**🚨 CRITICAL: Security violations may result in:**
- Immediate rejection of pull requests
- Required security audits and remediation
- Compliance violations and regulatory issues  
- Potential data breach risks

**⚠️ AI Agent Instructions:**
- **ALWAYS** review crate-specific security guidelines BEFORE making changes
- **COMPLETE** all security checklists as part of development process
- **PRIORITIZE** security requirements over convenience or speed
- **ESCALATE** any security concerns or questions immediately
- **NEVER** bypass or ignore security requirements

### 🛡️ Security Requirements

- **[security.md](./security.md)** - **🚨 MANDATORY SECURITY REVIEW** for all `admin-api` changes. Contains comprehensive security checklist covering HTTP API security, plain HTTP server requirements, input validation, injection prevention, and network isolation patterns.

**Key Security Requirements:**
1. **Input validation**: Always validate and sanitize input parameters using Axum extractors
2. **Error information**: Don't expose internal system details in error messages
3. **Network trust**: Assume trusted network environment (no built-in authentication)
4. **Injection prevention**: Prevent path traversal, command injection, and other attacks

## ⚡ Performance Guidelines

1. **Database queries**: Use efficient queries and proper indexing
2. **Pagination**: Implement cursor-based pagination for large datasets
3. **Response size**: Limit response sizes with configurable limits
4. **Async operations**: Leverage async/await for I/O operations

## 🔄 Migration and Compatibility

🚨 **MANDATORY**: When modifying existing handlers:

1. **Backward compatibility**: Maintain existing API contracts
2. **Deprecation strategy**: Use deprecation warnings for removed features
3. **Version management**: Consider API versioning for breaking changes
4. **Documentation updates**: Update documentation with changes

---

🚨 **CRITICAL**: This guide MUST be referenced when implementing new handlers or modifying existing ones to ensure
consistency with established patterns in the `admin-api` crate. 🚫 **NO EXCEPTIONS**.
