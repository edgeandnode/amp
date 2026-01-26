---
name: "testing-patterns"
description: "Three-tier testing strategy, test structure, Given-When-Then. Load when writing tests"
type: core
scope: "global"
---

# Testing Patterns - Rust

## 🎯 OVERVIEW

Comprehensive testing strategies for Rust projects, with emphasis on proper async testing, database integration, and
type-safe testing approaches using the three-tier testing strategy.

## 🚨 CRITICAL TESTING REQUIREMENTS

### Testing Framework Selection

#### ✅ Use standard `#[test]` for synchronous functions

```rust
#[test]
fn should_work_with_pure_functions() {
    //* Given
    let input = create_test_input();

    //* When
    let result = pure_function(input);

    //* Then
    assert_eq!(result, expected_value);
}
```

#### ✅ Use `#[tokio::test]` for async functions

```rust
#[tokio::test]
async fn should_work_with_async_operations() {
    //* Given
    let input_data = setup_test_data();

    //* When
    let result = some_async_function(input_data).await;

    //* Then
    assert_eq!(result, expected_value);
}
```

### 📝 GIVEN-WHEN-THEN Structure (MANDATORY)

Every test must use **MANDATORY** comments: `//* Given` (optional), `//* When`, `//* Then`

```rust
#[test]
fn validate_input_with_empty_string_fails() {
    //* Given
    // Optional - can be omitted for very simple tests
    let input = "";
    
    //* When
    // EXACTLY ONE function under test
    let result = validate_input(input);
    
    //* Then
    // ONLY assertions and assertion helper logic
    assert!(result.is_err(), "validation should fail for empty input");
}
```

### ❌ FORBIDDEN PATTERNS

#### Never use `unwrap()` in tests

```rust
// ❌ WRONG - Don't use unwrap() in tests
#[tokio::test]
async fn wrong_pattern() {
    //* Given
    let input = setup_data();

    //* When
    let result = risky_operation(input).await.unwrap(); // Wrong - can panic

    //* Then
    assert_eq!(result, value);
}

// ✅ CORRECT - Use expect() with descriptive messages
#[tokio::test]
async fn correct_pattern() {
    //* Given
    let input = setup_data();

    //* When
    let result = risky_operation(input).await
        .expect("risky_operation should succeed with valid input");

    //* Then
    assert_eq!(result, value);
}
```

#### Never test multiple functions in one test

```rust
// ❌ WRONG - Testing multiple functions violates single responsibility
#[tokio::test]
async fn wrong_multiple_functions() {
    //* Given
    let input = setup_test_data();

    //* When
    let result1 = function_a(input).await.expect("function_a should work");
    let result2 = function_b(input).await.expect("function_b should work"); // Wrong - multiple functions under test

    //* Then
    assert_eq!(result1 + result2, expected_value);
}

// ✅ CORRECT - Test exactly one function
#[tokio::test]
async fn correct_single_function() {
    //* Given
    let input = setup_test_data();

    //* When
    let result = function_a(input).await.expect("function_a should work");

    //* Then
    assert_eq!(result, expected_value);
}
```

## 📝 TEST CASE NAMING CONVENTIONS

All test functions must follow descriptive naming patterns that explain the scenario being tested. This makes tests self-documenting and improves maintainability.

### 🏷️ Required Naming Pattern

Use the format: `<function_name>_<scenario>_<expected_outcome>()`

**Components:**
- **function_name**: The exact name of the function being tested
- **scenario**: The specific input condition, state, or situation
- **expected_outcome**: What should happen (succeeds, fails, returns_none, etc.)

### ✅ Correct Examples

```rust
// Testing different scenarios for the same function
#[tokio::test]
async fn insert_job_with_valid_data_succeeds() { /* ... */ }

#[tokio::test] 
async fn insert_job_with_duplicate_id_fails() { /* ... */ }

#[tokio::test]
async fn insert_job_with_invalid_status_fails() { /* ... */ }

// Testing retrieval functions
#[tokio::test]
async fn get_by_id_with_existing_id_returns_record() { /* ... */ }

#[tokio::test]
async fn get_by_id_with_nonexistent_id_returns_none() { /* ... */ }

#[tokio::test]
async fn get_by_id_with_malformed_id_fails() { /* ... */ }

// Testing state transitions
#[test]
fn update_status_with_valid_transition_succeeds() { /* ... */ }

#[test]
fn update_status_with_invalid_transition_fails() { /* ... */ }

#[test]
fn update_status_with_terminal_state_fails() { /* ... */ }

// Testing edge cases and boundary conditions
#[test]
fn validate_worker_id_with_empty_input_fails() { /* ... */ }

#[test]
fn validate_worker_id_with_max_length_succeeds() { /* ... */ }

#[test]
fn validate_worker_id_with_too_long_input_fails() { /* ... */ }
```

### ❌ Incorrect Examples

```rust
// ❌ WRONG - Vague, non-descriptive names
#[test]
fn test_insert() { /* ... */ }

#[test]
fn insert_works() { /* ... */ }

#[test]
fn test_validation() { /* ... */ }

// ❌ WRONG - Including "test" in test names is redundant
#[test]
fn test_insert_job_with_valid_data_succeeds() { /* ... */ }

#[test]
fn insert_job_test_with_valid_data() { /* ... */ }

#[test]
fn validate_worker_id_test_returns_error() { /* ... */ }

// ❌ WRONG - Missing scenario description
#[test]
fn insert_job_succeeds() { /* ... */ }  // What input scenario?

#[test]
fn get_by_id_fails() { /* ... */ }      // Under what conditions?

// ❌ WRONG - Missing expected outcome
#[test]
fn insert_job_with_valid_data() { /* ... */ }  // Succeeds or fails?

#[test]
fn get_by_id_with_invalid_id() { /* ... */ }   // Returns what?

// ❌ WRONG - Testing multiple functions (violates single responsibility)
#[test]
fn create_and_update_job_succeeds() { /* ... */ }  // Should be split into two tests
```

### 🎯 Naming Guidelines by Test Type

#### **Unit Tests - Pure Logic**
Focus on input conditions and business rules:
```rust
fn validate_input_with_empty_string_fails() {}
fn calculate_total_with_valid_items_succeeds() {}
fn parse_config_with_malformed_json_fails() {}
fn format_timestamp_with_utc_returns_iso_string() {}
```

#### **Database Integration Tests**
Include database state and operations:
```rust
async fn insert_record_with_valid_data_succeeds() {}
async fn update_status_with_concurrent_modification_fails() {}
async fn delete_by_id_with_existing_record_succeeds() {}
async fn get_by_id_with_deleted_record_returns_none() {}
```

#### **API Integration Tests**
Focus on workflows and end-to-end scenarios:
```rust
async fn register_worker_and_schedule_job_workflow_succeeds() {}
async fn worker_heartbeat_update_with_expired_session_fails() {}
async fn job_completion_with_multiple_workers_maintains_consistency() {}
```

### 📏 Length and Clarity Guidelines

- **Be descriptive but concise** - aim for clarity over brevity
- **Use domain terminology** consistently
- **Avoid abbreviations** unless they're well-established in the domain
- **Maximum ~60 characters** when possible, but prioritize clarity

```rust
// Good balance of descriptive and concise
fn validate_email_with_missing_at_symbol_fails() {}
fn process_payment_with_insufficient_funds_returns_error() {}

// Too verbose (but acceptable if needed for clarity)
fn update_worker_heartbeat_timestamp_with_nonexistent_worker_id_succeeds_silently() {}

// Too abbreviated (avoid)
fn upd_hb_inv_wrk_fails() {}
```

---

## 📝 GIVEN-WHEN-THEN STRUCTURE

Every test must follow the GIVEN-WHEN-THEN pattern with **MANDATORY** `//* Given`, `//* When`, and `//* Then` comments. This structure ensures clear test organization and makes tests self-documenting.

### 🔧 Structure Requirements

#### `//* Given` - Setup (OPTIONAL)
- **Purpose**: Set up preconditions, test data, mocks, and system state
- **Content**: Variable declarations, database setup, mock configurations
- **Optional**: Can be omitted if no setup is required for simple tests

#### `//* When` - Action (REQUIRED)
- **Purpose**: Execute **EXACTLY ONE** function under test
- **Content**: **ONLY** the single function call being tested
- **Critical**: Must test exactly one function - multiple function calls indicate the test scope is too broad

#### `//* Then` - Verification (REQUIRED)
- **Purpose**: Assert expected outcomes and verify side effects
- **Content**: **ONLY** assertions and assertion-helping logic (like `.expect()` calls to extract values for assertions)
- **Restrictions**: No business logic, no additional function calls beyond assertion helpers

### 📋 Complete Example

```rust
#[tokio::test]
async fn function_name_scenario_expected_outcome() {
    //* Given
    let db = temp_metadata_db().await;
    let test_data = create_test_record();
    let expected_status = JobStatus::Scheduled;

    //* When
    let result = insert_record(&db.pool, &test_data).await;

    //* Then
    assert!(result.is_ok(), "record insertion should succeed with valid data");
    let record_id = result.expect("should return valid record ID");
    assert!(record_id.as_i64() > 0, "record ID should be positive");
    
    // Verify side effects
    let inserted_record = get_record_by_id(&db.pool, record_id).await
        .expect("should retrieve inserted record")
        .expect("record should exist");
    assert_eq!(inserted_record.status, expected_status);
}
```

### ✅ Simple Test Without Given Section

```rust
#[test]
fn validate_get_default_fails_if_uninitialized() {
    //* When
    let result = get_default();
    
    //* Then
    assert!(result.is_err(), "validation should fail for uninitialized state");
    let error = result.expect_err("should return validation error");
    assert!(matches!(error, ValidationError::EmptyInput),
        "Expected EmptyInput error, got {:?}", error);
}
```

### ❌ VIOLATIONS - What NOT to do

```rust
// ❌ WRONG - Missing mandatory comments
#[test]
fn bad_test_without_comments() {
    let input = "test";
    let result = validate_input(input);
    assert!(result.is_ok());
}

// ❌ WRONG - Multiple functions in When section
#[tokio::test]
async fn bad_test_multiple_functions() {
    //* Given
    let db = temp_metadata_db().await;
    
    //* When
    let user = create_user(&db.pool, "test").await;  // Function 1
    let result = update_user(&db.pool, user.id).await;  // Function 2 - WRONG!
    
    //* Then
    assert!(result.is_ok());
}

// ❌ WRONG - Business logic in Then section
#[tokio::test]
async fn bad_test_logic_in_then() {
    //* Given
    let db = temp_metadata_db().await;
    
    //* When
    let result = get_user(&db.pool, user_id).await;
    
    //* Then
    assert!(result.is_ok());
    let user = result.expect("should get user");
    
    // WRONG - Business logic in Then section
    let processed_name = user.name.to_uppercase();  // This belongs in Given
    let expected_email = format!("{}@test.com", processed_name);  // This belongs in Given
    assert_eq!(user.email, expected_email);
}
```

## 🗂️ THREE-TIER TESTING STRATEGY

### ⚠️ CRITICAL: Understand the three test types

The three-tier testing strategy provides comprehensive coverage across different levels of abstraction, ensuring reliability and correctness from individual functions to complete workflows. Each test type serves a specific purpose and has different requirements, performance characteristics, and organizational patterns.

### 🎯 Test Type Overview

| Test Type | Dependencies | Speed | Purpose | Location |
|-----------|--------------|--------|---------|----------|
| **Unit Tests** | None | Milliseconds | Pure business logic | Same file (`#[cfg(test)]`) |
| **In-tree Integration** | External (DB, Network) | Slower | Internal APIs with dependencies | `tests::it_*` submodules |
| **Public API Integration** | External (DB, Network) | Slower | End-to-end workflows | `tests/` directory |

---

## 🚀 Unit Tests

Unit tests must have **no external dependencies** and execute in **milliseconds**. These tests validate pure business logic, data transformations, and error handling without requiring database connections or external services.

### ✅ Requirements

- **NO EXTERNAL DEPENDENCIES**: No PostgreSQL database instance required
- **Performance**: Must complete execution in milliseconds  
- **Co-location**: Tests live within the same file as the code being tested
- **Module structure**: Use `#[cfg(test)]` annotated `tests` submodule

### 📁 File Organization

Unit tests can be organized in two ways:

#### Option 1: Co-located Tests (Recommended for Simple Cases)

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

        #[test]
        fn validate_worker_id_with_too_long_input_fails() {
            //* Given
            let long_id = "a".repeat(65);
            
            //* When
            let result = validate_worker_id(&long_id);
            
            //* Then
            assert!(result.is_err(), "validation should fail with too long input");
            let error = result.expect_err("should return validation error");
            assert!(matches!(error, ValidationError::TooLong),
                "Expected TooLong error, got {:?}", error);
        }
    }
}
```

#### Option 2: In-tree Tests Directory (For Complex Unit Test Suites)

Unit tests can also be extracted to the in-tree tests directory. The **critical distinction** from in-tree integration tests is that unit test submodules **NEVER EVER SHOULD START** with `it_`.

```rust
// <crate-root>/src/workers/tests/validation.rs <- ✅ CORRECT - Does NOT start with 'it_'
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

    #[test]
    fn validate_worker_id_with_too_long_input_fails() {
        //* Given
        let long_id = "a".repeat(65);
        
        //* When
        let result = validate_worker_id(&long_id);
        
        //* Then
        assert!(result.is_err(), "validation should fail with too long input");
        let error = result.expect_err("should return validation error");
        assert!(matches!(error, ValidationError::TooLong),
            "Expected TooLong error, got {:?}", error);
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

### 🎯 What to Test with Unit Tests

- **Data validation logic** (ID validation, input sanitization)
- **Business rule enforcement** (status transitions, constraint checking)
- **Data transformation functions** (parsing, formatting, conversion)
- **Error condition handling** (boundary cases, invalid inputs)
- **Pure computational functions** (calculations, algorithms)

---

## 🔗 In-tree Integration Tests

In-tree integration tests cover **internal functionality** not exposed through the crate's public API. These tests are named "integration" because they test functionality with **external dependencies** and can suffer from slowness and flakiness.

### 🎯 Purpose

- Test **internal module functions** that aren't part of the crate's public API
- Cover internal functionality that requires external dependencies (database, network, filesystem)
- Verify **integration between internal components**
- Test **edge cases and error conditions** that involve external systems

### ✅ Characteristics

- **External dependencies**: Use actual database connections or external services
- **Mandatory nesting**: Must live in `tests::it_*` submodules for test selection
- **File structure**: Either separate files or inline submodules
- **Flakiness risk**: May fail due to external dependency issues
- **Performance**: Slower execution due to external dependencies

### 📁 File Structure Options

#### Option 1: Inline Integration Test Submodule

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
    
    mod it_heartbeat {
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

#### Option 2: External Integration Test File

```rust
// <crate-root>/src/workers/tests/it_workers.rs
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

### 🎯 What to Test with In-tree Integration Tests

- **Database operations** (CRUD operations, complex queries)
- **Transaction behavior** (rollback on failure, atomicity)
- **Error handling with external systems** (network failures, database constraints)
- **Resource management** (connection pooling, cleanup)
- **Migration and schema changes** (forward and backward compatibility)

---

## 🌐 Public API Integration Tests

Public API tests are Rust's standard integration testing mechanism as defined in the [Rust Book](https://doc.rust-lang.org/book/ch11-03-test-organization.html#integration-tests). These tests verify **end-to-end functionality** by testing the integration between different code parts through the **crate's public API only**.

### 🎯 Purpose

- Test the **public crate interface** and all exported functionality
- Verify **integration between different components** of the crate
- Ensure **complete workflows** work correctly from a user perspective
- Test the crate as **external users would use it**

### ✅ Characteristics

- **Public API only**: No access to internal crate APIs unless explicitly made public
- **External location**: Located in `tests/` directory (separate from source code)
- **End-to-end testing**: Test complete user workflows and integration scenarios
- **External dependencies**: These ARE integration tests and MAY use external dependencies
- **Cargo integration**: Each file in `tests/` is compiled as a separate crate
- **File naming**: Must be named `it_*` for test filtering purposes

### 📁 File Structure

```rust
// <crate-root>/tests/it_api_workers.rs
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

### 🎯 What to Test with Public API Integration Tests

- **Complete user workflows** (registration → job assignment → completion)
- **Cross-resource interactions** (workers + jobs + locations)
- **Error propagation** through the public API
- **Concurrent operations** and consistency guarantees
- **Resource cleanup and lifecycle management**

---

## 🏃‍♂️ Running Tests

The three-tier testing strategy allows for **selective test execution** based on performance and dependency requirements.

### ⚡ Unit Tests (fast, no external dependencies)

```bash
# Run only unit tests, skip all in-tree integration tests
cargo test -p metadata-db 'tests::' -- --skip 'tests::it_'

# Run specific module's unit tests
cargo test -p metadata-db 'workers::tests::' -- --skip 'tests::it_'
```

### 🔗 In-tree Integration Tests (slower, requires external dependencies)

```bash
# Run only in-tree integration tests
cargo test -p metadata-db 'tests::it_'

# Run specific in-tree integration test suite
cargo test -p metadata-db 'tests::it_workers'

# Run in-tree integration tests for specific module
cargo test -p metadata-db 'workers::tests::it_'
```

### 🌐 Public API Integration Tests (slower, requires external dependencies)

```bash
# Run all public API integration tests
cargo test -p metadata-db --test '*'

# Run specific public API integration test file
cargo test -p metadata-db --test it_api_workers

# Run specific test within integration test file
cargo test -p metadata-db --test it_api_workers register_worker_and_schedule_job_workflow_succeeds
```

### 🔄 Complete Test Suite

```bash
# Run all tests in order of speed (fastest first)
cargo test -p metadata-db 'tests::' -- --skip 'tests::it_'  # Unit tests
cargo test -p metadata-db 'tests::it_'                      # In-tree integration  
cargo test -p metadata-db --test '*'                        # Public API integration
```

---

## 📝 Test Organization Best Practices

### 🏗️ Module Structure

Organize tests within modules to group related functionality:

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

### 📊 Progressive Test Complexity

Structure tests from simple to complex within each category:

```rust
mod feature_progression {
    use super::*;

    // 1. Basic functionality
    #[test]
    fn basic_functionality_works() {
        //* Given
        let input = create_basic_input();

        //* When
        let result = basic_function(input);

        //* Then
        assert!(result.is_ok());
    }

    // 2. With configuration
    #[test]
    fn with_custom_configuration_works() {
        //* Given
        let config = CustomConfig { option: true };

        //* When
        let result = function_with_config(config);

        //* Then
        assert_eq!(result, expected_configured_value);
    }

    // 3. Error scenarios
    #[tokio::test]
    async fn with_error_handling_works() {
        //* Given
        let invalid_input = create_invalid_input();

        //* When
        let result = async_function(invalid_input).await;

        //* Then
        assert!(result.is_err());
    }

    // 4. External dependencies
    #[tokio::test]
    async fn with_external_dependencies_works() {
        //* Given
        let db = temp_metadata_db().await;
        let test_data = create_test_data();

        //* When
        let result = function_with_db(&db.pool, test_data).await;

        //* Then
        assert!(result.is_ok());
    }

    // 5. Full integration
    #[tokio::test]
    async fn full_integration_workflow_succeeds() {
        //* Given
        let db = temp_metadata_db().await;
        let complete_workflow_data = create_workflow_data();

        //* When
        let result = complete_workflow(&db, complete_workflow_data).await;

        //* Then
        assert!(result.is_ok());
        verify_workflow_completion(&db).await;
    }
}
```

## 🧪 COMPREHENSIVE TESTING PATTERNS

### Basic Unit Testing Pattern

```rust
use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    mod constructors {
        use super::*;

        #[test]
        fn create_with_default_values_succeeds() {
            //* Given
            let config = Config::default();

            //* When
            let result = MyStruct::new(config);

            //* Then
            assert!(result.is_ok(), "creation with defaults should succeed");
            let instance = result.expect("should create valid instance");
            assert_eq!(instance.get_value(), 0);
        }

        #[test]
        fn create_with_custom_config_succeeds() {
            //* Given
            let config = Config { initial_value: 42 };

            //* When
            let result = MyStruct::new(config);

            //* Then
            assert!(result.is_ok(), "creation with custom config should succeed");
            let instance = result.expect("should create valid instance");
            assert_eq!(instance.get_value(), 42);
        }
    }

    mod combinators {
        use super::*;

        #[test]
        fn map_transforms_values_correctly() {
            //* Given
            let instance = MyStruct::new(Config { initial_value: 10 })
                .expect("should create valid instance");

            //* When
            let result = instance.map(|x| x * 2);

            //* Then
            assert_eq!(result.get_value(), 20);
        }
    }
}
```

### Error Handling Testing Pattern

```rust
#[cfg(test)]
mod tests {
    use super::*;

    mod error_handling {
        use super::*;

        #[test]
        fn create_with_negative_value_fails_with_validation_error() {
            //* Given
            let config = Config { initial_value: -1 };

            //* When
            let result = MyStruct::new(config);

            //* Then
            assert!(result.is_err(), "creation with negative value should fail");
            let error = result.expect_err("should return validation error");
            assert!(matches!(error, MyError::ValidationError(_)),
                "Expected ValidationError, got {:?}", error);
        }

        #[tokio::test]
        async fn fetch_with_network_error_fails_gracefully() {
            //* Given
            let mock_client = MockNetworkClient::new()
                .with_failure(NetworkError::ConnectionTimeout);

            //* When
            let result = fetch_with_retry("https://api.example.com", &mock_client).await;

            //* Then
            assert!(result.is_err(), "network operation should fail");
            let error = result.expect_err("should return network error");
            assert!(matches!(error, MyError::NetworkError(NetworkError::ConnectionTimeout)),
                "Expected ConnectionTimeout, got {:?}", error);
        }
    }
}
```

### Database Integration Testing Pattern

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::temp::temp_metadata_db;
    use std::sync::{Arc, Mutex};

    mod it_database_operations {
        use super::*;

        #[tokio::test]
        async fn insert_and_retrieve_record_succeeds() {
            //* Given
            let db = temp_metadata_db().await;
            let test_data = TestRecord {
                name: "test_record".to_string(),
                value: 42,
            };

            //* When
            let insert_result = insert_record(&db.pool, &test_data).await;

            //* Then
            assert!(insert_result.is_ok(), "record insertion should succeed");
            let record_id = insert_result.expect("should return valid record ID");

            let retrieved = get_record_by_id(&db.pool, record_id).await
                .expect("should retrieve inserted record")
                .expect("record should exist");
            assert_eq!(retrieved.name, test_data.name);
            assert_eq!(retrieved.value, test_data.value);
        }

        #[tokio::test]
        async fn transaction_rollback_on_failure_works() {
            //* Given
            let db = temp_metadata_db().await;

            //* When
            let result = perform_transaction_with_failure(&db.pool).await;

            //* Then
            assert!(result.is_err(), "transaction should fail as expected");

            // Verify rollback occurred
            let count = count_records(&db.pool).await
                .expect("should be able to count records");
            assert_eq!(count, 0, "no records should remain after rollback");
        }
    }
}
```

### Public API Integration Testing Pattern

```rust
// tests/it_api_integration.rs
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
async fn concurrent_worker_operations_maintain_consistency() {
    //* Given
    let db = temp_metadata_db().await;
    let worker_ids: Vec<_> = (0..5)
        .map(|i| WorkerNodeId::new(format!("worker-{}", i))
            .expect("should create valid worker ID"))
        .collect();

    let registration_tasks: Vec<_> = worker_ids.iter()
        .map(|id| db.register_worker(id))
        .collect();

    //* When
    let results = futures::future::join_all(registration_tasks).await;

    //* Then
    for result in results {
        assert!(result.is_ok(), "concurrent registration should succeed");
    }

    // Verify all workers are registered
    let workers = db.list_workers().await
        .expect("should list all workers");
    assert_eq!(workers.len(), 5, "all workers should be registered");
}
```

## 🎯 ASSERTION PATTERNS

### Rust-specific Assertions

```rust
fn assertions() {
    // Use descriptive assertion messages
    assert_eq!(actual, expected, "values should be equal");
    assert_ne!(actual, unexpected, "values should be different");
    assert!(condition, "condition should be true");
    assert!(result.is_ok(), "operation should succeed");
    assert!(result.is_err(), "operation should fail");

    // For Option types
    assert!(option.is_some(), "should contain value");
    assert!(option.is_none(), "should be empty");

    // For custom error types
    let error = result.expect_err("operation should fail with invalid input");
    assert!(matches!(error, MyError::ValidationError(_)), 
        "Expected ValidationError, got {:?}", error);

    // For Result types with expect
    let value = result.expect("operation should succeed with valid input");
}
```

### Testing Complex Data Structures

```rust
#[tokio::test]
async fn process_users_transforms_data_correctly() {
    //* Given
    let input = UserBatch {
        users: vec![
            User { id: 1, name: "Alice".to_string(), age: 30 },
            User { id: 2, name: "Bob".to_string(), age: 25 },
        ],
    };

    //* When
    let result = process_users(input).await
        .expect("user processing should succeed");

    //* Then
    assert_eq!(result.processed_users.len(), 2, "should process both users");

    // Test individual items
    let alice = result.processed_users.iter()
        .find(|u| u.id == 1)
        .expect("Alice should be in processed results");
    assert_eq!(alice.name, "Alice");
    assert!(alice.processed, "Alice should be marked as processed");

    let bob = result.processed_users.iter()
        .find(|u| u.id == 2)
        .expect("Bob should be in processed results");
    assert_eq!(bob.name, "Bob");
    assert!(bob.processed, "Bob should be marked as processed");
}
```

## 🔧 TEST ORGANIZATION PATTERNS

### Group Related Tests by Function Category

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

    mod combinators {
        use super::*;
        // Tests for transformation functions
        #[test]
        fn map_transforms_values_correctly() { /* ... */ }

        #[test]
        fn filter_removes_invalid_items() { /* ... */ }
    }

    mod predicates {
        use super::*;
        // Tests for boolean-returning functions
        #[test]
        fn is_valid_returns_true_for_good_input() { /* ... */ }

        #[test]
        fn is_valid_returns_false_for_bad_input() { /* ... */ }
    }

    mod error_handling {
        use super::*;
        // Tests for error conditions
        #[test]
        fn invalid_input_returns_validation_error() { /* ... */ }
    }

    mod it_integration {
        use super::*;
        use crate::temp::temp_metadata_db;
        // Tests for external integrations
        #[tokio::test]
        async fn database_operations_work_end_to_end() { /* ... */ }
    }
}
```

### Progressive Test Complexity

```rust
mod feature_progression {
    use super::*;

    #[test]
    fn basic_functionality_works() {
        //* Given
        let input = create_basic_input();

        //* When
        let result = basic_function(input);

        //* Then
        assert!(result.is_ok());
    }

    #[test]
    fn with_custom_configuration_works() {
        //* Given
        let config = CustomConfig { option: true };

        //* When
        let result = function_with_config(config);

        //* Then
        assert_eq!(result, expected_configured_value);
    }

    #[tokio::test]
    async fn with_error_handling_works() {
        //* Given
        let invalid_input = create_invalid_input();

        //* When
        let result = async_function(invalid_input).await;

        //* Then
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn with_external_dependencies_works() {
        //* Given
        let db = temp_metadata_db().await;
        let test_data = create_test_data();

        //* When
        let result = function_with_db(&db.pool, test_data).await;

        //* Then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn full_integration_workflow_succeeds() {
        //* Given
        let db = temp_metadata_db().await;
        let complete_workflow_data = create_workflow_data();

        //* When
        let result = complete_workflow(&db, complete_workflow_data).await;

        //* Then
        assert!(result.is_ok());
        verify_workflow_completion(&db).await;
    }
}
```

This comprehensive testing approach ensures reliable, maintainable test suites that properly validate Rust code while
maintaining the three-tier testing strategy and avoiding common pitfalls and anti-patterns.
