---
name: "test-functions"
description: "Test naming conventions, function structure, Given-When-Then, async tests, assertions, forbidden patterns. Load when writing or reviewing test functions"
type: core
scope: "global"
---

# Test Functions - Naming and Structure

**🚨 MANDATORY patterns for writing individual test functions in Rust**

## 🎯 PURPOSE

This document covers the essential patterns for writing a single test function: naming conventions, framework selection, Given-When-Then structure, assertions, and forbidden patterns.

For test file organization (where tests go in the directory structure), see [Test File Organization](test-files.md).

For understanding the three-tier testing strategy, see [Test Strategy](test-strategy.md).

---

## 📝 TEST NAMING CONVENTIONS

**Test naming conventions MUST appear first because they are the first decision when writing a test.**

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

## 🚨 TESTING FRAMEWORK SELECTION

### ✅ Use standard `#[test]` for synchronous functions

```rust
#[test]
fn pure_function_with_valid_input_returns_expected_value() {
    //* Given
    let input = create_test_input();

    //* When
    let result = pure_function(input);

    //* Then
    assert_eq!(result, expected_value);
}
```

### ✅ Use `#[tokio::test]` for async functions

```rust
#[tokio::test]
async fn async_function_with_test_data_succeeds() {
    //* Given
    let input_data = setup_test_data();

    //* When
    let result = some_async_function(input_data).await;

    //* Then
    assert_eq!(result, expected_value);
}
```

---

## 📝 GIVEN-WHEN-THEN STRUCTURE (MANDATORY)

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

---

## ❌ FORBIDDEN PATTERNS

### Never use `unwrap()` in tests

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

### Never test multiple functions in one test

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

---

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

---

## 🧪 COMPLETE EXAMPLES

### Unit Test Example

```rust
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
```

### Async Test Example

```rust
#[tokio::test]
async fn insert_record_with_valid_data_returns_record_id() {
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
    assert!(record_id > 0, "should return positive record ID");
}
```

### Error Handling Test Example

```rust
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
```

---

## CHECKLIST

Before submitting a test function for review, verify:

- [ ] Test name follows `<function_name>_<scenario>_<expected_outcome>` format
- [ ] Test name does NOT include the word "test" (it's already marked with `#[test]`)
- [ ] Test uses correct framework: `#[test]` for sync, `#[tokio::test]` for async
- [ ] Test has `//* Given`, `//* When`, and `//* Then` comments (Given optional if no setup needed)
- [ ] `//* When` section calls EXACTLY ONE function under test
- [ ] `//* Then` section contains ONLY assertions and assertion helpers
- [ ] No `unwrap()` calls - all use `.expect("descriptive message")` instead
- [ ] All assertions have descriptive failure messages
- [ ] Test focuses on a single scenario (not testing multiple functions or workflows)
- [ ] Test name is descriptive and explains what is being tested

## RATIONALE

### Why descriptive naming matters

Test names serve as documentation. When a test fails in CI, the name should immediately communicate what functionality broke and under what conditions. Generic names like `test_insert()` or `insert_works()` provide no context, forcing developers to read the entire test body to understand what failed.

The `<function_name>_<scenario>_<expected_outcome>` pattern ensures every test name answers three critical questions:
1. **What** is being tested? (function_name)
2. **Under what conditions**? (scenario)
3. **What should happen**? (expected_outcome)

### Why Given-When-Then structure is mandatory

The Given-When-Then structure enforces the Arrange-Act-Assert testing pattern, which is the foundation of readable and maintainable tests. The mandatory comment markers (`//* Given`, `//* When`, `//* Then`) make test structure immediately visible, even when scanning code quickly.

This pattern prevents common anti-patterns:
- Testing multiple functions in one test (violates single responsibility)
- Mixing setup, execution, and assertion logic (hard to debug)
- Unclear test boundaries (makes refactoring risky)

The `//* When` section's "exactly one function" rule is critical: it ensures tests have clear failure attribution. If a test calls multiple functions, it's impossible to know which function caused a failure without debugging.

### Why .expect() instead of .unwrap()

Tests should never panic without explanation. When `.unwrap()` panics, the error message is generic: "called `Result::unwrap()` on an `Err` value". This provides no context about what failed or why.

Using `.expect("descriptive message")` transforms panic messages into actionable debugging information: "risky_operation should succeed with valid input: Err(ValidationError(EmptyInput))". The developer immediately knows what was expected and what actually happened.

### Why no business logic in assertions

The `//* Then` section must contain only assertions because any business logic in this section obscures what's actually being verified. If you need to transform data before asserting on it, that transformation belongs in `//* Given` (setup) or indicates the test is verifying the wrong thing.

Example: If you're testing `get_user()` but then uppercase the returned name to compare against an expected value, you're actually testing two things: (1) get_user returns the right record, and (2) name uppercasing logic works. Split this into two tests.

---

This document covers all essential patterns for authoring individual test functions. For test file organization patterns, see [Test File Organization](test-files.md). For understanding the overall testing strategy, see [Test Strategy](test-strategy.md).
