---
name: "errors-handling"
description: "Error handling patterns, unwrap/expect prohibition, pattern matching. Load when handling errors or dealing with Result/Option types"
type: core
scope: "global"
---

# Rust Error Handling Patterns

**🚨 MANDATORY for ALL Rust code in the Amp project**

## 🎯 PURPOSE

This document establishes critical error handling standards for the Amp codebase, ensuring:

- **Safe error handling** - Explicit error paths without panics
- **Production reliability** - No unexpected crashes
- **Clear error flows** - Explicit handling of all failure cases

## 🔥 ERROR HANDLING - CRITICAL

### 1. 🔥 NEVER Use `.unwrap()` or `.expect()` in Production

**🚨 ABSOLUTELY CRITICAL - ZERO TOLERANCE POLICY**

**DO NOT** use `.unwrap()` or `.expect()` in production code paths unless you can prove the operation cannot fail.

**Why This Is Critical:**

- **Production panics are unacceptable** - They crash the entire process, potentially losing data and disrupting service
- **No stack unwinding guarantees** - Panics bypass destructors and cleanup code
- **Lost context** - Panic messages provide minimal debugging information compared to proper error handling
- **Distributed system failure** - In distributed systems like Amp, a panic in one component can cascade into system-wide failures
- **Data corruption risk** - Panics during database transactions can leave data in inconsistent states

```rust
// ❌ WRONG - Unwrapping without proof of safety
pub fn get_config(path: &str) -> Config {
    let contents = std::fs::read_to_string(path).unwrap();  // 🚨 CRITICAL VIOLATION - File might not exist!
    serde_json::from_str(&contents).unwrap()  // 🚨 CRITICAL VIOLATION - JSON might be invalid!
}

// ❌ WRONG - Generic expect message doesn't make it acceptable
pub fn get_config(path: &str) -> Config {
    let contents = std::fs::read_to_string(path)
        .expect("failed to read config");  // 🚨 STILL WRONG - will panic in production
    serde_json::from_str(&contents)
        .expect("failed to parse config")  // 🚨 STILL WRONG - will panic in production
}

// ✅ CORRECT - Explicit error handling with proper types
pub fn get_config(path: &str) -> Result<Config, ConfigError> {
    let contents = std::fs::read_to_string(path)
        .map_err(ConfigError::ReadFailed)?;

    let config = serde_json::from_str(&contents)
        .map_err(ConfigError::ParseFailed)?;

    Ok(config)
}
```

**🚨 Code Review Red Flags:**

Any occurrence of `.unwrap()` or `.expect()` in production code paths should trigger immediate rejection unless accompanied by:

1. **Proof of safety** - Logical analysis or type-system guarantee proving the operation cannot fail
2. **`# Panics` documentation** - Rustdoc documenting when and why panics can occur
3. **`// SAFETY:` comment** - Explanation of why this specific unwrap is safe

**Even then, prefer refactoring to eliminate the unwrap entirely.**

### 2. Prefer Pattern Matching

**ALWAYS** use explicit pattern matching over unwrapping. The type system is your ally - use it.

#### Pattern 1: `let-else` Statement (Preferred for Early Returns)

```rust
// ✅ CORRECT - let-else for early return with clear error
pub fn process_job(job_id: Option<JobId>) -> Result<(), Error> {
    let Some(id) = job_id else {
        return Err(Error::MissingJobId);
    };

    // Continue with id - type system guarantees it exists
    execute_job(id)
}

// ✅ CORRECT - let-else with complex unwrapping
pub async fn get_manifest(&self, hash: &Hash) -> Result<Manifest, Error> {
    let Some(path) = self.metadata_db
        .get_manifest_path(hash)
        .await?
    else {
        return Err(Error::ManifestNotFound { hash: hash.clone() });
    };

    self.store.retrieve(path).await
}
```

#### Pattern 2: `match` Statement (For Multiple Cases)

```rust
// ✅ CORRECT - match for explicit, exhaustive handling
pub fn handle_result(result: Result<String, DbError>) -> Response {
    match result {
        Ok(data) => Response::success(data),
        Err(DbError::NotFound) => Response::not_found(),
        Err(DbError::ConnectionFailed) => Response::retry_later(),
        Err(err) => Response::error(err),
    }
}

// ✅ CORRECT - match for Option with clear semantics
pub fn get_worker_status(id: &WorkerId) -> WorkerStatus {
    match self.workers.get(id) {
        Some(worker) => worker.status(),
        None => WorkerStatus::Unknown,
    }
}
```

#### Pattern 3: `if let` (For Single Case)

```rust
// ✅ CORRECT - if let for single case with side effects
pub fn maybe_log_error(result: Result<(), Error>) {
    if let Err(err) = result {
        tracing::error!(
            error = %err,
            error_source = logging::error_source(&err),
            "operation failed"
        );
    }
}

// ✅ CORRECT - if let for conditional execution
pub fn apply_migration(migration: Option<Migration>) {
    if let Some(m) = migration {
        m.apply();
    }
}
```

#### Pattern 4: Combinators (For Transformation Chains)

```rust
// ✅ CORRECT - Using combinators for transformation
pub fn get_worker_name(id: &WorkerId) -> String {
    self.workers
        .get(id)
        .map(|w| w.name.clone())
        .unwrap_or_else(|| format!("unknown-{}", id))
}

// ✅ CORRECT - ok_or for conversion with context
pub fn require_config(config: Option<Config>) -> Result<Config, Error> {
    config.ok_or(Error::ConfigMissing)
}

// ✅ CORRECT - and_then for chaining fallible operations
pub fn load_and_parse(path: &str) -> Result<Config, Error> {
    read_file(path)
        .and_then(|contents| parse_config(&contents))
}
```

### 3. Test Code Exception

**EXCEPTION**: `.expect()` with descriptive messages is **acceptable and recommended in test code**.

**Why?** Tests should fail loudly and clearly when preconditions aren't met. Test panics are expected and help identify issues quickly.

```rust
// ✅ CORRECT - expect in tests with descriptive messages
#[tokio::test]
async fn test_job_creation_with_valid_data_succeeds() {
    //* Given
    let db = temp_metadata_db().await;
    let job_data = create_test_job_data();

    //* When
    let job_id = create_job(&db, job_data)
        .await
        .expect("job creation should succeed with valid data");

    //* Then
    let retrieved = get_job_by_id(&db, job_id)
        .await
        .expect("should retrieve created job")
        .expect("job should exist in database");

    assert_eq!(retrieved.id, job_id);
}

// ❌ WRONG - unwrap in tests (no context for failure)
#[tokio::test]
async fn test_job_creation() {
    let db = temp_metadata_db().await;
    let job_id = create_job(&db, job_data).await.unwrap();  // Which step failed?
    let retrieved = get_job_by_id(&db, job_id).await.unwrap().unwrap();
    assert_eq!(retrieved.id, job_id);
}
```

**Test `.expect()` guidelines:**

- ✅ Always use `.expect()` with descriptive messages in test code
- ✅ Message format: `"<operation> should <expected behavior>"`
- ✅ Helps identify which precondition failed when tests break
- ❌ Never use `.unwrap()` even in tests - always prefer `.expect()` with context

## 🚨 CHECKLIST

Before committing Rust code, verify:

### Error Handling - CRITICAL

- [ ] 🔥 **ZERO `.unwrap()` calls in production code paths**
- [ ] 🔥 **ZERO `.expect()` calls in production code (except provably safe with documentation)**
- [ ] Pattern matching used for all `Result` and `Option` handling
- [ ] `let-else` used for early returns from `Option`
- [ ] `match` used for explicit multi-branch handling
- [ ] `if let` used for single-case handling
- [ ] Combinators (`.map()`, `.ok_or()`, `.and_then()`) used appropriately
- [ ] Test code uses `.expect()` with descriptive messages (NOT `.unwrap()`)
- [ ] All unwrap/expect uses in production code have:
  - [ ] `# Panics` rustdoc section
  - [ ] `// SAFETY:` comment explaining why it's safe
  - [ ] Logical analysis or type-system proof of safety

### Code Quality

- [ ] Functions return `Result<T, E>` for all fallible operations
- [ ] Error types provide rich context (see `errors-reporting.md`)
- [ ] No panic-inducing code without documentation and proof

## 🎓 RATIONALE

These patterns prioritize:

1. **Safety First** - Production code must never panic unexpectedly
2. **Clarity** - Explicit error handling makes code paths visible and maintainable
3. **Production Quality** - Code that handles errors gracefully and provides rich debugging context
4. **Test Clarity** - Tests that fail clearly with descriptive messages

**Remember**: Every `.unwrap()` or `.expect()` in production code is a potential crash waiting to happen.

## References
- [errors-reporting](errors-reporting.md) - Related: Error type design patterns
