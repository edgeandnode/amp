---
name: "rust-types"
description: "Type-driven design, newtypes, state machines, builder pattern. Load when designing APIs or data types"
type: core
scope: "global"
---

# Rust Type-Driven Design

**🚨 MANDATORY for ALL Rust code in the Amp project**

## 🎯 PURPOSE

This document establishes type-driven design patterns for the Amp codebase, ensuring:

- **Type-driven design** - Leveraging Rust's type system for correctness
- **Code reliability** - Provable correctness and defensive programming
- **Invalid states unrepresentable** - Compile-time guarantees over runtime checks

## TYPE-DRIVEN DESIGN PATTERNS

**ALWAYS** design APIs that make invalid states unrepresentable. Use the type system to enforce invariants rather than runtime checks.

### Pattern 1: Newtype Wrappers for Validated Data

```rust
// ❌ WRONG - Passing unvalidated strings everywhere
pub fn register_worker(node_id: String) -> Result<(), Error> {
    if !is_valid_node_id(&node_id) {
        return Err(Error::InvalidNodeId);
    }
    // ... use node_id (but could be called with invalid data)
}

// ✅ CORRECT - Type enforces validity
pub struct NodeId(String);

impl NodeId {
    /// Create a validated NodeId
    pub fn new(id: String) -> Result<Self, ValidationError> {
        if !is_valid_node_id(&id) {
            return Err(ValidationError::InvalidNodeId);
        }
        Ok(Self(id))
    }
}

pub fn register_worker(node_id: NodeId) -> Result<(), Error> {
    // Type system guarantees node_id is valid - no runtime checks needed
    // ...
}
```

### Pattern 2: State Machines with Types

Use distinct types for each state to prevent invalid transitions at compile time.

```rust
// ❌ WRONG - Runtime state checking
pub struct Job {
    status: JobStatus,
}

impl Job {
    pub fn start(&mut self) {
        assert_eq!(self.status, JobStatus::Scheduled);  // Runtime panic!
        self.status = JobStatus::Running;
    }
}

// ✅ CORRECT - Type system enforces valid state transitions
pub struct ScheduledJob { /* ... */ }
pub struct RunningJob { /* ... */ }

impl ScheduledJob {
    pub fn start(self) -> RunningJob {
        RunningJob { /* ... */ }  // Type transition - cannot call on wrong state
    }
}

// Usage
let job = ScheduledJob::new();
let job = job.start();  // ✅ Can only call start() on ScheduledJob
// job.start();  // ❌ Compile error - already consumed
```

### Pattern 3: Builder Pattern for Required Fields

Use builder pattern when construction has multiple required fields.

```rust
// ❌ WRONG - Easy to forget required fields
pub struct Config {
    pub database_url: Option<String>,
    pub port: Option<u16>,
}

// ✅ CORRECT - Builder enforces completeness
pub struct Config {
    database_url: String,  // No Option - guaranteed to exist
    port: u16,
}

impl ConfigBuilder {
    pub fn database_url(mut self, url: String) -> Self { /* ... */ }
    pub fn port(mut self, port: u16) -> Self { /* ... */ }

    pub fn build(self) -> Result<Config, BuildError> {
        Ok(Config {
            database_url: self.database_url.ok_or(BuildError::MissingDatabaseUrl)?,
            port: self.port.ok_or(BuildError::MissingPort)?,
        })
    }
}

// config.database_url is String, not Option<String> - no unwrapping needed!
```

## 🚨 CHECKLIST

Before committing Rust code, verify:

### Type-Driven Design

- [ ] Newtypes used for validated data (no bare primitives for domain concepts)
- [ ] Invalid states made unrepresentable through types
- [ ] Type system used to enforce invariants (not runtime checks)
- [ ] Builder pattern used for complex object construction
- [ ] State machines modeled with distinct types (not enums with runtime checks)

### Code Quality

- [ ] Types encode business logic and invariants
- [ ] API signatures prevent misuse at compile time
- [ ] No unnecessary runtime validation for type-guaranteed invariants

## 🎓 RATIONALE

These patterns prioritize:

1. **Type-Driven Design** - Use the type system to prevent entire classes of errors at compile time
2. **Compile-Time Guarantees** - Leverage Rust's type system to catch bugs before runtime
3. **API Safety** - Make it impossible to misuse APIs through clever type design
4. **Maintainability** - Self-documenting code where types encode invariants

**Remember**: The more you can express in types, the fewer runtime checks you need, and the more confident you can be in your code's correctness.
