---
name: "pattern-typestate"
description: "Typestate pattern — model state machines with distinct types to enforce valid transitions at compile time. Load when designing workflows, pipelines, or objects with lifecycle states"
type: core
scope: "global"
---

# Typestate Pattern (State Machines with Types)

**MANDATORY for ALL Rust code in the workspace**

## Rule

Use distinct types for each state to prevent invalid transitions at compile time. When an object has a lifecycle (created → started → completed), each phase should be a separate type so that only valid operations are available in each state.

If a struct uses a status enum and runtime assertions to guard transitions, invalid transitions are only caught at runtime. Replace the enum with distinct types that consume `self` on transition, making invalid transitions a compile error.

## Examples

```rust
// Bad — runtime state checking, panics on invalid transition
pub struct Job {
    status: JobStatus,
    // ...
}

impl Job {
    pub fn start(&mut self) {
        assert_eq!(self.status, JobStatus::Scheduled);  // Runtime panic!
        self.status = JobStatus::Running;
    }

    pub fn complete(&mut self) {
        assert_eq!(self.status, JobStatus::Running);  // Runtime panic!
        self.status = JobStatus::Completed;
    }
}

// Nothing prevents calling complete() on a Scheduled job at compile time
```

```rust
// Good — type system enforces valid state transitions
pub struct ScheduledJob {
    id: JobId,
    config: JobConfig,
}

pub struct RunningJob {
    id: JobId,
    config: JobConfig,
    started_at: Instant,
}

pub struct CompletedJob {
    id: JobId,
    config: JobConfig,
    started_at: Instant,
    completed_at: Instant,
}

impl ScheduledJob {
    pub fn start(self) -> RunningJob {
        RunningJob {
            id: self.id,
            config: self.config,
            started_at: Instant::now(),
        }
    }
}

impl RunningJob {
    pub fn complete(self) -> CompletedJob {
        CompletedJob {
            id: self.id,
            config: self.config,
            started_at: self.started_at,
            completed_at: Instant::now(),
        }
    }
}

// Usage:
let job = ScheduledJob::new(id, config);
let job = job.start();       // ScheduledJob -> RunningJob
let job = job.complete();    // RunningJob -> CompletedJob
// job.start();              // Compile error — CompletedJob has no start()
```

```rust
// Good — typestate with shared data via a generic parameter
pub struct Job<S> {
    id: JobId,
    config: JobConfig,
    state: S,
}

pub struct Scheduled;
pub struct Running { started_at: Instant }
pub struct Completed { started_at: Instant, completed_at: Instant }

impl Job<Scheduled> {
    pub fn start(self) -> Job<Running> {
        Job {
            id: self.id,
            config: self.config,
            state: Running { started_at: Instant::now() },
        }
    }
}

impl Job<Running> {
    pub fn complete(self) -> Job<Completed> {
        Job {
            id: self.id,
            config: self.config,
            state: Completed {
                started_at: self.state.started_at,
                completed_at: Instant::now(),
            },
        }
    }
}
```

## Why It Matters

Runtime state assertions are invisible to the compiler — they only fail when the wrong code path is executed, which might only happen in production under specific conditions. Typestate makes invalid transitions a compile error, eliminating an entire class of logic bugs. The type signature documents which operations are valid in each state, serving as both enforcement and documentation.

## Pragmatism Caveat

Not every stateful object needs the typestate pattern. If an object has only two states or its transitions are simple and well-tested, a status enum with clear documentation may be simpler and sufficient. Apply typestate when invalid transitions would cause serious bugs, when the state machine is complex enough that runtime assertions are easy to forget, or when the API is consumed by multiple callers who might not know the correct transition order. For objects stored in collections or databases (where a single concrete type is needed), a status enum is often the practical choice — typestate works best for in-memory, linear workflows.

## Checklist

Before committing code, verify:

- [ ] State transitions consume `self` (move semantics) to prevent reuse of the old state
- [ ] Each state type only exposes operations valid for that state
- [ ] No runtime assertions (`assert!`, `panic!`) for state validity that the type system could enforce
- [ ] State-specific data is only present in the types where it exists (e.g., `started_at` only in `Running`)
- [ ] Simple two-state objects or database-stored entities use status enums when typestate adds unnecessary complexity

## References

- [principle-type-driven-design](principle-type-driven-design.md) - Foundation: Design principle this pattern implements
- [pattern-builder](pattern-builder.md) - Related: Builder pattern can use typestate for compile-time required field enforcement
