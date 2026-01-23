---
name: "rust-modules"
description: "Modern module organization without mod.rs, file structure patterns. Load when creating modules or organizing Rust code"
type: core
scope: "global"
---

# Rust Module Organization

**🚨 MANDATORY for ALL Rust code in the Amp project**

## 🎯 PURPOSE

This document establishes modern module organization patterns for the Amp codebase, ensuring:

- **Modern module organization** - Clear, maintainable file structure
- **Edition 2018+ conventions** - No `mod.rs` files
- **Consistent file structure** - Predictable module layout

## 📁 MODULE ORGANIZATION

### 1. Never Use `mod.rs`

**DO NOT** use `mod.rs` files for module organization. Use named module files next to directories instead.

**Why?** Modern Rust (Edition 2018+) provides a clearer module system where the module name matches the file/directory name, improving navigation and reducing ambiguity.

```rust
// ❌ WRONG - Old-style mod.rs pattern
src/
  workers/
    mod.rs         // Contains worker module code and sub-module declarations
    node_id.rs
    heartbeat.rs

// ✅ CORRECT - Modern named module pattern
src/
  workers.rs       // Contains worker module code and sub-module declarations
  workers/
    node_id.rs
    heartbeat.rs
```

### 2. Module File Structure Pattern

**ALWAYS** use this structure for modules with sub-modules:

```
src/
  module_name.rs          # Main module file with pub use exports and mod declarations
  module_name/            # Directory containing sub-modules
    submodule_a.rs
    submodule_b.rs
```

**Main module file pattern:**

```rust
// In src/workers.rs

// Declare sub-modules
mod node_id;
mod heartbeat;

// Re-export public types for convenience
pub use node_id::{NodeId, NodeIdOwned};
pub use heartbeat::Heartbeat;

// Main module functionality
pub struct Worker {
    // ...
}

impl Worker {
    // ...
}
```

**Sub-module files:**

```rust
// In src/workers/node_id.rs

/// Worker node identifier
#[derive(Debug, Clone)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: String) -> Self {
        Self(id)
    }
}
```

## 🚨 CHECKLIST

Before committing Rust code, verify:

### Module Organization

- [ ] No `mod.rs` files exist in the module structure
- [ ] Named module files (e.g., `workers.rs`) used next to directories
- [ ] Sub-modules declared with `mod` in parent module file
- [ ] Public types re-exported with `pub use` for convenience
- [ ] Directory structure matches module hierarchy

## 🎓 RATIONALE

These patterns prioritize:

1. **Rust 2018+ Idioms** - Following modern Rust conventions
2. **Clarity** - Module names match file/directory names exactly
3. **Navigation** - Easy to find module definitions
4. **Maintainability** - Consistent patterns across the codebase
