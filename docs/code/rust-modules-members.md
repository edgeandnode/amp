---
name: "rust-modules-members"
description: "Module member ordering patterns for readability and navigation. Load when organizing module contents or reviewing code structure"
type: core
scope: "global"
---

# Module Member Ordering

**🚨 MANDATORY for ALL Rust code in the Amp project**

## 🎯 PURPOSE

This document establishes consistent ordering of module members for the Amp codebase, ensuring:

- **Readability** - Main API surface is immediately visible
- **Navigation** - Predictable location for different types of code
- **Maintainability** - Consistent structure across all modules

## 📋 MEMBER ORDERING

### Correct Order

**ALWAYS** organize module members in this order:

1. **Imports** (`use` statements) - see import ordering below
2. **Constants and statics** (`const`, `static`)
3. **Type aliases** (`type Foo = ...`)
4. **Main module members** - Public types, main functions (e.g., `run`, `execute`, `new`)
5. **Helper types and functions** - In dependency order (if A depends on B, then A comes first, then B)

### Import Statement Ordering

**ALWAYS** organize imports in separate groups in this order:

1. **`std` imports** - Standard library
2. **Third-party imports** - External crates
3. **`super` and `crate` imports** - Local project imports

```rust
// ✅ CORRECT - Imports in proper groups
use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::args::GlobalArgs;
use crate::client;
```

```rust
// ❌ WRONG - Mixed import groups
use crate::client;
use std::collections::HashMap;
use serde::Serialize;
use crate::args::GlobalArgs;
use std::sync::Arc;
```

### Common Violations

- Main public function (`run`, `main`, `execute`) buried after helper functions
- Public error types at the end of the file instead of near the top
- Helper structs/functions appearing before the main types they support
- Private implementation details scattered before public API

### Examples

```rust
// ❌ WRONG - Helper before main function
struct HelperResult { ... }
fn helper_function() { ... }
pub async fn run() { ... }  // Main function should be first
pub enum Error { ... }      // Public type should be near top
```

```rust
// ✅ CORRECT - Main members first, then helpers in dependency order
pub async fn run() { ... }  // Main function first
pub enum Error { ... }      // Public types early

struct HelperResult { ... } // Helper types after
fn helper_function() { ... } // Helper functions after
```

### Complete Module Example

```rust
//! Module documentation explaining purpose.

use std::collections::HashMap;

use tokio::sync::mpsc;

use crate::client;

const DEFAULT_TIMEOUT: u64 = 30;

type Result<T> = std::result::Result<T, Error>;

/// Main entry point for this module.
pub async fn run(args: Args) -> Result<()> {
    let data = fetch_data(&args).await?;
    process(data)
}

/// Errors for this module's operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("fetch failed")]
    FetchFailed(#[source] client::Error),
}

/// Command-line arguments.
#[derive(Debug, clap::Args)]
pub struct Args {
    pub id: i64,
}

// --- Private helpers below ---

async fn fetch_data(args: &Args) -> Result<Data> {
    // ...
}

fn process(data: Data) -> Result<()> {
    // ...
}

struct Data {
    // ...
}
```

## 🚨 CHECKLIST

Before committing Rust code, verify:

### Import Ordering

- [ ] `std` imports first
- [ ] Third-party crate imports second
- [ ] `super` and `crate` imports last
- [ ] Blank lines separating each group

### Module Member Ordering

- [ ] Main public function (`run`, `execute`, etc.) appears early in the file
- [ ] Public error types appear near the top, after main functions
- [ ] Public structs/types appear before private helpers
- [ ] Helper functions appear after the code that uses them
- [ ] No private implementation details scattered before public API

## 🎓 RATIONALE

These patterns prioritize:

1. **API-First Reading** - Readers see the public interface immediately
2. **Dependency Order** - Code flows top-to-bottom following call hierarchy
3. **Consistent Navigation** - Predictable structure across all modules
4. **Review Efficiency** - Reviewers can quickly find main logic
