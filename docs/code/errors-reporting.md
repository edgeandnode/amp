---
name: "errors-reporting"
description: "Error handling patterns, thiserror usage, error context. Load when defining errors or handling error types"
type: core
scope: "global"
---

# Error Reporting Patterns

**🚨 MANDATORY for ALL error handling in the Amp project**

## 📑 TABLE OF CONTENTS

1. [Purpose](#-purpose)
2. [Core Principles](#-core-principles)
   - [1. Use `thiserror::Error` Derive Macro](#1-use-thiserrorerror-derive-macro)
   - [2. Choose Error Type Structure Based on Error Sources](#2-choose-error-type-structure-based-on-error-sources)
   - [3. Error Variant Forms](#3-error-variant-forms)
   - [4. Wrap Source Errors to Provide Context](#4-wrap-source-errors-to-provide-context)
   - [5. Avoid `#[from]` Attribute and `From` Implementations](#5-avoid-from-attribute-and-from-implementations)
   - [6. Always Use `#[source]` Attribute](#6-always-use-source-attribute)
   - [7. Closure Parameter Naming Convention](#7-closure-parameter-naming-convention)
   - [8. One Variant Per Error Source](#8-one-variant-per-error-source)
   - [9. One Error Enum Per Fallible Function](#9-one-error-enum-per-fallible-function)
   - [10. No Unused Error Variants](#10-no-unused-error-variants)
   - [11. Error Documentation Template](#11-error-documentation-template)
   - [12. Avoid `BoxError` and `Box<dyn Error>`](#12-avoid-boxerror-and-boxdyn-error)
3. [Complete Example](#-complete-example)
4. [Checklist](#-checklist)
5. [Rationale](#-rationale)

## 🎯 PURPOSE

This document establishes consistent, production-grade error reporting patterns across the entire Amp codebase. These patterns ensure:

- **Explicit error propagation** - Clear visibility of where errors originate and are transformed
- **Rich error context** - Detailed information for debugging and user-facing error messages
- **Type-safe error handling** - Leverage Rust's type system to prevent error handling mistakes
- **Error chain preservation** - Maintain full error causality via `std::error::Error::source()`

## 📐 CORE PRINCIPLES

### 1. Use `thiserror::Error` Derive Macro

**ALWAYS** use the fully qualified form `#[derive(Debug, thiserror::Error)]` to avoid name clashes with user-defined `Error` types.

```rust
// ✅ CORRECT - Fully qualified form
#[derive(Debug, thiserror::Error)]
pub enum MyOperationError {
    // ...
}

// ❌ WRONG - May clash with custom Error types
use thiserror::Error;
#[derive(Debug, Error)]
pub enum Error {
    // ...
}
```

### 2. Choose Error Type Structure Based on Error Sources

#### Enums: Multiple Error Sources

Use **enums** when an operation has multiple distinct error sources or failure modes.

```rust
/// Errors specific to manifest registration operations
#[derive(Debug, thiserror::Error)]
pub enum RegisterManifestError {
    /// Failed to store manifest in dataset definitions store
    #[error("Failed to store manifest in dataset definitions store")]
    ManifestStorage(#[source] StoreError),

    /// Failed to register manifest in metadata database
    #[error("Failed to register manifest in metadata database")]
    MetadataRegistration(#[source] metadata_db::Error),
}
```

#### Structs: Single Error Source

Use **structs** when wrapping a single underlying error type or when there's only one error source.

```rust
/// Error when resolving revision references to manifest hashes
#[derive(Debug, thiserror::Error)]
#[error("Failed to query metadata database")]
pub struct ResolveRevisionError(#[source] pub metadata_db::Error);
```

### 3. Error Variant Forms

#### Tuple Form: Single Field (Default)

**ALWAYS** use tuple form when an error variant has a single field, unless explicitly required to use named fields.

```rust
// ✅ CORRECT - Tuple form for single source error
#[derive(Debug, thiserror::Error)]
pub enum GetManifestError {
    #[error("Failed to query manifest path from metadata database")]
    MetadataDbQueryPath(#[source] metadata_db::Error),

    #[error("Failed to retrieve manifest from object store")]
    ObjectStoreError(#[source] crate::manifests::GetError),
}
```

#### Named Fields: Multiple Fields or Context

Use named fields when providing additional context alongside the source error.

```rust
// ✅ CORRECT - Named fields for context
#[derive(Debug, thiserror::Error)]
pub enum GetDatasetError {
    #[error("Invalid dataset name '{name}': {source}")]
    InvalidDatasetName {
        name: String,
        source: NameError
    },

    #[error("Failed to get latest version for dataset '{namespace}/{name}': {source}")]
    GetLatestVersion {
        namespace: String,
        name: String,
        source: metadata_db::Error,
    },
}
```

### 4. Wrap Source Errors to Provide Context

**ALWAYS** wrap underlying error types in domain-specific error variants. This provides:
- Clear error origin
- Domain-specific error messages
- Ability to add context
- Type-safe error handling

```rust
// ✅ CORRECT - Wrapping with context
pub async fn delete_manifest(&self, hash: &Hash) -> Result<(), DeleteManifestError> {
    let mut tx = self
        .metadata_db
        .begin_txn()
        .await
        .map_err(DeleteManifestError::TransactionBegin)?;  // Wrapped with context

    let links = metadata_db::manifests::count_dataset_links_and_lock(&mut tx, hash)
        .await
        .map_err(DeleteManifestError::MetadataDbCheckLinks)?;  // Wrapped with context

    // ...
}

// ❌ WRONG - Propagating generic errors without context
pub async fn delete_manifest(&self, hash: &Hash) -> Result<(), metadata_db::Error> {
    let mut tx = self.metadata_db.begin_txn().await?;  // Lost context
    // ...
}
```

### 5. Avoid `#[from]` Attribute and `From` Implementations

**DO NOT** use `#[from]` attribute or manual `From` implementations unless explicitly required.

**Why?** Explicit `.map_err()` calls:
- Show exactly where error wrapping happens
- Make error flow more visible
- Prevent accidental implicit conversions
- Aid debugging and code comprehension

```rust
// ✅ CORRECT - Explicit error mapping
#[derive(Debug, thiserror::Error)]
pub enum MyError {
    #[error("Database operation failed")]
    Database(#[source] metadata_db::Error),  // No #[from]
}

pub async fn my_operation(&self) -> Result<(), MyError> {
    metadata_db::some_operation(&self.db)
        .await
        .map_err(MyError::Database)?;  // Explicit mapping
    Ok(())
}

// ❌ WRONG - Using #[from]
#[derive(Debug, thiserror::Error)]
pub enum MyError {
    #[error("Database operation failed")]
    Database(#[from] metadata_db::Error),  // Implicit conversion
}

pub async fn my_operation(&self) -> Result<(), MyError> {
    metadata_db::some_operation(&self.db).await?;  // Where did wrapping happen?
    Ok(())
}
```

### 6. Always Use `#[source]` Attribute

**MANDATORY**: Use `#[source]` attribute on all wrapped error types to preserve the error chain.

This enables:
- Error chain traversal via `std::error::Error::source()`
- Better debugging with full error causality
- Compatibility with error reporting libraries (e.g., `anyhow`, `eyre`)

```rust
// ✅ CORRECT - Using #[source]
#[derive(Debug, thiserror::Error)]
pub enum LinkManifestError {
    #[error("Failed to begin transaction")]
    TransactionBegin(#[source] metadata_db::Error),  // #[source] preserves chain

    #[error("Failed to link manifest to dataset in metadata database")]
    LinkManifestToDataset(#[source] metadata_db::Error),
}

// ❌ WRONG - Missing #[source]
#[derive(Debug, thiserror::Error)]
pub enum LinkManifestError {
    #[error("Failed to begin transaction")]
    TransactionBegin(metadata_db::Error),  // Error chain broken!
}
```

#### Special Case: Named Field `source`

When using named fields, if the field containing the wrapped error is named `source`, the `#[source]` attribute is **redundant** (but harmless). The `thiserror` crate automatically treats a field named `source` as the error source.

However, if the field has a different name (e.g., `error`, `inner`, `cause`), you **MUST** explicitly annotate it with `#[source]`.

```rust
// ✅ CORRECT - Field named 'source', #[source] is redundant but acceptable
#[derive(Debug, thiserror::Error)]
pub enum MyError {
    #[error("Operation failed: {message}")]
    Failed {
        message: String,
        #[source]  // Redundant but harmless
        source: metadata_db::Error,
    },
}

// ✅ ALSO CORRECT - Field named 'source', #[source] omitted
#[derive(Debug, thiserror::Error)]
pub enum MyError {
    #[error("Operation failed: {message}")]
    Failed {
        message: String,
        source: metadata_db::Error,  // Automatically treated as source
    },
}

// ✅ CORRECT - Field named 'error', #[source] is required
#[derive(Debug, thiserror::Error)]
pub enum MyError {
    #[error("Operation failed: {message}")]
    Failed {
        message: String,
        #[source]  // Required because field is not named 'source'
        error: metadata_db::Error,
    },
}

// ❌ WRONG - Field named 'error' without #[source]
#[derive(Debug, thiserror::Error)]
pub enum MyError {
    #[error("Operation failed: {message}")]
    Failed {
        message: String,
        error: metadata_db::Error,  // Error chain broken!
    },
}
```

**Recommendation:** When using named fields, **prefer naming the error field `source`** to avoid redundancy. If you use a different field name, you **MUST** annotate it with `#[source]`.

### 7. Closure Parameter Naming Convention

**ALWAYS** name the closure parameter in `.map_err()` as `err`, **NEVER** shortened to `e`, unless there's a naming conflict/shadowing.

```rust
// ✅ CORRECT - Full 'err' parameter name
metadata_db::some_operation(&self.db)
    .await
    .map_err(|err| MyError::Database(err))?;

// ✅ CORRECT - Simple case (can omit closure when variant is tuple)
metadata_db::some_operation(&self.db)
    .await
    .map_err(MyError::Database)?;

// ❌ WRONG - Shortened parameter name
metadata_db::some_operation(&self.db)
    .await
    .map_err(|e| MyError::Database(e))?;
```

### 8. One Variant Per Error Source

**NEVER** reuse the same error variant for multiple error sources. Each variant should describe a single, specific error condition.

```rust
// ✅ CORRECT - Distinct variants for different error sources
#[derive(Debug, thiserror::Error)]
pub enum LinkManifestError {
    #[error("Failed to begin transaction")]
    TransactionBegin(#[source] metadata_db::Error),

    #[error("Failed to link manifest to dataset in metadata database")]
    LinkManifestToDataset(#[source] metadata_db::Error),

    #[error("Failed to set dev tag for dataset")]
    SetDevTag(#[source] metadata_db::Error),

    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),
}

// ❌ WRONG - Reusing single variant for multiple sources
#[derive(Debug, thiserror::Error)]
pub enum LinkManifestError {
    #[error("Database error")]
    DatabaseError(#[source] metadata_db::Error),  // Used everywhere - no context!
}
```

### 9. One Error Enum Per Fallible Function

**Prefer** one error type per fallible function or closely related operation. Only reuse error types when functions share **ALL** error variants.

```rust
// ✅ CORRECT - Dedicated error type per operation
pub async fn register_manifest(&self) -> Result<(), RegisterManifestError> {
    // ...
}

pub async fn link_manifest(&self) -> Result<(), LinkManifestError> {
    // ...
}

pub async fn set_version_tag(&self) -> Result<(), SetVersionTagError> {
    // ...
}

// ✅ ACCEPTABLE - Shared error type when ALL variants are common
#[derive(Debug, thiserror::Error)]
pub enum TransactionError {
    #[error("Failed to begin transaction")]
    Begin(#[source] metadata_db::Error),

    #[error("Failed to commit transaction")]
    Commit(#[source] metadata_db::Error),
}

pub async fn operation_a(&self) -> Result<(), TransactionError> {
    // Uses both Begin and Commit
}

pub async fn operation_b(&self) -> Result<(), TransactionError> {
    // Also uses both Begin and Commit
}

// ❌ WRONG - Shared error type with unused variants
#[derive(Debug, thiserror::Error)]
pub enum SharedError {
    #[error("Failed to parse manifest")]
    ManifestParse(#[source] ManifestParseError),  // Used only in operation_a

    #[error("Failed to query database")]
    DatabaseQuery(#[source] metadata_db::Error),  // Used in both

    #[error("Provider not found")]
    ProviderNotFound,  // Used only in operation_b
}
```

### 10. No Unused Error Variants

**MANDATORY**: Every error variant **MUST** be actually used in code. Remove unused variants immediately.

```rust
// ✅ CORRECT - All variants are used
#[derive(Debug, thiserror::Error)]
pub enum GetManifestError {
    #[error("Failed to query manifest path from metadata database")]
    MetadataDbQueryPath(#[source] metadata_db::Error),  // Used in get_manifest()

    #[error("Failed to retrieve manifest from object store")]
    ObjectStoreError(#[source] crate::manifests::GetError),  // Used in get_manifest()
}

// ❌ WRONG - Unused variant
#[derive(Debug, thiserror::Error)]
pub enum GetManifestError {
    #[error("Failed to query manifest path from metadata database")]
    MetadataDbQueryPath(#[source] metadata_db::Error),

    #[error("Failed to retrieve manifest from object store")]
    ObjectStoreError(#[source] crate::manifests::GetError),

    #[error("Manifest not found")]
    NotFound,  // Never constructed anywhere - REMOVE THIS
}
```

### 11. Error Documentation Template

**MANDATORY**: Document each error variant following this template:

```rust
#[derive(Debug, thiserror::Error)]
pub enum MyOperationError {
    /// [Brief description - what this error represents]
    ///
    /// [Detailed explanation of when this error occurs]
    ///
    /// [Optional: Common causes as bullet list]
    /// - [Cause 1]
    /// - [Cause 2]
    /// - [Cause 3]
    ///
    /// [Optional: Additional context like transaction guarantees, retry safety, etc.]
    #[error("[User-facing error message]")]
    VariantName(#[source] UnderlyingError),
}
```

**Example:**

```rust
#[derive(Debug, thiserror::Error)]
pub enum LinkManifestError {
    /// Manifest does not exist in the system
    ///
    /// This occurs when attempting to link a manifest hash that hasn't been registered.
    /// The manifest must be registered first via `register_manifest` before it can be
    /// linked to a dataset.
    ///
    /// This error is detected via foreign key constraint violation (PostgreSQL error code 23503)
    /// when the database rejects the link operation due to the missing manifest.
    #[error("Manifest with hash '{0}' does not exist")]
    ManifestNotFound(Hash),

    /// Failed to commit transaction after successful database operations
    ///
    /// When a commit fails, PostgreSQL guarantees that all changes are rolled back.
    /// None of the operations in the transaction (linking manifest and updating dev tag)
    /// were persisted to the database.
    ///
    /// Possible causes:
    /// - Database connection lost during commit
    /// - Transaction conflict with concurrent operations (serialization failure)
    /// - Database constraint violation detected at commit time
    /// - Database running out of disk space or resources
    ///
    /// The operation is safe to retry from the beginning as no partial state was persisted.
    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),
}
```

### 12. Avoid `BoxError` and `Box<dyn Error>`

**DO NOT** use `BoxError`, `Box<dyn Error>`, or similar type erasure in production code. These are **ONLY** acceptable for rapid prototyping.

**Why?**
- Loses type information
- Prevents exhaustive error matching
- Hides error structure
- Makes error handling less precise

```rust
// ✅ CORRECT - Typed error variants
#[derive(Debug, thiserror::Error)]
pub enum GetDatasetError {
    #[error("Failed to retrieve manifest for dataset '{namespace}/{name}' version '{}'", version.as_deref().unwrap_or("latest"))]
    ManifestRetrievalError {
        namespace: String,
        name: String,
        version: Option<String>,
        source: crate::manifests::GetError,  // Concrete type
    },
}

// ❌ WRONG - Type-erased error (only for prototyping)
use common::BoxError;

#[derive(Debug, thiserror::Error)]
pub enum GetDatasetError {
    #[error("Failed to retrieve manifest")]
    ManifestRetrievalError {
        namespace: String,
        name: String,
        version: Option<String>,
        source: BoxError,  // Type information lost!
    },
}
```

**Exception:** During rapid prototyping or proof-of-concept work, `BoxError` may be used temporarily. However:
- It **MUST** be replaced with concrete types before merging to main
- Add a `TODO` comment indicating replacement is needed
- Track removal in code review

```rust
// 🔶 ACCEPTABLE (temporarily, with TODO)
#[derive(Debug, thiserror::Error)]
pub enum PrototypeError {
    // TODO: Replace BoxError with concrete type before production
    #[error("Generic error")]
    GenericError(BoxError),
}
```

## 📋 COMPLETE EXAMPLE

Putting it all together - a complete, production-grade error handling example:

```rust
use datasets_common::hash::Hash;
use metadata_db;
use crate::manifests::{GetError as ManifestGetError, DeleteError as ManifestDeleteError};

/// Errors that occur when deleting a manifest
///
/// This error type is used by `DatasetStore::delete_manifest()`.
#[derive(Debug, thiserror::Error)]
pub enum DeleteManifestError {
    /// Manifest is linked to one or more datasets and cannot be deleted
    ///
    /// Manifests must be unlinked from all datasets before deletion.
    #[error("Manifest is linked to datasets and cannot be deleted")]
    ManifestLinked,

    /// Failed to begin transaction
    ///
    /// This error occurs when the database connection fails to start a transaction,
    /// typically due to connection issues, database unavailability, or permission problems.
    #[error("Failed to begin transaction")]
    TransactionBegin(#[source] metadata_db::Error),

    /// Failed to check if manifest is linked to datasets
    #[error("Failed to check if manifest is linked to datasets")]
    MetadataDbCheckLinks(#[source] metadata_db::Error),

    /// Failed to delete manifest from metadata database
    #[error("Failed to delete manifest from metadata database")]
    MetadataDbDelete(#[source] metadata_db::Error),

    /// Failed to delete manifest from object store
    #[error("Failed to delete manifest from object store")]
    ObjectStoreError(#[source] ManifestDeleteError),

    /// Failed to commit transaction after successful database operations
    ///
    /// When a commit fails, PostgreSQL guarantees that all changes are rolled back.
    /// The manifest deletion was not persisted to the database.
    ///
    /// Possible causes:
    /// - Database connection lost during commit
    /// - Transaction conflict with concurrent operations (serialization failure)
    /// - Database constraint violation detected at commit time
    /// - Database running out of disk space or resources
    ///
    /// The operation is safe to retry from the beginning as no partial state was persisted.
    #[error("Failed to commit transaction")]
    TransactionCommit(#[source] metadata_db::Error),
}

// Usage in implementation
impl DatasetStore {
    /// Delete a manifest from both metadata database and object store
    ///
    /// Uses transaction with `SELECT FOR UPDATE` to check links before deletion, preventing
    /// concurrent link creation. Returns `ManifestLinked` error if linked to any datasets.
    /// Idempotent (returns `Ok(())` if not found). Deletes from object store before commit.
    pub async fn delete_manifest(&self, hash: &Hash) -> Result<(), DeleteManifestError> {
        // Begin transaction for atomic check-and-delete
        let mut tx = self
            .metadata_db
            .begin_txn()
            .await
            .map_err(DeleteManifestError::TransactionBegin)?;  // ✅ Explicit mapping

        // Check if manifest has remaining links (with row-level locking)
        let links = metadata_db::manifests::count_dataset_links_and_lock(&mut tx, hash)
            .await
            .map_err(DeleteManifestError::MetadataDbCheckLinks)?;  // ✅ Explicit mapping

        if links > 0 {
            return Err(DeleteManifestError::ManifestLinked);  // ✅ Direct variant construction
        }

        // Delete from metadata database (CASCADE deletes links/tags)
        let Some(path) = metadata_db::manifests::delete(&mut tx, hash)
            .await
            .map_err(DeleteManifestError::MetadataDbDelete)?  // ✅ Explicit mapping
            .map(Into::into)
        else {
            return Ok(());  // Idempotent - already deleted
        };

        // Delete manifest file from object store BEFORE committing transaction
        self.dataset_manifests_store
            .delete(path)
            .await
            .map_err(DeleteManifestError::ObjectStoreError)?;  // ✅ Explicit mapping

        // Commit transaction - releases locks
        tx.commit()
            .await
            .map_err(DeleteManifestError::TransactionCommit)?;  // ✅ Explicit mapping

        Ok(())
    }
}
```

## 🚨 CHECKLIST

Before committing error handling code, verify:

- [ ] All error types use `#[derive(Debug, thiserror::Error)]`
- [ ] Enums used for multiple error sources, structs for single sources
- [ ] Tuple form used for single-field variants (unless named fields provide context)
- [ ] All underlying errors are wrapped with domain-specific variants
- [ ] No `#[from]` attributes or `From` implementations (unless explicitly required)
- [ ] All wrapped errors use `#[source]` attribute
- [ ] Closure parameters in `.map_err()` are named `err` (not `e`)
- [ ] Each error variant is used for a single, distinct error source
- [ ] One error type per function (or shared only when all variants are common)
- [ ] No unused error variants exist
- [ ] All error variants are fully documented following the template
- [ ] No `BoxError` or `Box<dyn Error>` in production code

## 🎓 RATIONALE

These patterns prioritize:

1. **Explicitness over magic** - Explicit `.map_err()` makes error flow visible
2. **Context preservation** - Wrapping errors with domain-specific variants provides debugging context
3. **Type safety** - Concrete error types enable exhaustive matching and precise error handling
4. **Error chain integrity** - `#[source]` attribute preserves full error causality
5. **Maintainability** - Clear naming and documentation make errors easy to understand and evolve
6. **Production quality** - Avoiding type erasure and enforcing comprehensive documentation ensures robust error handling

## References
- [errors-handling](errors-handling.md) - Related: Error handling rules
