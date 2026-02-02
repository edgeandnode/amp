---
name: "code-pattern-docs"
description: "Pattern documentation format specification. Load when creating or editing pattern docs in docs/code/"
type: meta
scope: "global"
---

# Code Pattern Documentation Format

**🚨 MANDATORY for ALL pattern documents in `docs/code/`**

## 🎯 PURPOSE

This document establishes the format specification for code pattern documents, ensuring:

- **Discoverability** - AI agents can find and load relevant patterns automatically
- **Consistency** - Uniform structure across all pattern documents
- **Machine-readable metadata** - YAML frontmatter for automated discovery
- **Clear categorization** - Pattern types and scopes for organized access

## 📋 FRONTMATTER SCHEMA

Every pattern document **MUST** include YAML frontmatter at the beginning of the file.

```yaml
---
name: "pattern-name-kebab-case"
description: "Brief description. Load when [trigger conditions]"
type: "core|arch|crate|meta"
scope: "global|crate:<name>"
---
```

### Field Definitions

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `name` | YES | string | Unique identifier matching filename (minus `.md` extension) |
| `description` | YES | string | Discovery-optimized description with "Load when" trigger conditions |
| `type` | YES | enum | Pattern category: `core`, `arch`, `crate`, or `meta` |
| `scope` | YES | string | Application scope: `global` or `crate:<name>` |

### Pattern Types

#### `core` - Core Patterns
Fundamental coding patterns applicable across the entire codebase.

**Examples:**
- `rust-modules` - Module organization
- `rust-error-handling` - Error handling patterns
- `error-reporting` - Error type design
- `logging` - Structured logging
- `documentation` - Rustdoc patterns
- `test-functions` - Test naming and structure
- `test-files` - Test file placement

**Scope:** Always `global`

#### `arch` - Architectural Patterns
High-level architectural and organizational patterns.

**Examples:**
- `services-pattern` - Service crate structure
- `cargo-workspace-patterns` - Workspace organization

**Scope:** Always `global`

#### `crate` - Crate-Specific Patterns
Patterns specific to individual crates or modules.

**Examples:**
- `admin-api-crate` - Admin API implementation patterns
- `admin-api-security` - Admin API security checklist
- `metadata-db-crate` - Metadata DB patterns

**Scope:** Always `crate:<name>` (e.g., `crate:admin-api`)

#### `meta` - Meta Patterns
Documentation about documentation - format specifications and conventions.

**Examples:**
- `feature-docs` - Feature doc format
- `code-pattern-docs` - Pattern doc format (this document)

**Scope:** Always `global`

### Description Field Guidelines

**MANDATORY:** The description field **MUST** include a "Load when" clause that triggers when the agent should use this pattern.

**Format:** `"<Brief summary>. Load when <trigger condition>"`

**Good Examples:**
```yaml
description: "Modern module organization without mod.rs. Load when creating modules or organizing Rust code"
description: "Error handling patterns, unwrap/expect prohibition. Load when handling errors or dealing with Result/Option types"
description: "HTTP handler patterns using Axum. Load when working on admin-api crate"
description: "Security checklist for HTTP APIs. Load before admin-api changes"
```

**Bad Examples (DO NOT USE):**
```yaml
description: "Module organization patterns"  # Missing "Load when" trigger
description: "This document describes error handling"  # Too verbose, missing trigger
description: "Patterns for testing"  # Too vague, missing trigger
```

## 📁 NAMING CONVENTIONS

### File Naming

**Core and Arch Patterns:**
```
rust-modules.md
rust-error-handling.md
error-reporting.md
services-pattern.md
```

**Crate-Specific Patterns:**
```
<crate-name>-<pattern-type>.md

Examples:
admin-api-crate.md
admin-api-security.md
metadata-db-crate.md
metadata-db-security.md
```

**Meta Patterns:**
```
feature-docs.md
code-pattern-docs.md
```

**Rules:**
- Use kebab-case for all filenames
- No subdirectories - all files at `docs/code/` root
- Crate-specific patterns flatten to `<crate>-<type>.md`
- Name field in frontmatter **MUST** match filename (minus `.md`)

## 🔍 DISCOVERY MECHANISM

Pattern documents are discovered using the Grep tool with multiline pattern extraction:

**Pattern:** `^---\n[\s\S]*?\n---`
**Path:** `docs/code/`
**Glob:** `**/*.md`
**Options:** `multiline: true`, `output_mode: "content"`

This extracts all YAML frontmatter blocks from pattern documents for AI agent discovery.

## 📝 DOCUMENT STRUCTURE

### Required Sections

Every pattern document should follow this structure:

```markdown
---
[YAML frontmatter]
---

# [Pattern Title]

**🚨 MANDATORY for [applicability statement]**

## 🎯 PURPOSE

[Brief explanation of what this pattern achieves and why it exists]

## [Main Content Sections]

[Pattern-specific content organized by topic]

## ✅ CHECKLIST

[Verification checklist for pattern compliance]

## 🎓 RATIONALE

[Explanation of the principles and reasoning behind the pattern]
```

### Optional Sections

Depending on pattern complexity, additional sections may include:

- **📑 TABLE OF CONTENTS** - For lengthy documents
- **📋 COMPLETE EXAMPLES** - Comprehensive usage examples
- **⚙️ CONFIGURATION** - Setup and configuration guidance
- **🚨 CRITICAL REQUIREMENTS** - Absolute requirements highlighted

## ✅ VALIDATION CHECKLIST

Before committing a pattern document, verify:

### Frontmatter
- [ ] YAML frontmatter present at file start
- [ ] `name` field matches filename (minus `.md`)
- [ ] `description` includes "Load when" trigger clause
- [ ] `type` is one of: `core`, `arch`, `crate`, `meta`
- [ ] `scope` is valid: `global` or `crate:<name>`
- [ ] Frontmatter is valid YAML (no syntax errors)

### Content Structure
- [ ] Title follows pattern document conventions
- [ ] PURPOSE section explains pattern goals
- [ ] Main content is well-organized
- [ ] CHECKLIST section provides verification steps
- [ ] RATIONALE section explains principles

### Naming and Organization
- [ ] Filename uses kebab-case
- [ ] No subdirectories (all files at `docs/code/` root)
- [ ] Crate-specific patterns follow `<crate>-<type>.md` format
- [ ] Internal cross-references use correct paths

### Discovery
- [ ] Description is optimized for AI agent discovery
- [ ] Pattern can be found via Grep multiline pattern
- [ ] Trigger conditions are clear and specific

## 📚 EXAMPLE: Complete Pattern Document

```markdown
---
name: "rust-error-handling"
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

[Content...]

## ✅ CHECKLIST

Before committing Rust code, verify:

- [ ] 🔥 **ZERO `.unwrap()` calls in production code paths**
- [ ] 🔥 **ZERO `.expect()` calls in production code**
- [ ] Pattern matching used for all `Result` and `Option` handling

## 🎓 RATIONALE

These patterns prioritize:

1. **Safety First** - Production code must never panic unexpectedly
2. **Clarity** - Explicit error handling makes code paths visible
3. **Production Quality** - Code that handles errors gracefully
```

## 🎓 RATIONALE

This format specification enables:

1. **Automated Discovery** - AI agents can find patterns via frontmatter
2. **Contextual Loading** - Patterns load when relevant to current task
3. **Consistent Structure** - Uniform organization aids comprehension
4. **Machine Readability** - Structured metadata for tooling
5. **Scalability** - Easy to add new patterns following established format

**Remember:** Pattern documents are the foundation of AI-assisted development. Well-structured patterns enable autonomous, context-aware coding assistance.
