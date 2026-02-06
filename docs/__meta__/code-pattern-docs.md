---
name: "code-pattern-docs"
description: "Pattern documentation format specification. Load when creating or editing pattern docs in docs/code/"
type: meta
scope: "global"
---

# Code Pattern Documentation Format

**MANDATORY for ALL pattern documents in `docs/code/`**

## Table of Contents

1. [Core Principles](#1-core-principles)
2. [Frontmatter Requirements](#2-frontmatter-requirements)
3. [Naming Schema](#3-naming-schema)
4. [Cross-Reference Rules](#4-cross-reference-rules)
5. [Document Structure](#5-document-structure)
6. [Content Guidelines](#6-content-guidelines)
7. [Template](#7-template)
8. [Checklist](#8-checklist)

---

## 1. Core Principles

### Pattern Docs Are Authoritative

**CRITICAL**: Pattern documentation is the **ground truth** for how code should be written.

- If a pattern doc exists, the implementation **MUST** follow it
- If code diverges from documented patterns, the code is wrong OR the pattern must be updated
- Engineers **MUST** keep pattern docs accurate - outdated patterns are unacceptable
- When patterns evolve, update the pattern doc in the same PR

### Discoverability Through Frontmatter

Pattern docs use YAML frontmatter for lazy loading - AI agents query frontmatter to determine which patterns to load based on the current task context.

### Consistency and Machine Readability

This format specification ensures:

- **Uniform structure** across all pattern documents
- **Machine-readable metadata** for automated discovery
- **Clear categorization** via pattern types and scopes for organized access
- **Scalability** - easy to add new patterns following established format

### Avoid Context Bloat

Keep pattern docs focused and concise. CLAUDE.md should NOT hardcode pattern lists - use dynamic discovery instead.

---

## 2. Frontmatter Requirements

**CRITICAL**: Every pattern doc MUST begin with valid YAML frontmatter:

```yaml
---
name: "pattern-name-kebab-case"
description: "Brief description. Load when [trigger conditions]"
type: "core|arch|crate|meta"
scope: "global|crate:<name>"
---
```

### Field Requirements

| Field         | Required | Format                       | Description                                                            |
|---------------|----------|------------------------------|------------------------------------------------------------------------|
| `name`        | YES      | kebab-case                   | Unique identifier matching filename (minus .md)                        |
| `description` | YES      | Single line, succinct        | Discovery-optimized description (see guidelines below)                 |
| `type`        | YES      | `core`, `arch`, `crate`, or `meta` | Pattern category (see Type Definitions below)                    |
| `scope`       | YES      | `global` or `crate:<name>`   | Application scope: global or crate-specific                            |

### Type Definitions

| Type   | Purpose                          | Scope           | Characteristics                                      |
|--------|----------------------------------|-----------------|------------------------------------------------------|
| `core` | Fundamental coding patterns      | Always `global` | Applicable across entire codebase                    |
| `arch` | Architectural patterns           | Always `global` | High-level organizational and structural patterns    |
| `crate`| Crate-specific patterns          | `crate:<name>`  | Patterns for individual crates or modules            |
| `meta` | Documentation about documentation| Always `global` | Format specifications and conventions                |

#### `core` - Core Patterns

Fundamental coding standards applicable across the entire codebase.

**Examples:**
- `errors-handling` - Error handling rules
- `errors-reporting` - Error type design (thiserror)
- `rust-modules` - Module organization
- `rust-types` - Type-driven design
- `rust-documentation` - Rustdoc patterns
- `rust-service` - Two-phase handle+fut service pattern
- `logging` - Structured logging (tracing)
- `logging-errors` - Error logging patterns
- `test-strategy` - Three-tier testing strategy
- `test-files` - Test file placement
- `test-functions` - Test naming and structure
- `apps-cli` - CLI output formatting

#### `arch` - Architectural Patterns

High-level organizational and structural patterns.

**Examples:**
- `services` - Service crate structure
- `rust-workspace` - Workspace organization
- `rust-crate` - Crate manifest conventions
- `extractors` - Data extraction patterns

#### `crate` - Crate-Specific Patterns

Patterns scoped to individual crates or modules.

**Examples:**
- `crate-admin-api` - Admin API handler patterns
- `crate-admin-api-security` - Admin API security checklist
- `crate-metadata-db` - Metadata DB patterns
- `crate-metadata-db-security` - Metadata DB security checklist
- `crate-common-udf` - UDF documentation patterns

#### `meta` - Meta Patterns

Documentation format specifications. Meta patterns live in `docs/__meta__/`.

**Examples:**
- `feature-docs` - Feature doc format (`docs/__meta__/feature-docs.md`)
- `code-pattern-docs` - Pattern doc format (`docs/__meta__/code-pattern-docs.md`)

### Description Guidelines

Write descriptions optimized for dynamic discovery. Unlike skills (which are executed), pattern docs are loaded to guide implementation. Your description must answer two questions:

1. **What does this document explain?** - List specific patterns or concepts covered
2. **When should Claude load it?** - Include trigger terms via a "Load when" clause

**Requirements:**
- Written in third person (no "I" or "you")
- Include a "Load when" clause with trigger conditions
- Be specific - avoid vague words like "overview", "various", "handles"
- No ending period

**Examples:**
- ✅ `"Modern module organization without mod.rs. Load when creating modules or organizing Rust code"`
- ✅ `"Error handling patterns, unwrap/expect prohibition. Load when handling errors or dealing with Result/Option types"`
- ✅ `"HTTP handler patterns using Axum. Load when working on admin-api crate"`
- ❌ `"Module organization patterns"` (missing "Load when" trigger)
- ❌ `"This document describes error handling"` (too verbose, missing trigger)
- ❌ `"Patterns for testing"` (too vague, missing trigger)

### Discovery Command

The discovery command extracts all pattern frontmatter for lazy loading.

**Primary Method**: Use the Grep tool with multiline mode:
- **Pattern**: `^---\n[\s\S]*?\n---`
- **Path**: `docs/code/`
- **multiline**: `true`
- **output_mode**: `content`

**Fallback**: Bash command for manual use:
```bash
grep -Pzo '(?s)^---\n.*?\n---' docs/code/*.md 2>/dev/null | tr '\0' '\n'
```

**Cross-platform alternative** (macOS compatible):
```bash
awk '/^---$/{p=!p; print; next} p' docs/code/*.md
```

---

## 3. Naming Schema

**Principle:** prefix = group. Files sharing the same first kebab-case segment form a discoverable group.

**Pattern:** `<prefix>-<aspect>.md`

### Groups

```
errors-*                            # Error patterns (core)
├── errors-handling                 # Error handling rules
└── errors-reporting                # Error type design (thiserror)

rust-*                              # Rust language patterns (core/arch)
├── rust-crate                      # Crate manifest conventions (arch)
├── rust-documentation              # Rustdoc patterns (core)
├── rust-modules                    # Module organization (core)
│   └── rust-modules-members        # Module member ordering (core)
├── rust-service                    # Two-phase service pattern (core)
├── rust-types                      # Type-driven design (core)
└── rust-workspace                  # Workspace organization (arch)

test-*                              # Testing patterns (core)
├── test-strategy                   # Three-tier testing strategy
├── test-files                      # Test file placement
└── test-functions                  # Test naming and structure

logging-*                           # Logging patterns (core)
├── logging                         # Structured logging (tracing)
└── logging-errors                  # Error logging patterns

crate-*                             # Crate-specific patterns (crate)
├── crate-admin-api                 # Admin API handler patterns
│   └── crate-admin-api-security    # Admin API security checklist
├── crate-metadata-db               # Metadata DB patterns
│   └── crate-metadata-db-security  # Metadata DB security checklist
└── crate-common-udf                # UDF documentation patterns

Standalone patterns
├── apps-cli                        # CLI output formatting (core)
├── services                        # Service crate structure (arch)
└── extractors                      # Data extraction patterns (arch)
```

### Naming Rules

1. **Use kebab-case** - All lowercase, words separated by hyphens
2. **Prefix = group** - Shared first segment = same group
3. **Progressively specific** - Add specificity per segment
4. **Match filename** - `name` in frontmatter MUST match filename (minus `.md`)
5. **Flat directory** - All files at `docs/code/` root (no subdirectories)
6. **Crate patterns** - Use `crate-` prefix followed by crate name

### Benefits

- **Discoverable** - Searching a prefix finds all related patterns
- **Grouped** - Related patterns sort together alphabetically
- **Scalable** - Easy to add new patterns within a group
- **Organized** - Natural grouping when listing files

---

## 4. Cross-Reference Rules

Pattern documents may reference other patterns to establish relationships. Cross-references use defined relationship types and follow directional rules based on pattern type.

### Relationship Types

| Type | Meaning | Example |
|---|---|---|
| `Related` | Sibling in same prefix group | test-files <-> test-functions |
| `Foundation` | Core pattern a crate/arch pattern builds on | crate-admin-api -> errors-reporting |
| `Companion` | Paired doc for same crate | crate-admin-api <-> crate-admin-api-security |
| `Extends` | Specializes/refines another pattern | rust-modules-members -> rust-modules |

### Direction Rules

| From Type | Can Link To |
|---|---|
| `core` | Other core patterns (`Related`, `Extends`) |
| `arch` | Core patterns (`Foundation`), other arch patterns (`Related`) |
| `crate` | Core/arch patterns (`Foundation`), own companion (`Companion`) |
| `meta` | Nothing |

**Key principles:**
- Core patterns link laterally to related or parent core patterns
- Arch patterns reference the core patterns they build on
- Crate patterns reference the core/arch patterns they depend on, plus their security companion
- Meta patterns are self-contained and have no cross-references

### References Section Format

```markdown
## References
- [rust-modules](rust-modules.md) - Extends: Base module organization
- [errors-reporting](errors-reporting.md) - Foundation: Error type patterns
- [crate-admin-api-security](crate-admin-api-security.md) - Companion: Security checklist
```

### Examples

- ✅ `rust-modules-members` -> `rust-modules` (Extends: core to core)
- ✅ `crate-admin-api` -> `errors-reporting` (Foundation: crate to core)
- ✅ `crate-admin-api` <-> `crate-admin-api-security` (Companion: bidirectional)
- ✅ `services` -> `rust-modules` (Foundation: arch to core)
- ✅ `test-files` <-> `test-functions` (Related: core siblings)
- ❌ `code-pattern-docs` -> `rust-modules` (meta patterns have no cross-references)
- ❌ `rust-modules` -> `crate-admin-api` (core cannot reference crate patterns)

---

## 5. Document Structure

### Required Sections

Every pattern document should follow this general structure:

| Section | Required | Description |
|---------|:--------:|-------------|
| H1 Title | Yes | Human-readable pattern name |
| Applicability statement | Yes | Bold line stating mandatory scope |
| Main content sections | Yes | Pattern-specific content organized by topic |
| Checklist | Yes | Verification checklist for pattern compliance |
| References | No | Cross-references to related patterns (follow type rules) |

### Optional Sections

Include when relevant:

- **Table of Contents** - For lengthy documents
- **Complete Examples** - Comprehensive usage examples
- **Configuration** - Setup and configuration guidance

**CRITICAL**: No empty sections allowed. If you include a section header, it must have content. Omit optional sections entirely rather than leaving them empty.

---

## 6. Content Guidelines

### DO

- Keep patterns focused and actionable
- Reference specific crates and files with paths
- Include code snippets showing correct and incorrect usage
- Use consistent terminology throughout
- Include a verification checklist at the end
- Explain the reasoning behind patterns

### DON'T

- Duplicate content from feature docs (link instead)
- Include project-specific business logic
- Hardcode paths that may change frequently
- Add speculative or planned patterns
- Use vague descriptions ("various", "multiple", "etc.")
- Leave optional sections empty (omit them instead)

---

## 7. Template

Use this template when creating new pattern docs:

```markdown
---
name: "{{pattern-name-kebab-case}}"
description: "{{Brief summary. Load when [trigger conditions], no period}}"
type: "{{core|arch|crate|meta}}"
scope: "{{global or crate:<name>}}"
---

# {{Pattern Title - Human Readable}}

**MANDATORY for {{applicability statement}}**

## Table of Contents {{OPTIONAL - for lengthy documents}}

1. [Section Name](#section-name)
2. [Another Section](#another-section)
3. [Checklist](#checklist)

## {{Main Content Sections}}

{{Pattern-specific content organized by topic.
Include code examples showing correct and incorrect usage.
Reference specific crates and files where relevant.}}

### {{Subsection}}

{{Detailed guidance with examples:}}

```rust
// Good
{{correct_example()}}

// Bad
{{incorrect_example()}}
```

## References {{OPTIONAL - follow cross-reference rules}}

- [pattern-name](pattern-name.md) - Relationship: Brief description

## Checklist

Before committing code, verify:

- [ ] {{Verification item 1}}
- [ ] {{Verification item 2}}
- [ ] {{Verification item 3}}
```

---

## 8. Checklist

Before committing pattern documentation:

### Frontmatter

- [ ] Valid YAML frontmatter with opening and closing `---`
- [ ] `name` is kebab-case and matches filename (minus .md)
- [ ] `type` is one of: `core`, `arch`, `crate`, `meta`
- [ ] `scope` is valid: `global` or `crate:<name>`
- [ ] `description` includes "Load when" trigger clause (no ending period)
- [ ] Frontmatter is valid YAML (no syntax errors)

### Structure

- [ ] H1 title (human readable) after frontmatter
- [ ] Applicability statement (bold mandatory line)
- [ ] Main content sections with pattern details
- [ ] Checklist section for verification
- [ ] No empty sections (omit optional sections rather than leaving them empty)

### Naming and Organization

- [ ] File located at `docs/code/` root (no subdirectories)
- [ ] Filename uses kebab-case
- [ ] Filename uses appropriate prefix for its group
- [ ] Related patterns share the same prefix
- [ ] Crate-specific patterns follow `crate-<crate-name>.md` format
- [ ] Internal cross-references use correct paths

### Cross-References

- [ ] References use defined relationship types (`Related`, `Foundation`, `Companion`, `Extends`)
- [ ] Crate patterns reference foundation core patterns
- [ ] Security companions are bidirectionally linked
- [ ] Meta patterns have no cross-references

### Discovery

- [ ] Description is optimized for AI agent discovery
- [ ] Pattern can be found via Grep multiline pattern
- [ ] Trigger conditions are clear and specific

### Review

Use the `/code-pattern-fmt-check` skill to validate pattern docs before committing.
