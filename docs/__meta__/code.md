---
name: "code"
description: "Code guideline documentation format specification. Load when creating or editing guideline docs in docs/code/"
type: meta
scope: "global"
---

# Code Guideline Documentation Format

**MANDATORY for ALL guideline documents in `docs/code/`**

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

### Guideline Docs Are Authoritative

**CRITICAL**: Guideline documentation is the **ground truth** for how code should be written.

- If a guideline doc exists, the implementation **MUST** follow it
- If code diverges from documented guidelines, the code is wrong OR the guideline must be updated
- Engineers **MUST** keep guideline docs accurate - outdated guidelines are unacceptable
- When guidelines evolve, update the guideline doc in the same PR

### Discoverability Through Frontmatter

Guideline docs use YAML frontmatter for lazy loading - AI agents query frontmatter to determine which guidelines to load based on the current task context.

### Consistency and Machine Readability

This format specification ensures:

- **Uniform structure** across all guideline documents
- **Machine-readable metadata** for automated discovery
- **Clear categorization** via guideline types and scopes for organized access
- **Scalability** - easy to add new guidelines following established format

### Avoid Context Bloat

Keep guideline docs focused and concise. CLAUDE.md should NOT hardcode guideline lists - use dynamic discovery instead.

---

## 2. Frontmatter Requirements

**CRITICAL**: Every guideline doc MUST begin with valid YAML frontmatter:

```yaml
---
name: "guideline-name-kebab-case"
description: "Brief description. Load when [trigger conditions]"
type: "principle|core|arch|crate|meta"
scope: "global|crate:<name>"
---
```

### Field Requirements

| Field         | Required | Format                       | Description                                                            |
|---------------|----------|------------------------------|------------------------------------------------------------------------|
| `name`        | YES      | kebab-case                   | Unique identifier matching filename (minus .md)                        |
| `description` | YES      | Single line, succinct        | Discovery-optimized description (see guidelines below)                 |
| `type`        | YES      | `principle`, `core`, `arch`, `crate`, or `meta` | Guideline category (see Type Definitions below)          |
| `scope`       | YES      | `global` or `crate:<name>`   | Application scope: global or crate-specific                            |

### Type Definitions

| Type   | Purpose                          | Scope           | Characteristics                                      |
|--------|----------------------------------|-----------------|------------------------------------------------------|
| `principle` | Universal software principles | Always `global` | Best practices for optimal code quality              |
| `core` | Fundamental coding patterns      | Always `global` | Applicable across entire codebase                    |
| `arch` | Architectural patterns           | Always `global` | High-level organizational and structural patterns    |
| `crate`| Crate-specific patterns          | `crate:<name>`  | Patterns for individual crates or modules            |
| `meta` | Documentation about documentation| Always `global` | Format specifications and conventions                |

#### `principle` - Principle Guidelines

Universal software principles and best practices for optimal code quality. These are language-agnostic design principles that guide all implementation decisions.

**Examples:**
- `principle-law-of-demeter` - Law of Demeter (Principle of Least Knowledge)
- `principle-open-closed` - Open/Closed Principle
- `principle-single-responsibility` - Single Responsibility Principle
- `principle-type-driven-design` - Type-Driven Design (Make Illegal States Unrepresentable)
- `principle-idempotency` - Idempotency (Safe Retries in Distributed Systems)
- `principle-inversion-of-control` - Inversion of Control (Dependency Injection)
- `principle-validate-at-edge` - Validate at the Edge (Hard Shell, Soft Core)

#### `core` - Core Guidelines

Fundamental coding standards applicable across the entire codebase.

**Examples:**
- `errors-handling` - Error handling rules
- `errors-reporting` - Error type design (thiserror)
- `rust-modules` - Module organization
- `rust-documentation` - Rustdoc patterns
- `pattern-service` - Two-phase handle+fut service pattern
- `logging` - Structured logging (tracing)
- `logging-errors` - Error logging patterns
- `test-organization` - Test type selection and placement
- `test-files` - Test file placement
- `test-functions` - Test naming and structure
- `apps-cli` - CLI output formatting

#### `arch` - Architectural Guidelines

High-level organizational and structural guidelines.

**Examples:**
- `services` - Service crate structure
- `rust-workspace` - Workspace organization
- `rust-crate` - Crate manifest conventions
- `extractors` - Data extraction patterns

#### `crate` - Crate-Specific Guidelines

Guidelines scoped to individual crates or modules.

**Examples:**
- `crate-admin-api` - Admin API handler patterns
- `crate-admin-api-security` - Admin API security checklist
- `crate-metadata-db` - Metadata DB patterns
- `crate-metadata-db-security` - Metadata DB security checklist
- `crate-common-udf` - UDF documentation patterns

#### `meta` - Meta Guidelines

Documentation format specifications. Meta guidelines live in `docs/__meta__/`.

**Examples:**
- `features` - Feature doc format (`docs/__meta__/features.md`)
- `code` - Guideline doc format (`docs/__meta__/code.md`)
- `code-principle` - Principle guideline template (`docs/__meta__/code-principle.md`)
- `code-pattern` - Pattern guideline template (`docs/__meta__/code-pattern.md`)

### Description Guidelines

Write descriptions optimized for dynamic discovery. Unlike skills (which are executed), guideline docs are loaded to guide implementation. Your description must answer two questions:

1. **What does this document explain?** - List specific guidelines or concepts covered
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
- ❌ `"Guidelines for testing"` (too vague, missing trigger)

### Discovery Command

The discovery command extracts all guideline frontmatter for lazy loading.

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

**Format:** `<prefix>-<aspect>.md`

### Groups

```
principle-*                         # Universal software principles (principle) — see code-principle.md for template
├── principle-law-of-demeter       # Law of Demeter
├── principle-open-closed          # Open/Closed Principle
├── principle-single-responsibility # Single Responsibility Principle
├── principle-type-driven-design   # Type-Driven Design (Make Illegal States Unrepresentable)
├── principle-idempotency          # Idempotency (Safe Retries in Distributed Systems)
├── principle-inversion-of-control # Inversion of Control (Dependency Injection)
└── principle-validate-at-edge    # Validate at the Edge (Hard Shell, Soft Core)

errors-*                            # Error guidelines (core)
├── errors-handling                 # Error handling rules
└── errors-reporting                # Error type design (thiserror)

rust-*                              # Rust language guidelines (core/arch)
├── rust-crate                      # Crate manifest conventions (arch)
├── rust-documentation              # Rustdoc patterns (core)
├── rust-modules                    # Module organization (core)
│   └── rust-modules-members        # Module member ordering (core)
└── rust-workspace                  # Workspace organization (arch)

pattern-*                           # Design pattern guidelines (core) — see code-pattern.md for template
├── pattern-builder                 # Builder pattern for required fields
├── pattern-service                 # Two-phase handle+fut service pattern
└── pattern-typestate               # Typestate pattern for state machines

test-*                              # Testing guidelines (core)
├── test-organization               # Test type selection and placement
├── test-files                      # Test file placement
└── test-functions                  # Test naming and structure

logging-*                           # Logging guidelines (core)
├── logging                         # Structured logging (tracing)
└── logging-errors                  # Error logging patterns

crate-*                             # Crate-specific guidelines (crate)
├── crate-admin-api                 # Admin API handler patterns
│   └── crate-admin-api-security    # Admin API security checklist
├── crate-metadata-db               # Metadata DB patterns
│   └── crate-metadata-db-security  # Metadata DB security checklist
└── crate-common-udf                # UDF documentation patterns

Standalone guidelines
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

- **Discoverable** - Searching a prefix finds all related guidelines
- **Grouped** - Related guidelines sort together alphabetically
- **Scalable** - Easy to add new guidelines within a group
- **Organized** - Natural grouping when listing files

---

## 4. Cross-Reference Rules

Guideline documents may reference other guidelines to establish relationships. Cross-references use defined relationship types and follow directional rules based on guideline type.

### Relationship Types

| Type | Meaning | Example |
|---|---|---|
| `Related` | Sibling in same prefix group | test-files <-> test-functions |
| `Foundation` | Core guideline a crate/arch guideline builds on | crate-admin-api -> errors-reporting |
| `Companion` | Paired doc for same crate | crate-admin-api <-> crate-admin-api-security |
| `Extends` | Specializes/refines another guideline | rust-modules-members -> rust-modules |

### Direction Rules

| From Type | Can Link To |
|---|---|
| `principle` | Other principle patterns (`Related`) |
| `core` | Principle patterns (`Foundation`), other core patterns (`Related`, `Extends`) |
| `arch` | Principle/core patterns (`Foundation`), other arch patterns (`Related`) |
| `crate` | Principle/core/arch patterns (`Foundation`), own companion (`Companion`) |
| `meta` | Nothing |

**Key principles:**
- Principle guidelines are standalone and link laterally to other principle guidelines
- Core guidelines link laterally to related or parent core guidelines, and may reference principle guidelines as foundation
- Arch guidelines reference the principle/core guidelines they build on
- Crate guidelines reference the principle/core/arch guidelines they depend on, plus their security companion
- Meta guidelines are self-contained and have no cross-references

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
- ❌ `code` -> `rust-modules` (meta guidelines have no cross-references)
- ❌ `rust-modules` -> `crate-admin-api` (core cannot reference crate guidelines)

---

## 5. Document Structure

### Required Sections

Every guideline document should follow this general structure:

| Section | Required | Description |
|---------|:--------:|-------------|
| H1 Title | Yes | Human-readable guideline name |
| Applicability statement | Yes | Bold line stating mandatory scope |
| Main content sections | Yes | Guideline-specific content organized by topic |
| Checklist | Yes | Verification checklist for guideline compliance |
| References | No | Cross-references to related guidelines (follow type rules) |

### Optional Sections

Include when relevant:

- **Table of Contents** - For lengthy documents
- **Complete Examples** - Comprehensive usage examples
- **Configuration** - Setup and configuration guidance

**CRITICAL**: No empty sections allowed. If you include a section header, it must have content. Omit optional sections entirely rather than leaving them empty.

---

## 6. Content Guidelines

### DO

- Keep guidelines focused and actionable
- Reference specific crates and files with paths
- Include code snippets showing correct and incorrect usage
- Use consistent terminology throughout
- Include a verification checklist at the end
- Explain the reasoning behind guidelines

### DON'T

- Duplicate content from feature docs (link instead)
- Include project-specific business logic
- Hardcode paths that may change frequently
- Add speculative or planned guidelines
- Use vague descriptions ("various", "multiple", "etc.")
- Leave optional sections empty (omit them instead)

---

## 7. Template

Use this template when creating new guideline docs:

```markdown
---
name: "{{guideline-name-kebab-case}}"
description: "{{Brief summary. Load when [trigger conditions], no period}}"
type: "{{principle|core|arch|crate|meta}}"
scope: "{{global or crate:<name>}}"
---

# {{Guideline Title - Human Readable}}

**MANDATORY for {{applicability statement}}**

## Table of Contents {{OPTIONAL - for lengthy documents}}

1. [Section Name](#section-name)
2. [Another Section](#another-section)
3. [Checklist](#checklist)

## {{Main Content Sections}}

{{Guideline-specific content organized by topic.
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

- [guideline-name](guideline-name.md) - Relationship: Brief description

## Checklist

Before committing code, verify:

- [ ] {{Verification item 1}}
- [ ] {{Verification item 2}}
- [ ] {{Verification item 3}}
```

---

## 8. Checklist

Before committing guideline documentation:

### Frontmatter

- [ ] Valid YAML frontmatter with opening and closing `---`
- [ ] `name` is kebab-case and matches filename (minus .md)
- [ ] `type` is one of: `principle`, `core`, `arch`, `crate`, `meta`
- [ ] `scope` is valid: `global` or `crate:<name>`
- [ ] `description` includes "Load when" trigger clause (no ending period)
- [ ] Frontmatter is valid YAML (no syntax errors)

### Structure

- [ ] H1 title (human readable) after frontmatter
- [ ] Applicability statement (bold mandatory line)
- [ ] Main content sections with guideline details
- [ ] Checklist section for verification
- [ ] No empty sections (omit optional sections rather than leaving them empty)

### Naming and Organization

- [ ] File located at `docs/code/` root (no subdirectories)
- [ ] Filename uses kebab-case
- [ ] Filename uses appropriate prefix for its group
- [ ] Related guidelines share the same prefix
- [ ] Crate-specific guidelines follow `crate-<crate-name>.md` format
- [ ] Internal cross-references use correct paths

### Cross-References

- [ ] References use defined relationship types (`Related`, `Foundation`, `Companion`, `Extends`)
- [ ] Crate guidelines reference foundation core guidelines
- [ ] Security companions are bidirectionally linked
- [ ] Meta guidelines have no cross-references

### Discovery

- [ ] Description is optimized for AI agent discovery
- [ ] Guideline can be found via Grep multiline pattern
- [ ] Trigger conditions are clear and specific

### Review

Use the `/docs-code-fmt-check` skill to validate guideline docs before committing.
