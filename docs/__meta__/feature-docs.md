---
name: "feature-docs"
description: "Feature documentation format specification. Load when creating or editing feature docs in docs/features/"
type: meta
scope: "global"
---

# Feature Documentation Patterns

**MANDATORY for ALL feature documentation in `docs/features/`**

## Table of Contents

1. [Core Principles](#1-core-principles)
2. [Frontmatter Requirements](#2-frontmatter-requirements)
3. [Naming Schema](#3-naming-schema)
4. [Document Structure](#4-document-structure)
5. [Glossary References](#5-glossary-references)
6. [Content Guidelines](#6-content-guidelines)
7. [Template](#7-template)
8. [Checklist](#8-checklist)

---

## 1. Core Principles

### Feature Docs Are Authoritative

**CRITICAL**: Feature documentation is the **ground truth** for what a feature should do.

- If a feature doc exists, the implementation **MUST** align with it
- If code behaves differently than documented, the code is wrong OR the doc must be updated
- Engineers **MUST** keep feature docs accurate - outdated docs are unacceptable
- When implementation changes, update the feature doc in the same PR

### Describe Functionality, Not Implementation

Feature docs describe **what** a feature does and **why** it exists at an architectural level. They are not meant to document implementation internals — instead, they reference source files and let the code speak for itself.

- Focus on capabilities, behavior, and integration points
- Use Implementation sections to list source file references, not to explain code logic
- When tempted to describe an algorithm, data structure, or API surface in detail, add a source file reference instead

### Purpose
Feature docs provide contextual knowledge for AI agents to understand project features without loading all documentation upfront.

### Dynamic Discovery
Feature docs use YAML frontmatter for lazy loading - AI agents query frontmatter to determine which docs to load based on user queries.

### Avoid Context Bloat
Keep feature docs focused and concise. CLAUDE.md should NOT hardcode feature lists - use dynamic discovery instead.

---

## 2. Frontmatter Requirements

**CRITICAL**: Every feature doc MUST begin with valid YAML frontmatter:

```yaml
---
name: "feature-name-kebab-case"
description: "A one line short description of the feature/functionality"
type: "meta|feature|component"
status: "stable|experimental|unstable|development"
components: "prefix:name,prefix:name"
---
```

### Field Requirements

| Field         | Required | Format                            | Description                                                         |
|---------------|----------|-----------------------------------|---------------------------------------------------------------------|
| `name`        | YES      | kebab-case                        | Unique identifier matching filename (minus .md)                     |
| `description` | YES      | Single line, succinct             | Discovery-optimized description (see guidelines below)              |
| `type`        | YES      | `meta`, `feature`, or `component` | Document classification (see Type Definitions below)                |
| `status`      | YES      | enum                              | Maturity level: `stable`, `experimental`, `unstable`, `development` |
| `components`  | YES      | Prefixed, comma-separated         | Related crates/modules with type prefix                             |

### Type Definitions

| Type | Purpose | Characteristics |
|------|---------|-----------------|
| `meta` | Groups related features/concepts | High-level overview, no Usage section, cannot link to children |
| `feature` | Documents a product capability | User-facing functionality, requires Usage section with examples |
| `component` | Documents a software component | Internal architecture, requires Implementation section |

**meta docs:**
- Describe a domain or capability group (e.g., `data`, `admin`, `query`)
- Provide conceptual foundation and terminology
- MUST NOT link to child docs (children link up to meta)
- May omit Usage section (concrete usage lives in children)

**feature docs:**
- Describe user-facing product capabilities
- Focus on "what can users do" and "how to use it"
- MUST include Usage section with working examples
- May link to related features and parent meta docs

**component docs:**
- Describe internal software components/crates
- Focus on architecture, responsibilities, and integration
- MUST include Implementation section with source files
- May link to related components, features, and parent meta docs

### Status Definitions

The `status` field indicates the maturity and stability level of a feature:

| Status         | Work-in-Progress | Quality                   | Notes                                                                                |
|----------------|------------------|---------------------------|--------------------------------------------------------------------------------------|
| `stable`       | Production Ready | GA (General Availability) | Breaking changes require deprecation cycle; fully tested and documented              |
| `experimental` | Minor            | Dev Preview               | Functional but API may change between releases; suitable for evaluation              |
| `unstable`     | Active           | Alpha                     | Implemented but has sharp edges or incomplete areas; expect significant changes      |
| `development`  | Heavy            | N/A                       | Not implemented or under active design; details may change or feature may be removed |

#### State Progression

Features progress through maturity states:

```
development ─> unstable ─> experimental ─> stable
```

**Progression Criteria:**

- **development → unstable**: Core functionality implemented, basic tests pass
- **unstable → experimental**: API stabilizing, documentation complete, integration tests pass
- **experimental → stable**: Production testing complete, no breaking changes planned

**Regression
**: Features may regress if critical bugs are found, significant refactoring is needed, or API redesign becomes necessary.

### Component Prefixes (MANDATORY)

Components MUST use one of these prefixes:

| Prefix     | Usage                                | Example                             | 
|------------|--------------------------------------|-------------------------------------|
| `crate:`   | Library crates in `crates/`          | `crate:common`, `crate:metadata-db` |
| `service:` | Service crates in `crates/services/` | `service:server`, `service:worker`  |
| `app:`     | Binary crates in `crates/bin/`       | `app:ampd`                          |

**Example:**
```yaml
components: "service:server,crate:common,crate:dataset-store"
```

### Description Guidelines

Write descriptions optimized for dynamic discovery. Unlike skills (which are executed), feature docs are loaded to answer questions and navigate the codebase. Your description must answer two questions:

1. **What does this document explain?** - List specific capabilities or concepts covered
2. **When should Claude load it?** - Include trigger terms and questions this doc answers

**Requirements:**
- Written in third person (no "I" or "you")
- Include keywords users would search for or mention
- Be specific - avoid vague words like "overview", "various", "handles"
- No ending period

**Examples:**
- ✅ `"evm_encode_hex, evm_decode_hex functions for converting addresses and hashes. Load when working with hex encoding or binary conversion"`
- ✅ `"Batch SQL queries via JSON Lines HTTP endpoint. Load when asking about one-shot queries or the /query/jsonl API"`
- ❌ `"Overview of user-defined functions"` (vague, no trigger)
- ❌ `"Handles various data transformations"` (vague, no specifics)

### Discovery Command

The discovery command extracts all feature frontmatter for lazy loading.

**Primary Method**: Use the Grep tool with multiline mode:
- **Pattern**: `^---\n[\s\S]*?\n---`
- **Path**: `docs/features/`
- **multiline**: `true`
- **output_mode**: `content`

**Fallback**: Bash command for manual use:
```bash
grep -Pzo '(?s)^---\n.*?\n---' docs/features/*.md 2>/dev/null | tr '\0' '\n'
```

**Cross-platform alternative** (macOS compatible):
```bash
awk '/^---$/{p=!p; print; next} p' docs/features/*.md
```

---

## 3. Naming Schema

Feature names follow a hierarchical pattern from broad domain to specific feature:

**Pattern:** `<domain>-<subdomain>-<variant>`

### Examples by Domain

**Query Features:**
```
query                           # Meta-doc: Overview of all query capabilities
├── query-sql                   # SQL execution modes
│   ├── query-sql-batch         # Batch query execution
│   └── query-sql-streaming     # Streaming query execution
│       └── query-sql-streaming-joins  # Incremental joins
└── query-transport             # Query transports
    ├── query-transport-jsonl   # JSON Lines HTTP transport
    └── query-transport-flight  # Arrow Flight RPC transport
```

**Application Features:**
```
app-ampd                        # Meta-doc: ampd application overview
└── app-ampd-server             # Query server endpoints and configuration
```

**UDF Features:**
```
udf                             # Meta-doc: Overview of all UDFs
├── udf-builtin                 # Built-in UDFs meta doc
│   ├── udf-builtin-evm-log     # Event log decoding (includes evm_topic)
│   └── udf-builtin-evm-hex     # Hex encoding/decoding
└── udf-custom                  # Custom UDFs meta doc
    ├── udf-custom-javascript   # JavaScript custom UDFs
    └── udf-custom-wasm         # WebAssembly custom UDFs
```

**Extraction Features:**
```
extraction                      # Meta-doc: Data extraction overview
├── extraction-evm-rpc          # EVM RPC extraction
└── extraction-firehose         # Firehose extraction
```

**Admin Features:**
```
admin                           # Meta-doc: Management and administration
├── admin-jobs                  # Job management
│   ├── admin-jobs-list         # List and filter jobs
│   └── admin-jobs-control      # Start, stop, cancel jobs
├── admin-datasets              # Dataset management
│   ├── admin-datasets-list     # List registered datasets
│   └── admin-datasets-sync     # Trigger dataset sync
├── admin-workers               # Worker node management
└── admin-openapi               # OpenAPI spec and documentation
```

### Naming Rules

1. **Use kebab-case** - All lowercase, words separated by hyphens
2. **Domain first** - Start with the broad capability area
3. **Progressively specific** - Add specificity with each segment
4. **Match filename** - `name` field must match filename (minus .md)
5. **Alphabetical grouping** - Related features sort together

### Benefits

- **Discoverable** - Searching "query" finds all query features
- **Hierarchical** - Child docs reference parent meta docs for shared context
- **Scalable** - Easy to add new features in the hierarchy
- **Organized** - Natural grouping when listing files

---

## 4. Document Structure

### Required Sections by Type

Different document types have different required sections:

| Section | meta | feature | component |
|---------|:----:|:-------:|:---------:|
| H1 Title | ✓ | ✓ | ✓ |
| Summary | ✓ | ✓ | ✓ |
| Table of Contents | ✓ | ✓ | ✓ |
| Key Concepts | ✓ | ✓ | ✓ |
| Usage | ✗ | ✓ | optional |
| Architecture | optional | optional | optional |
| Implementation | ✗ | optional | ✓ |
| References | optional | optional | optional |

**Section descriptions:**

1. **H1 Title** - Human-readable feature name
2. **Summary** - 2-4 sentences expanding on the frontmatter description
3. **Table of Contents** - Links to all sections
4. **Key Concepts** - Core terminology and definitions
5. **Usage** - How to use/interact with the feature (with code examples)
6. **Architecture** - How the feature fits into the system (data flow, component interaction)
7. **Implementation** - Database schemas, file locations, internal notes
8. **References** - Cross-references to other feature docs

### Optional Sections

Include when relevant (for any type):

- **Configuration** - Configuration options and defaults
- **API Reference** - Key API endpoints or functions
- **Limitations** - Known constraints or limitations

**CRITICAL**: No empty sections allowed. If you include a section header, it must have content. Omit optional sections entirely rather than leaving them empty.

### References Section Format

Use simple list format with relationship type:

```markdown
## References

- [arrow-flight-query](arrow-flight-query.md) - Alternative: High-performance streaming
- [dataset-store](dataset-store.md) - Dependency: Dataset catalog access
- [common-udfs](common-udfs.md) - Dependency: SQL UDFs
```

**Relationship types:** `Dependency`, `Alternative`, `Related`, `Extended by`, `Base`

### Reference Direction Rules

Reference rules depend on document type:

| From Type | Can Link To |
|-----------|-------------|
| `meta` | Other meta docs only (siblings at same level) |
| `feature` | Parent meta, sibling features, related components |
| `component` | Parent meta, related features, child components |

**Key principles:**
- ✅ **meta** docs MUST NOT link to children (features or components link UP to meta)
- ✅ **component** docs MAY link to child components they manage
- ✅ **feature** and **component** docs link UP to their parent meta doc

**This rule applies to:**
- The References section
- Inline links in prose
- Links in Architecture diagrams or tables
- Any markdown link `[text](file.md)` pointing to a feature doc

**Rationale**: Meta docs provide stable, high-level context. Linking downward creates:
- Maintenance burden when child docs are added/removed/renamed
- Coupling between stable meta docs and volatile implementation details
- Circular dependency patterns in documentation

**Examples:**
- ✅ `data-store.md` (component) → links to `data.md` (meta) — child to parent
- ✅ `data-store.md` (component) → links to `data-metadata-caching.md` (feature) — component to managed feature
- ✅ `data-metadata-caching.md` (feature) → links to `data.md` (meta) — feature to parent meta
- ❌ `data.md` (meta) → links to `data-store.md` (component) — FORBIDDEN: meta to child
- ❌ `query.md` (meta) → lists `query-sql-streaming.md` (feature) — FORBIDDEN: meta to child

---

## 5. Glossary References

Avoid re-defining common terms. Instead, embed inline links to the [Glossary](../glossary.md).

**Inline term linking:**

```markdown
The [dataset](../glossary.md#dataset) contains multiple [revisions](../glossary.md#revision) organized by [namespace](../glossary.md#namespace).
```

This keeps feature docs focused and ensures consistent terminology across all documentation.

---

## 6. Content Guidelines

### DO

- Keep descriptions focused and actionable
- Reference specific crates and files with paths
- Include code snippets for clarity
- Use consistent terminology from Key Concepts
- Include curl/CLI examples for API features

### DON'T

- Duplicate content from pattern files (link instead)
- Include implementation details (use `docs/code/` for that)
- Hardcode paths that may change frequently
- Include verbose API documentation (use OpenAPI specs)
- Add speculative or planned features
- Use vague descriptions ("various", "multiple", "etc.")

---

## 7. Template

Use this template when creating new feature docs:

```markdown
---
name: "{{feature-name-kebab-case}}"
description: "{{What it explains + when to load it, third person, no period}}"
type: "{{meta|feature|component}}"
status: "{{stable|experimental|unstable|dev}}"
components: "{{prefix:name,prefix:name - use crate:, service:, or app:}}"
---

# {{Feature Title - Human Readable}}

## Summary

{{2-4 sentences providing more context than the frontmatter description.
Explain what this feature does, why it exists, and its primary use case.}}

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture) {{if complex}}
3. [Configuration](#configuration) {{if applicable}}
4. [Usage](#usage) {{REQUIRED for feature type, optional for component, omit for meta}}
5. [API Reference](#api-reference) {{if applicable}}
6. [Implementation](#implementation) {{REQUIRED for component type, optional for feature, omit for meta}}
7. [Limitations](#limitations) {{if applicable}}
8. [References](#references) {{if cross-referencing - follow type rules}}

## Key Concepts

{{Define 3-5 key terms used throughout this document:}}

- **Term 1**: Definition explaining what this term means in context
- **Term 2**: Definition explaining what this term means in context

## Architecture {{OPTIONAL - include only if feature is complex}}

{{Explain how this feature fits into the system. Include this section when:
- Feature has complex data flow or component interactions
- Request/response cycles need explanation
- System integration isn't obvious from usage examples

Omit this section for simple features where usage is self-explanatory.}}

### Request Flow {{or Data Flow, Component Interaction, etc.}}

1. Step one of the flow
2. Step two of the flow
3. Step three of the flow

## Configuration {{OPTIONAL}}

{{Configuration options, defaults, and environment variables}}

| Setting | Default | Description |
|---------|---------|-------------|
| option_name | default_value | What this option controls |

## Usage

{{Code examples showing how to use the feature}}

### Basic Usage

```bash
{{Command or code example}}
```

### Advanced Usage {{if applicable}}

```bash
{{More complex example}}
```

## API Reference {{OPTIONAL}}

{{For features with HTTP/gRPC APIs}}

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/path` | POST | What this endpoint does |

For request/response schemas, see [OpenAPI spec](../openapi-specs/spec.json):

```bash
jq '.paths["/path"]' docs/openapi-specs/spec.json
```

## Implementation {{OPTIONAL}}

{{Database schemas, file locations, internal implementation notes}}

### Database Schema {{if applicable}}

| Column | Type | Description |
|--------|------|-------------|
| column_name | TYPE | What this column stores |

### Source Files

- `crates/path/to/file.rs` - How this file relates to the feature

## Limitations {{OPTIONAL}}

{{Known constraints or limitations as bullet points}}

- Limitation one
- Limitation two

## References {{OPTIONAL}}

{{Cross-references to related feature docs}}

- [feature-name](feature-name.md) - Relationship: Brief description
- [another-feature](another-feature.md) - Relationship: Brief description
```

---

## 8. Checklist

Before committing feature documentation:

### Frontmatter

- [ ] Valid YAML frontmatter with opening and closing `---`
- [ ] `name` is kebab-case and matches filename (minus .md)
- [ ] `type` is one of: `meta`, `feature`, `component`
- [ ] `status` is one of: `stable`, `experimental`, `unstable`, `development`
- [ ] `description` explains what it covers and when to load it (no ending period)
- [ ] `components` uses prefixes: `crate:`, `service:`, or `app:`

### Structure (type-aware)

- [ ] H1 title (human readable) after frontmatter
- [ ] Summary section (2-4 sentences) after H1
- [ ] Table of Contents after Summary
- [ ] Key Concepts section with clear definitions
- [ ] **If type=feature**: Usage section with working code examples (REQUIRED)
- [ ] **If type=component**: Implementation section with source files (REQUIRED)
- [ ] **If type=meta**: No Usage or Implementation sections
- [ ] Architecture section (optional for all types)
- [ ] References section follows type rules (optional)
- [ ] No empty sections (omit optional sections rather than leaving them empty)

### Reference Direction (type-aware)

- [ ] **meta** docs do NOT link to children (features or components)
- [ ] **feature** docs link UP to parent meta, MAY link to related components
- [ ] **component** docs link UP to parent meta, MAY link to child components

### Quality

- [ ] Discoverable via grep command
- [ ] No duplicate content from `docs/code/` pattern files
- [ ] References specific files/crates where relevant
- [ ] Examples are accurate and tested
- [ ] No hardcoded values that may change

### Review

Use the `/feature-fmt-check` skill to validate feature docs before committing.
