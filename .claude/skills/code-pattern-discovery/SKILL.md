---
name: code-pattern-discovery
description: Load relevant coding patterns based on user query or work context. Use when asking about coding patterns, standards, or before implementing code.
---

# Code Pattern Discovery Skill

This skill provides dynamic code pattern documentation discovery for the Project Amp codebase. It enables lazy loading of pattern context based on user queries and coding tasks.

## When to Use This Skill

Use this skill when:
- User asks about coding patterns or standards
- User wants to understand how to implement something correctly
- User asks "how should I handle X?" or "what's the pattern for Y?"
- Before implementing code to understand existing patterns
- User asks about error handling, logging, testing, or documentation patterns
- User needs crate-specific implementation guidance

## How Pattern Discovery Works

Pattern documentation uses lazy loading via YAML frontmatter:

1. **Query frontmatter**: Extract all pattern metadata using discovery command
2. **Match user query**: Compare query against pattern names, descriptions, types, and scopes
3. **Load relevant docs**: Read only the matched pattern docs

## Available Commands

### Discovery Command

The discovery command extracts all pattern frontmatter for lazy loading.

**Primary Method**: Use the Grep tool with multiline mode:
- **Pattern**: `^---\n[\s\S]*?\n---`
- **Path**: `docs/code/`
- **Glob**: `**/*.md`
- **multiline**: `true`
- **output_mode**: `content`

Extracts YAML frontmatter from all pattern docs. Returns pattern metadata for matching.

**Fallback**: Bash command (if Grep tool is unavailable):
```bash
grep -Pzo '(?s)^---\n.*?\n---' docs/code/*.md 2>/dev/null | tr '\0' '\n'
```

**Cross-platform alternative** (macOS compatible):
```bash
awk '/^---$/{p=!p; print; next} p' docs/code/*.md
```

**Use this when**: You need to see all available patterns and their descriptions.

### Read Specific Pattern Doc

Use the Read tool to load `docs/code/<pattern-name>.md`.

**Use this when**: You've identified which pattern doc is relevant to the user's query or task.

## Discovery Workflow

### Step 1: Extract All Pattern Metadata

Run the discovery command (Grep tool primary, bash fallback).

### Step 2: Match User Query or Context

Compare the user's query or work context against:
- `name` field (exact or partial match)
- `description` field (semantic match with "Load when" triggers)
- `type` field (core, arch, crate, meta)
- `scope` field (global or crate-specific)

### Step 3: Load Matched Patterns

Read the full content of matched pattern docs using the Read tool.

## Query Matching Guidelines

### Pattern Types

**Core Patterns** (`type: core`):
- Error handling: "rust-error-handling", "error-reporting"
- Module organization: "rust-modules"
- Type design: "rust-type-design"
- Logging: "logging"
- Documentation: "documentation"
- Testing: "testing-patterns"

**Architectural Patterns** (`type: arch`):
- Service patterns: "services-pattern"
- Workspace structure: "cargo-workspace-patterns"

**Crate-Specific Patterns** (`type: crate`):
- Admin API: "admin-api-crate", "admin-api-security"
- Metadata DB: "metadata-db-crate", "metadata-db-security"
- Common: "udf-documentation"

**Meta Patterns** (`type: meta`):
- Pattern format: "code-pattern-docs"
- Feature format: "feature-docs"

### Exact Matches

- User asks "how do I handle errors?" -> match `name: "rust-error-handling"`
- User asks "module organization" -> match `name: "rust-modules"`
- User working in `admin-api` crate -> match `scope: "crate:admin-api"`
- User asks "how to write tests?" -> match `name: "testing-patterns"`

### Semantic Matches (Using "Load when" Triggers)

- User asks "how do I log errors?" -> match description "Load when adding logs or debugging"
- User asks "how to document functions?" -> match description "Load when documenting code or writing docs"
- User creating a service -> match description "Load when creating or modifying service crates"
- User defining error types -> match description "Load when defining errors or handling error types"

### Scope-Based Matches

- User editing `crates/services/admin-api` -> load patterns with `scope: "crate:admin-api"`
- User working on any crate -> load patterns with `scope: "global"`
- User modifying `crates/metadata-db` -> load patterns with `scope: "crate:metadata-db"`

## Important Guidelines

### Pre-approved Commands

These tools/commands can run without user permission:
- Discovery command (Grep tool or bash fallback) on `docs/code/` - Safe, read-only
- Reading pattern docs via Read tool - Safe, read-only

### When to Load Multiple Patterns

Load multiple pattern docs when:
- User task requires multiple aspects (e.g., error handling + logging)
- Core patterns that commonly go together (e.g., error-reporting + rust-error-handling)
- Crate-specific patterns should include related core patterns
- User is implementing a complex feature requiring multiple patterns

### Automatic Pattern Loading

**IMPORTANT**: This skill should be used proactively based on context:

- **Before editing code**: Load relevant core patterns (error-handling, modules, type-design)
- **Before adding logs**: Load logging pattern
- **Before writing tests**: Load testing-patterns
- **Before documenting**: Load documentation pattern
- **When creating crates**: Load cargo-workspace-patterns
- **When creating services**: Load services-pattern
- **Crate-specific work**: Load crate-specific patterns automatically

### When NOT to Use This Skill

- User asks about features -> Use `/feature-discovery` skill
- User needs to run commands -> Use appropriate `/code-*` skill
- Patterns are already loaded in context -> No need to reload
- User asks about implementation status -> Use `/feature-validate` skill

## Example Workflows

### Example 1: User Asks About Error Handling

**Query**: "How should I handle errors in Rust code?"

1. Run the discovery command to extract all pattern metadata
2. Match "error" against pattern names and descriptions
3. Find matches: `rust-error-handling` and `error-reporting`
4. Load both `docs/code/rust-error-handling.md` and `docs/code/error-reporting.md`
5. Provide guidance from loaded patterns

### Example 2: User Working on Admin API

**Context**: User editing `crates/services/admin-api/src/handlers.rs`

1. Run the discovery command to extract pattern metadata
2. Match `scope: "crate:admin-api"` patterns
3. Find matches: `admin-api-crate`, `admin-api-security`
4. Load relevant core patterns: `rust-error-handling`, `logging`, `documentation`
5. Load all matched pattern docs
6. Provide context-aware guidance

### Example 3: Before Writing Tests

**Query**: "I need to write tests for this function"

1. Run the discovery command to extract pattern metadata
2. Match "test" against pattern descriptions
3. Find match: `testing-patterns` with trigger "Load when writing tests"
4. Load `docs/code/testing-patterns.md`
5. Guide test implementation following patterns

### Example 4: Creating a New Service

**Query**: "How do I create a new service crate?"

1. Run the discovery command to extract pattern metadata
2. Match "service" and "crate" against pattern descriptions
3. Find matches: `services-pattern`, `cargo-workspace-patterns`
4. Load both pattern docs
5. Provide step-by-step guidance following patterns

### Example 5: No Specific Match

**Query**: "How should I structure this code?"

1. Run the discovery command to extract pattern metadata
2. Load general core patterns: `rust-modules`, `rust-type-design`, `rust-error-handling`
3. Provide general guidance based on loaded patterns
4. Ask clarifying questions if needed

## Common Mistakes to Avoid

### Anti-patterns

| Mistake | Why It's Wrong | Do This Instead |
|---------|----------------|-----------------|
| Hardcode pattern lists | Lists become stale | Always use dynamic discovery |
| Load all pattern docs | Bloats context | Use lazy loading via frontmatter |
| Skip discovery step | Miss relevant patterns | Match query/context to metadata first |
| Guess pattern names | May not exist | Run discovery command to verify |
| Ignore crate-specific patterns | Miss important guidance | Check scope field for relevant crates |

### Best Practices

- Use Grep tool first to see available patterns
- Match user query/context to pattern metadata before loading full docs
- Load only relevant patterns to avoid context bloat
- Always load crate-specific patterns when working on that crate
- Combine crate-specific patterns with relevant core patterns
- Use "Load when" triggers in descriptions for semantic matching

## Pattern Loading Priority

When multiple patterns match, prioritize in this order:

1. **Crate-specific patterns** - Most specific guidance
2. **Core patterns** - Fundamental coding standards
3. **Architectural patterns** - High-level organization
4. **Meta patterns** - Format specifications (load only when creating docs)

## Next Steps

After discovering relevant patterns:

1. **Understand context** - Read loaded pattern docs thoroughly
2. **Follow checklists** - Use pattern checklists to verify compliance
3. **Apply patterns consistently** - Follow established conventions
4. **Check related patterns** - Load additional patterns if referenced
5. **Begin implementation** - Use appropriate `/code-*` skills for development
