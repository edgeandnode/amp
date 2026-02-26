---
name: code-discovery
description: Load relevant code guidelines based on user query or work context. Use when asking about code guidelines, standards, or before implementing code.
---

# Code Guideline Discovery Skill

This skill provides dynamic code guideline documentation discovery for the Project Amp codebase. It enables lazy loading of guideline context based on user queries and coding tasks.

## When to Use This Skill

Use this skill when:
- User asks about code guidelines or standards
- User wants to understand how to implement something correctly
- User asks "how should I handle X?" or "what's the pattern for Y?"
- Before implementing code to understand existing guidelines
- User asks about error handling, logging, testing, or documentation patterns
- User needs crate-specific implementation guidance

## How Guideline Discovery Works

Guideline documentation uses lazy loading via YAML frontmatter:

1. **Query frontmatter**: Extract all guideline metadata using discovery command
2. **Match user query**: Compare query against guideline names, descriptions, types, and scopes
3. **Load relevant docs**: Read only the matched guideline docs

## Available Commands

### Discovery Command

The discovery command extracts all guideline frontmatter for lazy loading.

**Primary Method**: Use the Grep tool with multiline mode:
- **Pattern**: `^---\n[\s\S]*?\n---`
- **Path**: `docs/code/`
- **Glob**: `**/*.md`
- **multiline**: `true`
- **output_mode**: `content`

Extracts YAML frontmatter from all guideline docs. Returns guideline metadata for matching.

**Fallback**: Bash command (if Grep tool is unavailable):
```bash
grep -Pzo '(?s)^---\n.*?\n---' docs/code/*.md 2>/dev/null | tr '\0' '\n'
```

**Cross-platform alternative** (macOS compatible):
```bash
awk '/^---$/{p=!p; print; next} p' docs/code/*.md
```

**Use this when**: You need to see all available guidelines and their descriptions.

### Read Specific Guideline Doc

Use the Read tool to load `docs/code/<pattern-name>.md`.

**Use this when**: You've identified which guideline doc is relevant to the user's query or task.

## Discovery Workflow

### Step 1: Extract All Guideline Metadata

Run the discovery command (Grep tool primary, bash fallback).

### Step 2: Match User Query or Context

Compare the user's query or work context against:
- `name` field (exact or partial match)
- `description` field (semantic match with "Load when" triggers)
- `type` field (core, arch, crate, meta)
- `scope` field (global or crate-specific)

### Step 3: Load Matched Guidelines

Read the full content of matched guideline docs using the Read tool.

## Query Matching Guidelines

### Guideline Types

**Core Guidelines** (`type: core`):
- Error handling: "errors-handling", "errors-reporting"
- Module organization: "rust-modules"
- Logging: "logging", "logging-errors"
- Documentation: "rust-documentation"
- Testing: "test-functions", "test-files", "test-organization"
- CLI: "apps-cli"
- Service pattern: "pattern-service"

**Architectural Guidelines** (`type: arch`):
- Service architecture: "services"
- Workspace structure: "rust-workspace"
- Crate manifests: "rust-crate"
- Data extraction: "extractors"

**Crate-Specific Guidelines** (`type: crate`):
- Admin API: "crate-admin-api", "crate-admin-api-security"
- Metadata DB: "crate-metadata-db", "crate-metadata-db-security"
- Common: "crate-common-udf"

**Meta Guidelines** (`type: meta`):
- Guideline format: "code"
- Feature format: "features"

### Design Principles (Special Case)

When the user asks about design principles, or invokes `/code-discovery principles`:

1. **Run the discovery command** scoped to principles only — use the Grep tool with the same multiline pattern (`^---\n[\s\S]*?\n---`) but with glob `docs/code/principle-*.md`
2. **Load all matched principle docs** by reading every returned file
3. Present summaries and guidance from the loaded principles

### Exact Matches

- User asks "how do I handle errors?" -> match `name: "errors-handling"`
- User asks "module organization" -> match `name: "rust-modules"`
- User working in `admin-api` crate -> match `scope: "crate:admin-api"`
- User asks "how to write tests?" -> match "test-functions", "test-files", or "test-organization"

### Semantic Matches (Using "Load when" Triggers)

- User asks "how do I log errors?" -> match description "Load when adding logs or debugging"
- User asks "how to document functions?" -> match description "Load when documenting code or writing docs"
- User creating a service -> match "pattern-service" and "services"
- User defining error types -> match description "Load when defining errors or handling error types"

### Scope-Based Matches

- User editing `crates/services/admin-api` -> load patterns with `scope: "crate:admin-api"`
- User working on any crate -> load patterns with `scope: "global"`
- User modifying `crates/metadata-db` -> load patterns with `scope: "crate:metadata-db"`

## Important Guidelines

### Pre-approved Commands

These tools/commands can run without user permission:
- Discovery command (Grep tool or bash fallback) on `docs/code/` - Safe, read-only
- Reading guideline docs via Read tool - Safe, read-only

### When to Load Multiple Guidelines

Load multiple guideline docs when:
- User task requires multiple aspects (e.g., error handling + logging)
- Core patterns that commonly go together (e.g., errors-reporting + errors-handling)
- Crate-specific patterns should include related core guidelines
- User is implementing a complex feature requiring multiple patterns

### Automatic Guideline Loading

**IMPORTANT**: This skill should be used proactively based on context:

- **Before editing code**: Load relevant core guidelines (error-handling, modules, types)
- **Before adding logs**: Load logging guideline
- **Before writing tests**: Load test-functions, test-files, test-organization
- **Before documenting**: Load rust-documentation guideline
- **When creating crates**: Load rust-workspace, rust-crate
- **When creating services**: Load pattern-service, services
- **When working on extractors**: Load extractors
- **Crate-specific work**: Load crate-specific guidelines automatically

### When NOT to Use This Skill

- User asks about features -> Use `/feature-discovery` skill
- User needs to run commands -> Use appropriate `/code-*` skill
- Guidelines are already loaded in context -> No need to reload
- User asks about implementation status -> Use `/feature-validate` skill

## Example Workflows

### Example 1: User Asks About Error Handling

**Query**: "How should I handle errors in Rust code?"

1. Run the discovery command to extract all guideline metadata
2. Match "error" against guideline names and descriptions
3. Find matches: `errors-handling` and `errors-reporting`
4. Load both `docs/code/errors-handling.md` and `docs/code/errors-reporting.md`
5. Provide guidance from loaded guidelines

### Example 2: User Working on Admin API

**Context**: User editing `crates/services/admin-api/src/handlers.rs`

1. Run the discovery command to extract guideline metadata
2. Match `scope: "crate:admin-api"` patterns (prefix: `crate-admin-api`)
3. Find matches: `crate-admin-api`, `crate-admin-api-security`
4. Load relevant core guidelines: `errors-handling`, `logging`, `documentation`
5. Load all matched guideline docs
6. Provide context-aware guidance

### Example 3: Before Writing Tests

**Query**: "I need to write tests for this function"

1. Run the discovery command to extract guideline metadata
2. Match "test" against guideline descriptions
3. Find matches: test-functions, test-files, test-organization
4. Load `docs/code/test-functions.md` (and related test guideline docs as needed)
5. Guide test implementation following patterns

### Example 4: Creating a New Service

**Query**: "How do I create a new service crate?"

1. Run the discovery command to extract guideline metadata
2. Match "service" and "crate" against guideline descriptions
3. Find matches: `pattern-service`, `services`, `rust-workspace`, `rust-crate`
4. Load all matched guideline docs
5. Provide step-by-step guidance following patterns

### Example 5: No Specific Match

**Query**: "How should I structure this code?"

1. Run the discovery command to extract guideline metadata
2. Load general core guidelines: `rust-modules`, `errors-handling`
3. Provide general guidance based on loaded guidelines
4. Ask clarifying questions if needed

## Common Mistakes to Avoid

### Anti-patterns

| Mistake | Why It's Wrong | Do This Instead |
|---------|----------------|-----------------|
| Hardcode guideline lists | Lists become stale | Always use dynamic discovery |
| Load all guideline docs | Bloats context | Use lazy loading via frontmatter |
| Skip discovery step | Miss relevant guidelines | Match query/context to metadata first |
| Guess guideline names | May not exist | Run discovery command to verify |
| Ignore crate-specific guidelines | Miss important guidance | Check scope field for relevant crates |

### Best Practices

- Use Grep tool first to see available guidelines
- Match user query/context to guideline metadata before loading full docs
- Load only relevant guidelines to avoid context bloat
- Always load crate-specific guidelines when working on that crate
- Combine crate-specific guidelines with relevant core guidelines
- Use "Load when" triggers in descriptions for semantic matching

## Guideline Loading Priority

When multiple patterns match, prioritize in this order:

1. **Crate-specific patterns** - Most specific guidance
2. **Core patterns** - Fundamental coding standards
3. **Architectural patterns** - High-level organization
4. **Meta patterns** - Format specifications (load only when creating docs)

## Next Steps

After discovering relevant guidelines:

1. **Understand context** - Read loaded guideline docs thoroughly
2. **Follow checklists** - Use guideline checklists to verify compliance
3. **Apply patterns consistently** - Follow established conventions
4. **Check related patterns** - Load additional patterns if referenced
5. **Begin implementation** - Use appropriate `/code-*` skills for development
