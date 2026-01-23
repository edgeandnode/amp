---
name: feature-discovery
description: Load relevant feature docs based on user query. Use when user asks about features, functionality, or how things work in the project.
---

# Feature Discovery Skill

This skill provides dynamic feature documentation discovery for the Project Amp codebase. It enables lazy loading of feature context based on user queries.

## When to Use This Skill

Use this skill when:
- User asks about a feature or functionality
- User wants to understand how something works
- User asks "what is X?" or "how does X work?"
- User references a component and wants context
- Before implementing features to understand existing functionality
- User asks about Project Amp capabilities

## How Feature Discovery Works

Feature documentation uses lazy loading via YAML frontmatter:

1. **Query frontmatter**: Extract all feature metadata using discovery command
2. **Match user query**: Compare query against feature names, descriptions, and components
3. **Load relevant docs**: Read only the matched feature docs

## Available Commands

### Discovery Command

The discovery command extracts all feature frontmatter for lazy loading.

**Primary Method**: Use the Grep tool with multiline mode:
- **Pattern**: `^---\n[\s\S]*?\n---`
- **Path**: `docs/features/`
- **Glob**: `*.md`
- **multiline**: `true`
- **output_mode**: `content`

Extracts YAML frontmatter from all feature docs. Returns feature metadata for matching.

**Fallback**: Bash command (if Grep tool is unavailable):
```bash
grep -Pzo '(?s)^---\n.*?\n---' docs/features/*.md 2>/dev/null | tr '\0' '\n'
```

**Cross-platform alternative** (macOS compatible):
```bash
awk '/^---$/{p=!p; print; next} p' docs/features/*.md
```

**Use this when**: You need to see all available features and their descriptions.

### Read Specific Feature Doc

Use the Read tool to load `docs/features/<feature-name>.md`.

**Use this when**: You've identified which feature doc is relevant to the user's query.

## Discovery Workflow

### Step 1: Extract All Feature Metadata

Run the discovery command (Grep tool primary, bash fallback).

### Step 2: Match User Query

Compare the user's query against:
- `name` field (exact or partial match)
- `description` field (semantic match)
- `components` field (crate/module names)

### Step 3: Load Matched Features

Read the full content of matched feature docs using the Read tool.

## Query Matching Guidelines

### Exact Matches

- User asks about "admin API" -> match `name: "admin-api"`
- User mentions "JSONL queries" -> match `name: "jsonl-sql-batch-query"`
- User references "metadata database" -> match `components: "crate:metadata-db"`

### Semantic Matches

- User asks "how do I query data?" -> match descriptions containing "query"
- User asks "what are UDFs?" -> match descriptions containing "user-defined functions"
- User asks "how does extraction work?" -> match descriptions containing "extract"

### Component Matches

- User working in `crates/services/worker` -> match `components: "service:worker"`
- User editing `admin-api` handlers -> match `components: "crate:admin-api"`
- User modifying `server` code -> match `components: "service:server"`

## Important Guidelines

### Pre-approved Commands

These tools/commands can run without user permission:
- Discovery command (Grep tool or bash fallback) on `docs/features/` - Safe, read-only
- Reading feature docs via Read tool - Safe, read-only

### When to Load Multiple Features

Load multiple feature docs when:
- User query matches multiple features
- Feature has dependencies on other features (check Related Features section)
- User is working on integration between components

### When NOT to Use This Skill

- User asks about code patterns -> Use `/code-pattern-discovery` skill
- User needs to run commands -> Use appropriate `/code-*` skill
- User is doing simple file edits -> No discovery needed
- User asks about implementation details -> Use `/code-pattern-discovery` skill
- User asks if implementation matches docs -> Use `/feature-validate`

## Example Workflows

### Example 1: User Asks About a Feature

**Query**: "How do I execute SQL queries against datasets?"

1. Run the discovery command to extract all feature metadata
2. Match "sql" and "query" against feature descriptions
3. Find match: `jsonl-sql-batch-query` with description "Execute one-shot SQL batch queries..."
4. Load `docs/features/jsonl-sql-batch-query.md`
5. Provide summary from loaded doc

### Example 2: User Working on Multiple Components

**Query**: "I need to understand how the server handles requests"

1. Run the discovery command to extract feature metadata
2. Match features with `components: "service:server"`
3. Load all matched feature docs
4. Explain the interaction based on loaded context

### Example 3: Before Implementing a Feature

**Query**: "I want to add a new query endpoint"

1. Run the discovery command to extract feature metadata
2. Match existing query-related features
3. Load relevant docs to understand existing patterns
4. Identify related features for reference

### Example 4: No Match Found

**Query**: "How does feature X work?" (where X doesn't exist)

1. Run the discovery command to extract feature metadata
2. No matches found in frontmatter
3. Inform user no feature doc exists for X
4. Suggest using `/code-pattern-discovery` or asking for clarification

## Common Mistakes to Avoid

### Anti-patterns

| Mistake | Why It's Wrong | Do This Instead |
|---------|----------------|-----------------|
| Hardcode feature lists | Lists become stale | Always use dynamic discovery |
| Load all feature docs | Bloats context | Use lazy loading via frontmatter |
| Skip discovery step | Miss relevant features | Match query to metadata first |
| Guess feature names | May not exist | Run discovery command to verify |

### Best Practices

- Use Grep tool first to see available features
- Match user query to feature metadata before loading full docs
- Load only relevant features to avoid context bloat
- Follow cross-references in "Related Features" sections
- Check component field for crate-specific features

## Next Steps

After discovering relevant features:

1. **Understand context** - Read loaded feature docs thoroughly
2. **Check patterns** - Use `/code-pattern-discovery` for implementation details
3. **Identify related features** - Follow cross-references in Related Features
4. **Begin implementation** - Use appropriate `/code-*` skills for development
