---
name: code-pattern-fmt-check
description: Validate pattern doc format against the specification. Use when reviewing PRs, after editing pattern docs, or before commits
---

# Code Pattern Format Check Skill

This skill validates that code pattern documentation **format** follows the established patterns in `docs/__meta__/code-pattern-docs.md`.

## When to Use This Skill

Use this skill when:
- Reviewing a PR that includes pattern doc changes
- After creating or editing a pattern doc
- Before committing changes to `docs/code/`
- User requests a pattern doc format review

## Review Process

### Step 1: Identify Changed Pattern Docs

For recent commits:
```bash
git diff --name-only HEAD~1 | grep 'docs/code/.*\.md$'
```

For staged changes:
```bash
git diff --cached --name-only | grep 'docs/code/.*\.md$'
```

For unstaged changes:
```bash
git diff --name-only | grep 'docs/code/.*\.md$'
```

### Step 2: Validate Each Pattern Doc

For each changed pattern doc, verify:
1. Frontmatter format
2. Content structure
3. Discovery compatibility

## Format Reference

All format requirements are defined in [docs/__meta__/code-pattern-docs.md](../../docs/__meta__/code-pattern-docs.md). Read that file for:
- Frontmatter field requirements (`name`, `description`, `type`, `scope`)
- Description guidelines ("Load when" triggers, discovery optimization)
- Pattern type definitions (`core`, `arch`, `crate`, `meta`)
- Scope format rules (`global`, `crate:<name>`)
- Naming conventions (kebab-case, flattened crate-specific patterns)
- Required and optional sections (main content, Checklist)
- No subdirectories rule (all files at `docs/code/` root)

Use the **Checklist** section in `docs/__meta__/code-pattern-docs.md` to validate pattern docs.

### Discovery Validation

Verify frontmatter is extractable:

**Primary Method**: Use the Grep tool with multiline mode:
- **Pattern**: `^---\n[\s\S]*?\n---`
- **Path**: `docs/code/<pattern-name>.md`
- **multiline**: `true`
- **output_mode**: `content`

**Fallback**: Bash command:
```bash
grep -Pzo '(?s)^---\n.*?\n---' docs/code/<pattern-name>.md
```

**Cross-platform alternative** (macOS compatible):
```bash
awk '/^---$/{p=!p; print; next} p' docs/code/<pattern-name>.md
```

## Validation Process

1. **Identify changed files**: `git diff --name-only HEAD~1 | grep 'docs/code/.*\.md$'`
2. **Read the pattern doc** and **Read** [docs/__meta__/code-pattern-docs.md](../../docs/__meta__/code-pattern-docs.md)
3. **Validate** using the checklist in the format specification
4. **Report** findings using format below

## Review Report Format

After validation, provide a structured report listing issues found. Use the checklist from [docs/__meta__/code-pattern-docs.md](../../docs/__meta__/code-pattern-docs.md) as the validation criteria.

```markdown
## Pattern Doc Format Review: <filename>

### Issues Found
1. <issue description with line number>
2. <issue description with line number>

### Verdict: PASS/FAIL

<If FAIL, provide specific fixes needed referencing docs/__meta__/code-pattern-docs.md>
```

## Common Issues

When validation fails, refer to [docs/__meta__/code-pattern-docs.md](../../docs/__meta__/code-pattern-docs.md) for detailed requirements. Common issues include:

- Invalid frontmatter YAML syntax
- `name` not in kebab-case or doesn't match filename (minus `.md`)
- `description` missing "Load when" trigger clause
- `type` not one of: `core`, `arch`, `crate`, `meta`
- `scope` invalid format (not `global` or `crate:<name>`)
- Crate-specific patterns not following `<crate>-<type>.md` naming
- Pattern files in subdirectories (should be flat at `docs/code/` root)
- Missing required sections (main content, Checklist)
- Empty optional sections

## Frontmatter Validation Checklist

### Required Fields
- [ ] `name` field present and matches filename (minus `.md`)
- [ ] `description` field present with "Load when" trigger
- [ ] `type` field present and valid (`core`, `arch`, `crate`, `meta`)
- [ ] `scope` field present and valid (`global` or `crate:<name>`)

### Name Field
- [ ] Uses kebab-case
- [ ] Exactly matches filename without `.md` extension
- [ ] For crate-specific: follows `<crate>-<type>` format

### Description Field
- [ ] Includes "Load when" trigger clause
- [ ] Clear and concise
- [ ] Optimized for AI agent discovery

### Type Field
- [ ] Valid value: `core`, `arch`, `crate`, or `meta`
- [ ] Appropriate for pattern category

### Scope Field
- [ ] Valid format: `global` or `crate:<name>`
- [ ] `core`, `arch`, `meta` patterns use `global`
- [ ] Crate-specific patterns use `crate:<name>` format

## Content Structure Validation

### Required Sections
- [ ] Title with pattern name
- [ ] Applicability statement (bold mandatory line)
- [ ] Main content sections with pattern details
- [ ] **Checklist** section for verification

### Naming and Organization
- [ ] File located at `docs/code/` root (no subdirectories)
- [ ] Filename uses kebab-case
- [ ] Crate-specific patterns follow `<crate>-<type>.md`

## Pre-approved Commands

These tools/commands can run without user permission:
- Discovery command (Grep tool or bash fallback) on `docs/code/`
- All `git diff` and `git status` read-only commands
- Reading files via Read tool

## Next Steps

After format review:

1. **If format issues found** - List specific fixes needed
2. **If format passes** - Approve for commit
3. **Verify discovery** - Ensure frontmatter is extractable with Grep tool
