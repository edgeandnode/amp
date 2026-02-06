---
name: feature-fmt-check
description: Validate feature doc format against the specification. Use when reviewing PRs, after editing feature docs, or before commits
---

# Feature Format Check Skill

This skill validates that feature documentation **format** follows the established patterns in `docs/__meta__/feature-docs.md`.

## When to Use This Skill

Use this skill when:
- Reviewing a PR that includes feature doc changes
- After creating or editing a feature doc
- Before committing changes to `docs/features/`
- User requests a feature doc format review

## Review Process

### Step 1: Identify Changed Feature Docs

For recent commits:
```bash
git diff --name-only HEAD~1 | grep 'docs/features/.*\.md$'
```

For staged changes:
```bash
git diff --cached --name-only | grep 'docs/features/.*\.md$'
```

For unstaged changes:
```bash
git diff --name-only | grep 'docs/features/.*\.md$'
```

### Step 2: Validate Each Feature Doc

For each changed feature doc, verify:
1. Frontmatter format
2. Content structure
3. Discovery compatibility

## Format Reference

All format requirements are defined in [docs/__meta__/feature-docs.md](../../docs/__meta__/feature-docs.md). Read that file for:
- Frontmatter field requirements (`name`, `description`, `components`)
- Description guidelines (third person, "Load when" triggers, no ending period)
- Component prefix rules (`crate:`, `service:`, `app:`)
- Required and optional sections (Summary, Key Concepts, Architecture, Usage, etc.)
- Reference direction rules (references flow UP the hierarchy)
- No empty sections rule

Use the **Checklist** section in `docs/__meta__/feature-docs.md` to validate feature docs.

### Discovery Validation

Verify frontmatter is extractable:

**Primary Method**: Use the Grep tool with multiline mode:
- **Pattern**: `^---\n[\s\S]*?\n---`
- **Path**: `docs/features/<feature-name>.md`
- **multiline**: `true`
- **output_mode**: `content`

**Fallback**: Bash command:
```bash
grep -Pzo '(?s)^---\n.*?\n---' docs/features/<feature-name>.md
```

**Cross-platform alternative** (macOS compatible):
```bash
awk '/^---$/{p=!p; print; next} p' docs/features/<feature-name>.md
```

## Validation Process

1. **Identify changed files**: `git diff --name-only HEAD~1 | grep 'docs/features/.*\.md$'`
2. **Read the feature doc** and **Read** [docs/__meta__/feature-docs.md](../../docs/__meta__/feature-docs.md)
3. **Validate** using the checklist in the patterns file
4. **Report** findings using format below

## Review Report Format

After validation, provide a structured report listing issues found. Use the checklist from [docs/__meta__/feature-docs.md](../../docs/__meta__/feature-docs.md) as the validation criteria.

```markdown
## Feature Doc Format Review: <filename>

### Issues Found
1. <issue description with line number>
2. <issue description with line number>

### Verdict: PASS/FAIL

<If FAIL, provide specific fixes needed referencing docs/__meta__/feature-docs.md>
```

## Common Issues

When validation fails, refer to [docs/__meta__/feature-docs.md](../../docs/__meta__/feature-docs.md) for detailed requirements. Common issues include:

- Invalid frontmatter YAML syntax
- `name` not in kebab-case or doesn't match filename
- `description` has ending period or missing "Load when" trigger
- `components` missing required prefixes (`crate:`, `service:`, `app:`)
- Wrong section names (e.g., `Overview` instead of `Summary`)
- Missing required sections or empty optional sections
- References linking downward (violates "flow UP" rule)

## Pre-approved Commands

These tools/commands can run without user permission:
- Discovery command (Grep tool or bash fallback) on `docs/features/`
- All `git diff` and `git status` read-only commands
- Reading files via Read tool

## Next Steps

After format review:

1. **If format issues found** - List specific fixes needed
2. **If format passes** - Approve for commit
