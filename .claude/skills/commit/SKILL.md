---
name: commit
description: Generate and validate conventional commit messages with Rust workspace scope rules. Use when user says "commit", "git commit", "/commit", asks for help with commit messages, or requests to amend a commit. Enforces crate-based scoping and validates message accuracy against changesets.
---

# Git Commit Messages

## Format

**Tense**: Imperative mood — "Add", "Fix", "Implement", "Refactor", "Delete", etc.

### Template

```
{{type}}({{scope}}): {{description}}
                                      ← blank line
{{summary}}
                                      ← blank line
- {{detail}}
- {{detail}}
- {{detail}}
- {{detail}}
```

| Placeholder       | Description                                          | Constraint       |
|-------------------|------------------------------------------------------|------------------|
| `{{type}}`        | Commit type                                          | See Title/Types  |
| `{{scope}}`       | Principal crate name                                 | See Scope Rules  |
| `{{description}}` | Brief imperative summary of change                   | Title ≤72 chars  |
| `{{summary}}`     | Architectural reasoning paragraph                    | ~160 chars       |
| `{{detail}}`      | Bullet describing a logical/behavioral change        | 2-5 bullets      |


### Title

**Format**: `type(scope): description`

**Types**: `feat`, `refactor`, `fix`, `docs`, `chore`, `test`

**Scope**: Principal crate name from Rust workspace (see ["Rust Workspace Scope Rules"](#rust-workspace-scope-rules))

**Max 72 characters**
- **CRITICAL**: Exceeding 72 chars causes GitHub to truncate PR titles with "..."
- When PR has single commit, first line becomes PR title
- Keep under 72 characters at all costs

### Summary

**~160 characters** — Explain architectural reasoning behind the change.

Focus on the "why" not the "what": why this approach was chosen, what problem it solves, or what architectural goal it achieves.

### Technical Details

**2-5 bullet points** describing logical/behavioral changes.

- Focus on what behavior changed, not line-by-line code changes
- Use backticks for `code references` where helpful (function names, types, config keys, crate names, etc.)
- Each bullet should be a complete thought


## Rust Workspace Scope Rules

**MANDATORY**: Scope = principal crate name from workspace

**Process**:
1. Check `git status` and `git diff`
2. Identify which crate(s) contain changes
3. Choose crate with most significant architectural impact
4. Use exact crate name as scope

**Special cases**:
- CI changes: `chore(ci): ...`
- Dependencies: `chore(deps): ...`
- Root config: `chore: ...` (no scope)


## Examples

```
feat(metadata-db): add connection pooling support

Enable concurrent query execution to improve throughput for high-load scenarios.

- Increase pool size from 5 to 20 connections
- Add 30-second connection timeout
- Implement connection recycling on error
- Add monitoring metrics for pool utilization
```

```
refactor(dataset-store): separate manifest parsing from loading

Extract parsing logic into dedicated module to improve testability and code organization.

- Create `manifest::parser` module with `parse_manifest()`
- Move validation from `load_dataset()` to parser
- Add unit tests for edge cases
- Update error types to distinguish parsing vs loading
```

```
chore: add Rust analyzer configuration

- Configure `rust-analyzer.check.command` to use `clippy`
- Set import granularity to `module`
```


## Critical Requirements


**REQUIRED: Signing**
- Always use `git commit -s`
- Ensure GPG signing if configured


## Amending Commits

**When user requests to amend the latest commit, follow this process:**

1. **Review the commit changeset**:
   ```bash
   git show HEAD --stat
   git show HEAD
   git log -1 --pretty=format:"%H%n%s%n%n%b"
   ```

2. **Analyze compliance** - Check if current message:
   - ✅ Follows `type(scope): description` format
   - ✅ Has correct scope (matches principal crate from changes)
   - ✅ Has correct type (feat/refactor/fix/docs/chore/test)
   - ✅ Title ≤ 72 characters
   - ✅ Body has architectural reasoning + technical details
   - ✅ No AI attribution
   - ✅ Accurately describes the actual changes

3. **Report findings** - Tell user:
   - What crate(s) the commit actually modifies
   - Whether scope is correct or needs updating
   - Whether type matches the changes
   - Whether description accurately reflects changes
   - Any format violations (length, structure, etc.)

4. **Amend if needed** - If message needs correction:
   ```bash
   git commit --amend -s
   ```
