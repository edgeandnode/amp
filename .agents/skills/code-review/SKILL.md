---
name: code-review
description: Review code changes for bugs, guideline violations, security, and quality. Use when reviewing PRs, code changes, or before commits.
---

# Code Review Skill

This skill provides comprehensive code review guidance for evaluating changes to the codebase.

## When to Use This Skill

Use this skill when you need to:
- Review pull requests
- Review code changes before commits
- Audit code quality and adherence to project standards
- Provide feedback on implementation correctness and patterns

## Review Checklist

Please review this code change and provide feedback on:

### 1. Security Concerns

Review for security vulnerabilities:
- Exposed secrets or credentials
- Input validation and sanitization
- SQL injection, XSS, or other OWASP Top 10 vulnerabilities
- Authentication and authorization bypasses
- For security-sensitive crates (admin-api, metadata-db), verify compliance with crate-specific security guidelines in `docs/code/`

### 2. Principles Violations

Check for violations of the core design principles. These are foundational rules that take priority over stylistic concerns.

**Core principles** (read the full docs for details, examples, and checklists):

| Principle | One-liner | Full doc |
|-----------|-----------|----------|
| **Single Responsibility** | One struct = one reason to change | `docs/code/principle-single-responsibility.md` |
| **Open/Closed** | Extend via new types/trait impls, don't modify existing code | `docs/code/principle-open-closed.md` |
| **Law of Demeter** | Only talk to immediate collaborators — no `a.b().c().d()` chains | `docs/code/principle-law-of-demeter.md` |
| **Validate at the Edge** | Hard shell (boundary validates), soft core (domain trusts) | `docs/code/principle-validate-at-edge.md` |

Additional design principles (Type-Driven Design, Idempotency, Inversion of Control, etc.) are documented in `docs/code/principle-*.md`. Use `/code-discovery principles` to load them when relevant.

### 3. Potential Bugs

Look for common programming errors such as:
- Off-by-one errors
- Incorrect conditionals
- Use of wrong variable when multiple variables of same type are in scope
- `min` vs `max`, `first` vs `last`, flipped ordering
- Iterating over hashmap/hashset in order-sensitive operations

### 4. Panic Branches

Identify panic branches that cannot be locally proven to be unreachable:
- `unwrap` or `expect` calls
- Indexing operations
- Panicking operations on external data

**Note**: This overlaps with the error handling patterns documented in `docs/code/errors-handling.md`. Verify compliance with the project's error handling standards.

### 5. Backwards Compatibility

Verify backwards compatibility is maintained:
- Changes to `Deserialize` structs that break existing data
- Check that DB migrations keep compatibility when possible:
  - Use `IF NOT EXISTS`
  - Avoid dropping columns or altering their data types
  - Check if migration can be made friendly to rollbacks
- Breaking changes to HTTP APIs or CLIs

### 6. Coding Guideline Violations

Review the changeset against code guideline groups. Follow these three steps in order.

#### Step 1: Discover pattern groups

Run the Glob tool with pattern `docs/code/*.md`. For each file, extract the **first kebab-case segment** of the filename (everything before the first `-`, or the whole name if there is no `-`). Group files that share the same first segment. Files whose first segment is unique form a single-file group.

Always include all non-crate groups. Only include `crate-<crate-name>` groups when the diff modifies files inside that crate's directory.

Examples of current groups:

- `errors` — errors-handling, errors-reporting
- `rust` — rust-modules, rust-modules-members, rust-documentation, rust-crate, rust-workspace
- `pattern` — pattern-builder, pattern-service, pattern-typestate
- `test` — test-organization, test-files, test-functions
- `logging` — logging, logging-errors
- `apps` — apps-cli
- `services` — services
- `extractors` — extractors
- `crate-<crate-name>` — all `crate-<crate-name>*` files for that crate (e.g., `crate-admin-api` includes crate-admin-api and crate-admin-api-security)

#### Step 2: Spawn one Task agent per group — in parallel

For **every** applicable group from Step 1, spawn a `general-purpose` Task agent. Send **all** Task tool calls in a single message so they run concurrently.

Each agent's prompt MUST include:

1. **Which guideline files to read** — list the full paths (e.g., `docs/code/errors-handling.md`, `docs/code/errors-reporting.md`).
2. **How to obtain the diff** — instruct the agent to run `git diff main...HEAD` (or the appropriate base) via the Bash tool.
3. **What to do** — read every pattern file in the group, then review the diff for violations of any rule described in those patterns.
4. **What to return** — a list of violations, each with: file path, line number or range, the violated rule (quote or paraphrase), and a brief explanation. If no violations are found, return `No violations found for group: <group>`.

Example prompt for the `errors` group:

> You are a code-review agent. Your job is to check a code diff for violations of the code guidelines in your assigned group.
>
> **Your group**: errors
>
> **Step 1**: Read these guideline docs using the Read tool:
> - `docs/code/errors-handling.md`
> - `docs/code/errors-reporting.md`
>
> **Step 2**: Get the diff by running: `git diff main...HEAD`
>
> **Step 3**: Review every changed line in the diff against every rule in the guideline docs you read. Only flag actual violations — do not flag code that is compliant.
>
> **Step 4**: Return your findings as a list. Each item must include:
> - File path and line number(s)
> - The specific rule violated (quote or paraphrase from the guideline doc)
> - Brief explanation of why it is a violation
>
> If no violations are found, return: `No violations found for group: errors`

#### Step 3: Collect findings and report

After all Task agents complete, compile their results into the review output. Deduplicate any overlapping findings. Omit groups that reported no violations.

### 7. Testing

Evaluate test coverage and quality:
- Reduced test coverage without justification
- Tests that don't actually test the intended behavior
- Tests with race conditions or non-deterministic behavior
- Integration tests that should be unit tests (or vice versa)
- Changes to existing tests that weaken assertions
- Changes to tests that are actually a symptom of breaking changes to user-visible behaviour

### 8. Performance

Check for performance issues:
- Blocking operations in async code
- DB connection with lifetimes that exceed a local scope
- Inefficient algorithms or data structures

### 9. Documentation

Ensure documentation is up-to-date:
- The `config.sample.toml` should be kept up-to-date
- API changes are reflected in OpenAPI specs
- README and architectural docs reflect current behavior

### 10. Dead Code

Find dead code that is not caught by warnings:
- Overriding values that should be read first
- Silently dead code due to `pub`
- `todo!()` or `dbg!()` macros left in production code

### 11. Inconsistencies

Look for inconsistencies between comments and code:
- Documentation that doesn't match implementation
- Misleading variable names or comments
- Outdated comments after refactoring

### 12. Documentation Validation

When reviewing PRs, validate documentation alignment:

#### Format Validation

- **Feature docs** (`docs/features/*.md`): Invoke `/docs-features-fmt-check` to validate format compliance
- **Guideline docs** (`docs/code/*.md`): Invoke `/docs-code-fmt-check` to validate format compliance

#### Implementation Alignment

- **Feature docs**: Invoke `/feature-validate` to verify feature documentation aligns with actual code implementation
- Check whether code changes require feature doc updates (new features, changed behavior)
- Check whether feature doc changes reflect actual implementation state

**Process:**
1. Check if PR modifies files in `docs/features/` or `docs/code/`
2. If feature docs changed: Run `/docs-features-fmt-check` skill for format validation
3. If guideline docs changed: Run `/docs-code-fmt-check` skill for format validation
4. If PR changes feature-related code OR feature docs: Run `/feature-validate` to verify alignment
5. Report any format violations or implementation mismatches in the review

## Important Guidelines

### Focus on Actionable Feedback

- Provide specific, actionable feedback on actual lines of code
- Avoid general comments without code references
- Reference specific file paths and line numbers
- Suggest concrete improvements

### Guideline Compliance is Critical

Guideline violations should be treated seriously as they:
- Reduce codebase consistency
- Make maintenance harder
- May introduce security vulnerabilities (in security-sensitive crates)
- Conflict with established architectural decisions

Always run the pattern violation review (section 6) as part of every code review.

### Review Priority

Sections are ordered by priority — review from top to bottom:
1. **Security concerns** (§1, highest priority)
2. **Principles violations** (§2)
3. **Potential bugs** and **panic branches** (§3–4)
4. **Backwards compatibility** (§5)
5. **Code guideline violations** (§6)
6. **Testing** (§7)
7. **Performance** (§8)
8. **Documentation**, **dead code**, and **inconsistencies** (§9–11)

## Next Steps

After completing the code review:
1. Provide clear, prioritized feedback
2. Distinguish between blocking issues (bugs, security) and suggestions (style, performance)
3. Reference specific patterns from `docs/code/` when flagging violations
4. Suggest using `/code-format`, `/code-check`, and `/code-test` skills to validate fixes
