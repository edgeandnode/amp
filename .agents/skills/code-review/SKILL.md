---
name: code-review
description: Review code changes for bugs, pattern violations, security, and quality. Use when reviewing PRs, code changes, or before commits.
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

### 1. Potential Bugs

Look for common programming errors such as:
- Off-by-one errors
- Incorrect conditionals
- Use of wrong variable when multiple variables of same type are in scope
- `min` vs `max`, `first` vs `last`, flipped ordering
- Iterating over hashmap/hashset in order-sensitive operations

### 2. Panic Branches

Identify panic branches that cannot be locally proven to be unreachable:
- `unwrap` or `expect` calls
- Indexing operations
- Panicking operations on external data

**Note**: This overlaps with the error handling patterns documented in `docs/code/rust-error-handling.md`. Verify compliance with the project's error handling standards.

### 3. Dead Code

Find dead code that is not caught by warnings:
- Overriding values that should be read first
- Silently dead code due to `pub`
- `todo!()` or `dbg!()` macros left in production code

### 4. Performance

Check for performance issues:
- Blocking operations in async code
- DB connection with lifetimes that exceed a local scope
- Inefficient algorithms or data structures

### 5. Inconsistencies

Look for inconsistencies between comments and code:
- Documentation that doesn't match implementation
- Misleading variable names or comments
- Outdated comments after refactoring

### 6. Backwards Compatibility

Verify backwards compatibility is maintained:
- Changes to `Deserialize` structs that break existing data
- Check that DB migrations keep compatibility when possible:
  - Use `IF NOT EXISTS`
  - Avoid dropping columns or altering their data types
  - Check if migration can be made friendly to rollbacks
- Breaking changes to HTTP APIs or CLIs

### 7. Documentation

Ensure documentation is up-to-date:
- The `config.sample.toml` should be kept up-to-date
- API changes are reflected in OpenAPI specs
- README and architectural docs reflect current behavior

### 8. Security Concerns

Review for security vulnerabilities:
- Exposed secrets or credentials
- Input validation and sanitization
- SQL injection, XSS, or other OWASP Top 10 vulnerabilities
- Authentication and authorization bypasses
- For security-sensitive crates (admin-api, metadata-db), verify compliance with crate-specific security patterns in `docs/code/`

### 9. Testing

Evaluate test coverage and quality:
- Reduced test coverage without justification
- Tests that don't actually test the intended behavior
- Tests with race conditions or non-deterministic behavior
- Integration tests that should be unit tests (or vice versa)
- Changes to existing tests that weaken assertions
- Changes to tests that are actually a symptom of breaking changes to user-visible behaviour

### 10. Coding Pattern Violations

**Use `/code-pattern-discovery` to load relevant patterns based on the crates being modified, then verify the code changes comply with those patterns.**

**Process:**

1. **Identify affected crates**: Determine which crates are modified by the changes
2. **Load patterns**: Invoke `/code-pattern-discovery` to load applicable patterns:
   - Core patterns (error handling, module structure, type design)
   - Architectural patterns (services, workspace structure)
   - Crate-specific patterns (admin-api, metadata-db, etc.)
3. **Review compliance**: Check the code changes against each loaded pattern
4. **Flag violations**: Report any deviations from documented patterns

**Key patterns to verify** (loaded automatically by `/code-pattern-discovery`):

- **Error handling**: Proper use of `Result<T, E>`, error types, error context
- **Module structure**: Correct module organization and visibility
- **Type design**: Appropriate use of newtypes, enums, and type aliases
- **Services pattern**: Adherence to service architecture (if applicable)
- **Security patterns**: For admin-api and metadata-db crates
- **Testing patterns**: Test organization and coverage standards
- **Documentation patterns**: UDF documentation for query layer (if applicable)

**References:**
- Pattern documentation: `docs/code/` directory
- AGENTS.md: See [Coding Patterns](#2-coding-patterns) section
- Use `/code-pattern-discovery` skill to load and review patterns dynamically

### 11. Documentation Validation

When reviewing PRs, validate documentation alignment:

#### Format Validation

- **Feature docs** (`docs/features/*.md`): Invoke `/feature-fmt-check` to validate format compliance
- **Pattern docs** (`docs/code/*.md`): Invoke `/code-pattern-fmt-check` to validate format compliance

#### Implementation Alignment

- **Feature docs**: Invoke `/feature-validate` to verify feature documentation aligns with actual code implementation
- Check whether code changes require feature doc updates (new features, changed behavior)
- Check whether feature doc changes reflect actual implementation state

**Process:**
1. Check if PR modifies files in `docs/features/` or `docs/code/`
2. If feature docs changed: Run `/feature-fmt-check` skill for format validation
3. If pattern docs changed: Run `/code-pattern-fmt-check` skill for format validation
4. If PR changes feature-related code OR feature docs: Run `/feature-validate` to verify alignment
5. Report any format violations or implementation mismatches in the review

## Important Guidelines

### Focus on Actionable Feedback

- Provide specific, actionable feedback on actual lines of code
- Avoid general comments without code references
- Reference specific file paths and line numbers
- Suggest concrete improvements

### Pattern Compliance is Critical

Pattern violations should be treated seriously as they:
- Reduce codebase consistency
- Make maintenance harder
- May introduce security vulnerabilities (in security-sensitive crates)
- Conflict with established architectural decisions

Always invoke `/code-pattern-discovery` before completing a code review to ensure pattern compliance.

### Review Priority

Review items in this priority order:
1. **Security concerns** (highest priority)
2. **Potential bugs** and **panic branches**
3. **Backwards compatibility** issues
4. **Coding pattern violations**
5. **Testing** gaps
6. **Performance** issues
7. **Documentation** and **dead code**

## Next Steps

After completing the code review:
1. Provide clear, prioritized feedback
2. Distinguish between blocking issues (bugs, security) and suggestions (style, performance)
3. Reference specific patterns from `docs/code/` when flagging violations
4. Suggest using `/code-format`, `/code-check`, and `/code-test` skills to validate fixes
