---
name: code-test
description: Run targeted tests to validate changes. Prefer the smallest relevant scope; broaden only when necessary. Use just test-local for broad local coverage when needed.
---

# Code Testing Skill

This skill provides testing operations for the project codebase using cargo-nextest (preferred) or cargo test (fallback).

## When to Use This Skill

Use this skill when you need to run tests and have decided testing is warranted:
- Validate behavior changes or bug fixes
- Confirm localized changes with targeted test suites (unit, integration, local-only)
- Test specific packages or areas
- Respond to a user request to run tests

## Test Scope Selection (Default: Minimal)

Start with the smallest scope that covers the change. Only broaden if you need more confidence.

- Docs/comments-only changes: skip tests and state why
- Localized code change in 1-2 crates: run unit tests or targeted package tests
- Cross-cutting behavior changes or risky refactors: run `just test-local`
- End-to-end/external dependency changes: run `just test` or `just test-it` in CI
- If unsure, ask the user which scope they want

## Available Commands

### Run Local Tests Only (RECOMMENDED FOR LOCAL DEVELOPMENT)
```bash
just test-local [EXTRA_FLAGS]
```
Runs only tests that don't require external dependencies using the "local" nextest profile. Uses `cargo nextest run --profile local --workspace`.

**Requires**: cargo-nextest must be installed (this command will fail without it).

**Use this when**: Working locally without access to external services (databases, Firehose endpoints, etc.).

**This is the broadest local test sweep; use it when you need broader confidence.**

Examples:
- `just test-local` - run all local tests
- `just test-local -p metadata-db` - run local tests for a specific crate

### Run All Tests (REQUIRES EXTERNAL DEPENDENCIES)
```bash
just test [EXTRA_FLAGS]
```
Runs all tests (unit and integration) in the workspace. Uses `cargo nextest run --workspace` if nextest is available, otherwise falls back to `cargo test --workspace`.

**⚠️ WARNING**: This command requires external dependencies (PostgreSQL, Firehose services, etc.) that may not be available locally.

**Use this when**: Running in CI.

Examples:
- `just test` - run all tests
- `just test -- --nocapture` - run with output capture disabled
- `just test my_test_name` - run specific test by name

### Run Unit Tests Only (REQUIRES EXTERNAL DEPENDENCIES)
```bash
just test-unit [EXTRA_FLAGS]
```
Runs only unit tests, excluding integration tests and ampup package. Uses `cargo nextest run --workspace --exclude tests --exclude ampup`.

**⚠️ WARNING**: Some unit tests may require external dependencies (e.g., PostgreSQL for metadata-db tests).

**Use this when**: You want targeted unit coverage for a specific crate or area.

Examples:
- `just test-unit` - run all unit tests
- `just test-unit -p metadata-db` - run unit tests for metadata-db crate

### Run Integration Tests (REQUIRES EXTERNAL DEPENDENCIES)
```bash
just test-it [EXTRA_FLAGS]
```
Runs integration tests from the `tests` package. Uses `cargo nextest run --package tests`.

**⚠️ WARNING**: Integration tests require external dependencies (databases, Firehose endpoints, etc.).

**Use this when**: Running in CI for end-to-end validation.

Examples:
- `just test-it` - run all integration tests
- `just test-it test_name` - run specific integration test

### Run Ampup Tests (REQUIRES EXTERNAL DEPENDENCIES)
```bash
just test-ampup [EXTRA_FLAGS]
```
Runs tests for the ampup package. Uses `cargo nextest run --package ampup`.

**⚠️ WARNING**: May require external dependencies.

Examples:
- `just test-ampup` - run ampup tests

## Important Guidelines

### Cargo Nextest vs Cargo Test

The project prefers **cargo-nextest** for faster test execution:
- Nextest runs tests in parallel more efficiently
- Better output formatting and filtering
- Install with: `cargo install --locked cargo-nextest@^0.9`

All test commands automatically detect nextest availability:
- If nextest is installed: uses it automatically
- If not installed: falls back to `cargo test` with a warning

### Pre-approved Commands
This test command is pre-approved and can be run without user permission:
- `just test-local` - Broad local test sweep (use only when needed)
  - In Codex, still request escalation to run this outside the sandbox.

### Test Workflow Recommendations

1. **During local development**: Prefer targeted tests first; use `just test-local` only for broader confidence
2. **Before commits (local)**: Run the smallest relevant test scope; broaden only if the change is risky or cross-cutting
3. **In CI environments**: The CI system will run `just test` or other commands
4. **Local development**: Never run `just test`, `just test-unit`, `just test-it`, or `just test-ampup` locally. Those are for CI
5. **Codex sandbox**: Run tests only when warranted; prefer targeted scope. If running `just test-local`, request escalation (outside the sandbox)

### External Dependencies Required by Non-Local Tests

The following tests require external services that are typically not available in local development:
- **PostgreSQL database**: Required for metadata-db tests
- **Firehose endpoints**: Required for Firehose dataset tests
- **EVM RPC endpoints**: Required for EVM RPC dataset tests
- **Other services**: As configured in docker-compose or CI environment

**Use `just test-local` to avoid these dependencies during local development.**

### Common Test Flags

You can pass extra flags to cargo through the EXTRA_FLAGS parameter:
- `-p <package>` or `--package <package>` - test specific package
- `test_name` - run tests matching name
- `-- --nocapture` - show println! output (cargo test only)
- `-- --show-output` - show output from passing tests (nextest)

## Common Mistakes to Avoid

### ❌ Anti-patterns
- **Never run `cargo test` directly** - Use justfile tasks for proper configuration
- **Never run `just test` locally** - It requires external dependencies
- **Never skip tests when behavior changes** - Skipping is OK for docs/comments-only changes, but not for runtime changes
- **Never ignore failing tests** - Fix them or document why they fail
- **Never run integration tests locally** - Use `just test-local` or target unit tests instead

### ✅ Best Practices
- Prefer the smallest relevant test scope
- Run tests for behavior changes or bug fixes
- Fix failing tests immediately
- If nextest not installed, install it for better performance
- Run broader tests only when necessary

## Validation Loop Pattern

```
Code Change → Format → Check → Clippy → Targeted Tests (when needed)
                ↑                          ↓
                ←── Fix failures ──────────┘
```

If tests fail:
1. Read error messages carefully
2. Fix the issue
3. Format the fix (`just fmt-file`)
4. Check compilation (`just check-crate`)
5. Re-run the relevant tests (same scope as before)
6. Repeat until all pass

## Next Steps

After required tests pass:
1. **Generate schemas if needed** → See `.claude/skills/code-gen/SKILL.md`
2. **Review changes** → Ensure quality before commits
3. **Commit** → All checks and tests must be green

## Project Context

- This is a Rust workspace with multiple crates
- Integration tests are in the `tests/` package
- Some tests require external dependencies (databases, services)
- Test configurations are defined in `.config/nextest.toml`
- Nextest profiles: `default` and `local`
