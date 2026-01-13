---
name: code-test
description: Run tests to validate changes. Use after implementing features, fixing bugs, or before commits. Defaults to 'just test-local' which runs without external dependencies (recommended for local development).
---

# Code Testing Skill

This skill provides testing operations for the project codebase using cargo-nextest (preferred) or cargo test (fallback).

## When to Use This Skill

Use this skill when you need to:
- Run tests after implementing features or fixing bugs
- Validate that changes don't break existing functionality
- Run specific test suites (unit, integration, local-only)
- Test specific packages

## Available Commands

### Run Local Tests Only (RECOMMENDED FOR LOCAL DEVELOPMENT)
```bash
just test-local [EXTRA_FLAGS]
```
Runs only tests that don't require external dependencies using the "local" nextest profile. Uses `cargo nextest run --profile local --workspace`.

**Requires**: cargo-nextest must be installed (this command will fail without it).

**Use this when**: Working locally without access to external services (databases, Firehose endpoints, etc.).

**This is the PRIMARY testing command for local development.**

Examples:
- `just test-local` - run all local tests

### Run All Tests (REQUIRES EXTERNAL DEPENDENCIES)
```bash
just test [EXTRA_FLAGS]
```
Runs all tests (unit and integration) in the workspace. Uses `cargo nextest run --workspace` if nextest is available, otherwise falls back to `cargo test --workspace`.

**⚠️ WARNING**: This command requires external dependencies (PostgreSQL, Firehose services, etc.) that may not be available locally.

**Use this when**: Running in CI or when you have all external services configured.

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

**Use this when**: You want faster feedback than full test suite, but slower than test-local.

Examples:
- `just test-unit` - run all unit tests
- `just test-unit -p metadata-db` - run unit tests for metadata-db crate

### Run Integration Tests (REQUIRES EXTERNAL DEPENDENCIES)
```bash
just test-it [EXTRA_FLAGS]
```
Runs integration tests from the `tests` package. Uses `cargo nextest run --package tests`.

**⚠️ WARNING**: Integration tests require external dependencies (databases, Firehose endpoints, etc.).

**Use this when**: You need to validate end-to-end functionality and have external services available.

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
- `just test-local` - The ONLY test command that should be used for local development

### Test Workflow Recommendations

1. **During local development**: ALWAYS use `just test-local` - it's designed to work without external dependencies
2. **Before commits (local)**: Run `just test-local` to validate changes
3. **In CI environments**: The CI system will run `just test` or other commands with proper service configurations
4. **DO NOT run locally**: `just test`, `just test-unit`, `just test-it`, or `just test-ampup` unless you have explicitly configured all external services

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
- **Never skip tests** - Even for "trivial" changes
- **Never ignore failing tests** - Fix them or document why they fail
- **Never run integration tests locally** - Use `just test-local` instead

### ✅ Best Practices
- **Always use `just test-local`** for local development
- Run tests after EVERY code change
- Fix failing tests immediately
- If nextest not installed, install it for better performance
- Run tests before commits

## Validation Loop Pattern

```
Code Change → Format → Check → Clippy → Test
                ↑                          ↓
                ←── Fix failures ──────────┘
```

If tests fail:
1. Read error messages carefully
2. Fix the issue
3. Format the fix (`just fmt-file`)
4. Check compilation (`just check-crate`)
5. Re-run tests (`just test-local`)
6. Repeat until all pass

## Next Steps

After all tests pass:
1. **Generate schemas if needed** → See `.claude/skills/code-gen/SKILL.md`
2. **Review changes** → Ensure quality before commits
3. **Commit** → All checks and tests must be green

## Project Context

- This is a Rust workspace with multiple crates
- Integration tests are in the `tests/` package
- Some tests require external dependencies (databases, services)
- Test configurations are defined in `.config/nextest.toml`
- Nextest profiles: `default` and `local`
