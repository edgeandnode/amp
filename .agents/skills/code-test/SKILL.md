---
name: code-test
description: Run targeted tests to validate changes. Prefer the smallest relevant scope; broaden only when necessary. Use just test-local for broad local coverage when needed.
allowed-tools: "Bash(just test-local:*), Bash(just test:*), Bash(just test-unit:*), Bash(just test-integration:*), Bash(just test-e2e:*), Bash(cargo nextest run:*), Bash(cargo nextest list:*)"
---

# Code Testing Skill

This skill provides testing operations for the project codebase. 

All test commands use cargo-nextest exclusively. If not available ask the user to run the `just install-cargo-nextest` task to install it.

## Table of Contents

- [When to Use This Skill](#when-to-use-this-skill)
- [Test Scope Selection](#test-scope-selection-default-minimal)
- [Available Commands](#available-commands)
  - [Run Local Tests Only](#run-local-tests-only-recommended-for-local-development)
  - [Run All Tests](#run-all-tests-requires-external-dependencies)
  - [Run Unit Tests Only](#run-unit-tests-only)
  - [Run Integration Tests](#run-integration-tests-requires-external-dependencies)
  - [Run E2E Tests](#run-e2e-tests-requires-external-dependencies)
  - [Run Ampup Tests](#run-ampup-tests-requires-external-dependencies)
  - [Per-Crate Targeted Testing](#per-crate-targeted-testing)
- [Important Guidelines](#important-guidelines)
  - [Cargo Nextest](#cargo-nextest)
  - [Pre-approved Commands](#pre-approved-commands)
  - [Test Workflow Recommendations](#test-workflow-recommendations)
  - [External Dependencies](#external-dependencies-required-by-non-local-tests)
  - [Common Test Flags](#common-test-flags)
- [Common Mistakes to Avoid](#common-mistakes-to-avoid)
- [Validation Loop Pattern](#validation-loop-pattern)
- [Debugging](#debugging)
  - [Using Logs](#using-logs)
- [Next Steps](#next-steps)
- [Project Context](#project-context)

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
- End-to-end/external dependency changes: run `just test-integration`, `just test-e2e`, or `just test` in CI
- If unsure, ask the user which scope they want

## Available Commands

### Run Local Tests Only (RECOMMENDED FOR LOCAL DEVELOPMENT)
```bash
just test-local [EXTRA_FLAGS]
```
Runs only tests that don't require external dependencies using the `local` nextest profile. Uses `cargo nextest run --profile local --workspace`.

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
Runs all tests (unit and integration) in the workspace. Uses `cargo nextest run --workspace`.

**⚠️ WARNING**: This command requires external dependencies (PostgreSQL, Firehose services, etc.) that may not be available locally.

**Use this when**: Running in CI.

Examples:
- `just test` - run all tests
- `just test -- --nocapture` - run with output capture disabled
- `just test my_test_name` - run specific test by name

### Run Unit Tests Only
```bash
just test-unit [EXTRA_FLAGS]
```
Runs only unit tests using the `unit` nextest profile. Excludes integration tests (`it_*`), the top-level `tests` package, and the `ampup` package.

**Use this when**: You want fast feedback on pure logic changes. Unit tests have no external dependencies.

Examples:
- `just test-unit` - run all unit tests
- `just test-unit -p metadata-db` - run unit tests for metadata-db crate

### Run Integration Tests (REQUIRES EXTERNAL DEPENDENCIES)
```bash
just test-integration [EXTRA_FLAGS]
```
Runs integration tests (`it_*` tests across all crates) using the `integration` nextest profile. Excludes the top-level `tests` package and `ampup` package.

**⚠️ WARNING**: Integration tests require external dependencies (databases, Firehose endpoints, etc.).

**Use this when**: Running in CI or when you have external services available locally.

Examples:
- `just test-integration` - run all integration tests
- `just test-integration -p metadata-db` - run integration tests for a specific crate

### Run E2E Tests (REQUIRES EXTERNAL DEPENDENCIES)
```bash
just test-e2e [EXTRA_FLAGS]
```
Runs end-to-end tests from the top-level `tests/` workspace package using the `e2e` nextest profile.

**⚠️ WARNING**: E2E tests require external dependencies (databases, Firehose endpoints, etc.).

**Use this when**: Running in CI for end-to-end validation.

Examples:
- `just test-e2e` - run all e2e tests
- `just test-e2e test_name` - run specific e2e test

### Run Ampup Tests (REQUIRES EXTERNAL DEPENDENCIES)
```bash
just test-ampup [EXTRA_FLAGS]
```
Runs tests for the ampup package using the `ampup` nextest profile.

**⚠️ WARNING**: May require external dependencies.

Examples:
- `just test-ampup` - run ampup tests

### Per-Crate Targeted Testing

For targeted testing within a single crate, use `cargo nextest run` directly:

```bash
# Unit tests for a specific crate (skip in-tree integration tests)
cargo nextest run -p metadata-db -E 'not test(/::it_/)'

# Specific module's unit tests
cargo nextest run -p metadata-db -E 'test(/workers::tests::/)'

# In-tree integration tests for a crate
cargo nextest run -p metadata-db -E 'test(/::it_/)'

# Specific in-tree integration test suite
cargo nextest run -p metadata-db -E 'test(/::it_workers/)'

# Public API integration tests for a crate
cargo nextest run -p metadata-db -E 'kind(test)'

# Specific public API integration test file
cargo nextest run -p metadata-db -E 'test(it_api_workers)'

# Run a single test by name
cargo nextest run -p metadata-db -E 'test(=my_exact_test_name)'
```

## Important Guidelines

### Cargo Nextest

This project uses **cargo-nextest exclusively** for all test execution:
- Faster parallel test execution
- Better output formatting and filtering
- Filter expressions (`-E`) for precise test selection
- Install with: `just install-cargo-nextest`

### Pre-approved Commands
This test command is pre-approved and can be run without user permission:
- `just test-local` - Broad local test sweep (use only when needed)
- In Codex, still request escalation to run this outside the sandbox

### Test Workflow Recommendations

1. **During local development**: Prefer targeted tests first; use `just test-local` only for broader confidence
2. **Before commits (local)**: Run the smallest relevant test scope; broaden only if the change is risky or cross-cutting
3. **In CI environments**: The CI system will run `just test` or other commands
4. **Local development**: Never run `just test`, `just test-integration`, `just test-e2e`, or `just test-ampup` locally. Those are for CI
5. **Codex sandbox**: Run tests only when warranted; prefer targeted scope. If running `just test-local`, request escalation (outside the sandbox)

### External Dependencies Required by Non-Local Tests

The following tests require external services that are typically not available in local development:
- **PostgreSQL database**: Required for metadata-db tests
- **Firehose endpoints**: Required for Firehose dataset tests
- **EVM RPC endpoints**: Required for EVM RPC dataset tests
- **Other services**: As configured in docker-compose or CI environment

**Use `just test-local` to avoid these dependencies during local development.**

### Common Test Flags

You can pass extra flags through the EXTRA_FLAGS parameter:
- `-p <package>` or `--package <package>` - test specific package
- `-E '<filter>'` - nextest filter expression for precise test selection
- `test_name` - run tests matching name
- `-- --show-output` - show output from passing tests

## Common Mistakes to Avoid

### ❌ Anti-patterns
- **Never use `cargo test`** - Always use `cargo nextest run` or justfile tasks (which use nextest profiles). See [Per-Crate Targeted Testing](#per-crate-targeted-testing)
- **Never run `just test` locally** - It requires external dependencies
- **Never skip tests when behavior changes** - Skipping is OK for docs/comments-only changes, but not for runtime changes
- **Never ignore failing tests** - Fix them or document why they fail
- **Never run integration/e2e tests locally** - Use `just test-local` or target unit tests instead

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

## Debugging

### Using Logs

Tests use the `monitoring` crate's logging system. Enable structured logs via the `AMP_LOG` environment variable to diagnose failures.

**Environment variables:**

| Variable | Default | Values | Purpose |
|---|---|---|---|
| `AMP_LOG` | `info` | `error`, `warn`, `info`, `debug`, `trace` | Log level for all Amp workspace crates |
| `AMP_LOG_SPAN_EVENT` | *(none)* | `close`, `full` | Log tracing span lifecycle events |
| `RUST_LOG` | *(none)* | Standard `tracing` directives | Fine-grained per-crate overrides (takes precedence over `AMP_LOG`) |

**Examples with nextest:**

```bash
# Debug logging for a failing test
AMP_LOG=debug cargo nextest run -p metadata-db -E 'test(my_failing_test)'

# Trace logging (very verbose)
AMP_LOG=trace cargo nextest run -p worker -E 'test(my_test)'

# Debug a specific crate while keeping others quiet
RUST_LOG="metadata_db=trace,sqlx=warn" cargo nextest run -p metadata-db

# Include span open/close events for async debugging
AMP_LOG=debug AMP_LOG_SPAN_EVENT=full cargo nextest run -E 'test(my_test)'
```

**How it works:**
- `AMP_LOG` sets the log level for all 36 Amp workspace crates; external dependencies default to `error`
- `RUST_LOG` directives override `AMP_LOG` for specific crates (useful for noisy dependencies)
- Logging is initialized via `monitoring::logging::init()`, which is idempotent and already called by the test context builder
- Output goes to stderr, which nextest captures by default — use `-- --show-output` to see logs from passing tests

**See also:** [docs/code/logging.md](../../../docs/code/logging.md) for full logging configuration details.

## Next Steps

After required tests pass:
1. **Generate schemas if needed** → See `.claude/skills/code-gen/SKILL.md`
2. **Review changes** → Ensure quality before commits
3. **Commit** → All checks and tests must be green

## Project Context

- This is a Rust workspace with multiple crates
- E2E tests are in the top-level `tests/` package
- Some tests require external dependencies (databases, services)
- Test configurations are defined in `.config/nextest.toml`
- Nextest profiles: `default`, `unit`, `integration`, `e2e`, `local`, `ampup`
