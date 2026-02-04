---
name: code-check
description: Validate and lint code after changes. Use after editing Rust/TypeScript files, when user mentions compilation errors, type checking, linting, clippy warnings, or before commits/PRs. Ensures all code passes checks and has zero warnings.
---

# Code Checking Skill

This skill provides code validation and linting operations for the project codebase.

## When to Use This Skill

Use this skill when you need to:
- Validate Rust or TypeScript code after making changes
- Run compiler checks without building
- Lint code with clippy
- Ensure code quality before commits or PRs

## Command Selection Rules

Choose the appropriate command based on the number of crates edited:

| Crates Edited | Check Command | Clippy Command | Rationale |
|---------------|---------------|----------------|-----------|
| 1-2 crates    | `just check-crate <crate>` | `just clippy-crate <crate>` | Faster, focused feedback |
| 3+ crates     | `just check-rs` | `just clippy` | More efficient than multiple per-crate calls |

**Decision process:**
1. Identify which crates contain your edited files
2. If 2 or fewer crates: use per-crate commands for each crate
3. If 3 or more crates: use global commands

## Available Commands

### Check Rust Code
```bash
just check-rs [EXTRA_FLAGS]
```
Checks Rust code using `cargo check --all-targets`. This runs the compiler without producing binaries.

**Alias**: `just check` (same as `check-rs`)

Examples:
- `just check-rs` - check all Rust code
- `just check-rs --release` - check with release optimizations

### Check Specific Crate
```bash
just check-crate <CRATE> [EXTRA_FLAGS]
```
Checks a specific crate with all its targets using `cargo check --package <CRATE> --all-targets`.

Examples:
- `just check-crate metadata-db` - check the metadata-db crate
- `just check-crate common --release` - check common crate with release mode

### Lint Rust Code (Clippy)
```bash
just clippy [EXTRA_FLAGS]
```
Lints all Rust code using `cargo clippy --all-targets`. Clippy catches common mistakes and suggests improvements.

Examples:
- `just clippy` - lint all code
- `just clippy -- -D warnings` - treat warnings as errors

### Lint Specific Crate
```bash
just clippy-crate <CRATE> [EXTRA_FLAGS]
```
Lints a specific crate using `cargo clippy --package <CRATE> --all-targets --no-deps`.

The `--no-deps` flag ensures clippy only analyzes the specified crate's code, not its dependencies. This provides:
- **Faster execution**: Skip checking dependency code you don't control
- **Focused output**: See only warnings from your crate, not transitive dependencies
- **Actionable results**: All warnings shown are in code you can fix

Examples:
- `just clippy-crate admin-api` - lint the admin-api crate only
- `just clippy-crate dataset-store -- -D warnings` - lint with warnings as errors

### Check All Code
```bash
just check-all
```
Checks both Rust and TypeScript code by running `check-rs` and `check-ts` sequentially.

### Check TypeScript Code
```bash
just check-ts
```
Checks TypeScript code using `pnpm check`.

## Important Guidelines

### MANDATORY: Run Checks After Changes
**You MUST run checks after making code changes. Use the Command Selection Rules above to choose the right command.**

Before considering a task complete: all checks MUST pass AND all clippy warnings MUST be fixed.

### Example Workflows

**Single crate (1-2 crates edited):**
1. Edit files in `metadata-db` crate
2. Format: use `/code-format` skill
3. **Check compilation**: `just check-crate metadata-db`
   - If errors → fix → return to step 2
4. **Check clippy**: `just clippy-crate metadata-db`
   - If warnings → fix ALL → return to step 2
5. Repeat until: zero compilation errors AND zero clippy warnings

**Multiple crates (3+ crates edited):**
1. Edit files across multiple crates
2. Format: use `/code-format` skill
3. **Check compilation**: `just check-rs`
   - If errors → fix → return to step 2
4. **Check clippy**: `just clippy`
   - If warnings → fix ALL → return to step 2
5. Repeat until: zero compilation errors AND zero clippy warnings

## Common Mistakes to Avoid

### ❌ Anti-patterns
- **Never run `cargo check` directly** - Use `just check-crate` or `just check-rs`
- **Never run `cargo clippy` directly** - Justfile adds proper flags like `--no-deps`
- **Never ignore clippy warnings** - Clippy is enforced in CI, warnings will fail builds
- **Never skip the check step** - Even if "it should compile"
- **Never use global commands for 1-2 crates** - Use per-crate commands for efficiency
- **Never use per-crate commands for 3+ crates** - Use global commands for efficiency

### ✅ Best Practices
- Follow Command Selection Rules based on number of crates edited
- Use per-crate commands for 1-2 crates (faster, focused feedback)
- Use global commands for 3+ crates (more efficient)
- Fix compilation errors before running clippy
- Run clippy when you finish a coherent chunk of work or before committing
- Document any warnings you absolutely cannot fix (rare exception)

## Pre-approved Commands
These commands can run without user permission:
- `just check-rs` - Safe, read-only compilation check
- `just check-crate <crate>` - Safe, read-only compilation check
- `just clippy` - Safe, read-only linting
- `just clippy-crate <crate>` - Safe, read-only linting

## Next Steps

After all checks pass:
1. **Run targeted tests when warranted** → See `.claude/skills/code-test/SKILL.md`
2. **Generate schemas if needed** → See `.claude/skills/code-gen/SKILL.md`
3. **Commit changes** → Ensure all checks green first
