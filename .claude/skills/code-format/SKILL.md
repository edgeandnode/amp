---
name: code-format
description: Format Rust and TypeScript code automatically. Use immediately after editing .rs/.ts files, when user mentions formatting, code style, or before commits/PRs. Ensures consistent code style following project conventions.
---

# Code Formatting Skill

This skill provides code formatting operations for the project codebase, which is a Rust workspace with TypeScript components.

## When to Use This Skill

Use this skill when you need to:
- Format code after editing Rust or TypeScript files
- Check if code meets formatting standards
- Ensure code formatting compliance before commits

## Command Selection Rules

Choose the appropriate command based on the number of files edited:

| Files Edited | File Type  | Command to Use | Rationale |
|--------------|------------|----------------|-----------|
| 1-2 files    | Rust       | `just fmt-file <file>` (per file) | Faster, targeted formatting |
| 3+ files     | Rust       | `just fmt-rs` (global) | More efficient than multiple per-file calls |
| 1-2 files    | TypeScript | `just fmt-file <file>` (per file) | Faster, targeted formatting |
| 3+ files     | TypeScript | `just fmt-ts` (global) | More efficient than multiple per-file calls |

**Decision process:**
1. Count the number of files you edited (by type: Rust or TypeScript)
2. If 2 or fewer files of that type: run `just fmt-file` for each file
3. If 3 or more files of that type: run the global command (`just fmt-rs` or `just fmt-ts`)

## Available Commands

### Format Rust Code
```bash
just fmt-rs
```
Formats all Rust code using `cargo +nightly fmt --all`. This is the primary formatting command.

**Alias**: `just fmt` (same as `fmt-rs`)

### Check Rust Formatting
```bash
just fmt-rs-check
```
Checks Rust code formatting without making changes using `cargo +nightly fmt --all -- --check`.

**Alias**: `just fmt-check` (same as `fmt-rs-check`)

### Format Specific File
```bash
just fmt-file <FILE>
```
Formats a specific file (Rust or TypeScript) based on extension. Automatically detects file type.

Examples:
- `just fmt-file src/main.rs` - formats a Rust file
- `just fmt-file typescript/client.ts` - formats a TypeScript file

### Format TypeScript Code
```bash
just fmt-ts
```
Formats all TypeScript code using `pnpm format`.

### Check TypeScript Formatting
```bash
just fmt-ts-check
```
Checks TypeScript code formatting using `pnpm lint`.

## Important Guidelines

### MANDATORY: Format After Every Edit
**You MUST format code immediately after editing Rust or TypeScript files.**

This is a critical requirement from the project's development workflow:
- Never skip formatting after file edits
- Use the Command Selection Rules above to choose the right command
- Run formatting before any check or test commands

### Example Workflows

**Single file edit:**
1. Edit a Rust file: `src/common/utils.rs`
2. **IMMEDIATELY** run: `just fmt-file src/common/utils.rs`
3. Then run checks

**Multiple files edit (3+):**
1. Edit multiple Rust files across the codebase
2. **IMMEDIATELY** run: `just fmt-rs`
3. Then run checks

## Common Mistakes to Avoid

### ❌ Anti-patterns
- **Never run `cargo fmt` directly** - Use `just fmt-file` or `just fmt-rs`
- **Never run `rustfmt` directly** - The justfile includes proper flags
- **Never skip formatting** - Even "minor" edits need formatting
- **Never use `just fmt-rs` for 1-2 files** - Use `just fmt-file <file>` for efficiency
- **Never use `just fmt-file` for 3+ files** - Use `just fmt-rs` for efficiency

### ✅ Best Practices
- Format immediately after editing files
- Use `just fmt-file` for 1-2 files (faster, targeted)
- Use `just fmt-rs` for 3+ files (more efficient)
- Run `just fmt-rs-check` to verify formatting before commits

## Next Steps

After formatting your code:
1. **Check compilation** → See `.claude/skills/code-check/SKILL.md`
2. **Run clippy** → See `.claude/skills/code-check/SKILL.md`
3. **Run tests** → See `.claude/skills/code-test/SKILL.md`
