---
name: "apps-cli"
description: "Patterns for command line interfaces (output formatting, logs, etc.). Load when adding or updating commands."
type: core
scope: "global"
---

# CLI Patterns

## 🎯 PURPOSE

This document establishes consistent, machine-friendly CLI patterns across Amp CLIs.
These patterns ensure:

- **Scriptability** - Control-plane commands (`ampctl`) should support `--json` for tooling and automation
- **LLM friendliness** - Structured output is stable and parseable
- **Clean stdout** - Results go to stdout, logs go to stderr
- **Consistent UX** - Human and JSON outputs are derived from the same data

## 📑 TABLE OF CONTENTS

- [CLI Patterns](#cli-patterns)
  - [🎯 PURPOSE](#-purpose)
  - [📑 TABLE OF CONTENTS](#-table-of-contents)
  - [📐 CORE PRINCIPLES](#-core-principles)
    - [1. Stdout for Results, Stderr for Logs](#1-stdout-for-results-stderr-for-logs)
    - [2. Always Support `--json` (ampctl)](#2-always-support---json-ampctl)
    - [3. Single Output Path](#3-single-output-path)
  - [🧱 OUTPUT TYPES AND PRINTING (ampctl)](#-output-types-and-printing-ampctl)
    - [1. Use `GlobalArgs::print`](#1-use-globalargsprint)
    - [2. Output Types Must Implement `Serialize + Display`](#2-output-types-must-implement-serialize--display)
  - [✅ EXAMPLES](#-examples)
    - [Example Output Type](#example-output-type)
    - [Printing Output](#printing-output)
    - [Logging](#logging)
  - [✅ CHECKLIST](#-checklist)

## 📐 CORE PRINCIPLES

### 1. Stdout for Results, Stderr for Logs

**REQUIRED**: Command results must go to **stdout**. Logs and progress messages must go
to **stderr** via `tracing`. This keeps JSON output clean for piping and parsing.

### 2. Always Support `--json` (ampctl)

**REQUIRED (ampctl)**: Every `ampctl` command must accept `--json` and produce structured JSON
on stdout. This is mandatory for scripting, automation, and LLM usage.

### 3. Single Output Path

**REQUIRED (ampctl)**: Commands must not use `println!` directly for results. All outputs must flow
through the shared output formatter (`GlobalArgs::print`) to guarantee JSON support.

## 🧱 OUTPUT TYPES AND PRINTING (ampctl)

### 1. Use `GlobalArgs::print`

`GlobalArgs` already includes a global `--json` flag and output formatting support.
Every command should call `global.print(&result)` for its final output.

### 2. Output Types Must Implement `Serialize + Display`

Define a response struct that implements `serde::Serialize` and `Display`.
`Display` drives human output, while `Serialize` drives JSON output.

## ✅ EXAMPLES

### Example Output Type

```rust
#[derive(serde::Serialize)]
struct BuildResult {
    manifest_hash: String,
    models: usize,
}

impl std::fmt::Display for BuildResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Build complete")?;
        writeln!(f, "  Manifest hash: {}", self.manifest_hash)?;
        writeln!(f, "  Models: {}", self.models)
    }
}
```

### Printing Output

```rust
let result = BuildResult {
    manifest_hash: identity.to_string(),
    models: discovered_models.len(),
};

global.print(&result).map_err(Error::JsonSerialization)?;
```

### Logging

```rust
tracing::info!("Building dataset");
// Result still goes to stdout via global.print(...)
```

## ✅ CHECKLIST

- [ ] Command accepts `--json` via `GlobalArgs`
- [ ] Command outputs results via `global.print(...)`
- [ ] Output type implements `Serialize + Display`
- [ ] Logs and progress messages go to stderr via `tracing`
- [ ] No `println!`/`eprintln!` for results
