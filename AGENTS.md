# Project Amp - Technical Overview for Claude

## Project Summary

Project Amp is a high-performance ETL (Extract, Transform, Load) architecture for blockchain data services on The Graph. It focuses on extracting blockchain data from various sources, transforming it via SQL queries, and serving it through multiple query interfaces.

## Architecture Overview

### Data Flow

1. **Extract**: Pull data from blockchain sources (EVM RPC, Firehose, etc.)
2. **Transform**: Process data using SQL queries with custom UDFs
3. **Store**: Save as Parquet files (columnar format optimized for analytics)
4. **Serve**: Provide query interfaces (Arrow Flight gRPC, JSON Lines HTTP)

### Technology Stack

- **Language**: Rust
- **Query Engine**: Apache DataFusion
- **Storage Format**: Apache Parquet
- **Wire Format**: Apache Arrow
- **Database**: PostgreSQL (for metadata)

## Key Components

### 1. Main Binary (`ampd`)

- Central command dispatcher
- Commands:
  - `solo`: Start amp in local development mode
  - `server`: Start query servers
  - `worker`: Run distributed worker node
  - `controller`: Run controller with Admin API

### 2. Data Extraction (`dump`)

- Parallel extraction with configurable workers
- Resumable extraction (tracks progress)

### 3. Query Serving (`server`)

- **Arrow Flight Server** (port 1602): High-performance binary protocol
- **JSON Lines Server** (port 1603): Simple HTTP interface
- Features:
  - SQL query execution via DataFusion
  - Streaming query support

### 4. Data Sources

#### EVM RPC (`evm-rpc-datasets`)

- Connects to Ethereum-compatible JSON-RPC endpoints
- Tables: blocks, transactions, logs
- Batched requests for efficiency

#### Firehose (`firehose-datasets`)

- StreamingFast Firehose protocol (gRPC)
- Real-time blockchain data streaming
- Tables: blocks, transactions, logs, calls
- Protocol buffer-based

### 5. Core Libraries

#### `common`

- Shared utilities and abstractions
- Configuration management
- EVM-specific UDFs (User-Defined Functions)
- Catalog management
- Attestation support

#### `metadata-db`

- PostgreSQL-based metadata storage
- Tracks:
  - File metadata
  - Worker nodes
  - Job scheduling
  - Progress tracking
- Uses LISTEN/NOTIFY for distributed coordination

#### `dataset-store`

- Dataset management
- SQL dataset support
- Manifest parsing
- JavaScript UDF support

## Configuration

### Environment Variables

- `AMP_CONFIG`: Path to main config file
- `AMP_LOG`: Logging level (error/warn/info/debug/trace)
- `AMP_CONFIG_*`: Override config values

### Key Directories

1. **manifests_dir**: Dataset manifests (input)
2. **providers_dir**: External service configs
3. **data_dir**: Parquet file storage (output)

### Storage Support

- Local filesystem
- S3-compatible stores
- Google Cloud Storage
- Azure Blob Storage

## SQL Capabilities

### Custom UDFs (User-Defined Functions)

- `evm_decode_hex`: Convert EVM address or bytes32 to hex string
- `evm_encode_hex`: Convert hex string to EVM address or bytes32
- `evm_decode_log`: Decode EVM event logs
- `evm_topic`: Get event topic hash
- `eth_call`: Execute RPC calls during queries
- `evm_decode_params`: Decode function parameters
- `evm_encode_params`: Encode function parameters

### Dataset Definition

- Raw datasets: Direct extraction from sources
- SQL datasets: Views over other datasets
- Materialized views for performance

## Usage Examples

### Query via HTTP

```bash
curl -X POST http://localhost:1603 --data "select * from eth_rpc.logs limit 10"
```

### Python Integration

- Arrow Flight client available
- Marimo notebook examples provided

## 🤖 AI Agent Development Workflow

**This section provides guidance for AI agents (Claude Code and future versions) on how to work with this codebase.**

### 🚀 Quick Start for AI Agents

**If you're an AI agent working on this codebase, here's what you need to know immediately:**

1. **Use Skills for operations** → Invoke skills (`/code-format`, `/code-check`, `/code-test`, `/code-gen`) instead of running commands directly
2. **Skills wrap justfile tasks** → Skills provide the interface to `just` commands with proper guidance
3. **Follow the workflow** → Format → Check → Clippy → Test (in that order)
4. **Fix ALL warnings** → Zero tolerance for clippy warnings
5. **Check patterns** → Review `.patterns/` before implementing code

**Your first action**: If you need to run a command, invoke the relevant Skill!

### 📚 Documentation Structure: Separation of Concerns

This project uses three complementary documentation systems. Understanding their roles helps AI agents navigate efficiently:

| Documentation                  | Purpose                  | Content Focus                                                                                                                    |
| ------------------------------ | ------------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| **AGENTS.md** (this file)      | **WHY** and **WHAT**     | Project architecture, policies, goals, and principles. Answers "What is this project?" and "Why do we do things this way?"       |
| **Skills** (`.claude/skills/`) | **HOW** and **WHEN**     | Command-line operations and justfile usage. Answers "How do I run commands?" and "When should I use each command?"               |
| **Patterns** (`.patterns/`)    | **HOW** (implementation) | Code implementation patterns, standards, and guidelines. Answers "How do I write quality code that follows project conventions?" |

**Navigation Guide for AI Agents:**

- Need to understand the project? → Read this file (AGENTS.md)
- Need to run a command? → Invoke the appropriate Skill (`/code-format`, `/code-check`, `/code-test`, `/code-gen`)
- Need to write code? → Consult Patterns ([`.patterns/README.md`](.patterns/README.md))

### 🎯 Core Operating Principle

**🚨 MANDATORY: USE Skills for all common operations. Skills wrap justfile tasks with proper guidance.**

#### The Golden Rule

**USE Skills (`/code-format`, `/code-check`, `/code-test`, `/code-gen`) for all common operations. Only use `cargo`, `pnpm`, or other tools directly when the operation is NOT covered by a skill or NOT available in the justfile.**

**Decision process:**

1. **First**: Check if a skill exists for your operation
2. **If exists**: Invoke the skill (provides proper flags, setup, and error handling)
3. **If not exists**: You may run the tool directly (e.g., `cargo run`, `cargo build`, tool-specific commands)

#### Why Skills Are Preferred

- **Consistency**: Uniform command execution across all developers and AI agents
- **Correctness**: Skills ensure proper flags, setup, and error handling
- **Guidance**: Skills provide context on when and how to use commands
- **Pre-approved workflows**: Skills document which commands can run without user permission

#### Examples

- ✅ **Use skill**: `/code-format` skill (formatting)
- ✅ **Use skill**: `/code-check` skill (checking and linting)
- ✅ **Use skill**: `/code-test` skill (testing)
- ✅ **Direct tool OK**: `cargo run -p ampd -- dump --help` (running binaries not in justfile)
- ✅ **Direct tool OK**: `cargo build --release` (release builds not in justfile's check task)
- ✅ **Direct tool OK**: Tool-specific commands not covered by justfile tasks

#### Command Execution Hierarchy (Priority Order)

When determining which command to run, follow this strict hierarchy:

1. **Priority 1: Skills** (`.claude/skills/`)
   - Skills are the **SINGLE SOURCE OF TRUTH** for all command execution
   - If Skills document a command, use it EXACTLY as shown
   - Skills override any other guidance in AGENTS.md or elsewhere

2. **Priority 2: AGENTS.md workflow**
   - High-level workflow guidance (when to format, check, test)
   - Refers you to Skills for specific commands

3. **Priority 3: Everything else**
   - Other documentation is supplementary
   - When in conflict, Skills always win

#### Workflow Gate: Use Skills First

**Before running ANY command:**

1. Ask yourself: "Which Skill covers this operation?"
2. Invoke the appropriate skill (e.g., `/code-format`, `/code-check`, `/code-test`, `/code-gen`)
3. Let the skill guide you through the operation

**Example decision tree:**

- Need to format a file? → Use `/code-format` skill
- Need to check a crate? → Use `/code-check` skill
- Need to run tests? → Use `/code-test` skill

### ⚙️ Command-Line Operations Reference

**🚨 CRITICAL: Use skills for all operations - invoke them before running commands.**

Available skills and their purposes:

- **Formatting**: Use `/code-format` skill - Format code after editing files
- **Checking/Linting**: Use `/code-check` skill - Validate and lint code changes
- **Testing**: Use `/code-test` skill - Run tests to validate functionality
- **Code Generation**: Use `/code-gen` skill - Generate schemas and specs

Each Skill provides:

- ✅ **When to use** - Clear guidance on appropriate usage
- ✅ **Available operations** - All supported tasks with proper execution
- ✅ **Examples** - Real-world usage patterns
- ✅ **Pre-approved workflows** - Operations that can run without user permission
- ✅ **Workflow integration** - How operations fit into development flow

**Remember: If you don't know which operation to perform, invoke the appropriate Skill.**

### 📋 Pre-Implementation Checklist

**BEFORE writing ANY code, you MUST:**

1. **Understand the task** - Research the codebase and identify affected crate(s)
2. **Read implementation patterns** - Review [.patterns/README.md](.patterns/README.md)
3. **Check crate-specific guidelines** - If modifying a specific crate, read `.patterns/<crate>/crate.md`
4. **Review security requirements** - If the crate has security guidelines, read `.patterns/<crate>/security.md`

### 🔄 Typical Development Workflow

**Follow this workflow when implementing features or fixing bugs:**

#### 1. Research Phase

- Understand the codebase and existing patterns
- Identify related modules and dependencies
- Review test files and usage examples
- Consult `.patterns/` for relevant implementation patterns

#### 2. Planning Phase

- Create detailed implementation plan
- Identify validation checkpoints
- Consider edge cases and error handling
- Ask user questions if requirements are unclear

#### 3. Implementation Phase

**🚨 CRITICAL: Before running ANY command in this phase, invoke the relevant Skill.**

**Copy this checklist and track your progress:**

```
Development Progress:
- [ ] Step 1: Write code following patterns from .patterns/
- [ ] Step 2: Format code (use /code-format skill)
- [ ] Step 3: Check compilation (use /code-check skill)
- [ ] Step 4: Fix all compilation errors
- [ ] Step 5: Run clippy (use /code-check skill)
- [ ] Step 6: Fix ALL clippy warnings
- [ ] Step 7: Run tests (use /code-test skill)
- [ ] Step 8: Fix all test failures
- [ ] Step 9: All checks pass ✅
```

**Detailed workflow for EVERY code change:**

1. **Write code** following patterns from `.patterns/`

2. **Format immediately** (MANDATORY after EVERY edit):
   - **Use**: `/code-format` skill after editing ANY Rust or TypeScript file
   - **Validation**: Verify no formatting changes remain

3. **Check compilation**:
   - **Use**: `/code-check` skill after changes
   - **Must pass**: Fix all compilation errors
   - **Validation**: Ensure zero errors before proceeding

4. **Lint with clippy**:
   - **Use**: `/code-check` skill for linting
   - **Must pass**: Fix all clippy warnings
   - **Validation**: Re-run until zero warnings before proceeding

5. **Run tests**:
   - **Use**: `/code-test` skill to validate changes
   - **Must pass**: Fix all test failures
   - **Validation**: All tests green

6. **Iterate**: If any validation fails → fix → return to step 2

**Visual Workflow:**

```
Edit File → /code-format skill
          ↓
    /code-check skill (compile) → Fix errors?
          ↓                            ↓ Yes
    /code-check skill (clippy) → (loop back)
          ↓
    /code-test skill → Fix failures?
          ↓                 ↓ Yes
    All Pass ✅      (loop back)
```

**Remember**: Invoke Skills for all operations. If unsure which skill to use, refer to the Command-Line Operations Reference above.

#### 4. Completion Phase

- Ensure all automated checks pass (format, check, clippy, tests)
- Review changes against patterns and security guidelines
- Document any warnings you couldn't fix and why

### 📐 Code Implementation Patterns Reference

**AI agents should consult patterns when writing code:**

- **General patterns**: [.patterns/README.md](.patterns/README.md)
  - Cargo workspace patterns (crate creation, dependency management)
  - Testing patterns (test structure, naming conventions)
- **Service patterns**: [.patterns/services-pattern.md](.patterns/services-pattern.md)
  - Two-phase service creation pattern for `crates/services/*`
  - Service initialization and lifecycle management
- **Crate-specific patterns**: `.patterns/<crate>/crate.md`
  - admin-api: HTTP handler patterns, error handling, documentation
  - metadata-db: Database design patterns, API conventions
  - ampsync: Crate-specific development guidelines
- **Security guidelines**: `.patterns/<crate>/security.md`
  - Security checklists and threat mitigation
  - Input validation and injection prevention

### 🎯 Core Development Principles

**ALL AI agents MUST follow these principles:**

- **Research → Plan → Implement**: Never jump straight to coding
- **Pattern compliance**: Always follow established patterns from `.patterns/`
- **Zero tolerance for errors**: All automated checks must pass
- **Clarity over cleverness**: Choose clear, maintainable solutions
- **Security first**: Never skip security guidelines for security-sensitive crates

**Essential conventions:**

- **Never expose secrets/keys**: All sensitive data in environment variables
- **Maintain type safety**: Leverage Rust's type system fully
- **Prefer async operations**: This codebase uses async/await extensively
- **Always run tests**: Use `/code-test` skill
- **Always format code**: Use `/code-format` skill
- **Fix all warnings**: Use `/code-check` skill for clippy

### 📦 Summary: Key Takeaways for AI Agents

**You've learned the complete workflow. Here's what to remember:**

| What             | Where             | When                               |
| ---------------- | ----------------- | ---------------------------------- |
| **Run commands** | `.claude/skills/` | Check Skills BEFORE any command    |
| **Write code**   | `.patterns/`      | Follow patterns for implementation |
| **Format**       | `/code-format`    | IMMEDIATELY after each edit        |
| **Check**        | `/code-check`     | After formatting                   |
| **Lint**         | `/code-check`     | Fix ALL warnings                   |
| **Test**         | `/code-test`      | Validate all changes               |

**Golden Rules:**

1. ✅ Invoke Skills for all common operations
2. ✅ Skills wrap justfile tasks with proper guidance
3. ✅ Follow the workflow: Format → Check → Clippy → Test
4. ✅ Zero tolerance for errors and warnings
5. ✅ Every change improves the codebase

**Remember**: When in doubt, invoke the appropriate Skill!

## 📝 Additional Resources

For more detailed information about the project:

- **Technical documentation**: See `docs/` directory
- **OpenAPI specifications**: Review `docs/openapi-specs/`
- **OpenCLI specifications**: Review `docs/opencli-specs/`
- **Schema definitions**: Browse `docs/manifest-schemas/`
