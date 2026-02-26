# Project Amp - Technical Overview for Claude

## Project Summary

Project Amp is a high-performance ETL (Extract, Transform, Load) architecture for blockchain data services on The Graph. It focuses on extracting blockchain data from various sources, transforming it via SQL queries, and serving it through multiple query interfaces.

## Quick Start

**If you're an AI agent working on this codebase, here's what you need to know immediately:**

1. **Use Skills for operations** → Invoke skills (`/code-format`, `/code-check`, `/code-test`, `/code-gen`, `/feature-discovery`, `/docs-features-fmt-check`, `/feature-validate`) instead of running commands directly
2. **Skills wrap justfile tasks** → Skills provide the interface to `just` commands with proper guidance
3. **Follow the workflow** → Format → Check → Clippy → Targeted Tests (when needed)
4. **Fix ALL warnings** → Zero tolerance for clippy warnings
5. **Check patterns** → Use `/code-discovery` before implementing code

**Your first action**: If you need to run a command, invoke the relevant Skill! If you need to implement code, invoke
`/code-discovery` to load relevant guidelines!

**Testing default**: Run the smallest relevant test slice. Only broaden to `just test-local` for cross-cutting/high-risk changes or when you need broader confidence.

## Table of Contents

1. [Principles](#1-principles) - Core design principles guiding this codebase
2. [Code Guidelines](#2-coding-patterns) - Understanding coding standards via `/code-discovery` skill
3. [Development Workflow](#3-development-workflow) - How to develop with this codebase
4. [Feature Discovery](#4-feature-discovery) - Understanding project features via `/feature-discovery` skill
5. [Additional Resources](#5-additional-resources) - Links to documentation


## 1. Principles

**MANDATORY**: Before writing any code, read and internalize these 4 core design principles. Full details, examples, and checklists are in the linked docs — read them every time.

| Principle | One-liner | Full doc |
|-----------|-----------|----------|
| **Single Responsibility** | One struct = one reason to change | @docs/code/principle-single-responsibility.md |
| **Open/Closed** | Extend via new types/trait impls, don't modify existing code | @docs/code/principle-open-closed.md |
| **Law of Demeter** | Only talk to immediate collaborators — no `a.b().c().d()` chains | @docs/code/principle-law-of-demeter.md |
| **Validate at the Edge** | Hard shell (boundary validates), soft core (domain trusts) | @docs/code/principle-validate-at-edge.md |

Additional design principles (Type-Driven Design, Idempotency, Inversion of Control, etc.) are documented in `docs/code/principle-*.md`. Use `/code-discovery principles` to load them when relevant to your task.


## 2. Code Guidelines

Code guideline documentation lives in `docs/code/` with YAML frontmatter for dynamic discovery.

**Guideline docs are authoritative**: Guideline docs define how code should be written. All implementations MUST follow the patterns. If code doesn't follow a pattern, either fix the code or update the pattern (with team approval).

### Guideline Types

| Type               | Scope          | Purpose                                                       |
|--------------------|----------------|---------------------------------------------------------------|
| **Principle**      | `global`       | Universal software principles and best practices              |
| **Core**           | `global`       | Fundamental coding standards (error handling, modules, types) |
| **Architectural**  | `global`       | High-level patterns (services, workspace structure)           |
| **Crate-specific** | `crate:<name>` | Patterns for specific crates (admin-api, metadata-db)         |
| **Meta**           | `global`       | Documentation format specifications                           |

### Skill Invocation

| When You Need To                                              | Invoke This Skill         |
|---------------------------------------------------------------|---------------------------|
| Understand code guidelines before implementing                | `/code-discovery` |
| "How should I handle errors?", "What's the pattern for X?"    | `/code-discovery` |
| Load crate-specific guidelines for admin-api, metadata-db, etc. | `/code-discovery` |
| Creating or editing files in `docs/code/`                     | `/docs-code-fmt-check` |
| Reviewing PRs that modify guideline docs                        | `/docs-code-fmt-check` |

**Navigation:**

- Need to understand patterns? → `/code-discovery`
- Writing guideline docs? → `/docs-code-fmt-check` + [docs/__meta__/code.md](docs/__meta__/code.md)
- All patterns located in `docs/code/`


## 3. Development Workflow

**This section provides guidance for AI agents on how to develop with this codebase.**

### Documentation Structure: Separation of Concerns

This project uses three complementary documentation systems. Understanding their roles helps AI agents navigate efficiently:

| Documentation                  | Purpose                  | Content Focus                                                                                                                                                                |
|--------------------------------|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **AGENTS.md** (this file)      | **WHY** and **WHAT**     | Project architecture, policies, goals, and principles. Answers "What is this project?" and "Why do we do things this way?"                                                   |
| **Skills** (`.claude/skills/`) | **HOW** and **WHEN**     | Command-line operations and justfile usage. Answers "How do I run commands?" and "When should I use each command?"                                                           |
| **Patterns** (`docs/code/`)    | **HOW** (implementation) | Code implementation guidelines, standards, and guidelines (see [Code Guidelines](#2-coding-patterns)). Answers "How do I write quality code that follows project conventions?" |

**Navigation Guide for AI Agents:**

- Need to understand the project? → Read this file (AGENTS.md)
- Need to run a command? → Invoke the appropriate Skill (`/code-format`, `/code-check`, `/code-test`, `/code-gen`)
- Need to write code? → Use `/code-discovery` skill to load relevant guidelines

### Core Operating Principle

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
- Need to run tests? → Use `/code-test` skill (choose the smallest relevant scope)

### Command-Line Operations Reference

**🚨 CRITICAL: Use skills for all operations - invoke them before running commands.**

Available skills and their purposes:

- **Formatting**: Use `/code-format` skill - Format code after editing files
- **Checking/Linting**: Use `/code-check` skill - Validate and lint code changes
- **Testing**: Use `/code-test` skill - Run targeted tests when warranted
- **Code Generation**: Use `/code-gen` skill - Generate schemas and specs

Each Skill provides:

- ✅ **When to use** - Clear guidance on appropriate usage
- ✅ **Available operations** - All supported tasks with proper execution
- ✅ **Examples** - Real-world usage patterns
- ✅ **Pre-approved workflows** - Operations that can run without user permission
- ✅ **Workflow integration** - How operations fit into development flow

**Remember: If you don't know which operation to perform, invoke the appropriate Skill.**

### Pre-Implementation Checklist

**BEFORE writing ANY code, you MUST:**

1. **Understand the task** - Research the codebase and identify affected crate(s)
2. **Load implementation guidelines** - Use `/code-discovery` skill (see [Code Guidelines](#2-coding-patterns))
3. **Follow crate-specific guidelines** - Pattern discovery loads crate-specific and security guidelines automatically

### Typical Development Workflow

**Follow this workflow when implementing features or fixing bugs:**

#### 1. Research Phase

- Understand the codebase and existing guidelines
- Identify related modules and dependencies
- Review test files and usage examples
- Use `/code-discovery` to load relevant implementation guidelines

#### 2. Planning Phase

- **First**: Use `/code-discovery` to load relevant guidelines for the affected crate(s)
- Create detailed implementation plan based on the loaded guidelines
- Ensure plan follows required patterns (error handling, type design, module structure, etc.)
- Identify validation checkpoints
- Consider edge cases and error handling according to pattern guidelines
- Ask user questions if requirements are unclear

#### 3. Implementation Phase

**🚨 CRITICAL: Before running ANY command in this phase, invoke the relevant Skill.**

**Copy this checklist and track your progress:**

```
Development Progress:
- [ ] Step 1: Write code following patterns from Code Guidelines section (use /code-discovery)
- [ ] Step 2: Format code (use /code-format skill)
- [ ] Step 3: Check compilation (use /code-check skill)
- [ ] Step 4: Fix all compilation errors
- [ ] Step 5: Run clippy (use /code-check skill)
- [ ] Step 6: Fix ALL clippy warnings
- [ ] Step 7: Run targeted tests when warranted (use /code-test skill)
- [ ] Step 8: Fix test failures or document why tests were skipped
- [ ] Step 9: All required checks pass ✅
```

**Detailed workflow for each work chunk (and before committing):**

1. **Write code** following patterns from [Code Guidelines](#2-coding-patterns) (loaded via `/code-discovery`)

2. **Format before checks/commit**:
   - **Use**: `/code-format` skill when you finish a coherent chunk of work
   - **Validation**: Verify no formatting changes remain

3. **Check compilation**:
   - **Use**: `/code-check` skill after changes
   - **Must pass**: Fix all compilation errors
   - **Validation**: Ensure zero errors before proceeding

4. **Lint with clippy**:
   - **Use**: `/code-check` skill for linting
   - **Must pass**: Fix all clippy warnings
   - **Validation**: Re-run until zero warnings before proceeding

5. **Run targeted tests (when warranted)**:
   - **Use**: `/code-test` skill to select the smallest relevant scope
   - **Escalate**: Use `just test-local` only for cross-cutting/high-risk changes or when broader confidence is needed
   - **Validation**: Fix failures or record why tests were skipped

6. **Iterate**: If any validation fails → fix → return to step 2

**Visual Workflow:**

```
Edit File → /code-format skill
          ↓
    /code-check skill (compile) → Fix errors?
          ↓                            ↓ Yes
    /code-check skill (clippy) → (loop back)
          ↓
    Targeted tests (if needed) → Fix failures?
          ↓                 ↓ Yes
    All Pass ✅      (loop back)
```

**Remember**: Invoke Skills for all operations. If unsure which skill to use, refer to the Command-Line Operations Reference above.

#### 4. Completion Phase

- Ensure all required checks pass (format, check, clippy, and any tests you ran)
- If tests were skipped, document why and the risk assessment
- Review changes against patterns and security guidelines
- Document any warnings you couldn't fix and why

### Core Development Principles

**ALL AI agents MUST follow these principles:**

- **Research → Plan → Implement**: Never jump straight to coding
- **Guideline compliance**: Follow patterns from [Code Guidelines](#2-coding-patterns)
- **Zero tolerance for errors**: All automated checks must pass
- **Clarity over cleverness**: Choose clear, maintainable solutions
- **Security first**: Never skip security guidelines for security-sensitive crates

**Essential conventions:**

- **Never expose secrets/keys**: All sensitive data in environment variables
- **Maintain type safety**: Leverage Rust's type system fully
- **Prefer async operations**: This codebase uses async/await extensively
- **Run targeted tests when warranted**: Use `/code-test` skill and broaden only if necessary
- **Format code before checks/commit**: Use `/code-format` skill
- **Fix all warnings**: Use `/code-check` skill for clippy

### Summary: Key Takeaways for AI Agents

**You've learned the complete workflow. Here's what to remember:**

| What             | Where                                 | When                                |
|------------------|---------------------------------------|-------------------------------------|
| **Plan work**    | `/code-discovery`             | BEFORE creating any plan            |
| **Run commands** | `.claude/skills/`                     | Check Skills BEFORE any command     |
| **Write code**   | [Code Guidelines](#2-coding-patterns) | Load patterns before implementation |
| **Format**       | `/code-format`                        | Before checks or before committing  |
| **Check**        | `/code-check`                         | After formatting                    |
| **Lint**         | `/code-check`                         | Fix ALL warnings                    |
| **Test**         | `/code-test`                          | Validate changes with targeted tests when warranted |

**Golden Rules:**

1. ✅ Invoke Skills for all common operations
2. ✅ Skills wrap justfile tasks with proper guidance
3. ✅ Follow the workflow: Format → Check → Clippy → Targeted Tests (when needed)
4. ✅ Zero tolerance for errors and warnings
5. ✅ Every change improves the codebase

**Remember**: When in doubt, invoke the appropriate Skill!

## 4. Feature Discovery

Feature documentation lives in `docs/features/` with YAML frontmatter for dynamic discovery.

**Feature docs are authoritative**: If a feature doc exists, it is the ground truth. Implementation MUST align with documentation. If there's a mismatch, either fix the code or update the doc to reflect the correct state.

| When User Asks                                              | Invoke This Skill    |
|-------------------------------------------------------------|----------------------|
| "What is X?", "How does X work?", "Explain the Y feature"   | `/feature-discovery` |
| "What features does Amp have?", "What can this project do?" | `/feature-discovery` |
| Questions about project functionality or capabilities       | `/feature-discovery` |
| Need context before implementing a feature                  | `/feature-discovery` |
| Creating or editing files in `docs/features/`               | `/docs-features-fmt-check` |
| Reviewing PRs that modify feature docs (format check)       | `/docs-features-fmt-check` |
| "What's the status of feature X implementation?"            | `/feature-validate`  |
| Verify feature doc matches implementation                   | `/feature-validate`  |
| Check if documented functionality exists in code            | `/feature-validate`  |
| Audit feature docs for accuracy and test coverage           | `/feature-validate`  |

**Navigation:**

- Need to understand a feature? → `/feature-discovery`
- Writing feature docs? → `/docs-features-fmt-check` + [docs/__meta__/features.md](docs/__meta__/features.md)
- Validate implementation aligns with feature claims? → `/feature-validate`

## 5. Additional Resources

For more detailed information about the project:

- **Architecture overview**: See [docs/architecture.md](docs/architecture.md)
- **Technical documentation**: See `docs/` directory
- **OpenAPI specifications**: Review `docs/openapi-specs/`
- **OpenCLI specifications**: Review `docs/opencli-specs/`
- **Schema definitions**: Browse `docs/manifest-schemas/`
