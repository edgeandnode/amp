# Contributing to Amp

Thank you for your interest in contributing to Amp! We welcome contributions from the community and are grateful for your support.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [How to Contribute](#how-to-contribute)
- [Development Workflow](#development-workflow)
- [Contributor License Agreement](#contributor-license-agreement)
- [Coding Standards](#coding-standards)
- [Submitting Changes](#submitting-changes)
- [Getting Help](#getting-help)

## Code of Conduct

We are committed to providing a welcoming and inclusive experience for everyone. We expect all contributors to be respectful and professional in their interactions.

## Getting Started

See the [Installation section in the README](README.md#installation) for setup instructions.

## How to Contribute

We welcome various types of contributions:

- **Bug Reports**: Found a bug? Open an issue with a clear description and reproduction steps
- **Feature Requests**: Have an idea? Open an issue to discuss it first
- **Documentation**: Help improve our docs, examples, or code comments
- **Code Contributions**: Fix bugs, implement features, or improve performance
- **Testing**: Add test coverage or help with integration testing

### Reporting Bugs

When filing a bug report, please include:

- A clear, descriptive title
- Steps to reproduce the issue
- Expected vs. actual behavior
- Your environment (OS, Rust version, etc.)
- Any relevant logs or error messages
- Minimal code example if applicable

### Suggesting Enhancements

When suggesting a feature:

- Check if it's already been requested
- Provide a clear use case and rationale
- Explain why this enhancement would be useful to the community
- Consider implementation complexity and alternatives

## Development Workflow

Amp uses a structured development workflow with automated tooling via [just](https://github.com/casey/just) task runner.

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/bug-description
```

### 2. Format Code

Format your code before committing:

```bash
# Format all Rust code
just fmt

# Check formatting without making changes
just fmt-check

# Format TypeScript code
just fmt-ts
```

### 3. Check and Lint

Run compilation checks and clippy linter:

```bash
# Check Rust code compiles
just check

# Run clippy linter (fix all warnings)
just clippy

# Check a specific crate
just check-crate <crate-name>
just clippy-crate <crate-name>
```

### 4. Run Tests

```bash
# Run all tests (unit + integration)
just test

# Run unit tests only
just test-unit

# Run integration tests only
just test-it
```

**Note:** For faster test execution, install [cargo-nextest](https://nexte.st/):

```bash
cargo install --locked cargo-nextest@^0.9
```

## Contributor License Agreement

**Before your first contribution can be merged, you must sign our Contributor License Agreement (CLA).**

### How to Sign

1. **Read the [CLA](CLA.md)** carefully - it's a legally binding document
2. When you submit your **first pull request**, the CLA Assistant bot will comment on your PR
3. Follow the bot's instructions to sign the CLA electronically (via GitHub comment)
4. Once signed, the bot will update the PR status and your contribution can proceed

**Note**: You only need to sign the CLA once. It covers all your future contributions to Amp.

### AI-Generated Contributions

If you used **AI tools** to generate any part of your contribution, you must:

- **Disclose this in your pull request description**
  - Examples: GitHub Copilot, Claude Code, ChatGPT, Cursor, etc.
- **Ensure the AI-generated code complies with the CLA terms**
- **Verify that you have the rights to submit the AI-generated output**
- **Review and test all AI-generated code** - you are responsible for its correctness

The CLA explicitly covers AI-generated contributions. See [CLA](CLA.md) Section 1 for the definition of "AI System" and your responsibilities.

### Commit Messages

Write clear, descriptive commit messages following conventional commits format:

```
<type>(<scope>): <short summary>

<optional detailed description>

<optional footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `refactor`: Code refactoring (no functional changes)
- `test`: Test additions or modifications
- `chore`: Maintenance tasks (dependencies, tooling, etc.)
- `perf`: Performance improvements

**Scopes** (use the crate or component name):
- `metadata-db`, `admin-api`, `dataset-store`, `ampd`, etc.

**Example:**
```
feat(query): add support for streaming queries

Implements Arrow Flight streaming for large result sets.
This reduces memory usage and improves latency for queries
returning millions of rows.

Closes #123
```

## Coding Standards

Follow the coding patterns and standards documented in [`docs/code/`](docs/code/). Key patterns include:

- **Error handling**: Proper use of `Result<T, E>` and error types
- **Module structure**: Correct module organization and visibility
- **Type design**: Appropriate use of newtypes, enums, and type aliases

All code must pass `just fmt-check` and `just clippy` with zero warnings.

## Submitting Changes

Please do not merge `main` into your branch as you develop your pull request. Instead, rebase your branch on top of the latest `main` if your pull request branch is long-lived.

We try to keep the history of the `main` branch linear and avoid merge commits.

### Using Claude Code

Contributors using [Claude Code](https://claude.ai/claude-code) can leverage project-specific skills in `.claude/skills/`:

| Skill | Purpose |
|-------|---------|
| `/code-format` | Format Rust and TypeScript code |
| `/code-check` | Run compilation checks and clippy |
| `/code-test` | Run unit and integration tests |
| `/code-review` | Review code changes |
| `/commit` | Generate commit messages |
| `/code-pattern-discovery` | Load coding patterns |
| `/feature-discovery` | Load feature documentation |

## License

By contributing to Amp, you agree that your contributions will be licensed according to the terms specified in the [Contributor License Agreement](CLA.md). The project code is licensed under the terms in the [LICENSE](LICENSE) file.

---

Thank you for your contributions to Amp!
