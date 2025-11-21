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

## License

By contributing to Amp, you agree that your contributions will be licensed according to the terms specified in the [Contributor License Agreement](CLA.md). The project code is licensed under the terms in the [LICENSE](LICENSE) file.

---

Thank you for your contributions to Amp!
