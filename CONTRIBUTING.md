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

## Coding Standards

- Follow standard formatting (enforced by `rustfmt` and `prettier`)
- Address **ALL** linting warnings (clippy for Rust, ESLint for TypeScript)
- Use proper error handling - avoid `unwrap()` except in tests
- Write tests for new functionality
- Add documentation comments for public APIs
- Keep changes focused and atomic
- Follow existing patterns in the codebase
- Never commit secrets, API keys, or sensitive data

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

## Submitting Changes

### Pull Request Process

1. **Ensure all checks pass locally**:
   ```bash
   just fmt-rs        # Format code
   just check-rs      # Check compilation
   just clippy        # Lint code (fix ALL warnings)
   just test-local    # Run tests
   ```

2. **Open a Pull Request** on GitHub:
   - Use a clear, descriptive title
   - Reference any related issues (e.g., "Fixes #123" or "Closes #456")
   - Describe what changed and why
   - Include any breaking changes or migration notes
   - **Disclose if you used AI tools** to generate any code
   - Add screenshots/examples for UI or behavior changes

3. **Sign the CLA** (first-time contributors):
   - The CLA Assistant bot will comment on your PR
   - Follow the instructions to sign electronically
   - Your PR cannot be merged until the CLA is signed

4. **Respond to feedback**:
   - Address review comments promptly
   - Push new commits to your branch (they'll appear in the PR)
   - Mark conversations as resolved when addressed
   - Re-run checks after changes

### PR Review Process

- All PRs require at least one approval from a maintainer
- CI checks must pass:
  - Code formatting (`fmt-check`)
  - Compilation (`check`)
  - Linting (`clippy` - zero warnings required)
  - Tests (`test`)
- The CLA must be signed
- PRs may require multiple rounds of review
- Be patient and responsive to feedback

## Getting Help

If you need assistance:

- **Questions**: Open a GitHub issue with the `question` label
- **Bugs**: Check existing issues or open a new one with detailed reproduction steps
- **General Discussion**: Comment on relevant issues or PRs

## Additional Resources

- **Rust Book**: https://doc.rust-lang.org/book/
- **Just Manual**: https://github.com/casey/just
- **Cargo Nextest**: https://nexte.st/
- **DataFusion**: https://arrow.apache.org/datafusion/
- **Apache Arrow**: https://arrow.apache.org/

## License

By contributing to Amp, you agree that your contributions will be licensed according to the terms specified in the [Contributor License Agreement](CLA.md). The project code is licensed under the terms in the [LICENSE](LICENSE) file.

---

Thank you for your contributions to Amp!
