# AGENTS.md

**Build & Test Commands**
`Build`: cargo build --workspace
`Lint`: cargo clippy --workspace --all-features
`Format`: cargo fmt --all
`Test`: cargo test --workspace
`Single Test (nozzle)`: cargo test -p nozzle -- <NAME> --nocapture
`Single Test (crate)`: cargo test -p <crate> -- <NAME> --nocapture
`Test Subset`: cargo test -p <crate> --tests
**Code Style**

- **Imports**: std, extern, crate-local; blank lines between groups
- **Formatting**: run `cargo fmt --all`; honor `rustfmt.toml`
- **Types & Errors**: explicit types; use `Result`/`Option` and propagate with `?`; contextualize errors
- **Naming**: snake_case for vars/functions; PascalCase for types; UPPER_SNAKE for constants
- **Docs**: public items have `///` docs; module docs too
- **Tests**: unit/integration tests; tests hermetic and deterministic
- **Security**: no secrets in code; use env vars
- **CI**: ensure `fmt` and `lint` pass; run tests in CI
  **Rules**
- Cursor/Copilot rules: not detected; will be included if present.
