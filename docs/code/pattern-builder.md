---
name: "pattern-builder"
description: "Builder pattern for complex object construction with required fields. Load when designing constructors with multiple required parameters or optional configuration"
type: core
scope: "global"
---

# Builder Pattern for Required Fields

**MANDATORY for ALL Rust code in the workspace**

## Rule

Use the builder pattern when construction has multiple required fields. The built type should contain no `Option` fields for data that must always be present — the builder holds the optionality, and `build()` enforces completeness.

If a struct exposes required data as `Option` fields because "they're set during construction," the type leaks its construction concerns into every consumer. Consumers should never need to unwrap fields that are guaranteed to exist.

## Examples

```rust
// Bad — easy to forget required fields, consumers deal with Option
pub struct Config {
    pub database_url: Option<String>,
    pub port: Option<u16>,
}

// Every consumer must unwrap or check fields that should always exist
fn connect(config: &Config) {
    let url = config.database_url.as_ref().expect("missing url"); // runtime panic risk
}
```

```rust
// Good — builder enforces completeness, built type has no Option for required fields
pub struct Config {
    database_url: String,  // No Option — guaranteed to exist
    port: u16,
}

pub struct ConfigBuilder {
    database_url: Option<String>,
    port: Option<u16>,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self { database_url: None, port: None }
    }

    pub fn database_url(mut self, url: String) -> Self {
        self.database_url = Some(url);
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn build(self) -> Result<Config, BuildError> {
        Ok(Config {
            database_url: self.database_url.ok_or(BuildError::MissingDatabaseUrl)?,
            port: self.port.ok_or(BuildError::MissingPort)?,
        })
    }
}

// config.database_url is String, not Option<String> — no unwrapping needed
```

```rust
// Good — type-state builder enforces required fields at compile time
pub struct ConfigBuilder<Url, Port> {
    database_url: Url,
    port: Port,
}

pub struct Missing;
pub struct Set<T>(T);

impl ConfigBuilder<Missing, Missing> {
    pub fn new() -> Self {
        Self { database_url: Missing, port: Missing }
    }
}

impl<Port> ConfigBuilder<Missing, Port> {
    pub fn database_url(self, url: String) -> ConfigBuilder<Set<String>, Port> {
        ConfigBuilder { database_url: Set(url), port: self.port }
    }
}

impl<Url> ConfigBuilder<Url, Missing> {
    pub fn port(self, port: u16) -> ConfigBuilder<Url, Set<u16>> {
        ConfigBuilder { database_url: self.database_url, port: Set(port) }
    }
}

impl ConfigBuilder<Set<String>, Set<u16>> {
    pub fn build(self) -> Config {
        Config {
            database_url: self.database_url.0,
            port: self.port.0,
        }
    }
}

// build() only available when all required fields are set — compile-time enforcement
```

## Why It Matters

When required data is represented as `Option` fields, every consumer must handle the `None` case for data that should never be absent. The builder pattern isolates construction complexity in one place and produces a type that unconditionally guarantees all required fields exist. This eliminates an entire class of runtime panics from unwrapping "always-present" optional fields.

## Pragmatism Caveat

Not every struct needs a builder. If a struct has 2-3 fields that are all required and always available at construction time, a simple `new()` constructor is clearer. Use the builder pattern when construction is genuinely complex: many fields, a mix of required and optional, or when the construction order matters. A type-state builder (compile-time enforcement) is ideal when misuse would be a serious bug; a runtime `build() -> Result` is fine for configuration-style objects where a clear error message suffices.

## Checklist

Before committing code, verify:

- [ ] Built types use concrete fields (not `Option`) for data that must always be present
- [ ] Builder's `build()` method validates all required fields are set
- [ ] Consumers of the built type never unwrap fields that the builder guarantees
- [ ] Simple structs with few required fields use `new()` instead of a builder
- [ ] Type-state builders considered for safety-critical construction where compile-time enforcement is warranted

## References

- [principle-type-driven-design](principle-type-driven-design.md) - Foundation: Design principle this pattern implements
- [pattern-typestate](pattern-typestate.md) - Related: Type-state pattern used for compile-time builder enforcement
