---
name: "principle-least-surprise"
description: "Principle of Least Surprise — follow Rust idioms and conventions. Load when naming functions, designing constructors, implementing traits, or reviewing API surfaces"
type: "principle"
scope: "global"
---

# Principle of Least Surprise (Follow Rust Idioms and Conventions)

**MANDATORY for ALL code in the workspace**

## Rule

Code should behave the way a Rust developer expects it to behave based on its name, signature, and the idioms of the language. **The Rust standard library is the primary reference** — when `std` establishes a pattern for naming, trait usage, or method semantics, follow it. A function name is a semantic contract — if a developer can correctly guess what your function does, what it returns, and whether it changes state without reading the implementation, you've upheld this principle.

Follow these specific conventions (all derived from `std` patterns):

1. **Standard constructors**: Use `new` for infallible construction that takes ownership of its arguments. Use `From`/`Into` for infallible type conversions. Use `TryFrom` for fallible conversions that can fail with a typed error. Never invent custom constructor names like `create` or `make` when `new`, `From`, or `TryFrom` conveys the same intent.

2. **Standard parsing**: Implement `std::str::FromStr` for types that can be parsed from a string, instead of custom `parse`, `from_string`, or `from_str` inherent methods. This unlocks `"value".parse::<MyType>()` for free — the idiomatic way to parse strings in Rust.

3. **Conventional method prefixes**: Follow the verb conventions Rust developers rely on to predict behavior without reading source code:
   - `get_*` / `set_*`: Field access, simple and cheap. Avoid `get_` entirely when a bare noun suffices (prefer `len()` over `get_len()`).
   - `with_*`: Returns a modified copy or consumes self to produce a new value; does not mutate in place.
   - `to_*`: Expensive conversion that produces a new owned value (e.g., `to_string`, `to_vec`).
   - `as_*`: Cheap, borrowed view of the data (e.g., `as_str`, `as_bytes`). Must not allocate.
   - `into_*`: Consumes self, converting to a different type (e.g., `into_inner`, `into_vec`).
   - `is_*` / `has_*`: Returns `bool`. Never return anything else.
   - `try_*`: Fallible variant of an operation that would otherwise panic or return a non-`Result` type.

4. **Symmetry and consistency**: If your codebase uses `start`/`stop`, don't introduce `begin`/`halt`. If you have `add`, the inverse is `remove`, not `delete_item`. Broken patterns force developers to consult docs for what should be guessable.

5. **No hidden side effects**: A function named `calculate_tax` must not write to a database, send an email, or mutate global state. If a function has side effects, its name must reflect them. Better yet, split computation from side effects.

## Examples

1. **Use `From`/`TryFrom` instead of custom constructors**
Standard conversion traits give callers a predictable API and unlock `.into()` ergonomics.

```rust
// Bad — custom constructor name when TryFrom conveys the same intent
struct Port(u16);

impl Port {
    fn create(value: u32) -> Result<Self, PortError> {
        let port = u16::try_from(value).map_err(|_| PortError::OutOfRange)?;
        if port == 0 {
            return Err(PortError::Zero);
        }
        Ok(Self(port))
    }
}

// Caller must discover and remember the non-standard name
let port = Port::create(8080)?;
```

```rust
// Good — TryFrom is the idiomatic trait for fallible conversions
struct Port(u16);

impl TryFrom<u32> for Port {
    type Error = PortError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        let port = u16::try_from(value).map_err(|_| PortError::OutOfRange)?;
        if port == 0 {
            return Err(PortError::Zero);
        }
        Ok(Self(port))
    }
}

// Caller uses the standard pattern — no surprises
let port = Port::try_from(8080)?;
```

2. **Implement `FromStr` instead of custom parsing methods**
Custom parsing methods hide functionality that `FromStr` makes discoverable and composable.

```rust
// Bad — custom method name hides standard parsing intent
struct SubgraphId(String);

impl SubgraphId {
    fn from_string(s: &str) -> Result<Self, ParseError> {
        if s.is_empty() {
            return Err(ParseError::Empty);
        }
        if !s.starts_with("Qm") {
            return Err(ParseError::InvalidPrefix);
        }
        Ok(Self(s.to_owned()))
    }
}

// Caller must know about the non-standard method name
let id = SubgraphId::from_string("QmHash123")?;
```

```rust
// Good — FromStr enables the standard .parse() pattern
struct SubgraphId(String);

impl std::str::FromStr for SubgraphId {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(ParseError::Empty);
        }
        if !s.starts_with("Qm") {
            return Err(ParseError::InvalidPrefix);
        }
        Ok(Self(s.to_owned()))
    }
}

// Callers use the idiomatic pattern — works with any string type
let id: SubgraphId = "QmHash123".parse()?;
```

3. **Follow `as_` / `to_` / `into_` conventions**
Misusing these prefixes breaks the developer's mental model of cost and ownership.

```rust
// Bad — `as_` prefix but allocates a new String (expensive, not a borrow)
impl DeploymentId {
    fn as_string(&self) -> String {
        format!("deployment-{}", self.0)
    }
}

// Bad — `to_` prefix but takes ownership of self (should be `into_`)
impl RawConfig {
    fn to_validated(self) -> ValidatedConfig {
        ValidatedConfig { inner: self.inner }
    }
}
```

```rust
// Good — `as_` returns a borrowed view (cheap, no allocation)
impl DeploymentId {
    fn as_str(&self) -> &str {
        &self.0
    }
}

// Good — `to_` creates a new owned value (may allocate)
impl DeploymentId {
    fn to_display_string(&self) -> String {
        format!("deployment-{}", self.0)
    }
}

// Good — `into_` consumes self for zero-cost conversion
impl RawConfig {
    fn into_validated(self) -> ValidatedConfig {
        ValidatedConfig { inner: self.inner }
    }
}
```

4. **Boolean methods use `is_` / `has_` prefixes**
Developers seeing `is_` or `has_` expect `bool`. Returning anything else is a high-surprise violation.

```rust
// Bad — `is_` prefix but returns an Option, not a bool
impl Job {
    fn is_complete(&self) -> Option<CompletionTime> {
        self.completed_at
    }
}
```

```rust
// Good — `is_` returns bool, separate accessor for the data
impl Job {
    fn is_complete(&self) -> bool {
        self.completed_at.is_some()
    }

    fn completed_at(&self) -> Option<CompletionTime> {
        self.completed_at
    }
}
```

5. **No hidden side effects behind pure-looking names**
A function named after a computation must not secretly perform I/O or mutate shared state.

```rust
// Bad — name suggests pure computation, but writes to database
fn calculate_total(items: &[LineItem], db: &Database) -> Result<Money> {
    let total = items.iter().map(|i| i.price).sum();
    db.update_order_total(total)?; // hidden side effect
    Ok(total)
}
```

```rust
// Good — separate computation from side effects
fn calculate_total(items: &[LineItem]) -> Money {
    items.iter().map(|i| i.price).sum()
}

// Side effect is explicit in the caller
let total = calculate_total(&items);
db.update_order_total(total)?;
```

## Why It Matters

Every deviation from Rust idioms forces developers to stop and read the implementation to understand what a function does. This slows down code review, increases the chance of misuse, and makes the codebase harder to navigate. When code follows conventions, developers build accurate mental models from names alone — they can compose APIs correctly without consulting docs for every call. When code breaks conventions, bugs emerge from false assumptions: a developer calls `as_string()` in a hot loop because `as_` means "cheap borrow," not realizing it allocates on every call.

The `std` library is the gold standard because every Rust developer has internalized its patterns. When your types behave like `std` types — `FromStr` for parsing, `From`/`TryFrom` for conversions, `as_`/`to_`/`into_` for access semantics — developers apply their existing mental model without friction. Standard trait implementations also unlock ecosystem integration — serde, clap, and other libraries can use `From`, `TryFrom`, `FromStr`, and `Display` automatically. Custom methods require custom glue code.

## Pragmatism Caveat

Conventions are guidelines, not laws. When a domain term is clearer than the conventional prefix, the domain term wins — but this should be rare and justified. For example, a method named `compile` on a query builder is clearer than `into_compiled` even though it consumes `self`, because "compile" is the established domain verb.

When you deviate from a convention, add a brief comment explaining why the idiomatic alternative was not used. Undocumented deviations are always wrong — they leave the next developer guessing whether the deviation was intentional or accidental.

## Checklist

- [ ] Constructors use `new`, `From`, or `TryFrom` — not custom names like `create`, `make`, or `build` (unless it's a builder pattern)
- [ ] Types parseable from strings implement `FromStr`, not custom `from_string` / `parse_from` methods
- [ ] `as_*` methods are cheap borrows; `to_*` methods produce owned values; `into_*` methods consume self
- [ ] `is_*` and `has_*` methods return `bool` and nothing else
- [ ] Functions named after computations are pure — side effects are explicit in names or separated into distinct functions
- [ ] Symmetric operations use conventional pairs (`start`/`stop`, `add`/`remove`, `open`/`close`)

## References

- [principle-type-driven-design](principle-type-driven-design.md) - Related: Newtypes and validated types that benefit from idiomatic trait implementations
- [principle-validate-at-edge](principle-validate-at-edge.md) - Related: Edge validation via `FromStr` and `TryFrom` as idiomatic parsing boundaries

## External References

- [Rust API Guidelines — Naming](https://rust-lang.github.io/api-guidelines/naming.html)
- [Principle of Least Surprise (principles-wiki.net)](https://principles-wiki.net/principles:principle_of_least_surprise)
- [The Principle of Least Astonishment](https://dev.to/notmattlucas/the-principle-of-least-astonishment-3f9k)
- [What is the Principle of Least Astonishment?](https://softwareengineering.stackexchange.com/a/187462)
- [Wat — A lightning talk by Gary Bernhardt](https://www.destroyallsoftware.com/talks/wat)
