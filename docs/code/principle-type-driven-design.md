---
name: "principle-type-driven-design"
description: "Type-Driven Design — make illegal states unrepresentable. Load when designing data types, modeling domain state, or reviewing types that allow invalid combinations"
type: "principle"
scope: "global"
---

# Type-Driven Design (Make Illegal States Unrepresentable)

**MANDATORY for ALL code in the workspace**

## Rule

Design types so that invalid states cannot be constructed. Parse and validate at the boundary, then use types that structurally prevent illegal combinations throughout the codebase.

If a struct has multiple `Option` fields where certain combinations are invalid (e.g., both being `None`), the type allows illegal states. Replace it with an enum or restructured type that makes only valid combinations representable.

## Examples

1. **Enum over multiple Options**
When a struct has multiple `Option` fields where certain combinations are invalid, use an enum that only allows valid states.

```rust
// Bad — both fields None is invalid but representable
struct ContactInfo {
    email: Option<Email>,
    phone: Option<Phone>,
}

// A ContactInfo with email: None, phone: None is meaningless
// but the type allows it — every consumer must check defensively
```

```rust
// Good — the type ensures at least one contact method exists
enum ContactInfo {
    Email(Email),
    Phone(Phone),
    Both { email: Email, phone: Phone },
}

// No variant allows zero contact methods — invalid state is unrepresentable
```

2. **Newtypes for validated domain data**
Raw strings for domain concepts push validation responsibility onto every consumer. Parse once at the boundary into a newtype.

```rust
// Bad — raw String for validated domain data
fn process_url(url: String) {
    // Is this validated? Who knows. Every caller must wonder.
    // Every function downstream must re-validate or trust blindly.
}
```

```rust
// Good — parse, don't validate
struct Url(String);

impl Url {
    pub fn parse(input: String) -> Result<Self, ParseError> {
        // Validate once at the boundary
        if !is_valid_url(&input) {
            return Err(ParseError::InvalidUrl);
        }
        Ok(Self(input))
    }
}

fn process_url(url: Url) {
    // Type guarantees validity — no defensive checks needed
}
```

## Why It Matters

Bugs from invalid states are caught at compile time instead of runtime. When the type system prevents illegal combinations, you eliminate entire categories of defects: null checks you forgot to write, impossible states that slip through code review, edge cases that only surface in production. The result is fewer defensive checks scattered throughout the codebase and more confidence that if the code compiles, the data is valid.

## Pragmatism Caveat

Don't encode every business rule in types — encode structural invariants (invalid combinations of fields, data that must always be present together), not transient business logic that changes frequently. A discount percentage cap or a rate limit threshold belongs in runtime validation, not in the type system. Reserve type-level encoding for invariants that are fundamental to correctness and unlikely to change.

## Checklist

Before committing code, verify:

- [ ] Structs with multiple `Option` fields reviewed for invalid combinations that the type permits
- [ ] Domain concepts use newtypes that validate on construction, not raw primitives
- [ ] Enums used to represent mutually exclusive or dependent states instead of flag fields
- [ ] Validation happens at the boundary (parsing), not repeatedly throughout the codebase
- [ ] No defensive runtime checks for invariants already guaranteed by types


## References

- [principle-validate-at-edge](principle-validate-at-edge.md) - Related: Where validation happens — at system boundaries before data enters the domain
- [pattern-typestate](pattern-typestate.md) - Related: Typestate pattern for modeling state machines with distinct types
- [pattern-builder](pattern-builder.md) - Related: Builder pattern for complex object construction with required fields

## External References

- [Designing with Types: Making Illegal States Unrepresentable (F# for Fun and Profit)](https://fsharpforfunandprofit.com/posts/designing-with-types-making-illegal-states-unrepresentable/)
- [Parse, Don't Validate and Type-Driven Design in Rust](https://www.harudagondi.space/blog/parse-dont-validate-and-type-driven-design-in-rust/#maxims-of-type-driven-design)
- [The Ultimate Guide to Rust Newtypes](https://www.howtocodeit.com/guides/ultimate-guide-rust-newtypes)
- [Using Types To Guarantee Domain Invariants](https://lpalmieri.com/posts/2020-12-11-zero-to-production-6-domain-modelling/)
