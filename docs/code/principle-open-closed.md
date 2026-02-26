---
name: "principle-open-closed"
description: "Open/Closed Principle (OCP) for extensible design via traits. Load when adding new variants, extending behavior, or reviewing match/if-else chains"
type: "principle"
scope: "global"
---

# Open/Closed Principle (OCP)

**MANDATORY for ALL code in the workspace**

## Rule

Software entities should be open for extension and resistant to modification of established behavior. Add new behavior by introducing new types, trait implementations, or composable modules rather than editing proven logic. Prefer extension points over repeated branching when any of these signals are present:

1. **Cross-boundary extension**: Behavior is extended by other crates or independently deployed modules. Define extension points so each consumer can add behavior without changing shared code.
2. **Externally growing variant space**: Variants map to external protocols, providers, or integrations that are expected to evolve. New variants should be additive, not invasive edits.
3. **Repeated branching sites**: The same branching logic appears across multiple locations. Consolidate dispatch behind an extension boundary to avoid coordinated edits.

When none of these signals fire and the behavior is intentionally closed and local, direct branching can be the clearer choice.

## Examples

1. **Trait-based dispatch for growing variant sets**
When new variants are expected, use traits so that adding a new type requires no changes to existing code.

```rust
// Bad — adding a new shape requires modifying this function
fn area(shape: &Shape) -> f64 {
    match shape {
        Shape::Circle(r) => std::f64::consts::PI * r * r,
        Shape::Rectangle(w, h) => w * h,
        // Every new shape forces a change here
    }
}
```

```rust
// Good — new shapes implement the trait without touching existing code
trait Area {
    fn area(&self) -> f64;
}

struct Circle {
    radius: f64,
}

impl Area for Circle {
    fn area(&self) -> f64 {
        std::f64::consts::PI * self.radius * self.radius
    }
}

struct Rectangle {
    width: f64,
    height: f64,
}

impl Area for Rectangle {
    fn area(&self) -> f64 {
        self.width * self.height
    }
}

// Adding a Triangle requires no changes to Circle, Rectangle, or Area
```

2. **Independent extension via trait implementations**
Notification channels can be added independently without modifying existing dispatch logic.

```rust
// Bad — notification logic must be modified for each new channel
fn notify(channel: &str, message: &str) {
    if channel == "email" {
        send_email(message);
    } else if channel == "slack" {
        send_slack(message);
    }
    // Adding SMS means editing this function
}

// Good — new channels implement the trait independently
trait Notifier {
    fn notify(&self, message: &str);
}

struct EmailNotifier;
impl Notifier for EmailNotifier {
    fn notify(&self, message: &str) {
        send_email(message);
    }
}

// Adding SmsNotifier requires no changes to existing notifiers
```

## Why It Matters

The underlying motivation is that software entities should be open for extension but closed for modification. Modifying existing code to add new behavior risks introducing regressions in functionality that already works. The signals above make this motivation mechanically checkable rather than relying on predictions about future extension.

Trait-based extension in Rust naturally supports this—new types implement existing traits without touching the implementations that are already tested and deployed.

## Pragmatism Caveat

Not every `match` or `if-else` needs to become a trait. If none of the three signals fire—the enum is crate-local, matched in 1–2 places, and represents internal state—a simple `match` is clearer and more maintainable than a trait hierarchy. Over-abstracting prematurely adds complexity without benefit.

When a signal fires but you intentionally keep a `match`, add a brief comment explaining why (e.g., the variant set is frozen by a protocol spec, or the match sites are tightly co-located).


## Checklist

Before committing code, verify:

- [ ] New behavior can be introduced primarily by adding new code, not editing multiple existing paths
- [ ] Expected growth points are modeled as explicit extension boundaries (traits, strategy objects, plugin-like modules)
- [ ] Repeated branching over evolving variants is consolidated rather than duplicated
- [ ] Intentionally closed variant sets are documented with rationale
- [ ] Adding a new variant or behavior minimizes risk to previously tested paths


## References

- [principle-law-of-demeter](principle-law-of-demeter.md) - Related: Complementary design principle for loose coupling

## External References

- [Understanding the Open/Closed Principle](https://dev.to/dazevedo/understanding-the-openclosed-principle-ocp-from-solid-keep-code-flexible-yet-stable-jo7)
