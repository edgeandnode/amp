---
name: "principle-inversion-of-control"
description: "Inversion of Control — accept dependencies, don't create them. Load when designing components, managing dependencies, or improving testability"
type: "principle"
scope: "global"
---

# Inversion of Control (Dependency Injection)

**MANDATORY for ALL code in the workspace**

## Rule

Don't let components create or locate their own dependencies — pass them in from the caller. A component should declare what it needs (via constructor parameters or trait bounds), not decide where to get it.

Inject a dependency when either of these signals is true:

1. **It performs I/O**: The dependency makes network calls, reads from disk, or queries a database. I/O dependencies must be injectable so tests can avoid real external systems.
2. **It varies across contexts**: Production and test code currently need different instances (e.g., strategy selection, feature flags, configuration profiles). If only one implementation exists and no test needs a substitute, don't inject preemptively.

If neither signal fires — the type is deterministic, has no side effects, and never needs swapping (e.g., `Vec`, `String`, `HashMap`) — hardcoding is fine.

## Examples

1. **Inject collaborators as function parameters**
A function that creates its own HTTP client cannot be tested or reused with different configurations.

```rust
// Bad — function creates its own HTTP client internally
async fn fetch_price(token: &str) -> Result<f64> {
    let client = reqwest::Client::new();
    let resp = client.get(format!("https://api.example.com/price/{token}"))
        .send()
        .await?;
    resp.json().await
}
```

```rust
// Good — caller provides the HTTP client
async fn fetch_price(client: &reqwest::Client, token: &str) -> Result<f64> {
    let resp = client.get(format!("https://api.example.com/price/{token}"))
        .send()
        .await?;
    resp.json().await
}
```

2. **Generic structs over trait bounds**
A struct hardcoded to a concrete dependency cannot be tested without the real implementation.

```rust
// Bad — struct hardcodes a concrete implementation
struct OrderProcessor {
    db: PostgresPool,
}

impl OrderProcessor {
    fn new(connection_string: &str) -> Self {
        // Creates its own dependency — can't swap for testing
        Self {
            db: PostgresPool::connect(connection_string),
        }
    }
}
```

```rust
// Good — struct is generic over a trait, implementation injected at construction
trait OrderStore {
    async fn save(&self, order: &Order) -> Result<()>;
    async fn find(&self, id: OrderId) -> Result<Option<Order>>;
}

struct OrderProcessor<S> {
    store: S,
}

impl<S: OrderStore> OrderProcessor<S> {
    fn new(store: S) -> Self {
        Self { store }
    }

    async fn process(&self, order: &Order) -> Result<()> {
        // Uses the injected store — works with any implementation
        self.store.save(order).await
    }
}

// Production: OrderProcessor::new(PostgresOrderStore::new(pool))
// Tests:      OrderProcessor::new(MockOrderStore::new())
```

## Why It Matters

When components create their own dependencies, they become tightly coupled to concrete implementations. You can't test the order processor without a real database. You can't reuse the price fetcher with a different HTTP client configuration. You can't reason about what a function depends on without reading its body. The two signals above make this mechanically checkable: does it do I/O, or would a test need a different instance?

Inversion of Control makes dependencies explicit. The function signature or struct definition tells you exactly what collaborators are needed. This enables independent testing (swap in mocks), reuse across contexts (different callers provide different implementations), and clearer reasoning (explicit dependencies vs hidden ones).

## Pragmatism Caveat

Not every dependency needs injection. If neither signal fires — the type performs no I/O and never needs swapping — injection adds noise without benefit. Over-injecting trivial dependencies clutters function signatures and obscures the real collaborators.

When a signal fires but you intentionally hardcode the dependency, add a brief comment explaining why (e.g., the concrete type is the only implementation and wrapping it in a trait would add complexity for zero testability benefit).

## Checklist

Before committing code, verify:

- [ ] Components declare collaborators explicitly rather than locating or constructing them implicitly
- [ ] Dependencies with side effects or context-specific behavior are caller-controlled
- [ ] Hardcoded dependencies are limited to deterministic, context-invariant utilities
- [ ] Deviations from injection are intentional, local, and documented
- [ ] Tests can substitute external collaborators without changing production code


## External References

- [Beginner's Guide to Inversion of Control (HackerNoon)](https://hackernoon.com/beginners-guide-to-inversion-of-control)
- [Understanding Inversion of Control Principle (Medium)](https://medium.com/@amitkma/understanding-inversion-of-control-ioc-principle-163b1dc97454)
- [Inversion of Control (Kent C. Dodds)](https://kentcdodds.com/blog/inversion-of-control)
- [Dependency Injection in .NET (Manning)](https://livebook.manning.com/book/dependency-injection-in-dot-net/about-this-book)
