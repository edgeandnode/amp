---
name: "principle-law-of-demeter"
description: "Law of Demeter (Principle of Least Knowledge) for loose coupling. Load when reviewing method call chains, struct field access patterns, or coupling concerns"
type: "principle"
scope: "global"
---

# Law of Demeter (Principle of Least Knowledge)

**MANDATORY for ALL code in the workspace**

## Rule

A method should only call methods on objects it directly knows about. Don't reach through chains of objects to access something deep inside—each unit of code should only talk to its immediate collaborators.

A method `M` of an object `O` should only call methods on:

- `O` itself (methods on `self`)
- Objects passed as arguments to `M`
- Objects created by `M`
- Objects held in instance variables of `O`

If you find yourself chaining calls like `objectA.getB().getC().doSomething()`, you're violating this principle. Stop the chain at `getB()` or earlier—if you need data from a distant object, ask your direct collaborator to provide it instead of navigating through the object graph yourself.

**Not violations**: Fluent APIs where each call returns the same logical object are not reaching through collaborators. Builder chains (`Request::builder().method(GET).uri("/foo").body(())`), iterator adapters (`.filter(..).map(..).collect()`), and `Result`/`Option` combinators (`.map_err(..).context(..)`) operate on the same type, not progressively deeper objects.

## Examples

1. **Direct collaborator access**
Accessing nested data should go through the immediate collaborator, not chain through the object graph.

```rust
// Bad — reaches through the object graph
let name = order.get_customer().get_address().get_city();

// Good — ask the direct collaborator
let name = order.shipping_city();
```

2. **Receive what you need as a parameter**
Instead of navigating through intermediate objects to find configuration, accept the value directly.

```rust
// Bad — chaining through intermediate objects
fn process(registry: &Registry) {
    let timeout = registry.get_config().get_network().get_timeout();
}

// Good — receive what you need directly
fn process(timeout: Duration) {
    // use timeout
}
```

## Why It Matters

Violating this principle creates tight coupling between components that shouldn't know about each other. 
When the internal structure of a deeply nested object changes, every caller that reached into it breaks. 
Following it keeps modules loosely coupled and independently changeable.

## Pragmatism Caveat

In exceptional cases, a small violation may be justified if it genuinely simplifies the code without introducing meaningful coupling risk. 
This should be vanishingly rare and **must** be accompanied by a comment explaining why the violation is acceptable and why the standard alternatives (wrapper method, passing the value directly) were not used. 
An undocumented violation should be treated as incorrect.


## Checklist

Before committing code, verify:

- [ ] Methods interact with immediate collaborators, not distant objects reached through navigation chains
- [ ] Required data is provided via direct methods or explicit parameters rather than deep traversal
- [ ] Any deliberate exception is small in scope and documented with rationale
- [ ] Fluent/combinator chains on the same logical object are distinguished from true reach-through coupling


## External References

- [Law of Demeter — Principle of Least Knowledge](https://dev.to/dazevedo/law-of-demeter-principle-of-least-knowledge-35l2)
