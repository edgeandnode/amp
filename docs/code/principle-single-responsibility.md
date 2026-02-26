---
name: "principle-single-responsibility"
description: "Single Responsibility Principle (SRP) for focused module design. Load when designing structs, splitting modules, or reviewing types with multiple concerns"
type: "principle"
scope: "global"
---

# Single Responsibility Principle (SRP)

**MANDATORY for ALL code in the workspace**

## Rule

A struct or module should focus on a single responsibility. Split when any of these observable signals are present:

1. **Multiple external systems**: A struct's methods touch multiple distinct external systems (database, HTTP, filesystem, message queue). Each system boundary is a separate concern. Three or more is a strong signal; two may warrant splitting if the systems change independently.
2. **Disjoint field access**: A struct's methods can be grouped into subsets where each subset accesses a different set of fields with zero overlap. The groups are independent responsibilities sharing a struct by coincidence.
3. **Mixed I/O and transformation**: An `impl` block mixes I/O methods (network calls, disk reads, database queries) with pure transformation methods (parsing, formatting, computing). Separate the pure logic from the effectful operations.

When a signal fires, split the struct into focused types that each own one concern, then compose them together.

## Examples

A restaurant struct that handles orders, inventory, and billing has three reasons to change. Split into focused types composed together.

```rust
// Bad — one struct handling orders, inventory, and billing
struct Restaurant {
    orders: Vec<Order>,
    inventory: HashMap<String, u32>,
    revenue: f64,
}

impl Restaurant {
    fn place_order(&mut self, item: &str) {
        // Manages order tracking
        self.orders.push(Order::new(item));
        // Manages inventory
        *self.inventory.get_mut(item).unwrap() -= 1;
        // Manages billing
        self.revenue += self.price_for(item);
    }

    fn restock(&mut self, item: &str, qty: u32) { /* inventory logic */ }
    fn generate_report(&self) -> String { /* billing logic */ }
    fn cancel_order(&mut self, id: u64) { /* order + inventory + billing logic */ }
}
```

```rust
// Good — separate structs each owning one responsibility, composed together
struct OrderService {
    orders: Vec<Order>,
}

impl OrderService {
    fn place(&mut self, item: &str) -> OrderId {
        let order = Order::new(item);
        let id = order.id;
        self.orders.push(order);
        id
    }

    fn cancel(&mut self, id: OrderId) -> Option<Order> {
        // Only order tracking logic
    }
}

struct Inventory {
    stock: HashMap<String, u32>,
}

impl Inventory {
    fn reserve(&mut self, item: &str) -> Result<(), OutOfStock> {
        // Only inventory logic
    }

    fn restock(&mut self, item: &str, qty: u32) {
        // Only inventory logic
    }
}

struct Billing {
    revenue: f64,
}

impl Billing {
    fn charge(&mut self, amount: f64) {
        // Only billing logic
    }

    fn generate_report(&self) -> String {
        // Only billing logic
    }
}

// Compose the focused types together
struct Restaurant {
    orders: OrderService,
    inventory: Inventory,
    billing: Billing,
}
```

## Why It Matters

The underlying motivation is that a type should have only one reason to change. When a type has multiple responsibilities, every change carries the risk of breaking unrelated functionality. The signals above make this motivation mechanically checkable rather than relying on subjective judgment about "reasons to change."

Splitting responsibilities improves testability—each unit can be tested in isolation without setting up unrelated state. It improves maintainability—developers can understand and modify one concern without navigating others. And it isolates change—a new billing rule doesn't require touching inventory logic.

## Pragmatism Caveat

Not every struct with two fields needs to be split. If none of the three signals fire—methods share fields, touch the same external system, and I/O is not mixed with pure logic—the struct is fine as a single unit. SRP is about observable coupling between concerns, not minimizing field count.

When a signal fires but you intentionally keep the responsibilities together, add a brief comment explaining why they are co-located (e.g., performance, transactional atomicity).

## Checklist

Before committing code, verify:

- [ ] Each type has a clear, cohesive responsibility that can be described in one sentence
- [ ] Changes to one concern do not routinely require touching unrelated concerns in the same type
- [ ] Effectful orchestration and pure transformation logic are separated when they evolve independently
- [ ] Intentional co-location of multiple concerns is documented with explicit tradeoffs


## External References

- [Single Responsibility Principle with a Rust Example](https://medium.com/@dogabudak/single-responsibility-principle-with-a-rust-example-2940504e3ebd)
