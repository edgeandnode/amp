---
name: "principle-dry-wet"
description: "DRY and WET balance — when to deduplicate and when duplication is preferable to wrong abstraction. Load when extracting shared code, creating abstractions, or reviewing duplicated logic"
type: "principle"
scope: "global"
---

# DRY/WET Balance (Don't Repeat Yourself vs. Write Everything Twice)

**MANDATORY for ALL code in the workspace**

## Rule

Every piece of **knowledge** must have a single, unambiguous, authoritative representation within the system. Deduplicate when two pieces of code encode the same business rule, invariant, or decision. Do **not** deduplicate when two pieces of code merely look similar but represent independent concerns that will evolve separately.

Before extracting shared code, apply these checks:

1. **Same knowledge, not same shape**: The duplication encodes the same domain concept — a calculation formula, a validation rule, a protocol constraint. If two code blocks happen to look alike but serve different stakeholders or change for different reasons, they are coincidental duplication and should stay separate.
2. **Rule of Three**: Resist extracting on the first or second occurrence. Wait until you see the pattern a third time so you have enough evidence that the duplication is structural, not coincidental.
3. **Inline test for wrong abstraction**: If the shared code already has parameters or conditionals that toggle behavior per call-site, the abstraction may be wrong. Prefer inlining the code back into each caller and letting each evolve independently over adding another flag.

When in doubt, **duplication is far cheaper than the wrong abstraction** (Sandi Metz). Inlining a premature abstraction and tolerating temporary duplication is progress, not retreat.

## Examples

1. **Same knowledge — deduplicate**
Both functions compute the same discount formula. This is the same business rule duplicated.

```rust
// Bad — same discount formula duplicated in two places
fn online_price(base: f64) -> f64 {
    base * 0.9 // 10% discount
}

fn in_store_price(base: f64) -> f64 {
    base * 0.9 // 10% discount
}
```

```rust
// Good — single authoritative representation of the discount rule
fn apply_standard_discount(base: f64) -> f64 {
    base * 0.9
}
```

2. **Coincidental similarity — keep separate**
Two validation functions look alike today but serve different domains that will diverge.

```rust
// Bad — forced into one abstraction because the code looks similar
fn validate_input(value: &str, kind: &str) -> Result<String> {
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        return Err(anyhow!("{kind} must not be empty"));
    }
    if kind == "username" && trimmed.len() > 32 {
        return Err(anyhow!("username too long"));
    }
    if kind == "email" && !trimmed.contains('@') {
        return Err(anyhow!("invalid email"));
    }
    // More kind-specific branches accumulate here over time
    Ok(trimmed)
}
```

```rust
// Good — independent concerns stay separate, each free to evolve
fn validate_username(value: &str) -> Result<Username> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("username must not be empty"));
    }
    if trimmed.len() > 32 {
        return Err(anyhow!("username too long"));
    }
    Ok(Username(trimmed.to_owned()))
}

fn validate_email(value: &str) -> Result<Email> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("email must not be empty"));
    }
    if !trimmed.contains('@') {
        return Err(anyhow!("invalid email"));
    }
    Ok(Email(trimmed.to_owned()))
}
```

3. **Wrong abstraction — inline and restart**
A shared helper has accumulated conditionals to serve diverging call-sites. Inline it back.

```rust
// Bad — wrong abstraction with growing conditional complexity
fn build_query(table: &str, filters: &[Filter], include_deleted: bool, use_cache: bool) -> String {
    let mut q = format!("SELECT * FROM {table}");
    if !filters.is_empty() {
        q.push_str(" WHERE ");
        q.push_str(&filters.iter().map(|f| f.to_sql()).collect::<Vec<_>>().join(" AND "));
    }
    if !include_deleted {
        let keyword = if filters.is_empty() { " WHERE " } else { " AND " };
        q.push_str(&format!("{keyword}deleted_at IS NULL"));
    }
    if use_cache {
        q.push_str(" /* cached */");
    }
    q
}
```

```rust
// Good — each caller builds its own query, no shared conditional mess
fn build_user_query(filters: &[Filter]) -> String {
    let mut q = "SELECT * FROM users WHERE deleted_at IS NULL".to_string();
    for f in filters {
        q.push_str(&format!(" AND {}", f.to_sql()));
    }
    q
}

fn build_audit_query(filters: &[Filter]) -> String {
    // Audit queries include deleted records, different structure
    let mut q = "SELECT * FROM audit_log".to_string();
    if !filters.is_empty() {
        q.push_str(" WHERE ");
        q.push_str(&filters.iter().map(|f| f.to_sql()).collect::<Vec<_>>().join(" AND "));
    }
    q
}
```

4. **Extract shared knowledge into a trait**
When the shared concept is behavior rather than data, a trait provides a single authoritative definition without forcing unrelated call-sites together.

```rust
// Bad — retry logic duplicated across services
async fn fetch_prices(client: &Client, url: &str) -> Result<Prices> {
    for attempt in 0..3 {
        match client.get(url).send().await {
            Ok(resp) => return resp.json().await.map_err(Into::into),
            Err(e) if attempt < 2 => tokio::time::sleep(Duration::from_millis(100 << attempt)).await,
            Err(e) => return Err(e.into()),
        }
    }
    unreachable!()
}

async fn fetch_inventory(client: &Client, url: &str) -> Result<Inventory> {
    for attempt in 0..3 {
        match client.get(url).send().await {
            Ok(resp) => return resp.json().await.map_err(Into::into),
            Err(e) if attempt < 2 => tokio::time::sleep(Duration::from_millis(100 << attempt)).await,
            Err(e) => return Err(e.into()),
        }
    }
    unreachable!()
}
```

```rust
// Good — retry knowledge encoded once, reused via generic function
async fn with_retries<T, F, Fut>(max_attempts: u32, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    for attempt in 0..max_attempts {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) if attempt + 1 < max_attempts => {
                tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

// Each caller stays focused on its own concern
async fn fetch_prices(client: &Client, url: &str) -> Result<Prices> {
    with_retries(3, || async { Ok(client.get(url).send().await?.json().await?) }).await
}
```

## Why It Matters

Genuine duplication — the same business rule written in multiple places — means a change to that rule requires coordinated edits. Miss one and you have an inconsistency bug. Extracting shared knowledge into a single representation eliminates this class of defect.

However, premature deduplication is equally harmful. When code that merely *looks* alike is forced into a shared abstraction, every caller becomes coupled to every other caller's requirements. The abstraction accumulates parameters and conditionals to serve diverging needs, becoming harder to understand and modify than the original duplication ever was. Undoing a wrong abstraction is more expensive than never creating it.

The balance: deduplicate knowledge, tolerate duplicated code shapes.

## Pragmatism Caveat

The Rule of Three is a guideline, not a law. Sometimes two occurrences clearly encode the same invariant and the third will never come — deduplicating at two is fine when you are confident the knowledge is shared, not coincidentally similar. Conversely, even three occurrences should stay separate if they represent independent stakeholders likely to diverge.

When you choose to tolerate duplication intentionally, add a brief comment noting that the similarity is coincidental and the code should evolve independently. When you extract an abstraction, make sure it names the shared *concept*, not just the shared *shape*. An undocumented decision to deduplicate or to keep duplication is always wrong.

## Checklist

- [ ] Shared code encodes the same domain knowledge, not merely similar-looking syntax
- [ ] Extractions waited for at least three occurrences (or two with documented confidence)
- [ ] Shared abstractions have zero call-site-specific conditionals or behavior toggles
- [ ] Intentionally duplicated code has a comment explaining why it should remain separate
- [ ] Existing shared code is reviewed for wrong-abstraction signals before adding new callers

## References

- [principle-single-responsibility](principle-single-responsibility.md) - Related: SRP helps identify when an abstraction serves multiple concerns
- [principle-open-closed](principle-open-closed.md) - Related: OCP extension points are the right place for shared behavior

## External References

- [The Wrong Abstraction — Sandi Metz](https://sandimetz.com/blog/2016/1/20/the-wrong-abstraction)
- [Caught in a Bad Abstraction — Israeli Tech Radar](https://medium.com/israeli-tech-radar/caught-in-a-bad-abstraction-55bfe6634b83)
- [DRY: Most Over-rated Programming Principle — Gordon C](https://gordonc.bearblog.dev/dry-most-over-rated-programming-principle/)
- [DRY Principle in Rust — CodeSignal](https://codesignal.com/learn/courses/applying-clean-code-principles-in-rust/lessons/applying-clean-code-principles-in-rust-understanding-and-implementing-the-dry-principle)
- [12 Design Principles in Rust — Bagwan Pankaj](https://blog.bagwanpankaj.com/architecture/12-design-principles-you-can-implement-in-rust)
