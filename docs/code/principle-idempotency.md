---
name: "principle-idempotency"
description: "Idempotency — design operations safe to retry. Load when designing APIs, handling side effects, or building retry-safe distributed operations"
type: "principle"
scope: "global"
---

# Idempotency (Robustness in Distributed Systems)

**MANDATORY for ALL code in the workspace**

## Rule

Design state-altering operations so that executing them multiple times produces the same outcome as executing them once. Whether explicit idempotency machinery is needed depends on the entry point type:

1. **Webhook/API handlers receiving external requests** → treat as idempotency-required by default. External clients retry on timeouts and network failures.
2. **Queue/message consumers** → treat as idempotency-required by default. Brokers redeliver by design (at-least-once semantics).
3. **Service-to-service calls** (gRPC, HTTP between internal services) → design for idempotency when retries or timeout recovery are possible.
4. **In-process function calls** → explicit idempotency machinery is usually unnecessary because the caller controls execution.

For entry points that require idempotency, use idempotency keys, stored responses, and side-effect tracking to ensure safe retries. If retrying an operation produces a different result or causes additional side effects (duplicate rows, duplicate emails, double charges), the operation is not idempotent.

## Examples

1. **Idempotency key for database writes**
Without an idempotency key, retrying a payment request creates duplicate rows.

```rust
// Bad — retries create duplicate rows
async fn create_payment(db: &Pool, amount: u64) -> Result<PaymentId> {
    // Every call inserts a new row, even retries of the same logical request
    let id = sqlx::query_scalar!(
        "INSERT INTO payments (amount) VALUES ($1) RETURNING id",
        amount as i64
    )
    .fetch_one(db)
    .await?;

    Ok(id)
}
```

```rust
// Good — idempotency key prevents duplicate processing
async fn create_payment(
    db: &Pool,
    idempotency_key: &IdempotencyKey,
    amount: u64,
) -> Result<PaymentId> {
    // Check if this request was already processed
    if let Some(existing) = get_stored_response(db, idempotency_key).await? {
        return Ok(existing);
    }

    let id = sqlx::query_scalar!(
        "INSERT INTO payments (idempotency_key, amount) VALUES ($1, $2) RETURNING id",
        idempotency_key.as_str(),
        amount as i64
    )
    .fetch_one(db)
    .await?;

    store_response(db, idempotency_key, &id).await?;
    Ok(id)
}
```

2. **Side-effect tracking on retry**
Side effects like sending emails must be tracked so retries skip already-performed actions.

```rust
// Bad — side effect fires on every invocation, including retries
async fn process_order(db: &Pool, order: &Order) -> Result<()> {
    update_order_status(db, order.id, Status::Confirmed).await?;
    // Retrying this function sends duplicate confirmation emails
    send_confirmation_email(order.customer_email()).await?;
    Ok(())
}
```

```rust
// Good — record that the side effect was performed, skip on retry
async fn process_order(db: &Pool, order: &Order) -> Result<()> {
    update_order_status(db, order.id, Status::Confirmed).await?;

    if !was_email_sent(db, order.id).await? {
        send_confirmation_email(order.customer_email()).await?;
        mark_email_sent(db, order.id).await?;
    }

    Ok(())
}
```

## Why It Matters

In distributed systems, transient failures are inevitable — network timeouts, process crashes, message redeliveries. The entry point classification above makes this mechanically checkable: identify the entry point type, then determine whether idempotency machinery is required, rather than predicting whether retries "might" happen.

Idempotent operations enable safe retries, turning unreliable networks into reliable systems without complex coordination. Without idempotency, every retry becomes a potential source of data corruption, duplicate charges, or inconsistent state.

## Pragmatism Caveat

Not every operation needs idempotency machinery. Read-only (GET) operations are naturally idempotent. In-process functions that are already deterministic don't need additional infrastructure — if calling `f(x)` twice with the same input naturally produces the same result and no side effects, the function is already idempotent by construction. The entry point classification determines where explicit machinery is needed; don't add idempotency keys to purely internal calls where the caller controls execution.

## Checklist

Before committing code, verify:

- [ ] Retrying the same logical operation produces the same externally observable result
- [ ] Duplicate execution paths are handled explicitly (replay prior result, no-op, or safe merge)
- [ ] Side effects are guarded so retries do not repeat irreversible actions
- [ ] Persistence semantics prevent duplicate state from retries
- [ ] Cases where idempotency machinery is intentionally omitted are justified and documented


## External References

- [Idempotency in Depth (Luca Palmieri)](https://lpalmieri.com/posts/idempotency/)
