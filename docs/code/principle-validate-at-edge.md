---
name: "principle-validate-at-edge"
description: "Validate at the Edge — enforce input correctness at system boundaries. Load when designing API handlers, parsing external input, or deciding where to place validation logic"
type: "principle"
scope: "global"
---

# Validate at the Edge (Hard Shell, Soft Core)

**MANDATORY for ALL code in the workspace**

## Rule

Validate all external input at the system's entry points — API handlers, CLI parsers, message consumers, deserialization boundaries. Once data crosses the edge and is accepted, the domain trusts it completely. Domain logic should never re-validate what the boundary already guaranteed.

The boundary is the hard shell: it rejects malformed, out-of-range, or structurally invalid data before it reaches domain code. The domain is the soft core: it operates on validated types without defensive checks, focusing purely on business rules.

The examples below use newtypes and validated wrappers from [Type-Driven Design](principle-type-driven-design.md) to represent parsed data — edge validation produces the types, that pattern defines how to design them.

## Examples

1. **Newtypes at the API boundary**
Domain functions should receive validated types, not raw strings that require parsing inside business logic.

```rust
// Bad — domain function validates raw input deep inside business logic
fn calculate_discount(price_str: &str, percentage_str: &str) -> Result<f64> {
    // Parsing and validation buried in domain logic
    let price: f64 = price_str.parse()
        .map_err(|_| anyhow!("invalid price"))?;
    if price < 0.0 {
        return Err(anyhow!("price must be non-negative"));
    }
    let percentage: f64 = percentage_str.parse()
        .map_err(|_| anyhow!("invalid percentage"))?;
    if !(0.0..=100.0).contains(&percentage) {
        return Err(anyhow!("percentage must be 0-100"));
    }
    Ok(price * (1.0 - percentage / 100.0))
}
```

```rust
// Good — boundary validates and parses, domain receives validated types
struct Price(f64);

impl Price {
    fn parse(input: &str) -> Result<Self> {
        let value: f64 = input.parse()?;
        if value < 0.0 {
            return Err(anyhow!("price must be non-negative"));
        }
        Ok(Self(value))
    }

    fn value(&self) -> f64 {
        self.0
    }
}

struct Percentage(f64);

impl Percentage {
    fn parse(input: &str) -> Result<Self> {
        let value: f64 = input.parse()?;
        if !(0.0..=100.0).contains(&value) {
            return Err(anyhow!("percentage must be 0-100"));
        }
        Ok(Self(value))
    }

    fn as_factor(&self) -> f64 {
        self.0 / 100.0
    }
}

// Boundary: API handler validates input
async fn handle_discount(req: DiscountRequest) -> Result<Json<f64>> {
    let price = Price::parse(&req.price)?;
    let percentage = Percentage::parse(&req.percentage)?;
    Ok(Json(calculate_discount(price, percentage)))
}

// Domain: operates on validated types, no defensive checks
fn calculate_discount(price: Price, percentage: Percentage) -> f64 {
    price.value() * (1.0 - percentage.as_factor())
}
```

2. **Single validation point eliminates scattered checks**
When validation is spread across handler, service, and repository layers, it's unclear which layer is responsible.

```rust
// Bad — validation scattered across multiple layers
async fn handle_create_user(req: Json<CreateUserRequest>) -> Result<()> {
    // Handler checks some fields
    if req.email.is_empty() {
        return Err(anyhow!("email required"));
    }
    // Service checks others
    user_service.create(&req.name, &req.email).await
}

async fn create(name: &str, email: &str) -> Result<()> {
    // Service re-validates and checks more
    if !email.contains('@') {
        return Err(anyhow!("invalid email"));
    }
    if name.len() > 100 {
        return Err(anyhow!("name too long"));
    }
    repo.insert(name, email).await
}

async fn insert(name: &str, email: &str) -> Result<()> {
    // Repository checks even more
    if name.is_empty() {
        return Err(anyhow!("name required"));
    }
    // finally does the insert
    Ok(())
}
```

```rust
// Good — all validation at the boundary, domain receives fully validated types
struct UserName(String);

impl UserName {
    fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("name required"));
        }
        if trimmed.len() > 100 {
            return Err(anyhow!("name too long"));
        }
        Ok(Self(trimmed.to_owned()))
    }
}

struct Email(String);

impl Email {
    fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("email required"));
        }
        if !trimmed.contains('@') {
            return Err(anyhow!("invalid email"));
        }
        Ok(Self(trimmed.to_owned()))
    }
}

// Boundary: single validation point
async fn handle_create_user(req: Json<CreateUserRequest>) -> Result<()> {
    let name = UserName::parse(&req.name)?;
    let email = Email::parse(&req.email)?;
    user_service.create(name, email).await
}

// Domain: trusts validated types completely
async fn create(name: UserName, email: Email) -> Result<()> {
    repo.insert(name, email).await
}
```

3. **Cross-field constraints at the boundary**
When multiple fields have relationships (date ranges, mutual requirements), validate the relationship at the edge alongside individual field validation.

```rust
// Bad — cross-field check buried in domain logic
fn schedule_report(start: &str, end: &str) -> Result<()> {
    let start: NaiveDate = start.parse()?;
    let end: NaiveDate = end.parse()?;
    if start >= end {
        return Err(anyhow!("start must be before end"));
    }
    generate_report(start, end)
}
```

```rust
// Good — cross-field constraint validated at the edge via a composite type
struct DateRange {
    start: NaiveDate,
    end: NaiveDate,
}

impl DateRange {
    fn parse(start: &str, end: &str) -> Result<Self> {
        let start: NaiveDate = start.parse()?;
        let end: NaiveDate = end.parse()?;
        if start >= end {
            return Err(anyhow!("start must be before end"));
        }
        Ok(Self { start, end })
    }
}

// Boundary
async fn handle_schedule(req: ScheduleRequest) -> Result<()> {
    let range = DateRange::parse(&req.start, &req.end)?;
    report_service.generate(range).await
}

// Domain: trusts the range is valid
fn generate(range: DateRange) -> Result<Report> {
    // no defensive checks needed
}
```

4. **CLI boundary parsing**
CLI arguments should be parsed into validated types at the entry point, not deep in domain logic.

```rust
// Bad — CLI tool validates arguments deep in domain logic
fn run_indexer(args: &[String]) -> Result<()> {
    let batch_size: usize = args[1].parse()
        .map_err(|_| anyhow!("invalid batch size"))?;
    if batch_size == 0 || batch_size > 10_000 {
        return Err(anyhow!("batch size must be 1-10000"));
    }
    let rpc_url = &args[2];
    if !rpc_url.starts_with("http") {
        return Err(anyhow!("invalid RPC URL"));
    }
    // domain logic mixed with input parsing
    index_blocks(rpc_url, batch_size).await
}
```

```rust
// Good — CLI boundary parses into validated types before entering domain
struct BatchSize(usize);

impl BatchSize {
    fn parse(input: &str) -> Result<Self> {
        let value: usize = input.parse()?;
        if value == 0 || value > 10_000 {
            return Err(anyhow!("batch size must be 1-10000"));
        }
        Ok(Self(value))
    }
}

struct RpcUrl(Url);

impl RpcUrl {
    fn parse(input: &str) -> Result<Self> {
        let url: Url = input.parse()?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(anyhow!("RPC URL must use http(s)"));
        }
        Ok(Self(url))
    }
}

// CLI boundary: all validation here
fn main() -> Result<()> {
    let args = cli::parse();
    let batch_size = BatchSize::parse(&args.batch_size)?;
    let rpc_url = RpcUrl::parse(&args.rpc_url)?;
    index_blocks(&rpc_url, batch_size).await
}
```

5. **Config deserialization boundary**
Configuration values should be validated once during deserialization, not ad-hoc wherever they're consumed.

```rust
// Bad — config values validated ad-hoc wherever they're used
fn connect(config: &Config) -> Result<Pool> {
    if config.db_url.is_empty() {
        return Err(anyhow!("missing database URL"));
    }
    if config.pool_size == 0 {
        return Err(anyhow!("pool size must be positive"));
    }
    // defensive checks scattered in every consumer of Config
    Pool::connect(&config.db_url, config.pool_size).await
}
```

```rust
// Good — config deserialization boundary validates once, domain trusts the result
struct DatabaseConfig {
    url: DatabaseUrl,
    pool_size: PoolSize,
}

impl DatabaseConfig {
    fn from_env() -> Result<Self> {
        let url = DatabaseUrl::parse(&std::env::var("DATABASE_URL")?)?;
        let pool_size = PoolSize::parse(&std::env::var("POOL_SIZE")?)?;
        Ok(Self { url, pool_size })
    }
}

// Domain: no defensive checks needed
fn connect(config: &DatabaseConfig) -> Result<Pool> {
    Pool::connect(config.url.as_str(), config.pool_size.value()).await
}
```

## Why It Matters

Scattered validation clutters domain logic with defensive checks, makes it unclear which layer is responsible for correctness, and risks inconsistent validation (some paths validate, others don't). Edge validation creates a clear contract: the boundary guarantees data integrity, the domain focuses on business rules. When a new API endpoint is added, developers know exactly where validation belongs — at the edge — instead of guessing which layer should check what.

## Pragmatism Caveat

The signal for where validation belongs is whether the check depends only on the incoming request or needs external state:

- **Request-only checks → edge**: Field formats, required fields, type parsing, and cross-field constraints within the same input (e.g., `start_date < end_date`, "at least one of email or phone required"). These depend solely on the request and belong at the boundary.
- **Needs external state → domain**: Checks that require database lookups or other service calls (e.g., "does this user have sufficient balance?", "is this item still in stock?"). These belong in the domain because only the domain layer has access to the required state.

Don't try to push state-dependent checks to the boundary, and don't let request-only checks leak into the domain.

## Checklist

Before committing code, verify:

- [ ] Request-only invariants are enforced at explicit system boundaries before domain execution
- [ ] Domain interfaces consume validated domain types rather than raw, unparsed input
- [ ] Validation responsibilities are not duplicated across layers
- [ ] Checks requiring external state are kept in the domain layer
- [ ] New entry points establish a clear validation boundary and preserve the same contract


## References

- [principle-type-driven-design](principle-type-driven-design.md) - Related: Edge validation produces the validated types that make illegal states unrepresentable

## External References

- [Architecture Patterns with Python (O'Reilly)](https://www.oreilly.com/library/view/architecture-patterns-with/9781492052197/)
