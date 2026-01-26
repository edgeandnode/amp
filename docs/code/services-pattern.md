---
name: "services-pattern"
description: "Two-phase service creation pattern for services/*. Load when creating or modifying service crates"
type: arch
scope: "global"
---

# Service Pattern: Two-Phase Service Creation

**Applies to**: All crates under `crates/services/` **EXCEPT** `admin-api`

## Pattern Overview

Services in `crates/services/` follow a **two-phase functional initialization pattern**:

1. **Phase 1: Initialization** - Setup, resource allocation, dependency resolution (sync or async)
2. **Phase 2: Service Future** - Long-running service logic returned as `impl Future`

This separation enables:
- Early error detection (init phase fails fast)
- Service composition via `tokio::select!`
- Flexible lifecycle management
- Clear separation of setup vs runtime errors

## Core Pattern

### Function Signature

```rust
// Pattern A: Simple long-running service
pub fn new(
    // All dependencies injected via parameters
    param1: Type1,
    param2: Arc<Type2>,
    // ...
) -> impl Future<Output = Result<(), Error>>

// Pattern B: Two-phase with metadata return
pub async fn new(
    // Dependencies
    config: Arc<Config>,
    // ...
) -> Result<(Metadata, impl Future<Output = Result<(), RuntimeError>>), InitError>
```

### Key Characteristics

- **Function, not method**: `pub fn new()` (not a struct method)
- **Returns `impl Future`**: Service logic is lazy-evaluated
- **No exported structs**: Internal `Worker`/`Service` structs remain private
- **Dependency injection**: All dependencies passed as parameters
- **Single entry point**: One `pub fn new()` per service module

## Phase 1: Initialization

**Purpose**: Setup phase that runs before service starts

**Responsibilities**:
- Validate configuration
- Establish database connections
- Allocate resources (TCP listeners, channels)
- Create internal service structs
- Return initialization metadata (if applicable)

**Error handling**: Return `Result<_, InitError>` for setup failures

### Examples by Service Type

**Sync init (worker)**:
```rust
pub fn new(
    node_id: NodeId,
    config: Arc<Config>,
    metadata_db: MetadataDb,
    meter: Option<Meter>,
) -> impl Future<Output = Result<(), Error>> {
    // Phase 1: Synchronous initialization
    let worker_info = WorkerInfo { /* ... */ };

    // Phase 2: Async service future
    async move {
        let mut worker = Worker::new(node_id, config, metadata_db, meter);
        // ... service loop
    }
}
```

**Async init with metadata return (server)**:
```rust
pub async fn new(
    config: Arc<Config>,
    metadata_db: MetadataDb,
    flight_at: impl Into<Option<SocketAddr>>,
    jsonl_at: impl Into<Option<SocketAddr>>,
    meter: Option<&Meter>,
) -> Result<(BoundAddrs, impl Future<Output = Result<(), BoxError>>), InitError> {
    // Phase 1: Async initialization
    let service = flight::Service::create(config, metadata_db, meter).await?;
    let listener = TcpListener::bind(addr).await?;
    let bound_addr = listener.local_addr()?;

    // Phase 2: Service future
    let fut = async move {
        // Server logic
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(listener)
            .await
            .map_err(Into::into)
    };

    Ok((BoundAddrs { /* ... */ }, fut))
}
```

**Async init with router setup (controller)**:
```rust
pub async fn new(
    config: Arc<Config>,
    meter: Option<&Meter>,
    at: SocketAddr,
) -> Result<(SocketAddr, impl Future<Output = Result<(), BoxError>>), Error> {
    // Phase 1: Async initialization
    let metadata_db = config.metadata_db().await?;
    let dataset_store = DatasetStore::new(/* ... */);
    let scheduler = Scheduler::new(config.clone(), metadata_db.clone());

    let app = Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .merge(admin_api::router(ctx))
        .layer(/* ... */);

    let listener = TcpListener::bind(at).await?;
    let addr = listener.local_addr()?;

    // Phase 2: Server future
    let fut = async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(Into::into)
    };

    Ok((addr, fut))
}
```

## Phase 2: Service Future

**Purpose**: Long-running service execution

**Responsibilities**:
- Main service loop (event handling, processing)
- Graceful shutdown handling
- Runtime error propagation

**Error handling**: Future resolves to `Result<(), RuntimeError>`

**Common patterns**:
- Event loop with `tokio::select!`
- Graceful shutdown via `shutdown_signal()`
- Background task spawning with `AbortOnDropHandle`

## Requirements Checklist for AI Agents

When implementing or modifying a service in `crates/services/`, ensure:

### Module Structure
- [ ] Service module named `service` in `src/service.rs`
- [ ] Export in `lib.rs`: `pub mod service;`
- [ ] Internal implementation in `src/service/` subdirectory (if needed)
- [ ] No public exports of internal `Worker`/`Service` structs

### Function Signature
- [ ] `pub fn new()` or `pub async fn new()` (not a struct method)
- [ ] All dependencies passed as function parameters
- [ ] Returns `impl Future` (not `async fn` return type)
- [ ] Use `#[allow(clippy::manual_async_fn)]` if returning `impl Future` from non-async fn

### Error Handling
- [ ] Separate error types for initialization vs runtime
- [ ] Init errors in return type: `Result<_, InitError>`
- [ ] Runtime errors in future: `Future<Output = Result<(), RuntimeError>>`

### Two-Phase Implementation
- [ ] **Phase 1**: Initialization logic (sync or async setup)
- [ ] **Phase 2**: Service future with main logic
- [ ] Clear separation between setup errors and runtime errors

### Return Patterns
- [ ] Simple service: `impl Future<Output = Result<(), Error>>`
- [ ] With metadata: `Result<(Metadata, impl Future), InitError>`
- [ ] HTTP servers: Return bound address + future

## Service Composition Example

Services are designed to compose via `tokio::select!`:

```rust
// In ampd/src/solo_cmd.rs
let controller_fut = controller::service::new(config.clone(), meter.as_ref(), admin_addr).await?;
let server_fut = server::service::new(config.clone(), metadata_db.clone(), flight_addr, jsonl_addr, meter.as_ref()).await?;
let worker_fut = worker::service::new(worker_id, config.clone(), metadata_db, meter);

// Compose services - all run concurrently, first error/completion wins
tokio::select! {biased;
    res = controller_fut => res.map_err(Error::ControllerRuntime)?,
    res = worker_fut => res.map_err(Error::WorkerRuntime)?,
    res = server_fut => res.map_err(Error::ServerRuntime)?,
}
```

## Anti-Patterns to Avoid

### ❌ Exporting Internal Structs
```rust
// DON'T: Expose internal implementation
pub struct Worker { /* ... */ }

impl Worker {
    pub fn new() -> Self { /* ... */ }
    pub async fn run(self) -> Result<(), Error> { /* ... */ }
}
```

### ❌ Using `async fn` Return Type
```rust
// DON'T: This prevents proper type erasure
pub async fn new() -> Result<(), Error> {
    // ...
}

// DO: Return impl Future explicitly
pub fn new() -> impl Future<Output = Result<(), Error>> {
    async move {
        // ...
    }
}
```

### ❌ Creating Dependencies Internally
```rust
// DON'T: Create dependencies inside the function
pub fn new(config_path: &str) -> impl Future<Output = Result<(), Error>> {
    async move {
        let config = load_config(config_path)?; // ❌ Hidden dependency
        // ...
    }
}

// DO: Inject all dependencies
pub fn new(config: Arc<Config>) -> impl Future<Output = Result<(), Error>> {
    async move {
        // ...
    }
}
```

### ❌ Mixing Init and Runtime Errors
```rust
// DON'T: Single error type for both phases
pub async fn new() -> impl Future<Output = Result<(), Error>> {
    // Initialization can fail but caller can't distinguish
}

// DO: Separate error types
pub async fn new() -> Result<(Addr, impl Future<Output = Result<(), RuntimeError>>), InitError> {
    // Init phase errors returned as InitError
    let addr = bind().await?;

    // Runtime phase errors in future
    let fut = async move {
        // Runtime errors returned as RuntimeError
    };

    Ok((addr, fut))
}
```

## Reference Implementation

**Current services implementing this pattern**:
- `crates/services/worker/src/service.rs` - Sync init, event loop service
- `crates/services/server/src/service.rs` - Async init, multiple servers, conditional startup
- `crates/services/controller/src/service.rs` - Async init, HTTP server with router

**Not a service** (different pattern):
- `crates/services/admin-api` - Library providing HTTP handlers and router, not a standalone service
