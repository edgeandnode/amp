---
name: "pattern-service"
description: "Two-phase handle+fut service pattern: init phase returns (handle, impl Future), separated init/runtime errors. Load when implementing service functions or working with impl Future service patterns"
type: core
scope: "global"
---

# Two-Phase Service Pattern (Handle + Future)

**MANDATORY for ALL Rust code in the workspace**

## Rule

Use the two-phase service pattern when a service needs both initialization and long-running execution. Phase 1 (initialization) sets up resources and returns metadata; Phase 2 (the future) runs the service loop. Separate the phases with distinct error types so callers can distinguish setup failures from runtime failures.

If a service function mixes initialization and runtime into a single `async fn` with one error type, callers cannot distinguish setup failures (configuration, binding) from runtime failures (connection drops, processing errors). Split into two phases: a synchronous or async init that returns metadata and an `impl Future`, keeping internal structs private.

## Examples

1. **Simple long-running service (sync init)**
When initialization is synchronous and produces no metadata, return `impl Future` directly.

```rust
// Bad — async fn conflates init and runtime, exposes internal struct
pub struct Worker {
    node_id: NodeId,
    config: Arc<Config>,
}

impl Worker {
    pub fn new(node_id: NodeId, config: Arc<Config>) -> Self {
        Self { node_id, config }
    }

    pub async fn run(self) -> Result<(), Error> {
        // Init and runtime errors share one type
        let db = connect(&self.config).await?;
        loop { /* service loop */ }
    }
}
```

```rust
// Good — private struct, two-phase with impl Future
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

2. **Async init with metadata return (server)**
When initialization is async and callers need metadata (e.g., bound addresses), return `Result<(Metadata, impl Future), InitError>`.

```rust
// Bad — single error type, no metadata returned, hidden resource allocation
pub async fn start_server(config: Arc<Config>) -> Result<(), Error> {
    let service = flight::Service::create(config.clone()).await?;
    let listener = TcpListener::bind(addr).await?;
    // Caller doesn't know the bound address
    tonic::transport::Server::builder()
        .add_service(service)
        .serve_with_incoming(listener)
        .await?;
    Ok(())
}
```

```rust
// Good — separated init/runtime errors, metadata returned
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
        tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(listener)
            .await
            .map_err(Into::into)
    };

    Ok((BoundAddrs { /* ... */ }, fut))
}
```

3. **Async init with router setup (controller)**
When initialization involves composing a router with middleware, the same two-phase split applies.

```rust
// Bad — creates dependencies internally, mixes concerns
pub async fn new(config_path: &str, at: SocketAddr) -> Result<(), Error> {
    let config = load_config(config_path)?; // Hidden dependency
    let metadata_db = config.metadata_db().await?;
    let app = Router::new().route("/healthz", get(|| async { StatusCode::OK }));
    let listener = TcpListener::bind(at).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
```

```rust
// Good — dependencies injected, two-phase with metadata
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

## Why It Matters

When initialization and runtime are conflated, callers cannot react differently to setup failures (retry with different config) versus runtime failures (restart the service). Exposing internal structs forces callers to manage a two-step `new()` then `run()` ceremony and couples them to implementation details. The two-phase pattern returns an opaque future, keeping internals private and giving callers a clear contract: init succeeds or fails immediately, then the service runs independently.

## Pragmatism Caveat

Not every async function needs two phases. If a service has no meaningful initialization (no resource allocation, no metadata to return, no distinction between init and runtime errors), a simple `async fn` is clearer. Apply the two-phase pattern when there is genuine setup work that can fail independently from runtime, or when callers need metadata (bound addresses, handles) before the service starts running.

## Checklist

Before committing code, verify:

- [ ] Service entry point is `pub fn new()` or `pub async fn new()` — not a struct method
- [ ] Internal `Worker`/`Service` structs are private (not exported)
- [ ] All dependencies are injected as function parameters (not created internally)
- [ ] Returns `impl Future` — not `async fn` return type (use `#[allow(clippy::manual_async_fn)]` if needed)
- [ ] Init errors and runtime errors use separate error types when the service has meaningful init
- [ ] Metadata (bound addresses, handles) is returned from Phase 1, not extracted from the future

## References

- [principle-inversion-of-control](principle-inversion-of-control.md) - Foundation: All dependencies injected, not created internally
- [services](services.md) - Related: Architectural service organization
