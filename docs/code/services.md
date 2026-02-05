---
name: "services"
description: "Service crate architecture: module layout, service composition via tokio::select!, reference implementations. Load when creating or modifying service crates in crates/services/"
type: arch
scope: "global"
---

# Service Architecture

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

## Module Structure

Each service crate follows a consistent module layout:

- Service module named `service` in `src/service.rs`
- Export in `lib.rs`: `pub mod service;`
- Internal implementation in `src/service/` subdirectory (if needed)
- No public exports of internal `Worker`/`Service` structs

## Service Composition

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

## Reference Implementation

**Current services implementing this pattern**:
- `crates/services/worker/src/service.rs` - Sync init, event loop service
- `crates/services/server/src/service.rs` - Async init, multiple servers, conditional startup
- `crates/services/controller/src/service.rs` - Async init, HTTP server with router

**Not a service** (different pattern):
- `crates/services/admin-api` - Library providing HTTP handlers and router, not a standalone service

## Checklist

When implementing or modifying a service in `crates/services/`:

- [ ] Service module named `service` in `src/service.rs`
- [ ] Export in `lib.rs`: `pub mod service;`
- [ ] Internal implementation in `src/service/` subdirectory (if needed)
- [ ] No public exports of internal `Worker`/`Service` structs
- [ ] Service composes via `tokio::select!` with other services

## References

- [rust-service](rust-service.md) - Foundation: Two-phase handle+fut coding pattern
