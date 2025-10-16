# Admin API

## Summary

The Admin API provides a RESTful HTTP interface for managing Amp's ETL operations. This crate implements an Axum-based web server that exposes endpoints for dataset management, job control, storage location administration, file management, and provider configuration.

The API serves as the primary administrative interface for monitoring and controlling the Amp data pipeline, allowing operators to:
- Deploy and manage datasets with versioning support
- Trigger and monitor data extraction jobs
- Manage distributed storage locations
- Configure external data providers (EVM RPC, Firehose)
- Perform operations on Parquet files and their metadata

## API Documentation

Complete API documentation is available in the generated OpenAPI specification at [`docs/openapi-specs/admin.spec.json`](../../docs/openapi-specs/admin.spec.json).

The OpenAPI spec provides comprehensive documentation for all endpoints, including:
- Request/response schemas
- Pagination details
- Error handling
- Usage examples
- Data format specifications

## Crate Structure

The admin-api crate follows a modular handler-based architecture:

```
src/
├── ctx.rs              # Application context with shared state
├── handlers.rs         # Handler module declarations
├── handlers/
│   ├── common.rs       # Shared utilities for handlers
│   ├── error.rs        # Error response types
│   ├── datasets/       # Dataset management endpoints
│   ├── files/          # File management endpoints
│   ├── jobs/           # Job management endpoints
│   ├── locations/      # Storage location endpoints
│   └── providers/      # Provider configuration endpoints
├── lib.rs              # Main server setup and routing
└── scheduler.rs        # Job scheduling utilities
```

### Handler Registration

Handlers are registered in [`src/lib.rs`](src/lib.rs) using Axum's routing system. Each endpoint is mapped to its corresponding handler function using HTTP method routing helpers (`get()`, `post()`, `put()`, `delete()`).

## OpenAPI Spec Generation

OpenAPI specifications for the Admin API can be generated from the code annotations. This generates comprehensive API documentation including all endpoints, schemas, and usage examples.

To generate the OpenAPI specification, run:

```bash
just gen-admin-api-openapi-spec
```

This will generate the OpenAPI spec from the handler utoipa annotations and copy it to `docs/openapi-specs/admin.spec.json`.
