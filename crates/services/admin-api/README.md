# Admin API

## Summary

The Admin API provides a RESTful HTTP interface for managing Nozzle's ETL
operations.
This crate implements an Axum-based web server that exposes endpoints for
dataset management, job control, worker location administration, file management, and provider configuration.
The API serves as the primary administrative interface for monitoring and
controlling the Nozzle data pipeline,
allowing operators to deploy datasets, trigger data extraction jobs, monitor
job progress, manage distributed worker locations, configure external data providers,
and perform direct operations on Parquet files and their metadata.

## Crate Structure

The admin-api crate follows a modular handler-based architecture:

```
src/
├── ctx.rs              # Application context with shared state
├── handlers.rs         # Handler module declarations
├── handlers/
│   ├── common.rs       # Shared utilities for handlers
│   ├── datasets/       # Dataset management endpoints
│   ├── datasets.rs     # Dataset handler module
│   ├── files/          # File management endpoints
│   ├── files.rs        # File handler module
│   ├── jobs/           # Job management endpoints
│   ├── jobs.rs         # Job handler module
│   ├── locations/      # Worker location endpoints
│   ├── locations.rs    # Location handler module
│   ├── providers/      # Provider configuration endpoints
│   └── providers.rs    # Provider handler module
├── lib.rs              # Main server setup and routing
└── scheduler.rs        # Job scheduling utilities
```

### Handler Registration and Routing

Handlers are registered in [`src/lib.rs`](src/lib.rs) using Axum's routing
system.
Each endpoint is mapped to its corresponding handler function using the HTTP
method routing helpers (`get()`, `post()`, `put()`, `delete()`).

## API Reference

### Dataset Management

Dataset endpoints handle the lifecycle of data extraction configurations and
provide information about available datasets and their tables.

#### `GET /datasets`
Lists all available datasets in the system with their table information and
active locations.
Returns comprehensive dataset information including dataset names and types,
all tables within each dataset with their network associations,
and active storage locations for each table.

See [`handlers/datasets/get_all.rs`](src/handlers/datasets/get_all.rs) for more detailed information about this endpoint.

#### `GET /datasets/{id}`
Retrieves detailed information about a specific dataset by its ID,
including its tables and active locations.
The `id` parameter specifies the dataset name to retrieve and must be a valid
dataset name format.
Returns dataset information including name, type, version details,
and complete list of tables with their network associations and storage
locations.

See [`handlers/datasets/get_by_id.rs`](src/handlers/datasets/get_by_id.rs) for more detailed information about this endpoint.

#### `POST /datasets` (alias for `/deploy`)
Deploys a new dataset configuration to the system.
Accepts a JSON payload containing `dataset_name`, `version`,
and optional `manifest` fields.
Supports multiple deployment scenarios including existing datasets,
new manifest datasets, and new dataset definitions.
Returns deployment success confirmation upon successful completion.

See [`handlers/datasets/deploy.rs`](src/handlers/datasets/deploy.rs) for more detailed information about this endpoint.

#### `POST /deploy`
Primary endpoint for dataset deployment operations.
Accepts a JSON payload containing `dataset_name`, `version`,
and optional `manifest` fields.
Handles manifest registration for new datasets and schedules dataset dump jobs
via the scheduler.

See [`handlers/datasets/deploy.rs`](src/handlers/datasets/deploy.rs) for more detailed information about this endpoint.

#### `POST /datasets/{id}/dump`
Triggers a data extraction job for the specified dataset.
The `id` parameter identifies the target dataset name,
and the request accepts a JSON payload with `end_block` (optional last block
number) and `wait_for_completion` (optional boolean, default false) options.
Supports both asynchronous mode that returns immediately with job scheduling
confirmation, and synchronous mode that waits for job completion and returns
completion confirmation.

See [`handlers/datasets/dump.rs`](src/handlers/datasets/dump.rs) for more detailed information about this endpoint.

### Job Management

Job endpoints provide control and monitoring capabilities for data extraction
and processing jobs.

#### `GET /jobs`
Lists all jobs in the system with pagination support.
Accepts optional query parameters including `limit` for maximum jobs per page
(default: 50, max: 1000) and `last_job_id` for pagination cursor.
Returns paginated job data with job information and next cursor for continued
pagination.

See [`handlers/jobs/get_all.rs`](src/handlers/jobs/get_all.rs) for more detailed information about this endpoint.

#### `GET /jobs/{id}`
Retrieves detailed information about a specific job by its ID.
The `id` parameter must be a valid JobId identifier.
Returns comprehensive job information including status, progress,
and execution details.

See [`handlers/jobs/get_by_id.rs`](src/handlers/jobs/get_by_id.rs) for more detailed information about this endpoint.

#### `PUT /jobs/{id}/stop`
Stops a running job using the specified job ID.
The `id` parameter must be a valid JobId identifier.
This is an idempotent operation that handles job termination requests safely
by transitioning running or scheduled jobs to stop-requested state and
notifying workers.

See [`handlers/jobs/stop.rs`](src/handlers/jobs/stop.rs) for more detailed information about this endpoint.

#### `DELETE /jobs/{id}`
Deletes a specific job by its ID if it's in a terminal state.
The `id` parameter must be a valid JobId identifier.
Only jobs in terminal states (Completed, Stopped, Failed) can be deleted.
Non-terminal jobs are protected from accidental deletion.
Returns 404 if job doesn't exist, 409 if job exists but isn't in terminal state.

See [`handlers/jobs/delete_by_id.rs`](src/handlers/jobs/delete_by_id.rs) for more detailed information about this endpoint.

#### `DELETE /jobs?status=<filter>`
Deletes jobs based on status filter. Supports deleting jobs by various status criteria.
This is a bulk cleanup operation for finalized jobs.
The `status` query parameter accepts the following values
- `terminal`: Delete all jobs in terminal states (Completed, Stopped, Failed)
- `complete`: Delete all completed jobs
- `stopped`: Delete all stopped jobs  
- `error`: Delete all failed jobs

Only removes jobs that have completed their lifecycle and are safe to delete.
This endpoint is typically used for periodic cleanup and administrative maintenance.

See [`handlers/jobs/delete.rs`](src/handlers/jobs/delete.rs) for more detailed information about this endpoint.

### File Management

File endpoints provide CRUD operations for managing Parquet files stored in the system. These endpoints allow direct manipulation of individual files and their metadata within storage locations.

#### `GET /files/{file_id}`
Retrieves detailed information about a specific file by its ID.
The `file_id` parameter must be a positive integer identifying the file.
Returns comprehensive file information including location URL, size, version details, and complete Parquet metadata as JSON.
This endpoint provides all available information about a file, including the heavy metadata JSON field.

See [`handlers/files/get_by_id.rs`](src/handlers/files/get_by_id.rs) for more detailed information about this endpoint.


### Location Management

Location endpoints manage distributed worker nodes and their availability for
job execution.

#### `GET /locations`
Lists all worker locations with pagination support.
Accepts optional query parameters including `limit` for maximum locations per
page (default: 50, max: 1000) and `last_location_id` for pagination cursor.
Returns paginated location data with next cursor for continued pagination.

See [`handlers/locations/get_all.rs`](src/handlers/locations/get_all.rs) for more detailed information about this endpoint.

#### `GET /locations/{id}`
Retrieves detailed information about a specific worker location using the
provided location identifier.
The `id` parameter must be a positive integer identifying the location.
Returns comprehensive location information including URL, status,
and associated job details.

See [`handlers/locations/get_by_id.rs`](src/handlers/locations/get_by_id.rs) for more detailed information about this endpoint.

#### `GET /locations/{location_id}/files`
Lists all files within a specific location with pagination support.
The `location_id` parameter must be a positive integer identifying the location.
Accepts optional query parameters including `limit` for maximum files per page (default: 50, max: 1000) and `last_file_id` for pagination cursor.
Returns paginated file listing with essential file information (ID, name, size) optimized for bulk operations.
This endpoint uses lightweight file metadata without the heavy Parquet JSON metadata for optimal performance.

See [`handlers/locations/get_files.rs`](src/handlers/locations/get_files.rs) for more detailed information about this endpoint.

#### `DELETE /locations/{id}`
Removes a worker location from the system using the specified location
identifier.
The `id` parameter must be a positive integer identifying the location.
Accepts optional `force` query parameter to override safety checks.
Performs comprehensive cleanup including deleting associated files from object
store and removing location metadata from the database.

See [`handlers/locations/delete_by_id.rs`](src/handlers/locations/delete_by_id.rs) for more detailed information about this endpoint.

### Provider Management

Provider endpoints manage external data source configurations including EVM RPC endpoints, Firehose connections, and Substreams providers. These endpoints allow creating, retrieving, and deleting provider configurations that are used by datasets for data extraction.

#### `GET /providers`
Lists all provider configurations available in the system.
Returns complete provider information including configuration details for all registered providers.
This endpoint accesses cached provider configurations from the dataset store and filters out any providers that cannot be converted to valid API format.

**Security Note:** This endpoint returns the complete provider configuration including all configuration details. Ensure that sensitive information such as API keys and credentials are not stored in provider configuration files.

See [`handlers/providers/get_all.rs`](src/handlers/providers/get_all.rs) for more detailed information about this endpoint.

#### `POST /providers`
Creates a new provider configuration and stores it in the dataset store.
Accepts a JSON payload containing provider configuration with required fields including `name` (unique identifier), `kind` (provider type such as "evm-rpc", "firehose", "substreams"), `network` (blockchain network), and additional provider-specific configuration fields.
Converts JSON configuration data to TOML format for internal storage.
Returns 201 Created upon successful creation, 409 Conflict if provider name already exists.

See [`handlers/providers/create.rs`](src/handlers/providers/create.rs) for more detailed information about this endpoint.

#### `GET /providers/{name}`
Retrieves detailed information about a specific provider configuration by its name.
The `name` parameter is the unique identifier of the provider to retrieve.
Returns complete provider configuration including all stored configuration details.

**Security Note:** This endpoint returns the complete provider configuration including all configuration details. Ensure that sensitive information such as API keys and credentials are properly filtered before storage.

See [`handlers/providers/get_by_id.rs`](src/handlers/providers/get_by_id.rs) for more detailed information about this endpoint.

#### `DELETE /providers/{name}`
Deletes a specific provider configuration by its name from the dataset store.
The `name` parameter is the unique identifier of the provider to delete.
Performs comprehensive cleanup by removing both the configuration file from storage and the cached entry.
Returns 204 No Content upon successful deletion, 404 Not Found if provider doesn't exist.

**Safety Warning:** Once deleted, the provider configuration cannot be recovered. Any datasets using this provider may fail until a new provider is configured.

See [`handlers/providers/delete_by_id.rs`](src/handlers/providers/delete_by_id.rs) for more detailed information about this endpoint.
