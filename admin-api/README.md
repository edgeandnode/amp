# Admin API

## Summary

The Admin API provides a RESTful HTTP interface for managing Nozzle's ETL
operations.
This crate implements an Axum-based web server that exposes endpoints for
dataset management, job control, and worker location administration.
The API serves as the primary administrative interface for monitoring and
controlling the Nozzle data pipeline,
allowing operators to deploy datasets, trigger data extraction jobs, monitor
job progress, and manage distributed worker locations.

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
│   ├── jobs/           # Job management endpoints
│   ├── jobs.rs         # Job handler module
│   ├── locations/      # Worker location endpoints
│   └── locations.rs    # Location handler module
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

#### `DELETE /locations/{id}`
Removes a worker location from the system using the specified location
identifier.
The `id` parameter must be a positive integer identifying the location.
Accepts optional `force` query parameter to override safety checks.
Performs comprehensive cleanup including deleting associated files from object
store and removing location metadata from the database.

See [`handlers/locations/delete_by_id.rs`](src/handlers/locations/delete_by_id.rs) for more detailed information about this endpoint.
