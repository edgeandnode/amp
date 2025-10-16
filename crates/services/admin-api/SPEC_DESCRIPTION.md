Administration API for Amp, a high-performance ETL system for blockchain data services on The Graph.

## About

The Admin API provides a RESTful HTTP interface for managing Amp's ETL operations. This API serves as the primary administrative interface for monitoring and controlling the Amp data pipeline, allowing you to deploy datasets, trigger data extraction jobs, monitor job progress, manage distributed worker locations, configure external data providers, and perform operations on Parquet files and their metadata.

## Key Capabilities

### Dataset Management
Handle the lifecycle of data extraction configurations and access dataset information:
- List all registered datasets from the metadata database registry
- Register new dataset configurations with versioning support
- Trigger data extraction jobs for specific datasets or dataset versions
- Retrieve dataset details including tables and active storage locations

### Job Control
Control and monitor data extraction and processing jobs:
- List and retrieve job information with pagination
- Trigger extraction jobs with optional end block configuration
- Stop running jobs gracefully
- Delete jobs in terminal states (Completed, Stopped, Failed)
- Bulk cleanup operations for finalized jobs

### Storage Management
Manage locations where dataset tables are stored:
- Supports local filesystem, S3, GCS, and Azure Blob Storage
- List storage locations and their associated files
- Delete locations with comprehensive cleanup (removes files and metadata)
- Query file information including Parquet metadata and statistics

### Provider Configuration
Configure external blockchain data sources:
- Create, retrieve, and delete provider configurations
- Support for EVM RPC endpoints and Firehose streams
- Providers are reusable across multiple dataset definitions
- **Security Note**: Provider configurations may contain connection details; ensure sensitive information is properly managed

### Schema Analysis
Validate SQL queries and infer output schemas:
- Validate queries against registered datasets without execution
- Determine output schema using DataFusion's query planner
- Useful for building dynamic query tools and validating dataset definitions

## Pagination

All list endpoints use cursor-based pagination for efficient data retrieval:

### Query Parameters
- `limit`: Maximum items per page (default: 50, max: 1000)
- `last_*_id`: Cursor from previous page's `next_cursor` field

### Response Format
Paginated responses include:
- Array of items (e.g., `datasets`, `jobs`, `locations`, `files`)
- `next_cursor`: Cursor for the next page (absent when no more results)

### Usage Pattern

**First Page Request:**
```
GET /jobs?limit=100
```

**First Page Response:**
```json
{
  "jobs": [...],
  "next_cursor": 12345
}
```

**Next Page Request:**
```
GET /jobs?limit=100&last_job_id=12345
```

**Last Page Response:**
```json
{
  "jobs": [...]
  // No next_cursor field = end of results
}
```

### Cursor Formats

Endpoints use different cursor formats based on their data type:

**Integer ID Cursors (64-bit integers):**
Most endpoints use simple integer IDs as cursors:
- Jobs: `last_job_id=12345`
- Locations: `last_location_id=67890`
- Files: `last_file_id=54321`

**Version String Cursors:**
Dataset version endpoints use semantic version strings:
- Example: `last_version=1.0.0`
- Format: Standard semantic versioning

**Composite String Cursors:**
Dataset listings use a composite `"name:version"` format:
- Parameter: `last_dataset_id`
- Format: `"name:version"` (e.g., `eth_mainnet:1.0.0`)
- The colon (`:`) is valid per RFC 3986 and doesn't require URL encoding
- Both encoded (`eth_mainnet%3A1.0.0`) and unencoded (`eth_mainnet:1.0.0`) formats are accepted
- URL encoding the colon as `%3A` is recommended for maximum browser compatibility

Example:
```
GET /datasets?limit=50&last_dataset_id=eth_mainnet:1.0.0
```

## Error Handling

All error responses follow a consistent format with:
- `error_code`: Stable, machine-readable code (SCREAMING_SNAKE_CASE)
- `error_message`: Human-readable error description

Error codes are stable across API versions and suitable for programmatic error handling. Messages may change and should only be used for display or logging.

## Important Notes

### Dataset Registration
Supports two main scenarios:
- **Derived datasets** (kind="manifest"): Registered in both object store and metadata database
- **SQL datasets** (other kinds): Dataset definitions stored in object store

### Job Lifecycle
Jobs have the following terminal states that allow deletion:
- **Completed**: Job finished successfully
- **Stopped**: Job was manually stopped
- **Failed**: Job encountered an error

Non-terminal jobs (Scheduled, Running, StopRequested, Stopping) are protected from deletion.

### Storage Locations
- Locations can be active or inactive for queries
- Deleting a location performs comprehensive cleanup including file removal from object store
- Each location is associated with a specific dataset table and storage URL
