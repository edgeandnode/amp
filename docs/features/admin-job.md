---
name: "app-ampctl-job"
description: "Job management commands: list, inspect, stop, remove, prune, progress. Load when asking about managing extraction jobs via ampctl CLI"
type: feature
status: stable
components: "app:ampctl,crate:admin-client,service:admin-api"
---

# Job Management

## Summary

Job commands provide operational control over extraction jobs. Operators can list jobs with pagination, inspect job details, stop running jobs gracefully, remove individual terminal jobs, bulk-prune terminal jobs by status, and monitor sync progress with block-level detail for each table.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [API Reference](#api-reference)
4. [Implementation](#implementation)
5. [References](#references)

## Key Concepts

- **Job**: An extraction task that syncs blockchain data for a dataset, executed by a worker node
- **Terminal State**: A job that has finished executing (`COMPLETED`, `STOPPED`, or `ERROR`)
- **Progress**: Sync state of a job's tables, including `current_block`, `start_block`, and file statistics
- **Pagination**: Jobs are listed in pages using `--limit` (default: 50) and `--after` (cursor ID)

## Usage

**List jobs:**

View jobs with pagination support. Default limit is 50 jobs.

```bash
# List first 50 jobs (default)
ampctl job list
ampctl job ls  # alias

# List first 100 jobs
ampctl job list --limit 100

# Paginate - get next 50 jobs after ID 1234
ampctl job list --after 1234

# Filter by status (default: "active" — non-terminal jobs)
ampctl job list --status all
ampctl job list --status scheduled,running
ampctl job list -s completed

# Short aliases
ampctl job list -l 20 -a 500
```

**Inspect a job:**

Get detailed information about a specific job including status, worker assignment, and dataset descriptor.

```bash
ampctl job inspect 12345
ampctl job get 12345  # alias

# Extract specific fields
ampctl job inspect 12345 | jq '.status'
```

**Stop a running job:**

Request graceful termination of a running job. The worker finishes current operations before stopping. Confirm with `ampctl job inspect` afterwards.

```bash
ampctl job stop 12345
```

**Remove a job:**

Delete a specific job by ID. Only jobs in terminal states can be removed. Running jobs must be stopped first.

```bash
ampctl job rm 123
ampctl job remove 456  # alias
```

**Prune terminal jobs:**

Bulk-remove jobs by status filter. Defaults to all terminal states.

```bash
# Prune all terminal jobs (completed, stopped, error)
ampctl job prune

# Prune only completed jobs
ampctl job prune --status completed

# Prune only failed jobs
ampctl job prune --status error

# Prune only stopped jobs
ampctl job prune --status stopped
```

**Monitor job progress:**

View sync progress for all tables written by a job, including current block and file statistics.

```bash
ampctl job progress 123
ampctl job progress 123 --json
```

**JSON output for scripting:**

```bash
ampctl job list --json
ampctl job list --json | jq '.jobs[] | select(.status == "COMPLETED")'
ampctl job inspect 12345 --json | jq '.status'
```

## API Reference

For request/response schemas, see [Admin API OpenAPI spec](../openapi-specs/admin.spec.json):

```bash
jq '.paths | to_entries[] | select(.key | startswith("/jobs"))' docs/openapi-specs/admin.spec.json
```

## Implementation

### Source Files

- `crates/bin/ampctl/src/cmd/job/` - CLI command implementations
- `crates/clients/admin/src/jobs.rs` - Admin API client library
- `crates/services/admin-api/src/handlers/jobs/` - API endpoint handlers

## References

- [app-ampctl](app-ampctl.md) - Base: ampctl overview
- [admin](admin.md) - Related: Administration overview
- [admin-jobs-progress](admin-jobs-progress.md) - Related: Job progress API details
