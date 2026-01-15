---
name: "admin-workers-troubleshooting"
description: "Worker debugging workflows and troubleshooting. Load when debugging worker issues, troubleshooting heartbeats, or investigating worker failures"
components: "service:admin-api,crate:admin-client,crate:metadata-db,service:worker"
---

# Worker Troubleshooting

## Summary

Worker troubleshooting provides operators with practical workflows to debug worker issues, monitor worker health, and investigate failures. This includes identifying stalled workers, version mismatches, lifecycle analysis, and automated monitoring patterns.

For basic worker operations (list, inspect), see [admin-workers](admin-workers.md).

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Debugging Workflows](#debugging-workflows)
3. [Advanced Usage](#advanced-usage)
4. [References](#references)

## Key Concepts

- **Heartbeat Interval**: Workers send heartbeats every 1 second
- **Active Threshold**: Workers are considered active if heartbeat is within 5-30 seconds
- **Lifecycle Timestamps**:
  - `created_at`: First time worker registered (never changes)
  - `registered_at`: Last time worker re-registered (updates on restart)
  - `heartbeat_at`: Last heartbeat signal (updates every 1 second)
- **Version Metadata**: Build version, commit SHA, commit timestamp, and build date
- **Advisory Locks**: PostgreSQL locks preventing duplicate node_id instances

## Debugging Workflows

### Worker Health Monitoring

**Check if workers are alive:**

Verify that workers are actively sending heartbeats to confirm they're running and connected.

Workers send heartbeats every 1 second. A worker is considered active if its heartbeat is recent (typically within 5-30 seconds).

```bash
# List all workers and check last heartbeat
ampctl worker list

# Workers with recent heartbeats (< 10s ago) are healthy
# Workers with old heartbeats may be stalled or disconnected
```

**Identify stalled workers:**

Find workers that haven't sent heartbeats recently, which may indicate crashes or connectivity issues.

```bash
# Find workers without recent heartbeats
ampctl worker list --json | jq -r '.workers[] |
  select((.heartbeat_at | fromdateiso8601) < (now - 30)) |
  "⚠ Stale worker: \(.node_id) (last seen: \(.heartbeat_at))"'
```

**Understanding timestamp meanings:**

- `created_at`: First time worker registered (never changes)
- `registered_at`: Last time worker re-registered (updates on restart)
- `heartbeat_at`: Last heartbeat signal (updates every 1 second)

### Version Management

**Check worker build versions:**

View the software version and build information running on each worker.

```bash
# Get version for specific worker
ampctl worker inspect worker-01 --json | jq -r '.info.version'

# List all workers with their versions
ampctl worker list --json | jq -r '.workers[].node_id' | while read worker; do
  version=$(ampctl worker inspect "$worker" --json 2>/dev/null | jq -r '.info.version // "unknown"')
  echo "$worker: $version"
done
```

**Identify version mismatches:**

Detect when workers are running different software versions, which can help troubleshoot compatibility issues.

```bash
# Get unique versions across all workers
ampctl worker list --json | jq -r '.workers[].node_id' | while read worker; do
  ampctl worker inspect "$worker" --json 2>/dev/null | jq -r '.info.version // "unknown"'
done | sort -u

# Expected output if all workers are on same version:
# v0.0.22-15-g8b065bde

# Multiple lines indicate version mismatch across workers
```

**Verify deployment rollout:**

Monitor the progress of rolling deployments by counting how many workers are on each version.

```bash
# Count workers by version
ampctl worker list --json | jq -r '.workers[].node_id' | while read worker; do
  ampctl worker inspect "$worker" --json 2>/dev/null | jq -r '.info.version // "unknown"'
done | sort | uniq -c

# Example output:
# 3 v0.0.22-15-g8b065bde
# 2 v0.0.21-8-ga1b2c3d4
```

### Understanding Worker Lifecycle

**Detect worker restarts:**

Identify when workers have been restarted by comparing their initial creation and last registration times.

Compare `created_at` and `registered_at` timestamps:

```bash
ampctl worker inspect worker-01 --json | jq -r '
  "Created:     \(.created_at)",
  "Registered:  \(.registered_at)",
  "Heartbeat:   \(.heartbeat_at)"'

# If registered_at > created_at, the worker has restarted
# Large gap = long-running worker, small gap = recent restart
```

**Calculate worker uptime:**

Determine how long a worker has been running since its last restart.

```bash
# Time since last restart (registered_at)
ampctl worker inspect worker-01 --json | jq -r '
  (.registered_at | fromdateiso8601) as $reg |
  (now - $reg) / 86400 | floor |
  "Uptime: \(.) days since last restart"'
```

**Analyze registration patterns:**

Get a comprehensive view of all worker lifecycle timestamps to understand restart patterns across the fleet.

```bash
# Show all workers with their lifecycle timestamps
ampctl worker list --json | jq -r '.workers[].node_id' | while read worker; do
  ampctl worker inspect "$worker" --json 2>/dev/null | jq -r '
    "\(.node_id):",
    "  Created:    \(.created_at)",
    "  Registered: \(.registered_at)",
    "  Heartbeat:  \(.heartbeat_at)",
    ""'
done
```

### Troubleshooting Scenarios

**Scenario 1: Worker not accepting jobs**

```bash
# Step 1: Check if worker is registered
ampctl worker list | grep worker-01

# Step 2: Verify heartbeat is recent (< 10 seconds old)
ampctl worker inspect worker-01

# Step 3: Check worker version matches expected deployment
ampctl worker inspect worker-01 --json | jq -r '.info'

# Common causes:
# - Heartbeat stopped (worker crashed or network issue)
# - Worker running different version
# - Database connection lost
```

**Scenario 2: Worker disappeared**

```bash
# Check last known state
ampctl worker inspect worker-01

# Look for:
# - Last heartbeat timestamp (how long ago did it stop?)
# - Registered vs created timestamps (recent restart pattern?)
# - Version info (was it running expected version?)

# If heartbeat is old (> 60s):
# - Worker process likely crashed or stopped
# - Check worker logs for errors
# - Verify network connectivity to metadata DB
```

**Scenario 3: Version mismatch causing issues**

```bash
# List all workers with versions and heartbeats
ampctl worker list --json | jq -r '.workers[] | "\(.node_id): \(.heartbeat_at)"' | \
  while IFS=: read -r worker rest; do
    version=$(ampctl worker inspect "$worker" --json 2>/dev/null | jq -r '.info.version // "unknown"')
    echo "$worker: $version"
  done

# Identify outdated workers that need upgrading
# Verify all workers are running compatible versions
```

**Scenario 4: Multiple workers with same node_id (prevented by system)**

The system prevents this using PostgreSQL advisory locks. Each worker acquires an exclusive lock on its node_id. If a second worker tries to start with the same node_id:

- The second worker will fail to acquire the lock
- Registration will fail with a lock conflict error
- Check worker logs for "failed to acquire advisory lock" errors

```bash
# Verify only one worker per node_id
ampctl worker list --json | jq -r '.workers[].node_id' | sort | uniq -d

# If output is empty: ✓ No duplicate node IDs
# If output shows IDs: ✗ Database integrity issue (shouldn't happen)
```

## Advanced Usage

### Filtering with jq

**Find workers with stale heartbeats:**

Use jq to filter and extract only workers that haven't sent heartbeats within a specific time window.

```bash
# Workers not seen in last 30 seconds
ampctl worker list --json | jq -r '.workers[] |
  select((.heartbeat_at | fromdateiso8601) < (now - 30)) |
  .node_id'
```

**Extract build commit SHA:**

Retrieve the Git commit SHA for each worker to verify exact code versions.

```bash
# Get commit SHA for all workers
ampctl worker list --json | jq -r '.workers[].node_id' | while read worker; do
  commit=$(ampctl worker inspect "$worker" --json 2>/dev/null | jq -r '.info.commit_sha // "unknown"')
  echo "$worker: $commit"
done
```

**Filter workers by version:**

Find all workers running a specific software version for targeted operations or verification.

```bash
# Find workers running specific version
TARGET_VERSION="v0.0.22-15-g8b065bde"

ampctl worker list --json | jq -r '.workers[].node_id' | while read worker; do
  version=$(ampctl worker inspect "$worker" --json 2>/dev/null | jq -r '.info.version')
  if [ "$version" = "$TARGET_VERSION" ]; then
    echo "$worker"
  fi
done
```

### Monitoring Patterns

**Continuous health monitoring:**

Monitor worker status continuously with automatic updates using the watch command.

```bash
# Watch worker heartbeats in real-time
watch -n 5 'ampctl worker list'

# Press Ctrl+C to stop
```

**Alert on inactive workers:**

Create alerts for workers that stop sending heartbeats, useful for monitoring systems or cron jobs.

```bash
# Script to check for inactive workers
#!/bin/bash
THRESHOLD_SECONDS=30

ampctl worker list --json | jq -r '.workers[] |
  select((.heartbeat_at | fromdateiso8601) < (now - '$THRESHOLD_SECONDS')) |
  "ALERT: Worker \(.node_id) inactive since \(.heartbeat_at)"'

# Run this in a cron job or monitoring system
```

**Track worker count over time:**

Log the number of active workers periodically to track capacity trends and detect scaling issues.

```bash
# Log worker count periodically
while true; do
  count=$(ampctl worker list --json | jq '.workers | length')
  timestamp=$(date -Iseconds)
  echo "$timestamp: $count workers active"
  sleep 60
done
```

**Monitor worker version distribution:**

Track which versions are running across your fleet during deployments or upgrades.

```bash
# Get version distribution
ampctl worker list --json | jq -r '.workers[].node_id' | while read worker; do
  ampctl worker inspect "$worker" --json 2>/dev/null | jq -r '.info.version // "unknown"'
done | sort | uniq -c | sort -rn

# Example output:
#   5 v0.0.22-15-g8b065bde
#   2 v0.0.21-8-ga1b2c3d4
#   1 unknown
```

### Scripting and Automation

**Health check script:**

Automated script to verify all workers are healthy and exit with appropriate status codes for CI/CD integration.

```bash
#!/bin/bash
# check-workers.sh - Verify all workers are healthy

THRESHOLD_SECONDS=30
EXIT_CODE=0

echo "Checking worker health..."

ampctl worker list --json | jq -r '.workers[]' | while read -r worker; do
  node_id=$(echo "$worker" | jq -r '.node_id')
  heartbeat=$(echo "$worker" | jq -r '.heartbeat_at')
  heartbeat_ts=$(date -d "$heartbeat" +%s 2>/dev/null || echo 0)
  now_ts=$(date +%s)
  age=$((now_ts - heartbeat_ts))

  if [ $age -gt $THRESHOLD_SECONDS ]; then
    echo "❌ $node_id: heartbeat $age seconds old (threshold: $THRESHOLD_SECONDS)"
    EXIT_CODE=1
  else
    echo "✓ $node_id: healthy ($age seconds ago)"
  fi
done

exit $EXIT_CODE
```

**Deployment verification script:**

Verify all workers are running the expected version after a deployment, with exit codes for automation.

```bash
#!/bin/bash
# verify-deployment.sh - Ensure all workers are on expected version

EXPECTED_VERSION="v0.0.22-15-g8b065bde"
EXIT_CODE=0

echo "Verifying deployment: $EXPECTED_VERSION"

ampctl worker list --json | jq -r '.workers[].node_id' | while read worker; do
  version=$(ampctl worker inspect "$worker" --json 2>/dev/null | jq -r '.info.version')

  if [ "$version" = "$EXPECTED_VERSION" ]; then
    echo "✓ $worker: $version"
  else
    echo "❌ $worker: $version (expected: $EXPECTED_VERSION)"
    EXIT_CODE=1
  fi
done

exit $EXIT_CODE
```

**Export worker inventory:**

Generate a CSV report of all workers with their metadata for auditing or analysis in spreadsheet tools.

```bash
# Generate CSV report of all workers
echo "node_id,version,commit_sha,created_at,registered_at,heartbeat_at" > workers-inventory.csv

ampctl worker list --json | jq -r '.workers[].node_id' | while read worker; do
  ampctl worker inspect "$worker" --json 2>/dev/null | jq -r '
    [.node_id, .info.version, .info.commit_sha, .created_at, .registered_at, .heartbeat_at] |
    @csv'
done >> workers-inventory.csv

echo "Inventory saved to workers-inventory.csv"
```

## References

- [admin-workers](admin-workers.md) - Base: Worker management commands
- [app-ampd-worker](app-ampd-worker.md) - Related: Worker process implementation
- [admin](admin.md) - Related: Administration overview
