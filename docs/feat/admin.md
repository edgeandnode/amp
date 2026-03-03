---
name: "admin"
description: "Management and administration features for Amp. Load when asking about administration, management, jobs, datasets, or operators"
type: meta
components: "app:ampd,app:ampctl,service:controller,crate:admin-client,crate:admin-api"
---

# Administration

## Summary

Amp provides comprehensive management and administration capabilities for operators. This includes dataset management, job control, worker monitoring, storage configuration, and provider management through both CLI and REST API interfaces.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Usage](#usage)
4. [References](#references)

## Key Concepts

- **Operator**: User responsible for managing Amp infrastructure
- **Admin Client**: Library for communicating with the Admin API
- **Dataset Management**: Registration, deployment, and lifecycle of datasets
- **Job Control**: Monitoring, stopping, and managing extraction jobs

## Architecture

Administration features are accessed through two interfaces:

| Interface | Description |
|-----------|-------------|
| CLI | [ampctl](app-ampctl.md) command-line tool for operators |
| API | RESTful Admin API served by [ampd controller](app-ampd-controller.md) |

### Administration Flow

```
Operator → ampctl CLI → Admin API (port 1610) → Controller → Metadata DB
```

## Usage

Administration is performed through the [ampctl](app-ampctl.md) CLI or directly via the Admin API:

```bash
# CLI examples
ampctl datasets list
ampctl jobs list
ampctl worker list

# API examples
curl http://localhost:1610/datasets
curl http://localhost:1610/jobs
curl http://localhost:1610/workers
```

For detailed operations, see subdomain feature docs.

## References

- [app-ampctl](app-ampctl.md) - Related: Administration CLI
- [app-ampd-controller](app-ampd-controller.md) - Related: Admin API provider
