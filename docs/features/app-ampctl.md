---
name: "app-ampctl"
description: "Administration CLI for Amp operators. Load when asking about ampctl, admin commands, or operator tooling"
components: "app:ampctl,crate:admin-client"
---

# ampctl - Administration CLI

## Summary

ampctl is the administration CLI for Amp engine operators. It provides a comprehensive command-line interface for managing all aspects of Amp infrastructure through the Admin API. This is the operator's swiss-army knife for Amp administration.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [References](#references)

## Key Concepts

- **Admin CLI**: Command-line interface for infrastructure management
- **Admin API Client**: Communicates with the controller's Admin API
- **Operator Tooling**: Designed for DevOps engineers and administrators

## Architecture

ampctl communicates with the Amp controller's Admin API, providing CLI access to administrative capabilities:

| Capability | Description |
|------------|-------------|
| Dataset Management | List, register, deploy datasets |
| Job Control | Monitor, stop, delete extraction jobs |
| Storage Management | Query storage locations and file metadata |
| Provider Configuration | Manage EVM RPC and Firehose sources |
| Worker Monitoring | List workers and check heartbeat status |
| Schema Analysis | Validate SQL queries and infer schemas |

### Communication

```
ampctl → Admin API (port 1610) → Controller
```

## Configuration

### Admin API URL

ampctl connects to the controller's Admin API to execute commands. The Admin API URL can be configured in three ways:

**Default behavior:**

```bash
# Uses default: http://localhost:1610
ampctl worker list
```

**Via command-line flag:**

```bash
# Connect to production controller
ampctl --admin-url http://production:1610 worker list

# Connect to custom port
ampctl --admin-url http://localhost:8080 datasets list
```

**Via environment variable:**

```bash
# Set for all commands in session
export AMP_ADMIN_URL="http://production:1610"
ampctl worker list
ampctl datasets list
```

**Configuration precedence:**

1. `--admin-url` flag (highest priority)
2. `AMP_ADMIN_URL` environment variable
3. Default: `http://localhost:1610`

### Authentication

Optional authentication token for secured Admin API endpoints:

```bash
# Via flag
ampctl --auth-token <token> worker list

# Via environment variable
export AMP_AUTH_TOKEN="<token>"
ampctl worker list
```

## References

- [app-ampd-controller](app-ampd-controller.md) - Related: Controller and Admin API
