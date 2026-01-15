---
name: "app-ampd"
description: "ampd daemon for data extraction, transformation, and query serving. Load when asking about ampd, operational modes, or the Amp daemon"
components: "app:ampd"
---

# ampd - The Amp Daemon

## Summary

ampd is the core Amp daemon that handles data extraction, transformation, and query serving. It is a multi-mode executable that can run in different operational configurations depending on deployment needs, from single-node development setups to distributed production deployments.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [References](#references)

## Key Concepts

- **Daemon**: Long-running process that provides Amp's core functionality
- **Extraction**: Process of fetching and transforming blockchain data from sources
- **Query Serving**: Exposing transformed data via SQL query interfaces

## Architecture

ampd supports multiple operational modes and deployment patterns. For detailed information about modes, deployment patterns, and scaling strategies, see [Operational Modes](../modes.md).

### Quick Reference

| Command | Description |
|---------|-------------|
| `ampd solo` | Single-node mode for development |
| `ampd server` | Query server (Flight + JSONL) |
| `ampd controller` | Job scheduling and admin API |
| `ampd worker` | Data extraction worker |

## References

- [Operational Modes](../modes.md) - Related: Deployment patterns and scaling
