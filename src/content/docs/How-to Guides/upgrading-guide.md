---
title: Upgrading Guide
description: Task-oriented guide showing how to deploy amp
---

# Upgrading

This document describes the process for upgrading Amp between versions.

## Overview

Upgrading Amp typically involves:

1. Updating the Amp binary or container image
2. Running database migrations to update the metadata database schema
3. Restarting services with the new version

## Database Migrations

### The `migrate` Command

The `migrate` command runs database migrations on the metadata database to ensure the database schema is up-to-date with the current version of Amp. This is essential for maintaining database compatibility across version upgrades.

### When to Run Migrations

- **After upgrades**: After updating Amp to a new version that includes schema changes
- **Initial setup**: When setting up a new Amp deployment for the first time
- **Database recovery**: When restoring from backup or moving to a new database instance
- **CI/CD pipelines**: As part of automated deployment processes

### Basic Usage

```bash
# Run migrations on the metadata database
ampd migrate --config /path/to/config.toml
```

### Configuration Requirements

The migrate command requires:

- The `--config` parameter is **mandatory** (no default)
- Config file must contain valid `metadata_db.url` setting
- Database user must have sufficient permissions to:
  - Create and modify tables
  - Create indexes
  - Execute DDL statements
