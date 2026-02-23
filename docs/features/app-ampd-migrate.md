---
name: "app-ampd-migrate"
description: "ampd migrate command for metadata database migrations. Load when asking about database migrations, schema upgrades, or ampd migrate"
type: feature
status: "stable"
components: "app:ampd,crate:metadata-db,crate:config"
---

# ampd Migrate

## Summary

The `ampd migrate` command runs database migrations on the metadata database. It connects to the configured PostgreSQL instance, applies any pending schema migrations, and exits. This is a one-shot administrative command used during upgrades or initial setup.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Configuration](#configuration)
3. [Usage](#usage)
4. [References](#references)

## Key Concepts

- **Metadata Database**: PostgreSQL database storing job state, worker registrations, dataset definitions, and table revisions
- **Schema Migration**: Automatic application of pending DDL changes to bring the database schema up to date
- **One-Shot Command**: Unlike other ampd subcommands, `migrate` runs to completion and exits

## Configuration

`ampd migrate` requires `--config` (or `AMP_CONFIG`) to be provided. The config file must contain a valid
`metadata_db.url` (or the `AMP_CONFIG_METADATA_DB__URL` environment variable must be set).

| Setting           | Source                                       | Description                  |
|-------------------|----------------------------------------------|------------------------------|
| `metadata_db.url` | Config file or `AMP_CONFIG_METADATA_DB__URL` | PostgreSQL connection string |

## Usage

### Running Migrations

```bash
# Apply pending migrations using a config file
ampd --config .amp/config.toml migrate

# Using the AMP_CONFIG environment variable
export AMP_CONFIG=.amp/config.toml
ampd migrate
```

### When to Run

- **Initial setup**: Before starting any ampd service for the first time
- **After upgrades**: When a new ampd version includes schema changes
- **CI/CD pipelines**: As a pre-deployment step

### Auto-Migration Alternative

In non-production environments, the metadata database connection can be configured with
`auto_migrate = true` in the config file (defaults to `true` in solo mode). When enabled, services automatically apply migrations on startup, making the explicit
`migrate` command unnecessary.

```toml
[metadata_db]
url = "postgresql://localhost/amp"
auto_migrate = true
```

## References

- [app-ampd](app-ampd.md) - Base: ampd daemon overview
