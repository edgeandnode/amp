# Upgrading

This document describes the process for upgrading Nozzle between versions.

## Overview

Upgrading Nozzle typically involves:
1. Updating the Nozzle binary or container image
2. Running database migrations to update the metadata database schema
3. Restarting services with the new version

## Database Migrations

### The `migrate` Command

The `migrate` command runs database migrations on the metadata database to ensure the database schema is up-to-date with the current version of Nozzle. This is essential for maintaining database compatibility across version upgrades.

### When to Run Migrations

- **After upgrades**: After updating Nozzle to a new version that includes schema changes
- **Initial setup**: When setting up a new Nozzle deployment for the first time
- **Database recovery**: When restoring from backup or moving to a new database instance
- **CI/CD pipelines**: As part of automated deployment processes

### Basic Usage

```bash
# Run migrations on the metadata database
nozzle migrate --config /path/to/config.toml
```

### Configuration Requirements

The migrate command requires:
- The `--config` parameter is **mandatory** (no default)
- Config file must contain valid `metadata_db.url` setting
- Database user must have sufficient permissions to:
  - Create and modify tables
  - Create indexes
  - Execute DDL statements

### How Migrations Work

1. **Load configuration**: Reads the specified configuration file
2. **Validate settings**: Ensures `metadata_db.url` is present and valid
3. **Connect to database**: Establishes connection to PostgreSQL metadata database
4. **Check migration status**: Determines which migrations have already been applied
5. **Apply pending migrations**: Runs all unapplied migrations in sequential order
6. **Update tracking**: Records successful migrations in the database
7. **Exit status**: Returns success (0) or error (non-zero) exit code

### Migration Management

- Migrations are tracked in a special table within the metadata database
- Each migration is applied exactly once
- Migrations are applied in a specific order to ensure consistency
- Failed migrations will cause the command to exit with an error
- The command is idempotent - running it multiple times is safe

## Troubleshooting

### Migration Failures

**Permission errors**: Ensure database user has CREATE, ALTER, and DROP privileges

```sql
-- Grant necessary permissions
GRANT CREATE, ALTER, DROP ON DATABASE nozzle_metadata TO nozzle_user;
```

**Connection failures**: Verify `metadata_db.url` in config file is correct

```bash
# Test database connection
psql "postgresql://user:pass@host:5432/nozzle_metadata"
```

**Migration failures**: Check logs for specific error messages

```bash
# Run with debug logging
NOZZLE_LOG=debug nozzle migrate --config ./config/production.toml
```

**Partial migrations**: If a migration fails partway through, manual intervention may be required

```bash
# Check migration status in database
psql -c "SELECT * FROM _nozzle_migrations ORDER BY id;"

# May need to manually fix database state before retrying
```

### Version Compatibility

- **Backward compatibility**: Newer Nozzle versions can always read data written by older versions
- **Forward compatibility**: Older Nozzle versions may not be able to read data written by newer versions
- **Rolling back**: If you need to roll back, restore database from backup taken before migration

### Best Practices

1. **Backup first**: Always backup the metadata database before upgrading
   ```bash
   pg_dump -Fc nozzle_metadata > backup_$(date +%Y%m%d).dump
   ```

2. **Test in staging**: Test the upgrade process in a staging environment first

3. **Check release notes**: Review release notes for breaking changes and migration notes

4. **Plan downtime**: For major upgrades, plan for brief downtime window

5. **Monitor after upgrade**: Watch logs and metrics closely after upgrading

6. **Have rollback plan**: Know how to rollback if something goes wrong

## Version-Specific Notes

### Future Releases

Check the release notes for each version for specific upgrade instructions and breaking changes.

## See Also

- [Operational Modes](modes.md) - Understanding deployment patterns
- [Configuration Guide](config.md) - Detailed configuration options
- [Deployment Guide](deployment.md) - Production deployment patterns
