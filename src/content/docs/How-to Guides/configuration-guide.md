---
title: Configuration Mode
description: Task-oriented guide for all Amp components and settings
---

## How-To Guide: Configuring Amp

This guide provides step-by-step instructions for configuring Amp, including storage directories, service addresses, and authentication.

## How to Set Up Basic Configuration

### Create a Configuration File

1. Create a TOML configuration file based on the [sample config](config.sample.toml)
2. Set the configuration file path in the environment:
   ```bash
   export AMP_CONFIG=/path/to/your/config.toml
   ```

### Configure Required Storage Directories

Your configuration file must specify three directories:

```toml
# Directory containing dataset definitions (input)
manifests_dir = "/path/to/manifests"

# Directory containing provider configurations for external services
providers_dir = "/path/to/providers"

# Directory where extracted Parquet files are stored (output)
data_dir = "/path/to/data"
```

**Directory purposes**:

- `manifests_dir`: Contains dataset definitions (extraction input)
- `providers_dir`: Contains provider configurations for services like Firehose
- `data_dir`: Stores extracted Parquet tables (can be initially empty)

---

## How to Override Configuration with Environment Variables

### Override Any Configuration Value

To override values from your config file using environment variables:

1. Prefix the environment variable name with `AMP_CONFIG_`
2. Use uppercase for the configuration key name
3. Set the environment variable

**Example**: Override the `data_dir` value:

```bash
export AMP_CONFIG_DATA_DIR=/new/path/to/data
```

This environment variable will override the `data_dir` value in your TOML config file.

### Common Configuration Overrides

```bash
# Override manifests directory
export AMP_CONFIG_MANIFESTS_DIR=/path/to/manifests

# Override providers directory
export AMP_CONFIG_PROVIDERS_DIR=/path/to/providers

# Override data directory
export AMP_CONFIG_DATA_DIR=/path/to/data
```

---

## How to Configure Service Addresses

### Set Custom Service Ports

Add these optional keys to your configuration file to customize service addresses:

```toml
# Arrow Flight RPC server address
flight_addr = "0.0.0.0:1602"  # Default

# JSON Lines server address
jsonl_addr = "0.0.0.0:1603"   # Default

# Admin API server address
admin_api_addr = "0.0.0.0:1610"  # Default
```

### Bind to Specific Network Interfaces

To bind services to specific network interfaces:

```toml
# Bind only to localhost
flight_addr = "127.0.0.1:1602"
jsonl_addr = "127.0.0.1:1603"
admin_api_addr = "127.0.0.1:1610"

# Or bind to a specific IP
flight_addr = "192.168.1.100:1602"
```

### Use Non-Default Ports

To avoid port conflicts or meet deployment requirements:

```toml
flight_addr = "0.0.0.0:8080"
jsonl_addr = "0.0.0.0:8081"
admin_api_addr = "0.0.0.0:8082"
```

---

## How to Configure Logging

### Set Basic Log Level

Control logging verbosity with the `AMP_LOG` environment variable:

```bash
# Set to error level (least verbose)
export AMP_LOG=error

# Set to warning level
export AMP_LOG=warn

# Set to info level
export AMP_LOG=info

# Set to debug level (default)
export AMP_LOG=debug

# Set to trace level (most verbose)
export AMP_LOG=trace
```

### Configure Fine-Grained Logging

For more detailed control, use the standard `RUST_LOG` environment variable:

```bash
# Set specific module log levels
export RUST_LOG=amp=debug,tokio=warn

# Enable debug logging for specific components
export RUST_LOG=amp::server=debug,amp::worker=info
```

---

## How to Configure Object Storage

### Use Local Filesystem Storage

For local storage, use filesystem paths in your configuration:

```toml
manifests_dir = "/local/path/to/manifests"
providers_dir = "/local/path/to/providers"
data_dir = "/local/path/to/data"
```

### Configure S3-Compatible Storage

#### Set S3 Bucket URLs

In your configuration file:

```toml
manifests_dir = "s3://my-bucket/manifests"
providers_dir = "s3://my-bucket/providers"
data_dir = "s3://my-bucket/data"
```

#### Configure S3 Authentication

Set the following environment variables:

```bash
# Required: Access credentials
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key

# Required: Region
export AWS_DEFAULT_REGION=us-east-1

# Optional: Custom endpoint (for S3-compatible services)
export AWS_ENDPOINT=https://s3-compatible-service.com

# Optional: Session token (for temporary credentials)
export AWS_SESSION_TOKEN=your-session-token

# Optional: Allow non-TLS connections (not recommended for production)
export AWS_ALLOW_HTTP=true
```

### Configure Google Cloud Storage (GCS)

#### Set GCS Bucket URLs

In your configuration file:

```toml
manifests_dir = "gs://my-bucket/manifests"
providers_dir = "gs://my-bucket/providers"
data_dir = "gs://my-bucket/data"
```

#### Configure GCS Authentication

Choose one of these authentication methods:

**Option 1: Service Account File**

```bash
export GOOGLE_SERVICE_ACCOUNT_PATH=/path/to/service-account.json
```

**Option 2: Service Account Key (JSON)**

```bash
export GOOGLE_SERVICE_ACCOUNT_KEY='{"type":"service_account","project_id":"..."}'
```

**Option 3: Application Default Credentials**

```bash
# Use gcloud CLI to set up ADC
gcloud auth application-default login
```

---

## How to Set Up Different Deployment Configurations

### Development Configuration

For local development with filesystem storage:

```toml
# config.dev.toml
manifests_dir = "./datasets/manifests"
providers_dir = "./datasets/providers"
data_dir = "./datasets/data"

# Bind to localhost only
flight_addr = "127.0.0.1:1602"
jsonl_addr = "127.0.0.1:1603"
admin_api_addr = "127.0.0.1:1610"
```

```bash
export AMP_CONFIG=./config.dev.toml
export AMP_LOG=debug
```

### Production Configuration with S3

For production deployment with S3 storage:

```toml
# config.prod.toml
manifests_dir = "s3://prod-amp-bucket/manifests"
providers_dir = "s3://prod-amp-bucket/providers"
data_dir = "s3://prod-amp-bucket/data"

# Bind to all interfaces
flight_addr = "0.0.0.0:1602"
jsonl_addr = "0.0.0.0:1603"
admin_api_addr = "0.0.0.0:1610"
```

```bash
export AMP_CONFIG=./config.prod.toml
export AMP_LOG=info
export AWS_ACCESS_KEY_ID=prod-key
export AWS_SECRET_ACCESS_KEY=prod-secret
export AWS_DEFAULT_REGION=us-east-1
```

### Production Configuration with GCS

For production deployment with Google Cloud Storage:

```toml
# config.gcs.toml
manifests_dir = "gs://prod-amp-bucket/manifests"
providers_dir = "gs://prod-amp-bucket/providers"
data_dir = "gs://prod-amp-bucket/data"
```

```bash
export AMP_CONFIG=./config.gcs.toml
export AMP_LOG=info
export GOOGLE_SERVICE_ACCOUNT_PATH=/secrets/gcs-service-account.json
```

---

## How to Validate Your Configuration

### Check Configuration Loading

1. Set your configuration file path:

   ```bash
   export AMP_CONFIG=/path/to/config.toml
   ```

2. Run Amp with debug logging to verify configuration:

   ```bash
   export AMP_LOG=debug
   ampd migrate  # Or any other command
   ```

3. Check logs for configuration loading messages

### Verify Storage Access

Test access to configured storage directories:

**For filesystem storage**:

```bash
# Check directory permissions
ls -la /path/to/manifests
ls -la /path/to/providers
ls -la /path/to/data
```

**For S3 storage**:

```bash
# Test S3 access with AWS CLI
aws s3 ls s3://my-bucket/manifests/
aws s3 ls s3://my-bucket/providers/
aws s3 ls s3://my-bucket/data/
```

**For GCS storage**:

```bash
# Test GCS access with gsutil
gsutil ls gs://my-bucket/manifests/
gsutil ls gs://my-bucket/providers/
gsutil ls gs://my-bucket/data/
```

---

## Common Configuration Patterns

### Mixed Storage Configuration

Use different storage backends for different directories:

```toml
# Local manifests for easy editing
manifests_dir = "/local/manifests"

# Shared providers in S3
providers_dir = "s3://shared-bucket/providers"

# Data in GCS for production
data_dir = "gs://prod-bucket/data"
```

### Environment-Specific Overrides

Keep base configuration in TOML, override per environment:

```bash
# Base configuration
export AMP_CONFIG=./config.base.toml

# Development overrides
export AMP_CONFIG_DATA_DIR=/tmp/amp-data
export AMP_LOG=debug

# Production overrides
export AMP_CONFIG_DATA_DIR=s3://prod-bucket/data
export AMP_LOG=info
```

### High-Performance Configuration

For maximum performance in production:

```toml
# Use object storage for scalability
manifests_dir = "s3://amp-prod/manifests"
providers_dir = "s3://amp-prod/providers"
data_dir = "s3://amp-prod/data"

# Separate service endpoints for load distribution
flight_addr = "0.0.0.0:1602"
jsonl_addr = "0.0.0.0:1603"
admin_api_addr = "0.0.0.0:1610"
```

```bash
# Production logging
export AMP_LOG=warn  # Reduce log overhead
export RUST_LOG=amp::critical=info  # Keep critical component logs
```
