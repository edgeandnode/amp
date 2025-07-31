# Delta Lake Loader

High-performance data loader with zero-copy Arrow → Delta Lake integration. Provides memory and CPU efficient ETL processing with ACID transactions and automatic schema evolution.

## Basic Usage

```python
from nozzle.client import Client

# Configure connection
client = Client(url="grpc://localhost:8080")
client.configure_connection("my_delta", "delta_lake", {
    "table_path": "/tmp/delta_table",
    "partition_by": ["year", "month"]
})

# Load data
result = client.sql("""
    SELECT block_number, block_hash,
           EXTRACT(YEAR FROM timestamp) as year,
           EXTRACT(MONTH FROM timestamp) as month
    FROM ethereum_blocks
""").load("my_delta", "blocks_partitioned")

print(f"✅ {result.rows_loaded} rows in {result.duration:.2f}s")
```

### Common Issues
- **"Permission denied"**: Check `storage_options` credentials
- **"Partition column not found"**: Ensure partition columns exist in data
- **"Out of memory"**: Use streaming with `read_all=False`

## Configuration Reference

### Core Options
```python
config = {
    "table_path": "/path/to/table",           # Required: table location
    "partition_by": ["year", "month"],         # Partition columns
    "optimize_after_write": True,             # Auto-optimize files
    "schema_evolution": True,                 # Allow schema changes
    "storage_options": {}                     # Cloud credentials
}
```

### Storage Backends

**Local:**
```python
{"table_path": "/data/delta/table"}
```

**AWS S3:**
```python
{
    "table_path": "s3://bucket/table",
    "storage_options": {
        "AWS_ACCESS_KEY_ID": "key",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_REGION": "us-east-1"
    }
}
```

**Azure:**
```python
{
    "table_path": "abfss://container@account.dfs.core.windows.net/table",
    "storage_options": {
        "AZURE_STORAGE_ACCOUNT_NAME": "account",
        "AZURE_STORAGE_ACCOUNT_KEY": "key"
    }
}
```

**Google Cloud:**
```python
{
    "table_path": "gs://bucket/table",
    "storage_options": {
        "GOOGLE_SERVICE_ACCOUNT": "/path/to/service-account.json"
    }
}
```

## Usage Patterns

### Loading Modes
```python
from nozzle.loaders.base import LoadMode

# Create/replace table
result = client.sql(query).load("conn", "table", mode=LoadMode.OVERWRITE)

# Append to existing
result = client.sql(query).load("conn", "table", mode=LoadMode.APPEND)
```

### Streaming Large Datasets
```python
# Memory-efficient streaming
for batch_result in client.sql("SELECT * FROM huge_table").load("conn", "dest"):
    print(f"Loaded {batch_result.rows_loaded} rows")
```

### Schema Evolution
```python
# Automatically handles new columns
initial_data = client.sql("SELECT id, name FROM users").load("conn", "users")
extended_data = client.sql("SELECT id, name, email FROM users").load("conn", "users")
# New 'email' column added automatically
```

## Performance Optimization

### Partitioning
```python
# Partition by time for better query performance
config = {
    "table_path": "/data/events",
    "partition_by": ["year", "month"],
    "optimize_after_write": True
}

# Query will only scan relevant partitions
client.sql("SELECT * FROM events WHERE year = 2024 AND month = 1")
```

### File Size Optimization
```python
config = {
    "table_path": "/data/table",
    "file_size_hint": 128 * 1024 * 1024,  # 128MB files
    "max_rows_per_file": 1000000,         # 1M rows max
    "optimize_after_write": True           # Compact small files
}
```

### Manual Optimization
```python
from nozzle.loaders.implementations.deltalake_loader import DeltaLakeLoader

loader = DeltaLakeLoader(config)
with loader:
    # Compact small files
    result = loader.optimize_table()
    print(f"Optimization: {result['duration_seconds']}s")
    
    # Clean up old versions (careful!)
    vacuum_result = loader.vacuum_table(retention_hours=168)  # 7 days
    print(f"Vacuum deleted {vacuum_result['files_deleted']} files")
```

## Operations & Maintenance

### Table Statistics
```python
with loader:
    stats = loader.get_table_stats()
    print(f"Version: {stats['version']}")
    print(f"Files: {stats['num_files']}")
    print(f"Size: {stats['size_bytes']} bytes")
    print(f"Partitions: {stats['partition_columns']}")
```

### History & Versioning
```python
# View table history
history = loader.get_table_history(limit=5)
for entry in history:
    print(f"v{entry['version']}: {entry['operation']} at {entry['timestamp']}")
```

### Health Monitoring
```python
def check_table_health(loader):
    stats = loader.get_table_stats()
    
    # Too many small files?
    if stats['num_files'] > 1000:
        print("⚠️  Consider optimization")
    
    # Average file size too small?
    avg_size = stats['size_bytes'] / stats['num_files']
    if avg_size < 10 * 1024 * 1024:  # 10MB
        print("⚠️  Small files detected")
    
    return stats
```
