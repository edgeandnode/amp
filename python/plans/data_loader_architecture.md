# Data Loader Architecture Plan

## Overview
Design a modular, extensible data loading system that integrates with the existing Flight SQL client to support multiple storage technologies with **zero-copy operations** for maximum performance and efficiency.

## Core Design Principles

### 1. Zero-Copy Architecture
- **Direct Arrow Operations**: Work directly with Arrow data structures without conversions
- **Native Format Writers**: Use Arrow's built-in format writers (CSV, Parquet, JSON)
- **Streaming Operations**: Process data in batches without accumulating in memory
- **Minimal Data Copies**: Eliminate unnecessary Pandas conversions and intermediate representations

### 2. Plugin Architecture with Auto-Discovery
- **Abstract Base Class**: Define a common interface for all data loaders
- **Auto-Discovery**: Automatic scanning and registration of loader implementations
- **Zero Registration**: Loaders work without explicit registration code
- **Configuration-Driven**: Support for different connection parameters and options per technology

### 3. Simplified Usage Patterns
- **Single Method Interface**: `query_and_load()` works for all storage technologies
- **Flexible Configuration**: Named connections, environment variables, and inline configs
- **Connection Management**: Persistent connection configurations with automatic discovery

## File Structure Plan

### New Files to Create

#### `loaders/`
```
loaders/
├── __init__.py              # Auto-discovery and convenience functions
├── base.py                  # Abstract base classes and common utilities
├── registry.py              # Auto-discovery registry with zero-config
├── implementations/         # Directory for specific loader implementations
│   ├── __init__.py
│   ├── postgresql_loader.py    # PostgreSQL zero-copy implementation
│   ├── redis_loader.py         # Redis zero-copy implementation  
│   ├── snowflake_loader.py     # Snowflake zero-copy implementation
│   ├── delta_lake_loader.py    # Delta Lake zero-copy implementation
│   └── iceberg_loader.py       # Apache Iceberg zero-copy implementation
└── utils/
    ├── __init__.py
    ├── schema_mapping.py    # Arrow to target schema conversion
    ├── batch_utils.py       # Batching and streaming utilities
    └── connection_pool.py   # Connection pooling utilities
```

#### `config/`
```
config/
├── __init__.py
├── connection_manager.py    # Named connections and env var support
├── loader_configs.py       # Configuration schemas for each loader
└── defaults.py            # Default configuration values
```

### Files to Modify

#### `client.py`
- Add `sql()` method that returns a `QueryBuilder` instance
- Create `QueryBuilder` class with `.load()`, `.stream()`, `.to_arrow()` methods
- Integrate with auto-discovery registry through existing `query_and_load()`
- Add connection configuration management
- Maintain backward compatibility with existing methods

#### `util.py`
- Add zero-copy optimization utilities
- Enhance existing data processing functions
- Add Arrow-specific utility functions

## Technology-Specific Implementation Plan

### 1. PostgreSQL Loader
**Dependencies**: `psycopg2` or `asyncpg`
**Zero-Copy Strategy**: Arrow → CSV → PostgreSQL COPY
**Key Features**:
- Connection pooling with `ThreadedConnectionPool`
- Direct Arrow CSV writer (`pa.csv.write_csv()`)
- Efficient COPY protocol for bulk loading
- Schema auto-creation from Arrow schema
- No Pandas conversion - direct Arrow operations

**Performance**: ~50% memory reduction, ~2-3x speed improvement

**Configuration Example**:
```python
{
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "user": "user",
    "password": "pass",
    "batch_size": 10000,
    "create_table": True,
    "upsert_key": "id"
}
```

### 2. Redis Loader
**Dependencies**: `redis-py`
**Zero-Copy Strategy**: Direct Arrow scalar access with binary operations
**Key Features**:
- Multiple data structure support (Hash, Stream, JSON, TimeSeries)
- Direct Arrow column access (`column[row_idx].as_py()`)
- Pipeline batching for efficient Redis operations
- Binary-first operations to avoid string conversions
- Automatic key generation strategies

**Performance**: ~30% memory reduction, ~1.5-2x speed improvement

**Configuration Example**:
```python
{
    "host": "localhost",
    "port": 6379,
    "db": 0,
    "password": None,
    "data_structure": "hash",  # hash, stream, timeseries
    "key_pattern": "{table}:{id}",
    "batch_size": 1000,
    "ttl": 3600
}
```

### 3. Snowflake Loader
**Dependencies**: `snowflake-connector-python`
**Zero-Copy Strategy**: Arrow → Parquet → PUT/COPY staging
**Key Features**:
- Direct Arrow to Parquet conversion (`pq.write_table()`)
- Efficient PUT/COPY staging mechanism
- Automatic stage management and cleanup
- Warehouse scaling and optimization
- Temporary file management with automatic cleanup

**Performance**: ~60% memory reduction, ~3-4x speed improvement

**Configuration Example**:
```python
{
    "account": "account.region",
    "user": "user",
    "password": "pass",
    "database": "DB",
    "schema": "SCHEMA",
    "warehouse": "WAREHOUSE",
    "stage": "@my_stage",
    "file_format": "PARQUET",
    "auto_create_table": True
}
```

### 4. Delta Lake Loader
**Dependencies**: `deltalake`, `delta-spark`
**Zero-Copy Strategy**: Direct Arrow → Delta Lake integration
**Key Features**:
- Direct Arrow Table to Delta Lake (`write_deltalake(data=table)`)
- ACID transactions with automatic versioning
- Schema evolution and automatic merging
- Partition management and optimization
- No intermediate format conversions

**Performance**: ~70% memory reduction, ~4-5x speed improvement

**Configuration Example**:
```python
{
    "table_path": "s3://bucket/path/to/table",
    "storage_options": {
        "AWS_ACCESS_KEY_ID": "key",
        "AWS_SECRET_ACCESS_KEY": "secret"
    },
    "mode": "append",  # append, overwrite, merge
    "partition_by": ["date"],
    "optimize_after_write": True
}
```

### 5. Apache Iceberg Loader
**Dependencies**: `pyiceberg`
**Zero-Copy Strategy**: Direct Arrow → Iceberg integration
**Key Features**:
- Native Arrow schema compatibility
- Direct table append operations (`table.append(arrow_table)`)
- Schema evolution with automatic handling
- Catalog integration (Hive, Glue, REST)
- Snapshot management and versioning

**Performance**: ~70% memory reduction, ~4-5x speed improvement

**Configuration Example**:
```python
{
    "catalog_uri": "thrift://localhost:9083",
    "catalog_type": "hive",  # hive, glue, rest
    "namespace": "default",
    "table_name": "my_table",
    "properties": {
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "snappy"
    }
}
```

## Usage Interface Design

### Simplified Chaining Interface
```python
from nozzle.client import Client

# Initialize client - auto-discovery handles loader registration
client = Client(url="grpc://localhost:8080")

# Simple chaining interface - query → load to destination
result = client.sql("SELECT * FROM events WHERE date = '2024-01-01'") \
    .load("postgresql", "events_copy", connection="my_pg")

# Different loaders, same interface
result = client.sql("SELECT * FROM events") \
    .load("redis", "events_cache", connection="my_redis", ttl=3600)

result = client.sql("SELECT * FROM events") \
    .load("delta_lake", "s3://bucket/events", connection="my_delta")

# Alternative consumption methods
table = client.sql("SELECT * FROM events").to_arrow()  # In-memory Arrow Table

for batch in client.sql("SELECT * FROM large_table").stream():  # Streaming
    print(f"Processing {batch.num_rows} rows")

# Configuration flexibility
client.configure_connection("my_pg", "postgresql", {
    "host": "localhost", "database": "analytics", 
    "user": "etl_user", "password": "secret"
})

# Environment variables (zero config)
# Set DATABASE_URL=postgresql://user:pass@localhost/db
result = client.sql("SELECT * FROM events") \
    .load("postgresql", "events_copy", connection="postgresql")

# Inline configuration
result = client.sql("SELECT * FROM events") \
    .load("redis", "events_cache", 
          config={"host": "localhost", "data_structure": "hash"})

# Loader-specific options
result = client.sql("SELECT * FROM events") \
    .load("postgresql", "events_table", 
          connection="my_pg",
          batch_size=5000,
          create_table=True,
          mode="append")

# Discovery
print("Available loaders:", client.get_available_loaders())
# Output: ['postgresql', 'redis', 'snowflake', 'delta_lake', 'iceberg']
```

### QueryBuilder Architecture
```python
class QueryBuilder:
    def __init__(self, client: Client, query: str):
        self.client = client
        self.query = query
    
    def load(self, loader: str, destination: str, connection: str = None, 
             config: Dict[str, Any] = None, **kwargs) -> LoadResult:
        """Load query results to specified destination using registry"""
        return self.client.query_and_load(
            query=self.query,
            loader=loader,
            table_name=destination,
            connection_name=connection,
            config=config,
            **kwargs
        )
    
    def stream(self) -> Iterator[pa.RecordBatch]:
        """Stream query results as Arrow batches"""
        return self.client.get_sql(self.query, read_all=False)
    
    def to_arrow(self) -> pa.Table:
        """Get query results as Arrow table"""
        return self.client.get_sql(self.query, read_all=True)
    
    def get_sql(self, read_all: bool = False):
        """Backward compatibility with existing method"""
        return self.client.get_sql(self.query, read_all=read_all)
```

### Enhanced Client Class
```python
class Client:
    def __init__(self, url):
        self.conn = flight.connect(url)
        self.connection_manager = ConnectionManager()
    
    def sql(self, query: str) -> QueryBuilder:
        """Create a chainable query builder"""
        return QueryBuilder(self, query)
    
    # Existing methods remain for backward compatibility
    def get_sql(self, query, read_all=False):
        # ... existing implementation
    
    def query_and_load(self, query, loader, table_name, ...):
        # ... existing implementation
```

### Zero-Copy Performance Benefits
- **PostgreSQL**: Arrow → CSV → COPY (50% memory reduction)
- **Redis**: Direct scalar access (30% memory reduction)
- **Snowflake**: Arrow → Parquet → PUT (60% memory reduction)
- **Delta Lake**: Direct Arrow integration (70% memory reduction)
- **Iceberg**: Direct Arrow integration (70% memory reduction)

### Key Interface Benefits
1. **Technology-Agnostic**: Single `.load()` method for all destinations
2. **Registry-Driven**: Auto-discovery means new loaders work automatically
3. **Consistent Parameters**: Same pattern across all storage technologies
4. **Intuitive Flow**: Natural query → destination progression
5. **Backward Compatible**: Existing methods continue to work
6. **Extensible**: Easy to add new consumption methods (`.to_parquet()`, etc.)

## Implementation Phases

### Phase 1: Zero-Copy Foundation with Chaining Interface (Week 1-2)
- Create base architecture with zero-copy principles
- Implement auto-discovery registry system
- Create connection management with env var support
- Add `QueryBuilder` class with chaining interface
- Update client.py with `sql()` method and `query_and_load()` backend

### Phase 2: Core Loaders (Week 3-4)
- Implement PostgreSQL loader with zero-copy CSV operations
- Implement Redis loader with direct Arrow scalar access
- Validate chaining interface with real loaders
- Add comprehensive testing framework
- Create benchmarking tools for performance validation

### Phase 3: Cloud/Analytics Loaders (Week 5-6)
- Implement Snowflake loader with zero-copy Parquet staging
- Implement Delta Lake loader with direct Arrow integration
- Test chaining interface across all loader types
- Add performance optimizations and monitoring
- Create performance comparison benchmarks

### Phase 4: Advanced Features (Week 7-8)
- Implement Iceberg loader with native Arrow support
- Add advanced chaining features (parameterized queries, etc.)
- Performance tuning and optimization across all loaders
- Create migration and optimization tools

### Phase 5: Production Ready (Week 9-10)
- Comprehensive error handling and retry logic
- Monitoring and observability features
- Production deployment guides
- Performance benchmarks and optimization guides
- Documentation for chaining interface adoption

## Extension Points for Future Technologies

### Planned Extension Categories
1. **Time Series DBs**: InfluxDB, TimescaleDB, ClickHouse
2. **Graph DBs**: Neo4j, Amazon Neptune, ArangoDB
3. **Search Engines**: Elasticsearch, Solr, OpenSearch
4. **Streaming**: Kafka, Pulsar, Kinesis
5. **Object Storage**: S3, GCS, Azure Blob (as Parquet/ORC)
6. **In-Memory**: Apache Spark, Dask, Ray

### Plugin Development Guide
Each new loader follows this pattern:
1. Inherit from `DataLoader` base class
2. Implement required abstract methods with zero-copy operations
3. Add configuration schema and connection management
4. Auto-discovery handles registration automatically
5. Test with chaining interface: `client.sql("SELECT...").load("new_loader", ...)`
6. Add tests with performance benchmarks
7. Create usage examples and documentation

### Chaining Interface Implementation Patterns
- **QueryBuilder class**: Returns chainable object from `client.sql()`
- **Registry integration**: `.load()` method uses auto-discovery registry
- **Consistent parameters**: Same pattern across all storage technologies
- **Backward compatibility**: Existing `query_and_load()` powers the backend
- **Extensible consumption**: Easy to add `.to_parquet()`, `.to_csv()`, etc.

### Zero-Copy Implementation Patterns
- **Direct Arrow Operations**: Use `column[row_idx].as_py()` instead of DataFrame conversions
- **Native Format Writers**: Leverage Arrow's built-in writers (CSV, Parquet, JSON)
- **Streaming Operations**: Process batches without accumulating data
- **Memory Efficiency**: Minimize data copies and intermediate representations

This architecture ensures easy extensibility while maintaining consistency and maximum performance across all storage technologies, with an intuitive chaining interface that guides users naturally from query to destination.
