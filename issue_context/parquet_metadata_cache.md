# Plan: Add Size-Limited In-Memory Cache for ParquetMetaData Footer

## Overview
Add a foyer-based evicting cache to `QueryEnv` for caching ParquetMetaData footers with key `(LocationId, String)`, limited to 1 GB total memory using `ParquetMetaData::memory_size()` for size accounting.

## Current State Analysis
- **Current flow**: `NozzleReader::get_metadata()` calls `metadata_db.get_footer_bytes(location_id, file_name)` which queries PostgreSQL database every time
- **QueryEnv**: Simple struct with `df_env: Arc<RuntimeEnv>` and `isolate_pool: IsolatePool`
- **Cache key**: `(LocationId, String)` where `LocationId` is from metadata-db crate and `String` is the file name
- **Size accounting**: Use `ParquetMetaData::memory_size()` method for accurate memory usage tracking

## Implementation Steps

### 1. Add foyer dependency
- Add `foyer = { version = "0.18" }` to `common/Cargo.toml`

### 2. Update QueryEnv
- Add cache field: `parquet_footer_cache: Cache<(LocationId, String), Arc<ParquetMetaData>>`
- Configure cache with 1 GB memory limit using foyer's memory-based eviction
- Use `ParquetMetaData::memory_size()` for size calculation in cache configuration

### 3. Modify NozzleReaderFactory 
- Pass QueryEnv or just the cache to NozzleReaderFactory
- Update constructor to accept the cache reference

### 4. Update NozzleReader::get_metadata()
- Check cache first using `(location_id, file_name)` key
- If cache miss, fetch from database as before, parse ParquetMetaData, insert into cache
- If cache hit, return cached Arc<ParquetMetaData>
- Ensure cache insertion accounts for memory size via `memory_size()` method

### 5. Cache configuration specifics
- Set total memory limit to 1 GB (1_073_741_824 bytes)
- Configure foyer to use memory-based eviction policy
- Use LRU or similar eviction algorithm for optimal hit rates

### 6. Threading considerations  
- foyer Cache is thread-safe and designed for concurrent access
- Use Arc<ParquetMetaData> as value type to avoid cloning large metadata

## Key Benefits
- Reduces database queries for frequently accessed Parquet files
- Improves query performance for repeated file access
- Memory-bounded cache prevents excessive memory usage (1 GB limit)
- Accurate size accounting using ParquetMetaData's own memory_size method
- Thread-safe concurrent access

## Files to modify
- `common/Cargo.toml` - add foyer dependency
- `common/src/query_context.rs` - update QueryEnv struct and constructor
- `common/src/catalog/reader.rs` - update NozzleReaderFactory and NozzleReader
- Any code that creates QueryEnv instances