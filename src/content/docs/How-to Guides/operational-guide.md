---
title: Operational Mode
description: Task-oriented guide showing how to deploy amp
---

# How-To Guide: Operating Amp

This guide provides step-by-step instructions for operating Amp in different modes and deployment scenarios.

## How to Extract Data Using Serverless Mode

### Extract a Single Dataset

To extract blockchain data from a single dataset:

```bash
ampd dump --dataset eth_mainnet
```

This command will:

1. Load the eth_mainnet dataset definition
2. Check the metadata database for previous extraction progress
3. Extract data starting from the last extracted block (or genesis if fresh)
4. Write Parquet files to the configured data directory
5. Update metadata with extraction progress
6. Exit when complete

### Extract Data with Parallel Processing

To speed up extraction using multiple parallel jobs:

```bash
ampd dump --dataset eth_mainnet --n-jobs 4
```

The `--n-jobs` parameter configures the number of parallel worker jobs for faster extraction.

### Extract Data to a Specific Block

To extract data up to a specific block number:

```bash
ampd dump --dataset eth_mainnet --end-block 4000000 --n-jobs 4
```

This extracts data from the current position up to block 4,000,000 and then exits.

### Extract Multiple Datasets Simultaneously

To extract multiple datasets in a single operation:

```bash
ampd dump --dataset eth_mainnet,uniswap_v3
```

Use comma-separated dataset names to extract multiple datasets. Amp will automatically resolve dependencies.

### Extract Datasets from a Manifest File

To extract datasets defined in a manifest file:

```bash
ampd dump --dataset ./datasets/production.json
```

The manifest file should contain the dataset definitions in JSON format.

### Start Fresh Extraction (Discard Progress)

To discard existing progress and start extraction from the beginning:

```bash
ampd dump --dataset eth_mainnet --fresh
```

The `--fresh` flag discards any existing extraction progress for the dataset.

### Schedule Periodic Extraction

To run extraction periodically (e.g., every 30 minutes):

```bash
ampd dump --dataset eth_mainnet --run-every-mins 30
```

This keeps the process running and re-executes extraction every 30 minutes.

---

## How to Deploy in Single-Node Mode

### Run Development Mode

To run Amp with combined controller, server, and embedded worker for local development:

```bash
ampd dev
```

This command starts:

- Controller service for job management
- Query server for data access
- Embedded worker for extraction jobs

### Configure Development Mode

Before running development mode, ensure your configuration includes:

- Database connection settings
- Data directory path
- Dataset definitions
- Source endpoints (RPC, Firehose, etc.)

---

## How to Deploy in Distributed Mode

### Start the Controller

To run the controller service for job management:

```bash
ampd controller
```

The controller provides the Admin API for managing extraction jobs.

### Start the Query Server

To run the query server without extraction capabilities:

```bash
ampd server
```

The server provides:

- Arrow Flight interface for data queries
- JSON Lines interface for streaming queries
- No extraction or job management capabilities

### Start Worker Processes

To run standalone worker processes for extraction:

```bash
ampd worker
```

Workers:

- Execute scheduled extraction jobs
- Coordinate via the shared metadata database
- Can be scaled horizontally by running multiple instances

### Deploy a Complete Distributed System

1. **Set up the metadata database**:

   ```bash
   ampd migrate
   ```

2. **Start the controller** (on management node):

   ```bash
   ampd controller
   ```

3. **Start the query server** (on query node):

   ```bash
   ampd server
   ```

4. **Start worker processes** (on worker nodes):

   ```bash
   # Worker 1
   ampd worker

   # Worker 2 (on different node)
   ampd worker

   # Add more workers as needed
   ```

---

## How to Choose the Right Deployment Pattern

### Use Serverless Mode When You Need:

- One-off data extraction for specific analysis
- Initial dataset population with historical data
- Testing and verifying dataset configurations
- Scheduled batch jobs via cron or schedulers
- CI/CD pipeline integration

### Use Single-Node Mode When You Need:

- Local development and testing
- Simple deployments without scaling requirements
- Combined functionality in a single process

### Use Distributed Mode When You Need:

- Production deployments with high availability
- Horizontal scaling of extraction workers
- Separation of query serving from extraction
- Resource isolation between components
- Multi-node deployment for fault tolerance

---

## How to Manage Extraction Jobs

### Resume Interrupted Extraction

Amp automatically resumes from the last extracted block if interrupted. Simply re-run the same command:

```bash
ampd dump --dataset eth_mainnet
```

### Track Extraction Progress

Extraction progress is stored in the metadata database. The system tracks:

- Last successfully extracted block
- File metadata for written Parquet files
- Dataset dependency relationships

### Handle Dependencies

When extracting SQL datasets that depend on other datasets, Amp automatically:

1. Identifies required upstream datasets
2. Extracts upstream datasets first
3. Processes SQL transformations after dependencies are met

---

## How to Configure Parallel Extraction

### Set the Number of Jobs

Use the `-j` or `--n-jobs` parameter to configure parallelism:

```bash
# Use 8 parallel jobs for faster extraction
ampd dump --dataset eth_mainnet -j 8
```

### Optimize Job Count

Consider these factors when setting job count:

- Available CPU cores
- Network bandwidth to data sources
- Memory availability
- Rate limits on source endpoints

### Monitor Resource Usage

When running parallel extraction:

1. Monitor CPU utilization
2. Check memory consumption
3. Verify network throughput
4. Adjust job count based on bottlenecks

---

## Common Troubleshooting

### Extraction Stops Unexpectedly

If extraction stops before the end block:

1. Check the metadata database for the last extracted block
2. Verify source endpoint connectivity
3. Review logs for error messages
4. Re-run the command to resume from last position

### Performance Issues

To improve extraction performance:

1. Increase the number of parallel jobs (`--n-jobs`)
2. Verify network connectivity to source endpoints
3. Ensure sufficient disk I/O for Parquet file writing
4. Check database performance for metadata operations

### Data Consistency

To ensure data consistency:

1. Use the `--fresh` flag to restart extraction if data issues are suspected
2. Verify source endpoint reliability
3. Check for blockchain reorganizations in the logs
4. Ensure metadata database is properly synchronized
