---
title: Concepts
description: Learn a technical overview of Amp
---

## Overview

This page offers an overview of Amp, covering its core concepts, system architecture, and infrastructure design. It acts as a reference for developers and data teams seeking to understand how Amp processes, organizes, and delivers blockchain data for indexing, analytics, and enterprise use.

### Amp Foundation

Amp a high-performance solution built for data-heavy teams that need full control of their blockchain data.

Traditional indexing follows Extract, Transform, and Load (ETL) principles, while Amp’s architecture supports extraction and transformation directly in SQL, creating a consistent and simple workflow.

Its ELT (Extract, Load, Transform) architecture enables faster data access and flexible analytics, while verifying data correctness at the time of extraction. As a complete data warehouse, Amp manages the entire blockchain data lifecycle to meet modern enterprise requirements.

### Technical Lifecycle

| Stage         | Description                                                                                            |
| ------------- | ------------------------------------------------------------------------------------------------------ |
| **Extract**   | Pulls data directly from Ethereum JSON-RPC nodes                                                       |
| **Transform** | Processes data using SQL queries and blockchain-specific functions                                     |
| **Store**     | Saves optimized columnar data in optimized Apache Parquet format                                       |
| **Serve**     | Provides query-ready data production-ready interfaces, including SQL, REST, GraphQL, Snowflake, and BI |

> Note: The familiar SQL syntax, removes the need for custom parsers or blockchain-specific code.
> Object storage can be either local or using cloud services such as AWS or GCS.

## How Amp Works

### 1. High Performance

- Parallel data extraction with configurable worker pools
- Columnar storage optimized for large-scale analytics
- Streamed query execution for massive datasets
- Apache Arrow integration for efficient data transfer

### 2. Powerful Query Engine

- Full SQL support using **Apache DataFusion**
- Custom blockchain-specific SQL functions (e.g., decode logs, call contracts)
- Handles complex analytical queries efficiently
- Supports both real-time and historical data access

### 3. Blockchain Integration

- **EVM RPC:** Direct connection to Ethereum-compatible nodes
- **Batch requests:** Optimized batching for extraction efficiency
- **Multiple endpoints:** Support for fallback RPC endpoints
- **Custom chains:** Compatible with any EVM-based blockchain

### 4. Developer Experience

- Client libraries for **Python** and **TypeScript**
- Interactive notebooks (Marimo, Jupyter)
- REST and gRPC APIs for flexible integration
- Built-in monitoring and observability via **Grafana**

### 5. Advanced Platform Capabilities

- **Lineage & auditability:** Tracks every step of data extraction and transformation for verification and compliance.
- **Purpose-built:** Designed for co-located or enterprise environments, including regulated industries.
- **Extensible multichain access:** Unified APIs and a shared data lake for querying across multiple blockchains.
- **Local-first development:** Ability to index and query events in seconds on any laptop.
- **Auto-modeling:** Automatically generates schemas and datasets from smart contracts
- **Dataset publishing & composability:** Datasets that are discoverable, shareable, and capable of being remixed.
- **Production-grade APIs:** Support for SQL (streaming and batch), REST, and GraphQL (batch).
