# Amp Documentation

Welcome to the Amp documentation. Amp is a high-performance ETL (Extract, Transform, Load) system for blockchain data that extracts data from various blockchain sources, transforms it via SQL queries, and serves it through multiple query interfaces.

## Quick Start

New to Amp? Start here:

1. **[Operational Modes](modes.md)** - Understand the different ways to run Amp (serverless, single-node, distributed)
2. **[Configuration Guide](config.md)** - Learn how to configure Amp for your deployment
3. **[Upgrading Guide](upgrading.md)** - Upgrade Amp between versions

## Documentation Index

### Core Concepts

#### [Operational Modes](modes.md)

Complete guide to Amp's operational modes and deployment patterns:

- **Distributed Mode**: Separate server and worker components for production
- **Local Development Mode**: Combined server + worker for local testing
- Deployment patterns and architecture diagrams

#### [Configuration Guide](config.md)

Detailed configuration reference for all Amp components and settings.

#### [Upgrading Guide](upgrading.md)

Process for upgrading Amp between versions:

- Database migration workflows
- Upgrade procedures for different deployment types
- Troubleshooting and best practices

### Data Sources & Schemas

#### [Dataset Definition Schemas](dataset-def-schemas/README.md)

JSON schemas for defining datasets:

- Common dataset fields
- EVM RPC datasets
- Firehose datasets
- SQL/manifest datasets (derived datasets)

#### Dataset Schema Documentation

- **[EVM RPC Schema](schemas/evm-rpc.md)** - Schema for Ethereum-compatible JSON-RPC data sources
- **[Firehose EVM Schema](schemas/firehose-evm.md)** - Schema for StreamingFast Firehose protocol data

#### [User-Defined Functions (UDFs)](udfs.md)

Custom SQL functions available in Amp queries:

- EVM decoding functions
- RPC call functions
- Attestation functions
- JavaScript UDFs

### Operations

#### [Telemetry](telemetry.md)

Observability and monitoring:

- OpenTelemetry integration
- Metrics and tracing
- Logging configuration

#### [Reorgs](reorgs.md)

Understanding and handling blockchain reorganizations:

- How Amp detects and handles reorgs
- Impact on data consistency
- Best practices

### Reference

#### [Glossary](glossary.md)

Definitions of key terms and concepts used throughout Amp.

## See Also

- **[Project README](../README.md)** - Project overview and development setup
- **[AGENTS.md](../AGENTS.md)** - Technical overview for AI assistants and contributors
