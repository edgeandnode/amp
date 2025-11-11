# Glossary

A glossary defining key concepts and terminology used throughout the Amp project. Organized by logical and physical architecture layers.

## Logical

### Field

A column definition consisting of a triple `(name, type, nullable)`, where the `type` is an Arrow data type ([spec](https://arrow.apache.org/docs/format/Columnar.html#data-types)).

### Schema

A list of [fields](#field) that defines the structure of data in a table or query result.

### Query

A SQL query string or a [DataFusion](#datafusion) logical
plan ([spec](https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html)).
The query output conforms to a statically-known [schema](#schema).

### View

A named [query](#query) that is part of a [dataset](#dataset). Can be referred to in queries, as in `select * from dataset.view`.

### Table

A named collection of data with a fixed [schema](#schema) that can be queried using SQL.
Tables are the primary interface for accessing data within a [dataset](#dataset).

**Key characteristics:**

- Has a defined [schema](#schema) (list of [fields](#field) with names, types, and nullability)
- Physically stored as [Parquet](#parquet) files, typically partitioned by block ranges for blockchain data
- Accessible via SQL [queries](#query) as `"namespace/dataset_name".table_name` (quoted due to forward slash)
- Can be queried through [Arrow Flight](#arrow-flight) or HTTP JSON APIs

Tables contain the actual materialized data that users query, whether extracted directly from blockchain sources or
computed from SQL transformations.

### Dataset

A collection of [tables](#table) that represents a unit of ownership, publishing and versioning.
Datasets are identified by a namespace, name, and version/revision, and define how data is extracted,
transformed, and materialized into [Parquet](#parquet) files for querying.

### Dataset Namespace

An organizational grouping for datasets that provides logical separation and multi-tenancy support.
Namespaces help organize datasets by team, project, or organization (e.g., `my_org`, `edgeandnode`).
The default namespace is represented by `_`.

### Dataset Version

A specific revision of a dataset identified by either:

- **Semantic version**: Following semver format (e.g., `1.0.0`, `2.1.3`)
- **Special tags**: System-managed tags like `latest` (highest semantic version) or `dev` (most recent registration)
- **Manifest hash**: Direct reference to a specific manifest by its content hash

Datasets are referenced using the format: `namespace/name@revision` (e.g., `my_org/eth_mainnet@1.0.0`).

### Dataset Manifest

A structured definition file that specifies a [dataset's](#dataset) configuration, including its [kind](#dataset-kind),
data sources, transformations, [schema](#schema), and dependencies.
Acts as the blueprint for how Amp should process and materialize the dataset.

### Dataset Kind

The implementation type that determines how a [dataset](#dataset) processes data:

- **derived**: Transforms and combines data from other datasets using SQL [queries](#query)
- **evm-rpc**: Extracts blockchain data via Ethereum-compatible JSON-RPC endpoints
- **firehose**: Streams real-time blockchain data through StreamingFast Firehose protocol

### Dataset Category

A high-level classification grouping [datasets](#dataset) by their data processing approach:

- **Raw** (a.k.a. **Extractor Datasets**): Extracts data directly from external blockchain sources (includes _evm-rpc_ and _firehose_ [kinds](#dataset-kind))
- **Derived**: Transforms and combines data from existing datasets (_derived_ [kind](#dataset-kind))

## Physical

Amp currently adopts the FDAP stack for its physical layer, see https://www.influxdata.com/glossary/fdap-stack/.

### DataFusion

The query planner and execution engine used by Amp, see https://datafusion.apache.org.

### Arrow record batch

Arrow is an in-memory and over-the-wire data format. Query results are returned by DataFusion as a stream of Arrow record batches. See https://arrow.apache.org/docs/index.html.

### Parquet

The file format in which record batches are persisted, for example to materialize query results. See https://parquet.apache.org.

### Arrow Flight

The RPC protocol Amp uses for queries, with results returned as Arrow record batches over gRPC, see https://arrow.apache.org/docs/format/Flight.html.

## Architecture Components

### Amp Engine

The complete distributed system comprising all software components that run in a cluster: the [controller](#controller), [workers](#worker), [query server](#amp-server), and [metadata database](#metadata-database). The Amp engine provides the full data extraction, transformation, and query serving capabilities.

### Amp Cluster

Synonym for [Amp engine](#amp-engine), typically used when referring to deployments on cloud infrastructure. Emphasizes the distributed, multi-node nature of the system.

### Amp Server

The query server component of the Amp data plane that serves queries over the [Arrow Flight](#arrow-flight) protocol. Also referred to as the "Arrow Flight server" or "query server". Started via the `ampd server` command.

### Amp Daemon

A continuously running background process, following the Unix daemon concept. Refers to any of the `ampd` service processes: controller daemon, worker daemons, or query server daemon. The term emphasizes the long-running, background nature of these services.

### Controller

The component responsible for job scheduling, worker coordination, and exposing the [engine administration interface](#engine-administration-interface). Started via the `ampd controller` command.

### Worker

A process that executes data extraction jobs scheduled by the [controller](#controller). Multiple workers can run in parallel to scale extraction throughput. Started via the `ampd worker` command.

### Engine Administration Interface

The administrative API exposed by the [controller](#controller) for managing datasets, jobs, workers, providers, and storage. Accessed by the `ampctl` and `amp` CLIs. Also referred to as the "Admin API" in some contexts.

### Metadata Database

A PostgreSQL database that stores metadata about datasets, jobs, workers, files, and extraction progress. Used by the [controller](#controller) for state management and coordination across distributed components.
