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
- Accessible via SQL [queries](#query) as `dataset.table_name`
- Can be queried through [Arrow Flight](#arrow-flight) or HTTP JSON APIs

Tables contain the actual materialized data that users query, whether extracted directly from blockchain sources or
computed from SQL transformations.

### Dataset
A collection of [tables](#table) that represents a unit of ownership, publishing and versioning.
Datasets define how data is extracted, transformed, and materialized into [Parquet](#parquet) files for querying.

### Dataset Manifest
A structured definition file that specifies a [dataset's](#dataset) configuration, including its [kind](#dataset-kind),
data sources, transformations, [schema](#schema), and dependencies.
Acts as the blueprint for how Amp should process and materialize the dataset.

### Dataset Kind
The implementation type that determines how a [dataset](#dataset) processes data:
- **derived**: Transforms and combines data from other datasets using SQL [queries](#query)
- **evm-rpc**: Extracts blockchain data via Ethereum-compatible JSON-RPC endpoints
- **firehose**: Streams real-time blockchain data through StreamingFast Firehose protocol
- **substreams**: Processes data from Substreams packages with dynamic [schema](#schema) inference

### Dataset Category
A high-level classification grouping [datasets](#dataset) by their data processing approach:
- **Raw** (a.k.a. **Extractor Datasets**): Extracts data directly from external blockchain sources (includes _evm-rpc_, _firehose_, and _substreams_ [kinds](#dataset-kind))
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

