# Glossary

A WIP glossary to standardize terminology around Nozzle. Divided between logical and physical concepts.

## Logical

### Schema
A list of fields. A field is a triple `(name, type, nullable)`.  The `type` is an Arrow data type ([spec](https://arrow.apache.org/docs/format/Columnar.html#data-types)).

### Query
A SQL query string or a DataFusion logical plan ([spec](https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html)). The query output conforms to a statically-known schema.

### View
A named query that is part of a dataset. Can be referred to in queries, as in `select * from dataset.view`.

### Dataset
A user-defined set of views. A Dataset is a unit of ownership, publishing and versioning.

## Physical

Nozzle currently adopts the FDAP stack for its physical layer, see https://www.influxdata.com/glossary/fdap-stack/.

### DataFusion
The query planner and execution engine used by Nozzle, see https://datafusion.apache.org.

### Arrow record batch
Arrow is an in-memory and over-the-wire data format. Query results are returned by DataFusion as a stream of Arrow record batches. See https://arrow.apache.org/docs/index.html.

### Parquet
The file format in which record batches are persisted, for example to materialize query results. See https://parquet.apache.org.

### Arrow Flight
The RPC protocol Nozzle uses for queries, with results returned as Arrow record batches over gRPC, see https://arrow.apache.org/docs/format/Flight.html.

