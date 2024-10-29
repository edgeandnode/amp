# Glossary

A WIP glossary to standardize terminology around Nozzle.

## Logical layer

### Schema
A list of fields. A field is a triple `(name, type, nullable)`.  The `type` is an Arrow data type ([spec](https://arrow.apache.org/docs/format/Columnar.html#data-types)).

### View
A view is defined by a name, a schema and a query. The schema is the output schema of the query. The query is a DataFusion [logical plan](https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html), often derived from a SQL query string. The query may refer to other views.

### Dataset

A user-defined set of views. A Dataset is a unit of ownership, publishing and versioning.
