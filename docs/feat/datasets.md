---
name: "datasets"
description: "Dataset system overview, dataset kinds, manifests, tables, and schemas. Load when asking about datasets, dataset kinds, or the dataset architecture"
type: meta
status: stable
components: "crate:datasets-common,crate:datasets-raw,crate:datasets-derived,crate:amp-datasets-static"
---

# Datasets

## Summary

Datasets are the core data abstraction in Amp, describing the shape and structure of data through explicit Arrow schemas organized into named tables. Each dataset is defined by a JSON manifest that specifies its kind, tables, and schemas. The dataset system supports multiple kinds — raw blockchain data, SQL-derived transformations, and static file-backed data — unified under a common trait interface.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)

## Key Concepts

- **Dataset**: A named, versioned collection of tables with an explicit Arrow schema, identified by a content-addressable [hash reference](../glossary.md#hash-reference)
- **Manifest**: A JSON document that fully describes a dataset — its kind, tables, schemas, and kind-specific configuration
- **Kind**: The dataset type that determines the schema structure and data source expectations (`evm-rpc`, `firehose`, `solana`, `manifest`, `static`)
- **Table**: A named set of rows with a fixed Arrow schema. Raw tables include a reserved `_block_num` column for block ordering
- **Block Number**: A `u64` value (`BlockNum`) representing the chain height at which data was produced. Raw and derived datasets are block-indexed; static datasets are not

## Architecture

A dataset manifest defines the schema contract that the rest of the system relies on:

```
Manifest (JSON) → Dataset Kind → Tables + Arrow Schemas → Query Engine
```

### Data Flow

1. A JSON manifest is registered, validated, and content-hashed
2. The `kind` field determines which dataset implementation handles it
3. The dataset exposes its tables and Arrow schemas through the `Dataset` trait
4. The system uses the dataset's kind and network to resolve providers and populate tables
5. Tables are queryable via SQL once populated

