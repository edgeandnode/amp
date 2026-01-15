---
name: "udf"
description: "SQL UDFs for blockchain data transformation. Load when asking about custom functions or extending SQL queries"
components: "crate:common"
---

# User-Defined Functions

## Summary

Project Amp provides user-defined functions (UDFs) that extend SQL queries with custom data transformation capabilities. UDFs are registered with DataFusion's session context and can be used in SELECT, WHERE, and other SQL clauses to decode, encode, and transform blockchain data.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [UDF Categories](#udf-categories)
4. [Usage](#usage)
5. [Implementation](#implementation)

## Key Concepts

- **Scalar UDF**: A function that operates on individual values and returns a single value per row
- **DataFusion**: The SQL query engine that executes UDFs during query processing
- **Type Coercion**: Automatic conversion between SQL types and UDF input/output types

## Architecture

### UDF Registration

UDFs are registered with DataFusion's session context during query engine initialization. Each UDF implements the `ScalarUDFImpl` trait, which defines:
- Function name and signature
- Input type validation
- Execution logic
- Return type inference

## UDF Categories

Project Amp provides two categories of UDFs:

- **Built-in UDFs**: Pre-packaged functions maintained by Project Amp, optimized for common blockchain data operations. These are always available and require no additional setup.
- **Custom UDFs**: User-defined functions written in JavaScript and registered at runtime. These extend query capabilities with custom transformation logic specific to your use case.

### Built-in UDFs

Built-in UDFs are provided by Project Amp for common blockchain data operations. Categories include:

- Hex encoding/decoding for addresses and hashes
- ABI type encoding/decoding for Solidity types
- Function parameter encoding/decoding for contract calls
- Event log decoding for on-chain events
- Read-only contract calls via JSON-RPC
- Unit conversion for token amounts

Each category is documented in its own feature doc with detailed function signatures and examples.

### Custom UDFs

Custom UDFs allow users to define their own transformation logic when built-in functions don't cover your specific needs:

- **JavaScript UDFs**: Write custom functions in JavaScript for domain-specific data transformations

## Usage

UDFs are called like standard SQL functions within queries:

```sql
-- Use built-in UDFs to decode ERC20 transfer events
SELECT
    evm_decode_hex(address) as token,
    shift_units(decoded.value, -18) as amount
FROM (
    SELECT
        address,
        evm_decode_log(
            topic1, topic2, topic3, data,
            'Transfer(address indexed from, address indexed to, uint256 value)'
        ) as decoded
    FROM eth_rpc.logs
    WHERE topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')
)
```

## Implementation

### Source Files

- `crates/core/common/src/evm/udfs/` - Core UDF infrastructure
