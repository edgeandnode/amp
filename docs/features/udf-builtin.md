---
name: "udf-builtin"
description: "Built-in EVM SQL functions for decoding blockchain data. Load when asking about address encoding, log decoding, or contract calls in SQL"
type: meta
components: "crate:common"
---

# Built-in User-Defined Functions

## Summary

Project Amp provides built-in user-defined functions (UDFs) for SQL queries against blockchain data. These UDFs enable decoding and encoding of Ethereum addresses, transaction data, event logs, and function calls directly within SQL queries executed by DataFusion.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [Implementation](#implementation)
4. [References](#references)

## Key Concepts

- **Built-in UDFs**: Pre-packaged functions maintained by Project Amp, always available without additional setup
- **Custom UDFs**: User-defined functions for domain-specific transformations (documented separately)

## Usage

Built-in UDFs work together to decode and transform blockchain data. A typical workflow combines multiple UDFs:

```sql
-- Decode ERC20 Transfer events combining multiple built-in UDFs
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

This example uses:
- `evm_topic` to compute the event signature hash for filtering
- `evm_decode_log` to parse event parameters from topics and data
- `evm_decode_hex` to convert binary addresses to readable hex strings
- `shift_units` to convert raw token amounts to human-readable values

## Implementation

### Source Files

- `crates/core/common/src/evm/udfs/` - UDF implementations for EVM data transformations
- `crates/core/common/src/evm/udfs.rs` - UDF registration with DataFusion

## References

- [udf](udf.md) - Base: UDF overview
