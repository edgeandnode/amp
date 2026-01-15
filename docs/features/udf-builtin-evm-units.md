---
name: "udf-builtin-evm-units"
description: "shift_units function. Load when converting token amounts between wei and human-readable decimals"
components: "crate:common"
---

# Unit Conversion UDF

## Summary

The `shift_units` UDF shifts the decimal point of numeric values by a specified number of places. This is essential for converting between human-readable token amounts (e.g., "1.5 ETH") and raw blockchain values (e.g., "1500000000000000000 wei"). Positive shifts multiply by powers of 10, negative shifts divide.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Usage](#usage)
4. [Implementation](#implementation)
5. [Limitations](#limitations)
6. [References](#references)

## Key Concepts

- **Decimals**: ERC20 tokens define a `decimals` value (ETH=18, USDC=6, WBTC=8)
- **Raw Value**: On-chain amounts stored as integers without decimal points
- **Human Value**: Readable amounts with decimal points (e.g., "1.5")
- **Shift Direction**: Positive = multiply (human→raw), Negative = divide (raw→human)

## Architecture

The `shift_units` function is implemented as a DataFusion UDF that handles decimal point manipulation for numeric values. It supports multiple input types and preserves precision by returning string results.

## Usage

### shift_units

Shifts the decimal point by specified number of places.

```sql
-- Convert 1.5 ETH to wei (shift right by 18 places)
SELECT shift_units('1.5', 18)
-- Returns: "1500000000000000000"

-- Convert wei back to ETH (shift left by 18 places)
SELECT shift_units('1500000000000000000', -18)
-- Returns: "1.5"

-- Convert 100 USDC to raw units (6 decimals)
SELECT shift_units('100', 6)
-- Returns: "100000000"

-- Convert raw USDC back to human readable
SELECT shift_units('100000000', -6)
-- Returns: "100"
```

**Arguments:**
- `value`: Decimal/Integer string, or numeric type (Int8-64, UInt8-64, Float32/64, Decimal128/256, Utf8)
- `units`: Number of decimal places to shift (positive = right/multiply, negative = left/divide)

**Returns:** `Utf8` string representation of result without trailing zeros

### Common Token Decimals

| Token | Decimals | Raw → Human | Human → Raw |
|-------|----------|-------------|-------------|
| ETH/WETH | 18 | `shift_units(value, -18)` | `shift_units(value, 18)` |
| USDC/USDT | 6 | `shift_units(value, -6)` | `shift_units(value, 6)` |
| WBTC | 8 | `shift_units(value, -8)` | `shift_units(value, 8)` |
| DAI | 18 | `shift_units(value, -18)` | `shift_units(value, 18)` |

### Practical Examples

```sql
-- Format ETH transfer amounts
SELECT
    evm_decode_hex(tx_hash) as hash,
    shift_units(value, -18) as eth_amount
FROM eth_rpc.transactions
WHERE value > 0

-- Calculate USD value of token transfer
SELECT
    shift_units(amount, -decimals) as human_amount,
    CAST(shift_units(amount, -decimals) AS DOUBLE) * price as usd_value
FROM token_transfers t
JOIN token_prices p ON t.token = p.token
```

## Implementation

### Source Files

- `crates/core/common/src/evm/udfs/shift_units.rs` - Core UDF implementation with decimal shifting logic

## Limitations

- Maximum absolute units value is 32
- NULL input returns NULL output
- Value must be a valid decimal/integer string or numeric type
- Result is always returned as string to preserve precision

## References

- [udf-builtin](udf-builtin.md) - Base: Built-in UDF overview
