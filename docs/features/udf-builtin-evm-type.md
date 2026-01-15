---
name: "udf-builtin-evm-type"
description: "evm_encode_type, evm_decode_type functions. Load when ABI encoding/decoding Solidity types like uint256, address, bool"
components: "crate:common"
---

# ABI Type Encoding/Decoding UDFs

## Summary

The ABI type UDFs encode and decode values using Solidity type specifications. Use `evm_encode_type` to convert values to ABI-encoded binary format, and `evm_decode_type` to parse ABI-encoded binary data back into typed values. These are useful for working with raw contract return data or preparing data for contract interactions.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [Implementation](#implementation)
4. [Limitations](#limitations)
5. [References](#references)

## Key Concepts

- **ABI Encoding**: Ethereum's standard binary encoding format for typed data
- **Solidity Types**: Supported types include uint8-256, int8-256, address, bool, bytes, bytes1-32, string, and tuples
- **Type Literal**: The type argument must be a string literal, not a column reference

## Usage

### evm_encode_type

ABI-encodes a value according to a Solidity type.

```sql
-- Encode a uint256 value
SELECT evm_encode_type(12345, 'uint256')

-- Encode an address from a column
SELECT evm_encode_type(address, 'address')
FROM eth_rpc.logs

-- Encode a boolean
SELECT evm_encode_type(true, 'bool')
```

**Arguments:**
- `value`: The value to encode (type must be compatible with the Solidity type)
- `type` (Utf8): Solidity type string (e.g., "uint256", "address", "bool")

**Returns:** `Binary` ABI-encoded representation

### evm_decode_type

Decodes raw ABI-encoded binary data into a Solidity type.

```sql
-- Decode a uint256 value from contract return data
SELECT evm_decode_type(return_data, 'uint256') as balance

-- Decode an address
SELECT evm_decode_type(binary_data, 'address') as owner

-- Decode a tuple of values
SELECT evm_decode_type(binary_data, '(uint256,address,bool)') as result

-- Decode nested data types (tuples with arrays)
SELECT evm_decode_type(encoded, '(int256, int8, string[])') AS decoded
FROM (
    SELECT evm_encode_type(
        struct(1, 2, ['str1', 'str2', 'str3']),
        '(int256, int8, string[])'
    ) AS encoded
)
-- Returns: { "c0": "1", "c1": 2, "c2": ["str1", "str2", "str3"] }
```

**Arguments:**
- `data` (Binary): ABI-encoded binary data to decode
- `type` (Utf8): Solidity type string

**Returns:** Decoded value with Arrow type corresponding to the Solidity type. NULL if decoding fails.

### Type Mapping

| Solidity Type | Arrow Type |
|---------------|------------|
| uint8-uint64 | UInt8-UInt64 |
| uint65-uint128 | Decimal128 |
| uint129-uint256 | Decimal256 or Utf8* |
| int8-int64 | Int8-Int64 |
| int65-int128 | Decimal128 |
| int129-int256 | Decimal256 or Utf8* |
| address | FixedSizeBinary(20) |
| bool | Boolean |
| bytes | Binary |
| bytes1-bytes32 | FixedSizeBinary(N) |
| string | Utf8 |
| tuple | Struct |

*Values exceeding 251 bits use Utf8 string representation to preserve full precision.

## Implementation

### Source Files

- `crates/core/common/src/evm/udfs/evm_encode_type.rs` - ABI-encodes values to binary
- `crates/core/common/src/evm/udfs/evm_decode_type.rs` - Decodes ABI binary to typed values

## Limitations

- Type argument must be a string literal, not a column reference
- Exactly 2 arguments required
- Value must be convertible to the specified Solidity type
- Decoding returns NULL on failure rather than error

## References

- [udf-builtin](udf-builtin.md) - Base: Built-in UDF overview
- [udf-builtin-evm-params](udf-builtin-evm-params.md) - Related: Function parameter encoding
