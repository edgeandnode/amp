---
name: "udf-builtin-evm-hex"
description: "evm_encode_hex, evm_decode_hex functions. Load when converting addresses or hashes between hex strings and binary"
type: feature
components: "crate:common"
---

# Hex Encoding/Decoding UDFs

## Summary

The hex encoding UDFs convert between human-readable hex strings and binary representations used internally for Ethereum addresses (20 bytes) and hashes (32 bytes). Use `evm_encode_hex` to convert hex strings to binary for filtering and joins, and `evm_decode_hex` to convert binary back to readable hex strings for output.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [Implementation](#implementation)
4. [Limitations](#limitations)
5. [References](#references)

## Key Concepts

- **FixedSizeBinary(20)**: Arrow data type for Ethereum addresses (20 bytes)
- **FixedSizeBinary(32)**: Arrow data type for transaction hashes, block hashes, and topic values (32 bytes)
- **0x Prefix**: Optional on input, always present on output

## Usage

### evm_encode_hex

Converts hex strings to `FixedSizeBinary(20)` or `FixedSizeBinary(32)`.

```sql
-- Encode a 20-byte address
SELECT * FROM eth_rpc.logs
WHERE address = evm_encode_hex('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')

-- Encode a 32-byte hash (without 0x prefix also works)
SELECT * FROM eth_rpc.transactions
WHERE hash = evm_encode_hex('cb8f66bff7ba069c3938626df640d294fd9e2700671e4e567bcea4a6df2cb9ff')
```

**Arguments:**
- `hex_string` (Utf8): Hex string (40 or 64 characters), with or without `0x` prefix

**Returns:** `FixedSizeBinary(20)` for 40-char input, `FixedSizeBinary(32)` for 64-char input

### evm_decode_hex

Converts `FixedSizeBinary(20)` or `FixedSizeBinary(32)` to hex strings with `0x` prefix.

```sql
-- Decode address column to readable format
SELECT evm_decode_hex(address) as contract_address
FROM eth_rpc.logs
LIMIT 10

-- Decode transaction hash
SELECT evm_decode_hex(hash) as tx_hash
FROM eth_rpc.transactions
LIMIT 10
```

**Arguments:**
- `binary` (FixedSizeBinary(20) or FixedSizeBinary(32)): Binary data to decode

**Returns:** `Utf8` hex string with `0x` prefix, lowercase

## Implementation

### Source Files

- `crates/core/common/src/evm/udfs/evm_encode_hex.rs` - Converts hex strings to binary
- `crates/core/common/src/evm/udfs/evm_decode_hex.rs` - Converts binary to hex strings

## Limitations

- Input must decode to exactly 20 or 32 bytes
- Invalid hex characters will cause an error
- NULL input to `evm_decode_hex` returns NULL output
- NULL input to `evm_encode_hex` causes an error

## References

- [udf-builtin](udf-builtin.md) - Base: Built-in UDF overview
