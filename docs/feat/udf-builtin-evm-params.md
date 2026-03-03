---
name: "udf-builtin-evm-params"
description: "evm_encode_params, evm_decode_params functions. Load when encoding or decoding contract function calldata"
type: feature
components: "crate:common"
---

# Function Parameters Encoding/Decoding UDFs

## Summary

The function parameters UDFs encode and decode complete function call data including the 4-byte function selector. Use `evm_encode_params` to build calldata for contract interactions, and `evm_decode_params` to parse transaction input data into structured parameters. These are essential for analyzing contract calls in transaction data.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [Limitations](#limitations)
4. [Implementation](#implementation)
5. [References](#references)

## Key Concepts

- **Function Selector**: First 4 bytes of keccak256 hash of function signature, identifies which function is called
- **Named Parameters**: Function signature must include parameter names for decoding to produce named struct fields
- **Calldata**: Complete function call data = 4-byte selector + ABI-encoded parameters

## Usage

### evm_encode_params

Creates ABI-encoded function call data including the 4-byte selector.

```sql
-- Encode a transfer call
SELECT evm_encode_params(
    evm_encode_hex('0xRecipientAddress...'),
    1000000000000000000,
    'transfer(address to, uint256 amount)'
)

-- Encode an approve call with max uint256
SELECT evm_encode_params(
    spender_address,
    115792089237316195423570985008687907853269984665640564039457584007913129639935,
    'approve(address spender, uint256 amount)'
)
```

**Arguments:**
- `...values`: Values to encode (one per function parameter, in order)
- `signature` (Utf8): Solidity function signature with named parameters (must be last argument)

**Returns:** `Binary` complete function call data (4-byte selector + ABI-encoded params)

### evm_decode_params

Parses function call input data using the function signature.

```sql
-- Decode ERC20 transfer calls
SELECT
    evm_decode_hex(t.to_address) as token,
    (evm_decode_params(t.input, 'transfer(address to, uint256 amount)')).to as recipient,
    (evm_decode_params(t.input, 'transfer(address to, uint256 amount)')).amount as amount
FROM eth_rpc.transactions t
WHERE evm_decode_hex(t.input[1:4]) = '0xa9059cbb'  -- transfer selector

-- Decode Uniswap swap calls
SELECT
    params.amount0Out,
    params.amount1Out,
    evm_decode_hex(params.to) as recipient
FROM (
    SELECT evm_decode_params(
        input,
        'swap(uint256 amount0Out, uint256 amount1Out, address to, bytes data)'
    ) as params
    FROM eth_rpc.transactions
) t
```

**Arguments:**
- `data` (Binary): Function call input data (including 4-byte selector)
- `signature` (Utf8): Solidity function signature with named parameters

**Returns:** A struct with fields named after function parameters. Returns struct with NULL fields if selector doesn't match or decoding fails.

## Limitations

- Function signature must include parameter names (e.g., `transfer(address to, uint256 amount)` not `transfer(address,uint256)`)
- Signature must be a string literal, not a column reference
- Selector mismatch returns NULL fields rather than error
- Exactly 2 arguments for decode, N+1 arguments for encode (values + signature)

## Implementation

### Source Files

- `crates/core/common/src/evm/udfs/evm_function_params.rs` - Function params encode/decode implementation

## References

- [udf-builtin](udf-builtin.md) - Base: Built-in UDF overview
- [udf-builtin-evm-type](udf-builtin-evm-type.md) - Related: Raw ABI type encoding
