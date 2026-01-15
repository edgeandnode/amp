---
name: "udf-builtin-evm-log"
description: "evm_topic, evm_decode_log functions. Load when computing event signatures or decoding Ethereum event logs"
components: "crate:common"
---

# Event Log Decoding UDFs

## Summary

The event log UDFs work with Ethereum event logs emitted by smart contracts. Use `evm_topic` to calculate the topic0 hash for filtering events by type, and `evm_decode_log` to parse the indexed topics and data payload into structured event parameters. These are essential for analyzing on-chain events like token transfers, swaps, and other contract interactions.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Usage](#usage)
3. [Implementation](#implementation)
4. [Limitations](#limitations)
5. [References](#references)

## Key Concepts

- **topic0**: Keccak256 hash of the event signature, identifies the event type
- **Indexed Parameters**: Up to 3 parameters marked `indexed` are stored in topics (topic1, topic2, topic3)
- **Data Payload**: Non-indexed parameters are ABI-encoded in the data field
- **Anonymous Events**: Events without topic0 (not supported by these UDFs)

## Usage

### evm_topic

Computes the keccak256 hash of the event signature (topic0).

```sql
-- Get topic0 for ERC20 Transfer event
SELECT evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')
-- Returns: 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef

-- Filter logs by event type
SELECT * FROM eth_rpc.logs
WHERE topic0 = evm_topic('Transfer(address indexed from, address indexed to, uint256 value)')

-- Get topic0 for Uniswap V3 Swap event
SELECT evm_topic('Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)')
-- Returns: 0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67
```

**Arguments:**
- `signature` (Utf8): Solidity event signature

**Returns:** `FixedSizeBinary(32)` the keccak256 hash of the canonical event signature

### evm_decode_log

Parses Solidity event logs using the event signature.

```sql
-- Decode ERC20 Transfer events
SELECT
    evm_decode_hex(address) as token,
    evm_decode_hex(decoded.from) as sender,
    evm_decode_hex(decoded.to) as recipient,
    decoded.value as amount
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

-- Decode Uniswap V2 Swap events
SELECT
    evm_decode_hex(address) as pair,
    decoded.amount0In,
    decoded.amount1In,
    decoded.amount0Out,
    decoded.amount1Out
FROM (
    SELECT
        address,
        evm_decode_log(
            topic1, topic2, topic3, data,
            'Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)'
        ) as decoded
    FROM eth_rpc.logs
    WHERE topic0 = evm_topic('Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)')
)
```

**Arguments:**
- `topic1` (FixedSizeBinary(32)): First indexed topic (after topic0)
- `topic2` (FixedSizeBinary(32)): Second indexed topic
- `topic3` (FixedSizeBinary(32)): Third indexed topic
- `data` (Binary): Non-indexed event data
- `signature` (Utf8): Solidity event signature

**Returns:** A struct containing decoded event parameters with field names from the signature

## Implementation

### Source Files

- `crates/core/common/src/evm/udfs/evm_topic.rs` - Computes topic0 hash from event signature
- `crates/core/common/src/evm/udfs/evm_decode_log.rs` - Decodes event log data using signature

## Limitations

- **Indexed reference types**: Indexed string, bytes, arrays, and tuples are stored as keccak256 hashes, not actual values
- Signature must be a string literal
- Anonymous events (no topic0) are not supported
- Exactly 5 arguments required

## References

- [udf-builtin](udf-builtin.md) - Base: Built-in UDF overview
