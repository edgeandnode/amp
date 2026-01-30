---
name: "udf-builtin-evm-eth-call"
description: "eth_call function for read-only contract calls. Load when querying contract state or calling view functions in SQL"
type: feature
components: "crate:common"
---

# Contract Call UDF

## Summary

The `eth_call` UDF executes read-only contract calls against an Ethereum JSON-RPC endpoint during query execution. This enables querying on-chain state like token balances, allowances, or any view/pure function directly within SQL queries. Results include both the return data and any error message if the call reverted.

## Table of Contents

1. [Key Concepts](#key-concepts)
2. [Architecture](#architecture)
3. [Usage](#usage)
4. [Implementation](#implementation)
5. [Limitations](#limitations)
6. [References](#references)

## Key Concepts

- **eth_call**: Ethereum JSON-RPC method that simulates a transaction without creating one on-chain
- **View Functions**: Contract functions that read state but don't modify it (marked `view` or `pure` in Solidity)
- **Block Tag**: Specifies which block state to query against ("latest", "pending", "earliest", or block number)
- **Revert**: When a call fails, the error message is returned instead of data

## Architecture

### Async Execution

`eth_call` is an async UDF that makes HTTP requests to the configured Ethereum RPC endpoint. It includes retry logic (3 retries) for transient errors.

## Usage

### Prerequisites

**Important:** A JSON-RPC provider must be configured for the target blockchain network before using `eth_call`.

The RPC endpoint is configured per-chain and determines which network state is queried. Ensure your provider:
- Supports the `eth_call` JSON-RPC method
- Has access to the block range you need (archive node for historical queries)
- Has sufficient rate limits for your query volume

Without a configured RPC provider, `eth_call` queries will fail.

### eth_call

Executes read-only contract calls via JSON-RPC.

**Important:** eth_call requires dataset qualification. Always use the format `"namespace/name@revision".eth_call(...)` where the dataset identifier specifies which blockchain network and provider configuration to use. For example: `"edgeandnode/mainnet@0.0.1".eth_call(...)`.

**Arguments:**
- `from` (FixedSizeBinary(20) or NULL): Sender address (optional, can pass NULL directly)
- `to` (FixedSizeBinary(20)): Target contract address (required)
- `input` (Binary or NULL): Encoded function call data (use `evm_encode_params` to build, or NULL for no-argument calls)
- `block` (Utf8): Block number or tag ("latest", "pending", "earliest", or integer string)

**Returns:** A struct with two fields:
- `data` (Binary): Return data from the call (NULL on error)
- `message` (Utf8): Error message if the call reverted (NULL on success)

### Query ERC20 Token Info

Fetch token metadata like `name()` and `decimals()` to enrich query results with human-readable token information.

```sql
-- Query token name (no arguments)
SELECT
    evm_decode_hex(token_address) as token,
    evm_decode_type(
        ("edgeandnode/mainnet@0.0.1".eth_call(NULL, token_address, evm_encode_params('name()'), 'latest')).data,
        'string'
    ) as name
FROM tokens

-- Query token decimals
SELECT
    evm_decode_type(
        ("edgeandnode/mainnet@0.0.1".eth_call(NULL, token_address, evm_encode_params('decimals()'), 'latest')).data,
        'uint8'
    ) as decimals
FROM tokens
```

### Query Token Balances

Fetch current token holdings for addresses to analyze wallet portfolios or verify token distributions.

```sql
SELECT
    evm_decode_hex(token) as token,
    evm_decode_hex(holder) as holder,
    shift_units(
        evm_decode_type(result.data, 'uint256'),
        -18  -- assuming 18 decimals
    ) as balance
FROM (
    SELECT
        token,
        holder,
        "edgeandnode/mainnet@0.0.1".eth_call(
            NULL,
            token,
            evm_encode_params(holder, 'balanceOf(address account)'),
            'latest'
        ) as result
    FROM token_holders
)
WHERE result.message IS NULL
```

### Query at Historical Block

Query past state at a specific block to analyze historical balances or reconstruct state at a point in time. Requires an archive node.

```sql
SELECT
    evm_decode_type(result.data, 'uint256') as balance_at_block
FROM (
    SELECT "edgeandnode/mainnet@0.0.1".eth_call(
        NULL,
        evm_encode_hex('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'),  -- WETH
        evm_encode_params(
            evm_encode_hex('0x...holder...'),
            'balanceOf(address account)'
        ),
        '19000000'  -- block number as string
    ) as result
)
```

### Query Uniswap V2 Pair Reserves

Fetch liquidity pool reserves to calculate token prices or analyze DEX liquidity. Demonstrates decoding tuple return values.

```sql
SELECT
    evm_decode_hex(pair_address) as pair,
    evm_decode_type(result.data, '(uint112,uint112,uint32)') as reserves
FROM (
    SELECT
        pair_address,
        "edgeandnode/mainnet@0.0.1".eth_call(
            NULL,
            pair_address,
            evm_encode_params('getReserves()'),
            'latest'
        ) as result
    FROM uniswap_pairs
)
WHERE result.message IS NULL
```

### Call with NULL Input Data

Call a contract without any input data.

```sql
SELECT
    "edgeandnode/mainnet@0.0.1".eth_call(
        NULL,
        evm_encode_hex('0x0000000000000000000000000000000000000000'),
        NULL,
        'latest'
    ) as result
```

### Common Patterns

Handle reverted calls gracefully since `eth_call` returns errors in the result struct rather than failing the query.

#### Filter Successful Calls

Exclude rows where the contract call reverted.

```sql
WHERE ("namespace/name@revision".eth_call(...)).message IS NULL
```

#### Decode the Result Struct

Access the `data` field and decode it to the expected Solidity type.

```sql
evm_decode_type(("namespace/name@revision".eth_call(...)).data, 'uint256')
```

#### Handle Reverts Gracefully

Return NULL instead of propagating errors when calls fail.

```sql
SELECT
    evm_decode_hex(token) as token,
    CASE
        WHEN result.message IS NULL THEN evm_decode_type(result.data, 'uint256')
        ELSE NULL
    END as balance
FROM (
    SELECT token, "edgeandnode/mainnet@0.0.1".eth_call(NULL, token, evm_encode_params(holder, 'balanceOf(address)'), 'latest') as result
    FROM token_holders
)
```

#### Inspect Revert Reasons

Debug failed calls by examining the error message returned by the contract.

```sql
SELECT
    evm_decode_hex(contract) as contract,
    result.message as revert_reason
FROM (
    SELECT contract, "edgeandnode/mainnet@0.0.1".eth_call(NULL, contract, calldata, 'latest') as result
    FROM contracts
)
WHERE result.message IS NOT NULL
```

## Implementation

### Source Files

- `crates/core/common/src/evm/udfs/eth_call.rs` - Core UDF implementation with async JSON-RPC calls and retry logic

## Limitations

- Requires Ethereum JSON-RPC endpoint configured
- Requires dataset qualification (`"namespace/name@revision".eth_call`)
- `to` address is required (cannot be NULL, must be FixedSizeBinary(20))
- `block` is required (cannot be NULL)
- `from` must be NULL or FixedSizeBinary(20) - other types will produce an error
- `input` must be NULL or Binary - other types will produce an error
- Network latency affects query performance
- Rate limits may apply depending on RPC provider
- Large result sets with many eth_calls may timeout

## References

- [udf-builtin](udf-builtin.md) - Base: Built-in UDF overview
- [udf-builtin-evm-params](udf-builtin-evm-params.md) - Dependency: Build calldata with evm_encode_params
- [udf-builtin-evm-type](udf-builtin-evm-type.md) - Dependency: Decode return data with evm_decode_type
- [udf-builtin-evm-hex](udf-builtin-evm-hex.md) - Related: Convert addresses between hex and binary
- [udf-builtin-evm-units](udf-builtin-evm-units.md) - Related: Convert token amounts with shift_units
