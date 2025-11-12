---
title: Query Blockchain Datasets
description: Step-by-step guide to exploring Ethereum mainnet data using Amp Playground and the Amp.
slug: amp-registry-dataset
category: how-to-guide
---

## Overview

This guide walks you through how to query the `edgeandnode/ethereum_mainnet@0.0.1` dataset using:

- Amp Playground (interactive)
- The Amp gateway (programmatic, including dapps)

You’ll learn how to:

- Explore blocks, transactions, token transfers, and contract events
- Filter by addresses, tokens, and time ranges
- Join onchain tables for deeper analytics
- Authenticate via Privy and CLI access tokens
- Run optimized SQL queries from your dapps against live data

## Prerequisites

- Access to [Amp Playground](https://playground.amp.edgeandnode.com)
- Signed in via wallet
- Amp CLI installed (for generating access tokens)

## Dataset Schema

### Core Tables

| Table               | Description             | Key Columns                                                                                  |
| ------------------- | ----------------------- | -------------------------------------------------------------------------------------------- |
| **blocks**          | Block headers           | number, hash, timestamp, miner, transaction_count, gas_limit, gas_used                       |
| **transactions**    | Transaction details     | hash, from_address, to_address, value, gas (limit), gas_price, block_number, block_timestamp |
| **receipts**        | Transaction receipts    | transaction_hash, receipt_gas_used (actual), status, contract_address                        |
| **token_transfers** | ERC-20 token transfers  | token_address, from_address, to_address, value, block_number, transaction_hash               |
| **decoded_events**  | Decoded contract events | contract_address, event_name, block_number, transaction_hash, log_index                      |

> Important Notes

- All addresses (wallet, contract, token) are stored in lowercase
- Transaction hashes and other hex values are also lowercase
- Timestamps are in UTC timezone
- Gas prices are in wei (divide by 1e18 for ETH)
- Tables are partitioned by `block_number` and `block_timestamp` for optimal performance

## Get Started

To get started in the playground:

1. Open **Datasets → edgeandnode → ethereum_mainnet → 0.0.1**
2. Click **Playground** to open the SQL editor for this dataset

## Authentication

Amp supports two main ways to query datasets through the gateway:

1. **Privy Access Token** (handled automatically in UI/CLI)
2. **CLI Access Token (`amp auth token`)**

### 1. Privy Access Token

This is the default path for most users.

- Automatically handled in the Amp Playground UI
- Automatically handled in the Amp CLI after signing in

To establish a CLI session:

```bash
amp auth login
```

No additional setup needed for interactive queries.

### 2. Dapp Access Token

For backend services and dapps, generate an access token:

```bash
amp auth token
```

This command:

- Issues an access token bound to your Amp account / workspace
- Can be used in any dapp or backend that needs to query datasets through the gateway
- Store this token securely (environment variable, secret manager, etc.).

## Gateway Request Pattern

Once you have a token (Privy-backed CLI session or a token from amp auth token), you can call the gateway:

```bash
Copy code
curl -X POST "https://gateway.amp.edgeandnode.com/v1/query" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset": "edgeandnode/ethereum_mainnet@0.0.1",
    "query": "SELECT number, hash FROM blocks ORDER BY number DESC LIMIT 5"
  }'
```

### Example Response

```json
{
  "data": [
    { "number": 18500000, "hash": "0xabc123..." },
    { "number": 18499999, "hash": "0xdef456..." }
  ],
  "metadata": {
    "row_count": 5,
    "execution_time_ms": 145
  }
}
```

Use the same pattern in your dapps (Node.js, Python, etc.).

## Query Interface (Playground)

### Basic Query Structure

In the Amp Playground:

```sql
-- Basic transaction query
-- Basic transaction query
SELECT
  hash AS transaction_hash,
  from_address,
  to_address,
  value / 1e18 AS value_eth,  -- Convert wei to ETH
  gas AS gas_limit,
  gas_price / 1e9 AS gas_price_gwei  -- Convert wei to gwei
FROM transactions
WHERE block_number > 18000000
LIMIT 100;
```

### Query Builder Features

- **Auto-completion** – start typing a table/column and suggestions appear
- **Syntax highlighting** – SQL keywords, functions, and identifiers are colorized
- **Query validation** – obvious syntax errors are surfaced before execution
- **Execution metrics** – after running a query you’ll see duration and row count

## Query Your First Dataset

### 1. Block Analysis

```sql
SELECT
  number,
  hash,
  timestamp,
  miner,
  transaction_count,
  gas_limit,
  gas_used,
  (gas_used * 100.0 / gas_limit) AS gas_utilization_pct
FROM blocks
WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY number DESC
LIMIT 10;
```

### 2. Address Activity

Addresses are stored lowercase; always normalize inputs:

```sql
SELECT
  block_number,
  block_timestamp,
  hash AS transaction_hash,
  CASE
    WHEN from_address = LOWER('0xYourAddress') THEN 'sent'
    WHEN to_address   = LOWER('0xYourAddress') THEN 'received'
  END AS direction,
  value,
  gas,
  gas_price
FROM transactions
WHERE from_address = LOWER('0xYourAddress')
   OR to_address   = LOWER('0xYourAddress')
ORDER BY block_timestamp DESC
LIMIT 100;
```

> Replace 0xYourAddress with the address you care about.

### 3. Token Transfers (ERC-20)

```sql
SELECT
  block_timestamp,
  block_number,
  transaction_hash,
  token_address,
  from_address,
  to_address,
  value AS raw_value  -- Note: decimal places depend on token
FROM token_transfers
WHERE token_address = LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')  -- USDC
  AND block_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
ORDER BY block_timestamp DESC
LIMIT 100;
```

### 4. Gas & Fee Analysis

```sql
SELECT
  TIMESTAMP_TRUNC(t.block_timestamp, HOUR) AS hour,
  AVG(r.receipt_gas_used) AS avg_gas_used,
  AVG(t.gas_price / 1e9) AS avg_gas_price_gwei,
  SUM(r.receipt_gas_used * t.gas_price) / 1e18 AS total_fees_eth,
  COUNT(*) AS tx_count
FROM transactions t
JOIN receipts r
  ON t.hash = r.transaction_hash
WHERE t.block_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY hour
ORDER BY hour DESC;
```

> Note: receipt_gas_used is the actual gas consumed, while gas in transactions table is the gas limit.

### 5. Smart Contract Events

```sql
SELECT
  block_timestamp,
  block_number,
  transaction_hash,
  log_index,
  event_name,
  contract_address
FROM decoded_events
WHERE contract_address = LOWER('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')
  AND event_name = 'Transfer'
  AND block_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY block_timestamp DESC
LIMIT 100;
```

> Note: decoded_events contains standard events (ERC-20 Transfer, Approval, etc.) that have been decoded. Custom events may require additional processing.

## Advanced Querying

### Join Operations

```sql
SELECT
  t.hash AS transaction_hash,
  t.from_address,
  t.to_address,
  t.value,
  t.block_number,
  b.timestamp AS block_timestamp,
  b.miner,
  b.gas_used AS block_gas_used
FROM transactions t
JOIN blocks b
  ON t.block_number = b.number
WHERE b.timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY b.timestamp DESC
LIMIT 200;
```

## Window Function

```sql
SELECT
  block_timestamp,
  block_number,
  hash AS transaction_hash,
  value,
  SUM(value) OVER (
    ORDER BY block_number
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_inflow
FROM transactions
WHERE to_address = LOWER('0xYourAddress')
ORDER BY block_number
LIMIT 200;
```

## Aggregation Queries

```sql
SELECT
  DATE(block_timestamp)                   AS date,
  COUNT(*)                                AS total_transactions,
  SUM(receipt_gas_used * gas_price) / 1e18 AS total_fees_eth,
  AVG(value)                              AS avg_value
FROM transactions
WHERE block_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY date
ORDER BY date DESC;
```

## Query Optimization

### 1. Use Time Filters

```sql
WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
```

### 2. Avoid `SELECT *`

```sql
SELECT
  block_timestamp,
  number,
  miner,
  gas_used,
  transaction_count
FROM blocks
WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY block_timestamp DESC;
```

### 3. Always Use `LIMIT`

```sql
SELECT
  hash,
  from_address,
  to_address,
  value
FROM transactions
WHERE block_number > 18000000
LIMIT 1000;
```

### 4. Normalize Addresses

```sql
WHERE from_address = LOWER('0xMixedCaseInput')
   OR to_address   = LOWER('0xMixedCaseInput')
```

## Error Handling

| Error type       | Cause                   | Fix                                                         |
| ---------------- | ----------------------- | ----------------------------------------------------------- |
| **TIMEOUT**      | Wide scan, no filters   | Add `block_timestamp` / `block_number` filters, use `LIMIT` |
| **SYNTAX_ERROR** | Invalid SQL             | Check commas, function names, column names                  |
| **NO_DATA**      | Query returns zero rows | Check address case, contract/token address, table name      |
| **PERMISSION**   | Token or login issue    | Re-auth with Privy / regenerate `amp auth token`            |

## Quick Reference

### Useful Functions

- `CURRENT_TIMESTAMP ()`
- `TIMESTAMP_SUB (ts, INTERVAL n HOUR)`
- `TIMESTAMP_TRUNC (ts, HOUR|DAY)`
- `DATE`(block_timestamp)`
- `COUNT(*), SUM(), AVG()`
- `LOWER(address)`
