---
title: How to Extract Data Using Serverless Mode
description: Learn how to run a one-off extraction
slug: how-to/serverless-extraction
command: ampd dump
category: extraction
---

## Goal

Extract blockchain dataset data on demand using Amp’s serverless mode (ampd dump).

### When to Use

- One-time or scheduled data extraction
- Bootstrapping datasets
- Verifying dataset configuration
- CI/CD or event-driven jobs

## Steps

1. Choose the dataset

Identify your dataset name or manifest path (e.g., eth_mainnet).

2. Run a simple extraction

```bash
ampd dump --dataset eth_mainnet
```

3. Run in parallel for speed

```bash
ampd dump --dataset eth_mainnet --n-jobs 4
```

4. Extract a range of blocks

```bash
ampd dump --dataset eth_mainnet --end-block 4000000
```

5. Resume or start fresh
   Resume automatically (default)

Start fresh:

```bash
 ampd dump --dataset eth_mainnet --fresh
```

6. Automate the extraction
   Run periodically (e.g., every 30 minutes):

```bash
ampd dump --dataset eth_mainnet --run-every-mins 30
```

## Expected Outcome

Amp runs an ephemeral extraction job:

1. Loads dataset config
2. Extracts blockchain data
3. Writes Parquet files
4. Updates progress in metadata DB
5. Exits when done
