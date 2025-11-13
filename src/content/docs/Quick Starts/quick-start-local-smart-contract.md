---
title: Index Local Smart Contract
slug: amp-quickstart-index-local-smart-contract
category: tutorial
description: Learn how to index a local smart contract with Amp and query decoded events using a fully local workflow.
---

## Overview

This tutorial walks you through indexing events from a local smart contract using Amp. You’ll install the toolchain, run Foundry (Anvil/Forge/Cast), deploy a Counter contract, and query emitted events through Amp’s SQL interface. Everything runs locally except the initial tool downloads.

**Goal:**  
Get `curl` to return rows from your Counter contract through Amp in about 15 minutes.

---

## Prerequisites

You need:

- macOS or Linux
- `curl`, `git`
- Rust toolchain

Install Rust:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

## 1. Install the Tooling

### 1.1 Install Amp Toolchain (`ampup`)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://ampup.sh/install | sh
ampup install
ampd --version && ampctl --version
```

`ampup` installs Amp under `~/.amp.` Use `ampup use <version>` for a specific release.

> Note: `ampctl` is currently build from Source, From the repository root run:

```bash
cargo install --path crates/bin/ampctl
ampctl --version
```

Make sure `$HOME/.cargo/bin` is on your `PATH`.

### 1.2 Install Foundry

```bash
curl -L https://foundry.paradigm.xyz | bash
foundryup
forge --version && anvil --version
```

## 2. Start an Foundry (Anvil,Forge,and Cast) and Deploy the Contract

Create a project directory and keep three terminals open:

- Terminal 1: Anvil
- Terminal 2: deployment and Amp commands
- Terminal 3: `ampd dev`

This walkthrough runs outside the Amp repo and only needs the installed binaries. Create a working directory; the guide uses `~/amp-counter`, but you can use any path.

```bash
mkdir -p ~/amp-counter
cd ~/amp-counter
```

### 2.1 Create the Contract

```bash
forge init contracts
```

Replace `contracts/src/Counter.Sol` with:

```solidity
// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.20;

contract Counter {
    uint256 public number;
    event Count(uint256 count);

    function setNumber(uint256 newNumber) public {
        number = newNumber;
        emit Count(number);
    }

    function increment() public {
        number++;
        emit Count(number);
    }
}
```

### 2.2 Start Anvil (Terminal 1)

```bash
cd ~/amp-counter
anvil --block-time 1 --chain-id 31337
```

> Note: default private key:`0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80`

### 2.3 Compile and Deploy (Terminal 2)

```bash
cd ~/amp-counter
forge build --root contracts

QUICKSTART_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80

forge create contracts/src/Counter.sol:Counter \
  --rpc-url http://127.0.0.1:8545 \
  --private-key $QUICKSTART_KEY \
  --broadcast
```

Save the `Deployed to:` address as `COUNTER_ADDRESS`.

### 2.4 Emit Events

```bash
COUNTER_ADDRESS=<deployed address>

cast send $COUNTER_ADDRESS "increment()" \
  --rpc-url http://127.0.0.1:8545 \
  --private-key $QUICKSTART_KEY

cast send $COUNTER_ADDRESS "setNumber(uint256)" 41 \
  --rpc-url http://127.0.0.1:8545 \
  --private-key $QUICKSTART_KEY
```

Each call emits a `Count(number)` event.

## 3. Create the Amp Workspace

Amp requires three directories—manifests, providers, and data—plus a config file. Keep them under one root for simpler paths.

```bash
mkdir -p ~/amp-counter/amp/{data,manifests,providers}
```

Create `~/amp-counter/amp/config.toml`:

```toml
data_dir = "data"
providers_dir = "providers"
manifests_dir = "manifests"
```

## 4. Run `ampd dev` (Terminal 3)

```bash
cd ~/amp-counter/amp
export AMP_CONFIG=~/amp-counter/amp/config.toml
export AMP_LOG=info
ampd dev
```

Amp bundles:

- Admin API: 1610
- Flight: 1602
- JSON Lines: 1603

Leave this running.

## 5. Register the Provider and Datasets (Terminal 2)

### 5.1 Register the Anvil Provider

Create `~/amp-counter/amp/providers/anvil.toml`:

```toml
kind = "evm-rpc"
network = "anvil"
url = "http://127.0.0.1:8545"
```

Register it:

```bash
ampctl provider register anvil ~/amp-counter/amp/providers/anvil.toml
```

> This provider connects to your local Anvil instance, so all calls stay on your machine. For optional settings like rate limits or batching, see docs/providers/evm-rpc.sample.toml.

### 5.2 Register the Raw Dataset

Generate the EVM RPC manifest (canonical manifest):

```bash
ampctl gen-manifest \
  --network anvil \
  --kind evm-rpc \
  --name anvil \
  -o ~/amp-counter/amp/manifests/anvil.evm-rpc.json
```

Register it under a namespace (quickstart here) and pin a version:

```bash
ampctl dataset register quickstart/anvil \
  ~/amp-counter/amp/manifests/anvil.evm-rpc.json \
  --tag 0.0.1
```

### 5.3 Register the Derived Dataset

Create `~/amp-counter/amp/manifests/counter.manifest.json` with the full JSON manifest for the `counts` derived table:

````json
{
  "kind": "manifest",
  "dependencies": {
    "anvil": "quickstart/anvil@0.0.1"
  },
  "tables": {
    "counts": {
      "network": "anvil",
      "input": {
        "sql": "
        SELECT block_hash,
               tx_hash,
               block_num,
               timestamp,
               address,
               evm_decode_log(topic1, topic2, topic3, data, 'Count(uint256 count)') AS event
        FROM anvil.logs
        WHERE topic0 = evm_topic('Count(uint256 count)')
        "
      },
      "schema": {
        "arrow": {
          "fields": [
            { "name": "_block_num", "type": "UInt64", "nullable": false },
            { "name": "block_hash", "type": { "FixedSizeBinary": 32 }, "nullable": false },
            { "name": "tx_hash", "type": { "FixedSizeBinary": 32 }, "nullable": false },
            { "name": "address", "type": { "FixedSizeBinary": 20 }, "nullable": false },
            { "name": "block_num", "type": "UInt64", "nullable": false },
            {
              "name": "timestamp",
              "type": { "Timestamp": ["Nanosecond", "+00:00"] },
              "nullable": false
            },
            { "name": "count", "type": "Utf8", "nullable": true }
          ]
        }
      }
    }
  },
  "functions": {}
}
``

Register:

```bash
ampctl dataset register quickstart/counter \
  ~/amp-counter/amp/manifests/counter.manifest.json \
  --tag 0.0.1
````

## 6. Deploy the Datasets

Deploy raw dataset:

```bash
ampctl dataset deploy quickstart/anvil@0.0.1
ampctl jobs list --status active
```

Once complete:

```bash
ampctl dataset deploy quickstart/counter@0.0.1
```

Both jobs stay active and process new blocks and events in real time.

## 7. Query the Counter Table

`ampd dev` node exposes its JSON Lines endpoint at http://127.0.0.1:1603.
Query the derived dataset using `namespace/name` as the schema identifier

```bash
curl -X POST http://127.0.0.1:1603 \
  --data "SELECT block_num, timestamp, count
          FROM \"quickstart/counter\".counts
          ORDER BY block_num DESC
          LIMIT 5"
```

Example output:

```json
{"block_num":7,"timestamp":"2024-04-01T19:08:15Z","count":"41"}
{"block_num":6,"timestamp":"2024-04-01T19:08:12Z","count":"1"}
```

Parquet files are stored under:

```
~/amp-counter/amp/data/quickstart/anvil
```

## 8. Iterate and Extend

- Emit more events to generate more rows
- Add additional tables to the manifest
- Point the provider to another network and redeploy

## Troubleshoot
