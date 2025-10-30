import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import { exec as nodeExec } from "node:child_process"
import * as path from "node:path"
import { promisify } from "node:util"
import type { Template, TemplateAnswers } from "./Template.ts"
import { TemplateError } from "./Template.ts"

const exec = promisify(nodeExec)

/**
 * Local EVM RPC template for Anvil-based development
 *
 * This template provides a complete setup for local blockchain development using Anvil.
 * It includes a sample Counter contract with events and a pre-configured dataset.
 */
export const localEvmRpc: Template = {
  name: "local-evm-rpc",
  description: "Local development with Anvil",
  files: {
    "amp.config.ts": (answers: TemplateAnswers) =>
      `import { defineDataset } from "@edgeandnode/amp"

const event = (signature: string) => {
  return \`
    SELECT block_hash, tx_hash, block_num, timestamp, address,
           evm_decode_log(topic1, topic2, topic3, data, '\${signature}') as event
    FROM anvil_rpc.logs
    WHERE topic0 = evm_topic('\${signature}')
  \`
}

const transfer = event("Transfer(address indexed from, address indexed to, uint256 value)")
const count = event("Count(uint256 count)")

export default defineDataset(() => ({
  name: "${answers.datasetName}",
  version: "${answers.datasetVersion || "0.1.0"}",
  network: "anvil",
  dependencies: {
    anvil_rpc: "_/anvil_rpc@0.1.0",
  },
  tables: {
    counts: {
      sql: \`
        SELECT c.block_hash, c.tx_hash, c.address, c.block_num, c.timestamp,
               c.event['count'] as count
        FROM (\${count}) as c
      \`,
    },
    transfers: {
      sql: \`
        SELECT t.block_num, t.timestamp,
               t.event['from'] as from,
               t.event['to'] as to,
               t.event['value'] as value
        FROM (\${transfer}) as t
      \`,
    },
  },
}))
`,

    "README.md": (answers: TemplateAnswers) =>
      `# ${answers.projectName || answers.datasetName}

Local Amp development with Anvil blockchain testnet.

## What This Template Provides

**Dataset**: \`${answers.datasetName}\` (version \`${answers.datasetVersion || "0.1.0"}\`)
**Network**: Anvil local testnet
**Sample Contract**: Counter.sol with Count and Transfer events

This template sets up everything you need to learn Amp's data extraction flow using a local blockchain.

## Quick Start

### Prerequisites

**Required:**
- **[Amp Daemon (\`ampd\`)](https://ampup.sh)** - Core extraction and query engine:
  \`\`\`bash
  curl --proto '=https' --tlsv1.2 -sSf https://ampup.sh/install | sh
  \`\`\`
- **[Foundry](https://book.getfoundry.sh/getting-started/installation)** - For Anvil testnet and smart contracts:
  \`\`\`bash
  curl -L https://foundry.paradigm.xyz | bash && foundryup
  \`\`\`

**Optional (Better Developer Experience):**
- **Node.js 18+** - For TypeScript CLI with hot-reloading (\`amp dev\`, \`amp query\`)

**NOT Required:**
- PostgreSQL (temporary database used automatically in dev mode)

### 1. Start Anvil

In a separate terminal, start the local Anvil testnet:

\`\`\`bash
anvil
\`\`\`

This starts a local Ethereum node at \`http://localhost:8545\` with 10 pre-funded test accounts.

### 2. Deploy Sample Contracts (Optional)

If you want to deploy the included Counter contract:

\`\`\`bash
cd contracts
forge script script/Counter.s.sol \
  --broadcast \
  --rpc-url http://localhost:8545 \
  --sender 0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266 \
  --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
\`\`\`

This deploys the Counter contract and generates some test events (3 Count events + 2 Transfer events).

### 3. Start Amp development server

Start the Amp development server with hot-reloading:

\`\`\`bash
cd ..  # Go back to project root (where amp.toml is)

# Start ampd in development mode (single-node: controller + server + worker combined)
AMP_CONFIG=amp.toml ampd dev
\`\`\`

This single command starts **everything you need**:
- **Admin API** (port 1610) - Job management and control
- **Query Servers** - Arrow Flight (port 1602) and JSON Lines (port 1603)
- **Embedded Worker** - Data extraction from Anvil
- **Temporary PostgreSQL** - Metadata storage (no setup required!)

Leave \`ampd dev\` running and continue to the next step.

### 5. Register and Deploy the Dataset

In a **new terminal**, register the anvil dataset with the Admin API and deploy it to start extraction:

\`\`\`bash
# Register the anvil_rpc dataset
curl -X POST http://localhost:1610/datasets \\
  -H "Content-Type: application/json" \\
  -d "{\\"dataset_name\\": \\"_/anvil_rpc\\", \\"version\\": \\"0.1.0\\", \\"manifest\\": $(cat manifests/anvil_rpc.json | jq -c .)}"

# Deploy the dataset to trigger data extraction
curl -X POST http://localhost:1610/datasets/_/anvil_rpc/versions/0.1.0/deploy
\`\`\`

This will:
- Register the Anvil RPC dataset manifest with ampd
- Start extracting blocks, transactions, and logs from Anvil
- Store data as Parquet files in the \`data/\` directory
- Make data queryable via SQL once extraction begins

**Note:** Requires \`jq\` for JSON formatting. Install with \`brew install jq\` on macOS or \`apt-get install jq\` on Linux.

### 6. Query Your Data

In a **new terminal**, query your blockchain data:

**Option A: Simple HTTP queries (no additional tools needed)**
\`\`\`bash
# Query raw anvil_rpc logs
curl -X POST http://localhost:1603 --data "SELECT * FROM anvil_rpc.logs LIMIT 10"

# Query raw blocks
curl -X POST http://localhost:1603 --data "SELECT block_num, timestamp, hash FROM anvil_rpc.blocks LIMIT 10"

# Query raw transactions
curl -X POST http://localhost:1603 --data "SELECT * FROM anvil_rpc.transactions LIMIT 10"
\`\`\`

**Option B: TypeScript CLI (better formatting, if Node.js installed)**
\`\`\`bash
# Query raw anvil_rpc logs
npx @edgeandnode/amp query "SELECT * FROM anvil_rpc.logs LIMIT 10"

# Query decoded count events (requires amp dev running for TypeScript dataset)
npx @edgeandnode/amp query "SELECT * FROM ${answers.datasetName}.counts LIMIT 10"

# Query decoded transfer events
npx @edgeandnode/amp query "SELECT * FROM ${answers.datasetName}.transfers LIMIT 10"
\`\`\`

**Or if developing Amp locally:**
\`\`\`bash
bun /path/to/typescript/amp/src/cli/bun.ts query "SELECT * FROM anvil_rpc.logs LIMIT 10"
\`\`\`

## Project structure

\`\`\`
.
├── amp.config.ts          # TypeScript dataset configuration (optional, for amp dev)
│                          # Defines your dataset: ${answers.datasetName}@${answers.datasetVersion || "0.1.0"}
│
├── amp.toml               # Main Amp configuration (required for ampd)
│                          # Points to manifests/, providers/, and data/ directories
│
├── manifests/             # Dataset definitions (JSON manifests for ampd)
│   └── anvil_rpc.json     # Anvil EVM RPC dataset definition
│                          # Defines schemas for blocks, logs, and transactions tables
│
├── providers/             # Provider configurations (connection settings)
│   └── anvil.toml         # Anvil RPC endpoint at http://localhost:8545
│
├── data/                  # Parquet file storage (created automatically)
│   └── .gitkeep           # Ensures directory exists in git
│
├── contracts/             # Sample Foundry project
│   ├── src/
│   │   └── Counter.sol    # Sample contract with Count + Transfer events
│   ├── script/
│   │   └── Counter.s.sol  # Deployment script
│   └── foundry.toml       # Foundry configuration
│
├── README.md              # This file
└── .gitignore             # Git ignore rules
\`\`\`

## Pre-configured tables

Your \`amp.config.ts\` defines two SQL views that will extract and decode event data once you run \`amp dev\`:

1. **\`${answers.datasetName}.counts\`** - Decodes Count events from the Counter contract
   - Extracts: block_hash, tx_hash, address, block_num, timestamp, count
   - Uses \`evm_decode_log\` UDF to parse event data

2. **\`${answers.datasetName}.transfers\`** - Decodes Transfer events from the Counter contract
   - Extracts: block_num, timestamp, from, to, value
   - Uses \`evm_decode_log\` UDF to parse event data

These are SQL view definitions, not extracted data yet. Data extraction happens when you run \`amp dev\`.

### Dependencies

Your dataset depends on:
- **\`anvil_rpc\`** (_/anvil_rpc@0.1.0) - Provides raw blockchain data (blocks, transactions, logs) extracted from Anvil RPC

## Learn more

### Amp Documentation

- [Overview](../../docs/README.md) - Introduction to Amp
- [Executables](../../docs/exes.md) - Guide to \`ampd\`, \`ampctl\`, and \`amp\` CLI
- [Configuration](../../docs/config.md) - Advanced configuration options
- [User-Defined Functions](../../docs/udfs.md) - Available UDFs like \`evm_decode_log\`
- [Glossary](../../docs/glossary.md) - Key terminology

### Amp Concepts

- **Datasets** - SQL-based transformations over blockchain data
- **Tables** - SQL views that extract and decode blockchain events
- **UDFs** - Custom functions like \`evm_decode_log\`, \`evm_topic\`
- **Dependencies** - Datasets can build on other datasets

### Useful Commands

\`\`\`bash
# Start dev server with hot-reloading
amp dev

# Query your dataset
amp query "SELECT * FROM ${answers.datasetName}.counts"

# Open interactive query playground
amp studio

# Build manifest without starting server
amp build

# View available datasets
amp datasets

# Get help
amp --help
\`\`\`

### Modifying Your Dataset

Edit \`amp.config.ts\` to:
- Add new tables for different events
- Change SQL queries to transform data
- Add dependencies on other datasets
- Define custom JavaScript UDFs

Changes are automatically detected when running \`amp dev\`!

## Troubleshooting

**Anvil not running?**
\`\`\`bash
anvil
\`\`\`

**Port conflicts?**
- Anvil: 8545
- Amp Arrow Flight: 1602
- Amp JSON Lines: 1603
- Amp Admin API: 1610

**No data appearing?**
1. Make sure Anvil is running
2. Deploy contracts to generate events: \`cd contracts && forge script script/Counter.s.sol --broadcast --rpc-url http://localhost:8545\`
3. Check logs: \`amp dev --logs debug\`

**Contract deployment failed?**
- Ensure Anvil is running
- Try restarting Anvil
- Check the RPC URL matches: \`http://localhost:8545\`

**Want to start fresh?**
\`\`\`bash
# Stop ampd dev (Ctrl+C)
# Stop Anvil (Ctrl+C)
# Delete data directory
rm -rf data/
# Restart Anvil
anvil
# Restart ampd dev
AMP_CONFIG=amp.toml ampd dev
\`\`\`

**\`ampd\` not found?**
Make sure you installed it via ampup and it's in your PATH:
\`\`\`bash
curl --proto '=https' --tlsv1.2 -sSf https://ampup.sh/install | sh
# Then restart your terminal or source your profile
\`\`\`

## Next steps

1. **Deploy your own contracts** - Add contracts to \`contracts/src/\`
2. **Extract your events** - Update \`amp.config.ts\` with your event signatures
3. **Build complex queries** - Join tables, aggregate data, filter results
4. **Explore Amp Studio** - Use the visual query builder
5. **Move to testnet/mainnet** - When ready, switch from Anvil to real networks

---

For more information, see the [Amp documentation](https://github.com/edgeandnode/amp).
`,

    "contracts/foundry.toml": `[profile.default]
src = "src"
out = "out"
libs = ["lib"]
solc_version = "0.8.20"

# See more config options https://github.com/foundry-rs/foundry/blob/master/crates/config/README.md#all-options
`,

    "contracts/src/Counter.sol": `// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * @title Counter
 * @notice A simple counter contract that emits events for demonstration
 */
contract Counter {
    uint256 public count;

    event Count(uint256 count);
    event Transfer(address indexed from, address indexed to, uint256 value);

    constructor() {
        count = 0;
    }

    function increment() public {
        count += 1;
        emit Count(count);
    }

    function decrement() public {
        require(count > 0, "Counter: cannot decrement below zero");
        count -= 1;
        emit Count(count);
    }

    function transfer(address to, uint256 value) public {
        emit Transfer(msg.sender, to, value);
    }
}
`,

    "contracts/script/Counter.s.sol": `// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Script} from "forge-std/Script.sol";
import {Counter} from "../src/Counter.sol";

contract Deploy is Script {
    function run() external {
        vm.startBroadcast();

        Counter counter = new Counter();

        // Interact with the counter to generate some events
        counter.increment();
        counter.increment();
        counter.increment();
        counter.transfer(address(0x1), 100);
        counter.transfer(address(0x2), 200);

        vm.stopBroadcast();
    }
}
`,

    "contracts/remappings.txt": `forge-std/=lib/forge-std/src/
`,

    ".gitignore": `# Amp
data/
manifests/
providers/

# Foundry
cache/
out/
broadcast/
contracts/out/
contracts/cache/
contracts/broadcast/
contracts/lib/

# Rust
target/

# Node (nested workspaces)
**/node_modules/
**/dist/
**/*.tsbuildinfo

# Bun
bun.lockb

# Environment
.env
.env.local

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
`,

    "amp.toml": `# Amp configuration for local development with Anvil
# This config is used by ampd (the Rust daemon)

# Where extracted parquet files are stored
data_dir = "data"

# Directory containing provider configurations
providers_dir = "providers"

# Directory containing dataset manifests
# Note: Manifests here are NOT auto-loaded. You must register datasets via the Admin API.
# See README for registration commands.
dataset_defs_dir = "manifests"

# Optional: Temporary PostgreSQL will be used automatically in dev mode
# No need to configure metadata_db_url for local development
`,

    "providers/anvil.toml": `# Anvil local testnet provider configuration
kind = "evm-rpc"
network = "anvil"
url = "http://localhost:8545"
`,

    "manifests/anvil_rpc.json": `{
  "kind": "evm-rpc",
  "network": "anvil",
  "start_block": 0,
  "finalized_blocks_only": false,
  "tables": {
    "blocks": {
      "schema": {
        "arrow": {
          "fields": [
            { "name": "block_num", "type": "UInt64", "nullable": false },
            { "name": "timestamp", "type": { "Timestamp": ["Nanosecond", "+00:00"] }, "nullable": false },
            { "name": "hash", "type": { "FixedSizeBinary": 32 }, "nullable": false },
            { "name": "parent_hash", "type": { "FixedSizeBinary": 32 }, "nullable": false },
            { "name": "ommers_hash", "type": { "FixedSizeBinary": 32 }, "nullable": false },
            { "name": "miner", "type": { "FixedSizeBinary": 20 }, "nullable": false },
            { "name": "state_root", "type": { "FixedSizeBinary": 32 }, "nullable": false },
            { "name": "transactions_root", "type": { "FixedSizeBinary": 32 }, "nullable": false },
            { "name": "receipt_root", "type": { "FixedSizeBinary": 32 }, "nullable": false },
            { "name": "logs_bloom", "type": "Binary", "nullable": false },
            { "name": "difficulty", "type": { "Decimal128": [38, 0] }, "nullable": false },
            { "name": "gas_limit", "type": "UInt64", "nullable": false },
            { "name": "gas_used", "type": "UInt64", "nullable": false },
            { "name": "extra_data", "type": "Binary", "nullable": false },
            { "name": "mix_hash", "type": { "FixedSizeBinary": 32 }, "nullable": false },
            { "name": "nonce", "type": "UInt64", "nullable": false },
            { "name": "base_fee_per_gas", "type": { "Decimal128": [38, 0] }, "nullable": true },
            { "name": "withdrawals_root", "type": { "FixedSizeBinary": 32 }, "nullable": true },
            { "name": "blob_gas_used", "type": "UInt64", "nullable": true },
            { "name": "excess_blob_gas", "type": "UInt64", "nullable": true },
            { "name": "parent_beacon_root", "type": { "FixedSizeBinary": 32 }, "nullable": true }
          ]
        }
      },
      "network": "anvil"
    },
    "logs": {
      "schema": {
        "arrow": {
          "fields": [
            { "name": "block_hash", "type": { "FixedSizeBinary": 32 }, "nullable": false },
            { "name": "block_num", "type": "UInt64", "nullable": false },
            { "name": "timestamp", "type": { "Timestamp": ["Nanosecond", "+00:00"] }, "nullable": false },
            { "name": "tx_hash", "type": { "FixedSizeBinary": 32 }, "nullable": false },
            { "name": "tx_index", "type": "UInt32", "nullable": false },
            { "name": "log_index", "type": "UInt32", "nullable": false },
            { "name": "address", "type": { "FixedSizeBinary": 20 }, "nullable": false },
            { "name": "topic0", "type": { "FixedSizeBinary": 32 }, "nullable": true },
            { "name": "topic1", "type": { "FixedSizeBinary": 32 }, "nullable": true },
            { "name": "topic2", "type": { "FixedSizeBinary": 32 }, "nullable": true },
            { "name": "topic3", "type": { "FixedSizeBinary": 32 }, "nullable": true },
            { "name": "data", "type": "Binary", "nullable": false }
          ]
        }
      },
      "network": "anvil"
    },
    "transactions": {
      "schema": {
        "arrow": {
          "fields": [
            { "name": "block_hash", "type": { "FixedSizeBinary": 32 }, "nullable": false },
            { "name": "block_num", "type": "UInt64", "nullable": false },
            { "name": "timestamp", "type": { "Timestamp": ["Nanosecond", "+00:00"] }, "nullable": false },
            { "name": "tx_index", "type": "UInt32", "nullable": false },
            { "name": "tx_hash", "type": { "FixedSizeBinary": 32 }, "nullable": false },
            { "name": "to", "type": { "FixedSizeBinary": 20 }, "nullable": true },
            { "name": "nonce", "type": "UInt64", "nullable": false },
            { "name": "gas_price", "type": { "Decimal128": [38, 0] }, "nullable": true },
            { "name": "gas_limit", "type": "UInt64", "nullable": false },
            { "name": "value", "type": { "Decimal128": [38, 0] }, "nullable": false },
            { "name": "input", "type": "Binary", "nullable": false },
            { "name": "v", "type": "Binary", "nullable": false },
            { "name": "r", "type": "Binary", "nullable": false },
            { "name": "s", "type": "Binary", "nullable": false },
            { "name": "gas_used", "type": "UInt64", "nullable": false },
            { "name": "type", "type": "Int32", "nullable": false },
            { "name": "max_fee_per_gas", "type": { "Decimal128": [38, 0] }, "nullable": true },
            { "name": "max_priority_fee_per_gas", "type": { "Decimal128": [38, 0] }, "nullable": true },
            { "name": "max_fee_per_blob_gas", "type": { "Decimal128": [38, 0] }, "nullable": true },
            { "name": "from", "type": { "FixedSizeBinary": 20 }, "nullable": false },
            { "name": "status", "type": "Boolean", "nullable": false }
          ]
        }
      },
      "network": "anvil"
    }
  }
}
`,

    "data/.gitkeep": "",
  },
  postInstall: (projectPath: string) =>
    Effect.gen(function*() {
      const cwd = path.join(projectPath, "contracts")

      const run = (cmd: string, errMsg: string) =>
        Effect.tryPromise({
          try: () => exec(cmd, { cwd }),
          catch: (cause) => new TemplateError({ message: errMsg, cause }),
        })

      yield* Console.log("\nInstalling Foundry dependencies (forge-std)")
      // Ensure lib directory exists
      yield* run(
        "mkdir -p lib",
        "Failed to create contracts/lib directory.",
      )
      // Clone forge-std directly to avoid submodule setup issues
      yield* run(
        "git clone --depth 1 --branch v1.9.6 https://github.com/foundry-rs/forge-std.git lib/forge-std",
        "Failed to clone forge-std. You can do this manually:\n  cd contracts && mkdir -p lib && git clone --depth 1 --branch v1.9.6 https://github.com/foundry-rs/forge-std.git lib/forge-std",
      )

      yield* Console.log("Building contracts")
      yield* run(
        "forge build",
        "Failed to build Foundry contracts. You can do this manually:\n  cd contracts && forge build",
      )

      yield* Console.log("Foundry setup complete\n")
    }),
}
