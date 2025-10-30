import type { Template, TemplateAnswers } from "./Template.ts"

/**
 * Local EVM RPC template for Anvil-based development
 *
 * This template provides a quick-start setup for learning Amp with a local blockchain.
 * It includes a sample contract that generates 500 events for immediate querying.
 */
export const localEvmRpc: Template = {
  name: "local-evm-rpc",
  description: "Local development with Anvil and sample data",
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

const dataEmitted = event("DataEmitted(uint256 indexed id, address indexed sender, uint256 value, string message)")

export default defineDataset(() => ({
  name: "${answers.datasetName}",
  version: "${answers.datasetVersion || "0.1.0"}",
  network: "anvil",
  dependencies: {
    anvil_rpc: "_/anvil_rpc@0.1.0",
  },
  tables: {
    events: {
      sql: \`
        SELECT
          e.block_hash,
          e.tx_hash,
          e.address,
          e.block_num,
          e.timestamp,
          e.event['id'] as event_id,
          e.event['sender'] as sender,
          e.event['value'] as value,
          e.event['message'] as message
        FROM (\${dataEmitted}) as e
        ORDER BY e.block_num, e.event['id']
      \`,
    },
  },
}))
`,

    "README.md": (answers: TemplateAnswers) =>
      `# ${answers.projectName || answers.datasetName}

Learn Amp with 500 sample events on a local blockchain.

## What You Get

- **500 events** ready to query immediately after setup
- **Local Anvil testnet** - no external dependencies
- **Sample queries** demonstrating Amp's SQL capabilities
- **Educational walkthrough** of Amp's data flow

## Quick Start (5 steps, ~2 minutes)

### Prerequisites

Install these once:

\`\`\`bash
# Amp daemon (extraction & query engine)
curl --proto '=https' --tlsv1.2 -sSf https://ampup.sh/install | sh

# Foundry (Anvil testnet & Forge)
curl -L https://foundry.paradigm.xyz | bash && foundryup

# jq (JSON processing - needed for dataset registration)
# macOS:
brew install jq
# Linux:
sudo apt-get install jq
\`\`\`

### Step 1: Start Anvil

Open a terminal and start the local blockchain:

\`\`\`bash
anvil
\`\`\`

**Keep this running.** You should see:
\`\`\`
Listening on 127.0.0.1:8545
\`\`\`

### Step 2: Generate 500 Sample Events

In a **new terminal**, deploy the contract to generate events:

\`\`\`bash
cd contracts

# Install dependencies
forge install foundry-rs/forge-std --no-git

# Deploy contract and emit 500 events
forge script script/EventEmitter.s.sol \\
  --broadcast \\
  --rpc-url http://localhost:8545 \\
  --sender 0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266 \\
  --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80

cd ..
\`\`\`

This will:
- Deploy the EventEmitter contract
- Emit 500 \`DataEmitted\` events with different values
- Take ~5-10 seconds to complete

### Step 3: Start Amp

Start the Amp development server:

\`\`\`bash
AMP_CONFIG=amp.toml ampd dev
\`\`\`

**Keep this running.** You should see:
\`\`\`
Admin API started on port 1610
Query servers started (Arrow Flight: 1602, JSON Lines: 1603)
\`\`\`

### Step 4: Register Dataset

In a **new terminal**, register the dataset with Amp:

\`\`\`bash
# Register the provider (tells Amp where Anvil is)
curl -X POST http://localhost:1610/providers \\
  -H "Content-Type: application/json" \\
  -d '{
    "name": "anvil_rpc",
    "kind": "evm-rpc",
    "network": "anvil",
    "url": "http://localhost:8545"
  }'

# Build registration payload
jq -n \\
  --arg manifest "$(cat manifests/anvil_rpc.json | jq -c .)" \\
  '{namespace: "_", name: "anvil_rpc", version: "0.1.0", manifest: $manifest}' \\
  > /tmp/register.json

# Register the dataset
curl -X POST http://localhost:1610/datasets \\
  -H "Content-Type: application/json" \\
  -d @/tmp/register.json

# Deploy to trigger extraction
curl -X POST http://localhost:1610/datasets/_/anvil_rpc/versions/0.1.0/deploy \\
  -H "Content-Type: application/json" \\
  -d '{}'
\`\`\`

Wait ~10 seconds for extraction to complete. Watch the \`ampd dev\` terminal for extraction progress.

### Step 5: Query Your Data

Query the 500 events:

\`\`\`bash
# Simple count
curl -X POST http://localhost:1603 --data "SELECT COUNT(*) as total_events FROM anvil_rpc.logs"

# View first 10 events
curl -X POST http://localhost:1603 --data "SELECT * FROM anvil_rpc.logs LIMIT 10"

# Get event distribution
curl -X POST http://localhost:1603 --data "
  SELECT block_num, COUNT(*) as events_in_block
  FROM anvil_rpc.logs
  GROUP BY block_num
  ORDER BY block_num"
\`\`\`

**Success!** You now have a working Amp setup with real blockchain data.

### Optional: Use Amp Studio (Visual Query Interface)

For a better developer experience, use Amp Studio - a web-based query playground:

\`\`\`bash
# If using published package (requires studio to be built)
npx @edgeandnode/amp studio

# Or if developing Amp locally
bun /path/to/typescript/amp/src/cli/bun.ts studio
\`\`\`

This opens a visual query builder at \`http://localhost:1615\` with:
- Syntax highlighting and auto-completion
- Formatted query results
- Pre-populated example queries from your dataset
- Real-time error feedback

**Note**: Studio requires the frontend to be built. If it's not available, use curl commands below.

---

## Learning Amp: Query Examples

These examples demonstrate Amp's capabilities. Try them!

### 1. Basic Filtering

\`\`\`bash
# Events from a specific block
curl -X POST http://localhost:1603 --data "
  SELECT * FROM anvil_rpc.logs
  WHERE block_num = 1"

# Events from blocks 1-5
curl -X POST http://localhost:1603 --data "
  SELECT block_num, log_index, address
  FROM anvil_rpc.logs
  WHERE block_num BETWEEN 1 AND 5"
\`\`\`

### 2. Aggregations

\`\`\`bash
# Count events per block
curl -X POST http://localhost:1603 --data "
  SELECT block_num, COUNT(*) as event_count
  FROM anvil_rpc.logs
  GROUP BY block_num
  ORDER BY block_num"

# Get min/max/avg block numbers
curl -X POST http://localhost:1603 --data "
  SELECT
    MIN(block_num) as first_block,
    MAX(block_num) as last_block,
    COUNT(DISTINCT block_num) as total_blocks
  FROM anvil_rpc.logs"
\`\`\`

### 3. Decoded Events (Using Your Dataset)

Your \`amp.config.ts\` defines decoded tables. Query them:

\`\`\`bash
# Get decoded events (requires TypeScript dataset to be running)
curl -X POST http://localhost:1603 --data "
  SELECT event_id, sender, value, message
  FROM ${answers.datasetName}.events
  LIMIT 10"

# Aggregate by message
curl -X POST http://localhost:1603 --data "
  SELECT message, COUNT(*) as count
  FROM ${answers.datasetName}.events
  GROUP BY message
  ORDER BY count DESC"
\`\`\`

### 4. Joining Tables

\`\`\`bash
# Join logs with transactions
curl -X POST http://localhost:1603 --data "
  SELECT
    l.block_num,
    l.log_index,
    t.tx_hash,
    t.from,
    t.gas_used
  FROM anvil_rpc.logs l
  JOIN anvil_rpc.transactions t ON l.tx_hash = t.tx_hash
  LIMIT 10"

# Join with blocks
curl -X POST http://localhost:1603 --data "
  SELECT
    b.block_num,
    b.timestamp,
    COUNT(l.log_index) as event_count
  FROM anvil_rpc.blocks b
  LEFT JOIN anvil_rpc.logs l ON b.hash = l.block_hash
  GROUP BY b.block_num, b.timestamp
  ORDER BY b.block_num"
\`\`\`

### 5. Advanced: Using Amp UDFs

Amp provides custom SQL functions for Ethereum data:

\`\`\`bash
# Get event topic hash
curl -X POST http://localhost:1603 --data "
  SELECT evm_topic('DataEmitted(uint256,address,uint256,string)') as topic_hash"

# Decode event manually
curl -X POST http://localhost:1603 --data "
  SELECT
    block_num,
    evm_decode_log(
      topic1,
      topic2,
      topic3,
      data,
      'DataEmitted(uint256 indexed id, address indexed sender, uint256 value, string message)'
    ) as decoded
  FROM anvil_rpc.logs
  WHERE topic0 = evm_topic('DataEmitted(uint256,address,uint256,string)')
  LIMIT 5"
\`\`\`

### 6. Performance: Working with All 500 Events

\`\`\`bash
# Count all events by block
curl -X POST http://localhost:1603 --data "
  SELECT block_num, COUNT(*) as events
  FROM anvil_rpc.logs
  GROUP BY block_num"

# Find blocks with most events
curl -X POST http://localhost:1603 --data "
  SELECT block_num, COUNT(*) as event_count
  FROM anvil_rpc.logs
  GROUP BY block_num
  ORDER BY event_count DESC
  LIMIT 10"
\`\`\`

---

## Project Structure

\`\`\`
.
├── amp.config.ts          # TypeScript dataset (defines decoded event tables)
├── amp.toml               # Amp daemon configuration
├── manifests/
│   └── anvil_rpc.json     # Raw dataset definition (blocks, logs, transactions)
├── providers/
│   └── anvil.toml         # Anvil RPC endpoint config
├── data/                  # Parquet files (auto-created during extraction)
├── contracts/
│   ├── src/
│   │   └── EventEmitter.sol    # Sample contract (emits 500 events)
│   ├── script/
│   │   └── EventEmitter.s.sol  # Deployment script
│   └── foundry.toml
└── README.md              # This file
\`\`\`

## How Amp Works: The Data Flow

Understanding the architecture helps you extend this template:

1. **Data Source** → Anvil local blockchain running on port 8545
2. **Provider** → \`providers/anvil.toml\` tells Amp how to connect
3. **Raw Dataset** → \`manifests/anvil_rpc.json\` defines extraction schema
4. **Extraction** → \`ampd\` pulls data and stores as Parquet files in \`data/\`
5. **SQL Dataset** → \`amp.config.ts\` transforms raw data (decodes events)
6. **Query** → Query via HTTP (port 1603) or Arrow Flight (port 1602)

**Key Insight**: Raw data (blocks, logs, transactions) is extracted once. Your SQL views in \`amp.config.ts\` transform it on-the-fly during queries.

## The Sample Contract

The \`EventEmitter.sol\` contract emits one event type:

\`\`\`solidity
event DataEmitted(
  uint256 indexed id,      // Sequential ID (0-499)
  address indexed sender,  // Event sender
  uint256 value,           // Random value
  string message           // Rotating message
);
\`\`\`

The deployment script emits 500 events with varied data to demonstrate:
- Filtering by indexed fields (\`id\`, \`sender\`)
- Decoding non-indexed fields (\`value\`, \`message\`)
- Aggregating across blocks
- Joining with transaction data

## Next Steps

### 1. Deploy Your Own Contract

Replace the sample contract:

\`\`\`bash
# Add your contract to contracts/src/YourContract.sol
# Create deployment script in contracts/script/YourContract.s.sol
forge script script/YourContract.s.sol --broadcast --rpc-url http://localhost:8545
\`\`\`

### 2. Extract Your Events

Update \`amp.config.ts\`:

\`\`\`typescript
const myEvent = event("MyEvent(address indexed user, uint256 amount)")

export default defineDataset(() => ({
  // ... existing config
  tables: {
    my_events: {
      sql: \`
        SELECT
          e.block_num,
          e.event['user'] as user,
          e.event['amount'] as amount
        FROM (\${myEvent}) as e
      \`,
    },
  },
}))
\`\`\`

### 3. Move to Testnet/Mainnet

When ready to work with real networks:

1. Update \`providers/\` with real RPC endpoints
2. Change \`network\` in manifests
3. Adjust \`start_block\` to avoid extracting entire chain
4. Use \`finalized_blocks_only: true\` for production

### 4. Explore Advanced Features

- **Custom UDFs** - Write JavaScript functions in your dataset
- **Dependencies** - Build datasets on top of other datasets
- **Materialization** - Cache computed results for performance
- **Real-time streaming** - Subscribe to new data as it arrives

## Troubleshooting

**No events showing up?**
\`\`\`bash
# Check Anvil is running
curl -X POST http://localhost:8545 -H "Content-Type: application/json" -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

# Check ampd extracted data
ls -lah data/

# Re-deploy contract if needed
cd contracts && forge script script/EventEmitter.s.sol --broadcast --rpc-url http://localhost:8545
\`\`\`

**ampd not found?**
\`\`\`bash
curl --proto '=https' --tlsv1.2 -sSf https://ampup.sh/install | sh
# Restart terminal
\`\`\`

**Port conflicts?**
- Anvil: 8545
- Amp Admin: 1610
- Amp Arrow Flight: 1602
- Amp JSON Lines: 1603
- Amp Studio: 1615 (if using \`amp studio\`)

Stop conflicting services or change ports in config files.

**Want to start fresh?**
\`\`\`bash
# Stop ampd (Ctrl+C) and Anvil (Ctrl+C)
rm -rf data/          # Delete extracted data
rm -rf contracts/out/ # Delete compiled contracts
anvil                 # Restart Anvil
# Re-deploy contracts and re-register dataset
\`\`\`

## Learn More

- **Amp Documentation** - See \`docs/\` in the Amp repository
- **Foundry Book** - https://book.getfoundry.sh
- **DataFusion SQL** - https://datafusion.apache.org/user-guide/sql/

---

**Questions or issues?** Open an issue at https://github.com/edgeandnode/amp
`,

    "contracts/foundry.toml": `[profile.default]
src = "src"
out = "out"
libs = ["lib"]
solc_version = "0.8.20"

# See more config options https://github.com/foundry-rs/foundry/blob/master/crates/config/README.md#all-options
`,

    "contracts/src/EventEmitter.sol": `// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * @title EventEmitter
 * @notice Sample contract that emits 500 events for Amp demonstration
 * @dev This contract is designed to generate diverse event data for learning Amp's query capabilities
 */
contract EventEmitter {
    event DataEmitted(
        uint256 indexed id,
        address indexed sender,
        uint256 value,
        string message
    );

    /**
     * @notice Emits a batch of events
     * @param start Starting ID
     * @param count Number of events to emit
     */
    function emitBatch(uint256 start, uint256 count) external {
        for (uint256 i = 0; i < count; i++) {
            uint256 eventId = start + i;

            // Generate varied data for interesting queries
            uint256 value = (eventId * 123) % 1000; // Pseudo-random values 0-999
            string memory message = _getMessage(eventId % 5);

            emit DataEmitted(eventId, msg.sender, value, message);
        }
    }

    /**
     * @notice Returns a message based on index (creates 5 categories)
     */
    function _getMessage(uint256 index) internal pure returns (string memory) {
        if (index == 0) return "Action: Transfer";
        if (index == 1) return "Action: Mint";
        if (index == 2) return "Action: Burn";
        if (index == 3) return "Action: Swap";
        return "Action: Stake";
    }
}
`,

    "contracts/script/EventEmitter.s.sol": `// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import {Script} from "forge-std/Script.sol";
import {EventEmitter} from "../src/EventEmitter.sol";

/**
 * @title EventEmitter Deployment Script
 * @notice Deploys EventEmitter and emits 500 events in batches
 * @dev Batches events to avoid gas limits and demonstrate multi-block extraction
 */
contract Deploy is Script {
    // Emit events in batches to spread across multiple blocks
    uint256 constant BATCH_SIZE = 50;
    uint256 constant TOTAL_EVENTS = 500;

    function run() external {
        vm.startBroadcast();

        EventEmitter emitter = new EventEmitter();

        // Emit 500 events in batches of 50 (10 batches total)
        // This spreads events across multiple blocks for more interesting queries
        for (uint256 batch = 0; batch < TOTAL_EVENTS / BATCH_SIZE; batch++) {
            emitter.emitBatch(batch * BATCH_SIZE, BATCH_SIZE);
        }

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
}
