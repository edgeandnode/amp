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
    FROM anvil.logs
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
    anvil: "_/anvil@0.1.0",
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

- [Foundry](https://book.getfoundry.sh/getting-started/installation) - For deploying contracts
- [Anvil](https://book.getfoundry.sh/anvil/) - Local Ethereum node (comes with Foundry)
- Node.js 18+ - For running Amp CLI

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
forge script script/Deploy.s.sol \
  --broadcast \
  --rpc-url http://localhost:8545 \
  --sender 0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266 \
  --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
\`\`\`

This deploys the Counter contract and generates some test events (3 Count events + 2 Transfer events).

### 3. Start Amp development server

Start the Amp development server with hot-reloading:

\`\`\`bash
amp dev
\`\`\`

This will:
- Start extracting data from Anvil
- Enable hot-reloading when you modify \`amp.config.ts\`
- Expose query interfaces (Arrow Flight on 1602, JSON Lines on 1603, Admin API on 1610)

### 4. Query Your Data

Query your dataset using the Amp CLI:

\`\`\`bash
# See all count events
amp query "SELECT * FROM ${answers.datasetName}.counts LIMIT 10"

# See all transfer events
amp query "SELECT * FROM ${answers.datasetName}.transfers LIMIT 10"

# Query the raw anvil logs
amp query "SELECT * FROM anvil.logs LIMIT 10"
\`\`\`

Or open the Amp Studio in your browser for an interactive query playground:

\`\`\`bash
amp studio
\`\`\`

## Project structure

\`\`\`
.
├── amp.config.ts          # Dataset configuration (TypeScript)
│                          # Defines your dataset: ${answers.datasetName}@${answers.datasetVersion || "0.1.0"}
│
├── contracts/             # Sample Foundry project
│   ├── src/
│   │   └── Counter.sol    # Sample contract with Count + Transfer events
│   ├── script/
│   │   └── Deploy.s.sol   # Deployment script
│   └── foundry.toml       # Foundry configuration
│
├── README.md              # This file
└── .gitignore            # Git ignore rules

Note: The \`data/\` directory will be created automatically when you run \`amp dev\`
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
- **\`anvil\`** (_/anvil@0.1.0) - Provides raw blockchain data (blocks, transactions, logs)

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
2. Deploy contracts to generate events: \`cd contracts && forge script script/Deploy.s.sol --broadcast --rpc-url http://localhost:8545\`
3. Check logs: \`amp dev --logs debug\`

**Contract deployment failed?**
- Ensure Anvil is running
- Try restarting Anvil
- Check the RPC URL matches: \`http://localhost:8545\`

**Want to start fresh?**
\`\`\`bash
# Stop amp dev (Ctrl+C)
# Stop Anvil (Ctrl+C)
# Delete data directory
rm -rf data/
# Restart Anvil
anvil
# Restart amp dev
amp dev
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

    "contracts/script/Deploy.s.sol": `// SPDX-License-Identifier: MIT
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
      yield* run(
        "forge install foundry-rs/forge-std@v1.9.6",
        "Failed to install Foundry dependencies. You can do this manually:\n  cd contracts && forge install foundry-rs/forge-std@v1.9.6",
      )

      yield* Console.log("Building contracts")
      yield* run(
        "forge build",
        "Failed to build Foundry contracts. You can do this manually:\n  cd contracts && forge build",
      )

      yield* Console.log("Foundry setup complete\n")
    }),
}
