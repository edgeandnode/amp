# amp_demo Dataset Fixture

Test fixture based on the [amp-demo repository](https://github.com/edgeandnode/amp-demo) for testing `eth_call` functionality and event indexing with smart contracts.

## Overview

- **Namespace**: `edgeandnode`
- **Name**: `amp-demo`
- **Network**: `anvil`
- **Dependency**: `_/anvil_rpc@0.0.0`
- **Full Reference**: `edgeandnode/amp-demo@dev`

## Smart Contract

The `Counter.sol` contract provides:

### State

- `count` (uint256): Counter value, accessible via `count()` getter (selector: `0x06661abd`)

### Events

- `Incremented(uint256 count)`: Emitted when counter is incremented
- `Decremented(uint256 count)`: Emitted when counter is decremented

### Functions

- `increment()`: Increases count by 1 and emits `Incremented`
- `decrement()`: Decreases count by 1 and emits `Decremented` (reverts if count is 0)

## Generated Tables

The dataset uses `eventTables(abi, "anvil_rpc")` to automatically generate tables from contract events:

| Table         | Source Event                 |
| ------------- | ---------------------------- |
| `incremented` | `Incremented(uint256 count)` |
| `decremented` | `Decremented(uint256 count)` |

## Testing Use Cases

This fixture enables testing:

1. **`eventTables()` SDK function**: Automatic table generation from ABI events
2. **`eth_call` UDF**: Query contract state via SQL (e.g., `eth_call('0x...', '0x06661abd')`)
3. **Event indexing**: Index `Incremented`/`Decremented` events
4. **Full integration workflow**: Deploy -> Interact -> Dump -> Query

## Building Contracts

```bash
cd contracts
forge build
```

## Foundry Scripts

- `DeployCounter.s.sol`: Deploy the Counter contract
- `IncrementCounter.s.sol`: Interact with deployed contract

## Files

```
amp_demo/
├── amp.config.ts             # Dataset configuration
├── abi.ts                    # Counter contract ABI
├── package.json              # NPM package
├── tsconfig.json             # TypeScript config
└── contracts/
    ├── foundry.toml          # Foundry config
    ├── Counter.sol           # Counter contract
    ├── DeployCounter.s.sol   # Deployment script
    ├── IncrementCounter.s.sol # Interaction script
    └── lib/forge-std/        # Foundry standard library (submodule)
```
