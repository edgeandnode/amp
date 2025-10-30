import { defineDataset } from "@edgeandnode/amp"

export default defineDataset(() => ({
  name: "non_incremental",
  network: "mainnet",
  dependencies: {
    eth_rpc: "_/eth_rpc@0.0.0",
  },
  tables: {
    // This table uses a MAX aggregation which is a non-incremental operation
    max_gas_used: {
      sql: `
        SELECT max(b.gas_used) FROM eth_rpc.blocks b
      `,
      network: "mainnet",
    },
  },
  functions: {},
}))
