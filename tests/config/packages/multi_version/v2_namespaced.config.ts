import { defineDataset } from "@edgeandnode/amp"

export default defineDataset(() => ({
  namespace: "test_namespace",
  name: "multi_version",
  network: "mainnet",
  version: "0.0.2",
  dependencies: {
    multi_version: "_/multi_version@0.0.1",
  },
  tables: {
    blocks: {
      sql: "SELECT block_num, gas_limit, hash, parent_hash FROM multi_version.blocks",
      network: "mainnet",
    },
  },
  functions: {},
}))
