import { defineDataset } from "@edgeandnode/amp"

export default defineDataset(() => ({
  namespace: "test_namespace",
  name: "multi_version",
  network: "mainnet",
  version: "0.0.3",
  dependencies: {
    v2: "test_namespace/multi_version@0.0.2",
  },
  tables: {
    blocks: {
      sql: "SELECT block_num, gas_limit, hash FROM v2.blocks",
      network: "mainnet",
    },
  },
  functions: {},
}))
