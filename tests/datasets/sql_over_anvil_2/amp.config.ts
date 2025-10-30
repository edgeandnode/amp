import { defineDataset } from "@edgeandnode/amp"

export default defineDataset(() => ({
  name: "sql_over_anvil_2",
  network: "anvil",
  dependencies: {
    anvil_rpc: "_/anvil_rpc@0.0.0",
  },
  tables: {
    blocks: {
      sql: `select block_num, hash, parent_hash
from anvil_rpc.blocks`,
      network: "anvil",
    },
  },
  functions: {},
}))
