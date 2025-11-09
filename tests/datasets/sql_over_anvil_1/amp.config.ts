import { defineDataset } from "@edgeandnode/amp"

export default defineDataset(() => ({
  name: "sql_over_anvil_1",
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
    transactions: {
      sql: `select block_num, tx_hash, tx_index
from anvil_rpc.transactions`,
      network: "anvil",
    },
    logs: {
      sql: `select block_num, tx_hash, log_index
from anvil_rpc.logs`,
      network: "anvil",
    },
  },
  functions: {},
}))
