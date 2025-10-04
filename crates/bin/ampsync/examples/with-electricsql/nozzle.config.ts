import { defineDataset } from "nozzl";

export default defineDataset(() => ({
  name: "ampsync_example",
  network: "mainnet",
  version: "0.2.6",
  dependencies: {
    anvil: {
      owner: "graphprotocol",
      name: "anvil",
      version: "0.1.0",
    },
  },
  tables: {
    blocks: {
      sql: `SELECT block_num, timestamp, hash, nonce, miner FROM anvil.blocks`,
    },
    logs: {
      sql: `SELECT * FROM anvil.logs`,
    },
    transactions: {
      sql: `SELECT * FROM anvil.transactions`,
    },
  },
}));
