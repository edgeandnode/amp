import { defineDataset } from "nozzl";

export default defineDataset(() => ({
  name: "ampsync_example",
  network: "mainnet",
  version: "0.2.0",
  dependencies: {
    anvil: {
      owner: "graphprotocol",
      name: "anvil",
      version: "0.1.0",
    },
  },
  tables: {
    blocks: {
      sql: `SELECT * FROM anvil.blocks`,
    },
    logs: {
      sql: `SELECT * FROM anvil.logs`,
    },
    transactions: {
      sql: `SELECT * FROM anvil.transactions`,
    },
  },
}));
