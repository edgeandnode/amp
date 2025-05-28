import { defineDataset } from "nozzl"

const event = (event: string) => {
  return `
    SELECT block_num, timestamp, address, evm_decode(topic1, topic2, topic3, data, '${event}') as event
    FROM anvil.logs
    WHERE topic0 = evm_topic('${event}')
  `
}

const transfer = event("Transfer(address indexed from, address indexed to, uint256 value)")
const count = event("Count(uint256 count)")

export default defineDataset(() => ({
  name: "example",
  version: "0.1.0",
  dependencies: {
    mainnet: {
      owner: "graphprotocol",
      name: "mainnet",
      version: "0.1.0",
    },
  },
  tables: {
    counts: {
      network: "mainnet",
      sql: `
        SELECT c.address, c.block_num, c.timestamp, c.event['count'] as count
        FROM (${count}) as c`,
    },
    transfers: {
      network: "mainnet",
      sql: `
        SELECT t.block_num, t.timestamp, t.event['from'] as from, t.event['to'] as to, t.event['value'] as value
        FROM (${transfer}) as t
      `,
    },
  },

  // TODO: We should allow omitting this
  functions: {},
}))
