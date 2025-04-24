import { defineDataset } from "nozzl"

const event = (event: string) => {
  return `
    SELECT block_num, timestamp, evm_decode(topic1, topic2, topic3, data, '${event}') as event
    FROM firehose.logs
    WHERE topic0 = evm_topic('${event}')
  `
}

const transfer = event("Transfer(address indexed from, address indexed to, uint256 value)")

export default defineDataset(() => ({
  name: "transfers_eth_mainnet",
  version: "0.1.0",
  dependencies: {
    mainnet: {
      owner: "graphprotocol",
      name: "mainnet",
      version: "0.1.0",
    },
  },
  tables: {
    erc20_transfers: {
      sql: `
        SELECT t.block_num, t.timestamp, t.event['from'] as from, t.event['to'] as to, t.event['value'] as value
        FROM (${transfer}) as t
      `,
    },
  },
}))
