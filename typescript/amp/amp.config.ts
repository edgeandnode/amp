import { defineDataset } from "@edgeandnode/amp"

const event = (event: string) => {
  return `
    SELECT block_hash, tx_hash, block_num, timestamp, address, evm_decode_log(topic1, topic2, topic3, data, '${event}') as event
    FROM anvil.logs
    WHERE topic0 = evm_topic('${event}')
  `
}

const transfer = event("Transfer(address indexed from, address indexed to, uint256 value)")
const count = event("Count(uint256 count)")

export default defineDataset(() => ({
  name: "example",
  network: "anvil",
  dependencies: {
    anvil: "_/anvil@0.0.1",
  },
  tables: {
    blocks: {
      sql: `
        SELECT hash, block_num
        FROM anvil.blocks
      `,
    },
    counts: {
      sql: `
        SELECT c.block_hash, c.tx_hash, c.address, c.block_num, c.timestamp, c.event['count'] as count
        FROM (${count}) as c
      `,
    },
    transfers: {
      sql: `
        SELECT t.block_num, t.timestamp, t.event['from'] as from, t.event['to'] as to, t.event['value'] as value
        FROM (${transfer}) as t
      `,
    },
  },
}))
