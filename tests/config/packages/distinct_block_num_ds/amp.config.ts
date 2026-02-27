import { defineDataset } from "@edgeandnode/amp"

export default defineDataset(() => ({
  name: "distinct_block_num_ds",
  network: "anvil",
  dependencies: {
    anvil_rpc: "_/anvil_rpc@0.0.0",
  },
  tables: {
    // DISTINCT ON (_block_num) over a UNION ALL that produces duplicates.
    // Without DISTINCT ON this would return 2 rows per block.
    distinct_blocks: {
      sql: `SELECT DISTINCT ON (_block_num) _block_num, block_num, gas_used
FROM (
  SELECT _block_num, block_num, gas_used FROM anvil_rpc.blocks
  UNION ALL
  SELECT _block_num, block_num, gas_used FROM anvil_rpc.blocks
)`,
      network: "anvil",
    },
    // DISTINCT ON (block_num()) over a join that produces duplicates via UNION ALL.
    // parent_num comes from the join, not block_num.
    distinct_join_blocks: {
      sql: `SELECT DISTINCT ON (block_num()) block_num(), parent_num
FROM (
  SELECT child.block_num, parent.block_num AS parent_num
  FROM anvil_rpc.blocks child
  JOIN anvil_rpc.blocks parent ON child.parent_hash = parent.hash
  UNION ALL
  SELECT child.block_num, parent.block_num AS parent_num
  FROM anvil_rpc.blocks child
  JOIN anvil_rpc.blocks parent ON child.parent_hash = parent.hash
)`,
      network: "anvil",
    },
    // GROUP BY with _block_num as the first group column, aggregating duplicates
    // produced by UNION ALL. Without GROUP BY this would return 2 rows per block.
    group_by_blocks: {
      sql: `SELECT COUNT(*) AS cnt
FROM (
  SELECT _block_num, block_num FROM anvil_rpc.blocks
  UNION ALL
  SELECT _block_num, block_num FROM anvil_rpc.blocks
)
GROUP BY _block_num, block_num`,
      network: "anvil",
    },
  },
  functions: {},
}))
