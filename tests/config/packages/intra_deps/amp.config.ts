import { defineDataset } from "@edgeandnode/amp"

export default defineDataset(() => ({
  name: "intra_deps",
  network: "mainnet",
  dependencies: {
    eth_firehose: "_/eth_firehose@0.0.0",
  },
  tables: {
    /// base table - Added zz prefix to test dump order, it should be according to the dependency order, not alphanumeric
    zz_blocks_base: {
      sql: "SELECT block_num, gas_limit, gas_used, nonce, miner, hash, parent_hash FROM eth_firehose.blocks",
      network: "mainnet",
    },
    // derived table -- Added aa prefix, it should be dumped after zz_blocks_base
    aa_blocks_derived: {
      sql: "SELECT block_num, gas_limit, gas_used, miner, hash, parent_hash FROM self.zz_blocks_base",
      network: "mainnet",
    },
    // derived table -- Added mm prefix, it should be dumped after aa_blocks_derived
    mm_blocks_derived: {
      sql: "SELECT block_num, miner, hash, parent_hash FROM self.aa_blocks_derived",
      network: "mainnet",
    },
  },
  functions: {},
}))
