/**
 * Mock metadata for testing SQL intellisense features
 * Provides realistic test data representing Amp dataset metadata
 */

import type { DatasetSource } from "studio-cli/Studio/Model"

export const mockMetadata: ReadonlyArray<DatasetSource & { destination: string }> = [
  {
    source: "anvil.logs",
    destination: "anvil_logs",
    metadata_columns: [
      { name: "address", datatype: "address" },
      { name: "topics", datatype: "Uint32Array" },
      { name: "data", datatype: "bytes" },
      { name: "block_number", datatype: "bigint" },
      { name: "transaction_hash", datatype: "bytes32" },
      { name: "log_index", datatype: "int" },
      { name: "transaction_index", datatype: "int" },
    ],
  },
  {
    source: "anvil.transactions",
    destination: "anvil_transactions",
    metadata_columns: [
      { name: "hash", datatype: "bytes32" },
      { name: "from_address", datatype: "address" },
      { name: "to_address", datatype: "address" },
      { name: "value", datatype: "bigint" },
      { name: "gas", datatype: "bigint" },
      { name: "gas_price", datatype: "bigint" },
      { name: "input", datatype: "bytes" },
      { name: "block_number", datatype: "bigint" },
      { name: "transaction_index", datatype: "int" },
      { name: "nonce", datatype: "bigint" },
    ],
  },
  {
    source: "anvil.blocks",
    destination: "anvil_blocks",
    metadata_columns: [
      { name: "number", datatype: "bigint" },
      { name: "hash", datatype: "bytes32" },
      { name: "parent_hash", datatype: "bytes32" },
      { name: "nonce", datatype: "bigint" },
      { name: "sha3_uncles", datatype: "bytes32" },
      { name: "logs_bloom", datatype: "bytes" },
      { name: "transactions_root", datatype: "bytes32" },
      { name: "state_root", datatype: "bytes32" },
      { name: "receipts_root", datatype: "bytes32" },
      { name: "miner", datatype: "address" },
      { name: "difficulty", datatype: "bigint" },
      { name: "total_difficulty", datatype: "bigint" },
      { name: "extra_data", datatype: "bytes" },
      { name: "size", datatype: "bigint" },
      { name: "gas_limit", datatype: "bigint" },
      { name: "gas_used", datatype: "bigint" },
      { name: "timestamp", datatype: "bigint" },
    ],
  },
] as const

/**
 * Minimal metadata for testing edge cases with few columns
 */
export const mockMetadataMinimal: ReadonlyArray<
  Omit<DatasetSource, "source"> & { source: string; destination: string }
> = [
  {
    source: "test.simple" as any,
    destination: "test_simple",
    metadata_columns: [
      { name: "id", datatype: "int" },
      { name: "name", datatype: "string" },
    ],
  },
] as const

/**
 * Empty metadata for testing graceful degradation
 */
export const mockMetadataEmpty: ReadonlyArray<DatasetSource> = [] as const

/**
 * Large metadata for performance testing
 */
export const mockMetadataLarge: ReadonlyArray<{
  source: string
  destination: string
  metadata_columns: Array<{ name: string; datatype: string }>
}> = Array.from({ length: 100 }, (_, i) => ({
  source: `dataset${i}.table${i}`,
  destination: `dataset${i}_table${i}`,
  metadata_columns: Array.from({ length: 50 }, (__, j) => ({
    name: `column_${j}_${i}`,
    datatype: j % 4 === 0 ? "bigint" : j % 4 === 1 ? "string" : j % 4 === 2 ? "address" : "bytes32",
  })),
}))

/**
 * Helper function to get metadata by table name
 */
export function getMetadataByTableName(
  metadata: ReadonlyArray<DatasetSource>,
  tableName: string,
): DatasetSource | undefined {
  return metadata.find((m) => m.source === tableName)
}

/**
 * Helper function to get all column names from a dataset
 */
export function getAllColumnNames(metadata: ReadonlyArray<DatasetSource>): Array<string> {
  return metadata.flatMap((dataset) => dataset.metadata_columns.map((col) => col.name))
}

/**
 * Helper function to get all table names
 */
export function getAllTableNames(metadata: ReadonlyArray<DatasetSource>): Array<string> {
  return metadata.map((dataset) => dataset.source)
}
