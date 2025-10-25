/**
 * Tests for DatasetManifest to Metadata Converter
 */

import type { DatasetManifest } from "@edgeandnode/amp/Model"
import { describe, expect, it } from "vitest"

import { convertManifestToMetadata, mergeMetadataSources } from "../../../src/services/sql/manifestConverter"

describe("manifestConverter", () => {
  describe("convertManifestToMetadata", () => {
    it("should convert a simple manifest with one table", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
        name: "example",
        network: "mainnet",
        version: "1.0.0",
        dependencies: {},
        tables: {
          blocks: {
            network: "mainnet",
            input: { sql: "SELECT * FROM anvil.blocks" },
            schema: {
              arrow: {
                fields: [
                  { name: "block_number", type: "UInt64", nullable: false },
                  { name: "hash", type: "Utf8", nullable: false },
                ],
              },
            },
          },
        },
        functions: {},
      }

      const result = convertManifestToMetadata(manifest)

      expect(result).toHaveLength(1)
      expect(result[0]).toEqual({
        source: "example.blocks",
        network: "mainnet",
        columns: [
          { name: "block_number", datatype: "NUMERIC(20, 0)", nullable: false },
          { name: "hash", datatype: "TEXT", nullable: false },
        ],
      })
    })

    it("should convert a manifest with multiple tables", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
        name: "example",
        network: "ethereum",
        version: "1.0.0",
        dependencies: {},
        tables: {
          blocks: {
            network: "ethereum",
            input: { sql: "SELECT * FROM anvil.blocks" },
            schema: {
              arrow: {
                fields: [{ name: "block_number", type: "UInt64", nullable: false }],
              },
            },
          },
          transactions: {
            network: "ethereum",
            input: { sql: "SELECT * FROM anvil.transactions" },
            schema: {
              arrow: {
                fields: [
                  { name: "hash", type: "Utf8", nullable: false },
                  { name: "value", type: { Decimal128: [38, 18] }, nullable: false },
                ],
              },
            },
          },
        },
        functions: {},
      }

      const result = convertManifestToMetadata(manifest)

      expect(result).toHaveLength(2)
      expect(result[0]?.source).toBe("example.blocks")
      expect(result[1]?.source).toBe("example.transactions")
    })

    it("should handle nullable and non-nullable columns", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
        name: "test",
        network: "testnet",
        version: "1.0.0",
        dependencies: {},
        tables: {
          data: {
            network: "testnet",
            input: { sql: "SELECT * FROM source" },
            schema: {
              arrow: {
                fields: [
                  { name: "required_field", type: "Int64", nullable: false },
                  { name: "optional_field", type: "Utf8", nullable: true },
                ],
              },
            },
          },
        },
        functions: {},
      }

      const result = convertManifestToMetadata(manifest)

      expect(result[0]?.columns).toEqual([
        { name: "required_field", datatype: "BIGINT", nullable: false },
        { name: "optional_field", datatype: "TEXT", nullable: true },
      ])
    })

    it("should handle complex Arrow types", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
        name: "complex",
        network: "mainnet",
        version: "1.0.0",
        dependencies: {},
        tables: {
          events: {
            network: "mainnet",
            input: { sql: "SELECT * FROM logs" },
            schema: {
              arrow: {
                fields: [
                  { name: "timestamp", type: { Timestamp: ["Nanosecond", null] }, nullable: false },
                  { name: "amount", type: { Decimal128: [38, 18] }, nullable: false },
                  { name: "tags", type: { List: "Utf8" }, nullable: true },
                ],
              },
            },
          },
        },
        functions: {},
      }

      const result = convertManifestToMetadata(manifest)

      expect(result[0]?.columns).toEqual([
        { name: "timestamp", datatype: "TIMESTAMP", nullable: false },
        { name: "amount", datatype: "NUMERIC(38, 18)", nullable: false },
        { name: "tags", datatype: "TEXT[]", nullable: true },
      ])
    })

    it("should handle empty tables object", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
        name: "empty",
        network: "mainnet",
        version: "1.0.0",
        dependencies: {},
        tables: {},
        functions: {},
      }

      const result = convertManifestToMetadata(manifest)

      expect(result).toHaveLength(0)
    })
  })

  describe("mergeMetadataSources", () => {
    it("should merge raw sources with manifest metadata", () => {
      const rawSources = [
        {
          source: "anvil.blocks",
          metadata_columns: [
            { name: "number", datatype: "int64" },
            { name: "hash", datatype: "binary" },
          ],
        },
      ]

      const manifestMetadata = [
        {
          source: "example.blocks",
          network: "mainnet",
          columns: [
            { name: "block_number", datatype: "NUMERIC(20, 0)", nullable: false },
            { name: "transaction_hash", datatype: "TEXT", nullable: false },
          ],
        },
      ]

      const result = mergeMetadataSources(rawSources, manifestMetadata)

      expect(result).toHaveLength(2)
      expect(result[0]?.source).toBe("anvil.blocks")
      expect(result[1]?.source).toBe("example.blocks")
    })

    it("should handle raw sources with no manifest metadata", () => {
      const rawSources = [
        {
          source: "anvil.blocks",
          metadata_columns: [{ name: "number", datatype: "int64" }],
        },
      ]

      const result = mergeMetadataSources(rawSources, [])

      expect(result).toHaveLength(1)
      expect(result[0]?.source).toBe("anvil.blocks")
      expect(result[0]?.columns[0]?.nullable).toBe(true) // Raw sources default to nullable
    })

    it("should handle empty raw sources with manifest metadata", () => {
      const manifestMetadata = [
        {
          source: "example.blocks",
          network: "mainnet",
          columns: [{ name: "block_number", datatype: "NUMERIC(20, 0)", nullable: false }],
        },
      ]

      const result = mergeMetadataSources([], manifestMetadata)

      expect(result).toHaveLength(1)
      expect(result[0]?.source).toBe("example.blocks")
    })

    it("should preserve column metadata from both sources", () => {
      const rawSources = [
        {
          source: "raw.table",
          metadata_columns: [
            { name: "col1", datatype: "text" },
            { name: "col2", datatype: "int64" },
          ],
        },
      ]

      const manifestMetadata = [
        {
          source: "manifest.table",
          network: "testnet",
          columns: [
            { name: "col3", datatype: "TIMESTAMP", nullable: false },
            { name: "col4", datatype: "BYTEA", nullable: true },
          ],
        },
      ]

      const result = mergeMetadataSources(rawSources, manifestMetadata)

      expect(result[0]?.columns).toHaveLength(2)
      expect(result[1]?.columns).toHaveLength(2)
      expect(result[0]?.columns[0]?.name).toBe("col1")
      expect(result[1]?.columns[0]?.name).toBe("col3")
    })

    it("should handle both empty sources", () => {
      const result = mergeMetadataSources([], [])
      expect(result).toHaveLength(0)
    })

    it("should convert raw sources with unknown network", () => {
      const rawSources = [
        {
          source: "anvil.blocks",
          metadata_columns: [{ name: "number", datatype: "int64" }],
        },
      ]

      const result = mergeMetadataSources(rawSources, [])

      expect(result[0]?.network).toBe("unknown")
    })
  })

  describe("Integration: Full Workflow", () => {
    it("should handle a complete workflow from manifest to unified metadata", () => {
      // Step 1: Create a realistic manifest
      const manifest: DatasetManifest = {
        kind: "manifest",
        name: "uniswap",
        network: "mainnet",
        version: "1.0.0",
        dependencies: {},
        tables: {
          swaps: {
            network: "mainnet",
            input: { sql: "SELECT * FROM eth_rpc.logs WHERE topic0 = 'swap'" },
            schema: {
              arrow: {
                fields: [
                  { name: "block_number", type: "UInt64", nullable: false },
                  { name: "transaction_hash", type: "Utf8", nullable: false },
                  { name: "amount0", type: { Decimal128: [38, 18] }, nullable: false },
                  { name: "amount1", type: { Decimal128: [38, 18] }, nullable: false },
                  { name: "timestamp", type: { Timestamp: ["Nanosecond", null] }, nullable: false },
                ],
              },
            },
          },
        },
        functions: {},
      }

      // Step 2: Convert manifest to metadata
      const manifestMetadata = convertManifestToMetadata(manifest)

      // Step 3: Simulate raw sources
      const rawSources = [
        {
          source: "eth_rpc.blocks",
          metadata_columns: [
            { name: "number", datatype: "int64" },
            { name: "hash", datatype: "binary" },
          ],
        },
      ]

      // Step 4: Merge both sources
      const unified = mergeMetadataSources(rawSources, manifestMetadata)

      // Assertions
      expect(unified).toHaveLength(2)

      // Check raw source
      expect(unified[0]?.source).toBe("eth_rpc.blocks")
      expect(unified[0]?.columns).toHaveLength(2)

      // Check manifest source
      expect(unified[1]?.source).toBe("uniswap.swaps")
      expect(unified[1]?.network).toBe("mainnet")
      expect(unified[1]?.columns).toHaveLength(5)
      expect(unified[1]?.columns[0]?.name).toBe("block_number")
      expect(unified[1]?.columns[0]?.datatype).toBe("NUMERIC(20, 0)")
      expect(unified[1]?.columns[0]?.nullable).toBe(false)
    })
  })
})
