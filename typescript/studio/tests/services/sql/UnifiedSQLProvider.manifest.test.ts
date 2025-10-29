/**
 * Integration tests for UnifiedSQLProvider with DatasetManifest support
 * Tests manifest initialization and validation
 */

import type { DatasetManifest } from "@edgeandnode/amp/Model"
import { afterEach, describe, expect, test } from "vitest"

import { convertManifestToMetadata, mergeMetadataSources } from "../../../src/services/sql/manifestConverter"
import { SqlValidation } from "../../../src/services/sql/SqlValidation"
import { UnifiedSQLProvider } from "../../../src/services/sql/UnifiedSQLProvider"

import { mockMetadata, mockUDFs } from "./fixtures"

describe("UnifiedSQLProvider with DatasetManifest", () => {
  let provider: UnifiedSQLProvider | null = null

  afterEach(() => {
    provider?.dispose()
    provider = null
  })

  describe("Manifest Integration", () => {
    test("should initialize with null manifest", () => {
      provider = new UnifiedSQLProvider(
        mockMetadata,
        mockUDFs,
        {
          validationLevel: "full",
          enablePartialValidation: true,
          enableDebugLogging: false,
          minPrefixLength: 0,
          maxSuggestions: 50,
        },
        null, // No manifest
      )

      expect(provider).toBeDefined()
      // Provider should initialize successfully without manifest
      expect(provider.getValidator()).toBeDefined()
    })

    test("should initialize with a valid manifest", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
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
                  { name: "timestamp", type: { Timestamp: ["Nanosecond", null] }, nullable: false },
                ],
              },
            },
          },
        },
        functions: {},
      }

      provider = new UnifiedSQLProvider(
        mockMetadata,
        mockUDFs,
        {
          validationLevel: "full",
          enablePartialValidation: true,
          enableDebugLogging: false,
          minPrefixLength: 0,
          maxSuggestions: 50,
        },
        manifest,
      )

      expect(provider).toBeDefined()
      // Provider should initialize successfully with manifest
      expect(provider.getValidator()).toBeDefined()
    })

    test("should initialize with manifest containing multiple tables", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
        dependencies: {},
        tables: {
          table1: {
            network: "mainnet",
            input: { sql: "SELECT * FROM source1" },
            schema: {
              arrow: {
                fields: [{ name: "col1", type: "Int64", nullable: false }],
              },
            },
          },
          table2: {
            network: "mainnet",
            input: { sql: "SELECT * FROM source2" },
            schema: {
              arrow: {
                fields: [{ name: "col2", type: "Utf8", nullable: false }],
              },
            },
          },
          table3: {
            network: "mainnet",
            input: { sql: "SELECT * FROM source3" },
            schema: {
              arrow: {
                fields: [{ name: "col3", type: "Float64", nullable: false }],
              },
            },
          },
        },
        functions: {},
      }

      provider = new UnifiedSQLProvider(
        [],
        mockUDFs,
        {
          validationLevel: "full",
          enablePartialValidation: true,
          enableDebugLogging: false,
          minPrefixLength: 0,
          maxSuggestions: 50,
        },
        manifest,
      )

      // Provider should initialize successfully with multiple tables
      expect(provider).toBeDefined()
      expect(provider.getValidator()).toBeDefined()
    })

    test("should handle manifest with complex Arrow types", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
        dependencies: {},
        tables: {
          events: {
            network: "mainnet",
            input: { sql: "SELECT * FROM logs" },
            schema: {
              arrow: {
                fields: [
                  { name: "timestamp", type: { Timestamp: ["Nanosecond", "UTC"] }, nullable: false },
                  { name: "value", type: { Decimal128: [38, 18] }, nullable: false },
                  { name: "tags", type: { List: "Utf8" }, nullable: true },
                  { name: "data", type: "Binary", nullable: false },
                ],
              },
            },
          },
        },
        functions: {},
      }

      // Should not throw when initializing with complex types
      provider = new UnifiedSQLProvider(
        [],
        mockUDFs,
        {
          validationLevel: "full",
          enablePartialValidation: true,
          enableDebugLogging: false,
          minPrefixLength: 0,
          maxSuggestions: 50,
        },
        manifest,
      )

      expect(provider).toBeDefined()
      expect(provider.getValidator()).toBeDefined()
    })

    test("should merge raw sources and manifest tables for initialization", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
        dependencies: {},
        tables: {
          processed: {
            network: "mainnet",
            input: { sql: "SELECT * FROM anvil.blocks" },
            schema: {
              arrow: {
                fields: [{ name: "processed_field", type: "Utf8", nullable: false }],
              },
            },
          },
        },
        functions: {},
      }

      provider = new UnifiedSQLProvider(
        mockMetadata,
        mockUDFs,
        {
          validationLevel: "full",
          enablePartialValidation: true,
          enableDebugLogging: false,
          minPrefixLength: 0,
          maxSuggestions: 50,
        },
        manifest,
      )

      // Should initialize without errors, merging both raw and manifest sources
      expect(provider).toBeDefined()
      expect(provider.getValidator()).toBeDefined()
    })

    test("should handle empty manifest tables", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
        dependencies: {},
        tables: {},
        functions: {},
      }

      provider = new UnifiedSQLProvider(
        mockMetadata,
        mockUDFs,
        {
          validationLevel: "full",
          enablePartialValidation: true,
          enableDebugLogging: false,
          minPrefixLength: 0,
          maxSuggestions: 50,
        },
        manifest,
      )

      // Should handle empty manifest tables gracefully
      expect(provider).toBeDefined()
      expect(provider.getValidator()).toBeDefined()
    })
  })

  describe("Validation with Manifests", () => {
    test("should get validator instance with manifest", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
        dependencies: {},
        tables: {
          users: {
            network: "testnet",
            input: { sql: "SELECT * FROM raw_users" },
            schema: {
              arrow: {
                fields: [
                  { name: "id", type: "UInt64", nullable: false },
                  { name: "email", type: "Utf8", nullable: false },
                ],
              },
            },
          },
        },
        functions: {},
      }

      provider = new UnifiedSQLProvider(
        [],
        mockUDFs,
        {
          validationLevel: "full",
          enablePartialValidation: true,
          enableDebugLogging: false,
          minPrefixLength: 0,
          maxSuggestions: 50,
        },
        manifest,
      )

      const validator = provider.getValidator()
      expect(validator).toBeDefined()
    })

    test("should validate queries with manifest tables", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
        dependencies: {},
        tables: {
          users: {
            network: "testnet",
            input: { sql: "SELECT * FROM raw_users" },
            schema: {
              arrow: {
                fields: [
                  { name: "id", type: "UInt64", nullable: false },
                  { name: "email", type: "Utf8", nullable: false },
                ],
              },
            },
          },
        },
        functions: {},
      }

      // Convert manifest to metadata and create validator directly
      const manifestMetadata = convertManifestToMetadata(manifest)
      const mergedMetadata = mergeMetadataSources([], manifestMetadata)
      const unifiedSources = mergedMetadata.map((meta) => ({
        source: meta.source,
        metadata_columns: meta.columns.map((col) => ({
          name: col.name,
          datatype: col.datatype,
        })),
      }))

      const validator = new SqlValidation(unifiedSources as any, mockUDFs, {
        validationLevel: "full",
        enablePartialValidation: true,
        enableDebugLogging: false,
        minPrefixLength: 0,
        maxSuggestions: 50,
        enableSnippets: true,
        enableContextFiltering: true,
        enableAliasResolution: true,
        contextCacheTTL: 300000,
        enableSqlValidation: true,
      })

      // Valid query - should have no errors
      const validErrors = validator.validateQuery("SELECT id, email FROM test.users")
      expect(validErrors).toHaveLength(0)

      // Invalid column - should have errors
      const invalidErrors = validator.validateQuery("SELECT invalid_column FROM test.users")
      expect(invalidErrors.length).toBeGreaterThan(0)

      validator.dispose()
    })

    test("should validate with validation level off", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
        dependencies: {},
        tables: {
          data: {
            network: "testnet",
            input: { sql: "SELECT * FROM source" },
            schema: {
              arrow: {
                fields: [{ name: "col1", type: "Int64", nullable: false }],
              },
            },
          },
        },
        functions: {},
      }

      provider = new UnifiedSQLProvider(
        [],
        mockUDFs,
        {
          validationLevel: "off",
          enablePartialValidation: false,
          enableDebugLogging: false,
          minPrefixLength: 0,
          maxSuggestions: 50,
        },
        manifest,
      )

      const validator = provider.getValidator()
      // Should be null when validation is off
      expect(validator).toBeNull()
    })

    test("should validate SELECT * FROM manifest table queries", () => {
      const manifest: DatasetManifest = {
        kind: "manifest",
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
                  { name: "timestamp", type: { Timestamp: ["Nanosecond", null] }, nullable: false },
                ],
              },
            },
          },
        },
        functions: {},
      }

      // Convert manifest to metadata and create validator directly
      const manifestMetadata = convertManifestToMetadata(manifest)
      const mergedMetadata = mergeMetadataSources([], manifestMetadata)
      const unifiedSources = mergedMetadata.map((meta) => ({
        source: meta.source,
        metadata_columns: meta.columns.map((col) => ({
          name: col.name,
          datatype: col.datatype,
        })),
      }))

      const validator = new SqlValidation(unifiedSources as any, mockUDFs, {
        validationLevel: "full",
        enablePartialValidation: true,
        enableDebugLogging: false,
        minPrefixLength: 0,
        maxSuggestions: 50,
        enableSnippets: true,
        enableContextFiltering: true,
        enableAliasResolution: true,
        contextCacheTTL: 300000,
        enableSqlValidation: true,
      })

      // This is the critical test - validates the exact scenario from the user's request
      // Query against manifest table should be valid
      const errors = validator.validateQuery("SELECT * FROM example.blocks")
      expect(errors).toHaveLength(0)

      // Query with specific columns should also be valid
      const specificColumnErrors = validator.validateQuery("SELECT block_number, hash FROM example.blocks")
      expect(specificColumnErrors).toHaveLength(0)

      // Query with invalid table should have errors
      const invalidTableErrors = validator.validateQuery("SELECT * FROM example.nonexistent")
      expect(invalidTableErrors.length).toBeGreaterThan(0)

      validator.dispose()
    })
  })
})
