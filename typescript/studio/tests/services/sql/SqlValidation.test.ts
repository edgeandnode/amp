/**
 * SQL Validator Test Suite
 *
 * Comprehensive tests for SQL validation functionality including:
 * - Table/column reference validation
 * - Syntax validation (parentheses, keywords)
 * - Position mapping accuracy
 * - Error message quality and suggestions
 * - Validation levels (basic, standard, full)
 * - Performance and caching
 *
 * @file SqlValidator.test.ts
 */

import { MarkerSeverity } from "monaco-editor/esm/vs/editor/editor.api"
import type { DatasetSource } from "studio-cli/Studio/Model"
import { afterEach, beforeEach, describe, expect, test } from "vitest"

import { SqlValidation } from "../../../src/services/sql/SqlValidation.ts"
import type { CompletionConfig, UserDefinedFunction } from "../../../src/services/sql/types.ts"
import { mockMetadata } from "./fixtures/mockMetadata.ts"
import { mockUDFs } from "./fixtures/mockUDFs.ts"

// Use mock data directly - SqlValidator expects DatasetSource format
const testDatasets = [...mockMetadata] as Array<DatasetSource>
const testUdfs: Array<UserDefinedFunction> = [...mockUDFs]

describe("SqlValidator", () => {
  let validator: SqlValidation
  let config: CompletionConfig

  beforeEach(() => {
    config = {
      minPrefixLength: 0,
      maxSuggestions: 50,
      enableSnippets: true,
      enableContextFiltering: true,
      enableAliasResolution: true,
      contextCacheTTL: 30 * 1000,
      enableDebugLogging: false,
      enableSqlValidation: true,
      validationLevel: "full",
      enablePartialValidation: true,
    }

    validator = new SqlValidation(testDatasets, testUdfs, config)
  })

  afterEach(() => {
    validator.dispose()
  })

  describe("Table Validation", () => {
    test("should validate valid table references", () => {
      const query = "SELECT * FROM anvil.logs"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should detect unknown table names", () => {
      const query = "SELECT * FROM unknown_table"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(1)
      expect(errors[0].code).toBe("UNKNOWN_TABLE")
      expect(errors[0].message).toContain("unknown_table")
      expect(errors[0].severity).toBe(MarkerSeverity.Error)
    })

    test("should suggest similar table names", () => {
      const query = "SELECT * FROM anvil.log" // Missing 's'
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(1)
      expect(errors[0].code).toBe("UNKNOWN_TABLE")
      expect(errors[0].message).toContain("Did you mean")
      expect(errors[0].message).toContain("anvil.logs")
      expect(errors[0].data?.suggestion).toBe("anvil.logs")
    })

    test("should handle multiple table references", () => {
      const query = `
        SELECT l.*, t.hash 
        FROM anvil.logs as l
        JOIN anvil.transactions as t ON l.transaction_hash = t.hash
      `
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should handle ORDER BY with ASC/DESC keywords", () => {
      const query = "SELECT block_number FROM anvil.logs ORDER BY address DESC"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should handle ORDER BY with multiple columns and directions", () => {
      const query = "SELECT block_number FROM anvil.logs ORDER BY address ASC, block_number DESC"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should handle ORDER BY with NULLS FIRST/LAST", () => {
      const query = "SELECT block_number FROM anvil.logs ORDER BY address DESC NULLS FIRST"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should handle column aliases with AS keyword", () => {
      const query = "SELECT block_number AS block_num FROM anvil.logs"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should handle column aliases without AS keyword", () => {
      const query = "SELECT block_number block_num FROM anvil.logs"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should handle table aliases with AS keyword", () => {
      const query = "SELECT l.block_number FROM anvil.logs AS l"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should handle table aliases without AS keyword", () => {
      const query = "SELECT l.block_number FROM anvil.logs l"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should handle complex query with multiple aliases", () => {
      const query = `
        SELECT 
          l.block_number AS block_num,
          l.address addr,
          t.hash AS tx_hash
        FROM anvil.logs AS l
        JOIN anvil.transactions t ON l.transaction_hash = t.hash
      `
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should resolve table aliases in qualified column references", () => {
      const query = "SELECT c.block_number FROM anvil.logs AS c"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should resolve table aliases without AS keyword in qualified column references", () => {
      const query = "SELECT c.block_number FROM anvil.logs c"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should correctly report column not found when using alias with non-existent column", () => {
      const query = "SELECT c.block_hash FROM anvil.logs AS c"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(1)
      expect(errors[0].code).toBe("COLUMN_NOT_FOUND")
      expect(errors[0].message).toContain("Column 'block_hash' not found in table 'anvil.logs'")
      // Should NOT say "Table 'c' not found" - the alias should be resolved
      expect(errors[0].message).not.toContain("Table 'c' not found")
    })

    test("should handle subquery aliases without validation errors", () => {
      const query = `
        SELECT c.block_hash, c.event 
        FROM (
          SELECT block_number, address, evm_decode_log(topics, data, 'Count(uint256 count)') as event
          FROM anvil.logs
        ) AS c
      `
      const errors = validator.validateQuery(query)

      // Should not report errors for subquery columns since we can't validate them properly yet
      expect(errors).toHaveLength(0)
    })

    test("should detect multiple unknown tables", () => {
      const query = `
        SELECT * 
        FROM unknown_table1 u1
        JOIN unknown_table2 u2 ON u1.id = u2.id
      `
      const errors = validator.validateQuery(query)

      const unknownTableErrors = errors.filter((e) => e.code === "UNKNOWN_TABLE")
      expect(unknownTableErrors.length).toBeGreaterThanOrEqual(2)
    })
  })

  describe("Column Validation", () => {
    test("should validate valid column references", () => {
      const query = "SELECT block_number, address FROM anvil.logs"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should detect unknown column names", () => {
      const query = "SELECT unknown_column FROM anvil.logs"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(1)
      expect(errors[0].code).toBe("COLUMN_NOT_FOUND")
      expect(errors[0].message).toContain("unknown_column")
      expect(errors[0].severity).toBe(MarkerSeverity.Error)
    })

    test("should suggest similar column names", () => {
      const query = "SELECT block_num FROM anvil.logs" // Should suggest 'block_number'
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(1)
      expect(errors[0].code).toBe("COLUMN_NOT_FOUND")
      expect(errors[0].message).toContain("Did you mean")
      expect(errors[0].data?.suggestion).toBe("block_number")
    })

    test("should validate qualified column references", () => {
      const query = "SELECT anvil.logs.block_number FROM anvil.logs"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should detect ambiguous column references", () => {
      const query = `
        SELECT block_number 
        FROM anvil.logs l
        JOIN anvil.transactions t ON l.transaction_hash = t.hash
      `
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(1)
      expect(errors[0].code).toBe("AMBIGUOUS_COLUMN")
      expect(errors[0].severity).toBe(MarkerSeverity.Warning)
      expect(errors[0].message).toContain("ambiguous")
    })

    test("should list available columns in error messages", () => {
      const query = "SELECT invalid_col FROM anvil.logs"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(1)
      expect(errors[0].message).toContain("Available columns:")
      expect(errors[0].data?.availableColumns).toEqual(testDatasets[0].metadata_columns.map((col) => col.name))
    })
  })

  describe("Syntax Validation", () => {
    test("should validate balanced parentheses", () => {
      const query = "SELECT * FROM anvil.logs WHERE (block_number > 100)"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should detect unmatched opening parentheses", () => {
      const query = "SELECT * FROM anvil.logs WHERE (block_number > 100"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(1)
      expect(errors[0].code).toBe("UNMATCHED_OPENING_PAREN")
      expect(errors[0].severity).toBe(MarkerSeverity.Error)
    })

    test("should detect unmatched closing parentheses", () => {
      const query = "SELECT * FROM anvil.logs WHERE block_number > 100)"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(1)
      expect(errors[0].code).toBe("UNMATCHED_CLOSING_PAREN")
      expect(errors[0].severity).toBe(MarkerSeverity.Error)
    })

    test("should handle nested parentheses", () => {
      const query = "SELECT * FROM anvil.logs WHERE ((block_number > 100) AND (address IS NOT NULL))"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(0)
    })

    test("should detect missing FROM clause", () => {
      const query = "SELECT block_number"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(2) // second error is a COLUMN_NOT_FOUND since FROM table is not defined
      expect(errors[0].code).toBe("MISSING_FROM_CLAUSE")
      expect(errors[0].severity).toBe(MarkerSeverity.Warning)
    })
  })

  describe("Validation Levels", () => {
    test("basic level should only validate syntax", () => {
      const basicConfig = { ...config, validationLevel: "basic" as const }
      const basicValidator = new SqlValidation(testDatasets, testUdfs, basicConfig)

      const query = "SELECT unknown_column FROM unknown_table WHERE (unclosed_paren"
      const errors = basicValidator.validateQuery(query)

      // Should only find syntax errors, not table/column errors
      expect(errors.length).toBeGreaterThan(0)
      expect(errors.every((e) => e.code?.includes("PAREN") || e.code?.includes("CLAUSE"))).toBe(true)

      basicValidator.dispose()
    })

    test("standard level should validate syntax and tables", () => {
      const standardConfig = { ...config, validationLevel: "standard" as const }
      const standardValidator = new SqlValidation(testDatasets, testUdfs, standardConfig)

      const query = "SELECT unknown_column FROM unknown_table WHERE (unclosed_paren"
      const errors = standardValidator.validateQuery(query)

      // Should find syntax and table errors, but not column errors
      const errorCodes = errors.map((e) => e.code)
      expect(errorCodes).toContain("UNKNOWN_TABLE")
      expect(errorCodes.some((code) => code?.includes("PAREN"))).toBe(true)
      expect(errorCodes).not.toContain("COLUMN_NOT_FOUND")

      standardValidator.dispose()
    })

    test("full level should validate everything", () => {
      const query = "SELECT unknown_column FROM unknown_table WHERE (unclosed_paren"
      const errors = validator.validateQuery(query) // Using full level validator

      // Should find all types of errors
      const errorCodes = errors.map((e) => e.code)
      expect(errorCodes).toContain("UNKNOWN_TABLE")
      expect(errorCodes).toContain("COLUMN_NOT_FOUND")
      expect(errorCodes.some((code) => code?.includes("PAREN"))).toBe(true)
    })

    test("should skip validation when disabled", () => {
      const disabledConfig = { ...config, enableSqlValidation: false }
      const disabledValidator = new SqlValidation(testDatasets, testUdfs, disabledConfig)

      const query = "SELECT unknown_column FROM unknown_table WHERE (unclosed_paren"
      const errors = disabledValidator.validateQuery(query)

      expect(errors).toHaveLength(0)

      disabledValidator.dispose()
    })
  })

  describe("Position Mapping", () => {
    test("should provide accurate line and column positions", () => {
      const query = `SELECT *
FROM unknown_table
WHERE invalid_column = 1`
      const errors = validator.validateQuery(query)

      expect(errors.length).toBeGreaterThan(0)

      // Check that positions are reasonable (not just defaults)
      for (const error of errors) {
        expect(error.startLineNumber).toBeGreaterThan(0)
        expect(error.startColumn).toBeGreaterThan(0)
        expect(error.endLineNumber).toBeGreaterThanOrEqual(error.startLineNumber)
        expect(error.endColumn).toBeGreaterThanOrEqual(error.startColumn)
      }
    })

    test("should handle multi-line queries correctly", () => {
      const query = `
        SELECT 
          block_number,
          unknown_column
        FROM anvil.logs
        WHERE (unmatched_paren = 1
      `
      const errors = validator.validateQuery(query)

      expect(errors.length).toBeGreaterThan(0)

      // Verify that line numbers correspond to actual error locations
      const columnError = errors.find((e) => e.code === "COLUMN_NOT_FOUND")
      const parenError = errors.find((e) => e.code?.includes("PAREN"))

      if (columnError) {
        expect(columnError.startLineNumber).toBe(4) // Line with unknown_column
      }

      if (parenError) {
        expect(parenError.startLineNumber).toBe(6) // Line with unmatched parenthesis
      }
    })
  })

  describe("Performance and Caching", () => {
    test("should cache validation results", () => {
      const query = "SELECT * FROM anvil.logs"

      // First validation
      const errors1 = validator.validateQuery(query)
      let metrics = validator.getMetrics()
      expect(metrics.totalValidations).toBe(1)
      expect(metrics.cacheHits).toBe(0)

      // Second validation (should be cached)
      const errors2 = validator.validateQuery(query)
      metrics = validator.getMetrics()

      // Results should be identical
      expect(errors1).toEqual(errors2)

      // Cache metrics should show hit
      expect(metrics.totalValidations).toBe(2)
      expect(metrics.cacheHits).toBe(1)

      // Third validation (should also be cached)
      const errors3 = validator.validateQuery(query)
      metrics = validator.getMetrics()

      expect(errors2).toEqual(errors3)
      expect(metrics.totalValidations).toBe(3)
      expect(metrics.cacheHits).toBe(2)
    })

    test("should clear cache when data is updated", () => {
      const query = "SELECT * FROM anvil.logs"

      // Initial validation
      validator.validateQuery(query)
      let metrics = validator.getMetrics()
      expect(metrics.totalValidations).toBe(1)

      // Update data (should clear cache)
      validator.updateData(testDatasets, testUdfs)

      // Validate again (should not use cache)
      validator.validateQuery(query)
      metrics = validator.getMetrics()
      expect(metrics.totalValidations).toBe(2)
      expect(metrics.cacheHits).toBe(0)
    })

    test("should handle large queries efficiently", () => {
      const largeQuery = `
        SELECT 
          ${testDatasets[0].metadata_columns.map((col) => col.name).join(",\n          ")}
        FROM anvil.logs
        WHERE block_number > 100
          AND transaction_hash IS NOT NULL
          AND address IN ('0x123', '0x456', '0x789')
          AND topics[1] = '0xabc'
          AND data IS NOT NULL
      `

      const startTime = performance.now()
      const errors = validator.validateQuery(largeQuery)
      const duration = performance.now() - startTime

      expect(errors).toHaveLength(0)
      expect(duration).toBeLessThan(500) // Relaxed threshold for CI stability
    })

    test("should demonstrate cache performance benefit", () => {
      const complexQuery = `
        SELECT l.block_number, l.address, t.hash
        FROM anvil.logs l
        JOIN anvil.transactions t ON l.transaction_hash = t.hash
        WHERE l.block_number > 100
      `

      // Validate multiple times and ensure consistent results
      const results = []
      const iterations = 5

      for (let i = 0; i < iterations; i++) {
        results.push(validator.validateQuery(complexQuery))
      }

      // All results should be identical
      for (let i = 1; i < iterations; i++) {
        expect(results[i]).toEqual(results[0])
      }

      // Cache should have been hit for iterations 2-5
      const metrics = validator.getMetrics()
      expect(metrics.totalValidations).toBe(iterations)
      expect(metrics.cacheHits).toBe(iterations - 1)
    })
  })

  describe("Error Recovery", () => {
    test("should handle empty queries gracefully", () => {
      const errors = validator.validateQuery("")
      expect(errors).toHaveLength(0)
    })

    test("should handle whitespace-only queries", () => {
      const errors = validator.validateQuery("   \n\t  ")
      expect(errors).toHaveLength(0)
    })

    test("should not crash on malformed queries", () => {
      const malformedQueries = [
        "SELECT ;;; FROM ###",
        "INVALID QUERY WITH !@#$ CHARS",
        "SELECT * FROM ); DROP TABLE users; --",
        "\\x00\\xFF\\x01 BINARY DATA",
        "SELECT * FROM table WHERE column = 'unterminated string",
      ]

      for (const query of malformedQueries) {
        expect(() => validator.validateQuery(query)).not.toThrow()
        const errors = validator.validateQuery(query)
        expect(Array.isArray(errors)).toBe(true)
      }
    })
  })

  describe("String Similarity", () => {
    test("should calculate string similarity correctly", () => {
      // Access private method through casting for testing
      const calculateSimilarity = (validator as any).calculateStringSimilarity.bind(validator)

      // Exact matches
      expect(calculateSimilarity("test", "test")).toBe(1)

      // Complete mismatches
      expect(calculateSimilarity("abc", "xyz")).toBeLessThan(0.5)

      // Similar strings
      expect(calculateSimilarity("block_number", "block_num")).toBeGreaterThan(0.7)
      expect(calculateSimilarity("anvil.logs", "anvil.log")).toBeGreaterThan(0.8)

      // Case insensitive
      expect(calculateSimilarity("test", "TEST")).toBeLessThan(1) // Different case
      expect(calculateSimilarity("test", "test")).toBe(1) // Same case
    })
  })

  describe("Integration Scenarios", () => {
    test("scenario: basic table validation", () => {
      const query = "SELECT * FROM users WHERE id = 1"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(2) // second error is COLUMN_NOT_FOUND for id
      expect(errors[0].code).toBe("UNKNOWN_TABLE")
      expect(errors[0].message).toContain("users")
      expect(errors[0].data?.suggestion).toBeDefined()
      expect(errors[1].code).toBe("COLUMN_NOT_FOUND")
      expect(errors[1].message).toContain("id")
    })

    test("scenario: column validation with qualified names", () => {
      const query = "SELECT l.username FROM anvil.logs l"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(1)
      expect(errors[0].code).toBe("COLUMN_NOT_FOUND")
      expect(errors[0].message).toContain("username")
      expect(errors[0].message).toContain("anvil.logs")
    })

    test("scenario: syntax error with parentheses", () => {
      const query = "SELECT * FROM anvil.logs WHERE (block_number > 100"
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(1)
      expect(errors[0].code).toBe("UNMATCHED_OPENING_PAREN")
      expect(errors[0].startLineNumber).toBe(1)
    })

    test("scenario: multiple errors in complex query", () => {
      const query = `
        SELECT invalid_col, another_bad 
        FROM fake_table 
        WHERE (unmatched = 1
      `
      const errors = validator.validateQuery(query)

      expect(errors.length).toBeGreaterThanOrEqual(3)

      const errorCodes = errors.map((e) => e.code)
      expect(errorCodes).toContain("UNKNOWN_TABLE")
      expect(errorCodes).toContain("COLUMN_NOT_FOUND")
      expect(errorCodes.some((code) => code?.includes("PAREN"))).toBe(true)
    })

    test("scenario: join validation with aliases", () => {
      const query = `
        SELECT l.*, t.invalid_field
        FROM anvil.logs l
        JOIN anvil.transactions t ON l.transaction_hash = t.hash
      `
      const errors = validator.validateQuery(query)

      expect(errors).toHaveLength(1)
      expect(errors[0].code).toBe("COLUMN_NOT_FOUND")
      expect(errors[0].message).toContain("invalid_field")
    })
  })
})

describe("ValidationCache", () => {
  test("should be tested through SqlValidator integration", () => {
    // Cache functionality is tested in the Performance and Caching section above
    expect(true).toBe(true)
  })
})
