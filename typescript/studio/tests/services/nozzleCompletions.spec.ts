/**
 * Unit Tests for Nozzle SQL Completions
 *
 * Tests the core completion logic including context analysis, table/column suggestions,
 * UDF completions, and keyword suggestions for Monaco Editor SQL integration.
 */

import { describe, expect, it, beforeEach, vi } from "vitest"
import type * as monaco from "monaco-editor"
import { provideNozzleCompletions } from "../../src/services/nozzleCompletions"
import type {
  DatasetMetadata,
  UserDefinedFunction,
} from "../../src/services/nozzleSQLProviders"

// Mock udfSnippets module for UDF completion tests
vi.mock("../../src/services/udfSnippets", () => ({
  createUDFSuggestions: vi.fn((monaco: any, udfs: Array<UserDefinedFunction>) =>
    udfs.map((udf, index) => ({
      label: udf.name,
      kind: 3, // Function
      detail: "Nozzle UDF",
      insertText: `${udf.name}()`,
      sortText: `3-${index.toString().padStart(3, "0")}`,
    })),
  ),
}))

describe("provideNozzleCompletions", () => {
  let mockModel: monaco.editor.ITextModel
  let mockPosition: monaco.Position
  let mockMonaco: typeof monaco
  let testMetadata: Array<DatasetMetadata>
  let testUdfs: Array<UserDefinedFunction>

  beforeEach(() => {
    // Reset all mocks
    vi.clearAllMocks()

    // Mock Monaco instance
    mockMonaco = {
      languages: {
        CompletionItemKind: {
          Class: 7,
          Property: 5,
          Function: 3,
          Keyword: 14,
        },
      },
      Position: class MockPosition {
        constructor(
          public lineNumber: number,
          public column: number,
        ) {}
      },
    } as any

    // Mock text model with common methods needed for testing
    mockModel = {
      getLineContent: vi.fn(),
      getValue: vi.fn(),
      getWordAtPosition: vi.fn(),
    } as any

    // Test data setup
    testMetadata = [
      {
        source: "eth_rpc.blocks",
        metadata_columns: [
          { name: "number", dataType: "BIGINT", description: "Block number" },
          {
            name: "timestamp",
            dataType: "BIGINT",
            description: "Block timestamp",
          },
          { name: "hash", dataType: "VARCHAR", description: "Block hash" },
        ],
      },
      {
        source: "eth_rpc.transactions",
        metadata_columns: [
          {
            name: "hash",
            dataType: "VARCHAR",
            description: "Transaction hash",
          },
          {
            name: "from_address",
            dataType: "VARCHAR",
            description: "Sender address",
          },
          {
            name: "to_address",
            dataType: "VARCHAR",
            description: "Recipient address",
          },
        ],
      },
    ]

    testUdfs = [
      {
        name: "evm_decode_log",
        description: "Decodes EVM event logs",
        sql: "evm_decode_log(topic1, topic2, topic3, data, signature)",
      },
      {
        name: "${dataset}.eth_call",
        description: "Makes eth_call RPC requests",
        sql: "${dataset}.eth_call(from, to, data, block)",
      },
    ]
  })

  describe("Context Analysis - Table Expectations", () => {
    it("should suggest tables after FROM keyword", () => {
      // Setup: Cursor after "FROM "
      mockModel.getLineContent = vi.fn(() => "SELECT * FROM ")
      mockModel.getValue = vi.fn(() => "SELECT * FROM ")
      mockPosition = new mockMonaco.Position(1, 14) // After "FROM "

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      expect(result.suggestions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            label: "eth_rpc.blocks",
            kind: mockMonaco.languages.CompletionItemKind.Class,
            detail: "Table (3 columns)",
          }),
          expect.objectContaining({
            label: "eth_rpc.transactions",
            kind: mockMonaco.languages.CompletionItemKind.Class,
            detail: "Table (3 columns)",
          }),
        ]),
      )
    })

    it("should suggest tables after JOIN keyword", () => {
      mockModel.getLineContent = vi.fn(
        () => "SELECT * FROM eth_rpc.blocks JOIN ",
      )
      mockModel.getValue = vi.fn(() => "SELECT * FROM eth_rpc.blocks JOIN ")
      mockPosition = new mockMonaco.Position(1, 35)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      expect(result.suggestions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            label: "eth_rpc.transactions",
            kind: mockMonaco.languages.CompletionItemKind.Class,
          }),
        ]),
      )
    })

    it("should suggest tables after LEFT JOIN keyword", () => {
      mockModel.getLineContent = vi.fn(() => "SELECT * FROM blocks LEFT JOIN ")
      mockModel.getValue = vi.fn(() => "SELECT * FROM blocks LEFT JOIN ")
      mockPosition = new mockMonaco.Position(1, 30)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      expect(result.suggestions.length).toBeGreaterThan(0)
      expect(result.suggestions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            kind: mockMonaco.languages.CompletionItemKind.Class,
          }),
        ]),
      )
    })
  })

  describe("Context Analysis - Column Expectations", () => {
    it("should suggest columns after SELECT keyword", () => {
      mockModel.getLineContent = vi.fn(() => "SELECT ")
      mockModel.getValue = vi.fn(() => "SELECT ")
      mockPosition = new mockMonaco.Position(1, 7)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      expect(result.suggestions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            label: "number",
            kind: mockMonaco.languages.CompletionItemKind.Field,
            detail: "BIGINT - eth_rpc.blocks",
          }),
          expect.objectContaining({
            label: "hash",
            kind: mockMonaco.languages.CompletionItemKind.Field,
            detail: expect.stringContaining("VARCHAR"),
          }),
        ]),
      )
    })

    it("should suggest columns after WHERE keyword", () => {
      mockModel.getLineContent = vi.fn(
        () => "SELECT * FROM eth_rpc.blocks WHERE ",
      )
      mockModel.getValue = vi.fn(() => "SELECT * FROM eth_rpc.blocks WHERE ")
      mockPosition = new mockMonaco.Position(1, 32)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      // Should include columns, UDFs, and keywords
      expect(result.suggestions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            kind: mockMonaco.languages.CompletionItemKind.Field,
          }),
        ]),
      )
      // Also verify some suggestions are present
      expect(result.suggestions.length).toBeGreaterThan(0)
    })

    it("should suggest columns after GROUP BY keyword", () => {
      mockModel.getLineContent = vi.fn(
        () => "SELECT COUNT(*) FROM eth_rpc.blocks GROUP BY ",
      )
      mockModel.getValue = vi.fn(
        () => "SELECT COUNT(*) FROM eth_rpc.blocks GROUP BY ",
      )
      mockPosition = new mockMonaco.Position(1, 45)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      expect(result.suggestions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            kind: mockMonaco.languages.CompletionItemKind.Field,
          }),
        ]),
      )
    })
  })

  describe("UDF Function Completions", () => {
    it("should suggest UDF functions in SELECT context", () => {
      mockModel.getLineContent = vi.fn(() => "SELECT ")
      mockModel.getValue = vi.fn(() => "SELECT ")
      mockPosition = new mockMonaco.Position(1, 7)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      expect(result.suggestions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            label: "evm_decode_log",
            kind: mockMonaco.languages.CompletionItemKind.Function,
            detail: "Nozzle UDF",
          }),
          expect.objectContaining({
            label: "${dataset}.eth_call",
            kind: mockMonaco.languages.CompletionItemKind.Function,
            detail: "Nozzle UDF",
          }),
        ]),
      )
    })

    it("should include UDF functions in suggestions", () => {
      mockModel.getLineContent = vi.fn(() => "SELECT ")
      mockModel.getValue = vi.fn(() => "SELECT ")
      mockPosition = new mockMonaco.Position(1, 7)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      // Verify UDF functions are included in suggestions
      expect(result.suggestions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            label: "evm_decode_log",
            kind: mockMonaco.languages.CompletionItemKind.Function,
          }),
        ]),
      )
    })
  })

  describe("SQL Keyword Completions", () => {
    it("should suggest SQL keywords with appropriate priority", () => {
      mockModel.getLineContent = vi.fn(() => "")
      mockModel.getValue = vi.fn(() => "")
      mockPosition = new mockMonaco.Position(1, 1)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      expect(result.suggestions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            label: "SELECT",
            kind: mockMonaco.languages.CompletionItemKind.Keyword,
            detail: "SQL Keyword",
          }),
          expect.objectContaining({
            label: "FROM",
            kind: mockMonaco.languages.CompletionItemKind.Keyword,
          }),
        ]),
      )
    })

    it("should filter keywords by current word prefix", () => {
      mockModel.getLineContent = vi.fn(() => "SEL")
      mockModel.getValue = vi.fn(() => "SEL")
      mockPosition = new mockMonaco.Position(1, 4)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      // Should include SELECT but not FROM when filtering by "SEL"
      const keywordLabels = result.suggestions
        .filter(
          (s) => s.kind === mockMonaco.languages.CompletionItemKind.Keyword,
        )
        .map((s) => s.label)

      expect(keywordLabels).toContain("SELECT")
      expect(keywordLabels).not.toContain("FROM")
    })
  })

  describe("Priority Sorting", () => {
    it("should sort suggestions by priority (sortText)", () => {
      mockModel.getLineContent = vi.fn(() => "SELECT * FROM ")
      mockModel.getValue = vi.fn(() => "SELECT * FROM ")
      mockPosition = new mockMonaco.Position(1, 14)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      // Verify suggestions are sorted by sortText
      for (let i = 1; i < result.suggestions.length; i++) {
        const prev =
          result.suggestions[i - 1].sortText ||
          result.suggestions[i - 1].label.toString()
        const curr =
          result.suggestions[i].sortText ||
          result.suggestions[i].label.toString()
        expect(prev.localeCompare(curr)).toBeLessThanOrEqual(0)
      }
    })

    it("should give tables highest priority (1-xxx)", () => {
      mockModel.getLineContent = vi.fn(() => "FROM ")
      mockModel.getValue = vi.fn(() => "FROM ")
      mockPosition = new mockMonaco.Position(1, 5)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      const tableSuggestions = result.suggestions.filter(
        (s) => s.kind === mockMonaco.languages.CompletionItemKind.Class,
      )
      tableSuggestions.forEach((suggestion) => {
        expect(suggestion.sortText).toMatch(/^1-\d{3}$/)
      })
    })

    it("should give columns medium priority (2-xxx)", () => {
      mockModel.getLineContent = vi.fn(() => "SELECT ")
      mockModel.getValue = vi.fn(() => "SELECT ")
      mockPosition = new mockMonaco.Position(1, 7)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      const columnSuggestions = result.suggestions.filter(
        (s) => s.kind === mockMonaco.languages.CompletionItemKind.Field,
      )
      columnSuggestions.forEach((suggestion) => {
        expect(suggestion.sortText).toMatch(/^2-\d{3}-\d{3}$/)
      })
    })
  })

  describe("Error Handling", () => {
    it("should handle model errors gracefully", () => {
      // Mock model that throws errors
      mockModel.getLineContent = vi.fn(() => {
        throw new Error("Model error")
      })
      mockPosition = new mockMonaco.Position(1, 1)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      expect(result.suggestions).toEqual([])
    })

    it("should handle empty metadata gracefully", () => {
      mockModel.getLineContent = vi.fn(() => "SELECT * FROM ")
      mockModel.getValue = vi.fn(() => "SELECT * FROM ")
      mockPosition = new mockMonaco.Position(1, 14)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        [],
        testUdfs,
      )

      // Should still provide UDFs and keywords even without metadata
      expect(result.suggestions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            kind: mockMonaco.languages.CompletionItemKind.Function,
          }),
          expect.objectContaining({
            kind: mockMonaco.languages.CompletionItemKind.Keyword,
          }),
        ]),
      )
    })

    it("should handle empty UDFs gracefully", () => {
      mockModel.getLineContent = vi.fn(() => "SELECT ")
      mockModel.getValue = vi.fn(() => "SELECT ")
      mockPosition = new mockMonaco.Position(1, 7)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        [],
      )

      // Should still provide tables, columns, and keywords
      expect(result.suggestions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            kind: mockMonaco.languages.CompletionItemKind.Class,
          }),
          expect.objectContaining({
            kind: mockMonaco.languages.CompletionItemKind.Field,
          }),
          expect.objectContaining({
            kind: mockMonaco.languages.CompletionItemKind.Keyword,
          }),
        ]),
      )
    })
  })

  describe("Table Context Analysis", () => {
    it("should extract table names from complex queries", () => {
      const complexQuery = `
        SELECT b.number, t.hash 
        FROM eth_rpc.blocks b 
        JOIN eth_rpc.transactions t ON b.hash = t.block_hash 
        WHERE b.number > 1000
      `

      mockModel.getLineContent = vi.fn(() => "WHERE ")
      mockModel.getValue = vi.fn(() => complexQuery)
      mockPosition = new mockMonaco.Position(4, 15)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      // Should suggest columns from both tables that are in scope
      expect(result.suggestions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            kind: mockMonaco.languages.CompletionItemKind.Field,
            detail: expect.stringMatching(
              /(eth_rpc\.blocks|eth_rpc\.transactions)/,
            ),
          }),
        ]),
      )
    })

    it("should handle subqueries correctly", () => {
      const subQuery = "SELECT (SELECT COUNT(*) FROM eth_rpc.blocks WHERE "

      mockModel.getLineContent = vi.fn(() => subQuery)
      mockModel.getValue = vi.fn(() => subQuery)
      mockPosition = new mockMonaco.Position(1, subQuery.length)

      const result = provideNozzleCompletions(
        mockMonaco,
        mockModel,
        mockPosition,
        testMetadata,
        testUdfs,
      )

      // Should suggest columns appropriate for WHERE clause
      expect(result.suggestions).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            kind: mockMonaco.languages.CompletionItemKind.Field,
          }),
        ]),
      )
    })
  })
})
