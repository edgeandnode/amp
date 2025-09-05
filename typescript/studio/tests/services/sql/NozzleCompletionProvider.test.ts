/**
 * Unit tests for NozzleCompletionProvider
 * Tests table/column/UDF completions, context filtering, and Monaco integration
 */

import * as monaco from "monaco-editor/esm/vs/editor/editor.api"
import { afterEach, beforeEach, describe, expect, test } from "vitest"

import { NozzleCompletionProvider } from "../../../src/services/sql/NozzleCompletionProvider"
import { QueryContextAnalyzer } from "../../../src/services/sql/QueryContextAnalyzer"

import {
  countCompletionsByKind,
  createMockCancellationToken,
  createMockCompletionContext,
  createTestModel,
  disposeTestModel,
  extractCompletionLabels,
  findCompletionByLabel,
  mockMetadata,
  mockMetadataEmpty,
  mockUDFs,
  mockUDFsEmpty,
} from "./fixtures"

describe("NozzleCompletionProvider", () => {
  let provider: NozzleCompletionProvider
  let analyzer: QueryContextAnalyzer
  let testModels: Array<monaco.editor.ITextModel> = []

  beforeEach(() => {
    analyzer = new QueryContextAnalyzer()
    provider = new NozzleCompletionProvider(mockMetadata, mockUDFs, analyzer)
    testModels = []
  })

  afterEach(() => {
    // Cleanup all test models
    testModels.forEach((model) => disposeTestModel(model))
    testModels = []

    // Clear analyzer cache
    analyzer.clearCache()
  })

  /**
   * Helper to create and track test models for cleanup
   */
  function createAndTrackModel(content: string): monaco.editor.ITextModel {
    const model = createTestModel(content)
    testModels.push(model)
    return model
  }

  describe("Table Completions", () => {
    test("should provide table completions after FROM", async () => {
      const model = createAndTrackModel("SELECT * FROM ")
      const position = new monaco.Position(1, 15)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()
      if (result) {
        expect(result.suggestions).toBeDefined()
      }

      const labels = extractCompletionLabels(result ?? null)
      expect(labels).toContain("anvil.logs")
      expect(labels).toContain("anvil.transactions")
      expect(labels).toContain("anvil.blocks")
    })

    test("should provide table details in completion items", async () => {
      const model = createAndTrackModel("SELECT * FROM ")
      const position = new monaco.Position(1, 15)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )
      const logsCompletion = findCompletionByLabel(result ?? null, "anvil.logs")

      expect(logsCompletion).toBeDefined()
      expect(logsCompletion!.kind).toBe(
        monaco.languages.CompletionItemKind.Class,
      )
      expect(logsCompletion!.detail).toContain("Table")
      expect(logsCompletion!.detail).toContain("7 columns")
      expect(logsCompletion!.documentation).toBeDefined()
    })

    test("should handle empty metadata gracefully", async () => {
      const emptyProvider = new NozzleCompletionProvider(
        mockMetadataEmpty,
        mockUDFs,
        analyzer,
      )
      const model = createAndTrackModel("SELECT * FROM ")
      const position = new monaco.Position(1, 15)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await emptyProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()
      if (result) {
        expect(result.suggestions).toBeDefined()
      }

      // Should still provide basic keyword suggestions
      const labels = extractCompletionLabels(result ?? null)
      expect(labels.length).toBeGreaterThan(0)

      // But no table completions
      const tableCount = countCompletionsByKind(
        result ?? null,
        monaco.languages.CompletionItemKind.Class,
      )
      expect(tableCount).toBe(0)
    })
  })

  describe("Column Completions", () => {
    test("should provide column completions in SELECT", async () => {
      const model = createAndTrackModel("SELECT ")
      const position = new monaco.Position(1, 8)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      const labels = extractCompletionLabels(result ?? null)

      // Should contain columns from all tables when no specific table is in scope
      expect(labels).toContain("address")
      expect(labels).toContain("block_number")
      expect(labels).toContain("hash")
      expect(labels).toContain("number")
    })

    test("should filter columns by available tables", async () => {
      const model = createAndTrackModel("SELECT address FROM anvil.logs WHERE ")
      const position = new monaco.Position(1, 40)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      const labels = extractCompletionLabels(result ?? null)

      // Should contain columns from anvil.logs
      expect(labels).toContain("address")
      expect(labels).toContain("topics")
      expect(labels).toContain("data")
      expect(labels).toContain("transaction_hash")
    })

    test("should provide column details with data types", async () => {
      const model = createAndTrackModel("SELECT ")
      const position = new monaco.Position(1, 8)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )
      const addressCompletion = findCompletionByLabel(result ?? null, "address")

      expect(addressCompletion).toBeDefined()
      expect(addressCompletion!.kind).toBe(
        monaco.languages.CompletionItemKind.Field,
      )
      expect(addressCompletion!.detail).toContain("address")
      expect(addressCompletion!.detail).toContain("anvil.logs")
      expect(addressCompletion!.documentation).toBeDefined()
    })

    test("should support table aliases for column filtering", async () => {
      const model = createAndTrackModel("SELECT l. FROM anvil.logs l WHERE ")
      const position = new monaco.Position(1, 10)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      const labels = extractCompletionLabels(result ?? null)

      // Should contain columns from the aliased table
      expect(labels).toContain("address")
      expect(labels).toContain("topics")
      expect(labels).toContain("data")
    })
  })

  describe("UDF Completions", () => {
    test("should provide UDF completions in SELECT clause", async () => {
      const model = createAndTrackModel("SELECT ")
      const position = new monaco.Position(1, 8)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      const labels = extractCompletionLabels(result ?? null)

      // Should contain UDF functions
      expect(labels).toContain("evm_decode_log")
      expect(labels).toContain("evm_topic")
      expect(labels).toContain("attestation_hash")
      expect(labels).toContain("{dataset}.eth_call")
    })

    test("should provide UDF snippets with placeholders", async () => {
      const model = createAndTrackModel("SELECT evm_")
      const position = new monaco.Position(1, 12)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )
      const evmDecodeCompletion = findCompletionByLabel(
        result ?? null,
        "evm_decode_log",
      )

      expect(evmDecodeCompletion).toBeDefined()
      expect(evmDecodeCompletion!.kind).toBe(
        monaco.languages.CompletionItemKind.Function,
      )
      expect(evmDecodeCompletion!.insertText).toContain("${1:")
      expect(evmDecodeCompletion!.insertTextRules).toBe(
        monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
      )
      expect(evmDecodeCompletion!.command?.id).toBe(
        "editor.action.triggerParameterHints",
      )
    })

    test("should provide UDF documentation", async () => {
      const model = createAndTrackModel("SELECT evm_")
      const position = new monaco.Position(1, 12)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )
      const evmDecodeCompletion = findCompletionByLabel(
        result ?? null,
        "evm_decode_log",
      )

      expect(evmDecodeCompletion).toBeDefined()
      expect(evmDecodeCompletion!.documentation).toBeDefined()

      const docValue =
        typeof evmDecodeCompletion!.documentation === "string"
          ? evmDecodeCompletion!.documentation
          : evmDecodeCompletion!.documentation!.value

      expect(docValue).toContain("evm_decode_log")
      expect(docValue).toContain("Decodes an EVM event log")
    })

    test("should handle empty UDFs gracefully", async () => {
      const emptyUDFProvider = new NozzleCompletionProvider(
        mockMetadata,
        mockUDFsEmpty,
        analyzer,
      )
      const model = createAndTrackModel("SELECT ")
      const position = new monaco.Position(1, 8)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await emptyUDFProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      // Should not crash and should still provide other completions
      const functionCount = countCompletionsByKind(
        result ?? null,
        monaco.languages.CompletionItemKind.Function,
      )
      expect(functionCount).toBe(0)
    })
  })

  describe("Keyword Completions", () => {
    test("should provide contextual keywords after SELECT", async () => {
      const model = createAndTrackModel("SELECT * ")
      const position = new monaco.Position(1, 10)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      const labels = extractCompletionLabels(result ?? null)

      // Should contain contextual keywords for SELECT clause
      expect(labels).toContain("FROM")
      expect(labels).toContain("WHERE")
      expect(labels).toContain("GROUP BY")
      expect(labels).toContain("ORDER BY")
    })

    test("should provide contextual keywords after FROM", async () => {
      const model = createAndTrackModel("SELECT * FROM anvil.logs ")
      const position = new monaco.Position(1, 26)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      const labels = extractCompletionLabels(result ?? null)

      // Should contain contextual keywords for FROM clause
      expect(labels).toContain("WHERE")
      expect(labels).toContain("JOIN")
      expect(labels).toContain("LEFT JOIN")
      expect(labels).toContain("GROUP BY")
    })

    test("should provide operator completions in WHERE clause", async () => {
      const model = createAndTrackModel(
        "SELECT * FROM anvil.logs WHERE address ",
      )
      const position = new monaco.Position(1, 40)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      const labels = extractCompletionLabels(result ?? null)

      // Should contain operators when they're expected
      expect(labels).toContain("=")
      expect(labels).toContain("<>")
      expect(labels).toContain("LIKE")
    })
  })

  describe("Context-Aware Filtering", () => {
    test("should not provide suggestions inside string literals", async () => {
      const model = createAndTrackModel(
        "SELECT * FROM anvil.logs WHERE address = 'cursor here'",
      )
      const position = new monaco.Position(1, 50) // Inside string
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      // Should provide limited or no suggestions inside strings
      const labels = extractCompletionLabels(result ?? null)
      expect(labels.length).toBeLessThan(10) // Significantly fewer suggestions
    })

    test("should not provide suggestions inside comments", async () => {
      const model = createAndTrackModel("SELECT * -- cursor in comment")
      const position = new monaco.Position(1, 25) // Inside comment
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      // Should provide limited or no suggestions inside comments
      const labels = extractCompletionLabels(result ?? null)
      expect(labels.length).toBeLessThan(10) // Significantly fewer suggestions
    })

    test("should filter by prefix", async () => {
      const model = createAndTrackModel("SELECT addr")
      const position = new monaco.Position(1, 12)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      const labels = extractCompletionLabels(result ?? null)

      // Should contain items starting with "addr"
      expect(labels).toContain("address")

      // Should not contain unrelated items
      expect(labels).not.toContain("block_number")
      expect(labels).not.toContain("hash")
    })

    test("should respect minPrefixLength configuration", async () => {
      const configuredProvider = new NozzleCompletionProvider(
        mockMetadata,
        mockUDFs,
        analyzer,
        { minPrefixLength: 3 },
      )

      const model = createAndTrackModel("SELECT a")
      const position = new monaco.Position(1, 9)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await configuredProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()
      if (result) {
        expect(result.suggestions).toHaveLength(0) // Should be empty due to short prefix
      }
    })

    test("should respect maxSuggestions configuration", async () => {
      const configuredProvider = new NozzleCompletionProvider(
        mockMetadata,
        mockUDFs,
        analyzer,
        { maxSuggestions: 5 },
      )

      const model = createAndTrackModel("SELECT ")
      const position = new monaco.Position(1, 8)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await configuredProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()
      if (result) {
        expect(result.suggestions.length).toBeLessThanOrEqual(5)
      }
    })
  })

  describe("Completion Ranking and Sorting", () => {
    test("should sort completions by type priority", async () => {
      const model = createAndTrackModel("SELECT * FROM ")
      const position = new monaco.Position(1, 15)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()
      if (result) {
        expect(result.suggestions.length).toBeGreaterThan(0)

        // Check that sort text starts with appropriate priorities
        const firstSuggestion = result.suggestions[0]
        expect(firstSuggestion.sortText).toMatch(/^[1-5]/) // Priority prefix
      }
    })

    test("should preselect first table completion", async () => {
      const model = createAndTrackModel("SELECT * FROM ")
      const position = new monaco.Position(1, 15)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      if (result) {
        const tableCompletions = result.suggestions.filter(
          (item) => item.kind === monaco.languages.CompletionItemKind.Class,
        )

        expect(tableCompletions.length).toBeGreaterThan(0)
        expect(tableCompletions[0].preselect).toBe(true)
      }
    })
  })

  describe("Error Handling and Edge Cases", () => {
    test("should handle malformed queries gracefully", async () => {
      const model = createAndTrackModel("SELECT FROM ") // Missing column
      const position = new monaco.Position(1, 13)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()
      if (result) {
        expect(result.suggestions).toBeDefined()
      }

      // Should still provide completions even with malformed SQL
      const labels = extractCompletionLabels(result ?? null)
      expect(labels.length).toBeGreaterThan(0)
    })

    test("should handle empty query", async () => {
      const model = createAndTrackModel("")
      const position = new monaco.Position(1, 1)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()
      if (result) {
        expect(result.suggestions).toBeDefined()
      }

      // Should provide basic keyword suggestions
      const labels = extractCompletionLabels(result ?? null)
      expect(labels).toContain("SELECT")
    })

    test("should provide fallback suggestions on analysis failure", async () => {
      // Create a provider that will cause analysis to fail
      const model = createAndTrackModel("SELECT * FROM anvil.logs")
      const position = new monaco.Position(1, 15)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      // Mock the analyzer to throw an error
      const originalAnalyze = analyzer.analyzeContext
      analyzer.analyzeContext = () => {
        throw new Error("Analysis failed")
      }

      try {
        const result = await provider.provideCompletionItems(
          model,
          position,
          context,
          token,
        )

        expect(result).toBeDefined()
        if (result) {
          expect(result.suggestions).toBeDefined()
        }

        // Should still provide fallback suggestions
        const labels = extractCompletionLabels(result ?? null)
        expect(labels.length).toBeGreaterThan(0)
        expect(labels).toContain("SELECT")
      } finally {
        // Restore original analyzer
        analyzer.analyzeContext = originalAnalyze
      }
    })

    test("should handle cancellation token", async () => {
      const model = createAndTrackModel("SELECT * FROM anvil.logs")
      const position = new monaco.Position(1, 15)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken(true) // Cancelled

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      // Should handle cancelled requests gracefully
      expect(result).toBeDefined()
    })
  })

  describe("Range Calculation", () => {
    test("should calculate correct replacement range", async () => {
      const model = createAndTrackModel("SELECT addr")
      const position = new monaco.Position(1, 12) // After "addr"
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const result = await provider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      const addressCompletion = findCompletionByLabel(result ?? null, "address")
      expect(addressCompletion).toBeDefined()

      // Should replace the partial word "addr"
      const range = addressCompletion!.range as monaco.Range
      expect(range.startColumn).toBe(8) // Start of "addr"
      expect(range.endColumn).toBe(12) // End of "addr"
      expect(range.startLineNumber).toBe(1)
      expect(range.endLineNumber).toBe(1)
    })
  })
})
