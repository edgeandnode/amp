/**
 * Integration tests for SQL Intellisense System
 * Tests complete integration from provider setup to Monaco completions
 */

import * as monaco from "monaco-editor/esm/vs/editor/editor.api"
import { afterEach, beforeEach, describe, expect, test } from "vitest"

import {
  areProvidersActive,
  disposeAllProviders,
  getProviderMetrics,
  NozzleCompletionProvider,
  QueryContextAnalyzer,
  setupNozzleSQLProviders,
  updateProviderData,
} from "../../../src/services/sql"

import {
  createMockCancellationToken,
  createMockCompletionContext,
  createTestModel,
  disposeTestModel,
  extractCompletionLabels,
  mockMetadata,
  mockMetadataEmpty,
  mockUDFs,
  mockUDFsEmpty,
} from "./fixtures"

describe("SQL Intellisense Integration", () => {
  let testModels: Array<monaco.editor.ITextModel> = []
  let disposable: { dispose: () => void } | null = null

  afterEach(() => {
    // Cleanup all test models
    testModels.forEach((model) => disposeTestModel(model))
    testModels = []

    // Dispose providers
    if (disposable) {
      disposable.dispose()
      disposable = null
    }
    disposeAllProviders()
  })

  /**
   * Helper to create and track test models for cleanup
   */
  function createAndTrackModel(content: string): monaco.editor.ITextModel {
    const model = createTestModel(content)
    testModels.push(model)
    return model
  }

  describe("Provider Lifecycle Management", () => {
    test("should setup and dispose providers properly", () => {
      // Setup providers
      disposable = setupNozzleSQLProviders(mockMetadata, mockUDFs)

      expect(areProvidersActive()).toBe(true)
      expect(disposable).toBeDefined()
      expect(typeof disposable.dispose).toBe("function")

      // Dispose
      disposable.dispose()
      disposable = null

      expect(areProvidersActive()).toBe(false)
    })

    test("should handle setup with empty metadata", () => {
      disposable = setupNozzleSQLProviders(mockMetadataEmpty, mockUDFsEmpty)

      expect(areProvidersActive()).toBe(true)

      const metrics = getProviderMetrics()
      expect(metrics).toBeDefined()
      expect(metrics?.totalRequests).toBe(0)
    })

    test("should support metadata updates", () => {
      // Initial setup with empty metadata
      disposable = setupNozzleSQLProviders(mockMetadataEmpty, mockUDFsEmpty)
      expect(areProvidersActive()).toBe(true)

      // Update with real metadata
      updateProviderData(mockMetadata, mockUDFs)

      expect(areProvidersActive()).toBe(true)

      const metrics = getProviderMetrics()
      expect(metrics).toBeDefined()
    })

    test("should handle multiple dispose calls gracefully", () => {
      disposable = setupNozzleSQLProviders(mockMetadata, mockUDFs)

      expect(areProvidersActive()).toBe(true)

      // Manual dispose
      disposable.dispose()
      expect(areProvidersActive()).toBe(false)

      // Should be safe to call again
      expect(() => disposable!.dispose()).not.toThrow()
      disposable = null
    })

    test("should track performance metrics", () => {
      disposable = setupNozzleSQLProviders(mockMetadata, mockUDFs)

      const metrics = getProviderMetrics()
      expect(metrics).toBeDefined()
      expect(metrics?.totalRequests).toBe(0)
      expect(metrics?.averageResponseTime).toBe(0)
      expect(metrics?.cacheHitRate).toBe(0)
      expect(metrics?.failureCount).toBe(0)
      expect(metrics?.lastUpdate).toBeGreaterThan(0)
    })
  })

  describe("End-to-End Completion Flow", () => {
    beforeEach(() => {
      // Setup providers for each test
      disposable = setupNozzleSQLProviders(mockMetadata, mockUDFs)

      // Wait for Monaco to be available (mock setup)
      // if (typeof window !== "undefined" && !window.monaco) {
      //   ;(window as any).monaco = monaco
      // }
    })

    test("should provide table completions in real Monaco scenario", async () => {
      const model = createAndTrackModel("SELECT * FROM ")
      const position = new monaco.Position(1, 15)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      // Monaco is available for testing
      expect(monaco.languages).toBeDefined()

      // For integration testing, we'll directly test the completion provider
      // since Monaco's internal provider registration is complex to mock
      const contextAnalyzer = new QueryContextAnalyzer()
      const completionProvider = new NozzleCompletionProvider(
        mockMetadata,
        mockUDFs,
        contextAnalyzer,
      )

      const result = await completionProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      const labels = extractCompletionLabels(result ?? null)
      expect(labels).toContain("anvil.logs")
      expect(labels).toContain("anvil.transactions")
      expect(labels).toContain("anvil.blocks")

      completionProvider.dispose()
    })

    test("should provide column completions with table context", async () => {
      const model = createAndTrackModel("SELECT address FROM anvil.logs WHERE ")
      const position = new monaco.Position(1, 40)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const contextAnalyzer = new QueryContextAnalyzer()
      const completionProvider = new NozzleCompletionProvider(
        mockMetadata,
        mockUDFs,
        contextAnalyzer,
      )

      const result = await completionProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      const labels = extractCompletionLabels(result ?? null)
      // Should have columns from anvil.logs available
      expect(labels).toContain("address")
      expect(labels).toContain("topics")
      expect(labels).toContain("data")
      expect(labels).toContain("block_number")

      completionProvider.dispose()
    })

    test("should provide UDF completions with snippets", async () => {
      const model = createAndTrackModel("SELECT evm_")
      const position = new monaco.Position(1, 12)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const contextAnalyzer = new QueryContextAnalyzer()
      const completionProvider = new NozzleCompletionProvider(
        mockMetadata,
        mockUDFs,
        contextAnalyzer,
      )

      const result = await completionProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      const labels = extractCompletionLabels(result ?? null)
      expect(labels).toContain("evm_decode_log")
      expect(labels).toContain("evm_topic")
      expect(labels).toContain("evm_decode_params")

      // Check that UDF completions have snippet format
      const evmDecodeCompletion = result!.suggestions.find(
        (s) => (typeof s.label === "string" ? s.label : s.label.label) === "evm_decode_log",
      )

      expect(evmDecodeCompletion).toBeDefined()
      expect(evmDecodeCompletion!.kind).toBe(
        monaco.languages.CompletionItemKind.Function,
      )
      expect(evmDecodeCompletion!.insertText).toContain("${1:")
      expect(evmDecodeCompletion!.insertTextRules).toBe(
        monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
      )

      completionProvider.dispose()
    })

    test("should handle complex query scenarios", async () => {
      const query =
        "SELECT l.address FROM anvil.logs l LEFT JOIN anvil.transactions t ON l.transaction_hash = t.hash WHERE "
      const model = createAndTrackModel(query)
      const position = new monaco.Position(1, query.length + 1)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const contextAnalyzer = new QueryContextAnalyzer()
      const completionProvider = new NozzleCompletionProvider(
        mockMetadata,
        mockUDFs,
        contextAnalyzer,
      )

      const result = await completionProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()

      const labels = extractCompletionLabels(result ?? null)

      // Should have columns from both tables due to JOIN
      expect(labels).toContain("address") // from anvil.logs
      expect(labels).toContain("hash") // from anvil.transactions
      expect(labels).toContain("value") // from anvil.transactions

      // Should also have operators for WHERE clause
      expect(labels).toContain("=")
      expect(labels).toContain("AND")
      expect(labels).toContain("OR")

      completionProvider.dispose()
    })
  })

  describe("Error Recovery and Edge Cases", () => {
    beforeEach(() => {
      disposable = setupNozzleSQLProviders(mockMetadata, mockUDFs)
      // if (typeof window !== "undefined" && !window.monaco) {
      //   ;(window as any).monaco = monaco
      // }
    })

    test("should handle malformed SQL gracefully", async () => {
      const model = createAndTrackModel("SELECT FROM ") // Missing columns
      const position = new monaco.Position(1, 13)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const contextAnalyzer = new QueryContextAnalyzer()
      const completionProvider = new NozzleCompletionProvider(
        mockMetadata,
        mockUDFs,
        contextAnalyzer,
      )

      const result = await completionProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()
      expect(result!.suggestions).toBeDefined()

      // Should still provide table completions despite malformed SQL
      const labels = extractCompletionLabels(result ?? null)
      expect(labels.length).toBeGreaterThan(0)
      expect(labels).toContain("anvil.logs")

      completionProvider.dispose()
    })

    test("should handle cursor at end of file", async () => {
      const query = "SELECT * FROM anvil.logs"
      const model = createAndTrackModel(query)
      const position = new monaco.Position(1, query.length + 1) // Beyond end
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const contextAnalyzer = new QueryContextAnalyzer()
      const completionProvider = new NozzleCompletionProvider(
        mockMetadata,
        mockUDFs,
        contextAnalyzer,
      )

      const result = await completionProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()
      expect(result!.suggestions).toBeDefined()

      // Should handle gracefully and provide relevant completions
      const labels = extractCompletionLabels(result ?? null)
      expect(labels.length).toBeGreaterThan(0)

      completionProvider.dispose()
    })

    test("should handle empty query", async () => {
      const model = createAndTrackModel("")
      const position = new monaco.Position(1, 1)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const contextAnalyzer = new QueryContextAnalyzer()
      const completionProvider = new NozzleCompletionProvider(
        mockMetadata,
        mockUDFs,
        contextAnalyzer,
      )

      const result = await completionProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()
      expect(result!.suggestions).toBeDefined()

      // Should provide basic keyword completions
      const labels = extractCompletionLabels(result ?? null)
      expect(labels).toContain("SELECT")

      completionProvider.dispose()
    })
  })

  describe("Performance and Caching", () => {
    beforeEach(() => {
      disposable = setupNozzleSQLProviders(mockMetadata, mockUDFs)
      // if (typeof window !== "undefined" && !window.monaco) {
      //   ;(window as any).monaco = monaco
      // }
    })

    test("should benefit from analyzer caching on repeated queries", async () => {
      const model = createAndTrackModel("SELECT * FROM anvil.logs WHERE ")
      const position = new monaco.Position(1, 32)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const contextAnalyzer = new QueryContextAnalyzer()
      const completionProvider = new NozzleCompletionProvider(
        mockMetadata,
        mockUDFs,
        contextAnalyzer,
      )

      // Verify cache is initially empty
      expect(contextAnalyzer.getCacheStats().contextCache).toBe(0)

      // First call - should populate cache
      const result1 = await completionProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      // Verify cache now has one entry
      expect(contextAnalyzer.getCacheStats().contextCache).toBe(1)

      // Second call with identical parameters - should use cache
      const result2 = await completionProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      // Cache should still have one entry (same query cached)
      expect(contextAnalyzer.getCacheStats().contextCache).toBe(1)

      // Both should return identical results
      expect(result1).toBeDefined()
      expect(result2).toBeDefined()

      const labels1 = extractCompletionLabels(result1 ?? null)
      const labels2 = extractCompletionLabels(result2 ?? null)

      expect(labels1).toEqual(labels2)

      completionProvider.dispose()
    })

    test("should handle cache clearing", () => {
      const contextAnalyzer = new QueryContextAnalyzer()

      // Populate cache
      const model = createAndTrackModel("SELECT * FROM anvil.logs WHERE ")
      const position = new monaco.Position(1, 32)

      const context1 = contextAnalyzer.analyzeContext(model, position)
      expect(context1).toBeDefined()

      // Clear cache
      contextAnalyzer.clearCache()

      // Should still work after cache clear
      const context2 = contextAnalyzer.analyzeContext(model, position)
      expect(context2).toBeDefined()
      expect(context2).toEqual(context1)
    })
  })

  describe("Configuration and Customization", () => {
    test("should respect completion configuration", async () => {
      const config = {
        minPrefixLength: 2,
        maxSuggestions: 10,
        showDebugInfo: true,
      }

      disposable = setupNozzleSQLProviders(mockMetadata, mockUDFs, config)

      const model = createAndTrackModel("SELECT a")
      const position = new monaco.Position(1, 9)
      const context = createMockCompletionContext()
      const token = createMockCancellationToken()

      const contextAnalyzer = new QueryContextAnalyzer(config)
      const completionProvider = new NozzleCompletionProvider(
        mockMetadata,
        mockUDFs,
        contextAnalyzer,
        config,
      )

      const result = await completionProvider.provideCompletionItems(
        model,
        position,
        context,
        token,
      )

      expect(result).toBeDefined()
      expect(result!.suggestions).toBeDefined()

      // Should respect maxSuggestions
      expect(result!.suggestions.length).toBeLessThanOrEqual(
        config.maxSuggestions,
      )

      completionProvider.dispose()
    })
  })
})
