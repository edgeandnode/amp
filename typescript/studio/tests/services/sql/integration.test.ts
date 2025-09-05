/**
 * Integration tests for SQL Intellisense System
 * Tests complete integration from provider manager to Monaco completions
 */

import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { Effect, Scope } from 'effect'
import * as monaco from 'monaco-editor'
import { SQLIntellisenseManager, withSQLIntellisense, runWithSQLIntellisense } from '../../../src/services/sql/providerManager'
import { QueryContextAnalyzer } from '../../../src/services/sql/contextAnalyzer'
import { NozzleCompletionProvider } from '../../../src/services/sql/completionProvider'
import {
  mockMetadata,
  mockUDFs,
  mockMetadataEmpty,
  mockUDFsEmpty,
  createTestModel,
  disposeTestModel,
  createMockCompletionContext,
  createMockCancellationToken,
  extractCompletionLabels,
  runEffectTest
} from './fixtures'

describe('SQL Intellisense Integration', () => {
  let testModels: monaco.editor.ITextModel[] = []

  afterEach(() => {
    // Cleanup all test models
    testModels.forEach(model => disposeTestModel(model))
    testModels = []
  })

  /**
   * Helper to create and track test models for cleanup
   */
  function createAndTrackModel(content: string): monaco.editor.ITextModel {
    const model = createTestModel(content)
    testModels.push(model)
    return model
  }

  describe('SQLIntellisenseManager Lifecycle', () => {
    test('should initialize and dispose properly', async () => {
      await runEffectTest(
        Effect.scoped(
          Effect.gen(function* (_) {
            const manager = new SQLIntellisenseManager()
            
            // Initialize
            yield* _(manager.initialize(mockMetadata, mockUDFs))
            
            // Verify initialization
            expect(manager.isReady()).toBe(true)
            
            const stats = yield* _(manager.getStats())
            expect(stats.isInitialized).toBe(true)
            expect(stats.providerCount).toBeGreaterThan(0)
            expect(stats.providers).toContain('completion')
            
            // Test cleanup happens automatically via Effect finalizer
          })
        )
      )
    })

    test('should handle initialization with empty metadata', async () => {
      await runEffectTest(
        Effect.scoped(
          Effect.gen(function* (_) {
            const manager = new SQLIntellisenseManager()
            
            yield* _(manager.initialize(mockMetadataEmpty, mockUDFsEmpty))
            
            expect(manager.isReady()).toBe(true)
            
            const stats = yield* _(manager.getStats())
            expect(stats.isInitialized).toBe(true)
          })
        )
      )
    })

    test('should support metadata updates', async () => {
      await runEffectTest(
        Effect.scoped(
          Effect.gen(function* (_) {
            const manager = new SQLIntellisenseManager()
            
            // Initial setup with empty metadata
            yield* _(manager.initialize(mockMetadataEmpty, mockUDFsEmpty))
            
            let stats = yield* _(manager.getStats())
            expect(stats.isInitialized).toBe(true)
            
            // Update with real metadata
            yield* _(manager.updateMetadata(mockMetadata, mockUDFs))
            
            stats = yield* _(manager.getStats())
            expect(stats.isInitialized).toBe(true)
            expect(stats.providerCount).toBeGreaterThan(0)
          })
        )
      )
    })

    test('should handle multiple dispose calls gracefully', async () => {
      await runEffectTest(
        Effect.scoped(
          Effect.gen(function* (_) {
            const manager = new SQLIntellisenseManager()
            
            yield* _(manager.initialize(mockMetadata, mockUDFs))
            
            // Manual dispose (should not break when Effect finalizer runs)
            manager.dispose()
            expect(manager.isReady()).toBe(false)
            
            // Should be safe to call again
            manager.dispose()
          })
        )
      )
    })
  })

  describe('Utility Functions Integration', () => {
    test('withSQLIntellisense should provide working manager', async () => {
      const result = await runEffectTest(
        Effect.scoped(
          withSQLIntellisense(mockMetadata, mockUDFs, (manager) => 
            Effect.gen(function* (_) {
              expect(manager.isReady()).toBe(true)
              
              const stats = yield* _(manager.getStats())
              expect(stats.isInitialized).toBe(true)
              expect(stats.providers).toContain('completion')
              
              return 'success'
            })
          )
        )
      )
      
      expect(result).toBe('success')
    })

    test('runWithSQLIntellisense should work with async operations', async () => {
      const result = await runWithSQLIntellisense(
        mockMetadata,
        mockUDFs,
        async (manager) => {
          expect(manager.isReady()).toBe(true)
          
          // Test async operation
          await new Promise(resolve => setTimeout(resolve, 10))
          
          return 'async-success'
        }
      )
      
      expect(result).toBe('async-success')
    })
  })

  describe('End-to-End Completion Flow', () => {
    test('should provide table completions in real Monaco scenario', async () => {
      await runEffectTest(
        Effect.scoped(
          withSQLIntellisense(mockMetadata, mockUDFs, (manager) => 
            Effect.gen(function* (_) {
              const model = createAndTrackModel("SELECT * FROM ")
              const position = new monaco.Position(1, 15)
              const context = createMockCompletionContext()
              const token = createMockCancellationToken()
              
              // Get the registered completion provider
              const completionProvider = manager.getCompletionProvider()
              expect(completionProvider).toBeDefined()
              
              // Test completion
              const result = await completionProvider!.provideCompletionItems(
                model, position, context, token
              )
              
              expect(result).toBeDefined()
              
              const labels = extractCompletionLabels(result)
              expect(labels).toContain('anvil.logs')
              expect(labels).toContain('anvil.transactions')
              expect(labels).toContain('anvil.blocks')
              
              return 'completion-success'
            })
          )
        )
      )
    })

    test('should provide column completions with table context', async () => {
      await runEffectTest(
        Effect.scoped(
          withSQLIntellisense(mockMetadata, mockUDFs, (manager) => 
            Effect.gen(function* (_) {
              const model = createAndTrackModel("SELECT address FROM anvil.logs WHERE ")
              const position = new monaco.Position(1, 40)
              const context = createMockCompletionContext()
              const token = createMockCancellationToken()
              
              const completionProvider = manager.getCompletionProvider()
              expect(completionProvider).toBeDefined()
              
              const result = await completionProvider!.provideCompletionItems(
                model, position, context, token
              )
              
              expect(result).toBeDefined()
              
              const labels = extractCompletionLabels(result)
              // Should have columns from anvil.logs available
              expect(labels).toContain('address')
              expect(labels).toContain('topics')
              expect(labels).toContain('data')
              expect(labels).toContain('block_number')
              
              return 'column-completion-success'
            })
          )
        )
      )
    })

    test('should provide UDF completions with snippets', async () => {
      await runEffectTest(
        Effect.scoped(
          withSQLIntellisense(mockMetadata, mockUDFs, (manager) => 
            Effect.gen(function* (_) {
              const model = createAndTrackModel("SELECT evm_")
              const position = new monaco.Position(1, 12)
              const context = createMockCompletionContext()
              const token = createMockCancellationToken()
              
              const completionProvider = manager.getCompletionProvider()
              expect(completionProvider).toBeDefined()
              
              const result = await completionProvider!.provideCompletionItems(
                model, position, context, token
              )
              
              expect(result).toBeDefined()
              
              const labels = extractCompletionLabels(result)
              expect(labels).toContain('evm_decode_log')
              expect(labels).toContain('evm_topic')
              expect(labels).toContain('evm_decode_params')
              
              // Check that UDF completions have snippet format
              const evmDecodeCompletion = result!.suggestions.find(
                s => (typeof s.label === 'string' ? s.label : s.label.label) === 'evm_decode_log'
              )
              
              expect(evmDecodeCompletion).toBeDefined()
              expect(evmDecodeCompletion!.kind).toBe(monaco.languages.CompletionItemKind.Function)
              expect(evmDecodeCompletion!.insertText).toContain('${1:')
              expect(evmDecodeCompletion!.insertTextRules).toBe(
                monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
              )
              
              return 'udf-completion-success'
            })
          )
        )
      )
    })

    test('should handle complex query scenarios', async () => {
      await runEffectTest(
        Effect.scoped(
          withSQLIntellisense(mockMetadata, mockUDFs, (manager) => 
            Effect.gen(function* (_) {
              const query = "SELECT l.address FROM anvil.logs l LEFT JOIN anvil.transactions t ON l.transaction_hash = t.hash WHERE "
              const model = createAndTrackModel(query)
              const position = new monaco.Position(1, query.length + 1)
              const context = createMockCompletionContext()
              const token = createMockCancellationToken()
              
              const completionProvider = manager.getCompletionProvider()
              expect(completionProvider).toBeDefined()
              
              const result = await completionProvider!.provideCompletionItems(
                model, position, context, token
              )
              
              expect(result).toBeDefined()
              
              const labels = extractCompletionLabels(result)
              
              // Should have columns from both tables due to JOIN
              expect(labels).toContain('address')    // from anvil.logs
              expect(labels).toContain('hash')       // from anvil.transactions
              expect(labels).toContain('value')      // from anvil.transactions
              
              // Should also have operators for WHERE clause
              expect(labels).toContain('=')
              expect(labels).toContain('AND')
              expect(labels).toContain('OR')
              
              return 'complex-query-success'
            })
          )
        )
      )
    })
  })

  describe('Error Recovery and Edge Cases', () => {
    test('should handle malformed SQL gracefully', async () => {
      await runEffectTest(
        Effect.scoped(
          withSQLIntellisense(mockMetadata, mockUDFs, (manager) => 
            Effect.gen(function* (_) {
              const model = createAndTrackModel("SELECT FROM ") // Missing columns
              const position = new monaco.Position(1, 13)
              const context = createMockCompletionContext()
              const token = createMockCancellationToken()
              
              const completionProvider = manager.getCompletionProvider()
              expect(completionProvider).toBeDefined()
              
              const result = await completionProvider!.provideCompletionItems(
                model, position, context, token
              )
              
              expect(result).toBeDefined()
              expect(result!.suggestions).toBeDefined()
              
              // Should still provide table completions despite malformed SQL
              const labels = extractCompletionLabels(result)
              expect(labels.length).toBeGreaterThan(0)
              expect(labels).toContain('anvil.logs')
              
              return 'malformed-sql-success'
            })
          )
        )
      )
    })

    test('should handle cursor at end of file', async () => {
      await runEffectTest(
        Effect.scoped(
          withSQLIntellisense(mockMetadata, mockUDFs, (manager) => 
            Effect.gen(function* (_) {
              const query = "SELECT * FROM anvil.logs"
              const model = createAndTrackModel(query)
              const position = new monaco.Position(1, query.length + 1) // Beyond end
              const context = createMockCompletionContext()
              const token = createMockCancellationToken()
              
              const completionProvider = manager.getCompletionProvider()
              expect(completionProvider).toBeDefined()
              
              const result = await completionProvider!.provideCompletionItems(
                model, position, context, token
              )
              
              expect(result).toBeDefined()
              expect(result!.suggestions).toBeDefined()
              
              // Should handle gracefully and provide relevant completions
              const labels = extractCompletionLabels(result)
              expect(labels.length).toBeGreaterThan(0)
              
              return 'end-of-file-success'
            })
          )
        )
      )
    })

    test('should handle empty query', async () => {
      await runEffectTest(
        Effect.scoped(
          withSQLIntellisense(mockMetadata, mockUDFs, (manager) => 
            Effect.gen(function* (_) {
              const model = createAndTrackModel("")
              const position = new monaco.Position(1, 1)
              const context = createMockCompletionContext()
              const token = createMockCancellationToken()
              
              const completionProvider = manager.getCompletionProvider()
              expect(completionProvider).toBeDefined()
              
              const result = await completionProvider!.provideCompletionItems(
                model, position, context, token
              )
              
              expect(result).toBeDefined()
              expect(result!.suggestions).toBeDefined()
              
              // Should provide basic keyword completions
              const labels = extractCompletionLabels(result)
              expect(labels).toContain('SELECT')
              
              return 'empty-query-success'
            })
          )
        )
      )
    })
  })

  describe('Performance and Caching', () => {
    test('should benefit from analyzer caching on repeated queries', async () => {
      await runEffectTest(
        Effect.scoped(
          withSQLIntellisense(mockMetadata, mockUDFs, (manager) => 
            Effect.gen(function* (_) {
              const model = createAndTrackModel("SELECT * FROM anvil.logs WHERE ")
              const position = new monaco.Position(1, 32)
              const context = createMockCompletionContext()
              const token = createMockCancellationToken()
              
              const completionProvider = manager.getCompletionProvider()
              expect(completionProvider).toBeDefined()
              
              // First call
              const start1 = performance.now()
              const result1 = await completionProvider!.provideCompletionItems(
                model, position, context, token
              )
              const duration1 = performance.now() - start1
              
              // Second call (should be faster due to caching)
              const start2 = performance.now()
              const result2 = await completionProvider!.provideCompletionItems(
                model, position, context, token
              )
              const duration2 = performance.now() - start2
              
              // Both should return valid results
              expect(result1).toBeDefined()
              expect(result2).toBeDefined()
              
              const labels1 = extractCompletionLabels(result1)
              const labels2 = extractCompletionLabels(result2)
              
              expect(labels1).toEqual(labels2)
              
              // Second call should be faster (cached)
              expect(duration2).toBeLessThan(duration1)
              
              return 'caching-success'
            })
          )
        )
      )
    })

    test('should handle cache clearing', async () => {
      await runEffectTest(
        Effect.scoped(
          withSQLIntellisense(mockMetadata, mockUDFs, (manager) => 
            Effect.gen(function* (_) {
              const analyzer = manager.getContextAnalyzer()
              expect(analyzer).toBeDefined()
              
              // Populate cache
              const model = createAndTrackModel("SELECT * FROM anvil.logs WHERE ")
              const position = new monaco.Position(1, 32)
              
              await analyzer!.analyzeContext(model, position)
              
              let stats = analyzer!.getCacheStats()
              expect(stats.contextCache).toBeGreaterThan(0)
              
              // Clear cache
              yield* _(manager.clearCaches())
              
              stats = analyzer!.getCacheStats()
              expect(stats.contextCache).toBe(0)
              
              return 'cache-clearing-success'
            })
          )
        )
      )
    })
  })

  describe('Configuration and Customization', () => {
    test('should respect completion configuration', async () => {
      const config = {
        minPrefixLength: 2,
        maxSuggestions: 10,
        showDebugInfo: true
      }
      
      await runEffectTest(
        Effect.scoped(
          withSQLIntellisense(mockMetadata, mockUDFs, (manager) => 
            Effect.gen(function* (_) {
              const model = createAndTrackModel("SELECT a")
              const position = new monaco.Position(1, 9)
              const context = createMockCompletionContext()
              const token = createMockCancellationToken()
              
              const completionProvider = manager.getCompletionProvider()
              expect(completionProvider).toBeDefined()
              
              const result = await completionProvider!.provideCompletionItems(
                model, position, context, token
              )
              
              expect(result).toBeDefined()
              expect(result!.suggestions).toBeDefined()
              
              // Should respect maxSuggestions
              expect(result!.suggestions.length).toBeLessThanOrEqual(config.maxSuggestions)
              
              return 'configuration-success'
            }), 
            config
          )
        )
      )
    })
  })
})