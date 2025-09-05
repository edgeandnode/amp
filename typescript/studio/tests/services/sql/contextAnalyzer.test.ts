/**
 * Unit tests for QueryContextAnalyzer
 * Tests SQL context analysis, clause detection, and cursor position handling
 */

import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { Effect } from 'effect'
import * as monaco from 'monaco-editor'
import { QueryContextAnalyzer } from '../../../src/services/sql/contextAnalyzer'
import { 
  mockMetadata, 
  mockMetadataEmpty,
  basicQueries,
  qualifiedNameQueries,
  stringAndCommentQueries,
  edgeCaseQueries,
  createTestModel,
  disposeTestModel,
  runEffectTest
} from './fixtures'

describe('QueryContextAnalyzer', () => {
  let analyzer: QueryContextAnalyzer
  let testModels: monaco.editor.ITextModel[] = []

  beforeEach(() => {
    analyzer = new QueryContextAnalyzer()
    testModels = []
  })

  afterEach(() => {
    // Cleanup all test models
    testModels.forEach(model => disposeTestModel(model))
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

  describe('Basic Clause Detection', () => {
    test('should detect FROM clause context', async () => {
      const model = createAndTrackModel("SELECT * FROM ")
      const position = new monaco.Position(1, 15)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.currentClause).toBe('FROM')
      expect(context.expectsTable).toBe(true)
      expect(context.expectsColumn).toBe(false)
    })

    test('should detect SELECT clause context', async () => {
      const model = createAndTrackModel("SELECT ")
      const position = new monaco.Position(1, 8)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.currentClause).toBe('SELECT')
      expect(context.expectsColumn).toBe(true)
      expect(context.expectsFunction).toBe(true)
    })

    test('should detect WHERE clause context', async () => {
      const model = createAndTrackModel("SELECT * FROM anvil.logs WHERE ")
      const position = new monaco.Position(1, 33)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.currentClause).toBe('WHERE')
      expect(context.expectsColumn).toBe(true)
      expect(context.expectsOperator).toBe(false) // Not after column name yet
    })

    test('should detect JOIN clause context', async () => {
      const model = createAndTrackModel("SELECT * FROM anvil.logs LEFT JOIN ")
      const position = new monaco.Position(1, 37)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.currentClause).toBe('JOIN')
      expect(context.expectsTable).toBe(true)
    })

    test('should detect GROUP BY clause context', async () => {
      const model = createAndTrackModel("SELECT COUNT(*) FROM anvil.logs GROUP BY ")
      const position = new monaco.Position(1, 42)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.currentClause).toBe('GROUP BY')
      expect(context.expectsColumn).toBe(true)
    })

    test('should detect ORDER BY clause context', async () => {
      const model = createAndTrackModel("SELECT * FROM anvil.logs ORDER BY ")
      const position = new monaco.Position(1, 35)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.currentClause).toBe('ORDER BY')
      expect(context.expectsColumn).toBe(true)
    })
  })

  describe('Cursor Position Analysis', () => {
    test('should detect cursor inside string literal', async () => {
      const model = createAndTrackModel("SELECT * FROM anvil.logs WHERE address = 'cursor here'")
      const position = new monaco.Position(1, 50) // Inside string

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.cursorInString).toBe(true)
      expect(context.cursorInComment).toBe(false)
    })

    test('should detect cursor inside line comment', async () => {
      const model = createAndTrackModel("SELECT * -- cursor in comment")
      const position = new monaco.Position(1, 25) // Inside comment

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.cursorInComment).toBe(true)
      expect(context.cursorInString).toBe(false)
    })

    test('should detect cursor inside block comment', async () => {
      const model = createAndTrackModel("SELECT /* cursor here */ * FROM anvil.logs")
      const position = new monaco.Position(1, 15) // Inside block comment

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.cursorInComment).toBe(true)
      expect(context.cursorInString).toBe(false)
    })

    test('should handle cursor outside strings and comments', async () => {
      const model = createAndTrackModel("SELECT * FROM anvil.logs")
      const position = new monaco.Position(1, 10)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.cursorInString).toBe(false)
      expect(context.cursorInComment).toBe(false)
    })
  })

  describe('Table and Alias Detection', () => {
    test('should detect simple table reference', async () => {
      const model = createAndTrackModel("SELECT * FROM anvil.logs WHERE address = '0x123'")
      const position = new monaco.Position(1, 35)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.availableTables).toHaveLength(1)
      expect(context.availableTables[0].name).toBe('anvil.logs')
      expect(context.availableTables[0].alias).toBeUndefined()
    })

    test('should detect table with alias', async () => {
      const model = createAndTrackModel("SELECT * FROM anvil.logs l WHERE l.address = '0x123'")
      const position = new monaco.Position(1, 35)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.availableTables).toHaveLength(1)
      expect(context.availableTables[0].name).toBe('anvil.logs')
      expect(context.availableTables[0].alias).toBe('l')
      expect(context.availableAliases.get('l')).toBe('anvil.logs')
    })

    test('should detect multiple tables in JOIN', async () => {
      const model = createAndTrackModel(
        "SELECT * FROM anvil.logs l JOIN anvil.transactions t ON l.transaction_hash = t.hash"
      )
      const position = new monaco.Position(1, 70)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.availableTables).toHaveLength(2)
      
      const tableNames = context.availableTables.map(t => t.name)
      expect(tableNames).toContain('anvil.logs')
      expect(tableNames).toContain('anvil.transactions')
      
      const aliases = context.availableTables.map(t => t.alias)
      expect(aliases).toContain('l')
      expect(aliases).toContain('t')
    })

    test('should detect qualified table names', async () => {
      const model = createAndTrackModel("SELECT * FROM anvil.logs WHERE ")
      const position = new monaco.Position(1, 32)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      const table = context.availableTables[0]
      expect(table.dataset).toBe('anvil')
      expect(table.name).toBe('anvil.logs')
    })
  })

  describe('Dot Notation Context', () => {
    test('should detect column completion after qualified table name', async () => {
      const model = createAndTrackModel("SELECT anvil.logs.")
      const position = new monaco.Position(1, 19)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.expectsColumn).toBe(true)
      expect(context.currentClause).toBe('SELECT')
      expect(context.lastToken).toBe('.')
    })

    test('should detect column completion after alias', async () => {
      const model = createAndTrackModel("SELECT l. FROM anvil.logs l")
      const position = new monaco.Position(1, 10)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.expectsColumn).toBe(true)
      expect(context.lastToken).toBe('.')
    })
  })

  describe('Error Handling and Fallback', () => {
    test('should provide fallback context for empty query', async () => {
      const model = createAndTrackModel("")
      const position = new monaco.Position(1, 1)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      expect(context.expectsTable).toBe(false)
      expect(context.expectsColumn).toBe(false)
      expect(context.expectsFunction).toBe(false)
      expect(context.currentClause).toBeNull()
      expect(context.availableTables).toHaveLength(0)
    })

    test('should handle malformed SQL gracefully', async () => {
      const model = createAndTrackModel("SELECT FROM anvil.logs") // Missing columns
      const position = new monaco.Position(1, 15)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      // Should still detect FROM context despite malformed SQL
      expect(context.currentClause).toBe('FROM')
      expect(context.expectsTable).toBe(true)
    })

    test('should handle typos in keywords', async () => {
      const model = createAndTrackModel("SELEC * FROM ") // Typo in SELECT
      const position = new monaco.Position(1, 14)

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      // Should still detect FROM context
      expect(context.expectsTable).toBe(true)
    })

    test('should handle cursor beyond text length', async () => {
      const model = createAndTrackModel("SELECT * FROM anvil.logs")
      const position = new monaco.Position(1, 100) // Beyond text length

      const context = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )

      // Should provide reasonable fallback
      expect(context).toBeDefined()
      expect(context.availableTables).toHaveLength(1)
    })
  })

  describe('Caching', () => {
    test('should cache analysis results', async () => {
      const model = createAndTrackModel("SELECT * FROM anvil.logs WHERE ")
      const position = new monaco.Position(1, 32)

      // First call
      const start1 = performance.now()
      const context1 = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )
      const duration1 = performance.now() - start1

      // Second call (should be cached)
      const start2 = performance.now()
      const context2 = await runEffectTest(
        analyzer.analyzeContext(model, position)
      )
      const duration2 = performance.now() - start2

      expect(context1.currentClause).toBe(context2.currentClause)
      expect(context1.expectsColumn).toBe(context2.expectsColumn)
      
      // Second call should be significantly faster (cached)
      expect(duration2).toBeLessThan(duration1 * 0.5)
    })

    test('should clear cache', () => {
      analyzer.clearCache()
      const stats = analyzer.getCacheStats()
      expect(stats.contextCache).toBe(0)
    })

    test('should return cache statistics', () => {
      const stats = analyzer.getCacheStats()
      expect(stats).toHaveProperty('contextCache')
      expect(stats).toHaveProperty('tokenizerCache')
      expect(typeof stats.contextCache).toBe('number')
    })
  })

  describe('Comprehensive Test Scenarios', () => {
    // Test all basic query scenarios
    test.each(basicQueries)(
      'should analyze basic query: $description',
      async ({ query, position, expectedContext }) => {
        const model = createAndTrackModel(query)

        const context = await runEffectTest(
          analyzer.analyzeContext(model, position)
        )

        if (expectedContext?.expectsTable !== undefined) {
          expect(context.expectsTable).toBe(expectedContext.expectsTable)
        }
        if (expectedContext?.expectsColumn !== undefined) {
          expect(context.expectsColumn).toBe(expectedContext.expectsColumn)
        }
        if (expectedContext?.expectsFunction !== undefined) {
          expect(context.expectsFunction).toBe(expectedContext.expectsFunction)
        }
        if (expectedContext?.currentClause !== undefined) {
          expect(context.currentClause).toBe(expectedContext.currentClause)
        }
      }
    )

    // Test qualified name scenarios
    test.each(qualifiedNameQueries)(
      'should analyze qualified query: $description',
      async ({ query, position, expectedContext }) => {
        const model = createAndTrackModel(query)

        const context = await runEffectTest(
          analyzer.analyzeContext(model, position)
        )

        if (expectedContext?.expectsColumn !== undefined) {
          expect(context.expectsColumn).toBe(expectedContext.expectsColumn)
        }
        if (expectedContext?.currentClause !== undefined) {
          expect(context.currentClause).toBe(expectedContext.currentClause)
        }
      }
    )

    // Test string and comment scenarios
    test.each(stringAndCommentQueries)(
      'should analyze string/comment query: $description',
      async ({ query, position, expectedContext }) => {
        const model = createAndTrackModel(query)

        const context = await runEffectTest(
          analyzer.analyzeContext(model, position)
        )

        if (expectedContext?.cursorInString !== undefined) {
          expect(context.cursorInString).toBe(expectedContext.cursorInString)
        }
        if (expectedContext?.cursorInComment !== undefined) {
          expect(context.cursorInComment).toBe(expectedContext.cursorInComment)
        }
      }
    )
  })
})