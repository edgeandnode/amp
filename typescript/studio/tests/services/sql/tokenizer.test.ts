/**
 * Unit tests for SQL Tokenization via QueryContextAnalyzer
 * Tests SQL token recognition and context analysis functionality
 */

import { describe, test, expect, beforeEach, afterEach } from 'vitest'
import { QueryContextAnalyzer } from '../../../src/services/sql/contextAnalyzer'

describe('SQL Tokenization via Context Analyzer', () => {
  let analyzer: QueryContextAnalyzer

  beforeEach(() => {
    analyzer = new QueryContextAnalyzer()
  })

  afterEach(() => {
    analyzer.clearCache()
  })

  // Helper function to create proper mock models
  const createMockModel = (query: string) => ({
    getValue: () => query,
    getOffsetAt: (position: any) => position.column - 1,
    getLineContent: (lineNumber: number) => {
      const lines = query.split('\n')
      return lines[lineNumber - 1] || ''
    },
    getWordAtPosition: (position: any) => {
      const line = query.split('\n')[position.lineNumber - 1] || ''
      const beforeCursor = line.substring(0, position.column - 1)
      const match = beforeCursor.match(/(\w+)$/)
      if (match) {
        const word = match[1]
        const startColumn = position.column - word.length
        return {
          word,
          startColumn,
          endColumn: position.column
        }
      }
      return null
    }
  })

  describe('Basic SQL Recognition', () => {
    test('should recognize SELECT keyword context', () => {
      const mockModel = createMockModel("SELECT * FROM users")
      const mockPosition = { lineNumber: 1, column: 8 } // Position after "SELECT "
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.currentClause).toBe('SELECT')
      expect(context.expectsColumn).toBe(true)
      expect(context.expectsFunction).toBe(true)
    })

    test('should recognize FROM keyword context', () => {
      const mockModel = createMockModel("SELECT * FROM ")
      const mockPosition = { lineNumber: 1, column: 15 } // Position after "FROM "
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.currentClause).toBe('FROM')
      expect(context.expectsTable).toBe(true)
      expect(context.expectsColumn).toBe(false)
    })

    test('should recognize WHERE keyword context', () => {
      const mockModel = createMockModel("SELECT * FROM users WHERE ")
      const mockPosition = { lineNumber: 1, column: 27 } // Position after "WHERE "
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.currentClause).toBe('WHERE')
      expect(context.expectsColumn).toBe(true)
      expect(context.expectsFunction).toBe(true)
    })
  })

  describe('String and Comment Detection', () => {
    test('should detect cursor in string literal', () => {
      const mockModel = createMockModel("SELECT * FROM users WHERE name = 'test'")
      const mockPosition = { lineNumber: 1, column: 37 } // Position inside string
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.cursorInString).toBe(true)
      expect(context.cursorInComment).toBe(false)
    })

    test('should detect cursor in line comment', () => {
      const mockModel = createMockModel("SELECT * FROM users -- comment here")
      const mockPosition = { lineNumber: 1, column: 35 } // Position in comment
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.cursorInComment).toBe(true)
      expect(context.cursorInString).toBe(false)
    })
  })

  describe('Table and Alias Detection', () => {
    test('should detect table references', () => {
      const mockModel = createMockModel("SELECT * FROM anvil.logs WHERE ")
      const mockPosition = { lineNumber: 1, column: 32 } // Position after WHERE
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.availableTables).toHaveLength(1)
      expect(context.availableTables[0]).toBe('anvil.logs')
    })

    test('should detect table aliases', () => {
      const mockModel = createMockModel("SELECT * FROM anvil.logs l WHERE ")
      const mockPosition = { lineNumber: 1, column: 34 } // Position after WHERE
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.availableTables).toHaveLength(1)
      expect(context.tableAliases.get('l')).toBe('anvil.logs')
    })
  })

  describe('Error Recovery', () => {
    test('should handle empty query gracefully', () => {
      const mockModel = createMockModel("")
      const mockPosition = { lineNumber: 1, column: 1 }
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.currentClause).toBeNull()
      expect(context.availableTables).toHaveLength(0)
      expect(context.cursorInString).toBe(false)
      expect(context.cursorInComment).toBe(false)
    })

    test('should handle malformed SQL gracefully', () => {
      const mockModel = createMockModel("SELECT FROM users") // Missing columns
      const mockPosition = { lineNumber: 1, column: 13 } // Position after "FROM "
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.currentClause).toBe('FROM')
      expect(context.expectsTable).toBe(true)
    })
  })

  describe('Caching', () => {
    test('should provide cache clearing functionality', () => {
      // Create some analysis to populate cache
      const mockModel = createMockModel("SELECT * FROM users")
      const mockPosition = { lineNumber: 1, column: 8 }
      
      analyzer.analyzeContext(mockModel, mockPosition)
      
      // Clear cache should not throw
      expect(() => analyzer.clearCache()).not.toThrow()
    })
  })
})