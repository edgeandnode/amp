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

  describe('Basic SQL Recognition', () => {
    test('should recognize SELECT keyword context', () => {
      const mockModel = {
        getValue: () => "SELECT * FROM users",
        getOffsetAt: () => 8 // Position after "SELECT "
      } as any
      
      const mockPosition = { lineNumber: 1, column: 8 } as any
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.currentClause).toBe('SELECT')
      expect(context.expectsColumn).toBe(true)
      expect(context.expectsFunction).toBe(true)
    })

    test('should recognize FROM keyword context', () => {
      const mockModel = {
        getValue: () => "SELECT * FROM ",
        getOffsetAt: () => 15 // Position after "FROM "
      } as any
      
      const mockPosition = { lineNumber: 1, column: 15 } as any
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.currentClause).toBe('FROM')
      expect(context.expectsTable).toBe(true)
      expect(context.expectsColumn).toBe(false)
    })

    test('should recognize WHERE keyword context', () => {
      const mockModel = {
        getValue: () => "SELECT * FROM users WHERE ",
        getOffsetAt: () => 27 // Position after "WHERE "
      } as any
      
      const mockPosition = { lineNumber: 1, column: 27 } as any
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.currentClause).toBe('WHERE')
      expect(context.expectsColumn).toBe(true)
      expect(context.expectsFunction).toBe(true)
    })
  })

  describe('String and Comment Detection', () => {
    test('should detect cursor in string literal', () => {
      const mockModel = {
        getValue: () => "SELECT * FROM users WHERE name = 'test'",
        getOffsetAt: () => 37 // Position inside string
      } as any
      
      const mockPosition = { lineNumber: 1, column: 37 } as any
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.cursorInString).toBe(true)
      expect(context.cursorInComment).toBe(false)
    })

    test('should detect cursor in line comment', () => {
      const mockModel = {
        getValue: () => "SELECT * FROM users -- comment here",
        getOffsetAt: () => 35 // Position in comment
      } as any
      
      const mockPosition = { lineNumber: 1, column: 35 } as any
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.cursorInComment).toBe(true)
      expect(context.cursorInString).toBe(false)
    })
  })

  describe('Table and Alias Detection', () => {
    test('should detect table references', () => {
      const mockModel = {
        getValue: () => "SELECT * FROM anvil.logs WHERE ",
        getOffsetAt: () => 32 // Position after WHERE
      } as any
      
      const mockPosition = { lineNumber: 1, column: 32 } as any
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.availableTables).toHaveLength(1)
      expect(context.availableTables[0]).toBe('anvil.logs')
    })

    test('should detect table aliases', () => {
      const mockModel = {
        getValue: () => "SELECT * FROM anvil.logs l WHERE ",
        getOffsetAt: () => 34 // Position after WHERE
      } as any
      
      const mockPosition = { lineNumber: 1, column: 34 } as any
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.availableTables).toHaveLength(1)
      expect(context.tableAliases.get('l')).toBe('anvil.logs')
    })
  })

  describe('Error Recovery', () => {
    test('should handle empty query gracefully', () => {
      const mockModel = {
        getValue: () => "",
        getOffsetAt: () => 0
      } as any
      
      const mockPosition = { lineNumber: 1, column: 1 } as any
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.currentClause).toBeNull()
      expect(context.availableTables).toHaveLength(0)
      expect(context.cursorInString).toBe(false)
      expect(context.cursorInComment).toBe(false)
    })

    test('should handle malformed SQL gracefully', () => {
      const mockModel = {
        getValue: () => "SELECT FROM users", // Missing columns
        getOffsetAt: () => 13 // Position after "FROM "
      } as any
      
      const mockPosition = { lineNumber: 1, column: 13 } as any
      
      const context = analyzer.analyzeContext(mockModel, mockPosition)
      
      expect(context.currentClause).toBe('FROM')
      expect(context.expectsTable).toBe(true)
    })
  })

  describe('Caching', () => {
    test('should provide cache clearing functionality', () => {
      // Create some analysis to populate cache
      const mockModel = {
        getValue: () => "SELECT * FROM users",
        getOffsetAt: () => 8
      } as any
      
      const mockPosition = { lineNumber: 1, column: 8 } as any
      
      analyzer.analyzeContext(mockModel, mockPosition)
      
      // Clear cache should not throw
      expect(() => analyzer.clearCache()).not.toThrow()
    })
  })
})