/**
 * Unit tests for QueryContextAnalyzer
 * Tests SQL context analysis, clause detection, and cursor position handling
 */

import { afterEach, beforeEach, describe, expect, test } from "vitest"

import { QueryContextAnalyzer } from "../../../src/services/sql/QueryContextAnalyzer"

describe("QueryContextAnalyzer", () => {
  let analyzer: QueryContextAnalyzer

  beforeEach(() => {
    analyzer = new QueryContextAnalyzer()
  })

  afterEach(() => {
    // Clear analyzer cache
    analyzer.clearCache()
  })

  // Helper function to create proper mock models (same as tokenizer tests)
  const createMockModel = (query: string) => ({
    getValue: () => query,
    getOffsetAt: (position: any) => position.column - 1,
    getLineContent: (lineNumber: number) => {
      const lines = query.split("\n")
      return lines[lineNumber - 1] || ""
    },
    getWordAtPosition: (position: any) => {
      const line = query.split("\n")[position.lineNumber - 1] || ""
      const beforeCursor = line.substring(0, position.column - 1)
      const match = beforeCursor.match(/(\w+)$/)
      if (match) {
        const word = match[1]
        const startColumn = position.column - word.length
        return {
          word,
          startColumn,
          endColumn: position.column,
        }
      }
      return null
    },
  })

  // Helper function to create test positions
  const createMockPosition = (lineNumber: number, column: number) => ({
    lineNumber,
    column,
  })

  describe("Basic Clause Detection", () => {
    test("should detect FROM clause context", () => {
      const model = createMockModel("SELECT * FROM ")
      const position = createMockPosition(1, 15)

      const context = analyzer.analyzeContext(model, position)

      expect(context.currentClause).toBe("FROM")
      expect(context.expectsTable).toBe(true)
      expect(context.expectsColumn).toBe(false)
    })

    test("should detect SELECT clause context", () => {
      const model = createMockModel("SELECT ")
      const position = createMockPosition(1, 8)

      const context = analyzer.analyzeContext(model, position)

      expect(context.currentClause).toBe("SELECT")
      expect(context.expectsColumn).toBe(true)
      expect(context.expectsFunction).toBe(true)
    })

    test("should detect WHERE clause context", () => {
      const model = createMockModel("SELECT * FROM anvil.logs WHERE ")
      const position = createMockPosition(1, 33)

      const context = analyzer.analyzeContext(model, position)

      expect(context.currentClause).toBe("WHERE")
      expect(context.expectsColumn).toBe(true)
      expect(context.availableTables).toContain("anvil.logs")
    })

    test("should detect JOIN clause context", () => {
      const model = createMockModel("SELECT * FROM anvil.logs LEFT JOIN ")
      const position = createMockPosition(1, 37)

      const context = analyzer.analyzeContext(model, position)

      expect(context.currentClause).toBe("JOIN")
      expect(context.expectsTable).toBe(true)
    })

    test("should detect GROUP BY clause context", () => {
      const model = createMockModel("SELECT COUNT(*) FROM anvil.logs GROUP BY ")
      const position = createMockPosition(1, 42)

      const context = analyzer.analyzeContext(model, position)

      expect(context.currentClause).toBe("GROUP_BY")
      expect(context.expectsColumn).toBe(true)
    })

    test("should detect ORDER BY clause context", () => {
      const model = createMockModel("SELECT * FROM anvil.logs ORDER BY ")
      const position = createMockPosition(1, 35)

      const context = analyzer.analyzeContext(model, position)

      expect(context.currentClause).toBe("ORDER_BY")
      expect(context.expectsColumn).toBe(true)
    })
  })

  describe("Cursor Position Analysis", () => {
    test("should detect cursor inside string literal", () => {
      const model = createMockModel("SELECT * FROM users WHERE name = 'test'")
      const position = createMockPosition(1, 37) // Inside 'test'

      const context = analyzer.analyzeContext(model, position)

      expect(context.cursorInString).toBe(true)
      expect(context.cursorInComment).toBe(false)
    })

    test("should detect cursor inside line comment", () => {
      const model = createMockModel("SELECT * FROM users -- comment here")
      const position = createMockPosition(1, 35) // In comment

      const context = analyzer.analyzeContext(model, position)

      expect(context.cursorInComment).toBe(true)
      expect(context.cursorInString).toBe(false)
    })

    test("should handle cursor outside strings and comments", () => {
      const model = createMockModel("SELECT * FROM users WHERE id = 123")
      const position = createMockPosition(1, 35) // After '='

      const context = analyzer.analyzeContext(model, position)

      expect(context.cursorInString).toBe(false)
      expect(context.cursorInComment).toBe(false)
      expect(context.currentClause).toBe("WHERE")
    })
  })

  describe("Table and Alias Detection", () => {
    test("should detect simple table reference", () => {
      const model = createMockModel("SELECT * FROM anvil.logs WHERE ")
      const position = createMockPosition(1, 32)

      const context = analyzer.analyzeContext(model, position)

      expect(context.availableTables).toContain("anvil.logs")
      expect(context.availableTables).toHaveLength(1)
    })

    test("should detect table with alias", () => {
      const model = createMockModel("SELECT * FROM anvil.logs l WHERE ")
      const position = createMockPosition(1, 34)

      const context = analyzer.analyzeContext(model, position)

      expect(context.availableTables).toContain("anvil.logs")
      expect(context.tableAliases.get("l")).toBe("anvil.logs")
    })

    test("should detect multiple tables in JOIN", () => {
      const model = createMockModel(
        "SELECT * FROM anvil.logs l JOIN anvil.transactions t ON l.tx_hash = t.hash WHERE ",
      )
      const position = createMockPosition(1, 82)

      const context = analyzer.analyzeContext(model, position)

      expect(context.availableTables).toContain("anvil.logs")
      expect(context.availableTables).toContain("anvil.transactions")
      expect(context.tableAliases.get("l")).toBe("anvil.logs")
      expect(context.tableAliases.get("t")).toBe("anvil.transactions")
    })

    test("should detect qualified table names", () => {
      const model = createMockModel("SELECT * FROM dataset.table_name WHERE ")
      const position = createMockPosition(1, 40)

      const context = analyzer.analyzeContext(model, position)

      expect(context.availableTables).toContain("dataset.table_name")
    })
  })

  describe("Dot Notation Context", () => {
    test("should detect column completion after qualified table name", () => {
      const model = createMockModel("SELECT anvil.logs.")
      const position = createMockPosition(1, 19) // After the dot

      const context = analyzer.analyzeContext(model, position)

      expect(context.expectsColumn).toBe(true)
      expect(context.currentClause).toBe("SELECT")
    })

    test("should detect column completion after alias", () => {
      const model = createMockModel("SELECT l.* FROM anvil.logs l WHERE l.")
      const position = createMockPosition(1, 38) // After 'l.'

      const context = analyzer.analyzeContext(model, position)

      expect(context.expectsColumn).toBe(true)
      expect(context.availableTables).toContain("anvil.logs")
    })
  })

  describe("Error Handling and Fallback", () => {
    test("should provide fallback context for empty query", () => {
      const model = createMockModel("")
      const position = createMockPosition(1, 1)

      const context = analyzer.analyzeContext(model, position)

      expect(context.currentClause).toBeNull()
      expect(context.availableTables).toHaveLength(0)
      expect(context.cursorInString).toBe(false)
      expect(context.cursorInComment).toBe(false)
    })

    test("should handle malformed SQL gracefully", () => {
      const model = createMockModel("SELECT FROM users") // Missing columns
      const position = createMockPosition(1, 13) // After 'FROM '

      const context = analyzer.analyzeContext(model, position)

      expect(context.currentClause).toBe("FROM")
      expect(context.expectsTable).toBe(true)
    })

    test("should handle typos in keywords", () => {
      const model = createMockModel("SELCT * FROM users") // Typo in SELECT
      const position = createMockPosition(1, 19)

      const context = analyzer.analyzeContext(model, position)

      // Should still work reasonably well
      expect(context.availableTables).toContain("users")
    })

    test("should handle cursor beyond text length", () => {
      const model = createMockModel("SELECT * FROM users")
      const position = createMockPosition(1, 100) // Way beyond end

      const context = analyzer.analyzeContext(model, position)

      expect(context).toBeDefined()
      expect(context.availableTables).toContain("users")
    })
  })

  describe("Caching", () => {
    test("should cache analysis results", () => {
      const model = createMockModel("SELECT * FROM anvil.logs WHERE ")
      const position = createMockPosition(1, 32)

      // Initially cache should be empty
      expect(analyzer.getCacheStats().contextCache).toBe(0)

      // First call should populate cache
      const context1 = analyzer.analyzeContext(model, position)
      expect(analyzer.getCacheStats().contextCache).toBe(1)

      // Second call should use cache and return identical result
      const context2 = analyzer.analyzeContext(model, position)
      expect(analyzer.getCacheStats().contextCache).toBe(1) // Still 1, not 2

      // Results should be identical
      expect(context1).toEqual(context2)

      // Different query should add to cache
      const model2 = createMockModel("SELECT * FROM users")
      const position2 = createMockPosition(1, 18)
      analyzer.analyzeContext(model2, position2)
      expect(analyzer.getCacheStats().contextCache).toBe(2)
    })

    test("should clear cache", () => {
      const model = createMockModel("SELECT * FROM users")
      const position = createMockPosition(1, 8)

      // Populate cache
      analyzer.analyzeContext(model, position)
      expect(analyzer.getCacheStats().contextCache).toBe(1)

      // Clear cache should empty the cache
      analyzer.clearCache()
      expect(analyzer.getCacheStats().contextCache).toBe(0)

      // Should not throw on multiple clears
      expect(() => analyzer.clearCache()).not.toThrow()
      expect(analyzer.getCacheStats().contextCache).toBe(0)
    })
  })

  describe("Comprehensive Test Scenarios", () => {
    const testCases = [
      {
        name: "Basic table completion after FROM",
        query: "SELECT * FROM ",
        position: { lineNumber: 1, column: 15 },
        expected: {
          currentClause: "FROM",
          expectsTable: true,
          expectsColumn: false,
        },
      },
      {
        name: "Column and function completion in SELECT",
        query: "SELECT ",
        position: { lineNumber: 1, column: 8 },
        expected: {
          currentClause: "SELECT",
          expectsColumn: true,
          expectsFunction: true,
        },
      },
      {
        name: "Column completion in WHERE clause",
        query: "SELECT * FROM anvil.logs WHERE ",
        position: { lineNumber: 1, column: 32 },
        expected: {
          currentClause: "WHERE",
          expectsColumn: true,
          expectsFunction: true,
          expectsOperator: true,
          availableTablesContains: "anvil.logs",
        },
      },
      {
        name: "Additional column completion after comma in SELECT",
        query: "SELECT column1, ",
        position: { lineNumber: 1, column: 17 },
        expected: {
          currentClause: "SELECT",
          expectsColumn: true,
          expectsFunction: true,
        },
      },
    ]

    test.each(testCases)(
      "should analyze basic query: $name",
      ({ expected, position, query }) => {
        const model = createMockModel(query)
        const mockPosition = createMockPosition(
          position.lineNumber,
          position.column,
        )

        const context = analyzer.analyzeContext(model, mockPosition)

        expect(context.currentClause).toBe(expected.currentClause)
        expect(context.expectsTable).toBe(expected.expectsTable ?? false)
        expect(context.expectsColumn).toBe(expected.expectsColumn || false)
        expect(context.expectsFunction).toBe(expected.expectsFunction ?? false)
        expect(context.expectsOperator).toBe(expected.expectsOperator ?? false)

        if (expected.availableTablesContains) {
          expect(context.availableTables).toContain(
            expected.availableTablesContains,
          )
        }
      },
    )

    const qualifiedTestCases = [
      {
        name: "Column completion after qualified table with dot",
        query: "SELECT * FROM anvil.logs.",
        position: { lineNumber: 1, column: 25 },
        expected: {
          currentClause: "FROM",
          expectsTable: true,
          availableTablesContains: "anvil.logs.",
        },
      },
      {
        name: "Qualified column reference completion",
        query: "SELECT anvil.logs. FROM anvil.logs",
        position: { lineNumber: 1, column: 19 },
        expected: {
          currentClause: "SELECT",
          expectsColumn: true,
          availableTablesContains: "anvil.logs",
        },
      },
    ]

    test.each(qualifiedTestCases)(
      "should analyze qualified query: $name",
      ({ expected, position, query }) => {
        const model = createMockModel(query)
        const mockPosition = createMockPosition(
          position.lineNumber,
          position.column,
        )

        const context = analyzer.analyzeContext(model, mockPosition)

        expect(context.currentClause).toBe(expected.currentClause)
        expect(context.expectsTable).toBe(expected.expectsTable ?? false)
        expect(context.expectsColumn).toBe(expected.expectsColumn || false)

        if (expected.availableTablesContains) {
          expect(context.availableTables).toContain(
            expected.availableTablesContains,
          )
        }
      },
    )

    const stringCommentTestCases = [
      {
        name: "Cursor inside string literal",
        query: "SELECT * FROM users WHERE name = 'test string'",
        position: { lineNumber: 1, column: 40 }, // Inside string
        expected: {
          cursorInString: true,
          cursorInComment: false,
        },
      },
      {
        name: "Cursor inside line comment",
        query: "SELECT * FROM users -- this is a comment",
        position: { lineNumber: 1, column: 35 }, // Inside comment
        expected: {
          cursorInString: false,
          cursorInComment: true,
        },
      },
      {
        name: "Regular context with prefix",
        query: "SELECT * FROM anvil.logs WHERE bl",
        position: { lineNumber: 1, column: 35 }, // After 'bl'
        expected: {
          currentClause: "WHERE",
          expectsColumn: true,
          expectsFunction: true,
          currentPrefix: "bl",
          cursorInString: false,
          cursorInComment: false,
          availableTablesContains: "anvil.logs",
        },
      },
    ]

    test.each(stringCommentTestCases)(
      "should analyze string/comment query: $name",
      ({ expected, position, query }) => {
        const model = createMockModel(query)
        const mockPosition = createMockPosition(
          position.lineNumber,
          position.column,
        )

        const context = analyzer.analyzeContext(model, mockPosition)

        expect(context.cursorInString).toBe(expected.cursorInString || false)
        expect(context.cursorInComment).toBe(expected.cursorInComment || false)

        if (!expected.cursorInString && !expected.cursorInComment) {
          expect(context.currentClause).toBe(expected.currentClause)
          expect(context.expectsColumn).toBe(expected.expectsColumn || false)
          expect(context.expectsFunction).toBe(
            expected.expectsFunction ?? false,
          )
          expect(context.currentPrefix).toBe(expected.currentPrefix ?? "")

          if (expected.availableTablesContains) {
            expect(context.availableTables).toContain(
              expected.availableTablesContains,
            )
          }
        }
      },
    )
  })
})
