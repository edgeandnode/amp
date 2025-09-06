/**
 * Mock SQL queries for testing context analysis and completion features
 * Covers various SQL patterns and edge cases
 */

import * as monaco from "monaco-editor/esm/vs/editor/editor.api"

export interface TestQuery {
  /** The SQL query text */
  query: string
  /** Cursor position for testing completions */
  position: monaco.Position
  /** Expected context analysis results */
  expectedContext?: {
    expectsTable?: boolean
    expectsColumn?: boolean
    expectsFunction?: boolean
    currentClause?: string
    cursorInString?: boolean
    cursorInComment?: boolean
  }
  /** Description of what this query tests */
  description: string
}

/**
 * Basic SQL queries for fundamental context analysis
 */
export const basicQueries: Array<TestQuery> = [
  {
    query: "SELECT * FROM ",
    position: new monaco.Position(1, 15),
    expectedContext: {
      expectsTable: true,
      expectsColumn: false,
      currentClause: "FROM",
    },
    description: "Basic table completion after FROM",
  },
  {
    query: "SELECT ",
    position: new monaco.Position(1, 8),
    expectedContext: {
      expectsTable: false,
      expectsColumn: true,
      expectsFunction: true,
      currentClause: "SELECT",
    },
    description: "Column and function completion in SELECT",
  },
  {
    query: "SELECT * FROM anvil.logs WHERE ",
    position: new monaco.Position(1, 33),
    expectedContext: {
      expectsColumn: true,
      currentClause: "WHERE",
    },
    description: "Column completion in WHERE clause",
  },
  {
    query: "SELECT address, ",
    position: new monaco.Position(1, 17),
    expectedContext: {
      expectsColumn: true,
      expectsFunction: true,
      currentClause: "SELECT",
    },
    description: "Additional column completion after comma",
  },
]

/**
 * Qualified name queries for testing dataset.table parsing
 */
export const qualifiedNameQueries: Array<TestQuery> = [
  {
    query: "SELECT * FROM anvil.logs.",
    position: new monaco.Position(1, 26),
    expectedContext: {
      expectsColumn: true,
      currentClause: "FROM",
    },
    description: "Column completion after qualified table name with dot",
  },
  {
    query: "SELECT anvil.logs.address FROM anvil.logs",
    position: new monaco.Position(1, 8),
    expectedContext: {
      expectsColumn: true,
      currentClause: "SELECT",
    },
    description: "Qualified column reference completion",
  },
]

/**
 * UDF-related queries for testing function completions
 */
export const udfQueries: Array<TestQuery> = [
  {
    query: "SELECT evm_",
    position: new monaco.Position(1, 12),
    expectedContext: {
      expectsFunction: true,
      currentClause: "SELECT",
    },
    description: "UDF completion with evm_ prefix",
  },
  {
    query: "SELECT evm_decode_log(",
    position: new monaco.Position(1, 23),
    expectedContext: {
      expectsColumn: true,
      currentClause: "SELECT",
    },
    description: "Parameter completion inside UDF call",
  },
  {
    query: "SELECT anvil.eth_call(",
    position: new monaco.Position(1, 23),
    expectedContext: {
      expectsColumn: true,
      currentClause: "SELECT",
    },
    description: "Dataset-specific UDF parameter completion",
  },
]

/**
 * Complex queries with JOINs and aliases
 */
export const complexQueries: Array<TestQuery> = [
  {
    query: "SELECT l.address FROM anvil.logs l JOIN anvil.transactions t ON l.transaction_hash = t.hash WHERE ",
    position: new monaco.Position(1, 101),
    expectedContext: {
      expectsColumn: true,
      currentClause: "WHERE",
    },
    description: "Column completion in complex JOIN query with aliases",
  },
  {
    query: "SELECT * FROM anvil.logs l LEFT JOIN ",
    position: new monaco.Position(1, 38),
    expectedContext: {
      expectsTable: true,
      currentClause: "JOIN",
    },
    description: "Table completion after LEFT JOIN",
  },
]

/**
 * Queries with string literals and comments
 */
export const stringAndCommentQueries: Array<TestQuery> = [
  {
    query: "SELECT * FROM anvil.logs WHERE address = '",
    position: new monaco.Position(1, 44),
    expectedContext: {
      cursorInString: true,
      currentClause: "WHERE",
    },
    description: "Cursor inside string literal",
  },
  {
    query: "SELECT * -- this is a comment with cursor here ",
    position: new monaco.Position(1, 49),
    expectedContext: {
      cursorInComment: true,
    },
    description: "Cursor inside line comment",
  },
  {
    query: "SELECT * /* cursor in block comment */ FROM anvil.logs",
    position: new monaco.Position(1, 25),
    expectedContext: {
      cursorInComment: true,
    },
    description: "Cursor inside block comment",
  },
]

/**
 * Edge case queries for testing error recovery and malformed SQL
 */
export const edgeCaseQueries: Array<TestQuery> = [
  {
    query: "SELECT FROM ",
    position: new monaco.Position(1, 13),
    expectedContext: {
      expectsTable: true,
      currentClause: "FROM",
    },
    description: "Malformed query missing columns - should still provide table completion",
  },
  {
    query: "SELEC * FROM ",
    position: new monaco.Position(1, 14),
    expectedContext: {
      expectsTable: true,
    },
    description: "Typo in SELECT keyword - should still provide table completion",
  },
  {
    query: "",
    position: new monaco.Position(1, 1),
    expectedContext: {
      expectsTable: false,
      expectsColumn: false,
      expectsFunction: false,
    },
    description: "Empty query - should provide basic keyword completions",
  },
]

/**
 * All test queries combined for comprehensive testing
 */
export const allTestQueries = [
  ...basicQueries,
  ...qualifiedNameQueries,
  ...udfQueries,
  ...complexQueries,
  ...stringAndCommentQueries,
  ...edgeCaseQueries,
]

/**
 * Helper function to create Monaco Position from line/column
 */
export function createPosition(line: number, column: number): monaco.Position {
  return new monaco.Position(line, column)
}

/**
 * Helper function to get queries by category
 */
export function getQueriesByCategory(category: string): Array<TestQuery> {
  switch (category) {
    case "basic":
      return basicQueries
    case "qualified":
      return qualifiedNameQueries
    case "udf":
      return udfQueries
    case "complex":
      return complexQueries
    case "string-comment":
      return stringAndCommentQueries
    case "edge-case":
      return edgeCaseQueries
    default:
      return allTestQueries
  }
}
