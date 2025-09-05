/**
 * SQL Intellisense Types
 *
 * This module contains all TypeScript type definitions for the SQL intellisense system.
 * These types provide a foundation for context-aware SQL completions, including
 * table/column suggestions, UDF snippets, and query analysis.
 *
 * Key Components:
 * - UserDefinedFunction: Enhanced UDF interface with snippet support
 * - QueryContext: Context analysis results for cursor position
 * - CompletionConfig: Provider configuration and behavior settings
 * - DisposableHandle: Monaco provider lifecycle management
 *
 * @file types.ts
 * @author SQL Intellisense System
 */
import type { Position } from "monaco-editor/esm/vs/editor/editor.api"
import type { DatasetMetadata } from "nozzl/Studio/Model"

// Keep only MonacoITextModel as it's still used in QueryContextAnalyzer
export interface MonacoITextModel {
  getValue: () => string
  getOffsetAt: (position: Position) => number
  getLineContent: (lineNumber: number) => string
  getWordAtPosition: (
    position: Position,
  ) => { word: string; startColumn: number; endColumn: number } | null
}

/**
 * Enhanced User-Defined Function interface
 *
 * Extends the basic UDF structure from useUDFQuery with additional
 * metadata needed for intelligent completions and snippet generation.
 *
 * @interface UserDefinedFunction
 */
export interface UserDefinedFunction {
  /** The function name (e.g., 'evm_decode_log', '${dataset}.eth_call') */
  name: string

  /** Human-readable description of the function's purpose */
  description: string

  /** SQL function signature for documentation */
  sql: string

  /** Optional parameter names for snippet generation */
  parameters?: Array<string>

  /** Optional usage example for documentation */
  example?: string

  /** Return type information (optional) */
  returnType?: string

  /** Whether the function requires dataset prefix (e.g., anvil.eth_call) */
  requiresDatasetPrefix?: boolean
}

/**
 * Query Context Analysis Result
 *
 * Contains the analysis of the current cursor position within a SQL query.
 * This drives context-aware completions by indicating what type of suggestions
 * are appropriate at the current location.
 *
 * @interface QueryContext
 */
export interface QueryContext {
  /** True if cursor is in a position where table names are expected (e.g., after FROM) */
  expectsTable: boolean

  /** True if cursor is in a position where column names are expected (e.g., in SELECT) */
  expectsColumn: boolean

  /** True if cursor is in a position where function calls are expected */
  expectsFunction: boolean

  /** True if cursor is in a position where SQL keywords are expected */
  expectsKeyword: boolean

  /** True if cursor is in a position where operators are expected */
  expectsOperator: boolean

  /** List of table names that are in scope at the current cursor position */
  availableTables: Array<string>

  /** Map of table aliases to their full table names (e.g., 'l' -> 'anvil.logs') */
  tableAliases: Map<string, string>

  /** Current word prefix being typed (for filtering completions) */
  currentPrefix: string

  /** True if cursor is inside a string literal (should avoid completions) */
  cursorInString: boolean

  /** True if cursor is inside a comment (should avoid completions) */
  cursorInComment: boolean

  /** The SQL clause where the cursor is located (SELECT, FROM, WHERE, etc.) */
  currentClause: SqlClause | null

  /** Nesting depth for subqueries (0 = main query, 1+ = nested) */
  subqueryDepth: number

  /** Parent query contexts for nested queries */
  parentContexts: Array<QueryContext>
}

/**
 * SQL Clause Types
 *
 * Enumeration of SQL clauses where the cursor can be positioned.
 * Used to provide context-specific completions.
 */
export type SqlClause =
  | "SELECT"
  | "FROM"
  | "WHERE"
  | "JOIN"
  | "INNER_JOIN"
  | "LEFT_JOIN"
  | "RIGHT_JOIN"
  | "ON"
  | "GROUP_BY"
  | "HAVING"
  | "ORDER_BY"
  | "LIMIT"
  | "WITH"

/**
 * Completion Provider Configuration
 *
 * Settings that control the behavior of the completion provider,
 * including performance limits and feature toggles.
 *
 * @interface CompletionConfig
 */
export interface CompletionConfig {
  /** Minimum prefix length before showing completions (performance optimization) */
  minPrefixLength: number

  /** Maximum number of completion suggestions to show */
  maxSuggestions: number

  /** Whether to enable snippet-based completions for UDFs */
  enableSnippets: boolean

  /** Whether to enable context-aware filtering */
  enableContextFiltering: boolean

  /** Whether to enable table alias resolution */
  enableAliasResolution: boolean

  /** Cache TTL for context analysis results (milliseconds) */
  contextCacheTTL: number

  /** Whether to log debug information for completion analysis */
  enableDebugLogging: boolean
}

/**
 * Default completion configuration
 *
 * Provides sensible defaults for the completion provider behavior.
 * Can be overridden when creating the provider instance.
 */
export const DEFAULT_COMPLETION_CONFIG: CompletionConfig = {
  minPrefixLength: 0, // Show completions immediately
  maxSuggestions: 200, // Reasonable limit to avoid UI clutter
  enableSnippets: true, // Enable UDF snippets by default
  enableContextFiltering: true, // Enable smart context filtering
  enableAliasResolution: true, // Enable table alias support
  contextCacheTTL: 30 * 1000, // 30 second cache TTL
  enableDebugLogging: false, // Disable debug logging in production
}

/**
 * Monaco Editor Disposable Handle
 *
 * Provides a unified interface for disposing Monaco editor providers
 * and cleaning up resources to prevent memory leaks.
 *
 * @interface DisposableHandle
 */
export interface DisposableHandle {
  /** Cleanup function to dispose all registered providers and caches */
  dispose: () => void
}

// Removed unused interfaces:
// - CompletionItemOptions (no longer used after Monaco refactor)
// - TableInfo (not used - we use DatasetMetadata directly)
// - ColumnInfo (not used - we use DatasetMetadata directly)

/**
 * SQL Token Types
 *
 * Enumeration of SQL token types for basic parsing and context analysis.
 * Used by the QueryContextAnalyzer to understand query structure.
 */
export type SqlTokenType =
  | "KEYWORD" // SELECT, FROM, WHERE, etc.
  | "IDENTIFIER" // Table names, column names, aliases
  | "STRING" // String literals
  | "NUMBER" // Numeric literals
  | "OPERATOR" // =, <, >, LIKE, etc.
  | "DELIMITER" // Commas, parentheses, etc.
  | "WHITESPACE" // Spaces, tabs, newlines
  | "COMMENT" // -- comments and /* */ comments
  | "UNKNOWN" // Unrecognized tokens

/**
 * SQL Token
 *
 * Represents a single token in a SQL query with position information.
 * Used for basic parsing and context analysis.
 *
 * @interface SqlToken
 */
export interface SqlToken {
  /** Token type classification */
  type: SqlTokenType

  /** Actual token text */
  value: string

  /** Start position in the original query string */
  startOffset: number

  /** End position in the original query string */
  endOffset: number

  /** Line number (1-based) */
  line: number

  /** Column number (1-based) */
  column: number
}

/**
 * Completion Priority Constants
 *
 * Defines sort order priority for different completion types.
 * Lower numbers appear first in the completion list.
 */
export const COMPLETION_PRIORITY = {
  /** Tables (highest priority in FROM clause) */
  TABLE: "1",
  /** Columns from available tables */
  COLUMN: "2",
  /** User-defined functions */
  UDF: "3",
  /** SQL keywords */
  KEYWORD: "4",
  /** Operators */
  OPERATOR: "5",
} as const

// Removed ErrorRecovery interface - not used in the codebase

/**
 * Performance Metrics Interface
 *
 * Optional interface for tracking completion provider performance.
 * Useful for monitoring and optimization in production.
 *
 * @interface PerformanceMetrics
 */
export interface PerformanceMetrics {
  /** Total number of completion requests */
  totalRequests: number

  /** Average response time in milliseconds */
  averageResponseTime: number

  /** Cache hit rate (0-1) */
  cacheHitRate: number

  /** Number of failed completions */
  failureCount: number

  /** Last update timestamp */
  lastUpdate: number
}

// Re-export DatasetMetadata type for convenience
export type { DatasetMetadata }
