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
import type { IMarkdownString, IRange, languages, Position } from 'monaco-editor/esm/vs/editor/editor.api';
import type { DatasetMetadata } from 'nozzl/Studio/Model'

// Monaco Editor types - defined locally to avoid import issues
// Using ES2015 module syntax instead of namespaces

export interface MonacoCompletionItem {
  label: string | { label: string; detail?: string }
  kind?: languages.CompletionItemKind
  detail?: string
  documentation?: string | IMarkdownString
  sortText?: string
  filterText?: string
  preselect?: boolean
  insertText?: string
  insertTextRules?: languages.CompletionItemInsertTextRule
  range?: IRange | undefined
  command?: MonacoCommand | undefined
}

export interface MonacoCommand {
  id: string
  title: string
  arguments?: Array<any>
}

export enum MonacoCompletionItemKind {
  Function = 1,
  Field = 3,
  Class = 5,
  Keyword = 17,
  Operator = 11
}

export enum MonacoCompletionItemInsertTextRule {
  InsertAsSnippet = 4
}

export enum MonacoCompletionItemTag {
  Deprecated = 1
}

export interface MonacoIMarkdownString {
  value: string
  isTrusted?: boolean
}

export interface MonacoRange {
  startLineNumber: number
  startColumn: number
  endLineNumber: number
  endColumn: number
}

export interface MonacoPosition {
  lineNumber: number
  column: number
}

export interface MonacoITextModel {
  getValue: () => string
  getOffsetAt: (position: Position) => number
  getLineContent: (lineNumber: number) => string
  getWordAtPosition: (position: Position) => { word: string; startColumn: number; endColumn: number } | null
}

export interface MonacoCancellationToken {
  isCancellationRequested: boolean
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
  | 'SELECT'
  | 'FROM' 
  | 'WHERE'
  | 'JOIN'
  | 'INNER_JOIN'
  | 'LEFT_JOIN'
  | 'RIGHT_JOIN'
  | 'ON'
  | 'GROUP_BY'
  | 'HAVING'
  | 'ORDER_BY'
  | 'LIMIT'
  | 'WITH'

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
  minPrefixLength: 0,          // Show completions immediately
  maxSuggestions: 200,         // Reasonable limit to avoid UI clutter
  enableSnippets: true,        // Enable UDF snippets by default
  enableContextFiltering: true, // Enable smart context filtering
  enableAliasResolution: true, // Enable table alias support
  contextCacheTTL: 30 * 1000,  // 30 second cache TTL
  enableDebugLogging: false    // Disable debug logging in production
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

/**
 * Completion Item Factory Options
 * 
 * Configuration options for creating Monaco completion items
 * with consistent formatting and behavior.
 * 
 * @interface CompletionItemOptions
 */
export interface CompletionItemOptions {
  /** Completion item label (what the user sees) */
  label: string
  
  /** Monaco completion item kind (determines icon) */
  kind: MonacoCompletionItemKind
  
  /** Text to insert when completion is selected */
  insertText: string
  
  /** Brief description shown in completion popup */
  detail?: string
  
  /** Detailed documentation (supports markdown) */
  documentation?: string | MonacoIMarkdownString
  
  /** Sort order (lower values appear first) */
  sortText?: string
  
  /** Whether this item should be preselected */
  preselect?: boolean
  
  /** Text replacement range */
  range?: MonacoRange
  
  /** Whether to insert as snippet (for UDFs) */
  insertAsSnippet?: boolean
  
  /** Command to execute after insertion (e.g., trigger parameter hints) */
  command?: MonacoCommand
}

/**
 * Table Metadata for Completions
 * 
 * Simplified interface representing table information needed
 * for generating completions. Maps from the DatasetMetadata type.
 * 
 * @interface TableInfo
 */
export interface TableInfo {
  /** Full table name (e.g., 'anvil.logs') */
  name: string
  
  /** Table columns with type information */
  columns: Array<ColumnInfo>
  
  /** Optional table description */
  description?: string
}

/**
 * Column Metadata for Completions
 * 
 * Simplified interface representing column information needed
 * for generating completions.
 * 
 * @interface ColumnInfo
 */
export interface ColumnInfo {
  /** Column name (e.g., 'block_number') */
  name: string
  
  /** Column data type (e.g., 'bigint', 'address') */
  dataType: string
  
  /** Optional column description */
  description?: string
}

/**
 * SQL Token Types
 * 
 * Enumeration of SQL token types for basic parsing and context analysis.
 * Used by the QueryContextAnalyzer to understand query structure.
 */
export type SqlTokenType =
  | 'KEYWORD'      // SELECT, FROM, WHERE, etc.
  | 'IDENTIFIER'   // Table names, column names, aliases
  | 'STRING'       // String literals
  | 'NUMBER'       // Numeric literals
  | 'OPERATOR'     // =, <, >, LIKE, etc.
  | 'DELIMITER'    // Commas, parentheses, etc.
  | 'WHITESPACE'   // Spaces, tabs, newlines
  | 'COMMENT'      // -- comments and /* */ comments
  | 'UNKNOWN'      // Unrecognized tokens

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
  TABLE: '1',
  /** Columns from available tables */
  COLUMN: '2', 
  /** User-defined functions */
  UDF: '3',
  /** SQL keywords */
  KEYWORD: '4',
  /** Operators */
  OPERATOR: '5'
} as const

/**
 * Error Recovery Interface
 * 
 * Provides fallback completions when context analysis fails
 * or when dealing with malformed queries.
 * 
 * @interface ErrorRecovery
 */
export interface ErrorRecovery {
  /** Whether error recovery is enabled */
  enabled: boolean
  
  /** Fallback completions to show on analysis failure */
  fallbackCompletions: Array<MonacoCompletionItem>
  
  /** Maximum number of fallback completions */
  maxFallbackItems: number
}

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
