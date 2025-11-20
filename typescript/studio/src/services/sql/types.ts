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
import type { StudioModel } from "@edgeandnode/amp"
import type {
  CancellationToken,
  editor,
  IMarkdownString,
  languages,
  MarkerSeverity,
  Position,
  Range,
} from "monaco-editor"

import type { UserDefinedFunctionName } from "../../constants.ts"

// Re-export Monaco types for convenience
export type { CancellationToken, IMarkdownString, MarkerSeverity, Position, Range }

// Re-export Monaco editor and languages namespaces
export type { editor, languages }
// Simplified interface for test mocks and minimal Monaco integration
// This interface provides only the essential methods needed by our SQL analysis
export interface MonacoITextModel {
  getValue: () => string
  getOffsetAt: (position: Position) => number
  getLineContent: (lineNumber: number) => string
  getWordAtPosition: (position: Position) => { word: string; startColumn: number; endColumn: number } | null
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
  name: UserDefinedFunctionName

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

  // SQL Validation Feature Flags
  /** Whether to enable SQL validation and error markers */
  enableSqlValidation: boolean

  /** Validation level: 'basic' (syntax only), 'standard' (+ tables), 'full' (+ columns) */
  validationLevel: "basic" | "standard" | "full"

  /** Whether to enable validation for incomplete/partial queries as user types */
  enablePartialValidation: boolean
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

  // SQL Validation defaults
  enableSqlValidation: true, // Enable validation by default
  validationLevel: "standard", // Start with table validation
  enablePartialValidation: true, // Enable validation while typing
}

/**
 * Default validation cache configuration
 *
 * Provides default caching configuration for validation results.
 */
export const DEFAULT_VALIDATION_CACHE_CONFIG: ValidationCacheConfig = {
  maxEntries: 100, // Cache up to 100 queries
  ttl: 5 * 60 * 1000, // 5 minute TTL
  keyFunction: (query: string) => {
    // Simple hash function for cache keys
    let hash = 0
    const trimmed = query.trim().toLowerCase()
    for (let i = 0; i < trimmed.length; i++) {
      const char = trimmed.charCodeAt(i)
      hash = (hash << 5) - hash + char
      hash = hash & hash // Convert to 32-bit integer
    }
    return hash.toString()
  },
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
// - TableInfo (not used - we use DatasetSource directly)
// - ColumnInfo (not used - we use DatasetSource directly)

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
 * SQL Validation Error
 *
 * Represents a validation error found in a SQL query with position information
 * compatible with Monaco Editor's marker system.
 *
 * @interface SqlValidationError
 */
export interface SqlValidationError {
  /** Human-readable error message */
  message: string

  /** Error severity level (Error, Warning, Info) */
  severity: MarkerSeverity

  /** Start line number (1-based) */
  startLineNumber: number

  /** Start column number (1-based) */
  startColumn: number

  /** End line number (1-based) */
  endLineNumber: number

  /** End column number (1-based) */
  endColumn: number

  /** Error code for categorization */
  code?: string

  /** Additional error data for debugging */
  data?: any
}

/**
 * Validation Cache Configuration
 *
 * Configuration for validation result caching to improve performance.
 *
 * @interface ValidationCacheConfig
 */
export interface ValidationCacheConfig {
  /** Maximum number of cached validation results */
  maxEntries: number

  /** Time-to-live for cached results in milliseconds */
  ttl: number

  /** Function to generate cache keys from query strings */
  keyFunction: (query: string) => string
}

// MonacoMarkerSeverity is now replaced by directly imported MarkerSeverity
// This re-export is kept for backward compatibility during migration
export type { MarkerSeverity as MonacoMarkerSeverity }

/**
 * Validation Cache
 *
 * LRU cache for validation results to improve performance.
 * Implements a simple least-recently-used eviction policy.
 */
export class ValidationCache {
  private cache = new Map<string, { errors: Array<SqlValidationError>; timestamp: number; accessCount: number }>()
  private accessOrder: Array<string> = []
  private config: ValidationCacheConfig

  constructor(config: ValidationCacheConfig = DEFAULT_VALIDATION_CACHE_CONFIG) {
    this.config = config
  }

  /**
   * Get cached validation results for a query
   */
  get(query: string): Array<SqlValidationError> | null {
    const key = this.config.keyFunction(query)
    const entry = this.cache.get(key)

    if (!entry) {
      return null
    }

    // Check if entry has expired
    if (Date.now() - entry.timestamp > this.config.ttl) {
      this.delete(key)
      return null
    }

    // Update access order and count
    entry.accessCount++
    this.moveToEnd(key)

    return entry.errors
  }

  /**
   * Set validation results in cache
   */
  set(query: string, errors: Array<SqlValidationError>): void {
    const key = this.config.keyFunction(query)

    // If cache is full, evict least recently used
    if (this.cache.size >= this.config.maxEntries && !this.cache.has(key)) {
      this.evictLRU()
    }

    this.cache.set(key, {
      errors: [...errors], // Create defensive copy
      timestamp: Date.now(),
      accessCount: 1,
    })

    this.moveToEnd(key)
  }

  /**
   * Clear all cached entries
   */
  clear(): void {
    this.cache.clear()
    this.accessOrder = []
  }

  /**
   * Get cache statistics
   */
  getStats(): {
    size: number
    maxSize: number
    hitRate: number
  } {
    const totalAccess = Array.from(this.cache.values()).reduce((sum, entry) => sum + entry.accessCount, 0)

    return {
      size: this.cache.size,
      maxSize: this.config.maxEntries,
      hitRate: totalAccess > 0 ? this.cache.size / totalAccess : 0,
    }
  }

  private moveToEnd(key: string): void {
    const index = this.accessOrder.indexOf(key)
    if (index > -1) {
      this.accessOrder.splice(index, 1)
    }
    this.accessOrder.push(key)
  }

  private evictLRU(): void {
    if (this.accessOrder.length > 0) {
      const lruKey = this.accessOrder.shift()!
      this.cache.delete(lruKey)
    }
  }

  private delete(key: string): void {
    this.cache.delete(key)
    const index = this.accessOrder.indexOf(key)
    if (index > -1) {
      this.accessOrder.splice(index, 1)
    }
  }
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

/**
 * Extended DatasetSource type to handle different metadata structures
 * Some datasets have additional properties or alternative column formats
 */
export type ExtendedDatasetSource = StudioModel.DatasetSource & {
  // Alternative properties that may exist in some datasets
  destination?: string
  dataset_name?: string
  table_name?: string
  column_names?: Array<string>
}

/**
 * Type guard to check if dataset has metadata_columns
 */
export function hasMetadataColumns(dataset: ExtendedDatasetSource): dataset is ExtendedDatasetSource & {
  metadata_columns: Array<{ name: string; datatype: string }>
} {
  return "metadata_columns" in dataset && Array.isArray(dataset.metadata_columns)
}

/**
 * Type guard to check if dataset has column_names
 */
export function hasColumnNames(dataset: ExtendedDatasetSource): dataset is ExtendedDatasetSource & {
  column_names: Array<string>
} {
  return "column_names" in dataset && Array.isArray(dataset.column_names)
}

/**
 * Helper to get column names from dataset regardless of structure
 */
export function getColumnNames(dataset: ExtendedDatasetSource): Array<string> {
  if (hasMetadataColumns(dataset)) {
    return dataset.metadata_columns.map((col) => col.name)
  } else if (hasColumnNames(dataset)) {
    return dataset.column_names
  }
  return []
}

/**
 * Helper to get full table name from dataset
 */
export function getFullTableName(dataset: ExtendedDatasetSource): string | null {
  if (dataset.dataset_name && dataset.table_name) {
    return `${dataset.dataset_name}.${dataset.table_name}`
  }
  return null
}

/**
 * Manifest-based Table Metadata
 *
 * Represents column metadata extracted from DatasetManifest Arrow schemas.
 * This is used for intellisense on materialized datasets defined in amp configs.
 *
 * @interface ManifestTableMetadata
 */
export interface ManifestTableMetadata {
  /** Fully qualified table name (e.g., "example.blocks") */
  source: string

  /** Network the table belongs to (optional, defaults to "unknown" for derived datasets) */
  network: string | undefined

  /** Column definitions from Arrow schema */
  columns: Array<{
    /** Column name */
    name: string

    /** SQL-like type string for display (e.g., "BIGINT", "TIMESTAMP") */
    datatype: string

    /** Whether the column is nullable */
    nullable: boolean
  }>
}

/**
 * Unified metadata that can come from either DatasetSource (raw) or DatasetManifest (materialized)
 */
export type UnifiedTableMetadata = ManifestTableMetadata
