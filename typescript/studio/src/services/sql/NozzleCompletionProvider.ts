/**
 * Nozzle SQL Completion Provider
 *
 * This module implements the main completion provider for Nozzle SQL intellisense.
 * It integrates with Monaco Editor to provide context-aware SQL completions including:
 *
 * - Table suggestions from metadata API
 * - Column suggestions filtered by available tables
 * - User-defined function completions with snippets
 * - Context-aware SQL keyword suggestions
 * - Table alias resolution and completion
 *
 * The provider uses the QueryContextAnalyzer to understand cursor position context
 * and provides intelligent, filtered suggestions based on the current query state.
 *
 * @file completionProvider.ts
 * @author SQL Intellisense System
 */

// Monaco types are defined in types.ts to avoid import issues
import { Array } from "effect"
import type {
  CancellationToken,
  editor,
  IMarkdownString,
  IRange,
  Position,
} from "monaco-editor/esm/vs/editor/editor.api"
import { languages } from "monaco-editor/esm/vs/editor/editor.api"
import type { DatasetMetadata } from "nozzl/Studio/Model"

import { QueryContextAnalyzer } from "./QueryContextAnalyzer"
import type { CompletionConfig, QueryContext, UserDefinedFunction } from "./types"

import { COMPLETION_PRIORITY, DEFAULT_COMPLETION_CONFIG } from "./types"

/**
 * Nozzle SQL Completion Provider
 *
 * Main completion provider class that implements Monaco's CompletionItemProvider
 * interface. Provides intelligent, context-aware SQL completions for Nozzle queries.
 *
 * Features:
 * - Context-aware table and column suggestions
 * - UDF function completions with parameter snippets
 * - SQL keyword completions based on current clause
 * - Performance optimizations with caching and filtering
 * - Error recovery for malformed queries
 */
export class NozzleCompletionProvider implements languages.CompletionItemProvider {
  private analyzer: QueryContextAnalyzer
  private config: CompletionConfig

  // Core SQL keywords organized by context
  private readonly sqlKeywords = {
    clauses: [
      "SELECT",
      "FROM",
      "WHERE",
      "JOIN",
      "INNER JOIN",
      "LEFT JOIN",
      "RIGHT JOIN",
      "ON",
      "GROUP BY",
      "HAVING",
      "ORDER BY",
      "LIMIT",
      "OFFSET",
      "WITH",
      "UNION",
      "EXCEPT",
      "INTERSECT",
    ],
    functions: [
      "COUNT",
      "SUM",
      "AVG",
      "MIN",
      "MAX",
      "DISTINCT",
      "CASE",
      "WHEN",
      "THEN",
      "ELSE",
      "END",
    ],
    operators: [
      "AND",
      "OR",
      "NOT",
      "IN",
      "EXISTS",
      "BETWEEN",
      "LIKE",
      "ILIKE",
      "IS NULL",
      "IS NOT NULL",
    ],
    modifiers: ["ASC", "DESC", "DISTINCT", "ALL", "AS"],
  }

  constructor(
    private metadata: ReadonlyArray<DatasetMetadata>,
    private udfs: ReadonlyArray<UserDefinedFunction>,
    analyzer?: QueryContextAnalyzer,
    config: Partial<CompletionConfig> = DEFAULT_COMPLETION_CONFIG,
  ) {
    this.analyzer = analyzer || new QueryContextAnalyzer(config)
    this.config = {
      ...DEFAULT_COMPLETION_CONFIG,
      ...config,
    }
  }

  /**
   * Provide Completion Items (Monaco Interface Implementation)
   *
   * Main entry point called by Monaco Editor when user requests completions.
   * Returns a list of completion suggestions based on cursor position and context.
   *
   * @param model - Monaco text model containing the query
   * @param position - Current cursor position
   * @param context - Monaco completion context
   * @param token - Cancellation token
   * @returns Promise resolving to completion list
   */
  provideCompletionItems(
    model: editor.ITextModel,
    position: Position,
    _context: languages.CompletionContext,
    token: CancellationToken,
  ): languages.ProviderResult<languages.CompletionList> {
    try {
      // Check if request has been cancelled
      if (token.isCancellationRequested) {
        return { suggestions: [] }
      }

      // Analyze query context to determine what completions are appropriate
      const queryContext = this.analyzer.analyzeContext(model, position)

      // Don't provide completions if cursor is in string/comment
      if (queryContext.cursorInString || queryContext.cursorInComment) {
        return { suggestions: [] }
      }

      // Apply minimum prefix length filter
      if (queryContext.currentPrefix.length < this.config.minPrefixLength) {
        return { suggestions: [] }
      }

      // Generate completions based on context
      let suggestions: Array<languages.CompletionItem> = []

      // 1. Table completions (highest priority in appropriate contexts)
      if (queryContext.expectsTable) {
        const tableCompletions = this.createTableCompletions(position)
        suggestions = Array.appendAll(tableCompletions)(suggestions)
      }

      // 2. Column completions (context-filtered)
      if (queryContext.expectsColumn) {
        const columnCompletions = this.createColumnCompletions(
          queryContext,
          position,
        )
        suggestions = Array.appendAll(columnCompletions)(suggestions)
      }

      // 3. UDF function completions
      if (queryContext.expectsFunction) {
        const udfCompletions = this.createUDFCompletions(position)
        suggestions = Array.appendAll(udfCompletions)(suggestions)
      }

      // 4. SQL keyword completions
      if (queryContext.expectsKeyword) {
        const keywordCompletions = this.createKeywordCompletions(
          queryContext,
          position,
        )
        suggestions = Array.appendAll(keywordCompletions)(suggestions)
      }

      // 5. Operator completions
      if (queryContext.expectsOperator) {
        const operatorCompletions = this.createOperatorCompletions(position)
        suggestions = Array.appendAll(operatorCompletions)(suggestions)
      }

      // Filter by prefix and apply limits
      const filteredSuggestions = this.filterAndLimitSuggestions(
        suggestions,
        queryContext,
      )

      // Add range information for text replacement
      const suggestionsWithRange = this.addRangeToSuggestions(
        filteredSuggestions,
        position,
        queryContext.currentPrefix,
      )

      return {
        suggestions: suggestionsWithRange,
        incomplete: false, // We provide all available completions
      }
    } catch (error) {
      this.logError("Completion provider failed", error)
      return this.getFallbackCompletions(position)
    }
  }

  /**
   * Create Table Completions
   *
   * Generates completion items for database tables based on the current metadata.
   * Provides detailed documentation showing available columns.
   *
   * @private
   * @returns Array of table completion items
   */
  private createTableCompletions(
    position: Position,
  ): Array<languages.CompletionItem> {
    const completions: Array<languages.CompletionItem> = []

    this.metadata.forEach((dataset, index) => {
      // Create detailed documentation showing table schema
      const columnList = dataset.metadata_columns
        .map((col) => `- \`${col.name}\` (${col.datatype})`)
        .join("\n")

      const documentation: IMarkdownString = {
        value: [
          `**Dataset Table: ${dataset.source}**`,
          "",
          `Contains ${dataset.metadata_columns.length} columns:`,
          "",
          columnList,
          "",
          "*Click to insert table name in query*",
        ].join("\n"),
        isTrusted: true,
      }

      completions.push({
        label: dataset.source,
        kind: languages.CompletionItemKind.Class,
        detail: `Table (${dataset.metadata_columns.length} columns)`,
        documentation,
        insertText: dataset.source,
        sortText: `${COMPLETION_PRIORITY.TABLE}-${index.toString().padStart(3, "0")}`,
        preselect: index === 0, // Preselect first table
        filterText: dataset.source,
        // Add command to trigger parameter hints if this is a function-like context
        command: {
          id: "editor.action.triggerSuggest",
          title: "Re-trigger completion",
        },
        range: {
          startColumn: position.column,
          endColumn: position.column,
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
        },
      })
    })

    return completions
  }

  /**
   * Create Column Completions
   *
   * Generates completion items for table columns, filtered by tables that are
   * currently in scope (referenced in FROM clause or via aliases).
   *
   * @private
   * @param queryContext - Current query context
   * @returns Array of column completion items
   */
  private createColumnCompletions(
    queryContext: QueryContext,
    position: Position,
  ): Array<languages.CompletionItem> {
    const completions: Array<languages.CompletionItem> = []
    let columnIndex = 0

    this.metadata.forEach((dataset) => {
      // Skip tables not in scope (unless no tables are specified, then include all)
      if (
        queryContext.availableTables.length > 0 &&
        !queryContext.availableTables.includes(dataset.source)
      ) {
        return
      }

      dataset.metadata_columns.forEach((column) => {
        const documentation: IMarkdownString = {
          value: [
            `**Column: ${column.name}**`,
            `**Type:** ${column.datatype}`,
            `**Table:** ${dataset.source}`,
            "",
            "*Click to insert column name in query*",
          ].join("\n"),
          isTrusted: true,
        }

        completions.push({
          label: column.name,
          kind: languages.CompletionItemKind.Field,
          detail: `${column.datatype} - ${dataset.source}`,
          documentation,
          insertText: column.name,
          sortText: `${COMPLETION_PRIORITY.COLUMN}-${columnIndex.toString().padStart(4, "0")}`,
          filterText: column.name,
          range: {
            startLineNumber: position.lineNumber,
            startColumn: position.column,
            endLineNumber: position.lineNumber,
            endColumn: position.column,
          },
        })

        columnIndex++
      })
    })

    return completions
  }

  /**
   * Create UDF Completions
   *
   * Generates completion items for User-Defined Functions with intelligent
   * snippet insertion and parameter placeholders.
   *
   * @private
   * @param queryContext - Current query context
   * @returns Array of UDF completion items
   */
  private createUDFCompletions(
    position: Position,
  ): Array<languages.CompletionItem> {
    const completions: Array<languages.CompletionItem> = []

    this.udfs.forEach((udf, index) => {
      // Generate snippet with parameter placeholders
      const snippet = this.createUDFSnippet(udf)

      // Clean up display name for functions with dataset prefix
      const displayName = udf.name.replace("${dataset}", "{dataset}")

      const documentation: IMarkdownString = {
        value: [
          `**${displayName}** - Nozzle User-Defined Function`,
          "",
          udf.description,
          "",
          "**SQL Signature:**",
          "```sql",
          udf.sql.trim(),
          "```",
          "",
          udf.example ? `**Example:**\n\`\`\`sql\n${udf.example}\n\`\`\`` : "",
          "",
          "💡 **Tip:** Use Tab to navigate between parameters after insertion",
        ].join("\n"),
        isTrusted: true,
      }

      completions.push({
        label: displayName,
        kind: languages.CompletionItemKind.Function,
        detail: "Nozzle UDF",
        documentation,
        insertText: snippet,
        insertTextRules: languages.CompletionItemInsertTextRule.InsertAsSnippet,
        sortText: `${COMPLETION_PRIORITY.UDF}-${index.toString().padStart(3, "0")}`,
        filterText: udf.name,
        // Trigger parameter hints after insertion
        command: {
          id: "editor.action.triggerParameterHints",
          title: "Trigger Parameter Hints",
        },
        range: {
          startLineNumber: position.lineNumber,
          startColumn: position.column,
          endLineNumber: position.lineNumber,
          endColumn: position.column,
        },
      })
    })

    return completions
  }

  /**
   * Create UDF Snippet
   *
   * Generates Monaco Editor snippet text for UDF functions with proper
   * tab stops and parameter placeholders.
   *
   * @private
   * @param udf - User-defined function definition
   * @returns Monaco snippet string
   */
  private createUDFSnippet(udf: UserDefinedFunction): string {
    // Special handling for different UDF types
    switch (udf.name) {
      case "evm_decode_log":
        return "evm_decode_log(${1:topic1}, ${2:topic2}, ${3:topic3}, ${4:data}, '${5:signature}')$0"

      case "evm_topic":
        return "evm_topic('${1:signature}')$0"

      case "${dataset}.eth_call":
        return "${1:dataset}.eth_call(${2:from_address}, ${3:to_address}, ${4:input_data}, '${5:block}')$0"

      case "evm_decode_params":
        return "evm_decode_params(${1:input_data}, '${2:signature}')$0"

      case "evm_encode_params":
        return "evm_encode_params(${1:arg1}, ${2:arg2}, '${3:signature}')$0"

      case "evm_encode_type":
        return "evm_encode_type(${1:value}, '${2:type}')$0"

      case "evm_decode_type":
        return "evm_decode_type(${1:data}, '${2:type}')$0"

      case "attestation_hash":
        return "attestation_hash(${1:column1}${2:, ${3:column2}})$0"

      default:
        // Generic fallback using parameters if available
        if (udf.parameters && udf.parameters.length > 0) {
          const params = udf.parameters
            .map((param, i) => `\${${i + 1}:${param}}`)
            .join(", ")
          return `${udf.name}(${params})$0`
        }
        return `${udf.name}(\${1})$0`
    }
  }

  /**
   * Create Keyword Completions
   *
   * Generates SQL keyword completions appropriate for the current context.
   *
   * @private
   * @param queryContext - Current query context
   * @returns Array of keyword completion items
   */
  private createKeywordCompletions(
    queryContext: QueryContext,
    position: Position,
  ): Array<languages.CompletionItem> {
    const completions: Array<languages.CompletionItem> = []
    let keywordIndex = 0

    // Select appropriate keywords based on current clause context
    let keywords: Array<string> = []

    switch (queryContext.currentClause) {
      case "SELECT":
        keywords = [
          "DISTINCT",
          "FROM",
          "WHERE",
          "GROUP BY",
          "ORDER BY",
          "LIMIT",
        ]
        break
      case "FROM":
        keywords = [
          "WHERE",
          "JOIN",
          "INNER JOIN",
          "LEFT JOIN",
          "RIGHT JOIN",
          "GROUP BY",
          "ORDER BY",
        ]
        break
      case "WHERE":
      case "HAVING":
        keywords = ["AND", "OR", "NOT", "GROUP BY", "ORDER BY", "LIMIT"]
        break
      case "JOIN":
        keywords = ["ON", "WHERE", "GROUP BY", "ORDER BY"]
        break
      default:
        // Provide general keywords when context is unclear
        keywords = [...this.sqlKeywords.clauses, ...this.sqlKeywords.functions]
    }

    keywords.forEach((keyword) => {
      completions.push({
        label: keyword,
        kind: languages.CompletionItemKind.Keyword,
        detail: "SQL Keyword",
        insertText: keyword,
        sortText: `${COMPLETION_PRIORITY.KEYWORD}-${keywordIndex.toString().padStart(3, "0")}`,
        filterText: keyword,
        range: {
          startLineNumber: position.lineNumber,
          startColumn: position.column,
          endLineNumber: position.lineNumber,
          endColumn: position.column,
        },
      })
      keywordIndex++
    })

    return completions
  }

  /**
   * Create Operator Completions
   *
   * Generates SQL operator completions for WHERE, HAVING, and ON clauses.
   *
   * @private
   * @returns Array of operator completion items
   */
  private createOperatorCompletions(
    position: Position,
  ): Array<languages.CompletionItem> {
    const operators = [
      "=",
      "<>",
      "!=",
      "<",
      ">",
      "<=",
      ">=",
      "LIKE",
      "ILIKE",
      "IN",
      "NOT IN",
      "BETWEEN",
      "IS NULL",
      "IS NOT NULL",
    ]

    return operators.map((operator, index) => ({
      label: operator,
      kind: languages.CompletionItemKind.Operator,
      detail: "SQL Operator",
      insertText: operator,
      sortText: `${COMPLETION_PRIORITY.OPERATOR}-${index.toString().padStart(3, "0")}`,
      filterText: operator,
      range: {
        startLineNumber: position.lineNumber,
        startColumn: position.column,
        endLineNumber: position.lineNumber,
        endColumn: position.column,
      },
    }))
  }

  /**
   * Filter and Limit Suggestions
   *
   * Applies prefix filtering and suggestion limits to the completion list.
   *
   * @private
   */
  private filterAndLimitSuggestions(
    suggestions: Array<languages.CompletionItem>,
    queryContext: QueryContext,
  ): Array<languages.CompletionItem> {
    let filtered = suggestions

    // Apply prefix filtering if there's a current prefix
    if (queryContext.currentPrefix) {
      const lowerPrefix = queryContext.currentPrefix.toLowerCase()
      filtered = suggestions.filter((suggestion) => {
        const labelText = typeof suggestion.label === "string"
          ? suggestion.label
          : suggestion.label.label
        return (
          labelText.toLowerCase().includes(lowerPrefix) ||
          (suggestion.filterText &&
            suggestion.filterText.toLowerCase().includes(lowerPrefix))
        )
      })
    }

    // Apply suggestion limit
    if (filtered.length > this.config.maxSuggestions) {
      filtered = filtered.slice(0, this.config.maxSuggestions)
    }

    return filtered
  }

  /**
   * Add Range Information to Suggestions
   *
   * Calculates and adds text replacement ranges to completion items.
   *
   * @private
   */
  private addRangeToSuggestions(
    suggestions: Array<languages.CompletionItem>,
    position: Position,
    currentPrefix: string,
  ): Array<languages.CompletionItem> {
    const range = this.calculateReplacementRange(position, currentPrefix)

    return suggestions.map((suggestion) => ({
      ...suggestion,
      range,
    }))
  }

  /**
   * Calculate Replacement Range
   *
   * Determines the text range that should be replaced by the completion.
   *
   * @private
   */
  private calculateReplacementRange(
    position: Position,
    currentPrefix: string,
  ): IRange {
    const startColumn = position.column - currentPrefix.length
    const endColumn = position.column

    return {
      startLineNumber: position.lineNumber,
      startColumn,
      endLineNumber: position.lineNumber,
      endColumn,
    }
  }

  /**
   * Get Fallback Completions
   *
   * Provides basic keyword completions when context analysis fails.
   *
   * @private
   */
  private getFallbackCompletions(position: Position): languages.CompletionList {
    const basicKeywords = [
      "SELECT",
      "FROM",
      "WHERE",
      "JOIN",
      "ORDER BY",
      "GROUP BY",
      "LIMIT",
    ]

    const suggestions = basicKeywords.map<languages.CompletionItem>(
      (keyword, index) => ({
        label: keyword,
        kind: languages.CompletionItemKind.Keyword,
        detail: "SQL Keyword",
        insertText: keyword,
        sortText: index.toString().padStart(3, "0"),
        range: {
          startLineNumber: position.lineNumber,
          startColumn: position.column,
          endLineNumber: position.lineNumber,
          endColumn: position.column,
        },
      }),
    )

    this.logDebug("Using fallback completions")
    return { suggestions }
  }

  /**
   * Utility Methods
   */

  /**
   * Update Provider Data
   *
   * Updates the metadata and UDF information when data changes.
   * This is called by the provider manager when fresh data is available.
   */
  updateData(
    metadata: ReadonlyArray<DatasetMetadata>,
    udfs: ReadonlyArray<UserDefinedFunction>,
  ): void {
    this.metadata = metadata
    this.udfs = udfs
    this.analyzer.clearCache() // Clear analysis cache when data changes
  }

  /**
   * Dispose Resources
   *
   * Cleans up any resources held by the provider.
   */
  dispose(): void {
    this.analyzer.clearCache()
  }

  /**
   * Logging Utilities
   */

  private logDebug(message: string, data?: any): void {
    if (this.config.enableDebugLogging) {
      console.debug(`[NozzleCompletionProvider] ${message}`, data)
    }
  }

  private logError(message: string, error: any): void {
    console.error(`[NozzleCompletionProvider] ${message}`, error)
  }
}
