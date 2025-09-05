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
import type { DatasetMetadata } from 'nozzl/Studio/Model'
import { QueryContextAnalyzer } from './contextAnalyzer'
import type {
  UserDefinedFunction,
  CompletionConfig,
  CompletionItemOptions,
  TableInfo,
  ColumnInfo,
  QueryContext
} from './types'
import { DEFAULT_COMPLETION_CONFIG, COMPLETION_PRIORITY } from './types'

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
export class NozzleCompletionProvider implements monaco.languages.CompletionItemProvider {
  private analyzer: QueryContextAnalyzer
  private config: CompletionConfig

  // Core SQL keywords organized by context
  private readonly sqlKeywords = {
    clauses: ['SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'ON', 
              'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'OFFSET', 'WITH', 'UNION', 'EXCEPT', 'INTERSECT'],
    functions: ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'DISTINCT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'],
    operators: ['AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'ILIKE', 'IS NULL', 'IS NOT NULL'],
    modifiers: ['ASC', 'DESC', 'DISTINCT', 'ALL', 'AS']
  }

  constructor(
    private metadata: DatasetMetadata[],
    private udfs: UserDefinedFunction[],
    analyzer?: QueryContextAnalyzer,
    config: CompletionConfig = DEFAULT_COMPLETION_CONFIG
  ) {
    this.analyzer = analyzer || new QueryContextAnalyzer(config)
    this.config = config
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
  async provideCompletionItems(
    model: monaco.editor.ITextModel,
    position: monaco.Position,
    context: monaco.languages.CompletionContext,
    token: monaco.CancellationToken
  ): Promise<monaco.languages.CompletionList> {
    try {
      // Check if request has been cancelled
      if (token.isCancellationRequested) {
        return { suggestions: [] }
      }

      const startTime = performance.now()
      
      // Analyze query context to determine what completions are appropriate
      const queryContext = this.analyzer.analyzeContext(model, position)
      
      this.logDebug('Query context analysis:', queryContext)

      // Don't provide completions if cursor is in string/comment
      if (queryContext.cursorInString || queryContext.cursorInComment) {
        return { suggestions: [] }
      }

      // Apply minimum prefix length filter
      if (queryContext.currentPrefix.length < this.config.minPrefixLength) {
        return { suggestions: [] }
      }

      // Generate completions based on context
      const suggestions: monaco.languages.CompletionItem[] = []

      // 1. Table completions (highest priority in appropriate contexts)
      if (queryContext.expectsTable) {
        this.logDebug('Creating table completions...')
        const tableCompletions = this.createTableCompletions(queryContext)
        this.logDebug('Table completions created:', { count: tableCompletions.length, tables: tableCompletions.map(t => t.label) })
        suggestions.push(...tableCompletions)
      }

      // 2. Column completions (context-filtered)
      if (queryContext.expectsColumn) {
        const columnCompletions = this.createColumnCompletions(queryContext)
        suggestions.push(...columnCompletions)
      }

      // 3. UDF function completions
      if (queryContext.expectsFunction) {
        const udfCompletions = this.createUDFCompletions(queryContext)
        suggestions.push(...udfCompletions)
      }

      // 4. SQL keyword completions
      if (queryContext.expectsKeyword) {
        const keywordCompletions = this.createKeywordCompletions(queryContext)
        suggestions.push(...keywordCompletions)
      }

      // 5. Operator completions
      if (queryContext.expectsOperator) {
        const operatorCompletions = this.createOperatorCompletions(queryContext)
        suggestions.push(...operatorCompletions)
      }

      // Filter by prefix and apply limits
      const filteredSuggestions = this.filterAndLimitSuggestions(suggestions, queryContext)

      // Add range information for text replacement
      const suggestionsWithRange = this.addRangeToSuggestions(
        filteredSuggestions, 
        model, 
        position,
        queryContext.currentPrefix
      )

      const duration = performance.now() - startTime
      this.logDebug(`Completion generation completed in ${duration.toFixed(2)}ms, ${suggestionsWithRange.length} suggestions`)
      this.logDebug('Final suggestions:', suggestionsWithRange.map(s => ({ label: s.label, kind: s.kind })))

      return {
        suggestions: suggestionsWithRange,
        incomplete: false // We provide all available completions
      }

    } catch (error) {
      this.logError('Completion provider failed', error)
      return this.getFallbackCompletions(model, position)
    }
  }

  /**
   * Create Table Completions
   * 
   * Generates completion items for database tables based on the current metadata.
   * Provides detailed documentation showing available columns.
   * 
   * @private
   * @param queryContext - Current query context
   * @returns Array of table completion items
   */
  private createTableCompletions(queryContext: QueryContext): monaco.languages.CompletionItem[] {
    const completions: monaco.languages.CompletionItem[] = []

    this.metadata.forEach((dataset, index) => {
      // Create detailed documentation showing table schema
      const columnList = dataset.metadata_columns
        .map(col => `- \`${col.name}\` (${col.datatype})${col.description ? ': ' + col.description : ''}`)
        .join('\n')

      const documentation: monaco.IMarkdownString = {
        value: [
          `**Dataset Table: ${dataset.source}**`,
          '',
          `Contains ${dataset.metadata_columns.length} columns:`,
          '',
          columnList,
          '',
          '*Click to insert table name in query*'
        ].join('\n'),
        isTrusted: true
      }

      completions.push({
        label: dataset.source,
        kind: monaco.languages.CompletionItemKind.Class,
        detail: `Table (${dataset.metadata_columns.length} columns)`,
        documentation,
        insertText: dataset.source,
        sortText: `${COMPLETION_PRIORITY.TABLE}-${index.toString().padStart(3, '0')}`,
        preselect: index === 0, // Preselect first table
        filterText: dataset.source,
        // Add command to trigger parameter hints if this is a function-like context
        command: {
          id: 'editor.action.triggerSuggest',
          title: 'Re-trigger completion'
        }
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
  private createColumnCompletions(queryContext: QueryContext): monaco.languages.CompletionItem[] {
    const completions: monaco.languages.CompletionItem[] = []
    let columnIndex = 0

    this.metadata.forEach((dataset, datasetIndex) => {
      // Skip tables not in scope (unless no tables are specified, then include all)
      if (queryContext.availableTables.length > 0 && 
          !queryContext.availableTables.includes(dataset.source)) {
        return
      }

      dataset.metadata_columns.forEach((column) => {
        const documentation: monaco.IMarkdownString = {
          value: [
            `**Column: ${column.name}**`,
            `**Type:** ${column.datatype}`,
            `**Table:** ${dataset.source}`,
            '',
            column.description || 'No description available',
            '',
            '*Click to insert column name in query*'
          ].join('\n'),
          isTrusted: true
        }

        completions.push({
          label: column.name,
          kind: monaco.languages.CompletionItemKind.Field,
          detail: `${column.datatype} - ${dataset.source}`,
          documentation,
          insertText: column.name,
          sortText: `${COMPLETION_PRIORITY.COLUMN}-${columnIndex.toString().padStart(4, '0')}`,
          filterText: column.name
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
  private createUDFCompletions(queryContext: QueryContext): monaco.languages.CompletionItem[] {
    const completions: monaco.languages.CompletionItem[] = []

    this.udfs.forEach((udf, index) => {
      // Generate snippet with parameter placeholders
      const snippet = this.createUDFSnippet(udf)
      
      // Clean up display name for functions with dataset prefix
      const displayName = udf.name.replace('${dataset}', '{dataset}')
      
      const documentation: monaco.IMarkdownString = {
        value: [
          `**${displayName}** - Nozzle User-Defined Function`,
          '',
          udf.description,
          '',
          '**SQL Signature:**',
          '```sql',
          udf.sql.trim(),
          '```',
          '',
          udf.example ? `**Example:**\n\`\`\`sql\n${udf.example}\n\`\`\`` : '',
          '',
          '💡 **Tip:** Use Tab to navigate between parameters after insertion'
        ].join('\n'),
        isTrusted: true
      }

      completions.push({
        label: displayName,
        kind: monaco.languages.CompletionItemKind.Function,
        detail: 'Nozzle UDF',
        documentation,
        insertText: snippet,
        insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
        sortText: `${COMPLETION_PRIORITY.UDF}-${index.toString().padStart(3, '0')}`,
        filterText: udf.name,
        // Trigger parameter hints after insertion
        command: {
          id: 'editor.action.triggerParameterHints',
          title: 'Trigger Parameter Hints'
        }
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
      case 'evm_decode_log':
        return 'evm_decode_log(${1:topic1}, ${2:topic2}, ${3:topic3}, ${4:data}, \'${5:signature}\')$0'
      
      case 'evm_topic':
        return 'evm_topic(\'${1:signature}\')$0'
      
      case '${dataset}.eth_call':
        return '${1:dataset}.eth_call(${2:from_address}, ${3:to_address}, ${4:input_data}, \'${5:block}\')$0'
      
      case 'evm_decode_params':
        return 'evm_decode_params(${1:input_data}, \'${2:signature}\')$0'
      
      case 'evm_encode_params':
        return 'evm_encode_params(${1:arg1}, ${2:arg2}, \'${3:signature}\')$0'
      
      case 'evm_encode_type':
        return 'evm_encode_type(${1:value}, \'${2:type}\')$0'
      
      case 'evm_decode_type':
        return 'evm_decode_type(${1:data}, \'${2:type}\')$0'
      
      case 'attestation_hash':
        return 'attestation_hash(${1:column1}${2:, ${3:column2}})$0'
      
      default:
        // Generic fallback using parameters if available
        if (udf.parameters && udf.parameters.length > 0) {
          const params = udf.parameters
            .map((param, i) => `\${${i + 1}:${param}}`)
            .join(', ')
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
  private createKeywordCompletions(queryContext: QueryContext): monaco.languages.CompletionItem[] {
    const completions: monaco.languages.CompletionItem[] = []
    let keywordIndex = 0

    // Select appropriate keywords based on current clause context
    let keywords: string[] = []
    
    switch (queryContext.currentClause) {
      case 'SELECT':
        keywords = ['DISTINCT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT']
        break
      case 'FROM':
        keywords = ['WHERE', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'GROUP BY', 'ORDER BY']
        break
      case 'WHERE':
      case 'HAVING':
        keywords = ['AND', 'OR', 'NOT', 'GROUP BY', 'ORDER BY', 'LIMIT']
        break
      case 'JOIN':
        keywords = ['ON', 'WHERE', 'GROUP BY', 'ORDER BY']
        break
      default:
        // Provide general keywords when context is unclear
        keywords = [...this.sqlKeywords.clauses, ...this.sqlKeywords.functions]
    }

    keywords.forEach(keyword => {
      completions.push({
        label: keyword,
        kind: monaco.languages.CompletionItemKind.Keyword,
        detail: 'SQL Keyword',
        insertText: keyword,
        sortText: `${COMPLETION_PRIORITY.KEYWORD}-${keywordIndex.toString().padStart(3, '0')}`,
        filterText: keyword
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
   * @param queryContext - Current query context
   * @returns Array of operator completion items
   */
  private createOperatorCompletions(queryContext: QueryContext): monaco.languages.CompletionItem[] {
    const operators = ['=', '<>', '!=', '<', '>', '<=', '>=', 'LIKE', 'ILIKE', 'IN', 'NOT IN', 'BETWEEN', 'IS NULL', 'IS NOT NULL']
    
    return operators.map((operator, index) => ({
      label: operator,
      kind: monaco.languages.CompletionItemKind.Operator,
      detail: 'SQL Operator',
      insertText: operator,
      sortText: `${COMPLETION_PRIORITY.OPERATOR}-${index.toString().padStart(3, '0')}`,
      filterText: operator
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
    suggestions: monaco.languages.CompletionItem[], 
    queryContext: QueryContext
  ): monaco.languages.CompletionItem[] {
    let filtered = suggestions

    // Apply prefix filtering if there's a current prefix
    if (queryContext.currentPrefix) {
      const lowerPrefix = queryContext.currentPrefix.toLowerCase()
      filtered = suggestions.filter(suggestion => 
        suggestion.label.toLowerCase().includes(lowerPrefix) ||
        (suggestion.filterText && suggestion.filterText.toLowerCase().includes(lowerPrefix))
      )
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
    suggestions: monaco.languages.CompletionItem[],
    model: monaco.editor.ITextModel,
    position: monaco.Position,
    currentPrefix: string
  ): monaco.languages.CompletionItem[] {
    const range = this.calculateReplacementRange(model, position, currentPrefix)

    return suggestions.map(suggestion => ({
      ...suggestion,
      range
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
    model: monaco.editor.ITextModel,
    position: monaco.Position,
    currentPrefix: string
  ): monaco.Range {
    const line = model.getLineContent(position.lineNumber)
    const startColumn = position.column - currentPrefix.length
    const endColumn = position.column

    return new monaco.Range(
      position.lineNumber,
      startColumn,
      position.lineNumber,
      endColumn
    )
  }

  /**
   * Get Fallback Completions
   * 
   * Provides basic keyword completions when context analysis fails.
   * 
   * @private
   */
  private getFallbackCompletions(
    model: monaco.editor.ITextModel,
    position: monaco.Position
  ): monaco.languages.CompletionList {
    const basicKeywords = ['SELECT', 'FROM', 'WHERE', 'JOIN', 'ORDER BY', 'GROUP BY', 'LIMIT']
    
    const suggestions = basicKeywords.map((keyword, index) => ({
      label: keyword,
      kind: monaco.languages.CompletionItemKind.Keyword,
      detail: 'SQL Keyword',
      insertText: keyword,
      sortText: index.toString().padStart(3, '0')
    }))

    this.logDebug('Using fallback completions')
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
  updateData(metadata: DatasetMetadata[], udfs: UserDefinedFunction[]): void {
    this.metadata = metadata
    this.udfs = udfs
    this.analyzer.clearCache() // Clear analysis cache when data changes
    this.logDebug('Provider data updated', { 
      tableCount: metadata.length, 
      udfCount: udfs.length 
    })
  }

  /**
   * Dispose Resources
   * 
   * Cleans up any resources held by the provider.
   */
  dispose(): void {
    this.analyzer.clearCache()
    this.logDebug('Provider disposed')
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