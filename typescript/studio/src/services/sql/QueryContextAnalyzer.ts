/**
 * SQL Query Context Analyzer
 * 
 * This module provides sophisticated analysis of SQL queries to determine the appropriate
 * completion suggestions based on cursor position. It analyzes query structure, identifies
 * available tables, handles aliases, and provides context-aware completion filtering.
 * 
 * Key Features:
 * - SQL tokenization and basic parsing
 * - Cursor position context analysis  
 * - Table alias resolution
 * - Query clause detection
 * - String/comment detection to avoid inappropriate completions
 * - Performance-optimized caching
 * 
 * @file contextAnalyzer.ts
 * @author SQL Intellisense System
 */

import type { Position } from 'monaco-editor/esm/vs/editor/editor.api';

import type { 
  CompletionConfig,
  MonacoITextModel,
  QueryContext, 
  SqlClause, 
  SqlToken, 
  SqlTokenType
} from './types'

import { DEFAULT_COMPLETION_CONFIG } from './types'

/**
 * Query Context Analyzer
 * 
 * Main class responsible for analyzing SQL queries and determining what
 * completions are appropriate at a given cursor position.
 * 
 * The analyzer uses a combination of tokenization and pattern matching
 * to understand query structure without requiring a full SQL parser.
 * This provides good performance while handling most real-world queries.
 */
export class QueryContextAnalyzer {
  private contextCache = new Map<string, { context: QueryContext; timestamp: number }>()
  private config: CompletionConfig

  constructor(config: CompletionConfig = DEFAULT_COMPLETION_CONFIG) {
    this.config = config
  }

  /**
   * Analyze Query Context
   * 
   * Main entry point for context analysis. Takes a Monaco text model and cursor
   * position, then returns a QueryContext describing what completions are appropriate.
   * 
   * @param model - Monaco text model containing the SQL query
   * @param position - Cursor position within the model
   * @returns QueryContext describing completion opportunities
   */
  analyzeContext(
    model: MonacoITextModel,
    position: Position
  ): QueryContext {
    try {
      const query = model.getValue()
      const offset = model.getOffsetAt(position)

      // Check cache first for performance
      const cacheKey = this.getCacheKey(query, offset)
      const cached = this.contextCache.get(cacheKey)
      
      if (cached && this.isCacheValid(cached.timestamp)) {
        this.logDebug('Using cached context analysis')
        return cached.context
      }

      // Perform fresh analysis
      const context = this.analyzeQueryInternal(query, offset, model, position)
      
      // Cache the result
      this.contextCache.set(cacheKey, {
        context,
        timestamp: Date.now()
      })

      this.logDebug('Context analysis completed', context)
      return context

    } catch (error) {
      this.logError('Context analysis failed', error)
      return this.createFallbackContext()
    }
  }

  /**
   * Internal Query Analysis
   * 
   * Performs the actual context analysis by tokenizing the query,
   * identifying the cursor context, and building the QueryContext result.
   * 
   * @private
   */
  private analyzeQueryInternal(
    query: string,
    offset: number,
    model: MonacoITextModel,
    position: Position
  ): QueryContext {
    // Tokenize the query for analysis
    const tokens = this.tokenizeQuery(query)
    
    // Get current word/prefix being typed
    const currentPrefix = this.getCurrentPrefix(model, position)
    
    // Check if cursor is in string or comment (should avoid completions)
    const { inComment, inString } = this.getCursorStringCommentStatus(query, offset)
    if (inString || inComment) {
      return this.createEmptyContext(currentPrefix, inString, inComment)
    }

    // Find the clause where cursor is positioned
    const currentClause = this.detectCurrentClause(tokens, offset)
    
    // Analyze available tables and aliases
    const { availableTables, tableAliases } = this.analyzeTablesAndAliases(tokens)
    
    // Determine what types of completions are expected
    const expectations = this.analyzeExpectations(tokens, offset, currentClause)
    
    return {
      expectsTable: expectations.table,
      expectsColumn: expectations.column,
      expectsFunction: expectations.function,
      expectsKeyword: expectations.keyword,
      expectsOperator: expectations.operator,
      availableTables,
      tableAliases,
      currentPrefix,
      cursorInString: false,
      cursorInComment: false,
      currentClause,
      subqueryDepth: this.calculateSubqueryDepth(tokens, offset),
      parentContexts: [] // TODO: Implement for Phase 2
    }
  }

  /**
   * Tokenize SQL Query
   * 
   * Breaks down a SQL query into tokens for analysis. This is a lightweight
   * tokenizer that identifies the main SQL elements without full parsing.
   * 
   * @private
   * @param query - The SQL query string to tokenize
   * @returns Array of SQL tokens with position information
   */
  private tokenizeQuery(query: string): Array<SqlToken> {
    const tokens: Array<SqlToken> = []
    let position = 0
    let line = 1
    let column = 1

    // SQL Keywords for identification
    const keywords = new Set([
      'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'ON', 
      'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT', 'DISTINCT', 'AS',
      'WITH', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'LIKE', 'ILIKE',
      'BETWEEN', 'IS', 'NULL', 'TRUE', 'FALSE'
    ])

    while (position < query.length) {
      const startPos = position
      const startLine = line
      const startColumn = column

      const char = query[position]

      // Whitespace
      if (/\s/.test(char)) {
        const value = this.consumeWhitespace(query, position)
        tokens.push({
          type: 'WHITESPACE',
          value,
          startOffset: startPos,
          endOffset: position + value.length,
          line: startLine,
          column: startColumn
        })
        position += value.length
        if (char === '\n') {
          line++
          column = 1
        } else {
          column += value.length
        }
        continue
      }

      // Single line comments (-- comment)
      if (char === '-' && query[position + 1] === '-') {
        const value = this.consumeLineComment(query, position)
        tokens.push({
          type: 'COMMENT',
          value,
          startOffset: startPos,
          endOffset: position + value.length,
          line: startLine,
          column: startColumn
        })
        position += value.length
        column += value.length
        continue
      }

      // Multi-line comments (/* comment */)
      if (char === '/' && query[position + 1] === '*') {
        const value = this.consumeBlockComment(query, position)
        tokens.push({
          type: 'COMMENT',
          value,
          startOffset: startPos,
          endOffset: position + value.length,
          line: startLine,
          column: startColumn
        })
        const newlines = (value.match(/\n/g) || []).length
        if (newlines > 0) {
          line += newlines
          column = value.length - value.lastIndexOf('\n')
        } else {
          column += value.length
        }
        position += value.length
        continue
      }

      // String literals (single and double quotes)
      if (char === "'" || char === '"') {
        const value = this.consumeString(query, position, char)
        tokens.push({
          type: 'STRING',
          value,
          startOffset: startPos,
          endOffset: position + value.length,
          line: startLine,
          column: startColumn
        })
        position += value.length
        column += value.length
        continue
      }

      // Numbers
      if (/\d/.test(char)) {
        const value = this.consumeNumber(query, position)
        tokens.push({
          type: 'NUMBER',
          value,
          startOffset: startPos,
          endOffset: position + value.length,
          line: startLine,
          column: startColumn
        })
        position += value.length
        column += value.length
        continue
      }

      // Identifiers and keywords
      if (/[a-zA-Z_$]/.test(char)) {
        const value = this.consumeIdentifier(query, position)
        const upperValue = value.toUpperCase()
        tokens.push({
          type: keywords.has(upperValue) ? 'KEYWORD' : 'IDENTIFIER',
          value,
          startOffset: startPos,
          endOffset: position + value.length,
          line: startLine,
          column: startColumn
        })
        position += value.length
        column += value.length
        continue
      }

      // Operators and delimiters
      const value = this.consumeOperatorOrDelimiter(query, position)
      tokens.push({
        type: this.classifyOperatorOrDelimiter(value),
        value,
        startOffset: startPos,
        endOffset: position + value.length,
        line: startLine,
        column: startColumn
      })
      position += value.length
      column += value.length
    }

    return tokens
  }

  /**
   * Detect Current SQL Clause
   * 
   * Determines which SQL clause the cursor is currently positioned in
   * (SELECT, FROM, WHERE, etc.) based on token analysis.
   * 
   * @private
   */
  private detectCurrentClause(tokens: Array<SqlToken>, offset: number): SqlClause | null {
    // Find tokens before the cursor position
    const beforeCursor = tokens.filter(token => token.endOffset <= offset)
    
    // Look backwards for clause keywords
    for (let i = beforeCursor.length - 1; i >= 0; i--) {
      const token = beforeCursor[i]
      if (token.type === 'KEYWORD') {
        const keyword = token.value.toUpperCase()
        switch (keyword) {
          case 'SELECT': return 'SELECT'
          case 'FROM': return 'FROM'
          case 'WHERE': return 'WHERE'
          case 'JOIN':
          case 'INNER':
          case 'LEFT':
          case 'RIGHT': return 'JOIN'
          case 'ON': return 'ON'
          case 'GROUP': return 'GROUP_BY'
          case 'HAVING': return 'HAVING'
          case 'ORDER': return 'ORDER_BY'
          case 'LIMIT': return 'LIMIT'
          case 'WITH': return 'WITH'
        }
      }
    }

    return null
  }

  /**
   * Analyze Tables and Aliases
   * 
   * Identifies all tables mentioned in the query and builds a map of
   * table aliases to their full names.
   * 
   * @private
   */
  private analyzeTablesAndAliases(tokens: Array<SqlToken>): {
    availableTables: Array<string>
    tableAliases: Map<string, string>
  } {
    const availableTables: Array<string> = []
    const tableAliases = new Map<string, string>()

    // Look for FROM and JOIN clauses to find table references
    for (let i = 0; i < tokens.length - 1; i++) {
      const token = tokens[i]
      
      if (token.type === 'KEYWORD') {
        const keyword = token.value.toUpperCase()
        
        // Found FROM or JOIN, look for table name
        if (keyword === 'FROM' || keyword === 'JOIN') {
          const tableInfo = this.extractTableReference(tokens, i + 1)
          if (tableInfo.tableName) {
            availableTables.push(tableInfo.tableName)
            if (tableInfo.alias) {
              tableAliases.set(tableInfo.alias, tableInfo.tableName)
            }
          }
        }
      }
    }

    return { availableTables, tableAliases }
  }

  /**
   * Extract Table Reference
   * 
   * Starting from a token index, extracts table name and optional alias.
   * Handles patterns like "anvil.logs", "anvil.logs l", "anvil.logs AS l"
   * 
   * @private
   */
  private extractTableReference(tokens: Array<SqlToken>, startIndex: number): {
    tableName: string | null
    alias: string | null
  } {
    // Skip whitespace
    let i = startIndex
    while (i < tokens.length && tokens[i].type === 'WHITESPACE') {
      i++
    }

    if (i >= tokens.length || tokens[i].type !== 'IDENTIFIER') {
      return { tableName: null, alias: null }
    }

    // Get the table name (might be qualified like "anvil.logs")
    let tableName = tokens[i].value
    i++

    // Check for qualified name (schema.table)
    if (i < tokens.length - 1 && 
        tokens[i].type === 'OPERATOR' && tokens[i].value === '.' &&
        tokens[i + 1].type === 'IDENTIFIER') {
      tableName += '.' + tokens[i + 1].value
      i += 2
    }

    // Skip whitespace
    while (i < tokens.length && tokens[i].type === 'WHITESPACE') {
      i++
    }

    // Check for alias
    let alias: string | null = null
    
    // Pattern: "table AS alias"
    if (i < tokens.length - 2 && 
        tokens[i].type === 'KEYWORD' && tokens[i].value.toUpperCase() === 'AS' &&
        tokens[i + 2].type === 'IDENTIFIER') {
      alias = tokens[i + 2].value
    }
    // Pattern: "table alias" (without AS)
    else if (i < tokens.length && tokens[i].type === 'IDENTIFIER') {
      alias = tokens[i].value
    }

    return { tableName, alias }
  }

  /**
   * Analyze Completion Expectations
   * 
   * Based on the cursor position and surrounding context, determines what
   * types of completions should be offered (tables, columns, functions, etc.).
   * 
   * @private
   */
  private analyzeExpectations(
    tokens: Array<SqlToken>, 
    offset: number, 
    currentClause: SqlClause | null
  ): {
    table: boolean
    column: boolean
    function: boolean
    keyword: boolean
    operator: boolean
  } {
    // Find the token immediately before the cursor
    const beforeCursor = tokens.filter(token => token.endOffset <= offset)
    const lastToken = beforeCursor[beforeCursor.length - 1]
    
    // Default expectations based on clause
    let expectations = {
      table: false,
      column: false,
      function: false,
      keyword: true, // Keywords are generally always available
      operator: false
    }

    switch (currentClause) {
      case 'FROM':
      case 'JOIN':
        expectations.table = true
        break
        
      case 'SELECT':
      case 'GROUP_BY':
      case 'ORDER_BY':
        expectations.column = true
        expectations.function = true
        break
        
      case 'WHERE':
      case 'HAVING':
      case 'ON':
        expectations.column = true
        expectations.function = true
        expectations.operator = true
        break
    }

    // Refine based on immediate context
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    if (lastToken) {
      if (lastToken.type === 'OPERATOR' && lastToken.value === '.') {
        // After dot, expect columns (table.column)
        expectations = { table: false, column: true, function: false, keyword: false, operator: false }
      }
      else if (lastToken.type === 'IDENTIFIER') {
        // After identifier, might expect operators or keywords
        expectations.operator = true
      }
    }

    return expectations
  }

  /**
   * Helper Methods for Tokenization
   */

  private consumeWhitespace(query: string, start: number): string {
    let end = start
    while (end < query.length && /\s/.test(query[end])) {
      end++
    }
    return query.substring(start, end)
  }

  private consumeLineComment(query: string, start: number): string {
    let end = start + 2 // Skip '--'
    while (end < query.length && query[end] !== '\n') {
      end++
    }
    return query.substring(start, end)
  }

  private consumeBlockComment(query: string, start: number): string {
    let end = start + 2 // Skip '/*'
    while (end < query.length - 1) {
      if (query[end] === '*' && query[end + 1] === '/') {
        end += 2
        break
      }
      end++
    }
    return query.substring(start, end)
  }

  private consumeString(query: string, start: number, quote: string): string {
    let end = start + 1 // Skip opening quote
    while (end < query.length) {
      if (query[end] === quote) {
        end++
        break
      }
      if (query[end] === '\\') {
        end += 2 // Skip escaped character
      } else {
        end++
      }
    }
    return query.substring(start, end)
  }

  private consumeNumber(query: string, start: number): string {
    let end = start
    while (end < query.length && /[\d.]/.test(query[end])) {
      end++
    }
    return query.substring(start, end)
  }

  private consumeIdentifier(query: string, start: number): string {
    let end = start
    while (end < query.length && /[\w$.]/.test(query[end])) {
      end++
    }
    return query.substring(start, end)
  }

  private consumeOperatorOrDelimiter(query: string, start: number): string {
    // Multi-character operators
    const twoChar = query.substring(start, start + 2)
    if (['<=', '>=', '<>', '!=', '||'].includes(twoChar)) {
      return twoChar
    }
    
    // Single character
    return query[start]
  }

  private classifyOperatorOrDelimiter(value: string): SqlTokenType {
    const operators = new Set(['=', '<', '>', '<=', '>=', '<>', '!=', '+', '-', '*', '/', '%', '||'])
    const delimiters = new Set([',', '(', ')', '.', ';'])
    
    if (operators.has(value)) return 'OPERATOR'
    if (delimiters.has(value)) return 'DELIMITER'
    return 'UNKNOWN'
  }

  /**
   * Utility Methods
   */

  private getCurrentPrefix(model: MonacoITextModel, position: Position): string {
    const line = model.getLineContent(position.lineNumber)
    const beforeCursor = line.substring(0, position.column - 1)
    
    // Find the last word boundary
    const match = beforeCursor.match(/(\w+)$/)
    return match ? match[1] : ''
  }

  private getCursorStringCommentStatus(query: string, offset: number): { inString: boolean, inComment: boolean } {
    // Simple heuristic: check if cursor is between quotes or in comment
    const beforeCursor = query.substring(0, offset)
    
    // Count unescaped quotes
    const singleQuotes = (beforeCursor.match(/(?<!\\)'/g) || []).length
    const doubleQuotes = (beforeCursor.match(/(?<!\\)"/g) || []).length
    
    // If odd number of quotes, we're inside a string
    const inString = singleQuotes % 2 === 1 || doubleQuotes % 2 === 1
    if (inString) {
      return { inString: true, inComment: false }
    }

    // Check for line comments
    const lastLineStart = beforeCursor.lastIndexOf('\n') + 1
    const currentLine = beforeCursor.substring(lastLineStart)
    const inComment = currentLine.includes('--')

    return { inString, inComment }
  }

  private calculateSubqueryDepth(tokens: Array<SqlToken>, offset: number): number {
    // Count nested parentheses before cursor
    let depth = 0
    for (const token of tokens) {
      if (token.endOffset > offset) break
      if (token.value === '(') depth++
      if (token.value === ')') depth--
    }
    return Math.max(0, depth)
  }

  /**
   * Cache Management
   */

  private getCacheKey(query: string, offset: number): string {
    // Simple cache key based on query hash and offset
    const queryHash = this.simpleHash(query)
    return `${queryHash}:${offset}`
  }

  private simpleHash(str: string): number {
    let hash = 0
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i)
      hash = ((hash << 5) - hash) + char
      hash = hash & hash // Convert to 32-bit integer
    }
    return hash
  }

  private isCacheValid(timestamp: number): boolean {
    return (Date.now() - timestamp) < this.config.contextCacheTTL
  }

  /**
   * Cleanup and Utility Methods
   */

  clearCache(): void {
    this.contextCache.clear()
    this.logDebug('Context cache cleared')
  }

  /**
   * Get Cache Statistics
   * 
   * Returns cache statistics for testing and monitoring.
   * 
   * @returns Cache statistics
   */
  getCacheStats(): { contextCache: number } {
    return {
      contextCache: this.contextCache.size
    }
  }

  private createEmptyContext(currentPrefix = '', inString = false, inComment = false): QueryContext {
    return {
      expectsTable: false,
      expectsColumn: false,
      expectsFunction: false,
      expectsKeyword: false,
      expectsOperator: false,
      availableTables: [],
      tableAliases: new Map(),
      currentPrefix,
      cursorInString: inString,
      cursorInComment: inComment,
      currentClause: null,
      subqueryDepth: 0,
      parentContexts: []
    }
  }

  private createFallbackContext(): QueryContext {
    return {
      expectsTable: true,
      expectsColumn: true,
      expectsFunction: true,
      expectsKeyword: true,
      expectsOperator: false,
      availableTables: [],
      tableAliases: new Map(),
      currentPrefix: '',
      cursorInString: false,
      cursorInComment: false,
      currentClause: null,
      subqueryDepth: 0,
      parentContexts: []
    }
  }

  /**
   * Logging Utilities
   */

  private logDebug(message: string, data?: any): void {
    if (this.config.enableDebugLogging) {
      console.debug(`[QueryContextAnalyzer] ${message}`, data)
    }
  }

  private logError(message: string, error: any): void {
    console.error(`[QueryContextAnalyzer] ${message}`, error)
  }
}
