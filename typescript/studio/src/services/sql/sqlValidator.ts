/**
 * SQL Validator Service
 * 
 * Core validation service for SQL queries that identifies errors and invalid references.
 * Provides real-time validation for Monaco Editor with error markers.
 * 
 * Key Features:
 * - Table/column reference validation against metadata
 * - Basic SQL syntax error detection  
 * - Position-accurate error reporting
 * - Performance-optimized caching
 * - Progressive validation levels (basic, standard, full)
 * - Error recovery and fallback handling
 * 
 * @file sqlValidator.ts
 * @author SQL Error Markers System
 */

import * as monaco from "monaco-editor"
import type { DatasetMetadata } from 'nozzl/Studio/Model'
import { QueryContextAnalyzer } from './QueryContextAnalyzer'
import type {
  UserDefinedFunction,
  CompletionConfig,
  SqlValidationError,
  ValidationCache,
  SqlToken,
  SqlTokenType
} from './types'
import { 
  DEFAULT_COMPLETION_CONFIG, 
  DEFAULT_VALIDATION_CACHE_CONFIG,
  ValidationCache as ValidationCacheImpl
} from './types'

/**
 * SQL Validator
 * 
 * Main validator class that provides comprehensive SQL validation
 * including syntax checking and metadata validation.
 */
export class SqlValidator {
  private metadata: DatasetMetadata[]
  private udfs: UserDefinedFunction[]
  private config: CompletionConfig
  private contextAnalyzer: QueryContextAnalyzer
  private validationCache: ValidationCache
  
  // Performance tracking
  private metrics = {
    totalValidations: 0,
    cacheHits: 0,
    averageValidationTime: 0,
    lastValidation: Date.now()
  }

  constructor(
    metadata: DatasetMetadata[],
    udfs: UserDefinedFunction[],
    config: CompletionConfig = DEFAULT_COMPLETION_CONFIG
  ) {
    this.metadata = metadata
    this.udfs = udfs
    this.config = config
    this.contextAnalyzer = new QueryContextAnalyzer(config)
    this.validationCache = new ValidationCacheImpl(DEFAULT_VALIDATION_CACHE_CONFIG)
    
    this.logDebug('SqlValidator initialized', {
      tableCount: metadata.length,
      udfCount: udfs.length,
      validationLevel: config.validationLevel
    })
  }

  /**
   * Validate Query
   * 
   * Main entry point for SQL validation. Returns array of validation errors
   * with position information compatible with Monaco Editor markers.
   * 
   * @param query - SQL query string to validate
   * @returns Array of validation errors
   */
  validateQuery(query: string): SqlValidationError[] {
    const startTime = performance.now()
    
    try {
      console.log(`[SqlValidator] Starting validation for query: "${query}"`)
      console.log(`[SqlValidator] Validation enabled: ${this.config.enableSqlValidation}`)
      console.log(`[SqlValidator] Validation level: ${this.config.validationLevel}`)
      console.log(`[SqlValidator] Metadata available: ${this.metadata.length} tables`)
      
      this.metrics.totalValidations++
      
      // Check if validation is enabled
      if (!this.config.enableSqlValidation) {
        console.log(`[SqlValidator] Validation disabled, returning no errors`)
        return []
      }

      // Check cache first
      const cached = this.validationCache.get(query)
      if (cached !== null) {
        this.metrics.cacheHits++
        this.logDebug('Using cached validation result')
        console.log(`[SqlValidator] Using cached result: ${cached.length} errors`)
        return cached
      }

      console.log(`[SqlValidator] Performing fresh validation...`)
      // Perform validation based on level
      const errors = this.performValidation(query)
      
      // Cache the results
      this.validationCache.set(query, errors)
      
      // Update metrics
      const duration = performance.now() - startTime
      this.updateMetrics(duration)
      
      this.logDebug(`Validation completed in ${duration.toFixed(2)}ms`, {
        errorCount: errors.length,
        level: this.config.validationLevel
      })
      
      return errors

    } catch (error) {
      const duration = performance.now() - startTime
      this.updateMetrics(duration)
      this.logError('Validation failed', error)
      
      // Return empty array on error (fail gracefully)
      return []
    }
  }

  /**
   * Update Provider Data
   * 
   * Updates the metadata and UDF data for validation.
   * Clears cache to ensure fresh validation with new data.
   * 
   * @param metadata - Updated dataset metadata
   * @param udfs - Updated UDF definitions  
   */
  updateData(metadata: DatasetMetadata[], udfs: UserDefinedFunction[]): void {
    this.metadata = metadata
    this.udfs = udfs
    
    // Clear cache when data changes
    this.validationCache.clear()
    
    this.logDebug('Validator data updated', {
      tableCount: metadata.length,
      udfCount: udfs.length
    })
  }

  /**
   * Get Validation Metrics
   * 
   * Returns performance metrics for monitoring.
   */
  getMetrics(): typeof this.metrics {
    return { ...this.metrics }
  }

  /**
   * Clear Validation Cache
   * 
   * Clears the validation cache to force fresh validation.
   */
  clearCache(): void {
    this.validationCache.clear()
    this.logDebug('Validation cache cleared')
  }

  /**
   * Dispose Resources
   * 
   * Cleanup method to dispose validator resources.
   */
  dispose(): void {
    this.validationCache.clear()
    this.contextAnalyzer.clearCache()
    this.logDebug('SqlValidator disposed')
  }

  /**
   * Perform Validation
   * 
   * Core validation logic that performs actual error checking
   * based on the configured validation level.
   * 
   * @private
   */
  private performValidation(query: string): SqlValidationError[] {
    const errors: SqlValidationError[] = []
    
    // Skip validation for empty or whitespace-only queries
    if (!query.trim()) {
      return errors
    }

    // Tokenize query for analysis
    const tokens = this.tokenizeQuery(query)
    
    // Phase 1: Basic validation level includes syntax validation
    if (this.config.validationLevel === 'basic' || 
        this.config.validationLevel === 'standard' || 
        this.config.validationLevel === 'full') {
      
      errors.push(...this.validateSyntax(tokens))
    }

    // Phase 2: Standard level adds table validation
    if (this.config.validationLevel === 'standard' || 
        this.config.validationLevel === 'full') {
      
      errors.push(...this.validateTableReferences(tokens))
    }

    // Phase 3: Full level adds column validation  
    if (this.config.validationLevel === 'full') {
      errors.push(...this.validateColumnReferences(tokens))
    }

    
    return errors
  }

  /**
   * Validate SQL Syntax
   * 
   * Performs basic SQL syntax validation including:
   * - Unmatched parentheses
   * - Missing keywords
   * - Trailing commas
   * 
   * @private
   */
  private validateSyntax(tokens: SqlToken[]): SqlValidationError[] {
    const errors: SqlValidationError[] = []

    // Validate parentheses balance
    errors.push(...this.validateParentheses(tokens))
    
    // Validate basic SQL structure
    errors.push(...this.validateBasicStructure(tokens))

    return errors
  }

  /**
   * Validate Parentheses Balance
   * 
   * Checks for unmatched opening/closing parentheses.
   * 
   * @private
   */
  private validateParentheses(tokens: SqlToken[]): SqlValidationError[] {
    const errors: SqlValidationError[] = []
    let parenDepth = 0
    const parenStack: { token: SqlToken; depth: number }[] = []

    for (const token of tokens) {
      if (token.type === 'DELIMITER' && token.value === '(') {
        parenDepth++
        parenStack.push({ token, depth: parenDepth })
      } else if (token.type === 'DELIMITER' && token.value === ')') {
        parenDepth--
        if (parenDepth < 0) {
          errors.push({
            message: 'Unmatched closing parenthesis',
            severity: monaco.MarkerSeverity.Error,
            ...this.getTokenPosition(token),
            code: 'UNMATCHED_CLOSING_PAREN'
          })
          parenDepth = 0 // Reset to prevent cascading errors
        }
      }
    }

    // Check for unmatched opening parentheses
    if (parenDepth > 0) {
      const unmatchedParen = parenStack[parenStack.length - 1]
      errors.push({
        message: 'Unmatched opening parenthesis',
        severity: monaco.MarkerSeverity.Error,
        ...this.getTokenPosition(unmatchedParen.token),
        code: 'UNMATCHED_OPENING_PAREN'
      })
    }

    return errors
  }

  /**
   * Validate Basic SQL Structure
   * 
   * Performs basic structural validation like SELECT...FROM patterns.
   * 
   * @private
   */
  private validateBasicStructure(tokens: SqlToken[]): SqlValidationError[] {
    const errors: SqlValidationError[] = []

    // Find SELECT keywords
    const selectTokens = tokens.filter(token => 
      token.type === 'KEYWORD' && token.value.toUpperCase() === 'SELECT'
    )

    // Check each SELECT has corresponding FROM (simplified check)
    for (const selectToken of selectTokens) {
      const fromToken = this.findNextKeyword(tokens, selectToken, 'FROM')
      if (!fromToken) {
        errors.push({
          message: 'SELECT statement missing FROM clause',
          severity: monaco.MarkerSeverity.Warning,
          ...this.getTokenPosition(selectToken),
          code: 'MISSING_FROM_CLAUSE'
        })
      }
    }

    return errors
  }

  /**
   * Validate Table References
   * 
   * Validates that table names exist in the metadata.
   * Simplified implementation for Phase 2.
   * 
   * @private
   */
  private validateTableReferences(tokens: SqlToken[]): SqlValidationError[] {
    const errors: SqlValidationError[] = []

    // Find table references after FROM and JOIN keywords
    const tableRefs = this.extractTableReferences(tokens)
    console.log(`[SqlValidator] Extracted table references:`, tableRefs.map(ref => ({ name: ref.name, alias: ref.alias })))
    
    for (const tableRef of tableRefs) {
      if (!this.tableExists(tableRef.name)) {
        const suggestion = this.suggestSimilarTable(tableRef.name)
        const availableTables = this.metadata.map(d => `${d.dataset_name}.${d.table_name}`).slice(0, 3)
        const moreTables = this.metadata.length > 3 ? ` and ${this.metadata.length - 3} more` : ''
        
        let message: string
        if (suggestion) {
          message = `Unknown table '${tableRef.name}'. Did you mean '${suggestion}'?`
        } else {
          message = `Unknown table '${tableRef.name}'. Available tables: ${availableTables.join(', ')}${moreTables}`
        }
          
        errors.push({
          message,
          severity: monaco.MarkerSeverity.Error,
          ...this.getTokenPosition(tableRef.token),
          code: 'UNKNOWN_TABLE',
          data: { 
            tableName: tableRef.name, 
            suggestion,
            availableTables: this.metadata.map(d => `${d.dataset_name}.${d.table_name}`)
          }
        })
      }
    }

    return errors
  }

  /**
   * Validate Column References
   * 
   * Validates that column names exist in referenced tables.
   * Handles basic SELECT, WHERE clause column references.
   * 
   * @private
   */
  private validateColumnReferences(tokens: SqlToken[]): SqlValidationError[] {
    const errors: SqlValidationError[] = []
    
    // Extract table references and their aliases
    const tableRefs = this.extractTableReferences(tokens)
    const tableAliasMap = this.buildTableAliasMap(tableRefs)
    
    // Extract column references from SELECT and WHERE clauses
    const columnRefs = this.extractColumnReferences(tokens)
    console.log(`[SqlValidator] Extracted column references:`, columnRefs)
    
    for (const columnRef of columnRefs) {
      console.log(`[SqlValidator] Validating column reference:`, columnRef)
      const validationResult = this.validateColumnReference(columnRef, tableAliasMap)
      console.log(`[SqlValidator] Column validation result:`, validationResult)
      if (validationResult.error) {
        errors.push(validationResult.error)
      }
    }
    
    return errors
  }

  /**
   * Extract Column References
   * 
   * Extracts column references from SELECT, WHERE, and other clauses.
   * Handles qualified names (table.column) and unqualified names.
   * 
   * @private
   */
  private extractColumnReferences(tokens: SqlToken[]): Array<{
    name: string
    tableName?: string
    token: SqlToken
    clause: string
  }> {
    const columnRefs: Array<{
      name: string
      tableName?: string
      token: SqlToken
      clause: string
    }> = []
    
    let currentClause = ''
    
    for (let i = 0; i < tokens.length; i++) {
      const token = tokens[i]
      
      // Track current SQL clause
      if (token.type === 'KEYWORD') {
        const keyword = token.value.toUpperCase()
        if (['SELECT', 'WHERE', 'GROUP', 'ORDER', 'HAVING'].includes(keyword)) {
          currentClause = keyword
        }
      }
      
      // Handle wildcard SELECT (*) - always valid
      if (token.type === 'OPERATOR' && token.value === '*' && currentClause === 'SELECT') {
        continue // Skip wildcard - it's always valid
      }
      
      // Look for column references in relevant clauses
      if (token.type === 'IDENTIFIER' && 
          ['SELECT', 'WHERE', 'GROUP', 'ORDER', 'HAVING'].includes(currentClause)) {
        
        
        // Skip if this identifier is actually a table name (comes after FROM/JOIN)
        if (this.isTableReference(tokens, i)) {
          continue
        }
        
        // Check for qualified column name (table.column, table.*, or dataset.table.column)
        if (i < tokens.length - 2 && 
            tokens[i + 1].type === 'DELIMITER' && tokens[i + 1].value === '.') {
          
          const secondToken = tokens[i + 2]
          
          // Check for fully qualified reference (dataset.table.column or dataset.table.*)
          if (secondToken.type === 'IDENTIFIER' && 
              i < tokens.length - 4 && 
              tokens[i + 3].type === 'DELIMITER' && tokens[i + 3].value === '.') {
            
            const thirdToken = tokens[i + 4]
            
            // Skip wildcard references (dataset.table.*) - these are always valid
            if (thirdToken.type === 'OPERATOR' && thirdToken.value === '*') {
              i += 4 // Skip dataset.table.*
              continue
            }
            
            // Handle fully qualified column reference (dataset.table.column)
            if (thirdToken.type === 'IDENTIFIER') {
              const fullTableName = `${token.value}.${secondToken.value}`
              columnRefs.push({
                name: thirdToken.value,
                tableName: fullTableName,
                token: thirdToken,
                clause: currentClause
              })
              i += 4 // Skip dataset.table.column
              continue
            }
          }
          
          // Skip wildcard references (table.*) - these are always valid
          if (secondToken.type === 'OPERATOR' && secondToken.value === '*') {
            i += 2 // Skip the dot and asterisk
            continue
          }
          
          // Handle regular qualified column reference (table.column)
          if (secondToken.type === 'IDENTIFIER') {
            columnRefs.push({
              name: secondToken.value,
              tableName: token.value,
              token: secondToken,
              clause: currentClause
            })
            i += 2 // Skip the dot and column name
          }
        } else {
          // Check if this is a qualified identifier stored as a single token (e.g., "table.column" or "dataset.table.column")
          const parts = token.value.split('.')
          
          if (parts.length === 2) {
            // Check if this is a wildcard reference (table.* tokenized as single token ending with dot)
            if (parts[1] === '' && i < tokens.length - 1 && tokens[i + 1].type === 'OPERATOR' && tokens[i + 1].value === '*') {
              // This is table.* - skip it as wildcard is always valid
              console.log(`[SqlValidator] Skipping wildcard reference: ${parts[0]}.*`)
              i += 1 // Skip the * token
              continue
            }
            
            // Regular qualified reference: table.column (only if column name is not empty)
            if (parts[1] !== '') {
              columnRefs.push({
                name: parts[1],
                tableName: parts[0], 
                token: token,
                clause: currentClause
              })
            }
          } else if (parts.length === 3) {
            // Fully qualified reference: dataset.table.column
            const fullTableName = `${parts[0]}.${parts[1]}`
            columnRefs.push({
              name: parts[2],
              tableName: fullTableName,
              token: token,
              clause: currentClause
            })
          } else {
            // Unqualified column reference
            columnRefs.push({
              name: token.value,
              token: token,
              clause: currentClause
            })
          }
        }
      }
    }
    
    return columnRefs
  }

  /**
   * Validate Column Reference
   * 
   * Validates a single column reference against available tables.
   * 
   * @private
   */
  private validateColumnReference(
    columnRef: { name: string; tableName?: string; token: SqlToken; clause: string },
    tableAliasMap: Map<string, string>
  ): { error?: SqlValidationError } {
    
    // Check if this identifier is a UDF first - UDFs should not be validated as columns
    if (this.isUDF(columnRef.name)) {
      console.log(`[SqlValidator] Skipping column validation for UDF: ${columnRef.name}`)
      return {} // No error - this is a valid UDF
    }
    
    // If qualified (table.column), validate against specific table
    if (columnRef.tableName) {
      const fullTableName = tableAliasMap.get(columnRef.tableName) || columnRef.tableName
      console.log(`[SqlValidator] Qualified column - resolving alias "${columnRef.tableName}" -> "${fullTableName}"`)
      const table = this.findTable(fullTableName)
      
      if (!table) {
        return {
          error: {
            message: `Table '${columnRef.tableName}' not found for column '${columnRef.name}'`,
            severity: monaco.MarkerSeverity.Error,
            ...this.getTokenPosition(columnRef.token),
            code: 'TABLE_NOT_FOUND'
          }
        }
      }
      
      const columnExists = this.columnExistsInTable(columnRef.name, table)
      console.log(`[SqlValidator] Column '${columnRef.name}' exists in table '${fullTableName}':`, columnExists)
      const t = table as any
      const columnNames = t.metadata_columns?.map((c: any) => c.name) || t.column_names || []
      console.log(`[SqlValidator] Available columns in table:`, columnNames)
      
      if (!columnExists) {
        const suggestion = this.suggestSimilarColumn(columnRef.name, table)
        const availableColumns = columnNames.slice(0, 5) // Show first 5 columns
        const moreColumns = columnNames.length > 5 ? ` and ${columnNames.length - 5} more` : ''
        
        let message: string
        if (suggestion) {
          message = `Column '${columnRef.name}' not found in table '${fullTableName}'. Did you mean '${suggestion}'?`
        } else {
          message = `Column '${columnRef.name}' not found in table '${fullTableName}'. Available columns: ${availableColumns.join(', ')}${moreColumns}`
        }
          
        return {
          error: {
            message,
            severity: monaco.MarkerSeverity.Error,
            ...this.getTokenPosition(columnRef.token),
            code: 'COLUMN_NOT_FOUND',
            data: { 
              columnName: columnRef.name, 
              tableName: fullTableName, 
              suggestion,
              availableColumns: columnNames
            }
          }
        }
      }
    } else {
      // Unqualified column - check if it exists in any available table
      const availableTables = this.getAllAvailableTables(tableAliasMap)
      console.log(`[SqlValidator] getAllAvailableTables returned:`, availableTables.map(t => {
        const tb = t as any
        return {
          source: tb.source,
          destination: tb.destination,
          dataset_name: tb.dataset_name,
          table_name: tb.table_name,
          column_count: (tb.metadata_columns?.length || 0) + (tb.column_names?.length || 0)
        }
      }))
      const matchingTables = availableTables.filter(table => 
        this.columnExistsInTable(columnRef.name, table)
      )
      
      console.log(`[SqlValidator] Unqualified column '${columnRef.name}' - checking ${availableTables.length} available tables`)
      console.log(`[SqlValidator] Available tables:`, availableTables.map(t => t.source))
      console.log(`[SqlValidator] Matching tables for column '${columnRef.name}':`, matchingTables.length)
      
      if (matchingTables.length === 0) {
        // Find best suggestion from all available columns and track which tables have it
        let bestSuggestion: string | null = null
        let bestDistance = Infinity
        const allColumns: string[] = []
        const tablesWithSuggestion: string[] = []
        
        for (const table of availableTables) {
          const t = table as any
          const columnNames = t.metadata_columns?.map((c: any) => c.name) || t.column_names || []
          allColumns.push(...columnNames)
          for (const columnName of columnNames) {
            const distance = this.calculateStringDistance(columnRef.name, columnName)
            if (distance < bestDistance && distance <= 4) {
              bestDistance = distance
              bestSuggestion = columnName
              // Reset and find all tables that have this suggested column
              tablesWithSuggestion.length = 0
              for (const checkTable of availableTables) {
                if (this.columnExistsInTable(columnName, checkTable)) {
                  // Get the alias for this table from the tableAliasMap
                  const t = checkTable as any
                  const sourceTableName = t.source || (t.dataset_name ? `${t.dataset_name}.${t.table_name}` : '')
                  const tableAlias = this.getTableAlias(sourceTableName, tableAliasMap)
                  tablesWithSuggestion.push(tableAlias)
                }
              }
            }
          }
        }
        
        let message: string
        let suggestionData: any = { 
          columnName: columnRef.name, 
          suggestion: bestSuggestion,
          availableColumns: allColumns.slice(0, 10) // Limit for performance
        }
        
        if (bestSuggestion && tablesWithSuggestion.length > 0) {
          const tableList = tablesWithSuggestion.join(', ')
          message = `Column '${columnRef.name}' not found in any available table. Did you mean '${bestSuggestion}'? Available in tables: ${tableList}`
          suggestionData.suggestedTables = tablesWithSuggestion
        } else if (bestSuggestion) {
          message = `Column '${columnRef.name}' not found in any available table. Did you mean '${bestSuggestion}'?`
        } else {
          const sampleColumns = allColumns.slice(0, 5)
          const moreColumns = allColumns.length > 5 ? ` and ${allColumns.length - 5} more` : ''
          message = `Column '${columnRef.name}' not found in any available table. Available columns: ${sampleColumns.join(', ')}${moreColumns}`
        }
        
        return {
          error: {
            message,
            severity: monaco.MarkerSeverity.Error,
            ...this.getTokenPosition(columnRef.token),
            code: 'COLUMN_NOT_FOUND',
            data: suggestionData
          }
        }
      }
      
      if (matchingTables.length > 1) {
        const tableNames = matchingTables.map(t => t.source).join(', ')
        return {
          error: {
            message: `Column '${columnRef.name}' is ambiguous. Found in tables: ${tableNames}`,
            severity: monaco.MarkerSeverity.Warning,
            ...this.getTokenPosition(columnRef.token),
            code: 'AMBIGUOUS_COLUMN',
            data: { columnName: columnRef.name, tables: tableNames }
          }
        }
      }
    }
    
    return {}
  }

  /**
   * Build Table Alias Map
   * 
   * Creates a map from table aliases to full table names.
   * 
   * @private
   */
  private buildTableAliasMap(tableRefs: Array<{ name: string; token: SqlToken; alias?: string }>): Map<string, string> {
    const aliasMap = new Map<string, string>()
    
    console.log(`[SqlValidator] buildTableAliasMap - input tableRefs:`, tableRefs)
    
    for (const tableRef of tableRefs) {
      // Map alias to full table name
      if (tableRef.alias) {
        aliasMap.set(tableRef.alias, tableRef.name)
        console.log(`[SqlValidator] buildTableAliasMap - mapped alias "${tableRef.alias}" -> "${tableRef.name}"`)
      }
      // Also map table name to itself for direct references
      aliasMap.set(tableRef.name, tableRef.name)
      console.log(`[SqlValidator] buildTableAliasMap - mapped table "${tableRef.name}" -> "${tableRef.name}"`)
    }
    
    console.log(`[SqlValidator] buildTableAliasMap - final alias map:`, Array.from(aliasMap.entries()))
    return aliasMap
  }

  /**
   * Helper Methods for Column Validation
   */

  private getTableAlias(tableName: string, tableAliasMap: Map<string, string>): string {
    // Find the alias for this table name, or return the table name itself
    for (const [alias, fullTableName] of tableAliasMap.entries()) {
      if (fullTableName === tableName && alias !== tableName) {
        return alias // Return the alias (e.g., "t1", "t2")
      }
    }
    return tableName // Return the full table name if no alias found
  }

  private findTable(tableName: string): DatasetMetadata | null {
    console.log(`[SqlValidator] findTable - searching for table: "${tableName}"`)
    console.log(`[SqlValidator] findTable - available datasets:`, this.metadata.map(d => ({ 
      dataset_name: (d as any).dataset_name, 
      table_name: (d as any).table_name, 
      source: (d as any).source, 
      destination: (d as any).destination 
    })))
    
    const result = this.metadata.find(dataset => {
      const d = dataset as any
      // Check both possible structures
      const fullTableName = d.dataset_name ? `${d.dataset_name}.${d.table_name}` : null
      return d.source === tableName ||
             d.destination === tableName ||
             fullTableName === tableName
    }) || null
    
    console.log(`[SqlValidator] findTable - result:`, result ? { 
      dataset_name: (result as any).dataset_name, 
      table_name: (result as any).table_name,
      source: (result as any).source, 
      destination: (result as any).destination 
    } : null)
    return result
  }

  private columnExistsInTable(columnName: string, table: DatasetMetadata): boolean {
    const t = table as any
    console.log(`[SqlValidator] columnExistsInTable - checking column "${columnName}" in table:`, { 
      source: t.source, 
      destination: t.destination,
      dataset_name: t.dataset_name,
      table_name: t.table_name,
      metadata_columns_count: t.metadata_columns?.length || 0,
      column_names_count: t.column_names?.length || 0,
      metadata_columns: t.metadata_columns?.map((c: any) => c.name) || [],
      column_names: t.column_names || []
    })
    
    // Handle both structures: metadata_columns (objects) and column_names (strings)
    if (t.metadata_columns) {
      return t.metadata_columns.some((col: any) => col.name === columnName)
    } else if (t.column_names) {
      return t.column_names.includes(columnName)
    }
    return false
  }

  private suggestSimilarColumn(columnName: string, table: DatasetMetadata): string | null {
    const t = table as any
    // Get column names from either structure
    const columnNames = t.metadata_columns?.map((c: any) => c.name) || t.column_names || []
    if (columnNames.length === 0) {
      return null
    }

    const input = columnName.toLowerCase()
    let bestMatch: string | null = null
    let bestScore = 0

    for (const colName of columnNames) {
      const score = this.calculateStringSimilarity(input, colName.toLowerCase())
      
      // Consider a match if similarity score is above threshold
      if (score > 0.3 && score > bestScore) {
        bestMatch = colName
        bestScore = score
      }
    }

    // If no good match found, return null (don't suggest random column)
    return bestMatch
  }

  private getAllAvailableTables(tableAliasMap: Map<string, string>): DatasetMetadata[] {
    const tableNames = new Set(Array.from(tableAliasMap.values()))
    console.log(`[SqlValidator] getAllAvailableTables - tableNames from alias map:`, Array.from(tableNames))
    console.log(`[SqlValidator] getAllAvailableTables - available metadata sources:`, this.metadata.map(d => {
      const dt = d as any
      return dt.source || (dt.dataset_name ? `${dt.dataset_name}.${dt.table_name}` : 'undefined')
    }))
    
    const result = this.metadata.filter(dataset => {
      const d = dataset as any
      // Handle both metadata structures
      const sourceMatch = d.source && tableNames.has(d.source)
      const destinationMatch = d.destination && tableNames.has(d.destination)
      const fullTableName = d.dataset_name ? `${d.dataset_name}.${d.table_name}` : null
      const fullTableMatch = fullTableName && tableNames.has(fullTableName)
      
      return sourceMatch || destinationMatch || fullTableMatch
    })
    
    console.log(`[SqlValidator] getAllAvailableTables - filtered result:`, result.map(d => {
      const dt = d as any
      return { 
        source: dt.source, 
        destination: dt.destination,
        dataset_name: dt.dataset_name,
        table_name: dt.table_name,
        fullName: dt.dataset_name ? `${dt.dataset_name}.${dt.table_name}` : null
      }
    }))
    return result
  }

  private isTableReference(tokens: SqlToken[], index: number): boolean {
    // Look backward for FROM or JOIN keywords
    for (let i = index - 1; i >= 0; i--) {
      const token = tokens[i]
      if (token.type === 'KEYWORD') {
        const keyword = token.value.toUpperCase()
        if (keyword === 'FROM' || keyword === 'JOIN') {
          // Check if this identifier is close enough to be a table reference
          const tokensBetween = tokens.slice(i + 1, index)
          const nonWhitespaceTokens = tokensBetween.filter(t => t.type !== 'WHITESPACE')
          return nonWhitespaceTokens.length <= 2 // Allow for schema.table pattern
        }
        // If we hit another clause keyword, this is not a table reference
        if (['SELECT', 'WHERE', 'GROUP', 'ORDER', 'HAVING'].includes(keyword)) {
          break
        }
      }
    }
    return false
  }

  /**
   * Extract Table References
   * 
   * Extracts table names from FROM and JOIN clauses.
   * 
   * @private
   */
  private extractTableReferences(tokens: SqlToken[]): Array<{ name: string; token: SqlToken; alias?: string }> {
    console.log(`[SqlValidator] extractTableReferences called with ${tokens.length} tokens`)
    console.log(`[SqlValidator] First few tokens:`, tokens.slice(0, 10).map(t => ({ type: t.type, value: t.value })))
    const tableRefs: Array<{ name: string; token: SqlToken; alias?: string }> = []
    
    for (let i = 0; i < tokens.length - 1; i++) {
      const token = tokens[i]
      
      if (token.type === 'KEYWORD') {
        const keyword = token.value.toUpperCase()
        
        if (keyword === 'FROM' || keyword === 'JOIN') {
          const tableTokenIndex = this.findNextIdentifierIndex(tokens, i + 1)
          if (tableTokenIndex !== -1) {
            const tableToken = tokens[tableTokenIndex]
            let tableName = tableToken.value
            let nextIndex = tableTokenIndex + 1
            
            // Handle qualified names (schema.table)
            if (nextIndex < tokens.length && 
                tokens[nextIndex].type === 'DELIMITER' && tokens[nextIndex].value === '.' &&
                nextIndex + 1 < tokens.length && tokens[nextIndex + 1].type === 'IDENTIFIER') {
              tableName += '.' + tokens[nextIndex + 1].value
              nextIndex += 2
            }
            
            // Check for alias after table name
            let alias: string | undefined = undefined
            
            // Skip whitespace tokens
            while (nextIndex < tokens.length && tokens[nextIndex].type === 'WHITESPACE') {
              nextIndex++
            }
            
            console.log(`[SqlValidator] extractTableReferences - looking for alias after table "${tableName}", nextIndex:${nextIndex}`)
            if (nextIndex < tokens.length) {
              console.log(`[SqlValidator] extractTableReferences - nextToken:`, tokens[nextIndex])
              console.log(`[SqlValidator] extractTableReferences - next few tokens:`, tokens.slice(nextIndex, nextIndex + 4))
            } else {
              console.log(`[SqlValidator] extractTableReferences - EOF reached`)
            }
            
            // Check for AS keyword first
            if (nextIndex < tokens.length && 
                tokens[nextIndex].type === 'KEYWORD' && 
                tokens[nextIndex].value.toUpperCase() === 'AS') {
              console.log(`[SqlValidator] extractTableReferences - found AS keyword, skipping`)
              nextIndex++
              // Skip whitespace after AS
              while (nextIndex < tokens.length && tokens[nextIndex].type === 'WHITESPACE') {
                nextIndex++
              }
            }
            
            // Now look for the alias identifier
            if (nextIndex < tokens.length && tokens[nextIndex].type === 'IDENTIFIER') {
              alias = tokens[nextIndex].value
              console.log(`[SqlValidator] extractTableReferences - found alias: "${alias}"`)
            }
            
            tableRefs.push({ name: tableName, token: tableToken, alias })
          }
        }
      }
    }
    
    return tableRefs
  }

  /**
   * Check if UDF Exists
   * 
   * Checks if an identifier is a User Defined Function.
   * 
   * @private
   */
  private isUDF(name: string): boolean {
    return this.udfs.some(udf => udf.name === name)
  }

  /**
   * Check if Table Exists
   * 
   * Checks if a table name exists in the metadata.
   * 
   * @private
   */
  private tableExists(tableName: string): boolean {
    // Debug: Log table lookup attempt
    console.log(`[SqlValidator] Checking if table exists: "${tableName}"`)
    console.log(`[SqlValidator] Metadata count: ${this.metadata.length}`)
    
    if (this.metadata.length > 0) {
      console.log(`[SqlValidator] Available tables:`, this.metadata.map(d => ({
        source: (d as any).source,
        destination: (d as any).destination,
        dataset_name: (d as any).dataset_name,
        table_name: (d as any).table_name
      })))
    } else {
      console.log(`[SqlValidator] No metadata available!`)
    }
    
    const exists = this.metadata.some(dataset => {
      const d = dataset as any
      const sourceMatch = d.source === tableName
      const destinationMatch = d.destination === tableName
      const fullTableName = d.dataset_name ? `${d.dataset_name}.${d.table_name}` : null
      const fullTableMatch = fullTableName === tableName
      
      if (sourceMatch || destinationMatch || fullTableMatch) {
        console.log(`[SqlValidator] Found match for "${tableName}" in dataset:`, {
          source: d.source,
          destination: d.destination,
          dataset_name: d.dataset_name,
          table_name: d.table_name,
          fullTableName,
          sourceMatch,
          destinationMatch,
          fullTableMatch
        })
      }
      
      return sourceMatch || destinationMatch || fullTableMatch
    })
    
    console.log(`[SqlValidator] Table "${tableName}" exists: ${exists}`)
    return exists
  }

  /**
   * Suggest Similar Table
   * 
   * Suggests a similar table name using enhanced fuzzy matching.
   * 
   * @private
   */
  private suggestSimilarTable(tableName: string): string | null {
    const allTables = this.metadata.map(dataset => 
      `${dataset.dataset_name}.${dataset.table_name}`
    )
    
    if (allTables.length === 0) {
      return null
    }

    const input = tableName.toLowerCase()
    let bestMatch: string | null = null
    let bestScore = 0

    for (const table of allTables) {
      const score = this.calculateStringSimilarity(input, table.toLowerCase())
      
      // Consider a match if similarity score is above threshold
      if (score > 0.4 && score > bestScore) {
        bestMatch = table
        bestScore = score
      }
    }

    // If no good match found, suggest the first available table
    return bestMatch || (allTables.length > 0 ? allTables[0] : null)
  }

  /**
   * Calculate String Similarity
   * 
   * Uses Levenshtein distance to calculate similarity between strings.
   * Returns a score between 0 and 1, where 1 is exact match.
   * 
   * @private
   */
  private calculateStringSimilarity(str1: string, str2: string): number {
    const len1 = str1.length
    const len2 = str2.length
    const matrix: number[][] = []

    // Initialize matrix
    for (let i = 0; i <= len1; i++) {
      matrix[i] = [i]
    }
    for (let j = 0; j <= len2; j++) {
      matrix[0][j] = j
    }

    // Calculate Levenshtein distance
    for (let i = 1; i <= len1; i++) {
      for (let j = 1; j <= len2; j++) {
        if (str1.charAt(i - 1) === str2.charAt(j - 1)) {
          matrix[i][j] = matrix[i - 1][j - 1]
        } else {
          matrix[i][j] = Math.min(
            matrix[i - 1][j - 1] + 1, // substitution
            matrix[i][j - 1] + 1,     // insertion
            matrix[i - 1][j] + 1      // deletion
          )
        }
      }
    }

    // Convert distance to similarity score
    const maxLen = Math.max(len1, len2)
    const distance = matrix[len1][len2]
    return maxLen === 0 ? 1 : (maxLen - distance) / maxLen
  }

  /**
   * Calculate String Distance
   * 
   * Returns the raw Levenshtein distance between two strings.
   * Lower values indicate higher similarity.
   * 
   * @private
   */
  private calculateStringDistance(str1: string, str2: string): number {
    const len1 = str1.length
    const len2 = str2.length
    
    if (len1 === 0) return len2
    if (len2 === 0) return len1
    
    const matrix: number[][] = []

    // Initialize matrix
    for (let i = 0; i <= len1; i++) {
      matrix[i] = [i]
    }
    for (let j = 0; j <= len2; j++) {
      matrix[0][j] = j
    }

    // Calculate Levenshtein distance
    for (let i = 1; i <= len1; i++) {
      for (let j = 1; j <= len2; j++) {
        if (str1.charAt(i - 1) === str2.charAt(j - 1)) {
          matrix[i][j] = matrix[i - 1][j - 1]
        } else {
          matrix[i][j] = Math.min(
            matrix[i - 1][j - 1] + 1, // substitution
            matrix[i][j - 1] + 1,     // insertion
            matrix[i - 1][j] + 1      // deletion
          )
        }
      }
    }

    return matrix[len1][len2]
  }

  /**
   * Helper Methods
   */

  private tokenizeQuery(query: string): SqlToken[] {
    // Reuse the existing tokenizer from contextAnalyzer
    // This is a simplified implementation - in a real system we'd want
    // to avoid duplicating tokenization logic
    return this.contextAnalyzer['tokenizeQuery'](query)
  }

  /**
   * Get Token Position
   * 
   * Converts token position information to Monaco Editor compatible format.
   * Handles multi-line queries and different line endings correctly.
   * 
   * @private
   */
  private getTokenPosition(token: SqlToken): {
    startLineNumber: number
    startColumn: number
    endLineNumber: number
    endColumn: number
  } {
    // Handle multi-line tokens (like block comments)
    const lines = token.value.split(/\r?\n/)
    const endLineNumber = token.line + lines.length - 1
    const endColumn = lines.length > 1 
      ? lines[lines.length - 1].length + 1  // +1 for 1-based indexing
      : token.column + token.value.length

    return {
      startLineNumber: token.line,
      startColumn: token.column,
      endLineNumber: endLineNumber,
      endColumn: endColumn
    }
  }

  /**
   * Get Position Range from String Offsets
   * 
   * Converts string offset positions to Monaco line/column format.
   * Useful for errors that don't correspond to specific tokens.
   * 
   * @private
   */
  private getPositionFromOffset(query: string, startOffset: number, endOffset: number): {
    startLineNumber: number
    startColumn: number
    endLineNumber: number
    endColumn: number
  } {
    const lines = query.substring(0, startOffset).split(/\r?\n/)
    const startLineNumber = lines.length
    const startColumn = lines[lines.length - 1].length + 1 // 1-based

    // Calculate end position
    const endLines = query.substring(0, endOffset).split(/\r?\n/)
    const endLineNumber = endLines.length
    const endColumn = endLines[endLines.length - 1].length + 1 // 1-based

    return {
      startLineNumber,
      startColumn,
      endLineNumber,
      endColumn
    }
  }

  /**
   * Get Position for Text Range
   * 
   * Creates position information for a text range within the query.
   * Useful for highlighting entire expressions or clauses.
   * 
   * @private
   */
  private getTextRangePosition(query: string, text: string, fromOffset: number = 0): {
    startLineNumber: number
    startColumn: number
    endLineNumber: number
    endColumn: number
  } | null {
    const index = query.indexOf(text, fromOffset)
    if (index === -1) {
      return null
    }

    return this.getPositionFromOffset(query, index, index + text.length)
  }

  private findNextKeyword(tokens: SqlToken[], fromToken: SqlToken, keyword: string): SqlToken | null {
    const startIndex = tokens.indexOf(fromToken)
    for (let i = startIndex + 1; i < tokens.length; i++) {
      const token = tokens[i]
      if (token.type === 'KEYWORD' && token.value.toUpperCase() === keyword) {
        return token
      }
    }
    return null
  }

  private findNextIdentifier(tokens: SqlToken[], startIndex: number): SqlToken | null {
    for (let i = startIndex; i < tokens.length; i++) {
      const token = tokens[i]
      if (token.type === 'IDENTIFIER') {
        return token
      }
      if (token.type !== 'WHITESPACE') {
        break // Stop at first non-whitespace, non-identifier
      }
    }
    return null
  }

  private findNextIdentifierIndex(tokens: SqlToken[], startIndex: number): number {
    for (let i = startIndex; i < tokens.length; i++) {
      const token = tokens[i]
      if (token.type === 'IDENTIFIER') {
        return i
      }
      if (token.type !== 'WHITESPACE') {
        break // Stop at first non-whitespace, non-identifier
      }
    }
    return -1
  }

  private updateMetrics(duration: number): void {
    // Update average response time (exponential moving average)
    const alpha = 0.1
    this.metrics.averageValidationTime = 
      alpha * duration + (1 - alpha) * this.metrics.averageValidationTime
    this.metrics.lastValidation = Date.now()
  }

  /**
   * Logging Utilities
   */

  private logDebug(message: string, data?: any): void {
    if (this.config.enableDebugLogging) {
      console.debug(`[SqlValidator] ${message}`, data)
    }
  }

  private logError(message: string, error: any): void {
    console.error(`[SqlValidator] ${message}`, error)
  }
}
