/**
 * Enhanced SQL Completion Provider - Phase 2 Integration
 * 
 * This module integrates the advanced query parser (Phase 2) with the existing
 * Monaco Editor completion system. It provides a hybrid approach that uses both
 * the existing pattern-based analyzer (Phase 1) and the new AST-based analyzer
 * for superior completion accuracy and context understanding.
 * 
 * Key Features:
 * - Dual-provider architecture with intelligent fallback
 * - AST-based context analysis for complex queries
 * - Advanced completion scoring and ranking
 * - Seamless integration with existing Monaco setup
 * - Performance optimization with caching
 * 
 * @file enhancedCompletionProvider.ts
 * @author SQL Intellisense System - Phase 2
 */

import type { DatasetMetadata } from 'nozzl/Studio/Model'
import { QueryContextAnalyzer } from './contextAnalyzer'
import { 
  EnhancedContextAnalyzer, 
  CompletionScorer, 
  QueryContextType,
  CompletionType,
  type QueryContext as EnhancedQueryContext,
  type ScoredCompletion 
} from './advancedQueryParser'
import type {
  UserDefinedFunction,
  CompletionConfig,
  TableInfo,
  ColumnInfo,
  QueryContext as LegacyQueryContext
} from './types'
import { COMPLETION_PRIORITY } from './types'

/**
 * Enhanced Completion Provider Configuration
 */
export interface EnhancedCompletionConfig extends CompletionConfig {
  // Phase 2 specific options
  enableAdvancedScoring: boolean      // Use intelligent scoring system
  enableASTAnalysis: boolean          // Use AST-based context analysis
  fallbackToLegacy: boolean           // Fall back to Phase 1 if Phase 2 fails
  scoringWeights?: Partial<{           // Custom scoring weights
    contextRelevance: number
    semanticSimilarity: number
    usageFrequency: number
    recency: number
    specificity: number
  }>
}

/**
 * Default Enhanced Configuration
 */
export const DEFAULT_ENHANCED_CONFIG: EnhancedCompletionConfig = {
  minPrefixLength: 0,
  maxSuggestions: 50,
  enableSnippets: true,
  enableContextFiltering: true,
  enableAliasResolution: true,
  contextCacheTTL: 30000,
  enableDebugLogging: false,
  
  // Phase 2 enhancements
  enableAdvancedScoring: true,
  enableASTAnalysis: true,
  fallbackToLegacy: true,
  scoringWeights: {
    contextRelevance: 0.4,
    semanticSimilarity: 0.3,
    usageFrequency: 0.15,
    recency: 0.1,
    specificity: 0.05
  }
}

/**
 * Enhanced SQL Completion Provider
 * 
 * Integrates Phase 2 advanced query parsing with existing completion system.
 * Uses intelligent fallback strategy to ensure robustness.
 */
export class EnhancedCompletionProvider implements monaco.languages.CompletionItemProvider {
  private legacyAnalyzer: QueryContextAnalyzer
  private enhancedAnalyzer: EnhancedContextAnalyzer
  private completionScorer: CompletionScorer
  private config: EnhancedCompletionConfig
  
  private metadata: readonly DatasetMetadata[]
  private udfs: readonly UserDefinedFunction[]
  private tableInfoCache: Map<string, TableInfo> = new Map()
  private isDisposed = false

  constructor(
    metadata: readonly DatasetMetadata[],
    udfs: readonly UserDefinedFunction[],
    legacyAnalyzer: QueryContextAnalyzer,
    config: EnhancedCompletionConfig = DEFAULT_ENHANCED_CONFIG
  ) {
    this.metadata = metadata
    this.udfs = udfs
    this.legacyAnalyzer = legacyAnalyzer
    this.config = config
    
    // Initialize Phase 2 components
    this.enhancedAnalyzer = new EnhancedContextAnalyzer()
    
    const defaultWeights = {
      contextRelevance: 0.4,
      semanticSimilarity: 0.3,
      usageFrequency: 0.15,
      recency: 0.1,
      specificity: 0.05
    }
    
    this.completionScorer = new CompletionScorer({
      weights: { ...defaultWeights, ...config.scoringWeights },
      enableUsageTracking: true,
      enableSemanticSimilarity: true,
      maxSuggestions: config.maxSuggestions,
      minScore: 10,
      debugMode: config.enableDebugLogging
    })
    
    this.buildTableInfoCache()
    this.logDebug('Enhanced completion provider initialized', {
      tableCount: metadata.length,
      udfCount: udfs.length,
      astEnabled: config.enableASTAnalysis,
      scoringEnabled: config.enableAdvancedScoring
    })
  }

  /**
   * Provide Completion Items
   * 
   * Main completion provider interface. Uses hybrid approach with
   * Phase 2 AST analysis and Phase 1 fallback.
   */
  async provideCompletionItems(
    model: monaco.editor.ITextModel,
    position: monaco.Position,
    context: monaco.languages.CompletionContext,
    token: monaco.CancellationToken
  ): Promise<monaco.languages.CompletionList | null> {
    
    if (this.isDisposed || token.isCancellationRequested) {
      return null
    }

    const startTime = performance.now()
    
    try {
      // Get query text and cursor position
      const query = model.getValue()
      const offset = model.getOffsetAt(position)
      const currentWord = this.getCurrentWord(model, position)
      
      this.logDebug('Providing completions', {
        query: query.substring(0, 100) + (query.length > 100 ? '...' : ''),
        offset,
        currentWord,
        triggerKind: context.triggerKind
      })

      // Phase 2: Try AST-based analysis first
      let enhancedContext: EnhancedQueryContext | null = null
      let useEnhancedPath = this.config.enableASTAnalysis
      
      if (useEnhancedPath) {
        try {
          enhancedContext = this.enhancedAnalyzer.analyzeContext(query, offset)
          this.logDebug('Enhanced context analysis successful', {
            contextType: enhancedContext.contextType,
            availableTablesCount: enhancedContext.availableIdentifiers.tables.length,
            availableColumnsCount: enhancedContext.availableIdentifiers.columns.length
          })
          
          // Fall back to legacy if enhanced context is UNKNOWN (context detection failed)
          this.logDebug('Checking context type for fallback', {
            contextType: enhancedContext.contextType,
            enumValue: QueryContextType.UNKNOWN,
            areEqual: enhancedContext.contextType === QueryContextType.UNKNOWN,
            stringCompare: enhancedContext.contextType === 'UNKNOWN'
          })
          
          if (enhancedContext.contextType === QueryContextType.UNKNOWN || enhancedContext.contextType === 'UNKNOWN') {
            this.logDebug('Enhanced context returned UNKNOWN, falling back to legacy analyzer')
            useEnhancedPath = false
          }
        } catch (error) {
          this.logDebug('Enhanced context analysis failed, falling back to legacy', error)
          useEnhancedPath = false
        }
      }

      // Phase 1: Fallback or parallel legacy analysis
      let legacyContext: LegacyQueryContext | null = null
      if (!useEnhancedPath || this.config.fallbackToLegacy) {
        try {
          legacyContext = this.legacyAnalyzer.analyzeContext(model, position)
          this.logDebug('Legacy context analysis completed', {
            queryType: legacyContext.queryType,
            currentClause: legacyContext.currentClause
          })
        } catch (error) {
          this.logDebug('Legacy context analysis failed', error)
        }
      }

      // Generate completions using the best available context
      let suggestions: ScoredCompletion[] = []
      
      if (useEnhancedPath && enhancedContext) {
        // Use Phase 2 enhanced completions
        suggestions = await this.generateEnhancedCompletions(
          enhancedContext, 
          currentWord, 
          query, 
          offset
        )
      } else if (legacyContext) {
        // Use Phase 1 legacy completions (adapted to scoring system)
        suggestions = await this.generateLegacyCompletions(
          legacyContext, 
          currentWord, 
          query, 
          offset
        )
      } else {
        // Ultimate fallback - basic SQL keywords
        suggestions = this.generateFallbackCompletions(currentWord)
      }

      // Apply advanced scoring if enabled
      if (this.config.enableAdvancedScoring && enhancedContext) {
        const rawSuggestions = suggestions.map(s => ({
          label: s.label,
          type: s.type,
          detail: s.detail
        }))
        
        suggestions = this.completionScorer.scoreCompletions(
          rawSuggestions,
          enhancedContext,
          currentWord
        )
      }

      // Convert to Monaco completion items
      const monacoSuggestions = suggestions.map(s => this.toMonacoCompletionItem(s))
      
      const duration = performance.now() - startTime
      this.logDebug('Completions generated', {
        count: monacoSuggestions.length,
        duration: Math.round(duration),
        enhancedPath: useEnhancedPath
      })

      return {
        suggestions: monacoSuggestions,
        incomplete: false
      }

    } catch (error) {
      const duration = performance.now() - startTime
      this.logError('Completion generation failed', error, { duration })
      
      // Return basic fallback completions
      const fallback = this.generateFallbackCompletions('')
      return {
        suggestions: fallback.map(s => this.toMonacoCompletionItem(s)),
        incomplete: false
      }
    }
  }

  /**
   * Generate Enhanced Completions (Phase 2)
   * 
   * Uses AST-based context analysis to provide intelligent completions.
   */
  private async generateEnhancedCompletions(
    context: EnhancedQueryContext,
    currentWord: string,
    query: string,
    offset: number
  ): Promise<ScoredCompletion[]> {
    const suggestions: Array<{ label: string, type: CompletionType, detail?: string }> = []

    // Generate completions based on expected types
    for (const expectedType of context.expectedCompletions) {
      switch (expectedType) {
        case CompletionType.TABLE:
          suggestions.push(...this.generateTableCompletions(context))
          break
          
        case CompletionType.COLUMN:
          suggestions.push(...this.generateColumnCompletions(context))
          break
          
        case CompletionType.FUNCTION:
          suggestions.push(...this.generateFunctionCompletions(context))
          break
          
        case CompletionType.KEYWORD:
          suggestions.push(...this.generateKeywordCompletions(context))
          break
          
        case CompletionType.CTE:
          suggestions.push(...this.generateCTECompletions(context))
          break
          
        case CompletionType.ALIAS:
          suggestions.push(...this.generateAliasCompletions(context))
          break
      }
    }

    // Remove duplicates
    const uniqueSuggestions = this.deduplicateSuggestions(suggestions)
    
    // Return as ScoredCompletion format (scoring will be applied later)
    return uniqueSuggestions.map(s => ({
      label: s.label,
      type: s.type,
      insertText: s.label,
      detail: s.detail,
      score: {
        total: 0,
        contextRelevance: 0,
        semanticSimilarity: 0,
        usageFrequency: 0,
        recency: 0,
        specificity: 0,
        weights: this.config.scoringWeights || {}
      },
      kind: this.getMonacoKind(s.type),
      sortText: '000'
    }))
  }

  /**
   * Generate Legacy Completions (Phase 1)
   * 
   * Adapts existing Phase 1 completions to the new scoring system.
   */
  private async generateLegacyCompletions(
    context: LegacyQueryContext,
    currentWord: string,
    query: string,
    offset: number
  ): Promise<ScoredCompletion[]> {
    // Use existing logic but adapt to new format
    const suggestions: Array<{ label: string, type: CompletionType, detail?: string }> = []

    // Table completions
    if (context.expectsTable) {
      this.metadata.forEach(dataset => {
        suggestions.push({
          label: dataset.source,
          type: CompletionType.TABLE,
          detail: `Table • ${dataset.metadata_columns.length} columns`
        })
      })
    }

    // Column completions
    if (context.expectsColumn) {
      console.debug('[DEBUGGING] Generating column completions', {
        expectsColumn: context.expectsColumn,
        availableTablesCount: context.availableTables.length,
        metadataCount: this.metadata.length,
        availableTables: context.availableTables
      })
      
      if (context.availableTables.length > 0) {
        // Use specific tables from context
        context.availableTables.forEach(tableName => {
          const tableInfo = this.findTableInCache(tableName)
          if (tableInfo) {
            tableInfo.columns.forEach(column => {
              suggestions.push({
                label: `${tableName}.${column.name}`,
                type: CompletionType.COLUMN,
                detail: `Column • ${column.type}`
              })
            })
          }
        })
        console.debug('[DEBUGGING] Added columns from specific tables', { count: suggestions.length })
      } else {
        // No specific tables in context, show columns from all available tables
        console.debug('[DEBUGGING] No specific tables, adding columns from all datasets')
        let columnCount = 0
        this.metadata.forEach(dataset => {
          dataset.metadata_columns.forEach(column => {
            suggestions.push({
              label: `${dataset.source}.${column.name}`,
              type: CompletionType.COLUMN,
              detail: `Column • ${column.datatype} (from ${dataset.source})`
            })
            columnCount++
          })
        })
        console.debug('[DEBUGGING] Added columns from all tables', { 
          columnCount,
          totalSuggestions: suggestions.length,
          datasets: this.metadata.length 
        })
      }
    } else {
      console.debug('[DEBUGGING] Not expecting columns', { expectsColumn: context.expectsColumn })
    }

    // Function completions - only include UDFs when contextually appropriate
    if (!context.expectsTable || context.expectsFunction) {
      this.udfs.forEach(udf => {
        suggestions.push({
          label: udf.name.replace('${dataset}', '{dataset}'),
          type: CompletionType.FUNCTION,
          detail: udf.description
        })
      })
    }

    const defaultWeights = {
      contextRelevance: 0.4,
      semanticSimilarity: 0.3,
      usageFrequency: 0.15,
      recency: 0.1,
      specificity: 0.05
    }

    // Convert to scored format
    return suggestions.map(s => ({
      label: s.label,
      type: s.type,
      insertText: s.label,
      detail: s.detail,
      score: {
        total: 50, // Default score for legacy completions
        contextRelevance: 50,
        semanticSimilarity: 50,
        usageFrequency: 25,
        recency: 25,
        specificity: 50,
        weights: { ...defaultWeights, ...this.config.scoringWeights }
      },
      kind: this.getMonacoKind(s.type),
      sortText: '050'
    }))
  }

  private findTableInCache(tableName: string): TableInfo | undefined {
    return this.tableInfoCache.get(tableName) || Array.from(this.tableInfoCache.values()).find(t => 
      t.name === tableName || t.name.split('.').pop() === tableName
    )
  }

  /**
   * Generate Fallback Completions
   * 
   * Basic SQL keywords when all else fails.
   */
  private generateFallbackCompletions(currentWord: string): ScoredCompletion[] {
    const keywords = [
      'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 
      'RIGHT JOIN', 'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'DISTINCT'
    ]

    const defaultWeights = {
      contextRelevance: 0.4,
      semanticSimilarity: 0.3,
      usageFrequency: 0.15,
      recency: 0.1,
      specificity: 0.05
    }

    return keywords.map((keyword, index) => ({
      label: keyword,
      type: CompletionType.KEYWORD,
      insertText: keyword,
      detail: 'SQL Keyword',
      score: {
        total: 25,
        contextRelevance: 25,
        semanticSimilarity: keyword.toLowerCase().includes(currentWord.toLowerCase()) ? 75 : 25,
        usageFrequency: 25,
        recency: 25,
        specificity: 25,
        weights: { ...defaultWeights, ...this.config.scoringWeights }
      },
      kind: 17, // Monaco CompletionItemKind.Keyword
      sortText: (100 + index).toString()
    }))
  }

  /**
   * Completion Generators for Enhanced Context
   */

  private generateTableCompletions(context: EnhancedQueryContext): Array<{ label: string, type: CompletionType, detail?: string }> {
    const suggestions: Array<{ label: string, type: CompletionType, detail?: string }> = []
    
    this.metadata.forEach(dataset => {
      // Add full table name (e.g., "anvil.logs")
      suggestions.push({
        label: dataset.source,
        type: CompletionType.TABLE,
        detail: `Table • ${dataset.metadata_columns.length} columns`
      })
      
      // Also suggest just the table name if no schema conflicts (e.g., just "logs")
      const tableName = dataset.source.split('.')[1] || dataset.source
      if (this.isTableNameUnique(tableName)) {
        const schema = dataset.source.split('.')[0] || 'default'
        suggestions.push({
          label: tableName,
          type: CompletionType.TABLE,
          detail: `Table • ${schema}.${tableName}`
        })
      }
    })
    
    return suggestions
  }

  private generateColumnCompletions(context: EnhancedQueryContext): Array<{ label: string, type: CompletionType, detail?: string }> {
    const suggestions: Array<{ label: string, type: CompletionType, detail?: string }> = []
    
    context.availableIdentifiers.tables.forEach(table => {
      const tableInfo = this.tableInfoCache.get(`${table.schema || ''}.${table.name}`.replace(/^\./, ''))
      if (tableInfo) {
        const prefix = table.alias || table.name
        tableInfo.columns.forEach(column => {
          suggestions.push({
            label: `${prefix}.${column.name}`,
            type: CompletionType.COLUMN,
            detail: `Column • ${column.type}`
          })
          
          // Also suggest unqualified column name if unambiguous
          if (this.isColumnNameUnambiguous(column.name, context)) {
            suggestions.push({
              label: column.name,
              type: CompletionType.COLUMN,
              detail: `Column • ${column.type}`
            })
          }
        })
      }
    })
    
    return suggestions
  }

  private generateFunctionCompletions(context: EnhancedQueryContext): Array<{ label: string, type: CompletionType, detail?: string }> {
    return this.udfs.map(udf => ({
      label: udf.name.replace('${dataset}', '{dataset}'),
      type: CompletionType.FUNCTION,
      detail: udf.description
    }))
  }

  private generateKeywordCompletions(context: EnhancedQueryContext): Array<{ label: string, type: CompletionType, detail?: string }> {
    return context.availableIdentifiers.keywords.map(keyword => ({
      label: keyword,
      type: CompletionType.KEYWORD,
      detail: 'SQL Keyword'
    }))
  }

  private generateCTECompletions(context: EnhancedQueryContext): Array<{ label: string, type: CompletionType, detail?: string }> {
    return context.availableIdentifiers.ctes.map(cte => ({
      label: cte.name,
      type: CompletionType.CTE,
      detail: `CTE • ${cte.columns ? cte.columns.length + ' columns' : 'Common Table Expression'}`
    }))
  }

  private generateAliasCompletions(context: EnhancedQueryContext): Array<{ label: string, type: CompletionType, detail?: string }> {
    const suggestions: Array<{ label: string, type: CompletionType, detail?: string }> = []
    
    context.scope.selectAliases.forEach((expression, alias) => {
      suggestions.push({
        label: alias,
        type: CompletionType.ALIAS,
        detail: 'Column Alias'
      })
    })
    
    return suggestions
  }

  /**
   * Utility Methods
   */

  private getCurrentWord(model: monaco.editor.ITextModel, position: monaco.Position): string {
    const word = model.getWordAtPosition(position)
    return word ? word.word : ''
  }

  private deduplicateSuggestions(suggestions: Array<{ label: string, type: CompletionType, detail?: string }>): Array<{ label: string, type: CompletionType, detail?: string }> {
    const seen = new Set<string>()
    return suggestions.filter(s => {
      const key = `${s.label}:${s.type}`
      if (seen.has(key)) return false
      seen.add(key)
      return true
    })
  }

  private isTableNameUnique(tableName: string): boolean {
    let count = 0
    for (const dataset of this.metadata) {
      const datasetTableName = dataset.source.split('.')[1] || dataset.source
      if (datasetTableName === tableName) {
        count++
        if (count > 1) return false
      }
    }
    return count === 1
  }

  private isColumnNameUnambiguous(columnName: string, context: EnhancedQueryContext): boolean {
    let count = 0
    context.availableIdentifiers.tables.forEach(table => {
      const tableInfo = this.tableInfoCache.get(`${table.schema || ''}.${table.name}`.replace(/^\./, ''))
      if (tableInfo) {
        if (tableInfo.columns.some(col => col.name === columnName)) {
          count++
        }
      }
    })
    return count === 1
  }

  private getMonacoKind(type: CompletionType): number {
    // Monaco CompletionItemKind enum values
    switch (type) {
      case CompletionType.TABLE: return 17 // Module
      case CompletionType.COLUMN: return 10 // Property
      case CompletionType.FUNCTION: return 3 // Function
      case CompletionType.KEYWORD: return 17 // Keyword
      case CompletionType.CTE: return 17 // Module
      case CompletionType.ALIAS: return 6 // Variable
      case CompletionType.OPERATOR: return 24 // Operator
      case CompletionType.LITERAL: return 12 // Value
      default: return 1 // Text
    }
  }

  private toMonacoCompletionItem(scored: ScoredCompletion): any {
    const item: any = {
      label: scored.label,
      kind: scored.kind,
      insertText: scored.insertText,
      detail: scored.detail,
      sortText: scored.sortText,
      filterText: scored.filterText
    }

    // Add documentation if available
    if (scored.documentation) {
      item.documentation = scored.documentation
    }

    // Add additional text edits if available
    if (scored.additionalTextEdits) {
      item.additionalTextEdits = scored.additionalTextEdits
    }

    // Add debug information in development
    if (this.config.enableDebugLogging && scored.score.debugInfo) {
      item.documentation = {
        value: `${item.documentation || ''}\n\n**Debug Info:**\n- Score: ${scored.score.total}\n- Context: ${scored.score.debugInfo.contextMatch}\n- Similarity: ${scored.score.debugInfo.similarityReason}`
      }
    }

    return item
  }

  private buildTableInfoCache(): void {
    this.tableInfoCache.clear()
    
    this.metadata.forEach(dataset => {
      const columns: ColumnInfo[] = dataset.metadata_columns.map(col => ({
        name: col.name,
        type: col.datatype || 'unknown',
        nullable: true // Assume nullable since we don't have this info
      }))

      const tableInfo: TableInfo = {
        name: dataset.source,
        schema: dataset.source.split('.')[0] || 'default', // Extract schema from source like "anvil.logs"
        columns,
        rowCount: undefined // We don't have row count info
      }

      // Cache by full table name (e.g., "anvil.logs")
      this.tableInfoCache.set(dataset.source, tableInfo)
      
      // Also cache by table name only if unique (e.g., just "logs")
      const tableName = dataset.source.split('.')[1] || dataset.source
      if (this.isTableNameUnique(tableName)) {
        this.tableInfoCache.set(tableName, tableInfo)
      }
    })
    
    this.logDebug('Table info cache built', {
      cacheSize: this.tableInfoCache.size
    })
  }

  /**
   * Update Provider Data
   * 
   * Updates metadata and UDF data when fresh data is available.
   */
  updateData(metadata: readonly DatasetMetadata[], udfs: readonly UserDefinedFunction[]): void {
    if (this.isDisposed) return
    
    this.metadata = metadata
    this.udfs = udfs
    this.buildTableInfoCache()
    
    this.logDebug('Provider data updated', {
      tableCount: metadata.length,
      udfCount: udfs.length
    })
  }

  /**
   * Record Completion Usage
   * 
   * Records when a completion is selected for learning purposes.
   */
  recordCompletion(label: string, contextType: string): void {
    if (this.config.enableAdvancedScoring) {
      // Map legacy context to enhanced context type
      const enhancedContextType = this.mapLegacyToEnhancedContext(contextType)
      this.completionScorer.recordUsage(label, enhancedContextType)
    }
  }

  private mapLegacyToEnhancedContext(legacyContext: string): QueryContextType {
    switch (legacyContext.toLowerCase()) {
      case 'select': return QueryContextType.SELECT_LIST
      case 'from': return QueryContextType.FROM_CLAUSE
      case 'where': return QueryContextType.WHERE_CONDITION
      case 'join': return QueryContextType.JOIN_CLAUSE
      case 'group by': return QueryContextType.GROUP_BY
      case 'order by': return QueryContextType.ORDER_BY
      case 'having': return QueryContextType.HAVING_CONDITION
      default: return QueryContextType.UNKNOWN
    }
  }

  /**
   * Dispose Resources
   */
  dispose(): void {
    if (this.isDisposed) return
    
    this.tableInfoCache.clear()
    this.isDisposed = true
    
    this.logDebug('Enhanced completion provider disposed')
  }

  /**
   * Logging Utilities
   */

  private logDebug(message: string, data?: any): void {
    if (this.config.enableDebugLogging) {
      console.debug(`[EnhancedCompletionProvider] ${message}`, data)
    }
  }

  private logError(message: string, error: any, data?: any): void {
    console.error(`[EnhancedCompletionProvider] ${message}`, error, data)
  }
}