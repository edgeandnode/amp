/**
 * Enhanced SQL Intellisense Integration Tests
 * 
 * Tests the integration of Phase 2 advanced query parser with Monaco Editor.
 * Demonstrates the enhanced completion capabilities including AST-based parsing,
 * intelligent scoring, and context analysis.
 */

import { describe, test, expect } from 'vitest'
import { 
  SQLTokenizer, 
  SQLParser, 
  EnhancedContextAnalyzer, 
  CompletionScorer,
  QueryContextType,
  CompletionType
} from '../../../src/services/sql/advancedQueryParser'

describe('Enhanced SQL Intellisense Integration', () => {
  describe('SQL Tokenizer', () => {
    test('should tokenize basic SELECT query', () => {
      const tokenizer = new SQLTokenizer()
      const tokens = tokenizer.tokenize('SELECT id FROM users WHERE active = true')
      
      expect(tokens.length).toBeGreaterThan(8) // Should have multiple tokens including EOF
      expect(tokens[0].type).toBe('SELECT')
      expect(tokens[0].value).toBe('SELECT')
      expect(tokens.find(t => t.type === 'IDENTIFIER' && t.value === 'id')).toBeDefined()
      expect(tokens.find(t => t.type === 'FROM')).toBeDefined()
      expect(tokens.find(t => t.type === 'WHERE')).toBeDefined()
    })

    test('should handle complex query with JOINs', () => {
      const tokenizer = new SQLTokenizer()
      const query = `
        SELECT u.name, p.title 
        FROM users u 
        LEFT JOIN posts p ON u.id = p.user_id 
        WHERE u.active = true
      `
      const tokens = tokenizer.tokenize(query)
      
      expect(tokens.find(t => t.type === 'LEFT')).toBeDefined()
      expect(tokens.find(t => t.type === 'JOIN')).toBeDefined()
      expect(tokens.find(t => t.type === 'ON')).toBeDefined()
      expect(tokens.find(t => t.type === 'EQUALS')).toBeDefined()
    })

    test('should handle string literals and comments', () => {
      const tokenizer = new SQLTokenizer()
      const query = `-- This is a comment
      SELECT 'hello world', /* inline comment */ name FROM users`
      const tokens = tokenizer.tokenize(query)
      
      expect(tokens.find(t => t.type === 'LINE_COMMENT')).toBeDefined()
      expect(tokens.find(t => t.type === 'BLOCK_COMMENT')).toBeDefined()
      expect(tokens.find(t => t.type === 'STRING_LITERAL')).toBeDefined()
    })
  })

  describe('SQL Parser', () => {
    test('should parse basic SELECT statement', () => {
      const tokenizer = new SQLTokenizer()
      const parser = new SQLParser()
      
      const tokens = tokenizer.tokenize('SELECT id FROM users')
      const ast = parser.parse(tokens)
      
      expect(ast.root.type).toBe('SelectStatement')
      expect(ast.root.selectList.type).toBe('SelectList')
      expect(ast.root.fromClause?.type).toBe('FromClause')
      expect(ast.errors.length).toBe(0)
    })

    test('should handle parse errors gracefully', () => {
      const tokenizer = new SQLTokenizer()
      const parser = new SQLParser()
      
      const tokens = tokenizer.tokenize('SELECT FROM') // Invalid syntax
      const ast = parser.parse(tokens)
      
      expect(ast.root.type).toBe('SelectStatement')
      expect(ast.errors.length).toBeGreaterThan(0)
    })
  })

  describe('Enhanced Context Analyzer', () => {
    test('should analyze SELECT list context', () => {
      const analyzer = new EnhancedContextAnalyzer()
      const query = 'SELECT ' // Cursor at end, in SELECT list context
      const cursorOffset = 7
      
      const context = analyzer.analyzeContext(query, cursorOffset)
      
      expect(context.contextType).toBe(QueryContextType.SELECT_LIST)
      expect(context.expectedCompletions).toContain(CompletionType.COLUMN)
      expect(context.expectedCompletions).toContain(CompletionType.FUNCTION)
    })

    test('should analyze FROM clause context', () => {
      const analyzer = new EnhancedContextAnalyzer()
      const query = 'SELECT id FROM ' // Cursor at end, in FROM clause context
      const cursorOffset = 15
      
      const context = analyzer.analyzeContext(query, cursorOffset)
      
      expect(context.contextType).toBe(QueryContextType.FROM_CLAUSE)
      expect(context.expectedCompletions).toContain(CompletionType.TABLE)
      expect(context.expectedCompletions).toContain(CompletionType.CTE)
    })

    test('should analyze WHERE clause context', () => {
      const analyzer = new EnhancedContextAnalyzer()
      const query = 'SELECT id FROM users WHERE ' // Cursor at end, in WHERE clause context
      const cursorOffset = 27
      
      const context = analyzer.analyzeContext(query, cursorOffset)
      
      expect(context.contextType).toBe(QueryContextType.WHERE_CONDITION)
      expect(context.expectedCompletions).toContain(CompletionType.COLUMN)
      expect(context.expectedCompletions).toContain(CompletionType.OPERATOR)
    })

    test('should handle malformed queries with fallback', () => {
      const analyzer = new EnhancedContextAnalyzer()
      const query = 'SELECT id FROM users WHERE active =' // Incomplete query
      const cursorOffset = query.length
      
      const context = analyzer.analyzeContext(query, cursorOffset)
      
      // Should not crash and should provide some context
      expect(context).toBeDefined()
      expect(context.contextType).toBeDefined()
    })
  })

  describe('Completion Scorer', () => {
    test('should score completions based on context relevance', () => {
      const scorer = new CompletionScorer({
        weights: {
          contextRelevance: 1.0,
          semanticSimilarity: 0.0,
          usageFrequency: 0.0,
          recency: 0.0,
          specificity: 0.0
        },
        enableUsageTracking: false,
        enableSemanticSimilarity: false,
        maxSuggestions: 10,
        minScore: 0,
        debugMode: false
      })

      const suggestions = [
        { label: 'users', type: CompletionType.TABLE, detail: 'Table' },
        { label: 'id', type: CompletionType.COLUMN, detail: 'Column' },
        { label: 'SELECT', type: CompletionType.KEYWORD, detail: 'Keyword' }
      ]

      const context = {
        contextType: QueryContextType.FROM_CLAUSE,
        expectedCompletions: [CompletionType.TABLE],
        availableIdentifiers: {
          tables: [],
          columns: [],
          functions: [],
          keywords: [],
          ctes: []
        },
        scope: {
          type: 'ROOT' as const,
          tables: new Map(),
          ctes: new Map(),
          selectAliases: new Map()
        },
        contextFilters: {},
        cursorOffset: 0,
        currentNode: null
      }

      const scoredCompletions = scorer.scoreCompletions(suggestions, context, '')
      
      expect(scoredCompletions.length).toBe(3)
      
      // Table suggestion should score highest in FROM clause context
      const tableCompletion = scoredCompletions.find(c => c.label === 'users')
      const columnCompletion = scoredCompletions.find(c => c.label === 'id')
      
      expect(tableCompletion).toBeDefined()
      expect(columnCompletion).toBeDefined()
      expect(tableCompletion!.score.contextRelevance).toBeGreaterThan(columnCompletion!.score.contextRelevance)
    })

    test('should score completions based on semantic similarity', () => {
      const scorer = new CompletionScorer({
        weights: {
          contextRelevance: 0.0,
          semanticSimilarity: 1.0,
          usageFrequency: 0.0,
          recency: 0.0,
          specificity: 0.0
        },
        enableUsageTracking: false,
        enableSemanticSimilarity: true,
        maxSuggestions: 10,
        minScore: 0,
        debugMode: false
      })

      const suggestions = [
        { label: 'user_id', type: CompletionType.COLUMN, detail: 'Column' },
        { label: 'name', type: CompletionType.COLUMN, detail: 'Column' },
        { label: 'active', type: CompletionType.COLUMN, detail: 'Column' }
      ]

      const context = {
        contextType: QueryContextType.SELECT_LIST,
        expectedCompletions: [CompletionType.COLUMN],
        availableIdentifiers: {
          tables: [],
          columns: suggestions.map(s => s.label),
          functions: [],
          keywords: [],
          ctes: []
        },
        scope: {
          type: 'ROOT' as const,
          tables: new Map(),
          ctes: new Map(),
          selectAliases: new Map()
        },
        contextFilters: {},
        cursorOffset: 0,
        currentNode: null
      }

      const scoredCompletions = scorer.scoreCompletions(suggestions, context, 'user')
      
      expect(scoredCompletions.length).toBe(3)
      
      // 'user_id' should score highest due to semantic similarity with 'user'
      const userIdCompletion = scoredCompletions.find(c => c.label === 'user_id')
      const nameCompletion = scoredCompletions.find(c => c.label === 'name')
      
      expect(userIdCompletion).toBeDefined()
      expect(nameCompletion).toBeDefined()
      expect(userIdCompletion!.score.semanticSimilarity).toBeGreaterThan(nameCompletion!.score.semanticSimilarity)
    })

    test('should record and use usage statistics', () => {
      const scorer = new CompletionScorer({
        weights: {
          contextRelevance: 0.0,
          semanticSimilarity: 0.0,
          usageFrequency: 1.0,
          recency: 0.0,
          specificity: 0.0
        },
        enableUsageTracking: true,
        enableSemanticSimilarity: false,
        maxSuggestions: 10,
        minScore: 0,
        debugMode: false
      })

      // Record some usage
      scorer.recordUsage('frequently_used_column', QueryContextType.SELECT_LIST)
      scorer.recordUsage('frequently_used_column', QueryContextType.SELECT_LIST)
      scorer.recordUsage('rarely_used_column', QueryContextType.SELECT_LIST)

      const suggestions = [
        { label: 'frequently_used_column', type: CompletionType.COLUMN, detail: 'Column' },
        { label: 'rarely_used_column', type: CompletionType.COLUMN, detail: 'Column' },
        { label: 'never_used_column', type: CompletionType.COLUMN, detail: 'Column' }
      ]

      const context = {
        contextType: QueryContextType.SELECT_LIST,
        expectedCompletions: [CompletionType.COLUMN],
        availableIdentifiers: {
          tables: [],
          columns: suggestions.map(s => s.label),
          functions: [],
          keywords: [],
          ctes: []
        },
        scope: {
          type: 'ROOT' as const,
          tables: new Map(),
          ctes: new Map(),
          selectAliases: new Map()
        },
        contextFilters: {},
        cursorOffset: 0,
        currentNode: null
      }

      const scoredCompletions = scorer.scoreCompletions(suggestions, context, '')
      
      const frequentCompletion = scoredCompletions.find(c => c.label === 'frequently_used_column')
      const neverUsedCompletion = scoredCompletions.find(c => c.label === 'never_used_column')
      
      expect(frequentCompletion).toBeDefined()
      expect(neverUsedCompletion).toBeDefined()
      // Usage frequency should be different (and typically higher for frequently used items)
      expect(frequentCompletion!.score.usageFrequency).not.toBe(neverUsedCompletion!.score.usageFrequency)
    })
  })

  describe('Integration: End-to-End Workflow', () => {
    test('should provide complete SQL intellisense workflow', () => {
      // Simulate the complete workflow from tokenization to scored completions  
      const query = 'SELECT id, name FROM users WHERE '
      const cursorOffset = 34 // After WHERE

      // 1. Tokenize
      const tokenizer = new SQLTokenizer()
      const tokens = tokenizer.tokenize(query)
      expect(tokens.length).toBeGreaterThan(0)

      // 2. Parse to AST
      const parser = new SQLParser()
      const ast = parser.parse(tokens)
      expect(ast.root.type).toBe('SelectStatement')

      // 3. Analyze context
      const analyzer = new EnhancedContextAnalyzer()
      const context = analyzer.analyzeContext(query, cursorOffset)
      expect(context.contextType).toBe(QueryContextType.WHERE_CONDITION)

      // 4. Score completions
      const scorer = new CompletionScorer()
      const mockSuggestions = [
        { label: 'id', type: CompletionType.COLUMN, detail: 'Column • integer' },
        { label: 'name', type: CompletionType.COLUMN, detail: 'Column • text' },
        { label: 'active', type: CompletionType.COLUMN, detail: 'Column • boolean' },
        { label: 'AND', type: CompletionType.KEYWORD, detail: 'SQL Keyword' },
        { label: 'OR', type: CompletionType.KEYWORD, detail: 'SQL Keyword' }
      ]

      const scoredCompletions = scorer.scoreCompletions(mockSuggestions, context, '')
      
      expect(scoredCompletions.length).toBe(mockSuggestions.length)
      expect(scoredCompletions[0].score.total).toBeGreaterThan(0)
      
      // Completions should be sorted by score
      for (let i = 1; i < scoredCompletions.length; i++) {
        expect(scoredCompletions[i-1].score.total).toBeGreaterThanOrEqual(scoredCompletions[i].score.total)
      }
    })
  })
})