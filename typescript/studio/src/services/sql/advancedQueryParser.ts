/**
 * Advanced SQL Query Parser
 * 
 * This module provides AST-based query parsing for advanced SQL intellisense features.
 * It includes a comprehensive tokenizer and parser that can handle complex SQL constructs
 * including CTEs, subqueries, JOINs, and provides robust error recovery.
 * 
 * Key Features:
 * - Comprehensive SQL tokenization with keyword recognition
 * - AST-based query structure analysis
 * - Error recovery for malformed queries
 * - Context analysis for nested queries and CTEs
 * - Table alias resolution and scoping
 * 
 * @file advancedQueryParser.ts
 * @author SQL Intellisense System - Phase 2
 */

/**
 * SQL Token Types
 * 
 * Enumeration of all token types that can appear in SQL queries.
 * Used by the tokenizer to classify lexical elements.
 */
export enum SqlTokenType {
  // Keywords
  SELECT = 'SELECT',
  FROM = 'FROM', 
  WHERE = 'WHERE',
  JOIN = 'JOIN',
  INNER = 'INNER',
  LEFT = 'LEFT',
  RIGHT = 'RIGHT',
  FULL = 'FULL',
  OUTER = 'OUTER',
  ON = 'ON',
  AS = 'AS',
  AND = 'AND',
  OR = 'OR',
  NOT = 'NOT',
  IN = 'IN',
  EXISTS = 'EXISTS',
  BETWEEN = 'BETWEEN',
  LIKE = 'LIKE',
  IS = 'IS',
  NULL = 'NULL',
  GROUP = 'GROUP',
  BY = 'BY',
  HAVING = 'HAVING',
  ORDER = 'ORDER',
  LIMIT = 'LIMIT',
  OFFSET = 'OFFSET',
  DISTINCT = 'DISTINCT',
  ALL = 'ALL',
  UNION = 'UNION',
  INTERSECT = 'INTERSECT',
  EXCEPT = 'EXCEPT',
  WITH = 'WITH',
  RECURSIVE = 'RECURSIVE',
  CASE = 'CASE',
  WHEN = 'WHEN',
  THEN = 'THEN',
  ELSE = 'ELSE',
  END = 'END',
  TRUE = 'TRUE',
  FALSE = 'FALSE',
  
  // Identifiers and literals
  IDENTIFIER = 'IDENTIFIER',
  STRING_LITERAL = 'STRING_LITERAL',
  NUMBER_LITERAL = 'NUMBER_LITERAL',
  
  // Operators
  EQUALS = 'EQUALS',              // =
  NOT_EQUALS = 'NOT_EQUALS',      // !=, <>
  LESS_THAN = 'LESS_THAN',        // <
  GREATER_THAN = 'GREATER_THAN',  // >
  LESS_EQUAL = 'LESS_EQUAL',      // <=
  GREATER_EQUAL = 'GREATER_EQUAL', // >=
  PLUS = 'PLUS',                  // +
  MINUS = 'MINUS',                // -
  MULTIPLY = 'MULTIPLY',          // *
  DIVIDE = 'DIVIDE',              // /
  MODULO = 'MODULO',              // %
  
  // Delimiters and punctuation
  COMMA = 'COMMA',                // ,
  SEMICOLON = 'SEMICOLON',        // ;
  DOT = 'DOT',                    // .
  LEFT_PAREN = 'LEFT_PAREN',      // (
  RIGHT_PAREN = 'RIGHT_PAREN',    // )
  
  // Special tokens
  WHITESPACE = 'WHITESPACE',
  LINE_COMMENT = 'LINE_COMMENT',   // --
  BLOCK_COMMENT = 'BLOCK_COMMENT', // /* */
  
  // Error handling
  INVALID = 'INVALID',
  EOF = 'EOF'
}

/**
 * SQL Token
 * 
 * Represents a single token in the SQL query with position information.
 */
export interface SqlToken {
  type: SqlTokenType
  value: string
  startOffset: number
  endOffset: number
  line: number
  column: number
}

/**
 * Tokenizer Position
 * 
 * Tracks current position in the input string during tokenization.
 */
interface TokenizerPosition {
  offset: number
  line: number
  column: number
}

/**
 * SQL Tokenizer
 * 
 * Lexical analyzer for SQL queries that breaks input into classified tokens.
 * Handles all SQL constructs including keywords, operators, identifiers, strings,
 * comments, and provides error recovery for invalid tokens.
 */
export class SQLTokenizer {
  private input: string = ''
  private position: TokenizerPosition = { offset: 0, line: 1, column: 1 }
  private tokens: SqlToken[] = []
  
  // SQL Keywords - case insensitive
  private readonly keywords = new Set([
    'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER',
    'ON', 'AS', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS', 'NULL',
    'GROUP', 'BY', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET', 'DISTINCT', 'ALL',
    'UNION', 'INTERSECT', 'EXCEPT', 'WITH', 'RECURSIVE', 'CASE', 'WHEN', 'THEN',
    'ELSE', 'END', 'TRUE', 'FALSE', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'
  ])
  
  /**
   * Tokenize SQL Input
   * 
   * Main entry point for tokenization. Processes the entire input string
   * and returns an array of classified tokens.
   * 
   * @param input - SQL query string to tokenize
   * @returns Array of SQL tokens with position information
   */
  tokenize(input: string): SqlToken[] {
    this.input = input
    this.position = { offset: 0, line: 1, column: 1 }
    this.tokens = []
    
    while (!this.isAtEnd()) {
      const startPosition = { ...this.position }
      
      try {
        const token = this.scanToken()
        if (token) {
          this.tokens.push({
            ...token,
            startOffset: startPosition.offset,
            endOffset: this.position.offset,
            line: startPosition.line,
            column: startPosition.column
          })
        }
      } catch (error) {
        // Error recovery - create invalid token and continue
        const invalidValue = this.advance()
        this.tokens.push({
          type: SqlTokenType.INVALID,
          value: invalidValue,
          startOffset: startPosition.offset,
          endOffset: this.position.offset,
          line: startPosition.line,
          column: startPosition.column
        })
      }
    }
    
    // Add EOF token
    this.tokens.push({
      type: SqlTokenType.EOF,
      value: '',
      startOffset: this.position.offset,
      endOffset: this.position.offset,
      line: this.position.line,
      column: this.position.column
    })
    
    return this.tokens
  }
  
  /**
   * Scan Single Token
   * 
   * Analyzes the current position and extracts the next token.
   * 
   * @returns SQL token or null for whitespace/comments (depending on configuration)
   */
  private scanToken(): SqlToken | null {
    const char = this.advance()
    
    switch (char) {
      case ' ':
      case '\t':
      case '\r':
        return this.scanWhitespace()
        
      case '\n':
        this.position.line++
        this.position.column = 1
        return this.scanWhitespace()
        
      case ',': return this.createToken(SqlTokenType.COMMA, char)
      case ';': return this.createToken(SqlTokenType.SEMICOLON, char)
      case '.': return this.createToken(SqlTokenType.DOT, char)
      case '(': return this.createToken(SqlTokenType.LEFT_PAREN, char)
      case ')': return this.createToken(SqlTokenType.RIGHT_PAREN, char)
      case '+': return this.createToken(SqlTokenType.PLUS, char)
      case '-': 
        if (this.peek() === '-') {
          return this.scanLineComment()
        }
        return this.createToken(SqlTokenType.MINUS, char)
      case '*': return this.createToken(SqlTokenType.MULTIPLY, char)
      case '/': 
        if (this.peek() === '*') {
          return this.scanBlockComment()
        }
        return this.createToken(SqlTokenType.DIVIDE, char)
      case '%': return this.createToken(SqlTokenType.MODULO, char)
      case '=': return this.createToken(SqlTokenType.EQUALS, char)
      case '!':
        if (this.match('=')) {
          return this.createToken(SqlTokenType.NOT_EQUALS, '!=')
        }
        throw new Error(`Unexpected character: ${char}`)
      case '<':
        if (this.match('=')) {
          return this.createToken(SqlTokenType.LESS_EQUAL, '<=')
        } else if (this.match('>')) {
          return this.createToken(SqlTokenType.NOT_EQUALS, '<>')
        }
        return this.createToken(SqlTokenType.LESS_THAN, char)
      case '>':
        if (this.match('=')) {
          return this.createToken(SqlTokenType.GREATER_EQUAL, '>=')
        }
        return this.createToken(SqlTokenType.GREATER_THAN, char)
      case "'":
      case '"':
        return this.scanString(char)
        
      default:
        if (this.isAlpha(char)) {
          return this.scanIdentifier()
        } else if (this.isDigit(char)) {
          return this.scanNumber()
        } else {
          throw new Error(`Unexpected character: ${char}`)
        }
    }
  }
  
  /**
   * Scan Whitespace
   * 
   * Consumes all consecutive whitespace characters.
   * Currently returns null (whitespace is typically ignored).
   */
  private scanWhitespace(): SqlToken | null {
    while (!this.isAtEnd() && this.isWhitespace(this.peek())) {
      if (this.peek() === '\n') {
        this.position.line++
        this.position.column = 1
      }
      this.advance()
    }
    
    // For now, return null to ignore whitespace
    // Could return whitespace tokens if needed for formatting preservation
    return null
  }
  
  /**
   * Scan Line Comment
   * 
   * Processes SQL line comments (-- to end of line).
   */
  private scanLineComment(): SqlToken | null {
    this.advance() // consume second '-'
    
    let value = '--'
    while (!this.isAtEnd() && this.peek() !== '\n') {
      value += this.advance()
    }
    
    return this.createToken(SqlTokenType.LINE_COMMENT, value)
  }
  
  /**
   * Scan Block Comment
   * 
   * Processes SQL block comments (slash-star to star-slash).
   * Handles nested comments and provides error recovery for unclosed comments.
   */
  private scanBlockComment(): SqlToken | null {
    this.advance() // consume '*'
    
    let value = '/*'
    let depth = 1
    
    while (!this.isAtEnd() && depth > 0) {
      const char = this.advance()
      value += char
      
      if (char === '\n') {
        this.position.line++
        this.position.column = 1
      } else if (char === '/' && this.match('*')) {
        depth++
        value += '*'
      } else if (char === '*' && this.match('/')) {
        depth--
        value += '/'
      }
    }
    
    return this.createToken(SqlTokenType.BLOCK_COMMENT, value)
  }
  
  /**
   * Scan String Literal
   * 
   * Processes quoted string literals with escape sequence handling.
   * Supports both single and double quotes.
   * 
   * @param quote - Opening quote character (' or ")
   */
  private scanString(quote: string): SqlToken {
    let value = quote
    
    while (!this.isAtEnd() && this.peek() !== quote) {
      const char = this.advance()
      value += char
      
      if (char === '\n') {
        this.position.line++
        this.position.column = 1
      }
      
      // Handle escaped quotes
      if (char === '\\' && !this.isAtEnd()) {
        value += this.advance()
      }
    }
    
    if (!this.isAtEnd()) {
      value += this.advance() // closing quote
    }
    
    return this.createToken(SqlTokenType.STRING_LITERAL, value)
  }
  
  /**
   * Scan Identifier
   * 
   * Processes identifiers and keywords. Determines if the identifier
   * is a reserved keyword and classifies accordingly.
   */
  private scanIdentifier(): SqlToken {
    let value = this.input[this.position.offset - 1] // Include the already consumed character
    
    while (!this.isAtEnd() && (this.isAlphaNumeric(this.peek()) || this.peek() === '_')) {
      value += this.advance()
    }
    
    // Check if it's a keyword (case insensitive)
    const upperValue = value.toUpperCase()
    if (this.keywords.has(upperValue)) {
      return this.createToken(upperValue as SqlTokenType, value)
    }
    
    return this.createToken(SqlTokenType.IDENTIFIER, value)
  }
  
  /**
   * Scan Number
   * 
   * Processes numeric literals including integers and decimals.
   */
  private scanNumber(): SqlToken {
    let value = this.input[this.position.offset - 1] // Include the already consumed character
    
    // Integer part
    while (!this.isAtEnd() && this.isDigit(this.peek())) {
      value += this.advance()
    }
    
    // Decimal part
    if (!this.isAtEnd() && this.peek() === '.' && this.isDigit(this.peekNext())) {
      value += this.advance() // consume '.'
      
      while (!this.isAtEnd() && this.isDigit(this.peek())) {
        value += this.advance()
      }
    }
    
    return this.createToken(SqlTokenType.NUMBER_LITERAL, value)
  }
  
  /**
   * Helper Methods
   */
  
  private createToken(type: SqlTokenType, value: string): SqlToken {
    return {
      type,
      value,
      startOffset: 0, // Will be set by caller
      endOffset: 0,   // Will be set by caller
      line: 0,        // Will be set by caller
      column: 0       // Will be set by caller
    }
  }
  
  private isAtEnd(): boolean {
    return this.position.offset >= this.input.length
  }
  
  private advance(): string {
    if (this.isAtEnd()) return '\0'
    
    const char = this.input[this.position.offset]
    this.position.offset++
    this.position.column++
    
    return char
  }
  
  private peek(): string {
    if (this.isAtEnd()) return '\0'
    return this.input[this.position.offset]
  }
  
  private peekNext(): string {
    if (this.position.offset + 1 >= this.input.length) return '\0'
    return this.input[this.position.offset + 1]
  }
  
  private match(expected: string): boolean {
    if (this.isAtEnd()) return false
    if (this.input[this.position.offset] !== expected) return false
    
    this.advance()
    return true
  }
  
  private isWhitespace(char: string): boolean {
    return char === ' ' || char === '\t' || char === '\r' || char === '\n'
  }
  
  private isAlpha(char: string): boolean {
    return (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || char === '_'
  }
  
  private isDigit(char: string): boolean {
    return char >= '0' && char <= '9'
  }
  
  private isAlphaNumeric(char: string): boolean {
    return this.isAlpha(char) || this.isDigit(char)
  }
}

/**
 * AST Node Types
 * 
 * Defines the structure of SQL Abstract Syntax Tree nodes.
 */
export interface ASTNode {
  type: string
  startOffset: number
  endOffset: number
  parent?: ASTNode
  children: ASTNode[]
}

export interface SelectStatement extends ASTNode {
  type: 'SelectStatement'
  withClause?: WithClause
  selectList: SelectList
  fromClause?: FromClause
  joinClauses: JoinClause[]
  whereClause?: WhereClause
  groupByClause?: GroupByClause
  havingClause?: HavingClause
  orderByClause?: OrderByClause
  limitClause?: LimitClause
}

export interface WithClause extends ASTNode {
  type: 'WithClause'
  recursive: boolean
  cteDefinitions: CTEDefinition[]
}

export interface CTEDefinition extends ASTNode {
  type: 'CTEDefinition'
  name: string
  columns?: string[]
  query: SelectStatement
}

export interface SelectList extends ASTNode {
  type: 'SelectList'
  distinct: boolean
  items: SelectItem[]
}

export interface SelectItem extends ASTNode {
  type: 'SelectItem'
  expression: Expression
  alias?: string
}

export interface FromClause extends ASTNode {
  type: 'FromClause'
  table: TableExpression
}

export interface JoinClause extends ASTNode {
  type: 'JoinClause'
  joinType: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS'
  table: TableExpression
  condition?: Expression
}

export interface TableExpression extends ASTNode {
  type: 'TableExpression'
  name: string
  alias?: string
}

export interface WhereClause extends ASTNode {
  type: 'WhereClause'
  condition: Expression
}

export interface GroupByClause extends ASTNode {
  type: 'GroupByClause'
  expressions: Expression[]
}

export interface HavingClause extends ASTNode {
  type: 'HavingClause'
  condition: Expression
}

export interface OrderByClause extends ASTNode {
  type: 'OrderByClause'
  items: OrderByItem[]
}

export interface OrderByItem extends ASTNode {
  type: 'OrderByItem'
  expression: Expression
  direction: 'ASC' | 'DESC'
}

export interface LimitClause extends ASTNode {
  type: 'LimitClause'
  count: number
  offset?: number
}

export interface Expression extends ASTNode {
  type: 'Expression'
  // Expression types will be expanded in next phase
}

/**
 * Query AST Interface
 * 
 * Represents the complete Abstract Syntax Tree for a SQL query.
 */
export interface QueryAST {
  root: SelectStatement
  errors: ParseError[]
  tokens: SqlToken[]
  
  /**
   * Find AST node at specific position
   */
  findNodeAtOffset(offset: number): ASTNode | null
}

/**
 * Parse Error Interface
 * 
 * Represents errors encountered during parsing with recovery information.
 */
export interface ParseError {
  message: string
  startOffset: number
  endOffset: number
  line: number
  column: number
  recoverable: boolean
  expectedTokens?: SqlTokenType[]
}

/**
 * SQL Parser
 * 
 * Recursive descent parser that builds an AST from tokenized SQL input.
 * Provides error recovery and handles complex SQL constructs.
 */
export class SQLParser {
  private tokens: SqlToken[] = []
  private current = 0
  private errors: ParseError[] = []
  
  /**
   * Parse SQL tokens into AST
   * 
   * @param tokens - Array of SQL tokens from tokenizer
   * @returns Query AST with error information
   */
  parse(tokens: SqlToken[]): QueryAST {
    this.tokens = tokens
    this.current = 0
    this.errors = []
    
    try {
      const root = this.parseSelectStatement()
      
      return {
        root,
        errors: this.errors,
        tokens: this.tokens,
        findNodeAtOffset: (offset: number) => this.findNodeAtOffset(root, offset)
      }
    } catch (error) {
      // If parsing fails completely, return partial AST with errors
      return {
        root: {
          type: 'SelectStatement',
          startOffset: 0,
          endOffset: 0,
          children: [],
          selectList: { type: 'SelectList', distinct: false, items: [], startOffset: 0, endOffset: 0, children: [] },
          joinClauses: []
        } as SelectStatement,
        errors: [
          ...this.errors,
          {
            message: error instanceof Error ? error.message : 'Unknown parse error',
            startOffset: this.current < this.tokens.length ? this.tokens[this.current].startOffset : 0,
            endOffset: this.current < this.tokens.length ? this.tokens[this.current].endOffset : 0,
            line: this.current < this.tokens.length ? this.tokens[this.current].line : 1,
            column: this.current < this.tokens.length ? this.tokens[this.current].column : 1,
            recoverable: false
          }
        ],
        tokens: this.tokens,
        findNodeAtOffset: () => null
      }
    }
  }
  
  /**
   * Parse SELECT Statement
   * 
   * Main parsing method for SELECT statements with all clauses.
   */
  private parseSelectStatement(): SelectStatement {
    const startOffset = this.current < this.tokens.length ? this.tokens[this.current].startOffset : 0
    
    // WITH clause (optional)
    let withClause: WithClause | undefined
    if (this.check(SqlTokenType.WITH)) {
      withClause = this.parseWithClause()
    }
    
    // SELECT clause (required)
    this.consume(SqlTokenType.SELECT, "Expected 'SELECT'")
    const selectList = this.parseSelectList()
    
    // FROM clause (optional but common)
    let fromClause: FromClause | undefined
    if (this.check(SqlTokenType.FROM)) {
      fromClause = this.parseFromClause()
    }
    
    // JOIN clauses (multiple allowed)
    const joinClauses: JoinClause[] = []
    while (this.checkJoinKeyword()) {
      joinClauses.push(this.parseJoinClause())
    }
    
    // WHERE clause (optional)
    let whereClause: WhereClause | undefined
    if (this.check(SqlTokenType.WHERE)) {
      whereClause = this.parseWhereClause()
    }
    
    // GROUP BY clause (optional)
    let groupByClause: GroupByClause | undefined
    if (this.check(SqlTokenType.GROUP)) {
      groupByClause = this.parseGroupByClause()
    }
    
    // HAVING clause (optional)
    let havingClause: HavingClause | undefined
    if (this.check(SqlTokenType.HAVING)) {
      havingClause = this.parseHavingClause()
    }
    
    // ORDER BY clause (optional)
    let orderByClause: OrderByClause | undefined
    if (this.check(SqlTokenType.ORDER)) {
      orderByClause = this.parseOrderByClause()
    }
    
    // LIMIT clause (optional)
    let limitClause: LimitClause | undefined
    if (this.check(SqlTokenType.LIMIT)) {
      limitClause = this.parseLimitClause()
    }
    
    // Calculate endOffset from the last valid token or clause
    let endOffset = startOffset
    if (this.current > 0 && this.current <= this.tokens.length) {
      endOffset = this.tokens[this.current - 1].endOffset
    } else if (limitClause) {
      endOffset = limitClause.endOffset
    } else if (orderByClause) {
      endOffset = orderByClause.endOffset
    } else if (havingClause) {
      endOffset = havingClause.endOffset
    } else if (groupByClause) {
      endOffset = groupByClause.endOffset
    } else if (whereClause) {
      endOffset = whereClause.endOffset
    } else if (joinClauses.length > 0) {
      endOffset = joinClauses[joinClauses.length - 1].endOffset
    } else if (fromClause) {
      endOffset = fromClause.endOffset
    } else if (selectList) {
      endOffset = selectList.endOffset
    }
    
    // Build children array for AST traversal
    const children: ASTNode[] = []
    if (withClause) children.push(withClause)
    if (selectList) children.push(selectList)
    if (fromClause) children.push(fromClause)
    if (joinClauses) children.push(...joinClauses)
    if (whereClause) children.push(whereClause)
    if (groupByClause) children.push(groupByClause)
    if (havingClause) children.push(havingClause)
    if (orderByClause) children.push(orderByClause)
    if (limitClause) children.push(limitClause)
    
    return {
      type: 'SelectStatement',
      startOffset,
      endOffset,
      children,
      withClause,
      selectList,
      fromClause,
      joinClauses,
      whereClause,
      groupByClause,
      havingClause,
      orderByClause,
      limitClause
    }
  }
  
  // AST Builder Implementation - Phase 2.1.2
  
  private parseWithClause(): WithClause {
    const startToken = this.advance() // consume WITH
    const startOffset = startToken.startOffset
    
    const recursive = this.check(SqlTokenType.RECURSIVE)
    if (recursive) this.advance()
    
    const cteDefinitions: CTEDefinition[] = []
    
    do {
      cteDefinitions.push(this.parseCTEDefinition())
    } while (this.match(SqlTokenType.COMMA))
    
    const endOffset = this.previous().endOffset
    
    return {
      type: 'WithClause',
      recursive,
      cteDefinitions,
      startOffset,
      endOffset,
      children: cteDefinitions
    }
  }
  
  private parseCTEDefinition(): CTEDefinition {
    const startOffset = this.peek().startOffset
    
    const nameToken = this.consume(SqlTokenType.IDENTIFIER, "Expected CTE name")
    const name = nameToken.value
    
    // Optional column list
    let columns: string[] | undefined
    if (this.match(SqlTokenType.LEFT_PAREN)) {
      columns = []
      do {
        const columnToken = this.consume(SqlTokenType.IDENTIFIER, "Expected column name")
        columns.push(columnToken.value)
      } while (this.match(SqlTokenType.COMMA))
      
      this.consume(SqlTokenType.RIGHT_PAREN, "Expected ')' after column list")
    }
    
    this.consume(SqlTokenType.AS, "Expected 'AS' in CTE definition")
    this.consume(SqlTokenType.LEFT_PAREN, "Expected '(' before CTE query")
    
    const query = this.parseSelectStatement()
    
    this.consume(SqlTokenType.RIGHT_PAREN, "Expected ')' after CTE query")
    
    const endOffset = this.previous().endOffset
    
    return {
      type: 'CTEDefinition',
      name,
      columns,
      query,
      startOffset,
      endOffset,
      children: [query]
    }
  }
  
  private parseSelectList(): SelectList {
    const startOffset = this.peek().startOffset
    
    // Check for DISTINCT
    const distinct = this.match(SqlTokenType.DISTINCT)
    
    const items: SelectItem[] = []
    
    // Handle SELECT *
    if (this.check(SqlTokenType.MULTIPLY)) {
      this.advance()
      items.push({
        type: 'SelectItem',
        expression: {
          type: 'Expression',
          startOffset: this.previous().startOffset,
          endOffset: this.previous().endOffset,
          children: []
        },
        startOffset: this.previous().startOffset,
        endOffset: this.previous().endOffset,
        children: []
      })
    } else {
      // Parse select items
      do {
        items.push(this.parseSelectItem())
      } while (this.match(SqlTokenType.COMMA))
    }
    
    const endOffset = this.previous().endOffset
    
    return {
      type: 'SelectList',
      distinct,
      items,
      startOffset,
      endOffset,
      children: items
    }
  }
  
  private parseSelectItem(): SelectItem {
    const startOffset = this.peek().startOffset
    
    const expression = this.parseExpression()
    
    // Check for alias
    let alias: string | undefined
    if (this.match(SqlTokenType.AS)) {
      const aliasToken = this.consume(SqlTokenType.IDENTIFIER, "Expected alias after 'AS'")
      alias = aliasToken.value
    } else if (this.check(SqlTokenType.IDENTIFIER) && !this.isNextTokenClauseKeyword()) {
      // Implicit alias (identifier without AS)
      const aliasToken = this.advance()
      alias = aliasToken.value
    }
    
    const endOffset = this.previous().endOffset
    
    return {
      type: 'SelectItem',
      expression,
      alias,
      startOffset,
      endOffset,
      children: [expression]
    }
  }
  
  private parseFromClause(): FromClause {
    const startToken = this.advance() // consume FROM
    const startOffset = startToken.startOffset
    
    const table = this.parseTableExpression()
    const endOffset = this.previous().endOffset
    
    return {
      type: 'FromClause',
      table,
      startOffset,
      endOffset,
      children: [table]
    }
  }
  
  private parseTableExpression(): TableExpression {
    const startOffset = this.peek().startOffset
    
    // Parse table name (could be schema.table)
    let name = this.consume(SqlTokenType.IDENTIFIER, "Expected table name").value
    
    // Handle schema.table syntax
    if (this.match(SqlTokenType.DOT)) {
      const tableName = this.consume(SqlTokenType.IDENTIFIER, "Expected table name after '.'").value
      name = `${name}.${tableName}`
    }
    
    // Check for alias
    let alias: string | undefined
    if (this.match(SqlTokenType.AS)) {
      const aliasToken = this.consume(SqlTokenType.IDENTIFIER, "Expected alias after 'AS'")
      alias = aliasToken.value
    } else if (this.check(SqlTokenType.IDENTIFIER) && !this.isNextTokenClauseKeyword()) {
      // Implicit alias (identifier without AS)
      const aliasToken = this.advance()
      alias = aliasToken.value
    }
    
    const endOffset = this.previous().endOffset
    
    return {
      type: 'TableExpression',
      name,
      alias,
      startOffset,
      endOffset,
      children: []
    }
  }
  
  private parseJoinClause(): JoinClause {
    const startOffset = this.peek().startOffset
    
    // Parse join type
    let joinType: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS' = 'INNER'
    
    if (this.match(SqlTokenType.LEFT)) {
      joinType = 'LEFT'
      this.match(SqlTokenType.OUTER) // Optional OUTER
    } else if (this.match(SqlTokenType.RIGHT)) {
      joinType = 'RIGHT'
      this.match(SqlTokenType.OUTER) // Optional OUTER
    } else if (this.match(SqlTokenType.FULL)) {
      joinType = 'FULL'
      this.match(SqlTokenType.OUTER) // Optional OUTER
    } else if (this.match(SqlTokenType.INNER)) {
      joinType = 'INNER'
    }
    
    this.consume(SqlTokenType.JOIN, "Expected 'JOIN'")
    
    const table = this.parseTableExpression()
    
    // Parse join condition
    let condition: Expression | undefined
    if (this.match(SqlTokenType.ON)) {
      condition = this.parseExpression()
    }
    
    const endOffset = this.previous().endOffset
    
    const children: ASTNode[] = [table]
    if (condition) children.push(condition)
    
    return {
      type: 'JoinClause',
      joinType,
      table,
      condition,
      startOffset,
      endOffset,
      children
    }
  }
  
  private parseWhereClause(): WhereClause {
    const startToken = this.advance() // consume WHERE
    const startOffset = startToken.startOffset
    
    const condition = this.parseExpression()
    const endOffset = this.previous().endOffset
    
    return {
      type: 'WhereClause',
      condition,
      startOffset,
      endOffset,
      children: [condition]
    }
  }
  
  private parseExpression(): Expression {
    return this.parseOrExpression()
  }
  
  private parseOrExpression(): Expression {
    let expr = this.parseAndExpression()
    
    while (this.match(SqlTokenType.OR)) {
      const operator = this.previous()
      const right = this.parseAndExpression()
      
      expr = {
        type: 'Expression',
        startOffset: expr.startOffset,
        endOffset: right.endOffset,
        children: [expr, right]
      }
    }
    
    return expr
  }
  
  private parseAndExpression(): Expression {
    let expr = this.parseEqualityExpression()
    
    while (this.match(SqlTokenType.AND)) {
      const operator = this.previous()
      const right = this.parseEqualityExpression()
      
      expr = {
        type: 'Expression',
        startOffset: expr.startOffset,
        endOffset: right.endOffset,
        children: [expr, right]
      }
    }
    
    return expr
  }
  
  private parseEqualityExpression(): Expression {
    let expr = this.parseComparisonExpression()
    
    while (this.matchAny([SqlTokenType.EQUALS, SqlTokenType.NOT_EQUALS])) {
      const operator = this.previous()
      const right = this.parseComparisonExpression()
      
      expr = {
        type: 'Expression',
        startOffset: expr.startOffset,
        endOffset: right.endOffset,
        children: [expr, right]
      }
    }
    
    return expr
  }
  
  private parseComparisonExpression(): Expression {
    let expr = this.parseAdditiveExpression()
    
    while (this.matchAny([
      SqlTokenType.LESS_THAN, 
      SqlTokenType.GREATER_THAN,
      SqlTokenType.LESS_EQUAL,
      SqlTokenType.GREATER_EQUAL,
      SqlTokenType.LIKE,
      SqlTokenType.IN
    ])) {
      const operator = this.previous()
      const right = this.parseAdditiveExpression()
      
      expr = {
        type: 'Expression',
        startOffset: expr.startOffset,
        endOffset: right.endOffset,
        children: [expr, right]
      }
    }
    
    return expr
  }
  
  private parseAdditiveExpression(): Expression {
    let expr = this.parseMultiplicativeExpression()
    
    while (this.matchAny([SqlTokenType.PLUS, SqlTokenType.MINUS])) {
      const operator = this.previous()
      const right = this.parseMultiplicativeExpression()
      
      expr = {
        type: 'Expression',
        startOffset: expr.startOffset,
        endOffset: right.endOffset,
        children: [expr, right]
      }
    }
    
    return expr
  }
  
  private parseMultiplicativeExpression(): Expression {
    let expr = this.parseUnaryExpression()
    
    while (this.matchAny([SqlTokenType.MULTIPLY, SqlTokenType.DIVIDE, SqlTokenType.MODULO])) {
      const operator = this.previous()
      const right = this.parseUnaryExpression()
      
      expr = {
        type: 'Expression',
        startOffset: expr.startOffset,
        endOffset: right.endOffset,
        children: [expr, right]
      }
    }
    
    return expr
  }
  
  private parseUnaryExpression(): Expression {
    if (this.matchAny([SqlTokenType.NOT, SqlTokenType.MINUS])) {
      const operator = this.previous()
      const expr = this.parseUnaryExpression()
      
      return {
        type: 'Expression',
        startOffset: operator.startOffset,
        endOffset: expr.endOffset,
        children: [expr]
      }
    }
    
    return this.parsePrimaryExpression()
  }
  
  private parsePrimaryExpression(): Expression {
    const startOffset = this.peek().startOffset
    
    // Number literal
    if (this.match(SqlTokenType.NUMBER_LITERAL)) {
      return {
        type: 'Expression',
        startOffset,
        endOffset: this.previous().endOffset,
        children: []
      }
    }
    
    // String literal
    if (this.match(SqlTokenType.STRING_LITERAL)) {
      return {
        type: 'Expression',
        startOffset,
        endOffset: this.previous().endOffset,
        children: []
      }
    }
    
    // NULL
    if (this.match(SqlTokenType.NULL)) {
      return {
        type: 'Expression',
        startOffset,
        endOffset: this.previous().endOffset,
        children: []
      }
    }
    
    // Identifier (column reference or function)
    if (this.check(SqlTokenType.IDENTIFIER)) {
      return this.parseIdentifierExpression()
    }
    
    // Parenthesized expression or subquery
    if (this.match(SqlTokenType.LEFT_PAREN)) {
      if (this.check(SqlTokenType.SELECT)) {
        // Subquery
        const subquery = this.parseSelectStatement()
        this.consume(SqlTokenType.RIGHT_PAREN, "Expected ')' after subquery")
        
        return {
          type: 'Expression',
          startOffset,
          endOffset: this.previous().endOffset,
          children: [subquery]
        }
      } else {
        // Parenthesized expression
        const expr = this.parseExpression()
        this.consume(SqlTokenType.RIGHT_PAREN, "Expected ')' after expression")
        
        return {
          type: 'Expression',
          startOffset,
          endOffset: this.previous().endOffset,
          children: [expr]
        }
      }
    }
    
    // Case expression
    if (this.match(SqlTokenType.CASE)) {
      return this.parseCaseExpression()
    }
    
    throw new Error(`Unexpected token in expression: ${this.peek().type}`)
  }
  
  private parseIdentifierExpression(): Expression {
    const startOffset = this.peek().startOffset
    let name = this.advance().value // consume identifier
    
    // Handle qualified names (schema.table.column or table.column)
    while (this.match(SqlTokenType.DOT)) {
      const nextPart = this.consume(SqlTokenType.IDENTIFIER, "Expected identifier after '.'").value
      name += `.${nextPart}`
    }
    
    // Check if this is a function call
    if (this.match(SqlTokenType.LEFT_PAREN)) {
      return this.parseFunctionCall(name, startOffset)
    }
    
    return {
      type: 'Expression',
      startOffset,
      endOffset: this.previous().endOffset,
      children: []
    }
  }
  
  private parseFunctionCall(functionName: string, startOffset: number): Expression {
    const args: Expression[] = []
    
    // Parse function arguments
    if (!this.check(SqlTokenType.RIGHT_PAREN)) {
      do {
        args.push(this.parseExpression())
      } while (this.match(SqlTokenType.COMMA))
    }
    
    this.consume(SqlTokenType.RIGHT_PAREN, "Expected ')' after function arguments")
    
    return {
      type: 'Expression',
      startOffset,
      endOffset: this.previous().endOffset,
      children: args
    }
  }
  
  private parseCaseExpression(): Expression {
    const startOffset = this.previous().startOffset // CASE already consumed
    const children: ASTNode[] = []
    
    // CASE expression branches
    while (this.match(SqlTokenType.WHEN)) {
      const condition = this.parseExpression()
      this.consume(SqlTokenType.THEN, "Expected 'THEN' after WHEN condition")
      const result = this.parseExpression()
      
      children.push(condition, result)
    }
    
    // ELSE clause (optional)
    if (this.match(SqlTokenType.ELSE)) {
      const elseResult = this.parseExpression()
      children.push(elseResult)
    }
    
    this.consume(SqlTokenType.END, "Expected 'END' after CASE expression")
    
    return {
      type: 'Expression',
      startOffset,
      endOffset: this.previous().endOffset,
      children
    }
  }
  
  private parseGroupByClause(): GroupByClause {
    const startToken = this.advance() // consume GROUP
    const startOffset = startToken.startOffset
    
    this.consume(SqlTokenType.BY, "Expected 'BY' after 'GROUP'")
    
    const expressions: Expression[] = []
    
    do {
      expressions.push(this.parseExpression())
    } while (this.match(SqlTokenType.COMMA))
    
    const endOffset = this.previous().endOffset
    
    return {
      type: 'GroupByClause',
      expressions,
      startOffset,
      endOffset,
      children: expressions
    }
  }
  
  private parseHavingClause(): HavingClause {
    const startToken = this.advance() // consume HAVING
    const startOffset = startToken.startOffset
    
    const condition = this.parseExpression()
    const endOffset = this.previous().endOffset
    
    return {
      type: 'HavingClause',
      condition,
      startOffset,
      endOffset,
      children: [condition]
    }
  }
  
  private parseOrderByClause(): OrderByClause {
    const startToken = this.advance() // consume ORDER
    const startOffset = startToken.startOffset
    
    this.consume(SqlTokenType.BY, "Expected 'BY' after 'ORDER'")
    
    const items: OrderByItem[] = []
    
    do {
      items.push(this.parseOrderByItem())
    } while (this.match(SqlTokenType.COMMA))
    
    const endOffset = this.previous().endOffset
    
    return {
      type: 'OrderByClause',
      items,
      startOffset,
      endOffset,
      children: items
    }
  }
  
  private parseOrderByItem(): OrderByItem {
    const startOffset = this.peek().startOffset
    
    const expression = this.parseExpression()
    
    // Check for ASC/DESC
    let direction: 'ASC' | 'DESC' = 'ASC'
    if (this.check(SqlTokenType.IDENTIFIER)) {
      const directionToken = this.peek()
      if (directionToken.value.toUpperCase() === 'ASC') {
        this.advance()
        direction = 'ASC'
      } else if (directionToken.value.toUpperCase() === 'DESC') {
        this.advance()
        direction = 'DESC'
      }
    }
    
    const endOffset = this.previous().endOffset
    
    return {
      type: 'OrderByItem',
      expression,
      direction,
      startOffset,
      endOffset,
      children: [expression]
    }
  }
  
  private parseLimitClause(): LimitClause {
    const startToken = this.advance() // consume LIMIT
    const startOffset = startToken.startOffset
    
    const countToken = this.consume(SqlTokenType.NUMBER_LITERAL, "Expected number after 'LIMIT'")
    const count = parseInt(countToken.value, 10)
    
    // Check for OFFSET
    let offset: number | undefined
    if (this.match(SqlTokenType.OFFSET)) {
      const offsetToken = this.consume(SqlTokenType.NUMBER_LITERAL, "Expected number after 'OFFSET'")
      offset = parseInt(offsetToken.value, 10)
    }
    
    const endOffset = this.previous().endOffset
    
    return {
      type: 'LimitClause',
      count,
      offset,
      startOffset,
      endOffset,
      children: []
    }
  }
  
  /**
   * Parser Helper Methods
   */
  
  private findNodeAtOffset(node: ASTNode, offset: number): ASTNode | null {
    if (offset >= node.startOffset && offset <= node.endOffset) {
      // Check children for more specific match
      for (const child of node.children) {
        const result = this.findNodeAtOffset(child, offset)
        if (result) return result
      }
      return node
    }
    return null
  }
  
  private check(type: SqlTokenType): boolean {
    if (this.isAtEnd()) return false
    return this.peek().type === type
  }
  
  private checkJoinKeyword(): boolean {
    return this.check(SqlTokenType.JOIN) || 
           this.check(SqlTokenType.INNER) ||
           this.check(SqlTokenType.LEFT) ||
           this.check(SqlTokenType.RIGHT) ||
           this.check(SqlTokenType.FULL)
  }
  
  private consume(type: SqlTokenType, message: string): SqlToken {
    if (this.check(type)) return this.advance()
    
    const current = this.peek()
    this.errors.push({
      message,
      startOffset: current.startOffset,
      endOffset: current.endOffset,
      line: current.line,
      column: current.column,
      recoverable: true,
      expectedTokens: [type]
    })
    
    throw new Error(message)
  }
  
  private advance(): SqlToken {
    if (!this.isAtEnd()) this.current++
    return this.previous()
  }
  
  private isAtEnd(): boolean {
    return this.current >= this.tokens.length || this.peek().type === SqlTokenType.EOF
  }
  
  private peek(): SqlToken {
    if (this.current >= this.tokens.length) {
      return {
        type: SqlTokenType.EOF,
        value: '',
        startOffset: 0,
        endOffset: 0,
        line: 1,
        column: 1
      }
    }
    return this.tokens[this.current]
  }
  
  private previous(): SqlToken {
    return this.tokens[this.current - 1]
  }
  
  private match(type: SqlTokenType): boolean {
    if (this.check(type)) {
      this.advance()
      return true
    }
    return false
  }
  
  private matchAny(types: SqlTokenType[]): boolean {
    for (const type of types) {
      if (this.check(type)) {
        this.advance()
        return true
      }
    }
    return false
  }
  
  private isNextTokenClauseKeyword(): boolean {
    const token = this.peek()
    if (token.type !== SqlTokenType.IDENTIFIER) return false
    
    const upperValue = token.value.toUpperCase()
    const clauseKeywords = [
      'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER',
      'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'UNION', 'INTERSECT', 'EXCEPT'
    ]
    
    return clauseKeywords.includes(upperValue)
  }
}

/**
 * Enhanced Context Analysis - Phase 2.1.3
 * 
 * Provides advanced context analysis using AST-based cursor position detection
 * and intelligent query scope analysis for superior completion suggestions.
 */

/**
 * Query Context Types
 * 
 * Defines the different contexts where the cursor can be positioned in a SQL query.
 */
export enum QueryContextType {
  SELECT_LIST = 'SELECT_LIST',           // In SELECT column list
  FROM_CLAUSE = 'FROM_CLAUSE',           // In FROM table reference
  JOIN_CLAUSE = 'JOIN_CLAUSE',           // In JOIN table reference
  JOIN_CONDITION = 'JOIN_CONDITION',     // In JOIN ON condition
  WHERE_CONDITION = 'WHERE_CONDITION',   // In WHERE clause
  GROUP_BY = 'GROUP_BY',                 // In GROUP BY clause
  HAVING_CONDITION = 'HAVING_CONDITION', // In HAVING clause
  ORDER_BY = 'ORDER_BY',                 // In ORDER BY clause
  CTE_DEFINITION = 'CTE_DEFINITION',     // In CTE definition
  SUBQUERY = 'SUBQUERY',                 // Inside a subquery
  FUNCTION_ARGS = 'FUNCTION_ARGS',       // Inside function arguments
  CASE_WHEN = 'CASE_WHEN',               // In CASE WHEN condition
  CASE_THEN = 'CASE_THEN',               // In CASE THEN result
  EXPRESSION = 'EXPRESSION',             // General expression context
  UNKNOWN = 'UNKNOWN'                    // Unable to determine context
}

/**
 * Scope Information
 * 
 * Represents the current scope and available identifiers at cursor position.
 */
export interface QueryScope {
  type: 'ROOT' | 'CTE' | 'SUBQUERY'
  name?: string
  parent?: QueryScope
  
  // Available tables and their aliases in this scope
  tables: Map<string, TableScope>
  
  // Available CTEs from outer scopes
  ctes: Map<string, CTEScope>
  
  // Column aliases from SELECT list (for ORDER BY, HAVING)
  selectAliases: Map<string, string>
}

export interface TableScope {
  name: string           // Original table name (e.g., 'users')
  alias?: string         // Table alias if any (e.g., 'u')
  schema?: string        // Schema name if qualified (e.g., 'public')
  columns?: string[]     // Available columns if known
}

export interface CTEScope {
  name: string          // CTE name
  columns?: string[]    // CTE columns if specified
  definedAt: number     // Offset where CTE was defined
}

/**
 * Query Context Result
 * 
 * Contains comprehensive context information at cursor position.
 */
export interface QueryContext {
  contextType: QueryContextType
  cursorOffset: number
  
  // AST node at cursor position
  currentNode: ASTNode | null
  
  // Current query scope and parent scopes
  scope: QueryScope
  
  // Suggested completion types based on context
  expectedCompletions: CompletionType[]
  
  // Available identifiers for completion
  availableIdentifiers: {
    tables: TableScope[]
    columns: string[]
    functions: string[]
    keywords: string[]
    ctes: CTEScope[]
  }
  
  // Context-specific filters
  contextFilters: {
    requiresAggregate?: boolean    // In HAVING or SELECT with GROUP BY
    allowsWindowFunctions?: boolean // Context supports window functions
    requiresGroupByColumns?: boolean // Must be in GROUP BY for selection
  }
}

export enum CompletionType {
  TABLE = 'TABLE',
  COLUMN = 'COLUMN',
  FUNCTION = 'FUNCTION',
  KEYWORD = 'KEYWORD',
  CTE = 'CTE',
  ALIAS = 'ALIAS',
  OPERATOR = 'OPERATOR',
  LITERAL = 'LITERAL'
}

/**
 * Enhanced Context Analyzer
 * 
 * Provides advanced context analysis using AST parsing and scope resolution.
 */
export class EnhancedContextAnalyzer {
  private tokenizer: SQLTokenizer
  private parser: SQLParser
  
  constructor() {
    this.tokenizer = new SQLTokenizer()
    this.parser = new SQLParser()
  }
  
  /**
   * Analyze Query Context
   * 
   * Main entry point for context analysis. Parses the query and determines
   * the context at the specified cursor position.
   * 
   * @param query - SQL query string
   * @param cursorOffset - Cursor position in the query
   * @returns Comprehensive context information
   */
  analyzeContext(query: string, cursorOffset: number): QueryContext {
    try {
      // Tokenize and parse the query
      const tokens = this.tokenizer.tokenize(query)
      const ast = this.parser.parse(tokens)
      
      // Find the AST node at cursor position
      const currentNode = ast.findNodeAtOffset(cursorOffset)
      
      // Check if AST is valid - if root has no range or too many errors, use fallback
      if (ast.root.startOffset === ast.root.endOffset || ast.errors.length > 0 || !currentNode) {
        return this.createFallbackContext(query, cursorOffset)
      }
      
      // Build scope hierarchy
      const scope = this.buildScopeHierarchy(ast.root, cursorOffset)
      
      // Determine context type
      const contextType = this.determineContextType(currentNode, cursorOffset, ast.root)
      
      // Generate completion suggestions
      const expectedCompletions = this.getExpectedCompletions(contextType, currentNode)
      
      // Collect available identifiers
      const availableIdentifiers = this.collectAvailableIdentifiers(scope, contextType)
      
      // Apply context filters
      const contextFilters = this.getContextFilters(contextType, scope, currentNode)
      
      return {
        contextType,
        cursorOffset,
        currentNode,
        scope,
        expectedCompletions,
        availableIdentifiers,
        contextFilters
      }
    } catch (error) {
      // Fallback for parse errors
      return this.createFallbackContext(query, cursorOffset)
    }
  }
  
  /**
   * Build Scope Hierarchy
   * 
   * Constructs the nested scope structure for the query, tracking available
   * tables, CTEs, and aliases at each scope level.
   */
  private buildScopeHierarchy(node: ASTNode, cursorOffset: number): QueryScope {
    const rootScope: QueryScope = {
      type: 'ROOT',
      tables: new Map(),
      ctes: new Map(),
      selectAliases: new Map()
    }
    
    this.analyzeScopeRecursive(node, rootScope, cursorOffset)
    return rootScope
  }
  
  private analyzeScopeRecursive(node: ASTNode, currentScope: QueryScope, cursorOffset: number): void {
    if (node.type === 'SelectStatement') {
      const selectStmt = node as SelectStatement
      
      // Process WITH clause for CTEs
      if (selectStmt.withClause) {
        this.processCTEDefinitions(selectStmt.withClause, currentScope)
      }
      
      // Process FROM clause for table references
      if (selectStmt.fromClause) {
        this.processTableReference(selectStmt.fromClause.table, currentScope)
      }
      
      // Process JOIN clauses for additional table references
      selectStmt.joinClauses.forEach(joinClause => {
        this.processTableReference(joinClause.table, currentScope)
      })
      
      // Process SELECT list for column aliases
      this.processSelectList(selectStmt.selectList, currentScope)
    }
    
    // Recursively process child nodes
    node.children.forEach(child => {
      this.analyzeScopeRecursive(child, currentScope, cursorOffset)
    })
  }
  
  private processCTEDefinitions(withClause: WithClause, scope: QueryScope): void {
    withClause.cteDefinitions.forEach(cte => {
      const cteScope: CTEScope = {
        name: cte.name,
        columns: cte.columns,
        definedAt: cte.startOffset
      }
      scope.ctes.set(cte.name, cteScope)
    })
  }
  
  private processTableReference(table: TableExpression, scope: QueryScope): void {
    const tableScope: TableScope = {
      name: table.name,
      alias: table.alias
    }
    
    // Handle schema.table syntax
    if (table.name.includes('.')) {
      const [schema, tableName] = table.name.split('.')
      tableScope.schema = schema
      tableScope.name = tableName
    }
    
    // Add table to scope using alias if available, otherwise use table name
    const key = table.alias || table.name
    scope.tables.set(key, tableScope)
  }
  
  private processSelectList(selectList: SelectList, scope: QueryScope): void {
    selectList.items.forEach(item => {
      if (item.alias) {
        // Map alias to the original expression (simplified)
        scope.selectAliases.set(item.alias, 'expression')
      }
    })
  }
  
  /**
   * Determine Context Type
   * 
   * Analyzes the AST node and cursor position to determine the specific
   * context where completions should be provided.
   */
  private determineContextType(node: ASTNode | null, cursorOffset: number, root: SelectStatement): QueryContextType {
    if (!node) return QueryContextType.UNKNOWN
    
    // Check if we're in a specific clause based on node type
    switch (node.type) {
      case 'SelectList':
      case 'SelectItem':
        return QueryContextType.SELECT_LIST
        
      case 'FromClause':
      case 'TableExpression':
        return QueryContextType.FROM_CLAUSE
        
      case 'JoinClause':
        // Determine if we're in the table reference or condition
        const joinClause = node as JoinClause
        if (joinClause.condition && cursorOffset >= joinClause.condition.startOffset) {
          return QueryContextType.JOIN_CONDITION
        }
        return QueryContextType.JOIN_CLAUSE
        
      case 'WhereClause':
        return QueryContextType.WHERE_CONDITION
        
      case 'GroupByClause':
        return QueryContextType.GROUP_BY
        
      case 'HavingClause':
        return QueryContextType.HAVING_CONDITION
        
      case 'OrderByClause':
      case 'OrderByItem':
        return QueryContextType.ORDER_BY
        
      case 'CTEDefinition':
        return QueryContextType.CTE_DEFINITION
        
      case 'Expression':
        // Determine expression context based on parent
        if (node.parent) {
          return this.determineExpressionContext(node.parent)
        }
        return QueryContextType.EXPRESSION
        
      default:
        return QueryContextType.UNKNOWN
    }
  }
  
  private determineExpressionContext(parentNode: ASTNode): QueryContextType {
    switch (parentNode.type) {
      case 'SelectItem':
        return QueryContextType.SELECT_LIST
      case 'WhereClause':
        return QueryContextType.WHERE_CONDITION
      case 'HavingClause':
        return QueryContextType.HAVING_CONDITION
      case 'JoinClause':
        return QueryContextType.JOIN_CONDITION
      default:
        return QueryContextType.EXPRESSION
    }
  }
  
  /**
   * Get Expected Completions
   * 
   * Based on the context type and current node, determine what types of
   * completions should be offered.
   */
  private getExpectedCompletions(contextType: QueryContextType, node: ASTNode | null): CompletionType[] {
    switch (contextType) {
      case QueryContextType.SELECT_LIST:
        return [CompletionType.COLUMN, CompletionType.FUNCTION, CompletionType.LITERAL, CompletionType.KEYWORD]
        
      case QueryContextType.FROM_CLAUSE:
      case QueryContextType.JOIN_CLAUSE:
        return [CompletionType.TABLE, CompletionType.CTE]
        
      case QueryContextType.WHERE_CONDITION:
      case QueryContextType.HAVING_CONDITION:
      case QueryContextType.JOIN_CONDITION:
        return [CompletionType.COLUMN, CompletionType.FUNCTION, CompletionType.OPERATOR, CompletionType.LITERAL]
        
      case QueryContextType.GROUP_BY:
      case QueryContextType.ORDER_BY:
        return [CompletionType.COLUMN, CompletionType.ALIAS, CompletionType.FUNCTION]
        
      case QueryContextType.FUNCTION_ARGS:
        return [CompletionType.COLUMN, CompletionType.LITERAL, CompletionType.FUNCTION]
        
      default:
        return [CompletionType.COLUMN, CompletionType.FUNCTION, CompletionType.KEYWORD, CompletionType.OPERATOR]
    }
  }
  
  /**
   * Collect Available Identifiers
   * 
   * Gathers all identifiers available in the current scope for completion.
   */
  private collectAvailableIdentifiers(scope: QueryScope, contextType: QueryContextType): QueryContext['availableIdentifiers'] {
    const tables: TableScope[] = []
    const columns: string[] = []
    const ctes: CTEScope[] = []
    
    // Collect tables from current scope
    scope.tables.forEach(table => {
      tables.push(table)
      
      // Add qualified column references
      const tablePrefix = table.alias || table.name
      if (table.columns) {
        table.columns.forEach(column => {
          columns.push(`${tablePrefix}.${column}`)
          columns.push(column) // Also add unqualified
        })
      }
    })
    
    // Collect CTEs
    scope.ctes.forEach(cte => {
      ctes.push(cte)
    })
    
    // Add standard SQL functions
    const functions = [
      'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COALESCE', 'CASE',
      'SUBSTR', 'LENGTH', 'UPPER', 'LOWER', 'TRIM',
      // Nozzle-specific UDFs
      'evm_decode_log', 'evm_topic', 'eth_call', 'attestation_hash'
    ]
    
    // Context-appropriate keywords
    const keywords = this.getContextKeywords(contextType)
    
    return {
      tables,
      columns,
      functions,
      keywords,
      ctes
    }
  }
  
  private getContextKeywords(contextType: QueryContextType): string[] {
    const baseKeywords = ['AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS', 'NULL']
    
    switch (contextType) {
      case QueryContextType.SELECT_LIST:
        return [...baseKeywords, 'DISTINCT', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END']
        
      case QueryContextType.FROM_CLAUSE:
        return ['AS', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'FULL', 'OUTER']
        
      case QueryContextType.WHERE_CONDITION:
      case QueryContextType.HAVING_CONDITION:
        return [...baseKeywords, 'CASE', 'WHEN', 'THEN', 'ELSE', 'END']
        
      case QueryContextType.ORDER_BY:
        return ['ASC', 'DESC', 'NULLS', 'FIRST', 'LAST']
        
      default:
        return baseKeywords
    }
  }
  
  /**
   * Get Context Filters
   * 
   * Determines context-specific filtering rules for completions.
   */
  private getContextFilters(contextType: QueryContextType, scope: QueryScope, node: ASTNode | null): QueryContext['contextFilters'] {
    const filters: QueryContext['contextFilters'] = {}
    
    // Determine if we're in an aggregate context
    if (contextType === QueryContextType.HAVING_CONDITION) {
      filters.requiresAggregate = true
    }
    
    // Check if GROUP BY is present (affects SELECT clause rules)
    if (contextType === QueryContextType.SELECT_LIST) {
      // This would require analyzing the full query structure
      filters.requiresGroupByColumns = false // Simplified for now
    }
    
    // Window functions are allowed in SELECT and ORDER BY
    if (contextType === QueryContextType.SELECT_LIST || contextType === QueryContextType.ORDER_BY) {
      filters.allowsWindowFunctions = true
    }
    
    return filters
  }
  
  /**
   * Create Fallback Context
   * 
   * Provides a basic context when parsing fails or context cannot be determined.
   */
  private createFallbackContext(query: string, cursorOffset: number): QueryContext {
    // Use simple heuristics to determine context when AST parsing fails
    const contextType = this.detectContextHeuristically(query, cursorOffset)
    
    const expectedCompletions: CompletionType[] = []
    switch (contextType) {
      case QueryContextType.SELECT_LIST:
        expectedCompletions.push(CompletionType.COLUMN, CompletionType.FUNCTION, CompletionType.KEYWORD)
        break
      case QueryContextType.FROM_CLAUSE:
        expectedCompletions.push(CompletionType.TABLE, CompletionType.CTE)
        break
      case QueryContextType.WHERE_CONDITION:
        expectedCompletions.push(CompletionType.COLUMN, CompletionType.OPERATOR, CompletionType.FUNCTION)
        break
      default:
        expectedCompletions.push(CompletionType.KEYWORD, CompletionType.FUNCTION)
    }
    
    return {
      contextType,
      cursorOffset,
      currentNode: null,
      scope: {
        type: 'ROOT',
        tables: new Map(),
        ctes: new Map(),
        selectAliases: new Map()
      },
      expectedCompletions,
      availableIdentifiers: {
        tables: [],
        columns: [],
        functions: ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'],
        keywords: ['SELECT', 'FROM', 'WHERE', 'GROUP', 'ORDER', 'LIMIT'],
        ctes: []
      },
      contextFilters: {}
    }
  }
  
  private detectContextHeuristically(query: string, cursorOffset: number): QueryContextType {
    const queryUpToCursor = query.substring(0, cursorOffset).trim()
    
    // Simple pattern matching for context detection
    if (/\bSELECT\s*$/i.test(queryUpToCursor)) {
      return QueryContextType.SELECT_LIST
    }
    
    if (/\bFROM\s*$/i.test(queryUpToCursor)) {
      return QueryContextType.FROM_CLAUSE
    }
    
    if (/\bWHERE\s*$/i.test(queryUpToCursor)) {
      return QueryContextType.WHERE_CONDITION
    }
    
    if (/\bSELECT\s+\w+.*\bFROM\s*$/i.test(queryUpToCursor)) {
      return QueryContextType.FROM_CLAUSE
    }
    
    if (/\bSELECT\s+[\w\s,]*\bFROM\s+\w+.*\bWHERE\s*$/i.test(queryUpToCursor)) {
      return QueryContextType.WHERE_CONDITION
    }
    
    // If query starts with SELECT and we haven't matched FROM or WHERE patterns, assume SELECT_LIST
    if (/\bSELECT\b/i.test(queryUpToCursor)) {
      return QueryContextType.SELECT_LIST
    }
    
    return QueryContextType.UNKNOWN
  }
}

/**
 * Completion Scoring System - Phase 2.2.1
 * 
 * Provides intelligent ranking and scoring of completion suggestions based on
 * context relevance, usage patterns, semantic similarity, and user preferences.
 */

/**
 * Completion Item with Scoring
 * 
 * Represents a completion suggestion with comprehensive scoring information.
 */
export interface ScoredCompletion {
  // Basic completion info
  label: string
  type: CompletionType
  insertText: string
  detail?: string
  documentation?: string
  
  // Scoring information
  score: CompletionScore
  
  // Monaco completion item properties
  kind: number
  sortText: string
  filterText?: string
  additionalTextEdits?: any[]
}

export interface CompletionScore {
  total: number           // Final weighted score (0-1000)
  
  // Individual scoring components
  contextRelevance: number    // How well it fits the current context (0-100)
  semanticSimilarity: number  // Text similarity to user input (0-100) 
  usageFrequency: number      // How often it's used (0-100)
  recency: number            // How recently it was used (0-100)
  specificity: number        // How specific/precise the match is (0-100)
  
  // Weighting factors applied
  weights: ScoreWeights
  
  // Debug information
  debugInfo?: {
    contextMatch?: string
    similarityReason?: string
    frequencyData?: number
    lastUsed?: Date
  }
}

export interface ScoreWeights {
  contextRelevance: number    // Weight for context matching (default: 0.4)
  semanticSimilarity: number  // Weight for text similarity (default: 0.3)
  usageFrequency: number      // Weight for usage patterns (default: 0.15)
  recency: number            // Weight for recent usage (default: 0.1)
  specificity: number        // Weight for match specificity (default: 0.05)
}

export interface UsageStatistics {
  itemName: string
  contextType: QueryContextType
  usageCount: number
  lastUsed: Date
  averageSessionUsage: number
}

/**
 * Completion Scoring Configuration
 */
export interface ScoringConfig {
  weights: ScoreWeights
  enableUsageTracking: boolean
  enableSemanticSimilarity: boolean
  maxSuggestions: number
  minScore: number
  debugMode: boolean
}

/**
 * Advanced Completion Scorer
 * 
 * Implements intelligent scoring and ranking of completion suggestions.
 */
export class CompletionScorer {
  private config: ScoringConfig
  private usageStats: Map<string, UsageStatistics[]> = new Map()
  
  constructor(config?: Partial<ScoringConfig>) {
    this.config = {
      weights: {
        contextRelevance: 0.4,
        semanticSimilarity: 0.3,
        usageFrequency: 0.15,
        recency: 0.1,
        specificity: 0.05
      },
      enableUsageTracking: true,
      enableSemanticSimilarity: true,
      maxSuggestions: 50,
      minScore: 10,
      debugMode: false,
      ...config
    }
  }
  
  /**
   * Score Completions
   * 
   * Main entry point for scoring completion suggestions based on context
   * and user input patterns.
   * 
   * @param suggestions - Raw completion suggestions
   * @param context - Current query context
   * @param userInput - Current user input for similarity matching
   * @returns Scored and ranked completions
   */
  scoreCompletions(
    suggestions: Array<{ label: string, type: CompletionType, detail?: string }>,
    context: QueryContext,
    userInput: string = ''
  ): ScoredCompletion[] {
    const scoredItems: ScoredCompletion[] = suggestions.map(suggestion => 
      this.scoreIndividualCompletion(suggestion, context, userInput)
    )
    
    // Sort by total score (descending)
    scoredItems.sort((a, b) => b.score.total - a.score.total)
    
    // Apply limits and filters
    const filteredItems = scoredItems
      .filter(item => item.score.total >= this.config.minScore)
      .slice(0, this.config.maxSuggestions)
    
    // Generate sort text for Monaco
    this.generateSortText(filteredItems)
    
    return filteredItems
  }
  
  /**
   * Score Individual Completion
   * 
   * Calculates comprehensive score for a single completion suggestion.
   */
  private scoreIndividualCompletion(
    suggestion: { label: string, type: CompletionType, detail?: string },
    context: QueryContext,
    userInput: string
  ): ScoredCompletion {
    const weights = this.config.weights
    
    // Calculate individual score components
    const contextRelevance = this.calculateContextRelevance(suggestion, context)
    const semanticSimilarity = this.calculateSemanticSimilarity(suggestion.label, userInput)
    const usageFrequency = this.calculateUsageFrequency(suggestion.label, context.contextType)
    const recency = this.calculateRecencyScore(suggestion.label, context.contextType)
    const specificity = this.calculateSpecificity(suggestion.label, userInput)
    
    // Calculate weighted total score
    const total = Math.round(
      contextRelevance * weights.contextRelevance +
      semanticSimilarity * weights.semanticSimilarity +
      usageFrequency * weights.usageFrequency +
      recency * weights.recency +
      specificity * weights.specificity
    ) * 10 // Scale to 0-1000
    
    const score: CompletionScore = {
      total,
      contextRelevance,
      semanticSimilarity,
      usageFrequency,
      recency,
      specificity,
      weights,
      ...(this.config.debugMode && {
        debugInfo: {
          contextMatch: context.contextType,
          similarityReason: this.getSemanticSimilarityReason(suggestion.label, userInput),
          frequencyData: this.getFrequencyData(suggestion.label, context.contextType),
          lastUsed: this.getLastUsed(suggestion.label, context.contextType)
        }
      })
    }
    
    return {
      label: suggestion.label,
      type: suggestion.type,
      insertText: suggestion.label,
      detail: suggestion.detail,
      score,
      kind: this.getMonacoKind(suggestion.type),
      sortText: '' // Will be set by generateSortText
    }
  }
  
  /**
   * Calculate Context Relevance Score
   * 
   * Determines how well a completion fits the current query context.
   */
  private calculateContextRelevance(
    suggestion: { label: string, type: CompletionType },
    context: QueryContext
  ): number {
    const expectedTypes = context.expectedCompletions
    const baseScore = expectedTypes.includes(suggestion.type) ? 80 : 20
    
    // Apply context-specific bonuses
    let bonus = 0
    
    switch (context.contextType) {
      case QueryContextType.SELECT_LIST:
        if (suggestion.type === CompletionType.COLUMN) {
          bonus += this.isColumnInScope(suggestion.label, context) ? 20 : 5
        } else if (suggestion.type === CompletionType.FUNCTION) {
          bonus += this.isAggregate(suggestion.label) && context.contextFilters.requiresAggregate ? 15 : 0
        }
        break
        
      case QueryContextType.FROM_CLAUSE:
        if (suggestion.type === CompletionType.TABLE) {
          bonus += 15
        } else if (suggestion.type === CompletionType.CTE) {
          bonus += this.isCTEInScope(suggestion.label, context) ? 20 : 0
        }
        break
        
      case QueryContextType.WHERE_CONDITION:
        if (suggestion.type === CompletionType.COLUMN) {
          bonus += this.isColumnInScope(suggestion.label, context) ? 15 : 0
        } else if (suggestion.type === CompletionType.OPERATOR) {
          bonus += 10
        }
        break
        
      case QueryContextType.JOIN_CONDITION:
        if (suggestion.type === CompletionType.COLUMN && this.isPotentialJoinColumn(suggestion.label, context)) {
          bonus += 25
        }
        break
        
      case QueryContextType.ORDER_BY:
        if (suggestion.type === CompletionType.ALIAS && context.scope.selectAliases.has(suggestion.label)) {
          bonus += 20
        }
        break
    }
    
    return Math.min(100, baseScore + bonus)
  }
  
  /**
   * Calculate Semantic Similarity Score
   * 
   * Measures text similarity between user input and suggestion.
   */
  private calculateSemanticSimilarity(suggestion: string, userInput: string): number {
    if (!userInput || !this.config.enableSemanticSimilarity) return 50
    
    const input = userInput.toLowerCase()
    const label = suggestion.toLowerCase()
    
    // Exact prefix match
    if (label.startsWith(input)) {
      return 100
    }
    
    // Subsequence match (characters in order)
    if (this.isSubsequence(input, label)) {
      return 85
    }
    
    // Contains all characters
    if (this.containsAllCharacters(input, label)) {
      return 70
    }
    
    // Fuzzy match with edit distance
    const editDistance = this.calculateEditDistance(input, label)
    const maxLength = Math.max(input.length, label.length)
    const similarity = maxLength === 0 ? 0 : (1 - editDistance / maxLength) * 100
    
    return Math.max(0, similarity)
  }
  
  /**
   * Calculate Usage Frequency Score
   * 
   * Scores based on how often this completion has been used.
   */
  private calculateUsageFrequency(label: string, contextType: QueryContextType): number {
    if (!this.config.enableUsageTracking) return 50
    
    const contextStats = this.usageStats.get(contextType) || []
    const itemStats = contextStats.find(stat => stat.itemName === label)
    
    if (!itemStats) return 25
    
    // Normalize usage count (log scale to prevent dominance)
    const normalizedCount = Math.log(itemStats.usageCount + 1) * 20
    return Math.min(100, normalizedCount)
  }
  
  /**
   * Calculate Recency Score
   * 
   * Scores based on how recently this completion was used.
   */
  private calculateRecencyScore(label: string, contextType: QueryContextType): number {
    if (!this.config.enableUsageTracking) return 50
    
    const contextStats = this.usageStats.get(contextType) || []
    const itemStats = contextStats.find(stat => stat.itemName === label)
    
    if (!itemStats) return 25
    
    const now = Date.now()
    const lastUsed = itemStats.lastUsed.getTime()
    const hoursSinceUsed = (now - lastUsed) / (1000 * 60 * 60)
    
    // Exponential decay: 100% if used in last hour, 50% after 24 hours
    const recencyScore = Math.max(0, 100 * Math.exp(-hoursSinceUsed / 12))
    return recencyScore
  }
  
  /**
   * Calculate Specificity Score
   * 
   * Rewards more specific/exact matches over generic ones.
   */
  private calculateSpecificity(suggestion: string, userInput: string): number {
    if (!userInput) return 50
    
    const input = userInput.toLowerCase()
    const label = suggestion.toLowerCase()
    
    // Exact match
    if (input === label) return 100
    
    // Exact prefix with short suffix
    if (label.startsWith(input)) {
      const remainingLength = label.length - input.length
      return Math.max(50, 100 - remainingLength * 5)
    }
    
    // Partial match specificity
    const matchLength = this.getLongestCommonSubstring(input, label)
    const specificity = (matchLength / Math.max(input.length, label.length)) * 100
    
    return specificity
  }
  
  /**
   * Record Usage Statistics
   * 
   * Updates usage tracking when a completion is selected.
   */
  recordUsage(label: string, contextType: QueryContextType): void {
    if (!this.config.enableUsageTracking) return
    
    const contextStats = this.usageStats.get(contextType) || []
    let itemStats = contextStats.find(stat => stat.itemName === label)
    
    if (!itemStats) {
      itemStats = {
        itemName: label,
        contextType,
        usageCount: 0,
        lastUsed: new Date(),
        averageSessionUsage: 0
      }
      contextStats.push(itemStats)
      this.usageStats.set(contextType, contextStats)
    }
    
    itemStats.usageCount++
    itemStats.lastUsed = new Date()
    
    // Update average session usage (simple moving average)
    itemStats.averageSessionUsage = (itemStats.averageSessionUsage * 0.9) + (1 * 0.1)
  }
  
  /**
   * Helper Methods
   */
  
  private generateSortText(completions: ScoredCompletion[]): void {
    completions.forEach((item, index) => {
      // Generate zero-padded sort text based on rank
      const rank = index.toString().padStart(3, '0')
      item.sortText = rank
    })
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
  
  private isColumnInScope(columnName: string, context: QueryContext): boolean {
    return context.availableIdentifiers.columns.includes(columnName)
  }
  
  private isCTEInScope(cteName: string, context: QueryContext): boolean {
    return context.availableIdentifiers.ctes.some(cte => cte.name === cteName)
  }
  
  private isPotentialJoinColumn(columnName: string, context: QueryContext): boolean {
    // Check if column appears in multiple tables (common join pattern)
    const availableColumns = context.availableIdentifiers.columns
    const unqualified = columnName.split('.').pop() || columnName
    
    return availableColumns.filter(col => 
      col.endsWith(`.${unqualified}`) || col === unqualified
    ).length > 1
  }
  
  private isAggregate(functionName: string): boolean {
    const aggregates = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT']
    return aggregates.includes(functionName.toUpperCase())
  }
  
  private getFrequencyData(label: string, contextType: QueryContextType): number {
    const contextStats = this.usageStats.get(contextType) || []
    const itemStats = contextStats.find(stat => stat.itemName === label)
    return itemStats?.usageCount || 0
  }
  
  private getLastUsed(label: string, contextType: QueryContextType): Date | undefined {
    const contextStats = this.usageStats.get(contextType) || []
    const itemStats = contextStats.find(stat => stat.itemName === label)
    return itemStats?.lastUsed
  }
  
  private getSemanticSimilarityReason(suggestion: string, userInput: string): string {
    if (!userInput) return 'No input'
    
    const input = userInput.toLowerCase()
    const label = suggestion.toLowerCase()
    
    if (label.startsWith(input)) return 'Prefix match'
    if (this.isSubsequence(input, label)) return 'Subsequence match'
    if (this.containsAllCharacters(input, label)) return 'Character match'
    return 'Fuzzy match'
  }
  
  // String matching utility methods
  
  private isSubsequence(shorter: string, longer: string): boolean {
    let i = 0
    for (const char of longer) {
      if (i < shorter.length && char === shorter[i]) {
        i++
      }
    }
    return i === shorter.length
  }
  
  private containsAllCharacters(input: string, target: string): boolean {
    const inputChars = new Set(input.split(''))
    const targetChars = new Set(target.split(''))
    
    for (const char of Array.from(inputChars)) {
      if (!targetChars.has(char)) return false
    }
    return true
  }
  
  private calculateEditDistance(s1: string, s2: string): number {
    const matrix = Array(s1.length + 1).fill(null).map(() => Array(s2.length + 1).fill(null))
    
    for (let i = 0; i <= s1.length; i++) matrix[i][0] = i
    for (let j = 0; j <= s2.length; j++) matrix[0][j] = j
    
    for (let i = 1; i <= s1.length; i++) {
      for (let j = 1; j <= s2.length; j++) {
        if (s1[i-1] === s2[j-1]) {
          matrix[i][j] = matrix[i-1][j-1]
        } else {
          matrix[i][j] = Math.min(
            matrix[i-1][j] + 1,    // deletion
            matrix[i][j-1] + 1,    // insertion  
            matrix[i-1][j-1] + 1   // substitution
          )
        }
      }
    }
    
    return matrix[s1.length][s2.length]
  }
  
  private getLongestCommonSubstring(s1: string, s2: string): number {
    const matrix = Array(s1.length + 1).fill(null).map(() => Array(s2.length + 1).fill(0))
    let maxLength = 0
    
    for (let i = 1; i <= s1.length; i++) {
      for (let j = 1; j <= s2.length; j++) {
        if (s1[i-1] === s2[j-1]) {
          matrix[i][j] = matrix[i-1][j-1] + 1
          maxLength = Math.max(maxLength, matrix[i][j])
        }
      }
    }
    
    return maxLength
  }
}