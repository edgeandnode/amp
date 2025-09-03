/**
 * Nozzle SQL Completions - Core completion logic for SQL editor
 *
 * This service provides intelligent SQL completions by analyzing query context
 * and suggesting relevant tables, columns, functions, and keywords based on
 * cursor position and surrounding SQL structure.
 *
 * Features:
 * - Context-aware suggestions (FROM vs SELECT vs WHERE clauses)
 * - Table completions with metadata information
 * - Column completions filtered by available tables
 * - UDF function completions with snippets
 * - Basic SQL keyword completions
 */

import type * as monaco from "monaco-editor"
import type { DatasetMetadata, UserDefinedFunction } from "./nozzleSQLProviders"
import { createUDFSuggestions } from "./udfSnippets"

/**
 * Represents the current context where the cursor is positioned in a SQL query
 */
interface QueryContext {
  /** True if cursor expects a table name (after FROM, JOIN, etc.) */
  expectsTable: boolean
  /** True if cursor expects a column name (in SELECT, WHERE, etc.) */
  expectsColumn: boolean
  /** True if cursor expects a function name */
  expectsFunction: boolean
  /** List of table names that are available in current scope */
  availableTables: Array<string>
  /** The current word being typed */
  currentWord: string
  /** Current SQL clause context */
  currentClause:
    | "SELECT"
    | "FROM"
    | "WHERE"
    | "JOIN"
    | "GROUP BY"
    | "ORDER BY"
    | "HAVING"
    | null
}

/**
 * Main completion provider function that analyzes context and returns appropriate suggestions
 */
export function provideNozzleCompletions(
  monaco: typeof import("monaco-editor"),
  model: monaco.editor.ITextModel,
  position: monaco.Position,
  metadata: Array<DatasetMetadata>,
  udfs: Array<UserDefinedFunction>,
): monaco.languages.CompletionList {
  try {
    // Analyze the current query context
    const context = analyzeQueryContext(model, position)
    const suggestions: Array<monaco.languages.CompletionItem> = []

    // 1. Table suggestions when context suggests FROM, JOIN, etc.
    if (context.expectsTable) {
      suggestions.push(...createTableSuggestions(monaco, metadata))
    }

    // 2. Column suggestions based on identified tables in query
    if (context.expectsColumn) {
      suggestions.push(
        ...createColumnSuggestions(monaco, metadata, context.availableTables),
      )
    }

    // 3. UDF function suggestions
    if (context.expectsFunction || context.currentClause === "SELECT") {
      suggestions.push(...createUDFSuggestions(monaco, udfs))
    }

    // 4. SQL keyword suggestions (always available but lower priority)
    suggestions.push(...createKeywordSuggestions(monaco, context))

    // Sort suggestions by priority (sortText)
    const sortedSuggestions = suggestions.sort((a, b) =>
      (a.sortText || a.label.toString()).localeCompare(
        b.sortText || b.label.toString(),
      ),
    )

    console.log("🔍 SQL Completions:", {
      context: context.currentClause,
      expectsTable: context.expectsTable,
      expectsColumn: context.expectsColumn,
      availableTables: context.availableTables,
      suggestionsCount: sortedSuggestions.length,
    })

    return {
      suggestions: sortedSuggestions,
    }
  } catch (error) {
    console.error("❌ Error in provideNozzleCompletions:", error)
    return { suggestions: [] }
  }
}

/**
 * Analyzes the SQL query to determine what type of completions are expected
 * at the current cursor position. Uses regex patterns to identify SQL clauses
 * and context.
 */
function analyzeQueryContext(
  model: monaco.editor.ITextModel,
  position: monaco.Position,
): QueryContext {
  // Get text content around cursor position
  const lineContent = model.getLineContent(position.lineNumber)
  const beforeCursor = lineContent.substring(0, position.column - 1)
  const fullQuery = model.getValue().toLowerCase()
  const beforeCursorLower = beforeCursor.toLowerCase()

  // Extract current word being typed
  const wordMatch = beforeCursor.match(/(\w+)$/)
  const currentWord = wordMatch ? wordMatch[1] : ""

  // Determine current SQL clause context
  const currentClause = getCurrentClause(beforeCursorLower)

  // Context analysis using regex patterns
  const context: QueryContext = {
    // Expects table after FROM, JOIN keywords
    expectsTable:
      /\b(from|join|inner\s+join|left\s+join|right\s+join|full\s+join|cross\s+join)\s*$/i.test(
        beforeCursorLower,
      ),

    // Expects column in SELECT, WHERE, GROUP BY, ORDER BY
    expectsColumn:
      /\b(select|where|group\s+by|order\s+by|having)\s*$/i.test(
        beforeCursorLower,
      ) ||
      /\b(select\s+(?:\w+\s*,\s*)*)\s*$/i.test(beforeCursorLower) ||
      /\b(and|or)\s*$/i.test(beforeCursorLower),

    // Expects function in SELECT or after opening parenthesis
    expectsFunction:
      /\b(select)\s*$/i.test(beforeCursorLower) || /\(\s*$/i.test(beforeCursor),

    // Extract available tables from the query
    availableTables: extractTablesFromQuery(fullQuery),

    currentWord,
    currentClause,
  }

  return context
}

/**
 * Determines the current SQL clause based on the text before cursor
 */
function getCurrentClause(beforeCursor: string): QueryContext["currentClause"] {
  if (
    /\bselect\b(?!.*\b(?:from|where|group\s+by|order\s+by|having)\b)/.test(
      beforeCursor,
    )
  )
    return "SELECT"
  if (
    /\bfrom\b(?!.*\b(?:where|group\s+by|order\s+by|having)\b)/.test(
      beforeCursor,
    )
  )
    return "FROM"
  if (/\bwhere\b(?!.*\b(?:group\s+by|order\s+by|having)\b)/.test(beforeCursor))
    return "WHERE"
  if (
    /\b(?:inner\s+join|left\s+join|right\s+join|full\s+join|cross\s+join|join)\b(?!.*\b(?:where|group\s+by|order\s+by|having)\b)/.test(
      beforeCursor,
    )
  )
    return "JOIN"
  if (/\bgroup\s+by\b(?!.*\b(?:order\s+by|having)\b)/.test(beforeCursor))
    return "GROUP BY"
  if (/\bhaving\b(?!.*\border\s+by\b)/.test(beforeCursor)) return "HAVING"
  if (/\border\s+by\b/.test(beforeCursor)) return "ORDER BY"

  return null
}

/**
 * Extracts table names from the full query text to understand available tables
 * Looks for patterns like "FROM table_name" and "JOIN table_name"
 */
function extractTablesFromQuery(query: string): Array<string> {
  const tables: Array<string> = []

  // Match FROM and JOIN patterns
  const fromPattern =
    /\b(?:from|join|inner\s+join|left\s+join|right\s+join|full\s+join|cross\s+join)\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)/gi

  let match
  while ((match = fromPattern.exec(query)) !== null) {
    tables.push(match[1])
  }

  return [...new Set(tables)] // Remove duplicates
}

/**
 * Creates table suggestions from metadata with rich documentation
 */
function createTableSuggestions(
  monaco: typeof import("monaco-editor"),
  metadata: Array<DatasetMetadata>,
): Array<monaco.languages.CompletionItem> {
  return metadata.map((dataset, index) => ({
    label: dataset.source,
    kind: monaco.languages.CompletionItemKind.Class,
    detail: `Table (${dataset.metadata_columns.length} columns)`,
    documentation: {
      value: `**Dataset Table: ${dataset.source}**\n\nAvailable columns:\n${dataset.metadata_columns
        .map(
          (col) =>
            `- \`${col.name}\` (${col.dataType}): ${col.description || "No description available"}`,
        )
        .join("\n")}`,
      isTrusted: true,
    },
    insertText: dataset.source,
    // Priority: Tables get highest priority (1-xxx)
    sortText: `1-${index.toString().padStart(3, "0")}`,
  }))
}

/**
 * Creates column suggestions filtered by available tables in the current query context
 */
function createColumnSuggestions(
  monaco: typeof import("monaco-editor"),
  metadata: Array<DatasetMetadata>,
  availableTables: Array<string>,
): Array<monaco.languages.CompletionItem> {
  const suggestions: Array<monaco.languages.CompletionItem> = []

  metadata.forEach((dataset, datasetIndex) => {
    // Only include columns from tables that are available in the query context
    // If no specific tables are in scope, show all columns
    const isTableAvailable =
      availableTables.length === 0 || availableTables.includes(dataset.source)

    if (isTableAvailable) {
      dataset.metadata_columns.forEach((column, columnIndex) => {
        suggestions.push({
          label: column.name,
          kind: monaco.languages.CompletionItemKind.Field,
          detail: `${column.dataType} - ${dataset.source}`,
          documentation: {
            value: `**Column: ${column.name}**\n\nType: \`${column.dataType}\`\nTable: \`${dataset.source}\`\n\n${
              column.description || "No description available"
            }`,
            isTrusted: true,
          },
          insertText: column.name,
          // Priority: Columns get medium priority (2-xxx)
          sortText: `2-${datasetIndex.toString().padStart(3, "0")}-${columnIndex.toString().padStart(3, "0")}`,
        })
      })
    }
  })

  return suggestions
}

// Note: createUDFSuggestions function has been moved to udfSnippets.ts
// and is imported at the top of this file for advanced snippet support

/**
 * Creates basic SQL keyword suggestions with context-aware filtering
 */
function createKeywordSuggestions(
  monaco: typeof import("monaco-editor"),
  context: QueryContext,
): Array<monaco.languages.CompletionItem> {
  // Define keywords by context
  const keywordsByContext = {
    SELECT: ["DISTINCT", "TOP", "ALL", "*"],
    FROM: [
      "WHERE",
      "INNER JOIN",
      "LEFT JOIN",
      "RIGHT JOIN",
      "FULL JOIN",
      "CROSS JOIN",
    ],
    WHERE: [
      "AND",
      "OR",
      "NOT",
      "IN",
      "EXISTS",
      "BETWEEN",
      "LIKE",
      "IS NULL",
      "IS NOT NULL",
    ],
    JOIN: ["ON", "USING"],
    "GROUP BY": ["HAVING"],
    "ORDER BY": ["ASC", "DESC", "LIMIT"],
    HAVING: ["ORDER BY", "LIMIT"],
    general: [
      "SELECT",
      "FROM",
      "WHERE",
      "GROUP BY",
      "ORDER BY",
      "HAVING",
      "LIMIT",
      "OFFSET",
    ],
  }

  let keywords: Array<string> = []

  // Add context-specific keywords
  if (context.currentClause && keywordsByContext[context.currentClause]) {
    keywords.push(...keywordsByContext[context.currentClause])
  }

  // Always include general keywords
  keywords.push(...keywordsByContext.general)

  // Remove duplicates and filter by current word
  keywords = [...new Set(keywords)]
  if (context.currentWord) {
    keywords = keywords.filter((keyword) =>
      keyword.toLowerCase().startsWith(context.currentWord.toLowerCase()),
    )
  }

  return keywords.map((keyword, index) => ({
    label: keyword,
    kind: monaco.languages.CompletionItemKind.Keyword,
    detail: "SQL Keyword",
    insertText: keyword,
    // Priority: Keywords get lowest priority (4-xxx)
    sortText: `4-${index.toString().padStart(3, "0")}`,
  }))
}
