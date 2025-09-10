/**
 * Unified SQL Provider
 *
 * Consolidates all SQL intellisense functionality into a single, cohesive provider.
 * This replaces the previous multi-provider architecture with a simpler, more maintainable solution.
 *
 * Features:
 * - Single point of initialization and disposal
 * - Consolidated Monaco provider registration
 * - Built-in validation with Monaco events
 * - Simplified API for Editor integration
 * - Strong TypeScript typing
 *
 * @file UnifiedSQLProvider.ts
 */

import type { editor, IDisposable, Position } from "monaco-editor/esm/vs/editor/editor.api"
import * as monaco from "monaco-editor/esm/vs/editor/editor.api"
import type { DatasetSource } from "nozzl/Studio/Model"

import { NozzleCompletionProvider } from "./NozzleCompletionProvider"
import { QueryContextAnalyzer } from "./QueryContextAnalyzer"
import { SqlValidator } from "./sqlValidator"
import type { CompletionConfig, UserDefinedFunction } from "./types"
import { UdfSnippetGenerator } from "./UDFSnippetGenerator"

/**
 * Configuration for SQL Provider
 */
export interface SQLProviderConfig {
  validationLevel: "basic" | "standard" | "full" | "off"
  enablePartialValidation: boolean
  enableDebugLogging: boolean
  minPrefixLength: number
  maxSuggestions: number
}

/**
 * Unified SQL Provider Interface
 */
export interface ISQLProvider {
  setup(editor: editor.IStandaloneCodeEditor): void
  dispose(): void
  getValidator(): SqlValidator | null
}

/**
 * Unified SQL Provider Class
 *
 * Single provider that handles all SQL intellisense functionality including:
 * - Code completion
 * - Validation with error markers
 * - Hover information
 * - Signature help
 * - UDF snippets
 */
export class UnifiedSQLProvider implements ISQLProvider {
  private completionProvider: NozzleCompletionProvider | undefined
  private contextAnalyzer: QueryContextAnalyzer | undefined
  private snippetGenerator: UdfSnippetGenerator | undefined
  private validator: SqlValidator | undefined

  private completionDisposable: IDisposable | undefined
  private hoverDisposable: IDisposable | undefined
  private signatureDisposable: IDisposable | undefined

  private validationDisposable: IDisposable | undefined
  private validationTimeout: NodeJS.Timeout | undefined

  private isDisposed = false

  constructor(
    private readonly sources: ReadonlyArray<DatasetSource>,
    private readonly udfs: ReadonlyArray<UserDefinedFunction>,
    private readonly config: SQLProviderConfig,
  ) {}

  /**
   * Setup all providers and register with Monaco Editor
   */
  setup(editor: editor.IStandaloneCodeEditor): void {
    if (this.isDisposed) {
      throw new Error("Cannot setup disposed SQL provider")
    }

    try {
      this.initializeProviders()
      this.registerMonacoProviders()
      this.setupValidation(editor)

      this.logDebug("UnifiedSQLProvider setup completed successfully")
    } catch (error) {
      this.logError("Failed to setup UnifiedSQLProvider", error)
      this.dispose()
      throw error
    }
  }

  /**
   * Get the SQL validator instance
   */
  getValidator(): SqlValidator | null {
    return this.validator || null
  }

  /**
   * Dispose all providers and cleanup resources
   */
  dispose(): void {
    if (this.isDisposed) return

    try {
      this.logDebug("Disposing UnifiedSQLProvider")

      // Clear validation timeout
      if (this.validationTimeout) {
        clearTimeout(this.validationTimeout)
        this.validationTimeout = undefined
      }

      // Dispose Monaco providers
      this.completionDisposable?.dispose()
      this.hoverDisposable?.dispose()
      this.signatureDisposable?.dispose()
      this.validationDisposable?.dispose()

      // Dispose internal providers
      this.completionProvider?.dispose()
      this.validator?.dispose()
      this.contextAnalyzer?.clearCache()

      // Clear references
      this.completionProvider = undefined
      this.contextAnalyzer = undefined
      this.snippetGenerator = undefined
      this.validator = undefined
      this.completionDisposable = undefined
      this.hoverDisposable = undefined
      this.signatureDisposable = undefined
      this.validationDisposable = undefined

      this.isDisposed = true

      this.logDebug("UnifiedSQLProvider disposed successfully")
    } catch (error) {
      this.logError("Error disposing UnifiedSQLProvider", error)
    }
  }

  /**
   * Initialize all internal providers
   * @private
   */
  private initializeProviders(): void {
    const completionConfig: CompletionConfig = {
      enableDebugLogging: this.config.enableDebugLogging,
      minPrefixLength: this.config.minPrefixLength,
      maxSuggestions: this.config.maxSuggestions,
      enableSqlValidation: this.config.validationLevel !== "off",
      validationLevel: this.config.validationLevel !== "off" ? this.config.validationLevel : "basic",
      enablePartialValidation: this.config.enablePartialValidation,
      enableSnippets: true,
      enableContextFiltering: true,
      enableAliasResolution: true,
      contextCacheTTL: 300000,
    }

    this.contextAnalyzer = new QueryContextAnalyzer(completionConfig)
    this.completionProvider = new NozzleCompletionProvider(
      [...this.sources],
      [...this.udfs],
      this.contextAnalyzer,
      completionConfig,
    )
    this.snippetGenerator = new UdfSnippetGenerator()

    if (this.config.validationLevel !== "off") {
      this.validator = new SqlValidator([...this.sources], [...this.udfs], completionConfig)
    }
  }

  /**
   * Register all Monaco language providers
   * @private
   */
  private registerMonacoProviders(): void {
    // Register completion provider
    this.completionDisposable = monaco.languages.registerCompletionItemProvider("sql", {
      provideCompletionItems: async (model, position, context, token) => {
        try {
          if (!this.completionProvider) return { suggestions: [] }

          const result = await this.completionProvider.provideCompletionItems(model, position, context, token)
          return result
        } catch (error) {
          this.logError("Completion provider failed", error)
          return { suggestions: this.createFallbackCompletions(position) }
        }
      },
      triggerCharacters: [" ", ".", "\n", "\t"],
    })

    // Register hover provider for UDF documentation
    this.hoverDisposable = monaco.languages.registerHoverProvider("sql", {
      provideHover: (model, position) => {
        try {
          return this.provideHover(model, position)
        } catch (error) {
          this.logError("Hover provider failed", error)
          return null
        }
      },
    })

    // Register signature help provider for UDF parameters
    this.signatureDisposable = monaco.languages.registerSignatureHelpProvider("sql", {
      signatureHelpTriggerCharacters: ["(", ","],
      signatureHelpRetriggerCharacters: [","],
      provideSignatureHelp: (model, position) => {
        try {
          return this.provideSignatureHelp(model, position)
        } catch (error) {
          this.logError("Signature help provider failed", error)
          return null
        }
      },
    })
  }

  /**
   * Setup validation with Monaco events
   * @private
   */
  private setupValidation(editor: editor.IStandaloneCodeEditor): void {
    if (this.config.validationLevel === "off" || !this.validator) {
      return
    }

    const model = editor.getModel()
    if (!model) return

    const validateQuery = () => {
      if (!this.validator || this.isDisposed) return

      const query = model.getValue()

      // Clear markers for empty queries
      if (!query.trim()) {
        monaco.editor.setModelMarkers(model, "sql-validator", [])
        return
      }

      try {
        const errors = this.validator.validateQuery(query)
        const markers: Array<editor.IMarkerData> = errors.map((error: any) => ({
          severity: error.severity,
          message: error.message,
          startLineNumber: error.startLineNumber,
          startColumn: error.startColumn,
          endLineNumber: error.endLineNumber,
          endColumn: error.endColumn,
          code: error.code,
          source: "nozzle-sql-validator",
        }))

        monaco.editor.setModelMarkers(model, "sql-validator", markers)
        this.logDebug(`Validation completed: ${errors.length} errors found`)
      } catch (error) {
        this.logError("SQL validation failed", error)
        monaco.editor.setModelMarkers(model, "sql-validator", [])
      }
    }

    // Initial validation
    validateQuery()

    // Setup debounced validation on content change
    const contentChangeDisposable = model.onDidChangeContent(() => {
      if (this.validationTimeout) {
        clearTimeout(this.validationTimeout)
      }

      this.validationTimeout = setTimeout(() => {
        validateQuery()
        this.validationTimeout = undefined
      }, 500) as NodeJS.Timeout // 500ms debounce
    })

    // Cleanup on model disposal
    const modelDisposeDisposable = model.onWillDispose(() => {
      contentChangeDisposable.dispose()
      modelDisposeDisposable.dispose()
      if (this.validationTimeout) {
        clearTimeout(this.validationTimeout)
        this.validationTimeout = undefined
      }
      monaco.editor.setModelMarkers(model, "sql-validator", [])
    })

    // Store disposal reference
    this.validationDisposable = {
      dispose: () => {
        contentChangeDisposable.dispose()
        modelDisposeDisposable.dispose()
        if (this.validationTimeout) {
          clearTimeout(this.validationTimeout)
          this.validationTimeout = undefined
        }
      },
    }

    // Cleanup on editor disposal
    editor.onDidDispose(() => {
      this.validationDisposable?.dispose()
    })
  }

  /**
   * Provide hover information for UDFs
   * @private
   */
  private provideHover(
    model: editor.ITextModel,
    position: Position,
  ): monaco.languages.ProviderResult<monaco.languages.Hover> {
    const word = model.getWordAtPosition(position)
    if (!word) return null

    // Check if it's a UDF function
    const udf = this.udfs.find((u) => u.name === word.word || u.name.replace("${dataset}", "{dataset}") === word.word)

    if (!udf || !this.snippetGenerator) return null

    return this.snippetGenerator.createHoverInfo(udf)
  }

  /**
   * Provide signature help for UDFs
   * @private
   */
  private provideSignatureHelp(
    _model: editor.ITextModel,
    _position: Position,
  ): monaco.languages.ProviderResult<monaco.languages.SignatureHelpResult> {
    // Placeholder for signature help implementation
    return null
  }

  /**
   * Create fallback completions when main provider fails
   * @private
   */
  private createFallbackCompletions(position: Position): Array<monaco.languages.CompletionItem> {
    const basicKeywords = [
      "SELECT",
      "FROM",
      "WHERE",
      "JOIN",
      "INNER JOIN",
      "LEFT JOIN",
      "GROUP BY",
      "ORDER BY",
      "HAVING",
      "LIMIT",
      "DISTINCT",
      "AS",
    ]

    return basicKeywords.map((keyword, index) => ({
      label: keyword,
      kind: monaco.languages.CompletionItemKind.Keyword,
      detail: "SQL Keyword",
      insertText: keyword,
      sortText: index.toString().padStart(3, "0"),
      range: {
        startColumn: position.column,
        startLineNumber: position.lineNumber,
        endColumn: position.column,
        endLineNumber: position.lineNumber,
      },
    }))
  }

  /**
   * Logging utilities
   * @private
   */
  private logDebug(_message: string, _data?: any): void {
    // Debug logging removed for production
  }

  private logError(message: string, error: any): void {
    console.error(`[UnifiedSQLProvider] ${message}`, error)
  }
}
