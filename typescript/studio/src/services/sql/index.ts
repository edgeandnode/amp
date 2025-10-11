/**
 * Amp SQL Intellisense Services
 *
 * This module provides the main public API for setting up SQL intellisense
 * in the Amp Studio query playground. It handles provider registration,
 * lifecycle management, and integration with Monaco Editor.
 *
 * Key Features:
 * - One-click provider setup and registration
 * - Automatic lifecycle management and cleanup
 * - Error recovery and fallback handling
 * - Performance monitoring and optimization
 * - Hot-swapping of metadata and UDF updates
 *
 * Usage:
 * ```typescript
 * import { setupAmpSQLProviders } from './services/sql'
 *
 * const disposable = setupAmpSQLProviders(metadata, udfs)
 * // ... later
 * disposable.dispose()
 * ```
 *
 * @file index.ts
 * @author SQL Intellisense System
 */

import type { editor, IDisposable, Position } from "monaco-editor/esm/vs/editor/editor.api"
import * as monaco from "monaco-editor/esm/vs/editor/editor.api"
import type { DatasetSource } from "studio-cli/Studio/Model"

import { AmpCompletionProvider } from "./AmpCompletionProvider.ts"
import { QueryContextAnalyzer } from "./QueryContextAnalyzer.ts"
import { SqlValidation } from "./SqlValidation.ts"
import type { CompletionConfig, DisposableHandle, PerformanceMetrics, UserDefinedFunction } from "./types.ts"
import { DEFAULT_COMPLETION_CONFIG } from "./types.ts"
import { UdfSnippetGenerator } from "./UDFSnippetGenerator.ts"

/**
 * SQL Provider Manager
 *
 * Manages the lifecycle of all SQL-related providers for Monaco Editor.
 * Handles registration, updates, and cleanup of completion providers,
 * hover providers, and other language features.
 */
class SqlProviderManager {
  private completionProvider?: AmpCompletionProvider | undefined
  private contextAnalyzer?: QueryContextAnalyzer | undefined
  private snippetGenerator?: UdfSnippetGenerator | undefined
  private validator?: SqlValidation | undefined

  private completionDisposable?: IDisposable | undefined
  private hoverDisposable?: IDisposable | undefined
  private signatureDisposable?: IDisposable | undefined

  private isDisposed = false

  get disposed(): boolean {
    return this.isDisposed
  }
  private metrics: PerformanceMetrics = {
    totalRequests: 0,
    averageResponseTime: 0,
    cacheHitRate: 0,
    failureCount: 0,
    lastUpdate: Date.now(),
  }

  private static readonly FALLBACK_KEYWORDS = [
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
  ] as const

  constructor(
    private metadata: ReadonlyArray<DatasetSource>,
    private udfs: ReadonlyArray<UserDefinedFunction>,
    private config: CompletionConfig = DEFAULT_COMPLETION_CONFIG,
  ) {}

  /**
   * Setup All Providers
   *
   * Registers all SQL language providers with Monaco Editor and
   * returns a disposable handle for cleanup.
   *
   * @returns DisposableHandle for cleanup
   */
  setup(): DisposableHandle {
    try {
      // Initialize core components
      this.contextAnalyzer = new QueryContextAnalyzer(this.config)
      this.completionProvider = new AmpCompletionProvider(
        this.metadata,
        this.udfs,
        this.contextAnalyzer,
        this.config,
      )
      this.snippetGenerator = new UdfSnippetGenerator()
      this.validator = new SqlValidation(this.metadata, this.udfs, this.config)

      // Register completion provider
      this.completionDisposable = monaco.languages.registerCompletionItemProvider("sql", {
        provideCompletionItems: this.handleCompletionRequest.bind(this),
        triggerCharacters: [" ", ".", "\n", "\t"],
      })

      // Register hover provider for UDF documentation
      this.hoverDisposable = monaco.languages.registerHoverProvider("sql", {
        provideHover: this.handleHoverRequest.bind(this),
      })

      // Register signature help provider for UDF parameters
      this.signatureDisposable = monaco.languages.registerSignatureHelpProvider("sql", {
        signatureHelpTriggerCharacters: ["(", ","],
        signatureHelpRetriggerCharacters: [","],
        provideSignatureHelp: this.handleSignatureHelpRequest.bind(this),
      })

      this.logDebug("Amp SQL providers setup completed successfully")

      // Return disposable handle
      return {
        dispose: () => this.dispose(),
      }
    } catch (error) {
      this.logError("Failed to setup SQL providers", error)

      // Cleanup any partial setup
      this.dispose()

      // Return no-op disposable
      return { dispose: () => {} }
    }
  }

  /**
   * Update Provider Data
   *
   * Updates the metadata and UDF data for all providers.
   * This is called when fresh data is available from the API.
   *
   * @param metadata - Updated dataset metadata
   * @param udfs - Updated UDF definitions
   */
  updateData(metadata: ReadonlyArray<DatasetSource>, udfs: ReadonlyArray<UserDefinedFunction>): void {
    if (this.isDisposed) {
      this.logWarning("Attempted to update disposed provider manager")
      return
    }

    try {
      this.metadata = metadata
      this.udfs = udfs

      if (this.completionProvider) {
        this.completionProvider.updateData(metadata, udfs)
      }

      if (this.validator) {
        this.validator.updateData(metadata, udfs)
      }

      this.logDebug("Provider data updated", {
        tableCount: metadata.length,
        udfCount: udfs.length,
      })
    } catch (error) {
      this.logError("Failed to update provider data", error)
    }
  }

  /**
   * Get Performance Metrics
   *
   * Returns current performance metrics for monitoring.
   *
   * @returns Current performance metrics
   */
  getMetrics(): PerformanceMetrics {
    return { ...this.metrics }
  }

  /**
   * Get SQL Validator Instance
   *
   * Returns the validator instance for direct access to validation functionality.
   *
   * @returns SqlValidator instance or null if not initialized
   */
  getValidator(): SqlValidation | null {
    return this.validator || null
  }

  /**
   * Dispose All Providers
   *
   * Cleans up all registered providers and resources.
   */
  dispose(): void {
    if (this.isDisposed) {
      return
    }

    try {
      this.logDebug("Disposing Amp SQL providers")

      // Dispose Monaco providers
      if (this.completionDisposable) {
        this.completionDisposable.dispose()
        this.completionDisposable = undefined
      }

      if (this.hoverDisposable) {
        this.hoverDisposable.dispose()
        this.hoverDisposable = undefined
      }

      if (this.signatureDisposable) {
        this.signatureDisposable.dispose()
        this.signatureDisposable = undefined
      }

      // Dispose internal providers
      if (this.completionProvider) {
        this.completionProvider.dispose()
        this.completionProvider = undefined
      }

      if (this.validator) {
        this.validator.dispose()
        this.validator = undefined
      }

      if (this.contextAnalyzer) {
        this.contextAnalyzer.clearCache()
        this.contextAnalyzer = undefined
      }

      this.snippetGenerator = undefined
      this.isDisposed = true

      this.logDebug("Amp SQL providers disposed successfully")
    } catch (error) {
      this.logError("Error disposing SQL providers", error)
    }
  }

  /**
   * Handle completion requests with metrics and error handling
   * @private
   */
  private async handleCompletionRequest(
    model: editor.ITextModel,
    position: Position,
    context: monaco.languages.CompletionContext,
    token: monaco.CancellationToken,
  ): Promise<monaco.languages.CompletionList> {
    const startTime = performance.now()
    try {
      this.metrics.totalRequests++

      if (!this.completionProvider) {
        return { suggestions: [] }
      }

      const result = await this.completionProvider.provideCompletionItems(model, position, context, token)
      const duration = performance.now() - startTime
      this.updateMetrics(duration, true)

      return result || { suggestions: [] }
    } catch (error) {
      const duration = performance.now() - startTime
      this.updateMetrics(duration, false)
      this.logError("Completion provider failed", error)

      return { suggestions: this.createFallbackCompletions(position) }
    }
  }

  /**
   * Handle hover requests with error handling
   * @private
   */
  private handleHoverRequest(
    model: editor.ITextModel,
    position: Position,
  ): monaco.languages.ProviderResult<monaco.languages.Hover> {
    try {
      return this.provideHover(model, position)
    } catch (error) {
      this.logError("Hover provider failed", error)
      return null
    }
  }

  /**
   * Handle signature help requests with error handling
   * @private
   */
  private handleSignatureHelpRequest(
    model: editor.ITextModel,
    position: Position,
  ): monaco.languages.ProviderResult<monaco.languages.SignatureHelpResult> {
    try {
      return this.provideSignatureHelp(model, position)
    } catch (error) {
      this.logError("Signature help provider failed", error)
      return null
    }
  }

  /**
   * Provide Hover Information
   *
   * Provides hover information for UDF functions.
   *
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
   * Provide Signature Help
   *
   * Provides parameter hints for UDF functions.
   *
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
   * Create Fallback Completions
   *
   * Provides basic SQL completions when the main provider fails.
   *
   * @private
   */
  private createFallbackCompletions(position: Position): Array<monaco.languages.CompletionItem> {
    const range = {
      startColumn: position.column,
      startLineNumber: position.lineNumber,
      endColumn: position.column,
      endLineNumber: position.lineNumber,
    }

    const completions = new Array<monaco.languages.CompletionItem>(SqlProviderManager.FALLBACK_KEYWORDS.length)
    for (let i = 0; i < SqlProviderManager.FALLBACK_KEYWORDS.length; i++) {
      const keyword = SqlProviderManager.FALLBACK_KEYWORDS[i]
      const sortText = i < 10 ? `00${i}` : i < 100 ? `0${i}` : `${i}`

      completions[i] = {
        label: keyword,
        kind: monaco.languages.CompletionItemKind.Keyword,
        detail: "SQL Keyword",
        insertText: keyword,
        sortText,
        range,
      }
    }

    return completions
  }

  /**
   * Update Performance Metrics
   *
   * Updates performance tracking metrics.
   *
   * @private
   */
  private updateMetrics(duration: number, success: boolean): void {
    if (!success) {
      this.metrics.failureCount++
    }

    // Update average response time (simple moving average)
    const alpha = 0.1 // Smoothing factor
    this.metrics.averageResponseTime = alpha * duration + (1 - alpha) * this.metrics.averageResponseTime

    this.metrics.lastUpdate = Date.now()
  }

  /**
   * Logging Utilities
   */
  private logDebug(_message: string, _data?: unknown): void {
    // Debug logging removed for production
  }

  private logWarning(message: string, data?: unknown): void {
    console.warn(`[SqlProviderManager] ${message}`, data)
  }

  private logError(message: string, error: unknown): void {
    console.error(`[SqlProviderManager] ${message}`, error)
  }
}

/**
 * Active provider manager instance
 * Used to ensure only one manager is active at a time
 */
let activeProviderManager: SqlProviderManager | null = null

/**
 * Setup Amp SQL Providers
 *
 * Main public API function for setting up SQL intellisense providers.
 * This is the primary entry point for integrating with Monaco Editor.
 *
 * @param metadata - Dataset metadata from the API
 * @param udfs - User-defined function definitions
 * @param config - Optional provider configuration
 * @returns Disposable handle for cleanup
 */
export function setupAmpSQLProviders(
  metadata: ReadonlyArray<DatasetSource>,
  udfs: ReadonlyArray<UserDefinedFunction>,
  config?: Partial<CompletionConfig>,
): DisposableHandle {
  // Dispose existing manager if present
  if (activeProviderManager) {
    activeProviderManager.dispose()
    activeProviderManager = null
  }

  // Create and setup new manager
  const finalConfig = { ...DEFAULT_COMPLETION_CONFIG, ...config }
  activeProviderManager = new SqlProviderManager(metadata, udfs, finalConfig)

  const disposable = activeProviderManager.setup()

  // Return enhanced disposable that also cleans up the active manager
  return {
    dispose: () => {
      disposable.dispose()
      if (activeProviderManager) {
        activeProviderManager.dispose()
        activeProviderManager = null
      }
    },
  }
}

/**
 * Update Provider Data
 *
 * Updates the metadata and UDF data for the currently active providers.
 * This should be called when fresh data is received from the API.
 *
 * @param metadata - Updated dataset metadata
 * @param udfs - Updated UDF definitions
 */
export function updateProviderData(
  metadata: ReadonlyArray<DatasetSource>,
  udfs: ReadonlyArray<UserDefinedFunction>,
): void {
  if (activeProviderManager) {
    activeProviderManager.updateData(metadata, udfs)
  }
}

/**
 * Get Provider Performance Metrics
 *
 * Returns performance metrics for monitoring and debugging.
 *
 * @returns Current performance metrics or null if no provider is active
 */
export function getProviderMetrics(): PerformanceMetrics | null {
  return activeProviderManager ? activeProviderManager.getMetrics() : null
}

/**
 * Dispose All Providers
 *
 * Convenience function to dispose all active providers.
 * Generally not needed as the disposable handle should be used instead.
 */
export function disposeAllProviders(): void {
  if (activeProviderManager) {
    activeProviderManager.dispose()
    activeProviderManager = null
  }
}

/**
 * Check if Providers are Active
 *
 * Returns true if SQL providers are currently active and registered.
 *
 * @returns True if providers are active
 */
export function areProvidersActive(): boolean {
  return activeProviderManager !== null && !activeProviderManager.disposed
}

/**
 * Get Active SQL Validator
 *
 * Returns the currently active SQL validator instance for direct validation access.
 *
 * @returns SqlValidator instance or null if no provider is active
 */
export function getActiveValidator(): SqlValidation | null {
  const validator = activeProviderManager ? activeProviderManager.getValidator() : null
  return validator
}

// Export new unified provider
export { type ISQLProvider, type SQLProviderConfig, UnifiedSQLProvider } from "./UnifiedSQLProvider"

// Export all types and classes for advanced usage
export { AmpCompletionProvider } from "./AmpCompletionProvider"
export { QueryContextAnalyzer } from "./QueryContextAnalyzer"
export { SqlValidation } from "./SqlValidation.ts"
export * from "./types"
export { createUdfCompletionItem, createUdfSnippet, UdfSnippetGenerator } from "./UDFSnippetGenerator"
