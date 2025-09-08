/**
 * Nozzle SQL Intellisense Services
 * 
 * This module provides the main public API for setting up SQL intellisense
 * in the Nozzle Studio query playground. It handles provider registration,
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
 * import { setupNozzleSQLProviders } from './services/sql'
 * 
 * const disposable = setupNozzleSQLProviders(metadata, udfs)
 * // ... later
 * disposable.dispose()
 * ```
 * 
 * @file index.ts
 * @author SQL Intellisense System
 */

// Monaco editor will be available globally in browser - no need to import
import type { DatasetMetadata } from 'nozzl/Studio/Model'
import { QueryContextAnalyzer } from './contextAnalyzer'
import { NozzleCompletionProvider } from './completionProvider'
import { UdfSnippetGenerator } from './udfSnippets'
import { SqlValidator } from './sqlValidator'
import type {
  UserDefinedFunction,
  CompletionConfig,
  DisposableHandle,
  PerformanceMetrics
} from './types'
import { DEFAULT_COMPLETION_CONFIG } from './types'

/**
 * SQL Provider Manager
 * 
 * Manages the lifecycle of all SQL-related providers for Monaco Editor.
 * Handles registration, updates, and cleanup of completion providers,
 * hover providers, and other language features.
 */
class SqlProviderManager {
  private completionProvider?: NozzleCompletionProvider
  private contextAnalyzer?: QueryContextAnalyzer
  private snippetGenerator?: UdfSnippetGenerator
  private validator?: SqlValidator
  
  private completionDisposable?: monaco.IDisposable
  private hoverDisposable?: monaco.IDisposable
  private signatureDisposable?: monaco.IDisposable
  
  private isDisposed = false
  private metrics: PerformanceMetrics = {
    totalRequests: 0,
    averageResponseTime: 0,
    cacheHitRate: 0,
    failureCount: 0,
    lastUpdate: Date.now()
  }

  constructor(
    private metadata: DatasetMetadata[],
    private udfs: UserDefinedFunction[],
    private config: CompletionConfig = DEFAULT_COMPLETION_CONFIG
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
      this.logDebug('Setting up Nozzle SQL providers', {
        tableCount: this.metadata.length,
        udfCount: this.udfs.length
      })

      // Check if Monaco is available
      if (typeof window === 'undefined' || !window.monaco) {
        this.logDebug('Monaco editor not yet available, providers will be set up when editor loads')
        return { dispose: () => {} }
      }

      const monaco = window.monaco

      // Initialize core components
      this.contextAnalyzer = new QueryContextAnalyzer(this.config)
      this.completionProvider = new NozzleCompletionProvider(
        this.metadata,
        this.udfs,
        this.contextAnalyzer,
        this.config
      )
      this.snippetGenerator = new UdfSnippetGenerator()
      this.validator = new SqlValidator(this.metadata, this.udfs, this.config)

      // Register completion provider
      this.completionDisposable = monaco.languages.registerCompletionItemProvider('sql', {
        provideCompletionItems: async (model, position, context, token) => {
          const startTime = performance.now()
          try {
            this.metrics.totalRequests++
            
            const result = await this.completionProvider!.provideCompletionItems(
              model, position, context, token
            )
            
            const duration = performance.now() - startTime
            this.updateMetrics(duration, true)
            
            return result
          } catch (error) {
            const duration = performance.now() - startTime
            this.updateMetrics(duration, false)
            this.logError('Completion provider failed', error)
            
            // Return fallback completions
            return { suggestions: this.createFallbackCompletions() }
          }
        },
        // Trigger completions on space, dot, and newline
        triggerCharacters: [' ', '.', '\n', '\t']
      })

      // Register hover provider for UDF documentation
      this.hoverDisposable = monaco.languages.registerHoverProvider('sql', {
        provideHover: (model, position) => {
          try {
            return this.provideHover(model, position)
          } catch (error) {
            this.logError('Hover provider failed', error)
            return null
          }
        }
      })

      // Register signature help provider for UDF parameters
      this.signatureDisposable = monaco.languages.registerSignatureHelpProvider('sql', {
        signatureHelpTriggerCharacters: ['(', ','],
        signatureHelpRetriggerCharacters: [','],
        provideSignatureHelp: (model, position) => {
          try {
            return this.provideSignatureHelp(model, position)
          } catch (error) {
            this.logError('Signature help provider failed', error)
            return null
          }
        }
      })

      this.logDebug('Nozzle SQL providers setup completed successfully')

      // Return disposable handle
      return {
        dispose: () => this.dispose()
      }

    } catch (error) {
      this.logError('Failed to setup SQL providers', error)
      
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
  updateData(metadata: DatasetMetadata[], udfs: UserDefinedFunction[]): void {
    if (this.isDisposed) {
      this.logWarning('Attempted to update disposed provider manager')
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

      this.logDebug('Provider data updated', {
        tableCount: metadata.length,
        udfCount: udfs.length
      })

    } catch (error) {
      this.logError('Failed to update provider data', error)
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
  getValidator(): SqlValidator | null {
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
      this.logDebug('Disposing Nozzle SQL providers')

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

      this.logDebug('Nozzle SQL providers disposed successfully')

    } catch (error) {
      this.logError('Error disposing SQL providers', error)
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
    model: monaco.editor.ITextModel,
    position: monaco.Position
  ): monaco.languages.ProviderResult<monaco.languages.Hover> {
    // Get the word at the current position
    const word = model.getWordAtPosition(position)
    if (!word) {
      return null
    }

    // Check if it's a UDF function
    const udf = this.udfs.find(u => 
      u.name === word.word || 
      u.name.replace('${dataset}', '{dataset}') === word.word
    )

    if (!udf) {
      return null
    }

    // Generate hover information using snippet generator
    if (this.snippetGenerator) {
      return this.snippetGenerator.createHoverInfo(udf)
    }

    return null
  }

  /**
   * Provide Signature Help
   * 
   * Provides parameter hints for UDF functions.
   * 
   * @private
   */
  private provideSignatureHelp(
    model: monaco.editor.ITextModel,
    position: monaco.Position
  ): monaco.languages.ProviderResult<monaco.languages.SignatureHelpResult> {
    // This is a placeholder for signature help implementation
    // Full implementation would require parsing function calls and parameter positions
    return null
  }

  /**
   * Create Fallback Completions
   * 
   * Provides basic SQL completions when the main provider fails.
   * 
   * @private
   */
  private createFallbackCompletions(): monaco.languages.CompletionItem[] {
    const basicKeywords = [
      'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 
      'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'DISTINCT', 'AS'
    ]

    return basicKeywords.map((keyword, index) => ({
      label: keyword,
      kind: monaco.languages.CompletionItemKind.Keyword,
      detail: 'SQL Keyword',
      insertText: keyword,
      sortText: index.toString().padStart(3, '0')
    }))
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
    this.metrics.averageResponseTime = 
      alpha * duration + (1 - alpha) * this.metrics.averageResponseTime

    this.metrics.lastUpdate = Date.now()
  }

  /**
   * Logging Utilities
   */

  private logDebug(message: string, data?: any): void {
    if (this.config.enableDebugLogging) {
      console.debug(`[SqlProviderManager] ${message}`, data)
    }
  }

  private logWarning(message: string, data?: any): void {
    console.warn(`[SqlProviderManager] ${message}`, data)
  }

  private logError(message: string, error: any): void {
    console.error(`[SqlProviderManager] ${message}`, error)
  }
}

/**
 * Active provider manager instance
 * Used to ensure only one manager is active at a time
 */
let activeProviderManager: SqlProviderManager | null = null

/**
 * Setup Nozzle SQL Providers
 * 
 * Main public API function for setting up SQL intellisense providers.
 * This is the primary entry point for integrating with Monaco Editor.
 * 
 * @param metadata - Dataset metadata from the API
 * @param udfs - User-defined function definitions
 * @param config - Optional provider configuration
 * @returns Disposable handle for cleanup
 */
export function setupNozzleSQLProviders(
  metadata: DatasetMetadata[],
  udfs: UserDefinedFunction[],
  config?: Partial<CompletionConfig>
): DisposableHandle {
  console.debug('[setupNozzleSQLProviders] Starting setup...', {
    hasExistingManager: !!activeProviderManager,
    metadataCount: metadata.length,
    udfCount: udfs.length
  })

  // Dispose existing manager if present
  if (activeProviderManager) {
    console.debug('[setupNozzleSQLProviders] Disposing existing manager')
    activeProviderManager.dispose()
    activeProviderManager = null
  }

  // Create and setup new manager
  const finalConfig = { ...DEFAULT_COMPLETION_CONFIG, ...config }
  activeProviderManager = new SqlProviderManager(metadata, udfs, finalConfig)
  
  console.debug('[setupNozzleSQLProviders] Created new manager, calling setup()...')
  const disposable = activeProviderManager.setup()

  console.debug('[setupNozzleSQLProviders] Setup completed, manager should be active:', !!activeProviderManager)

  // Return enhanced disposable that also cleans up the active manager
  return {
    dispose: () => {
      disposable.dispose()
      if (activeProviderManager) {
        activeProviderManager.dispose()
        activeProviderManager = null
      }
    }
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
  metadata: DatasetMetadata[],
  udfs: UserDefinedFunction[]
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
  return activeProviderManager !== null && !activeProviderManager['isDisposed']
}

/**
 * Get Active SQL Validator
 * 
 * Returns the currently active SQL validator instance for direct validation access.
 * 
 * @returns SqlValidator instance or null if no provider is active
 */
export function getActiveValidator(): SqlValidator | null {
  const validator = activeProviderManager ? activeProviderManager.getValidator() : null
  console.debug('[getActiveValidator] Called:', {
    hasActiveManager: !!activeProviderManager,
    managerDisposed: activeProviderManager ? activeProviderManager['isDisposed'] : 'N/A',
    hasValidator: !!validator
  })
  return validator
}

// Export all types and classes for advanced usage
export * from './types'
export { QueryContextAnalyzer } from './contextAnalyzer'
export { NozzleCompletionProvider } from './completionProvider'
export { UdfSnippetGenerator, createUdfSnippet, createUdfCompletionItem } from './udfSnippets'
export { SqlValidator } from './sqlValidator'