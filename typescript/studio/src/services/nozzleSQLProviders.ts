/**
 * Nozzle SQL Providers - Monaco Editor SQL Language Providers
 *
 * This service provides custom completion, hover, and code action providers
 * for Monaco Editor to enhance the SQL editing experience with Nozzle-specific
 * features like dataset metadata, UDF functions, and context-aware completions.
 *
 * Key features:
 * - Dynamic table/column completions from metadata API
 * - UDF function suggestions with snippets
 * - Context-aware completions based on SQL clause analysis
 * - Proper provider lifecycle management to prevent memory leaks
 */

import type * as monaco from "monaco-editor"
import { provideUDFHover } from "./udfSnippets"

// Type definitions for our data structures
export interface DatasetMetadata {
  source: string
  metadata_columns: Array<{
    name: string
    dataType: string
    description?: string
  }>
}

export interface UserDefinedFunction {
  name: string
  description: string
  sql: string
}

// Global disposable tracker for provider cleanup
let globalDisposables: Array<any> = []

/**
 * Main function to set up all Nozzle SQL providers for Monaco Editor.
 * This function registers completion, hover, and code action providers
 * and manages their lifecycle to prevent memory leaks.
 *
 * @param monaco The Monaco Editor instance from @monaco-editor/react
 * @param metadata Array of dataset metadata containing tables and columns
 * @param udfs Array of user-defined functions with descriptions
 * @throws Error if Monaco editor is not available
 */
export function setupNozzleSQLProviders(
  monaco: typeof import("monaco-editor"),
  metadata: Array<DatasetMetadata>,
  udfs: Array<UserDefinedFunction>,
): void {
  // Ensure Monaco is available
  if (!monaco || !monaco.languages) {
    throw new Error(
      "Monaco Editor is not available. Ensure it is properly loaded.",
    )
  }

  try {
    // Clean up any existing providers first to prevent conflicts
    disposeProviders()

    // Register completion provider with trigger characters
    const completionProvider = monaco.languages.registerCompletionItemProvider(
      "sql",
      {
        triggerCharacters: [" ", ".", "\n"],
        provideCompletionItems: async (model, position, context, token) => {
          return provideNozzleCompletions(
            monaco,
            model,
            position,
            metadata,
            udfs,
            context,
            token,
          )
        },
      },
    )

    // Register hover provider for UDF documentation - using enhanced version from udfSnippets.ts
    const hoverProvider = monaco.languages.registerHoverProvider("sql", {
      provideHover: (model, position) => {
        return provideUDFHover(monaco, model, position, udfs)
      },
    })

    // Register code action provider for quick fixes
    const codeActionProvider = monaco.languages.registerCodeActionProvider(
      "sql",
      {
        provideCodeActions: (model, range, context) => {
          return provideNozzleCodeActions(
            monaco,
            model,
            range,
            context,
            metadata,
          )
        },
      },
    )

    // Store disposables for cleanup
    globalDisposables.push(
      completionProvider,
      hoverProvider,
      codeActionProvider,
    )

    console.log("✅ Nozzle SQL providers registered successfully", {
      tablesCount: metadata.length,
      udfsCount: udfs.length,
    })
  } catch (error) {
    console.error("❌ Failed to setup Nozzle SQL providers:", error)
    // Clean up any partial registrations
    disposeProviders()
    throw error
  }
}

/**
 * Properly disposes of all registered providers to prevent memory leaks.
 * This should be called when the editor is unmounted or when re-registering providers.
 */
export function disposeProviders(): void {
  try {
    globalDisposables.forEach((disposable) => {
      if (disposable && typeof disposable.dispose === "function") {
        disposable.dispose()
      }
    })
    globalDisposables = []
    console.log("🧹 Disposed Nozzle SQL providers")
  } catch (error) {
    console.warn("⚠️ Error disposing providers:", error)
  }
}

/**
 * Provides completion items for SQL queries based on context analysis.
 * This is the core completion logic that determines what suggestions to show
 * based on cursor position and query context.
 */
async function provideNozzleCompletions(
  monaco: typeof import("monaco-editor"),
  model: monaco.editor.ITextModel,
  position: monaco.Position,
  metadata: Array<DatasetMetadata>,
  udfs: Array<UserDefinedFunction>,
  context: monaco.languages.CompletionContext,
  token: monaco.CancellationToken,
): Promise<monaco.languages.CompletionList> {
  // Check if operation was cancelled
  if (token.isCancellationRequested) {
    return { suggestions: [] }
  }

  try {
    // Import our completion logic (will be created next)
    const { provideNozzleCompletions: provideCompletions } = await import(
      "./nozzleCompletions"
    )
    return provideCompletions(monaco, model, position, metadata, udfs)
  } catch (error) {
    console.error("❌ Error in completion provider:", error)
    return { suggestions: [] }
  }
}

// Note: provideUDFHover function has been moved to udfSnippets.ts
// and is imported at the top of this file for enhanced UDF documentation with usage tips

/**
 * Provides code actions for common SQL and Nozzle-specific issues.
 * This includes quick fixes for common mistakes and helpful suggestions.
 */
function provideNozzleCodeActions(
  monaco: typeof import("monaco-editor"),
  model: monaco.editor.ITextModel,
  range: monaco.Range,
  context: monaco.languages.CodeActionContext,
  metadata: Array<DatasetMetadata>,
): monaco.languages.ProviderResult<monaco.languages.CodeActionList> {
  const actions: Array<monaco.languages.CodeAction> = []

  try {
    const query = model.getValue()

    // Quick fix for incorrect eth_call usage
    if (query.includes(".eth_call") && !query.match(/\w+\.eth_call/)) {
      actions.push({
        title: "🔧 Fix eth_call usage - add dataset prefix",
        kind: "quickfix",
        edit: {
          edits: [
            {
              resource: model.uri,
              textEdit: {
                range: range,
                text: query.replace(
                  /([a-zA-Z_]\w*\.)?eth_call/g,
                  "${dataset}.eth_call",
                ),
              },
            },
          ],
        },
      })
    }

    // Suggest available tables when none are referenced
    if (!query.match(/FROM\s+\w+\.\w+/i) && metadata.length > 0) {
      const availableTables = metadata
        .slice(0, 3)
        .map((d) => d.source)
        .join(", ")
      actions.push({
        title: `💡 Available tables: ${availableTables}${metadata.length > 3 ? "..." : ""}`,
        kind: "quickfix",
        command: {
          id: "nozzle.showAvailableTables",
          title: "Show Available Tables",
        },
      })
    }

    return { actions, dispose: () => {} }
  } catch (error) {
    console.error("❌ Error in code action provider:", error)
    return { actions: [], dispose: () => {} }
  }
}

/**
 * Checks if the providers are currently registered.
 * Useful for debugging and testing purposes.
 */
export function areProvidersRegistered(): boolean {
  return globalDisposables.length > 0
}

/**
 * Gets information about currently registered providers.
 * Useful for debugging and monitoring.
 */
export function getProviderInfo() {
  return {
    registeredCount: globalDisposables.length,
    isActive: globalDisposables.length > 0,
    types: ["completion", "hover", "codeAction"],
  }
}
