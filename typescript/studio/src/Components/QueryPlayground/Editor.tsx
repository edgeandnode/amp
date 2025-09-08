"use client"

import type { EditorProps as MonacoEditorProps } from "@monaco-editor/react"
import MonacoEditor, { loader } from "@monaco-editor/react"
import { useStore } from "@tanstack/react-form"
import * as monaco from "monaco-editor/esm/vs/editor/editor.api"
// eslint-disable-next-line import-x/default
import editorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker"
import { useEffect, useRef } from "react"

import { useMetadataSuspenseQuery } from "@/hooks/useMetadataQuery"
import { useUDFSuspenseQuery } from "@/hooks/useUDFQuery"
import type { DisposableHandle } from "@/services/sql"
import { setupNozzleSQLProviders } from "@/services/sql"
import { getActiveValidator } from "@/services/sql"
import { updateProviderData } from "@/services/sql"

import { ErrorMessages } from "../Form/ErrorMessages"
import { useFieldContext } from "../Form/form"
// import {
//   setupNozzleSQLProviders,
//   updateProviderData,
//   getActiveValidator,
//   type DisposableHandle,
// } from "../../services/sql"

self.MonacoEnvironment = {
  getWorker() {
    return new editorWorker()
  },
}

export type EditorProps = Omit<MonacoEditorProps, "defaultLanguage" | "language"> & {
  id: string
  onSubmit?: () => void
  // SQL Validation configuration
  validationLevel?: 'basic' | 'standard' | 'full' | 'off'
  enablePartialValidation?: boolean
}
export function Editor({
  height = 450,
  id,
  onSubmit,
  theme = "vs-dark",
  validationLevel = 'full',
  enablePartialValidation = true,
  ...rest
}: Readonly<EditorProps>) {
  const field = useFieldContext<string>()
  const errors = useStore(field.store, (state) => state.meta.errors)
  const touched = useStore(field.store, (state) => state.meta.isTouched)
  const hasErrors = errors.length > 0 && touched

  // Data hooks for SQL intellisense
  const metadataQuery = useMetadataSuspenseQuery()
  const udfQuery = useUDFSuspenseQuery()

  // Provider lifecycle management
  const providersRef = useRef<DisposableHandle | null>(null)
  // Monaco editor instance ref for validation
  const editorRef = useRef<monaco.editor.IStandaloneCodeEditor | null>(null)

  /**
   * Setup SQL providers when Monaco editor is available
   */
  const setupProviders = () => {
    if (
      metadataQuery.data &&
      udfQuery.data &&
      typeof window !== "undefined" &&
      window.monaco
    ) {
      // Dispose existing providers first
      if (providersRef.current) {
        providersRef.current.dispose()
      }

      // Setup providers with initial data
      providersRef.current = setupNozzleSQLProviders(
        [...metadataQuery.data],
        [...udfQuery.data],
        {
          // Enable debug logging in development
          enableDebugLogging: process.env.NODE_ENV === "development",
          // Allow completions without prefix for better UX (especially for columns in SELECT)
          minPrefixLength: 0,
          maxSuggestions: 50,
          // Validation configuration
          enableSqlValidation: validationLevel !== 'off',
          validationLevel: validationLevel !== 'off' ? validationLevel : 'basic',
          enablePartialValidation,
        },
      )

      console.debug("[Editor] SQL intellisense providers initialized", {
        tableCount: metadataQuery.data.length,
        udfCount: udfQuery.data.length,
      })
    }
  }

  /**
   * Effect: Update providers when data changes
   *
   * Handles hot-swapping of metadata and UDF data when the hooks
   * refetch fresh data from the API.
   */
  useEffect(() => {
    if (providersRef.current && metadataQuery.data && udfQuery.data) {
      updateProviderData([...metadataQuery.data], [...udfQuery.data])
      console.debug(
        "[Editor] SQL intellisense providers updated with fresh data",
      )
    }
  }, [
    metadataQuery.data,
    udfQuery.data,
    metadataQuery.dataUpdatedAt,
    udfQuery.dataUpdatedAt,
  ])

  /**
   * Effect: SQL Validation with Error Markers
   * 
   * Sets up real-time SQL validation with Monaco Editor markers.
   * Uses debouncing to avoid excessive validation during rapid typing.
   * Includes error recovery to prevent crashes from validation failures.
   */
  useEffect(() => {
    const editor = editorRef.current
    let validator = getActiveValidator()
    
    // If validator is null but we have all required data, re-setup providers (fixes hot reload issue)
    if (!validator && editor && metadataQuery.data && udfQuery.data && validationLevel !== 'off') {
      console.debug('[Editor] Validator not available, re-setting up providers...')
      setupProviders()
      validator = getActiveValidator()
    }
    
    // Check if validation is enabled and editor is available
    if (!editor || !validator || !metadataQuery.data || !udfQuery.data || validationLevel === 'off') {
      return
    }

    let timeoutId: NodeJS.Timeout | null = null
    
    const validateWithDebounce = () => {
      if (timeoutId) {
        clearTimeout(timeoutId)
      }
      
      timeoutId = setTimeout(() => {
        try {
          const model = editor.getModel()
          if (!model) return

          const query = model.getValue()
          
          // Skip validation for empty queries
          if (!query.trim()) {
            window.monaco.editor.setModelMarkers(model, 'sql-validator', [])
            return
          }

          // Perform validation
          const errors = validator.validateQuery(query)
          
          // Convert validation errors to Monaco markers
          const markers: monaco.editor.IMarkerData[] = errors.map(error => ({
            severity: error.severity,
            message: error.message,
            startLineNumber: error.startLineNumber,
            startColumn: error.startColumn,
            endLineNumber: error.endLineNumber,
            endColumn: error.endColumn,
            code: error.code,
            source: 'nozzle-sql-validator'
          }))
          
          // Set markers in Monaco Editor
          window.monaco.editor.setModelMarkers(model, 'sql-validator', markers)
          
          console.debug(`[Editor] Validation completed: ${errors.length} errors found`)
          
        } catch (error) {
          console.error('[Editor] SQL validation failed:', error)
          // Clear markers on validation error to prevent stale markers
          const model = editor.getModel()
          if (model) {
            window.monaco.editor.setModelMarkers(model, 'sql-validator', [])
          }
        }
      }, 500) // 500ms debounce delay
    }

    // Initial validation
    validateWithDebounce()
    
    // Set up content change listener
    const model = editor.getModel()
    if (model) {
      const disposable = model.onDidChangeContent(() => {
        validateWithDebounce()
      })
      
      // Cleanup function
      return () => {
        if (timeoutId) {
          clearTimeout(timeoutId)
        }
        disposable.dispose()
        // Clear markers on cleanup
        window.monaco?.editor.setModelMarkers(model, 'sql-validator', [])
        console.debug('[Editor] SQL validation disposed')
      }
    }

    // Cleanup function for timeout
    return () => {
      if (timeoutId) {
        clearTimeout(timeoutId)
      }
    }
  }, [metadataQuery.data, udfQuery.data, validationLevel, enablePartialValidation, providersRef.current]) // Re-run when metadata, validation config, or providers change

  /**
   * Cleanup providers on unmount
   */
  useEffect(() => {
    return () => {
      if (providersRef.current) {
        providersRef.current.dispose()
        providersRef.current = null
      }
      // Clear editor reference
      editorRef.current = null
    }
  }, [])

  // use the installed monaco-editor type instead of it being brought down from a CDN
  loader.config({ monaco })

  return (
    <div className="w-full h-full p-0 m-0 flex flex-col gap-y-3">
      <MonacoEditor
        {...rest}
        defaultLanguage="sql"
        language="sql"
        height={height}
        theme={theme}
        value={field.state.value}
        onChange={(val) => field.handleChange(val || "")}
        data-state={hasErrors ? "invalid" : undefined}
        aria-invalid={hasErrors ? "true" : undefined}
        aria-describedby={hasErrors ? `${id}-invalid` : undefined}
        onMount={(editor) => {
          // Store editor reference for validation
          editorRef.current = editor
          
          // Add keyboard shortcut for CMD+ENTER / CTRL+ENTER
          // When user hits CMD/CTRL+ENTER, we submit the query
          editor.addCommand(
            // monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter
            2048 | 3, // KeyMod.CtrlCmd | KeyCode.Enter
            () => {
              onSubmit?.()
            },
          )

          // Enable SQL language features for better intellisense experience
          editor.updateOptions({
            suggest: {
              showWords: false, // Disable generic word suggestions to prioritize SQL completions
              showKeywords: true, // Let our provider handle SQL keywords
              filterGraceful: true, // Enable fuzzy matching
              snippetsPreventQuickSuggestions: false, // Allow snippets with quick suggestions
            },
            quickSuggestions: {
              strings: false, // Disable completions inside string literals
              comments: false, // Disable completions inside comments
              other: true, // Enable completions in other contexts
            },
            parameterHints: {
              enabled: true, // Enable parameter hints for UDF functions
              cycle: true, // Allow cycling through parameter hints
            },
            hover: {
              enabled: true, // Enable hover information for UDF functions
              delay: 300, // Show hover after 300ms
            },
            // Enhanced editing experience
            wordWrap: "on",
            minimap: { enabled: false }, // Disable minimap for better focus
            renderLineHighlight: "gutter", // Highlight current line in gutter only
          })

          // Setup SQL intellisense providers now that Monaco is available
          // Dispose existing providers first
          if (providersRef.current) {
            providersRef.current.dispose()
          }

          // Setup providers with initial data
          providersRef.current = setupNozzleSQLProviders(
            metadataQuery.data,
            udfQuery.data,
            {
              // Enable debug logging in development
              enableDebugLogging: process.env.NODE_ENV === "development",
              // Allow completions without prefix for better UX (especially for columns in SELECT)
              minPrefixLength: 0,
              maxSuggestions: 50,
            },
          )
        }}
      />
      {hasErrors ? <ErrorMessages id={`${id}-invalid`} errors={errors} /> : null}
    </div>
  )
}
