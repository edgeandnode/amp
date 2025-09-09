"use client"

import type { EditorProps as MonacoEditorProps } from "@monaco-editor/react"
import MonacoEditor, { loader } from "@monaco-editor/react"
import { useStore } from "@tanstack/react-form"
import * as monaco from "monaco-editor"
// eslint-disable-next-line import-x/default
import editorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker"
import { useEffect, useRef } from "react"

import { useSourcesSuspenseQuery } from "@/hooks/useSourcesQuery"
import { useUDFSuspenseQuery } from "@/hooks/useUDFQuery"
import { type DisposableHandle, getActiveValidator, setupNozzleSQLProviders } from "@/services/sql"

import { ErrorMessages } from "../Form/ErrorMessages"
import { useFieldContext } from "../Form/form"
;(self as any).MonacoEnvironment = {
  getWorker() {
    return new editorWorker()
  },
}

interface EditorRefs {
  sources: Array<any> | null
  udfs: Array<any> | null
  providersRef: React.MutableRefObject<DisposableHandle | null>
  validatorRef: React.MutableRefObject<any | null>
  validationTimeoutRef: React.MutableRefObject<NodeJS.Timeout | null>
}

export type EditorProps = Omit<MonacoEditorProps, "defaultLanguage" | "language"> & {
  id: string
  onSubmit?: () => void
  // SQL Validation configuration
  validationLevel?: "basic" | "standard" | "full" | "off"
  enablePartialValidation?: boolean
}

/**
 * Setup validation with Monaco events
 */
function setupValidation(
  editor: monaco.editor.IStandaloneCodeEditor,
  model: monaco.editor.ITextModel,
  refs: EditorRefs,
  _props: EditorProps,
) {
  let validationTimeout: NodeJS.Timeout | null = null

  const validateQuery = () => {
    // Ensure we have fresh validator reference
    if (!refs.validatorRef.current) {
      refs.validatorRef.current = getActiveValidator()
    }

    const validator = refs.validatorRef.current
    if (!validator) return

    const query = model.getValue()

    // Clear markers for empty queries
    if (!query.trim()) {
      monaco.editor.setModelMarkers(model, "sql-validator", [])
      return
    }

    try {
      const errors = validator.validateQuery(query)
      const markers: Array<monaco.editor.IMarkerData> = errors.map((error: any) => ({
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
      console.debug(`[Editor] Validation completed: ${errors.length} errors found`)
    } catch (error) {
      console.error("[Editor] SQL validation failed:", error)
      monaco.editor.setModelMarkers(model, "sql-validator", [])
    }
  }

  // Initial validation
  validateQuery()

  // Setup debounced validation on content change
  const contentChangeDisposable = model.onDidChangeContent(() => {
    if (validationTimeout) {
      clearTimeout(validationTimeout)
    }

    validationTimeout = setTimeout(() => {
      validateQuery()
      validationTimeout = null
    }, 500) // 500ms debounce

    // Store timeout ref for cleanup
    refs.validationTimeoutRef.current = validationTimeout
  })

  // Cleanup on model disposal
  model.onWillDispose(() => {
    contentChangeDisposable.dispose()
    if (validationTimeout) {
      clearTimeout(validationTimeout)
    }
    monaco.editor.setModelMarkers(model, "sql-validator", [])
  })
}

/**
 * Setup Monaco editor with consolidated initialization
 */
function setupMonacoEditor(
  editor: monaco.editor.IStandaloneCodeEditor,
  refs: EditorRefs,
  props: EditorProps,
) {
  const model = editor.getModel()
  if (!model) return

  // 1. Configure editor options
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

  // 2. Add keyboard shortcuts
  editor.addCommand(
    monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
    () => props.onSubmit?.(),
  )

  // 3. Initialize providers with current data
  if (refs.sources && refs.udfs) {
    refs.providersRef.current = setupNozzleSQLProviders(
      refs.sources,
      refs.udfs,
      {
        enableDebugLogging: process.env.NODE_ENV === "development",
        minPrefixLength: 0,
        maxSuggestions: 50,
        enableSqlValidation: props.validationLevel !== "off",
        validationLevel: props.validationLevel !== "off" ? props.validationLevel : "basic",
        enablePartialValidation: props.enablePartialValidation,
      },
    )
    refs.validatorRef.current = getActiveValidator()

    console.debug("[Editor] SQL intellisense providers initialized", {
      tableCount: refs.sources.length,
      udfCount: refs.udfs.length,
    })
  }

  // 4. Setup validation with Monaco events
  if (props.validationLevel !== "off") {
    setupValidation(editor, model, refs, props)
  }

  // 5. Register disposal handlers
  editor.onDidDispose(() => {
    if (refs.providersRef.current) {
      refs.providersRef.current.dispose()
      refs.providersRef.current = null
    }
    if (refs.validationTimeoutRef.current) {
      clearTimeout(refs.validationTimeoutRef.current)
      refs.validationTimeoutRef.current = null
    }
  })
}

export function Editor({
  enablePartialValidation = true,
  height = 450,
  id,
  onSubmit,
  theme = "vs-dark",
  validationLevel = "full",
  ...rest
}: Readonly<EditorProps>) {
  const field = useFieldContext<string>()
  const errors = useStore(field.store, (state) => state.meta.errors)
  const touched = useStore(field.store, (state) => state.meta.isTouched)
  const hasErrors = errors.length > 0 && touched

  // Data hooks for SQL intellisense
  const sourcesQuery = useSourcesSuspenseQuery()
  const udfQuery = useUDFSuspenseQuery()

  // Mutable refs for Monaco
  const editorRef = useRef<monaco.editor.IStandaloneCodeEditor | null>(null)
  const providersRef = useRef<DisposableHandle | null>(null)
  const validatorRef = useRef<any | null>(null)
  const validationTimeoutRef = useRef<NodeJS.Timeout | null>(null)

  // Single cleanup effect for unmount
  useEffect(() => {
    return () => {
      // Cleanup all resources
      if (providersRef.current) providersRef.current.dispose()
      if (validationTimeoutRef.current) clearTimeout(validationTimeoutRef.current)
    }
  }, []) // Empty deps - only on unmount

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
          // Store editor reference
          editorRef.current = editor

          // Setup Monaco editor with consolidated initialization
          setupMonacoEditor(editor, {
            sources: sourcesQuery.data ? [...sourcesQuery.data] : null,
            udfs: udfQuery.data ? [...udfQuery.data] : null,
            providersRef,
            validatorRef,
            validationTimeoutRef,
          }, {
            id,
            validationLevel,
            enablePartialValidation,
            onSubmit,
          })
        }}
      />
      {hasErrors ? <ErrorMessages id={`${id}-invalid`} errors={errors} /> : null}
    </div>
  )
}
