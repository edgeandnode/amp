"use client"

import type { EditorProps as MonacoEditorProps } from "@monaco-editor/react"
import MonacoEditor, { loader } from "@monaco-editor/react"
import { useStore } from "@tanstack/react-form"
import * as monaco from "monaco-editor"
// eslint-disable-next-line import-x/default
import editorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker"
import { useEffect, useRef } from "react"

import { USER_DEFINED_FUNCTIONS } from "@/constants"
import { useAmpConfigStreamQuery } from "@/hooks/useAmpConfigStream"
import { useSourcesSuspenseQuery } from "@/hooks/useSourcesQuery"
import { UnifiedSQLProvider } from "@/services/sql/UnifiedSQLProvider"

import { ErrorMessages } from "../Form/ErrorMessages"
import { useFieldContext } from "../Form/form"
import { MONACO_AMP_DARK, registerCustomTheme } from "./monaco-theme"

self.MonacoEnvironment = {
  getWorker() {
    return new editorWorker()
  },
}

export type EditorProps = Omit<MonacoEditorProps, "defaultLanguage" | "language" | "theme"> & {
  id: string
  onSubmit?: () => void
  // SQL Validation configuration
  validationLevel?: "basic" | "standard" | "full" | "off"
  enablePartialValidation?: boolean
}

export function Editor({
  enablePartialValidation = true,
  height = 450,
  id,
  onSubmit,
  validationLevel = "full",
  ...rest
}: Readonly<EditorProps>) {
  const field = useFieldContext<string>()
  const errors = useStore(field.store, (state) => state.meta.errors)
  const touched = useStore(field.store, (state) => state.meta.isTouched)
  const hasErrors = errors.length > 0 && touched

  // Data hooks for SQL intellisense (static data - loaded once)
  const sourcesQuery = useSourcesSuspenseQuery()

  // Single SQL provider ref
  const sqlProviderRef = useRef<UnifiedSQLProvider | null>(null)

  // Stream amp config manifest and update provider when it changes
  const ampConfigQuery = useAmpConfigStreamQuery({
    onSuccess: (newManifest) => {
      // When manifest updates from SSE, update the SQL provider with new data
      sqlProviderRef.current?.updateManifest(newManifest.manifest)
    },
  })

  // Single cleanup effect for unmount
  useEffect(() => {
    return () => {
      // Cleanup SQL provider
      sqlProviderRef.current?.dispose()
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
        theme={MONACO_AMP_DARK}
        value={field.state.value}
        onChange={(val: string | undefined) => field.handleChange(val || "")}
        data-state={hasErrors ? "invalid" : undefined}
        aria-invalid={hasErrors ? "true" : undefined}
        aria-describedby={hasErrors ? `${id}-invalid` : undefined}
        beforeMount={(monaco) => {
          registerCustomTheme(monaco)
        }}
        onMount={(editor: monaco.editor.IStandaloneCodeEditor) => {
          // Configure editor options
          editor.updateOptions({
            fixedOverflowWidgets: true,
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

          // Add keyboard shortcuts
          editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => onSubmit?.())

          // Initialize unified SQL provider with static data and amp config manifest
          if (sourcesQuery.data) {
            sqlProviderRef.current = new UnifiedSQLProvider(
              sourcesQuery.data,
              USER_DEFINED_FUNCTIONS,
              {
                validationLevel,
                enablePartialValidation,
                enableDebugLogging: process.env.NODE_ENV === "development",
                minPrefixLength: 0,
                maxSuggestions: 50,
              },
              ampConfigQuery.data?.manifest, // Pass the manifest from amp config stream
            )

            sqlProviderRef.current.setup(editor)
          }
        }}
      />
      {hasErrors ? <ErrorMessages id={`${id}-invalid`} errors={errors} /> : null}
    </div>
  )
}
