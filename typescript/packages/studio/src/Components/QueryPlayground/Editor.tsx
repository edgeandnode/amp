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
import { UnifiedSQLProvider } from "@/services/sql/UnifiedSQLProvider"

import { ErrorMessages } from "../Form/ErrorMessages"
// eslint-disable-next-line @effect/dprint
import { useFieldContext } from "../Form/form"
// eslint-disable-next-line import-x/newline-after-import
;(self as any).MonacoEnvironment = {
  getWorker() {
    return new editorWorker()
  },
}

export type EditorProps = Omit<MonacoEditorProps, "defaultLanguage" | "language"> & {
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
  theme = "vs-dark",
  validationLevel = "full",
  ...rest
}: Readonly<EditorProps>) {
  const field = useFieldContext<string>()
  const errors = useStore(field.store, (state) => state.meta.errors)
  const touched = useStore(field.store, (state) => state.meta.isTouched)
  const hasErrors = errors.length > 0 && touched

  // Data hooks for SQL intellisense (static data - loaded once)
  const sourcesQuery = useSourcesSuspenseQuery()
  const udfQuery = useUDFSuspenseQuery()

  // Single SQL provider ref
  const sqlProviderRef = useRef<UnifiedSQLProvider | null>(null)

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
        theme={theme}
        value={field.state.value}
        onChange={(val) => field.handleChange(val || "")}
        data-state={hasErrors ? "invalid" : undefined}
        aria-invalid={hasErrors ? "true" : undefined}
        aria-describedby={hasErrors ? `${id}-invalid` : undefined}
        onMount={(editor) => {
          // Configure editor options
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

          // Add keyboard shortcuts
          editor.addCommand(
            monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
            () => onSubmit?.(),
          )

          // Initialize unified SQL provider with static data
          if (sourcesQuery.data && udfQuery.data) {
            sqlProviderRef.current = new UnifiedSQLProvider(
              sourcesQuery.data,
              udfQuery.data,
              {
                validationLevel,
                enablePartialValidation,
                enableDebugLogging: process.env.NODE_ENV === "development",
                minPrefixLength: 0,
                maxSuggestions: 50,
              },
            )

            sqlProviderRef.current.setup(editor)

            console.debug("[Editor] UnifiedSQLProvider initialized", {
              tableCount: sourcesQuery.data.length,
              udfCount: udfQuery.data.length,
            })
          }
        }}
      />
      {hasErrors ? <ErrorMessages id={`${id}-invalid`} errors={errors} /> : null}
    </div>
  )
}
