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

import { ErrorMessages } from "../Form/ErrorMessages"
import { useFieldContext } from "../Form/form"

self.MonacoEnvironment = {
  getWorker() {
    return new editorWorker()
  },
}

export type EditorProps = Omit<
  MonacoEditorProps,
  "defaultLanguage" | "language"
> & {
  id: string
  onSubmit?: () => void
}
export function Editor({
  height = 450,
  id,
  onSubmit,
  theme = "vs-dark",
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

  /**
   * Cleanup providers on unmount
   */
  useEffect(() => {
    return () => {
      if (providersRef.current) {
        providersRef.current.dispose()
        providersRef.current = null
      }
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
      {hasErrors ? (
        <ErrorMessages id={`${id}-invalid`} errors={errors} />
      ) : null}
    </div>
  )
}
