"use client"

import type { EditorProps as MonacoEditorProps } from "@monaco-editor/react"
import MonacoEditor from "@monaco-editor/react"
import { useStore } from "@tanstack/react-form"
import { useEffect, useRef } from "react"

import { ErrorMessages } from "../Form/ErrorMessages"
import { useFieldContext } from "../Form/form"
import { useMetadataSuspenseQuery } from "../../hooks/useMetadataQuery"
import { useUDFSuspenseQuery } from "../../hooks/useUDFQuery"
import {
  setupNozzleSQLProviders,
  updateProviderData,
  type DisposableHandle,
} from "../../services/sql"

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
      updateProviderData(metadataQuery.data, udfQuery.data)
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
   * Cleanup providers on unmount
   */
  useEffect(() => {
    return () => {
      if (providersRef.current) {
        providersRef.current.dispose()
        providersRef.current = null
        console.debug("[Editor] SQL intellisense providers disposed")
      }
    }
  }, [])

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
          setupProviders()

          console.debug(
            "[Editor] Monaco editor mounted with SQL intellisense configuration",
          )
        }}
      />
      {hasErrors ? (
        <ErrorMessages id={`${id}-invalid`} errors={errors} />
      ) : null}
    </div>
  )
}
