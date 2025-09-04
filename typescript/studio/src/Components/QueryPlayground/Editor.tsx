"use client"

import type { EditorProps as MonacoEditorProps } from "@monaco-editor/react"
import MonacoEditor from "@monaco-editor/react"
import { useStore } from "@tanstack/react-form"
import { useEffect } from "react"
import type * as monaco from "monaco-editor"

import { ErrorMessages } from "../Form/ErrorMessages"
import { useFieldContext } from "../Form/form"
import {
  setupNozzleSQLProviders,
  disposeProviders,
} from "../../services/nozzleSQLProviders"
import { useMetadataSuspenseQuery } from "../../hooks/useMetadataQuery"
import { useUDFSuspenseQuery } from "../../hooks/useUDFQuery"
import { useEditor } from "../../contexts/EditorContext"

export type EditorProps = Omit<
  MonacoEditorProps,
  "defaultLanguage" | "language"
> & {
  id: string
}
export function Editor({
  height = 450,
  id,
  theme = "vs-dark",
  ...rest
}: Readonly<EditorProps>) {
  const field = useFieldContext<string>()
  const errors = useStore(field.store, (state) => state.meta.errors)
  const touched = useStore(field.store, (state) => state.meta.isTouched)
  const hasErrors = errors.length > 0 && touched

  // Get editor context for text insertion functionality
  const { editorRef } = useEditor()

  // Fetch metadata and UDFs for intelligent completions
  const { data: metadata } = useMetadataSuspenseQuery()
  const { data: udfs } = useUDFSuspenseQuery()

  /**
   * Sets up SQL providers when editor mounts or data changes
   * This enables intelligent autocompletion with table/column suggestions
   * and UDF documentation based on the current Nozzle configuration
   */
  useEffect(() => {
    // Only setup providers if we have all required data
    if (editorRef.current && metadata && udfs && window.monacoInstance) {
      try {
        console.log("🔧 Setting up Nozzle SQL providers...", {
          editorInstance: !!editorRef.current,
          tablesCount: metadata.length,
          udfsCount: udfs.length,
          metadata: metadata.map((m) => ({
            source: m.source,
            columns: m.metadata_columns.length,
          })),
        })

        setupNozzleSQLProviders(window.monacoInstance, metadata, udfs)
      } catch (error) {
        console.error("Failed to setup SQL providers:", error)
      }
    } else {
      console.log("⏳ Waiting for SQL provider setup dependencies...", {
        hasEditor: !!editorRef.current,
        hasMetadata: !!metadata,
        hasUdfs: !!udfs,
        hasMonaco: !!window.monacoInstance,
        metadataCount: metadata?.length || 0,
        udfsCount: udfs?.length || 0,
      })
    }
  }, [metadata, udfs]) // Re-run when data changes

  /**
   * Cleanup providers when component unmounts to prevent memory leaks
   */
  useEffect(() => {
    return () => {
      console.log("🧹 Cleaning up SQL providers on unmount")
      disposeProviders()
    }
  }, [])

  /**
   * Handles Monaco editor mount event
   * Stores reference and triggers initial provider setup
   */
  const handleEditorMount = (
    editor: monaco.editor.IStandaloneCodeEditor,
    monacoInstance: typeof monaco,
  ) => {
    // Store editor reference for provider setup
    editorRef.current = editor

    // Store monaco instance for provider setup
    window.monacoInstance = monacoInstance

    console.log("Monaco Editor mounted, ready for SQL providers")

    // Immediately try to setup SQL providers if data is available
    if (metadata && udfs) {
      try {
        console.log(
          "🚀 Setting up SQL providers immediately on editor mount...",
          {
            tablesCount: metadata.length,
            udfsCount: udfs.length,
          },
        )

        setupNozzleSQLProviders(monacoInstance, metadata, udfs)
      } catch (error) {
        console.error("Failed to setup SQL providers on mount:", error)
      }
    } else {
      console.log("⏳ Data not yet available for SQL providers on mount...", {
        hasMetadata: !!metadata,
        hasUdfs: !!udfs,
        metadataCount: metadata?.length || 0,
        udfsCount: udfs?.length || 0,
      })
    }

    // Call original onMount if provided
    if (rest.onMount) {
      rest.onMount(editor, monacoInstance)
    }
  }

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
        onMount={handleEditorMount}
        data-state={hasErrors ? "invalid" : undefined}
        aria-invalid={hasErrors ? "true" : undefined}
        aria-describedby={hasErrors ? `${id}-invalid` : undefined}
        // Enhanced Monaco options for better SQL editing experience
        options={{
          // Enable advanced completion features
          acceptSuggestionOnCommitCharacter: true,
          acceptSuggestionOnEnter: "on",
          accessibilitySupport: "auto",
          autoIndent: "full",
          automaticLayout: true,
          // Trigger suggestions on typing
          quickSuggestions: {
            other: true,
            comments: false,
            strings: false,
          },
          suggestOnTriggerCharacters: true,
          // Hover configuration
          hover: {
            enabled: true,
            delay: 300,
          },
          // Additional editor enhancements
          wordWrap: "off",
          lineNumbers: "on",
          minimap: { enabled: false },
          scrollBeyondLastLine: false,
          // Allow mouse wheel zoom
          mouseWheelZoom: true,
          ...rest.options,
        }}
      />
      {hasErrors ? (
        <ErrorMessages id={`${id}-invalid`} errors={errors} />
      ) : null}
    </div>
  )
}
