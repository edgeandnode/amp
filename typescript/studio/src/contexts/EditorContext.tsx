/**
 * Editor Context for SQL Playground
 *
 * Provides a way to communicate between the Monaco Editor and other components
 * like MetadataBrowser that need to insert text into the editor.
 */

import { createContext, useContext, useRef } from "react"
import type { ReactNode } from "react"
import type * as monaco from "monaco-editor"

interface EditorContextType {
  editorRef: React.MutableRefObject<
    monaco.editor.IStandaloneCodeEditor | undefined
  >
  insertText: (text: string) => void
  insertColumn: (columnName: string) => void
}

const EditorContext = createContext<EditorContextType | null>(null)

interface EditorProviderProps {
  children: ReactNode
}

export function EditorProvider({ children }: EditorProviderProps) {
  const editorRef = useRef<monaco.editor.IStandaloneCodeEditor>()

  const insertText = (text: string) => {
    if (editorRef.current) {
      const editor = editorRef.current
      const position = editor.getPosition()
      if (position) {
        editor.executeEdits("insert-text", [
          {
            range: {
              startLineNumber: position.lineNumber,
              startColumn: position.column,
              endLineNumber: position.lineNumber,
              endColumn: position.column,
            },
            text,
            forceMoveMarkers: true,
          },
        ])
        // Move cursor to end of inserted text
        editor.setPosition({
          lineNumber: position.lineNumber,
          column: position.column + text.length,
        })
        editor.focus()
      }
    }
  }

  const insertColumn = (columnName: string) => {
    if (editorRef.current) {
      const editor = editorRef.current
      const model = editor.getModel()
      const position = editor.getPosition()

      if (model && position) {
        const currentValue = model.getValue()
        const lineContent = model.getLineContent(position.lineNumber)
        const beforeCursor = lineContent.substring(0, position.column - 1)

        // Add space before column name if needed
        const needsSpace =
          beforeCursor.length > 0 && !beforeCursor.endsWith(" ")
        const textToInsert = (needsSpace ? " " : "") + columnName

        insertText(textToInsert)

        console.log("📝 Inserted column into editor:", {
          columnName,
          insertedText: textToInsert,
          position: { line: position.lineNumber, column: position.column },
        })
      }
    }
  }

  const contextValue: EditorContextType = {
    editorRef,
    insertText,
    insertColumn,
  }

  return (
    <EditorContext.Provider value={contextValue}>
      {children}
    </EditorContext.Provider>
  )
}

export function useEditor() {
  const context = useContext(EditorContext)
  if (!context) {
    throw new Error("useEditor must be used within an EditorProvider")
  }
  return context
}
