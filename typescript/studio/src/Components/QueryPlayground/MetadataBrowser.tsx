"use client"

import { Collapsible } from "@base-ui-components/react/collapsible"
import { FolderIcon, FolderOpenIcon, TableIcon } from "@phosphor-icons/react"
import { useCallback } from "react"

import { useMetadataSuspenseQuery } from "../../hooks/useMetadataQuery"
import { useEditor } from "../../contexts/EditorContext"

/**
 * Column button component with insert functionality
 */
interface ColumnButtonProps {
  column: {
    name: string
    dataType: string
    description?: string
  }
  tableName: string
  onInsert: (columnName: string) => void
}

function ColumnButton({ column, tableName, onInsert }: ColumnButtonProps) {
  const handleClick = useCallback(() => {
    // Insert qualified column name (table.column)
    onInsert(`${tableName}.${column.name}`)
  }, [column.name, tableName, onInsert])

  return (
    <button
      key={column.name}
      type="button"
      onClick={handleClick}
      className="w-full flex items-center justify-between text-sm cursor-pointer border-none outline-none hover:bg-slate-800 px-4 py-1.5 rounded-md transition-colors"
      title={column.description || `Insert ${tableName}.${column.name}`}
      data-testid="column-button"
    >
      <span>{column.name}</span>
      <span className="ml-auto text-purple-500/70">{column.dataType}</span>
    </button>
  )
}

export type MetadataBrowserProps = {}
export function MetadataBrowser() {
  const { data: metadata } = useMetadataSuspenseQuery()
  const { insertColumn } = useEditor()

  // Convert single metadata object to array for consistent handling
  // This supports both the current single object format and future array format
  const metadataArray = Array.isArray(metadata) ? metadata : [metadata]

  /**
   * Inserts a column reference into the editor at the current cursor position
   * This provides a convenient way to add table.column references to queries
   */
  const handleInsertColumn = useCallback(
    (columnName: string) => {
      try {
        insertColumn(columnName)
      } catch (error) {
        console.error("❌ Failed to insert column:", error)
      }
    },
    [insertColumn],
  )

  return (
    <div className="flex flex-col gap-y-4 p-6">
      <p className="text-sm">Dataset Metadata</p>

      {metadataArray.map((dataset, index) => (
        <Collapsible.Root
          key={dataset.source}
          className="w-full"
          data-testid="dataset-item"
        >
          {/* Dataset header with table info */}
          <Collapsible.Trigger className="group flex items-start relative w-full gap-x-1 px-0 py-2 text-sm cursor-pointer">
            <TableIcon className="size-4 text-inherit" aria-hidden="true" />
            <div className="w-full flex flex-col gap-y-1">
              <span className="self-start font-medium">{dataset.source}</span>
              <span className="text-xs text-white/55 self-start">
                {dataset.metadata_columns.length} columns available
              </span>
            </div>
          </Collapsible.Trigger>

          {/* Collapsible column list */}
          <Collapsible.Panel className="border-l border-white/25 ml-4 pl-2">
            <div className="w-full flex flex-col gap-y-1 mt-2">
              <span className="text-xs text-white/40 mb-2 pl-4">
                Click any column to add it to your query
              </span>
              {dataset.metadata_columns.map((column) => (
                <ColumnButton
                  key={`${dataset.source}.${column.name}`}
                  column={column}
                  tableName={dataset.source}
                  onInsert={handleInsertColumn}
                />
              ))}
            </div>
          </Collapsible.Panel>
        </Collapsible.Root>
      ))}

      {/* Helper text for multiple datasets */}
      {metadataArray.length > 1 && (
        <div className="text-xs text-white/40 border-t border-white/10 pt-4 mt-2">
          💡 Tip: Use qualified names (table.column) when querying multiple
          datasets
        </div>
      )}
    </div>
  )
}
