"use client"

import { useCallback } from "react"
import { Collapsible } from "@base-ui-components/react/collapsible"
import { Button } from "@graphprotocol/gds-react"
import {
  FolderIcon,
  FolderOpenIcon,
  TableIcon,
} from "@graphprotocol/gds-react/icons"

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

  // Get the first metadata object from the array
  // In the future, we might want to show all datasets, but for now we show the first one
  const firstMetadata = metadata[0]
  
  // Early return if no metadata is available
  if (!firstMetadata) {
    return (
      <div className="flex flex-col gap-y-4 p-6">
        <p className="text-14">Dataset Metadata</p>
        <p className="text-12 text-space-700">No metadata available</p>
      </div>
    )
  }

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
      <p className="text-14">Dataset Metadata</p>
      <div className="group flex items-start relative w-full gap-x-1 px-0 py-2 text-sm">
        <TableIcon
          alt="Dataset Metadata"
          className="text-inherit"
          aria-hidden="true"
          size={5}
          variant="regular"
        />
        <div className="w-full flex flex-col gap-y-1 items-center justify-start">
          <span className="self-start text-14">Logs source</span>
          <span className="text-12 text-space-700 self-start">
            {firstMetadata.source}
          </span>
        </div>
      </div>
      <Collapsible.Root className="w-full box-border flex flex-col justify-center gap-y-3">
        <Collapsible.Trigger className="group flex items-start relative w-full gap-x-1 px-0 py-2 cursor-pointer">
          <FolderIcon
            className="text-inherit group-data-[panel-open]:hidden block"
            aria-hidden="true"
            size={5}
            variant="regular"
            alt=""
          />
          <FolderOpenIcon
            className="size-4 text-inherit group-data-[panel-open]:block hidden"
            aria-hidden="true"
            size={5}
            variant="regular"
            alt=""
          />
          <div className="w-full flex flex-col gap-y-1 items-center justify-start">
            <span className="self-start text-14">Columns</span>
            <span className="text-12 text-space-700 self-start text-left">
              These columns are available on all queries. They represent
              metadata about the transaction, decoded from the logs.
            </span>
          </div>
        </Collapsible.Trigger>
        <Collapsible.Panel className="box-border overflow-y-auto overflow-x-hidden border-l border-white/20 ml-4 pl-1">
          <div className="w-full flex flex-col gap-y-1">
            {firstMetadata.metadata_columns.map((col) => (
              <Button
                key={col.name}
                type="button"
                className="w-full flex items-center justify-between text-sm cursor-pointer border-none outline-none hover:bg-space-1200 px-4 py-1.5 rounded-4"
              >
                <span className="text-14">{col.name}</span>
                <span className="ml-auto text-purple-200">{col.dataType}</span>
              </Button>
            ))}
          </div>
        </Collapsible.Panel>
      </Collapsible.Root>
    </div>
  )
}
