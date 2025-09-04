"use client"

import { useCallback, useState } from "react"
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
  
  // State for tracking which dataset source is currently selected
  const [selectedDatasetIndex, setSelectedDatasetIndex] = useState(0)
  
  // Early return if no metadata is available
  if (!metadata || metadata.length === 0) {
    return (
      <div className="flex flex-col gap-y-4 p-6">
        <p className="text-14">Dataset Metadata</p>
        <p className="text-12 text-space-700">No metadata available</p>
      </div>
    )
  }
  // Get the currently selected metadata object
  const selectedMetadata = metadata[selectedDatasetIndex] || metadata[0]

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
        <div className="w-full flex flex-col gap-y-1 justify-start">
          <span className="self-start text-14">
            Available datasets ({metadata.length})
          </span>
          <div className="flex flex-wrap gap-1">
            {metadata.map((datasetMeta, index) => (
              <button
                key={`${datasetMeta.source}-${index}`}
                onClick={() => setSelectedDatasetIndex(index)}
                className={`px-3 py-1.5 text-12 rounded-md transition-colors border ${
                  selectedDatasetIndex === index
                    ? 'bg-purple-600 text-white border-purple-600'
                    : 'bg-space-1200 text-space-300 border-space-1400 hover:bg-space-1100'
                }`}
                title={`Switch to ${datasetMeta.source} dataset`}
              >
                {datasetMeta.source}
              </button>
            ))}
          </div>
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
            {selectedMetadata.metadata_columns.map((col) => (
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
