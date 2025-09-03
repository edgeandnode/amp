"use client"

import { Collapsible } from "@base-ui-components/react/collapsible"
import { FolderIcon, FolderOpenIcon, TableIcon } from "@phosphor-icons/react"

import { useMetadataSuspenseQuery } from "../../hooks/useMetadataQuery"

/** @todo define way to add metadata column to playground input */
export type MetadataBrowserProps = {}
export function MetadataBrowser() {
  const { data: metadata } = useMetadataSuspenseQuery()

  return (
    <div className="flex flex-col gap-y-4 p-6">
      <p className="text-sm">Dataset Metadata</p>
      <div className="group flex items-start relative w-full gap-x-1 px-0 py-2 text-sm">
        <TableIcon className="size-4 text-inherit" aria-hidden="true" />
        <div className="w-full flex flex-col gap-y-1 items-center justify-start">
          <span className="self-start">Logs source</span>
          <span className="text-xs text-gray-600 dark:text-white/55 self-start">
            {metadata.source}
          </span>
        </div>
      </div>
      <Collapsible.Root className="w-full box-border flex flex-col justify-center gap-y-3">
        <Collapsible.Trigger className="group flex items-start relative w-full gap-x-1 px-0 py-2 text-sm cursor-pointer">
          <FolderIcon
            className="size-4 text-inherit group-data-[panel-open]:hidden block"
            aria-hidden="true"
          />
          <FolderOpenIcon
            className="size-4 text-inherit group-data-[panel-open]:block hidden"
            aria-hidden="true"
          />
          <div className="w-full flex flex-col gap-y-1 items-center justify-start">
            <span className="self-start">Columns</span>
            <span className="text-xs text-gray-600 dark:text-white/55 self-start text-left">
              These columns are available on all queries. They represent
              metadata about the transaction, decoded from the logs.
            </span>
          </div>
        </Collapsible.Trigger>
        <Collapsible.Panel className="box-border overflow-y-auto overflow-x-hidden border-l border-gray-300 dark:border-white/25 ml-4 pl-1">
          <div className="w-full flex flex-col gap-y-1">
            {metadata.metadata_columns.map((col) => (
              <button
                key={col.name}
                type="button"
                className="w-full flex items-center justify-between text-sm cursor-pointer border-none outline-none hover:bg-gray-50 dark:hover:bg-slate-800 px-4 py-1.5 rounded-md"
              >
                <span className="text-gray-950 dark:text-white">
                  {col.name}
                </span>
                <span className="ml-auto text-purple-500/70">
                  {col.dataType}
                </span>
              </button>
            ))}
          </div>
        </Collapsible.Panel>
      </Collapsible.Root>
    </div>
  )
}
